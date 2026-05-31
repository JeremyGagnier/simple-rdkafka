use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::error::Error;
use std::fmt::Debug;
use std::io::Cursor;
use std::sync::Arc;

use ciborium;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::OwnedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::Offset;
use rdkafka::util::Timeout;
use rdkafka::{Message, TopicPartitionList};
use serde::{Deserialize, Serialize, de, ser};
use tokio;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep, timeout};

struct MessageReverseOrd(OwnedMessage);
impl Ord for MessageReverseOrd {
    fn cmp(&self, other: &Self) -> Ordering {
        other.0.offset().cmp(&self.0.offset())
    }
}

impl PartialOrd for MessageReverseOrd {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for MessageReverseOrd {
    fn eq(&self, other: &Self) -> bool {
        self.0.offset() == other.0.offset()
    }
}

impl Eq for MessageReverseOrd {}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DLQMessage<E, V>
where
    E: Debug + Error + ser::Serialize,
    V: ser::Serialize,
{
    pub error: E,
    pub value: V,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct KafkaProducerConfig {
    pub host_addrs: Vec<String>,
    pub topic_name: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct KafkaConsumerConfig {
    pub host_addrs: Vec<String>,
    pub topic_name: String,
    pub group_name: String,
    pub skip_to_latest: bool,
    pub max_threads: i32,
    pub dlq_config: Option<KafkaProducerConfig>,
}

pub struct SimpleProducer {
    producer: FutureProducer,
    topic_name: String,
}

#[derive(Debug)]
pub enum ProducerError {
    KafkaError {
        error: KafkaError,
    },
    EncoderError {
        error: ciborium::ser::Error<std::io::Error>,
    },
}

impl SimpleProducer {
    pub fn new(config: KafkaProducerConfig) -> KafkaResult<SimpleProducer> {
        SimpleProducer::new_with_overrides(config, |c| c)
    }

    pub fn new_with_overrides<F>(
        config: KafkaProducerConfig,
        overrides: F,
    ) -> KafkaResult<SimpleProducer>
    where
        F: FnOnce(&mut ClientConfig) -> &mut ClientConfig,
    {
        // FEAT: Codify the possible configuration settings as enums.
        let mut empty_config = ClientConfig::new();
        let base_config = empty_config
            .set("bootstrap.servers", config.host_addrs.join(","))
            .set("queue.buffering.max.ms", "0"); // Do not buffer
        let producer = overrides(base_config).create()?;
        Ok(SimpleProducer {
            topic_name: config.topic_name,
            producer,
        })
    }

    async fn produce<K, V, F>(
        &self,
        key: &K,
        payload: &V,
        record_builder: F,
    ) -> Result<(), ProducerError>
    where
        K: ser::Serialize,
        V: ser::Serialize,
        F: FnOnce(FutureRecord<'_, Vec<u8>, Vec<u8>>) -> FutureRecord<'_, Vec<u8>, Vec<u8>>,
    {
        let mut key_bytes: Vec<u8> = Vec::new();
        let mut payload_bytes: Vec<u8> = Vec::new();
        ciborium::into_writer(&key, &mut key_bytes)
            .map_err(|error| ProducerError::EncoderError { error })?;
        ciborium::into_writer(&payload, &mut payload_bytes)
            .map_err(|error| ProducerError::EncoderError { error })?;
        self.produce_bytes(&key_bytes, &payload_bytes, record_builder)
            .await
    }

    async fn produce_bytes<F>(
        &self,
        key_bytes: &Vec<u8>,
        payload_bytes: &Vec<u8>,
        record_builder: F,
    ) -> Result<(), ProducerError>
    where
        F: FnOnce(FutureRecord<'_, Vec<u8>, Vec<u8>>) -> FutureRecord<'_, Vec<u8>, Vec<u8>>,
    {
        let record = record_builder(
            FutureRecord::to(&self.topic_name)
                .key(key_bytes)
                .payload(payload_bytes),
        );
        // FEAT: Allow configuring the timeout
        self.producer
            .send(record, Timeout::Never)
            .await
            .map_err(|error| ProducerError::KafkaError { error: error.0 })?;
        Ok(())
    }

    pub async fn send_with_key<K: ser::Serialize, V: ser::Serialize>(
        &self,
        key: &K,
        value: &V,
    ) -> Result<(), ProducerError> {
        self.produce(key, value, |r| r).await
    }

    pub async fn send_with_partition<T: ser::Serialize>(
        &self,
        message: &T,
        partition: i32,
    ) -> Result<(), ProducerError> {
        self.produce(&(), message, |r| r.partition(partition)).await
    }

    pub async fn send<T: ser::Serialize>(&self, message: &T) -> Result<(), ProducerError> {
        self.produce(&(), message, |r| r).await
    }
}

pub struct SimpleConsumer<T: de::DeserializeOwned, E: Error + Send> {
    consumer: StreamConsumer,
    handler: fn(&T) -> Result<(), E>,
    max_threads: i32,
    dlq_producer: Option<SimpleProducer>,
}

#[derive(Debug)]
pub enum ConsumerError<E: Debug + Error> {
    KafkaError {
        error: KafkaError,
    },
    HandlerError {
        error: E,
    },
    DecoderError {
        error: ciborium::de::Error<std::io::Error>,
    },
    NoPayload,
}

impl<T, E> SimpleConsumer<T, E>
where
    T: ser::Serialize + de::DeserializeOwned + Send + 'static,
    E: ser::Serialize + Error + Send + 'static,
{
    pub fn new(
        config: KafkaConsumerConfig,
        handler: fn(&T) -> Result<(), E>,
    ) -> KafkaResult<SimpleConsumer<T, E>> {
        SimpleConsumer::new_with_overrides_and_partitions(config, handler, |c| c, None)
    }

    pub fn new_with_overrides<F>(
        config: KafkaConsumerConfig,
        handler: fn(&T) -> Result<(), E>,
        overrides: F,
    ) -> KafkaResult<SimpleConsumer<T, E>>
    where
        F: FnOnce(&mut ClientConfig) -> &mut ClientConfig,
    {
        SimpleConsumer::new_with_overrides_and_partitions(config, handler, overrides, None)
    }

    pub fn new_with_partitions(
        config: KafkaConsumerConfig,
        handler: fn(&T) -> Result<(), E>,
        partitions: Option<Vec<i32>>,
    ) -> KafkaResult<SimpleConsumer<T, E>> {
        SimpleConsumer::new_with_overrides_and_partitions(config, handler, |c| c, partitions)
    }

    pub fn new_with_overrides_and_partitions<F>(
        config: KafkaConsumerConfig,
        handler: fn(&T) -> Result<(), E>,
        overrides: F,
        partitions: Option<Vec<i32>>,
    ) -> KafkaResult<SimpleConsumer<T, E>>
    where
        F: FnOnce(&mut ClientConfig) -> &mut ClientConfig,
    {
        // FEAT: Codify the possible configuration settings as enums.
        let mut empty_config = ClientConfig::new();
        let base_config = empty_config
            .set("group.id", config.group_name)
            .set("bootstrap.servers", config.host_addrs.join(","))
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set(
                "auto.offset.reset",
                if config.skip_to_latest {
                    "latest"
                } else {
                    "earliest"
                },
            );
        let consumer: StreamConsumer = overrides(base_config).create()?;
        if let Some(partition_ids) = partitions {
            let mut list = TopicPartitionList::new();
            for partition_id in partition_ids {
                if config.skip_to_latest {
                    list.add_partition_offset(&config.topic_name, partition_id, Offset::End)?;
                } else {
                    list.add_partition(&config.topic_name, partition_id);
                }
            }
            consumer.assign(&list)?;
        } else {
            consumer.subscribe(&[config.topic_name.as_str()])?;
            // FEAT: This code should be ran on post_rebalance and requires defining a CounsumerContext.
            /*
            if config.skip_to_latest {
                if let Ok(tpl) = consumer.subscription() {
                    for ((topic, partition_id), offset) in tpl.to_topic_map() {
                        if offset != Offset::End {
                            consumer.seek(&topic, partition_id, Offset::End, Timeout::After(Duration::from_secs(1)))?;
                        }
                    }
                }
            }
            */
        }
        let dlq_producer = if let Some(dlq_config) = config.dlq_config {
            Some(SimpleProducer::new(dlq_config)?)
        } else {
            None
        };
        Ok(SimpleConsumer {
            consumer: consumer,
            handler: handler,
            max_threads: config.max_threads,
            dlq_producer: dlq_producer,
        })
    }

    fn decode<M: Message>(&self, message: &M) -> Result<T, ConsumerError<E>> {
        let payload = message.payload().ok_or_else(|| ConsumerError::NoPayload)?;
        // FEAT: Allow the user to select an encoding
        let decoded: T = ciborium::from_reader(Cursor::new(payload))
            .map_err(|error| ConsumerError::DecoderError { error: error })?;
        Ok(decoded)
    }

    fn consume(self: &Arc<Self>, owned_msg: OwnedMessage) -> JoinHandle<OwnedMessage> {
        let self_clone = Arc::clone(self);
        tokio::spawn(async move {
            match self_clone.decode(&owned_msg) {
                Ok(message) => {
                    if let Some(producer) = &self_clone.dlq_producer {
                        if let Err(error) = (self_clone.handler)(&message) {
                            // HandlerErrors are sent to the DLQ.
                            log::error!(
                                "error handling message, sending to the configured DLQ: {error:#?}"
                            );
                            let dlq_message = DLQMessage {
                                error: error,
                                value: message,
                            };
                            let key_bytes = if let Some(key) = owned_msg.key() {
                                key.to_vec()
                            } else {
                                Vec::new()
                            };
                            let mut payload_bytes: Vec<u8> = Vec::new();
                            if let Err(encode_error) =
                                ciborium::into_writer(&dlq_message, &mut payload_bytes)
                            {
                                log::error!("error encoding DLQ message: {encode_error:#?}");
                            } else {
                                while let Err(producer_error) = producer
                                    .produce_bytes(&key_bytes, &payload_bytes, |i| i)
                                    .await
                                {
                                    log::error!(
                                        "error publishing message to the configured DLQ: {producer_error:#?}"
                                    );
                                    sleep(Duration::from_millis(10)).await;
                                }
                            }
                        };
                    } else {
                        // When there's no DLQ retry indefinitely
                        // FEAT: Include a handler error type that is unretryable
                        loop {
                            if let Err(error) = (self_clone.handler)(&message) {
                                log::error!("error handling message, retrying: {error:#?}");
                                sleep(Duration::from_millis(10)).await;
                            } else {
                                break;
                            }
                        }
                    }
                }
                Err(error) => log::error!("error handling message, skipping: {error:#?}"),
            }
            owned_msg
        })
    }

    pub fn run_consumer(self) -> JoinHandle<ConsumerError<E>> {
        let self_arc = Arc::new(self);
        let self_clone = Arc::clone(&self_arc);
        tokio::spawn(async move {
            let mut handles: Vec<Option<JoinHandle<OwnedMessage>>> =
                Vec::with_capacity(self_clone.max_threads as usize);
            for _ in 0..self_clone.max_threads {
                handles.push(None);
            }
            let mut offset_heap: BinaryHeap<MessageReverseOrd> = BinaryHeap::new();
            let mut scan_idx = 0; // The current index in handles which is empty
            let mut next_offset: i64 = -1; // The next offset to commit
            let error: ConsumerError<E>;
            loop {
                // Loop over handles, stopping once we find a spot that can be overridden.
                let starting_idx = scan_idx;
                while !handles[scan_idx]
                    .as_ref()
                    .is_none_or(|handle| handle.is_finished())
                {
                    scan_idx = (scan_idx + 1) % self_clone.max_threads as usize;
                    // Each time all threads are checked do a short sleep
                    if scan_idx == starting_idx {
                        sleep(Duration::from_millis(10)).await;
                    }
                }
                // Iterate over all handles for completed ones.
                for handle in handles.iter_mut() {
                    if handle.as_ref().is_some_and(|handle| handle.is_finished()) {
                        let message = handle.take().unwrap().await.unwrap();
                        offset_heap.push(MessageReverseOrd(message))
                    }
                }
                // Find the largest offset to commit.
                let mut commitable_message = None;
                while let Some(message) = offset_heap.peek()
                    && message.0.offset() == next_offset
                {
                    commitable_message = offset_heap.pop();
                    next_offset += 1;
                }
                if let Some(message) = commitable_message {
                    // If the commit fails it's fine because we will commit later offsets as we go.
                    // FEAT: Allow the user to configure the commit mode (async vs sync)
                    let mut tpl = TopicPartitionList::new();
                    if let Err(commit_err) = tpl.add_partition_offset(
                        message.0.topic(),
                        message.0.partition(),
                        Offset::Offset(message.0.offset()),
                    ) {
                        log::warn!("error building commit offset list: {commit_err:#?}");
                    } else if let Err(error) = self_clone
                        .consumer
                        .commit(&tpl, rdkafka::consumer::CommitMode::Async)
                    {
                        log::warn!("error committing message: {error:#?}")
                    }
                }
                // If we don't receive a message in 10ms then loop again to check for completed consumes.
                // FEAT: Allow the user to configure this timeout.
                if let Some(result) = timeout(Duration::from_millis(10), self_clone.consumer.recv())
                    .await
                    .ok()
                {
                    let borrowed_message = match result {
                        Ok(message) => {
                            // Initialize next_offset on the first consume
                            if next_offset == -1 {
                                next_offset = message.offset();
                            }
                            message
                        }
                        Err(e) => {
                            error = ConsumerError::KafkaError { error: e };
                            break;
                        }
                    };
                    handles[scan_idx] = Some(self_clone.consume(borrowed_message.detach()));
                }
            }
            error
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::mocking::MockCluster;
    use std::collections::HashSet;
    use std::fmt;
    use std::sync::Mutex;
    use std::sync::OnceLock;
    use std::time::Instant;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct TestMessage {
        text: String,
    }

    fn override_consumer_config(config: &mut ClientConfig) -> &mut ClientConfig {
        config.set("test.mock.num.brokers", "1")
    }

    fn override_producer_config(config: &mut ClientConfig) -> &mut ClientConfig {
        config.set("test.mock.num.brokers", "1")
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct TestError {}
    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Error")
        }
    }
    impl Error for TestError {}

    #[tokio::test]
    async fn initialize_consumer() {
        let test_config = KafkaConsumerConfig {
            host_addrs: Vec::new(),
            topic_name: "test_topic".to_string(),
            group_name: "test_group".to_string(),
            skip_to_latest: true,
            max_threads: 64,
            dlq_config: None,
        };
        let consumer_result = SimpleConsumer::<(), TestError>::new_with_overrides(
            test_config,
            |_: &()| Ok(()),
            override_consumer_config,
        );
        assert!(consumer_result.is_ok());
        let consumer = consumer_result.unwrap();
        let handle = consumer.run_consumer();
        handle.abort();
    }

    #[tokio::test]
    async fn test_producer_sends_message() {
        let producer_config = KafkaProducerConfig {
            host_addrs: Vec::new(),
            topic_name: "test_produce_topic".to_string(),
        };
        let producer_result =
            SimpleProducer::new_with_overrides(producer_config, override_producer_config);
        assert!(producer_result.is_ok());
        let producer = producer_result.unwrap();
        let result = producer.send(&()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_producer_consumer_end_to_end() {
        static CONSUMED: OnceLock<Arc<Mutex<Vec<TestMessage>>>> = OnceLock::new();
        assert!(CONSUMED.set(Arc::new(Mutex::new(Vec::new()))).is_ok());
        fn handler(message: &TestMessage) -> Result<(), TestError> {
            CONSUMED
                .get()
                .unwrap()
                .lock()
                .unwrap()
                .push(message.clone());
            Ok(())
        }

        let topic_name = "test_topic";
        let test_msg1 = TestMessage {
            text: "test1".to_string(),
        };
        let test_msg2 = TestMessage {
            text: "test2".to_string(),
        };
        let test_msg3 = TestMessage {
            text: "test3".to_string(),
        };

        let mock_cluster = MockCluster::new(1).unwrap();
        mock_cluster.create_topic(topic_name, 1, 1).unwrap();
        let bootstrap_servers = mock_cluster.bootstrap_servers();

        let consumer_config = KafkaConsumerConfig {
            host_addrs: vec![bootstrap_servers.clone()],
            topic_name: topic_name.to_string(),
            group_name: "test_e2e_group".to_string(),
            skip_to_latest: false,
            max_threads: 1,
            dlq_config: None,
        };
        let producer_config = KafkaProducerConfig {
            host_addrs: vec![bootstrap_servers.clone()],
            topic_name: topic_name.to_string(),
        };

        let consumer_result = SimpleConsumer::<TestMessage, TestError>::new_with_partitions(
            consumer_config,
            handler,
            Some(vec![0]),
        );
        assert!(consumer_result.is_ok());
        let consumer = consumer_result.unwrap();

        let producer_result = SimpleProducer::new(producer_config);
        assert!(producer_result.is_ok());
        let producer = producer_result.unwrap();

        // Check that messages produced before the consumer starts are consumed.
        assert!(producer.send(&test_msg1).await.is_ok());

        let consumer_handle = consumer.run_consumer();

        assert!(producer.send(&test_msg2).await.is_ok());
        assert!(producer.send(&test_msg3).await.is_ok());

        let timeout = Instant::now() + Duration::from_secs(1);
        while Instant::now() < timeout {
            if CONSUMED.get().unwrap().lock().unwrap().len() >= 3 {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }

        let consumed_messages = CONSUMED.get().unwrap().lock().unwrap().clone();
        assert_eq!(consumed_messages.len(), 3);

        let received_set: HashSet<String> =
            consumed_messages.into_iter().map(|msg| msg.text).collect();
        assert!(received_set.contains("test1"));
        assert!(received_set.contains("test2"));
        assert!(received_set.contains("test3"));

        consumer_handle.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 64)]
    async fn test_consumer_multiple_threads() {
        static CONSUMED: OnceLock<Arc<Mutex<Vec<()>>>> = OnceLock::new();
        assert!(CONSUMED.set(Arc::new(Mutex::new(Vec::new()))).is_ok());
        fn handler(message: &()) -> Result<(), TestError> {
            // Use thread sleep so that the function is not async.
            std::thread::sleep(Duration::from_millis(50));
            CONSUMED.get().unwrap().lock().unwrap().push(*message);
            Ok(())
        }

        let topic_name = "test_topic";
        let mock_cluster = MockCluster::new(1).unwrap();
        mock_cluster.create_topic(topic_name, 1, 1).unwrap();
        let bootstrap_servers = mock_cluster.bootstrap_servers();

        let consumer_config = KafkaConsumerConfig {
            host_addrs: vec![bootstrap_servers.clone()],
            topic_name: topic_name.to_string(),
            group_name: "test_e2e_group".to_string(),
            skip_to_latest: false,
            max_threads: 64,
            dlq_config: None,
        };
        let producer_config = KafkaProducerConfig {
            host_addrs: vec![bootstrap_servers.clone()],
            topic_name: topic_name.to_string(),
        };

        let consumer_result = SimpleConsumer::<(), TestError>::new_with_partitions(
            consumer_config,
            handler,
            Some(vec![0]),
        );
        assert!(consumer_result.is_ok());
        let consumer = consumer_result.unwrap();

        let producer_result = SimpleProducer::new(producer_config);
        assert!(producer_result.is_ok());
        let producer = producer_result.unwrap();

        let mut message_count = 0;
        // Produce a bunch of messages so that there is a backlog to take up the threads.
        for _ in 0..100 {
            assert!(producer.send(&()).await.is_ok());
            message_count += 1;
        }

        let consumer_handle = consumer.run_consumer();

        for _ in 100..200 {
            assert!(producer.send(&()).await.is_ok());
            message_count += 1;
        }

        let timeout = Instant::now() + Duration::from_secs(1);
        while Instant::now() < timeout {
            if CONSUMED.get().unwrap().lock().unwrap().len() >= message_count {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }

        let consumed_messages = CONSUMED.get().unwrap().lock().unwrap().clone();
        assert_eq!(consumed_messages.len(), message_count);

        consumer_handle.abort();
    }
}
