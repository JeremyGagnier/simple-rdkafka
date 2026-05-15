use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::error::Error;
use std::fmt::Debug;
use std::io::Cursor;
use std::sync::Arc;

use ciborium;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
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
        value: &V,
        record_builder: F,
    ) -> Result<(), ProducerError>
    where
        K: ser::Serialize,
        V: ser::Serialize,
        F: FnOnce(FutureRecord<'_, Vec<u8>, Vec<u8>>) -> FutureRecord<'_, Vec<u8>, Vec<u8>>,
    {
        let mut key_bytes: Vec<u8> = Vec::new();
        let mut payload: Vec<u8> = Vec::new();
        ciborium::into_writer(&key, &mut key_bytes)
            .map_err(|error| ProducerError::EncoderError { error })?;
        ciborium::into_writer(&value, &mut payload)
            .map_err(|error| ProducerError::EncoderError { error })?;
        let record = record_builder(
            FutureRecord::to(&self.topic_name)
                .key(&key_bytes)
                .payload(&payload),
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

    pub async fn send_message<M: Message>(&self, message: &M) -> Result<(), ProducerError> {
        let mut record = FutureRecord::to(&self.topic_name).partition(message.partition());
        if let Some(key) = message.key() {
            record = record.key(key);
        }
        if let Some(payload) = message.payload() {
            record = record.payload(payload);
        }
        self.producer
            .send(record, Timeout::Never)
            .await
            .map_err(|error| ProducerError::KafkaError { error: error.0 })?;
        Ok(())
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

impl<T: de::DeserializeOwned + 'static, E: Error + Send + 'static> SimpleConsumer<T, E> {
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

    pub fn new_with_partitions<F>(
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
        // TODO: Confirm the possible configuration settings
        let mut empty_config = ClientConfig::new();
        let base_config = empty_config
            .set("group.id", config.group_name)
            .set("bootstrap.servers", config.host_addrs.join(","))
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "true")
            .set_log_level(RDKafkaLogLevel::Debug);
        let consumer: StreamConsumer = overrides(base_config).create()?;
        if let Some(partition_ids) = partitions {
            let mut list = TopicPartitionList::new();
            for partition_id in partition_ids {
                list.add_partition(&config.topic_name, partition_id);
            }
            consumer.assign(&list)?;
        } else {
            consumer.subscribe(&[config.topic_name.as_str()])?;
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

    pub fn handle<M: Message>(&self, message: &M) -> Result<(), ConsumerError<E>> {
        let payload = message.payload().ok_or_else(|| ConsumerError::NoPayload)?;
        // FEAT: Allow the user to select an encoding
        let decoded: T = ciborium::from_reader(Cursor::new(payload))
            .map_err(|error| ConsumerError::DecoderError { error: error })?;
        (self.handler)(&decoded).map_err(|error| ConsumerError::HandlerError { error })?;
        Ok(())
    }

    pub fn consume(self: &Arc<Self>, owned_msg: OwnedMessage) -> JoinHandle<OwnedMessage> {
        let self_clone = Arc::clone(self);
        tokio::spawn(async move {
            if let Some(producer) = &self_clone.dlq_producer {
                match self_clone.handle(&owned_msg) {
                    // HandlerErrors are sent to the DLQ.
                    // FEAT: Bundle the error with the message
                    Err(ConsumerError::HandlerError { error }) => {
                        log::error!(
                            "error handling message, sending to the configured DLQ: {error:#?}"
                        );
                        while let Err(producer_error) = producer.send_message(&owned_msg).await {
                            log::error!(
                                "error publishing message to the configured DLQ: {producer_error:#?}"
                            );
                            sleep(Duration::from_millis(10)).await;
                        }
                    }
                    // No payload or decoder errors are skipped
                    Err(error) => log::error!("error handling message, skipping: {error:#?}"),
                    _ => (),
                };
            } else {
                // When there's no DLQ retry indefinitely
                // FEAT: Include a handler error type that is unretryable
                loop {
                    match self_clone.handle(&owned_msg) {
                        Err(ConsumerError::HandlerError { error }) => {
                            log::error!("error handling message, retrying: {error:#?}");
                            sleep(Duration::from_millis(10)).await;
                        }
                        Err(error) => {
                            log::error!("error handling message, skipping: {error:#?}");
                            break;
                        }
                        _ => break,
                    }
                }
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
                    // TODO: Figure out if there is a better way to commit the offset without the heavy TopicPartitionList.
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

pub fn kafka_producer_from_config(config: KafkaProducerConfig) -> KafkaResult<FutureProducer> {
    // TODO: Confirm the possible configuration settings
    ClientConfig::new()
        .set("bootstrap.servers", config.host_addrs.join(","))
        .set("queue.buffering.max.ms", "0") // Do not buffer
        .create()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn override_consumer_config(config: &mut ClientConfig) -> &mut ClientConfig {
        config.set("test.mock.num.brokers", "3")
    }

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
        let consumer_result = SimpleConsumer::<(), KafkaError>::new_with_overrides(
            test_config,
            |_: &()| Ok(()),
            override_consumer_config,
        );
        assert!(consumer_result.is_ok());
        let consumer = consumer_result.unwrap();
        consumer.run_consumer();
    }
}
