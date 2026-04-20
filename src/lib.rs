use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::error::Error;
use std::fmt::Debug;
use std::io::Cursor;

use ciborium;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::Consumer;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{Message, TopicPartitionList};
use tokio;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep, timeout};
use serde::{Deserialize, Serialize, de, ser};

struct MessageReverseOrd(BorrowedMessage<'static>);
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
        let producer = ClientConfig::new()
            .set("bootstrap.servers", config.host_addrs.join(","))
            .set("queue.buffering.max.ms", "0") // Do not buffer
            .create()?;
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
        F: Fn(FutureRecord<'_, Vec<u8>, Vec<u8>>) -> FutureRecord<'_, Vec<u8>, Vec<u8>>,
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

    pub async fn send_message(
        &self,
        message: &BorrowedMessage<'static>,
    ) -> Result<(), ProducerError> {
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

impl<T: de::DeserializeOwned, E: Error + Send> SimpleConsumer<T, E> {
    pub fn new(
        config: KafkaConsumerConfig,
        handler: fn(&T) -> Result<(), E>,
    ) -> KafkaResult<SimpleConsumer<T, E>> {
        SimpleConsumer::new_with_partitions(config, handler, None)
    }

    pub fn new_with_partitions(
        config: KafkaConsumerConfig,
        handler: fn(&T) -> Result<(), E>,
        partitions: Option<Vec<i32>>,
    ) -> KafkaResult<SimpleConsumer<T, E>> {
        // TODO: Confirm the possible configuration settings
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", config.group_name)
            .set("bootstrap.servers", config.host_addrs.join(","))
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "true")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create()?;
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

    pub fn handle<'a>(
        &'static self,
        borrowed_msg: &BorrowedMessage<'a>,
    ) -> Result<(), ConsumerError<E>> {
        let payload = borrowed_msg
            .payload()
            .ok_or_else(|| ConsumerError::NoPayload)?;
        // FEAT: Allow the user to select an encoding
        let message: T = ciborium::from_reader(Cursor::new(payload))
            .map_err(|error| ConsumerError::DecoderError { error: error })?;
        (self.handler)(&message).map_err(|error| ConsumerError::HandlerError { error })?;
        Ok(())
    }

    pub fn consume(
        &'static self,
        borrowed_msg: BorrowedMessage<'static>,
    ) -> JoinHandle<BorrowedMessage<'static>> {
        tokio::spawn(async move {
            if let Some(producer) = &self.dlq_producer {
                match self.handle(&borrowed_msg) {
                    // HandlerErrors are sent to the DLQ.
                    // FEAT: Bundle the error with the message
                    Err(ConsumerError::HandlerError { error }) => {
                        log::error!("error handling message, sending to the configured DLQ: {error:#?}");
                        while let Err(producer_error) = producer.send_message(&borrowed_msg).await {
                            log::error!("error publishing message to the configured DLQ: {producer_error:#?}");
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
                loop{
                    match self.handle(&borrowed_msg) {
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
            borrowed_msg
        })
    }

    pub async fn run_consumer(&'static mut self) -> JoinHandle<ConsumerError<E>> {
        tokio::spawn(async move {
            let mut handles: Vec<Option<JoinHandle<BorrowedMessage<'static>>>> =
                Vec::with_capacity(self.max_threads as usize);
            for _ in 0..self.max_threads {
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
                    scan_idx = (scan_idx + 1) % self.max_threads as usize;
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
                    if let Err(error) = self.consumer.commit_message(&message.0, rdkafka::consumer::CommitMode::Async) {
                        log::warn!("error committing message: {error:#?}")
                    }
                }
                // If we don't receive a message in 10ms then loop again to check for completed consumes.
                if let Some(result) = timeout(Duration::from_millis(10), self.consumer.recv())
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
                    handles[scan_idx] = Some(self.consume(borrowed_message));
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
