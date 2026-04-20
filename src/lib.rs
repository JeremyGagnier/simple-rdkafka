use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::error::Error;
use std::io::{BufReader, Cursor};

use ciborium;

use rdkafka::Message;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, MessageStream};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use tokio;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep, timeout};

use serde::{Deserialize, Serialize, de};

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
    pub require_ack: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct KafkaConsumerConfig {
    pub host_addrs: Vec<String>,
    pub topic_name: String,
    pub group_name: String,
    pub skip_to_latest: bool,
    pub max_threads: i32,
}

pub struct SimpleConsumer<T: de::DeserializeOwned, E: Error + Send> {
    consumer: StreamConsumer,
    handler: fn(&T) -> Result<(), E>,
    max_threads: i32,
    dlq_publisher: Option<()>, // TODO: Implement publisher code
}

pub enum ConsumerError<E: Error> {
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
        consumer.subscribe(&[config.topic_name.as_str()])?;
        Ok(SimpleConsumer {
            consumer: consumer,
            handler: handler,
            max_threads: config.max_threads,
            dlq_publisher: None,
        })
    }

    pub fn handle<'a>(
        &'static self,
        borrowed_msg: &BorrowedMessage<'a>,
    ) -> Result<(), ConsumerError<E>> {
        let payload = borrowed_msg
            .payload()
            .ok_or_else(|| ConsumerError::NoPayload)?;
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
            if let Some(publisher) = self.dlq_publisher {
                match self.handle(&borrowed_msg) {
                    // HandlerErrors are retried
                    Err(ConsumerError::HandlerError { error }) => {
                        // TODO: Publish to DLQ
                    }
                    // No payload or decoder errors are skipped
                    Err(_) => (),
                    _ => (),
                };
            } else {
                // When there's no DLQ retry indefinitely
                // FEAT: Include a handler error type that is unretryable
                while let Err(ConsumerError::HandlerError { error }) = self.handle(&borrowed_msg) {}
            }
            borrowed_msg
        })
    }

    pub async fn run_consumer(&'static mut self) -> JoinHandle<ConsumerError<E>> {
        tokio::spawn(async move {
            let mut handles: Vec<Option<JoinHandle<BorrowedMessage<'static>>>> =
                Vec::with_capacity(self.max_threads as usize);
            let mut offset_heap: BinaryHeap<MessageReverseOrd> = BinaryHeap::new();
            for _ in 0..self.max_threads {
                handles.push(None);
            }
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
                        // FEAT: Handle join errors more gracefully
                        let message = handle.take().unwrap().await.expect("JoinHandle error");
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
                    self.consumer
                        .commit_message(&message.0, rdkafka::consumer::CommitMode::Async)
                        .ok();
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
