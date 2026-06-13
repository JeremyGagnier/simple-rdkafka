use crate::config::*;
use crate::producer::*;
use crate::retryable::*;

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::error::Error;
use std::fmt::Debug;
use std::io::Cursor;
use std::sync::{Arc, OnceLock};

use ciborium;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::OwnedMessage;
use rdkafka::topic_partition_list::Offset;
use rdkafka::util::Timeout;
use rdkafka::{ClientContext, Message, TopicPartitionList};
use serde::{Deserialize, Serialize, de, ser};
use tokio;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep, timeout};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DLQMessage<E, V>
where
    E: Debug + Error + ser::Serialize,
    V: ser::Serialize,
{
    pub error: E,
    pub value: V,
}

// For storing messages in a min heap based on offset
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

#[derive(Debug)]
pub enum ConsumerError<E: Retryable> {
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
impl<E: Retryable> std::fmt::Display for ConsumerError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            &ConsumerError::KafkaError { error } => write!(f, "Kafka error: {}", error),
            &ConsumerError::HandlerError { error } => write!(f, "Handler error: {}", error),
            &ConsumerError::DecoderError { error } => write!(f, "Decoder error: {}", error),
            &ConsumerError::NoPayload => write!(f, "No payload"),
        }
    }
}
impl<E: Retryable> Error for ConsumerError<E> {}
impl<E: Retryable> Retryable for ConsumerError<E> {
    fn retryable(&self) -> bool {
        match &self {
            // Not enough information is known about the underlying KafkaErrors that are
            // unretryable so they are all assumed to be retryable.
            &ConsumerError::KafkaError { error: _ } => true,
            &ConsumerError::HandlerError { error } => error.retryable(),
            &ConsumerError::DecoderError { error: _ } => false,
            &ConsumerError::NoPayload => false,
        }
    }
}

struct SimpleConsumerContext {
    skip_to_latest: bool,
    rebalanced: OnceLock<()>,
}
impl ClientContext for SimpleConsumerContext {}
impl ConsumerContext for SimpleConsumerContext {
    fn post_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Assign(tpl) => {
                if self.skip_to_latest && self.rebalanced.get().is_none() {
                    self.rebalanced.set(()).ok();
                    for ((topic, partition_id), offset) in tpl.to_topic_map() {
                        if offset != Offset::End {
                            consumer
                                .seek(&topic, partition_id, Offset::End, Timeout::Never)
                                .ok();
                        }
                    }
                }
            }
            _ => (),
        }
    }
}
pub struct SimpleConsumer<T: de::DeserializeOwned, E: Retryable + Send> {
    consumer: StreamConsumer<SimpleConsumerContext>,
    handler: fn(&T) -> Result<(), E>,
    max_threads: i32,
    dlq_producer: Option<SimpleProducer>,
}
impl<T, E> SimpleConsumer<T, E>
where
    T: ser::Serialize + de::DeserializeOwned + Send + 'static,
    E: ser::Serialize + Retryable + Send + 'static,
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
        let context = SimpleConsumerContext {
            skip_to_latest: config.skip_to_latest,
            rebalanced: OnceLock::new(),
        };
        let consumer: StreamConsumer<SimpleConsumerContext> =
            overrides(base_config).create_with_context(context)?;
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
            // Skip to latest is implemented in the consumer context for regular subscriptions
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
                                    .produce_bytes(&key_bytes, &payload_bytes, true, |i| i)
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
                        loop {
                            if let Err(error) = (self_clone.handler)(&message)
                                && error.retryable()
                            {
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
