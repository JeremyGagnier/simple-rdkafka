use crate::config::*;
use crate::retryable::*;

use std::error::Error;
use std::fmt::Debug;

use ciborium;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use serde::ser;
use tokio;
use tokio::time::Duration;

/// Use this to produce messages to a single kafka topic. Configured with `config::ProducerConfig`.
/// This producer encodes messages in ciborium format and is intended to be used with `SimpleConsumer`.
///
/// # Example
///
/// ```no_run
/// use simple_rdkafka::config::ProducerConfig;
/// use simple_rdkafka::producer::SimpleProducer;
/// use serde::Serialize;
/// use tokio::time::Duration;
///
/// #[derive(Serialize)]
/// struct Message {
///   text: String,
/// }
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///   let config = ProducerConfig {
///     host_addrs: vec!["localhost:9092".to_string()],
///     topic_name: "example-topic".to_string(),
///     timeout: Duration::from_secs(1),
///   };
///
///   let producer = SimpleProducer::new(config)?;
///   let message = Message { text: "hello".into() };
///
///   producer.send(&message, true).await?;
///   Ok(())
/// }
/// ```
pub struct SimpleProducer {
  producer: FutureProducer,
  topic_name: String,
  timeout: Duration,
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
impl std::fmt::Display for ProducerError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match &self {
      &ProducerError::KafkaError { error } => write!(f, "Kafka error: {}", error),
      &ProducerError::EncoderError { error } => write!(f, "Encoder error: {}", error),
    }
  }
}
impl Error for ProducerError {}
impl Retryable for ProducerError {
  fn retryable(&self) -> bool {
    match &self {
      // Not enough information is known about the underlying KafkaErrors that are
      // unretryable so they are all assumed to be retryable.
      &ProducerError::KafkaError { error: _ } => true,
      &ProducerError::EncoderError { error: _ } => false,
    }
  }
}

impl SimpleProducer {
  pub fn new(config: ProducerConfig) -> KafkaResult<SimpleProducer> {
    SimpleProducer::new_with_overrides(config, |c| c)
  }

  pub fn new_with_overrides<F>(config: ProducerConfig, overrides: F) -> KafkaResult<SimpleProducer>
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
      timeout: config.timeout,
    })
  }

  async fn produce<K, V, F>(
    &self,
    key: &K,
    payload: &V,
    sync: bool,
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
    self
      .produce_bytes(&key_bytes, &payload_bytes, sync, record_builder)
      .await
  }

  pub(crate) async fn produce_bytes<F>(
    &self,
    key_bytes: &Vec<u8>,
    payload_bytes: &Vec<u8>,
    sync: bool,
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
    let mut timeout = Timeout::Never;
    if !sync {
      timeout = Timeout::After(self.timeout);
    }
    self
      .producer
      .send(record, timeout)
      .await
      .map_err(|error| ProducerError::KafkaError { error: error.0 })?;
    if sync {
      self.flush(Timeout::Never)?;
    }
    Ok(())
  }

  pub fn flush(&self, timeout: Timeout) -> Result<(), ProducerError> {
    self
      .producer
      .flush(timeout)
      .map_err(|error| ProducerError::KafkaError { error })
  }

  pub async fn send_with_key<K: ser::Serialize, V: ser::Serialize>(
    &self,
    key: &K,
    value: &V,
    sync: bool,
  ) -> Result<(), ProducerError> {
    self.produce(key, value, sync, |r| r).await
  }

  pub async fn send_with_partition<T: ser::Serialize>(
    &self,
    message: &T,
    partition: i32,
    sync: bool,
  ) -> Result<(), ProducerError> {
    self
      .produce(&(), message, sync, |r| r.partition(partition))
      .await
  }

  pub async fn send<T: ser::Serialize>(
    &self,
    message: &T,
    sync: bool,
  ) -> Result<(), ProducerError> {
    self.produce(&(), message, sync, |r| r).await
  }
}
