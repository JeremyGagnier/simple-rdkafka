use serde::{Deserialize, Serialize};
use tokio::time::Duration;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ProducerConfig {
  /// The addresses of the Kafka brokers.
  pub host_addrs: Vec<String>,
  /// The name of the topic to produce to.
  pub topic_name: String,
  /// How long the producer will wait when adding a message to the send buffer for non-sync publishes.
  pub timeout: Duration,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ConsumerConfig {
  /// The addresses of the Kafka brokers.
  pub host_addrs: Vec<String>,
  /// The name of the topic to consume from.
  pub topic_name: String,
  /// The consumer group name. Each consumer group has its own set of offsets for a topic.
  pub group_name: String,
  /// If true the consumer will attempt to set the offset to the latest offset for each partition
  /// upon startup.
  pub skip_to_latest: bool,
  /// The maximum number of messages that will be processed in parallel.
  pub max_threads: i32,
  /// Optional producer to send failed messages to. They will be sent as `DLQMessage`s.
  pub dlq_config: Option<ProducerConfig>,
}
