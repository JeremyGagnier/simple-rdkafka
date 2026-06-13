use serde::{Deserialize, Serialize};
use tokio::time::Duration;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct KafkaProducerConfig {
    pub host_addrs: Vec<String>,
    pub topic_name: String,
    pub timeout: Duration,
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
