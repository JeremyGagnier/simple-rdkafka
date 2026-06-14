pub mod config;
pub mod consumer;
pub mod dlq;
pub mod producer;
pub mod retryable;

#[cfg(test)]
mod tests {
  use crate::config::*;
  use crate::consumer::*;
  use crate::producer::*;
  use crate::retryable::*;

  use std::error::Error;
  use std::fmt::Debug;
  use std::sync::{Arc, OnceLock};

  use rdkafka::config::ClientConfig;
  use rdkafka::mocking::MockCluster;
  use serde::{Deserialize, Serialize};
  use std::collections::HashSet;
  use std::fmt;
  use std::sync::Mutex;
  use std::time::Instant;
  use tokio;
  use tokio::time::{Duration, sleep};
  use tokio_util::sync::CancellationToken;

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
  impl Retryable for TestError {
    fn retryable(&self) -> bool {
      true
    }
  }

  #[tokio::test]
  async fn initialize_consumer() {
    let test_config = ConsumerConfig {
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
    let handle = consumer.run_consumer(CancellationToken::new());
    handle.abort();
  }

  #[tokio::test]
  async fn test_producer_sends_message() {
    let producer_config = ProducerConfig {
      host_addrs: Vec::new(),
      topic_name: "test_produce_topic".to_string(),
      timeout: Duration::from_secs(1),
    };
    let producer_result =
      SimpleProducer::new_with_overrides(producer_config, override_producer_config);
    assert!(producer_result.is_ok());
    let producer = producer_result.unwrap();
    let result = producer.send(&(), true).await;
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

    let consumer_config = ConsumerConfig {
      host_addrs: vec![bootstrap_servers.clone()],
      topic_name: topic_name.to_string(),
      group_name: "test_e2e_group".to_string(),
      skip_to_latest: false,
      max_threads: 1,
      dlq_config: None,
    };
    let producer_config = ProducerConfig {
      host_addrs: vec![bootstrap_servers.clone()],
      topic_name: topic_name.to_string(),
      timeout: Duration::from_secs(1),
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
    assert!(producer.send(&test_msg1, true).await.is_ok());

    let consumer_handle = consumer.run_consumer(CancellationToken::new());

    assert!(producer.send(&test_msg2, true).await.is_ok());
    assert!(producer.send(&test_msg3, true).await.is_ok());

    let timeout = Instant::now() + Duration::from_secs(1);
    while Instant::now() < timeout {
      if CONSUMED.get().unwrap().lock().unwrap().len() >= 3 {
        break;
      }
      sleep(Duration::from_millis(50)).await;
    }

    let consumed_messages = CONSUMED.get().unwrap().lock().unwrap().clone();
    assert_eq!(consumed_messages.len(), 3);

    let received_set: HashSet<String> = consumed_messages.into_iter().map(|msg| msg.text).collect();
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

    let consumer_config = ConsumerConfig {
      host_addrs: vec![bootstrap_servers.clone()],
      topic_name: topic_name.to_string(),
      group_name: "test_e2e_group".to_string(),
      skip_to_latest: false,
      max_threads: 64,
      dlq_config: None,
    };
    let producer_config = ProducerConfig {
      host_addrs: vec![bootstrap_servers.clone()],
      topic_name: topic_name.to_string(),
      timeout: Duration::from_secs(1),
    };

    let consumer_result =
      SimpleConsumer::<(), TestError>::new_with_partitions(consumer_config, handler, Some(vec![0]));
    assert!(consumer_result.is_ok());
    let consumer = consumer_result.unwrap();

    let producer_result = SimpleProducer::new(producer_config);
    assert!(producer_result.is_ok());
    let producer = producer_result.unwrap();

    let mut message_count = 0;
    // Produce a bunch of messages so that there is a backlog to take up the threads.
    for _ in 0..100 {
      assert!(producer.send(&(), true).await.is_ok());
      message_count += 1;
    }

    let consumer_handle = consumer.run_consumer(CancellationToken::new());

    for _ in 100..200 {
      assert!(producer.send(&(), true).await.is_ok());
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
