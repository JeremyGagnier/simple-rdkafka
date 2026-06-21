use serde::{Deserialize, Serialize, ser};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum DLQData<T: ser::Serialize>
{
  /// Message is used when the message can be deserialized.
  Message(T),
  /// Bytes is used when the message cannot be deserialized.
  Bytes(Vec<u8>),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DLQMessage<V: ser::Serialize>
{
  /// error that caused the message to be published to the DLQ.
  pub error: String,
  /// value is the payload of the message that encountered the error during handling.
  pub value: DLQData<V>,
  /// partition is the partition that the message was sent to.
  pub partition: i32,
}
