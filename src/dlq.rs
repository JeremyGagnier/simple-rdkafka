use serde::{Deserialize, Serialize, ser};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum DLQData<V>
where
  V: ser::Serialize,
{
  Message(V),
  Bytes(Vec<u8>),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DLQMessage<V>
where
  V: ser::Serialize,
{
  pub error: String,
  pub value: DLQData<V>,
}
