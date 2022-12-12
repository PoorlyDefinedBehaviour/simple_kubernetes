pub mod worker_proto {
    tonic::include_proto!("worker");
}

pub type WorkerId = String;

use prost::{DecodeError, Message};
pub use worker_proto::*;

use crate::worker::LocalTask;

impl CurrentState {
    pub fn available_memory(&self) -> u64 {
        self.max_memory - self.memory_allocated
    }
}

impl From<LocalTask> for Task {
    fn from(input: LocalTask) -> Self {
        Self {
            name: input.name,
            state: input.state,
            image: input.image,
        }
    }
}

impl TryFrom<etcd_rs::KeyValue> for worker_proto::CurrentState {
    type Error = DecodeError;

    fn try_from(kv: etcd_rs::KeyValue) -> Result<Self, Self::Error> {
        worker_proto::CurrentState::try_from(&kv)
    }
}

impl TryFrom<&etcd_rs::KeyValue> for worker_proto::CurrentState {
    type Error = DecodeError;

    fn try_from(kv: &etcd_rs::KeyValue) -> Result<Self, Self::Error> {
        worker_proto::CurrentState::decode(kv.value.as_ref())
    }
}
