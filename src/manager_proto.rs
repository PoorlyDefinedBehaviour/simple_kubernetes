pub mod manager_proto {
    tonic::include_proto!("manager");
}

use prost::{DecodeError, Message};

pub use manager_proto::*;

use crate::resources_proto;

impl NodeHeartbeatRequest {
    #[tracing::instrument(name = "NodeHeartbeatRequest::available_memory", skip_all)]
    pub fn available_memory(&self) -> u64 {
        let available_memory = self.max_memory - self.memory_allocated;
        tracing::Span::current().record("available_memory", available_memory);
        available_memory
    }
}

impl From<resources_proto::Task> for Task {
    fn from(input: resources_proto::Task) -> Self {
        Self {
            name: input.name,
            containers: input.containers.into_iter().map(Container::from).collect(),
        }
    }
}

impl From<resources_proto::Container> for Container {
    fn from(input: resources_proto::Container) -> Self {
        Self {
            name: input.name,
            status: input.status.map(|status| Status {
                state: status.state,
            }),
        }
    }
}

impl TryFrom<etcd_rs::KeyValue> for manager_proto::NodeHeartbeatRequest {
    type Error = DecodeError;

    fn try_from(kv: etcd_rs::KeyValue) -> Result<Self, Self::Error> {
        manager_proto::NodeHeartbeatRequest::decode(kv.value.as_ref())
    }
}
