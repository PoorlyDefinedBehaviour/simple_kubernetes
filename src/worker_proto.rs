pub mod worker_proto {
    tonic::include_proto!("worker");
}

pub type WorkerId = String;

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
