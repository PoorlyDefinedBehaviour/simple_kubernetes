pub mod task_proto {
    tonic::include_proto!("task");
}

pub use task_proto::*;
use uuid::Uuid;

use crate::definition;

pub type TaskName = String;

impl From<definition::Definition> for Tasks {
    fn from(input: definition::Definition) -> Self {
        Self {
            tasks: input.spec.containers.into_iter().map(Task::from).collect(),
        }
    }
}

impl From<definition::ContainerSpec> for Task {
    fn from(input: definition::ContainerSpec) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            container_id: String::new(),
            name: input.name,
            state: State::Pending.into(),
            image: input.image,
            port_bindings: input.ports.into_iter().map(PortBinding::from).collect(),
        }
    }
}

impl From<definition::Port> for PortBinding {
    fn from(input: definition::Port) -> Self {
        Self {
            port: input.container_port as u32,
            protocol: input.protocol,
        }
    }
}

impl State {
    pub fn as_u16(&self) -> u16 {
        match self {
            State::Pending => 0,
            State::Running => 1,
            State::Completed => 2,
            State::Failed => 3,
            State::Created => 4,
        }
    }

    pub fn as_i32(&self) -> i32 {
        self.as_u16() as i32
    }
}

impl TryFrom<&str> for State {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let state = match value {
            // One of created, restarting, running, removing, paused, exited, or dead
            "running" | "restarting" => State::Running,
            "exited" => State::Failed,
            "paused" | "dead" => State::Failed,
            "created" => State::Created,
            s => return Err(anyhow::anyhow!("unexpected state. state={}", s)),
        };

        Ok(state)
    }
}

impl Task {
    pub fn is_running(&self) -> bool {
        self.state == State::Running.as_i32()
    }

    pub fn is_completed(&self) -> bool {
        self.state == State::Completed.as_i32()
    }
}
