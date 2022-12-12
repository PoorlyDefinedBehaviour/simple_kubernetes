pub mod task_proto {
    tonic::include_proto!("task");
}

pub use task_proto::*;
use uuid::Uuid;

use crate::definition;
use anyhow::{Context, Result};

pub type TaskId = String;
pub type TaskSetName = String;

impl From<definition::Definition> for TaskSet {
    fn from(input: definition::Definition) -> Self {
        Self {
            name: input.metadata_name().to_owned(),
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
            resources: Some(Resources::from(input.resources)),
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

impl From<definition::Resources> for Resources {
    fn from(input: definition::Resources) -> Self {
        Self {
            requests: Some(ResourcesSpec {
                memory: input.requests.memory,
                cpu: input.requests.cpu,
            }),
            limits: Some(ResourcesSpec {
                memory: input.limits.memory,
                cpu: input.limits.cpu,
            }),
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

impl TaskSet {
    /// Returns the amount of memory needed to run every container in the set.
    pub fn necessary_memory_in_bytes(&self) -> Result<u64> {
        let mut memory_requests = 0;
        for task in self.tasks.iter() {
            memory_requests += task.memory_requests_in_bytes()?;
        }
        Ok(memory_requests)
    }
}

impl Task {
    pub fn is_running(&self) -> bool {
        self.state == State::Running.as_i32()
    }

    pub fn is_completed(&self) -> bool {
        self.state == State::Completed.as_i32()
    }

    /// Returns the amount of memory requested to run the container.
    pub fn memory_requests_in_bytes(&self) -> Result<u64> {
        let requests_memory = self
            .resources
            .as_ref()
            .and_then(|resources| resources.requests.as_ref())
            .map(|requests| requests.memory.as_ref())
            .context("container must define resources")?;

        to_bytes(requests_memory)
    }
}

/// Translates a string like 64Mi or 250m to the same amount in bytes.
fn to_bytes(repr: &str) -> Result<u64> {
    const MB: u64 = 1_000_000;
    const MIB: u64 = 1_048_576;

    let (n, unit) = split_in_n_and_unit(repr)?;

    match unit {
        "Mi" => Ok(n * MIB),
        "m" => Ok(n * MB),
        s => Err(anyhow::anyhow!("unexpected unit: {}", s)),
    }
}

fn split_in_n_and_unit(repr: &str) -> Result<(u64, &str)> {
    let mut i = 0;
    for character in repr.chars() {
        if !character.is_ascii_digit() {
            break;
        }
        i += 1;
    }

    let (n, s) = repr.split_at(i);
    let n = n.parse::<u64>()?;
    Ok((n, s))
}
