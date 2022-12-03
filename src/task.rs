use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq)]
pub enum State {
    Pending,
    Scheduled,
    Completed,
    Running,
    Failed,
}

pub struct Task {
    id: Uuid,
    name: String,
    state: State,
    image: String,
    memory: usize,
    disk: usize,
    exposed_ports: HashSet<u16>,
    port_bindings: HashMap<String, String>,
    restart_policy: String,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
}

pub struct TaskEvent {
    id: Uuid,
    state: State,
    created_at: DateTime<Utc>,
    task_id: Uuid,
}

pub struct Config {
    /// Name of the task.
    name: String,
    attach_stdin: bool,
    attach_stdout: bool,
    attach_stderr: bool,
    /// Command to run inside the container.
    command: Vec<String>,
    /// The image that will be run as a container.
    image: String,
    /// How much memory the container will need.
    memory: usize,
    /// How much disk space the container will need.
    disk: usize,
    /// Key value pairs to pass to the container as env variables.
    env_variables: Vec<String>,
    /// When the container should be restarted.
    restart_policy: String,
}
