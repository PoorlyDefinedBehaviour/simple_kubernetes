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
