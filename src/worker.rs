use std::collections::{HashMap, VecDeque};

use uuid::Uuid;

use crate::task::Task;

pub struct Worker {
    task_queue: VecDeque<Task>,
    database: HashMap<Uuid, Task>,
}

impl Worker {
    pub fn collect_stats(&mut self) {
        todo!()
    }

    pub fn run_task(&mut self) {
        todo!()
    }

    pub fn start_task(&mut self) {
        todo!()
    }

    pub fn stop_task(&mut self) {
        todo!()
    }
}
