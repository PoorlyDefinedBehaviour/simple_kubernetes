use std::collections::{HashMap, VecDeque};

use uuid::Uuid;

use crate::task::{Task, TaskEvent};

pub struct Manager {
    task_queue: VecDeque<Task>,
    workers: Vec<String>,
    task_database: HashMap<String, Vec<Task>>,
    event_database: HashMap<String, Vec<TaskEvent>>,
    worker_task_map: HashMap<String, Vec<Uuid>>,
    task_worker_map: HashMap<String, Vec<Uuid>>,
}

impl Manager {
    fn update_tasks(&mut self) {
        todo!()
    }

    fn select_worker(&mut self) {
        todo!()
    }

    fn send_work(&mut self) {
        todo!()
    }
}
