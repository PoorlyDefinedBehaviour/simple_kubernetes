use anyhow::Result;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::SocketAddr,
    sync::Arc,
};
use tokio::sync::Mutex;
use tracing::info;
use uuid::Uuid;

use crate::{
    definition::Definition,
    scheduler::{CandidateSelectionInput, Scheduler},
    task::{Task, TaskEvent, TaskName},
    worker::{Worker, WorkerId},
};

struct TaskEntry {
    definition: Definition,
    task: Task,
    workers: HashSet<WorkerId>,
}

#[derive(Debug, Clone)]
pub struct RemoteWorker {
    pub worker_id: WorkerId,
    pub worker_addr: SocketAddr,
}

pub struct Manager {
    task_queue: VecDeque<Task>,
    /// The set of workers in the cluster.
    workers: Mutex<HashMap<WorkerId, RemoteWorker>>,
    /// The state of the tasks running in the cluster.
    tasks: HashMap<TaskName, TaskEntry>,
    scheduler: Box<dyn Scheduler>,
}

impl Manager {
    #[tracing::instrument(name = "Manager::new", skip_all)]
    pub fn new(scheduler: Box<dyn Scheduler>) -> Self {
        Self {
            task_queue: VecDeque::new(),
            workers: Mutex::new(HashMap::new()),
            tasks: HashMap::new(),
            scheduler,
        }
    }

    #[tracing::instrument(name = "Manager::register_worker", skip_all, fields(
        worker = ?worker
    ))]
    pub async fn register_worker(&self, worker: RemoteWorker) {
        let mut workers = self.workers.lock().await;
        if workers.insert(worker.worker_id, worker.clone()).is_none() {
            info!(?worker, "new worker registered");
        }
    }

    #[tracing::instrument(name = "Manager::apply", skip_all, fields(
      definition = ?definition
    ))]
    pub async fn apply(&mut self, definition: Definition) -> Result<()> {
        // let candidate_nodes = self
        //     .scheduler
        //     .select_candidate_nodes(&CandidateSelectionInput {
        //         definition: &definition,
        //         workers: &self.workers,
        //     })
        //     .await?;

        // let worker = self
        //     .workers
        //     .get_mut(&candidate_nodes[0])
        //     .expect("worker should exist");

        // match self.tasks.get(definition.metadata_name()) {
        //     // It is a new task.
        //     None => {
        //         worker.run_task(&definition).await?;
        //     }
        //     // Task is already running in the cluster.
        //     Some(task) => {
        //         todo!()
        //     }
        // }

        todo!()
    }
}
