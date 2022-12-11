use anyhow::Result;
use etcd_rs::{Client, ClientConfig, KeyValueOp, PutRequest};
use prost::Message;
use serde::Deserialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::info;
use uuid::Uuid;

use crate::manager_proto::RegisterWorkerRequest;
use crate::task_proto::{Task, TaskName, Tasks};
use crate::{
    definition::Definition,
    scheduler::{CandidateSelectionInput, Scheduler},
    worker::WorkerId,
};

#[derive(Debug, Deserialize)]
pub struct Config {
    pub worker: WorkerConfig,
}

#[derive(Debug, Deserialize)]
pub struct WorkerConfig {
    pub heartbeat_timeout: u64,
}

impl Config {
    #[tracing::instrument(name = "manager::Config::from_file", skip_all, fields(
      file_path = ?file_path.as_ref()
  ))]
    pub async fn from_file(file_path: impl AsRef<Path>) -> Result<Self> {
        let file_contents = tokio::fs::read_to_string(file_path.as_ref()).await?;

        let config: Config = serde_yaml::from_str(&file_contents)?;

        Ok(config)
    }
}

struct TaskEntry {
    definition: Definition,
    workers: HashSet<WorkerId>,
}

impl TryFrom<RegisterWorkerRequest> for RemoteWorker {
    type Error = anyhow::Error;

    fn try_from(input: RegisterWorkerRequest) -> Result<RemoteWorker> {
        Ok(RemoteWorker {
            worker_id: Uuid::parse_str(&input.worker_id)?,
            heartbeat_received_at: Instant::now(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct RemoteWorker {
    pub worker_id: WorkerId,
    pub heartbeat_received_at: Instant,
    // pub max_memory: usize,
    // pub memory_allocated: usize,
    // pub max_disk_size: usize,
    // pub disk_allocated: usize,
}

pub struct Manager {
    config: Config,
    task_queue: VecDeque<Task>,
    /// The set of workers in the cluster.
    workers: Mutex<HashMap<WorkerId, RemoteWorker>>,
    /// The state of the tasks running in the cluster.
    tasks: HashMap<TaskName, TaskEntry>,
    scheduler: Box<dyn Scheduler>,
    etcd: Client,
}

impl Manager {
    #[tracing::instrument(name = "Manager::new", skip_all)]
    pub async fn new(config: Config, scheduler: Box<dyn Scheduler>) -> Result<Self> {
        Ok(Self {
            config,
            task_queue: VecDeque::new(),
            workers: Mutex::new(HashMap::new()),
            tasks: HashMap::new(),
            scheduler,
            etcd: Client::connect(ClientConfig::new([
                "http://127.0.0.1:2379".into(),
                "http://127.0.0.1:2380".into(),
            ]))
            .await?,
        })
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
    pub async fn apply(&self, definition: Definition) -> Result<()> {
        let mut workers = self.workers.lock().await;

        // Remove workers that haven't sent a heartbeat in a while because they may be dead.
        let workers_to_remove: Vec<WorkerId> = workers
            .iter()
            .filter_map(|(worker_id, worker)| {
                if worker.heartbeat_received_at.elapsed()
                    > Duration::from_secs(self.config.worker.heartbeat_timeout)
                {
                    Some(*worker_id)
                } else {
                    None
                }
            })
            .collect();

        for worker_id in workers_to_remove {
            workers.remove(&worker_id);
        }

        let worker_id = self
            .scheduler
            .select_node_to_run_container(&CandidateSelectionInput {
                definition: &definition,
                workers: &workers,
            })
            .await?;

        let tasks = Tasks::from(definition);

        self.set_worker_desired_state(worker_id, tasks).await?;

        Ok(())
    }

    /// Sets the state that the worker should match. The worker will see that the desired state changed
    /// and make the necessary changes.
    #[tracing::instrument(name = "manager::set_worker_desired_state", skip_all, fields(
        worker_id = %worker_id,
        tasks = ?tasks
    ))]
    async fn set_worker_desired_state(&self, worker_id: WorkerId, tasks: Tasks) -> Result<()> {
        let key = format!("node/{}/desired_state", worker_id);
        info!(?key, "setting the node state");

        let buffer = tasks.encode_to_vec();

        self.etcd.put(PutRequest::new(key, buffer)).await?;

        Ok(())
    }
}
