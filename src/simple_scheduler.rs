use crate::constants::WORKER_TASK_LIST_KEY;
use anyhow::Result;
use chrono::Utc;
use etcd_rs::{Client, KeyValueOp, PutRequest};
use prost::Message;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use tokio::select;

use tracing::{error, info, warn};

use crate::constants::{TASK_LIST_KEY, WORKER_LIST_KEY};
use crate::list_watcher::ListWatcher;

use crate::manager_proto;
use crate::resources_proto::{self, Task, TaskName};
use crate::worker::WorkerId;

#[derive(Debug, Deserialize)]
pub struct Config {
    worker: WorkerConfig,
}

#[derive(Debug, Deserialize)]
struct WorkerConfig {
    heartbeat_timeout_secs: u64,
}

impl Config {
    #[tracing::instrument("simple_scheduler::Config::from_file", skip_all, fields(
        file_path = ?file_path.as_ref()
    ))]
    pub async fn from_file(file_path: impl AsRef<Path>) -> Result<Self> {
        let file_contents = tokio::fs::read_to_string(file_path.as_ref()).await?;

        let config: Config = serde_yaml::from_str(&file_contents)?;

        Ok(config)
    }
}

pub struct SimpleScheduler {
    config: Config,
    /// Information about workers that are part of the cluster.
    workers: HashMap<WorkerId, manager_proto::NodeHeartbeatRequest>,
    /// Tasks that are running in the cluster.
    tasks: HashMap<TaskName, resources_proto::Task>,
    /// Client used to communicate with etcd.
    etcd: Client,
}

impl SimpleScheduler {
    #[tracing::instrument(name = "SimpleScheduler::new", skip_all, fields(
        config = ?config
    ))]
    pub fn new(config: Config, etcd: Client) -> Self {
        Self {
            config,
            workers: HashMap::new(),
            tasks: HashMap::new(),
            etcd,
        }
    }

    #[tracing::instrument(name = "SimpleScheduler::watch_cluster_state_changes", skip_all)]
    pub async fn watch_cluster_state_changes(mut self) {
        let (tasks_tx, mut tasks_rx) = tokio::sync::mpsc::channel::<resources_proto::Task>(100);
        let tasks_list_watcher = ListWatcher::new(
            // TODO: make configurable
            Duration::from_secs(15),
            tasks_tx,
            self.etcd.clone(),
        );

        tokio::spawn(tasks_list_watcher.list_and_watch(TASK_LIST_KEY.to_owned()));

        let (workers_tx, mut workers_rx) =
            tokio::sync::mpsc::channel::<manager_proto::NodeHeartbeatRequest>(100);
        let workers_list_watcher = ListWatcher::new(
            // TODO: make configurable
            Duration::from_secs(15),
            workers_tx,
            self.etcd.clone(),
        );

        tokio::spawn(workers_list_watcher.list_and_watch(WORKER_LIST_KEY.to_owned()));

        let mut ensure_state_consistency_interval = tokio::time::interval(Duration::from_secs(15));

        loop {
            select! {
                message = tasks_rx.recv() => {
                    let mut task = match message {
                        None => {
                            error!("tasks list watcher channel closed unexpectedly");
                            return;
                        }
                        Some(v) => v
                    };


                    if let Err(error) = self.remove_dead_workers().await {
                        error!(?error, "unable to remove dead workers");
                        continue;
                    };
                    if let Err(error) = self.schedule(&mut task).await  {
                        error!(?error, "unable to schedule tasks");
                    }

                    self.tasks.insert(task.name.clone(), task);
                },
                message = workers_rx.recv() => {
                    let state = match message {
                        None => {
                            error!("workers list watcher channel closed unexpectedly");
                            return;
                        }
                        Some(v) => v
                    };

                    // worker id is empty after a worker is deleted.
                    if !state.worker_id.is_empty() {
                        self.workers.insert(state.worker_id.clone(), state);
                    }
                },
                _ = ensure_state_consistency_interval.tick() => {
                    if let Err(error) = self.ensure_state_consistency().await {
                        error!(?error, "unable to ensure state consistency");
                    }
                }
            }
        }
    }

    #[tracing::instrument(name = "SimpleScheduler::schedule", skip_all)]
    async fn schedule(&mut self, task: &mut Task) -> Result<()> {
        // TODO: this is wrong, if a worker is already running a task
        // its state should be modified.

        match self.select_worker(&task)? {
            None => Err(anyhow::anyhow!("no workers are able to execute the task")),
            Some(worker_id) => {
                let key = format!("{WORKER_TASK_LIST_KEY}/{}/{}", worker_id, task.name);

                info!(?worker_id, task_name = ?task.name, "assigning taskset to worker");

                task.node = Some(resources_proto::Node { id: worker_id });

                self.etcd
                    .put(PutRequest::new(key, task.encode_to_vec()))
                    .await?;

                Ok(())
            }
        }
    }

    #[tracing::instrument(name = "SimpleScheduler::ensure_state_consistency", skip_all)]
    async fn ensure_state_consistency(&mut self) -> Result<()> {
        if let Err(error) = self.remove_dead_workers().await {
            error!(?error, "unable to remove dead workers");
        };

        let tasks_to_reschedule: Vec<_> = self
            .tasks
            .iter()
            .filter_map(|(_, task)| match &task.node {
                None => None,
                Some(node) => {
                    if self.workers.contains_key(&node.id) {
                        None
                    } else {
                        // Dead nodes will have been removed from the workers map so we
                        // need to reschedule the task in anotehr node.
                        Some(task)
                    }
                }
            })
            .cloned()
            .collect();

        for mut task in tasks_to_reschedule {
            if let Err(error) = self.schedule(&mut task).await {
                warn!(?error, ?task, "unable to schedule task");
            }
            self.tasks.insert(task.name.clone(), task);
        }

        Ok(())
    }

    #[tracing::instrument(name = "SimpleScheduler::remove_dead_workers", skip_all)]
    async fn remove_dead_workers(&mut self) -> Result<()> {
        let mut workers_to_remove = vec![];

        let now = Utc::now().timestamp_millis();
        let heartbeat_timeout =
            Duration::from_secs(self.config.worker.heartbeat_timeout_secs).as_millis() as i64;

        for worker in self.workers.values() {
            if now - worker.timestamp > heartbeat_timeout {
                workers_to_remove.push(worker.worker_id.clone());
            }
        }

        info!(
            ?workers_to_remove,
            "workers that have not sent a heartbeat in a while"
        );

        for worker_id in workers_to_remove {
            let response = self
                .etcd
                .delete(format!("{WORKER_LIST_KEY}/{worker_id}"))
                .await?;

            if response.deleted > 0 {
                self.workers.remove(&worker_id);
            }
        }

        Ok(())
    }

    #[tracing::instrument(name = "SimpleScheduler::select_worker", skip_all)]
    fn select_worker(&self, task: &Task) -> Result<Option<WorkerId>> {
        let memory_necessary_to_run_task = task.necessary_memory_in_bytes()?;

        let mut worker_with_the_minimum_amount_of_tasks = None;

        for (worker_id, worker_state) in self.workers.iter().filter(|(_worker_id, worker_state)| {
            worker_state.available_memory() >= memory_necessary_to_run_task
        }) {
            if worker_with_the_minimum_amount_of_tasks.is_none() {
                worker_with_the_minimum_amount_of_tasks = Some((worker_id, worker_state));
            } else if worker_state.tasks.len()
                < worker_with_the_minimum_amount_of_tasks
                    .unwrap()
                    .1
                    .tasks
                    .len()
            {
                worker_with_the_minimum_amount_of_tasks = Some((worker_id, worker_state));
            }
        }

        let worker_id =
            worker_with_the_minimum_amount_of_tasks.map(|(worker_id, _)| worker_id.clone());

        Ok(worker_id)
    }
}
