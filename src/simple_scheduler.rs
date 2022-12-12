use anyhow::Result;

use chrono::Utc;
use etcd_rs::{Client, KeyValueOp, PutRequest};
use prost::Message;
use std::collections::HashMap;
use std::time::Duration;
use tokio::select;
use tracing::{error, info};

use crate::list_watcher::ListWatcher;

use crate::task_proto::{self, TaskSet, TaskSetName};

use crate::worker_proto::{self, WorkerId};

#[derive(Debug)]
pub struct Config {
    pub heartbeat_timeout_secs: Duration,
}

struct WorkerTasks {
    worker_id: Option<WorkerId>,
    taskset: task_proto::TaskSet,
}

pub struct SimpleScheduler {
    config: Config,
    /// Information about workers that are part of the cluster.
    workers: HashMap<WorkerId, worker_proto::CurrentState>,
    /// Tasks that are running in the cluster.
    tasks: HashMap<TaskSetName, WorkerTasks>,
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
        let (tasks_tx, mut tasks_rx) = tokio::sync::mpsc::channel::<task_proto::TaskSet>(100);
        let tasks_list_watcher = ListWatcher::new(
            // TODO: make configurable
            Duration::from_secs(15),
            tasks_tx,
            self.etcd.clone(),
        );

        let tasks_desired_state_key = "tasks/desired_state/".to_owned();

        info!(?tasks_desired_state_key, "spawning tasks list watcher");
        tokio::spawn(tasks_list_watcher.list_and_watch(tasks_desired_state_key));

        let (workers_tx, mut workers_rx) =
            tokio::sync::mpsc::channel::<worker_proto::CurrentState>(100);
        let workers_list_watcher = ListWatcher::new(
            // TODO: make configurable
            Duration::from_secs(15),
            workers_tx,
            self.etcd.clone(),
        );

        let workers_current_state_key = "workers/current_state".to_owned();

        info!(?workers_current_state_key, "spawning tasks list watcher");
        tokio::spawn(workers_list_watcher.list_and_watch(workers_current_state_key));

        loop {
            select! {
                message = tasks_rx.recv() => {
                    let taskset = match message {
                        None => {
                            error!("tasks list watcher channel closed unexpectedly");
                            return;
                        }
                        Some(v) => v
                    };

                    self.tasks.insert(taskset.name.clone(), WorkerTasks{ worker_id: None, taskset:taskset.clone() });
                    if let Err(error) = self.schedule(taskset).await {
                        error!(?error, "unable to schedule tasks");
                    }
                },
                message = workers_rx.recv() => {
                    let current_state = match message {
                        None => {
                            error!("workers list watcher channel closed unexpectedly");
                            return;
                        }
                        Some(v) => v
                    };

                    self.workers.insert(current_state.worker_id.clone(), current_state);

                    dbg!(&self.workers);
                }
            }
        }
    }

    #[tracing::instrument(name = "SimpleScheduler::schedule", skip_all)]
    async fn schedule(&mut self, taskset: TaskSet) -> Result<()> {
        self.remove_dead_workers();

        // TODO: this is wrong, if a worker is already running a task
        // its state should be modified.

        match self.select_worker(&taskset)? {
            None => Err(anyhow::anyhow!("no workers are able to execute the tasks")),
            Some(worker_id) => {
                let key = format!("workers/tasksets/{}/{}", worker_id, taskset.name);

                info!(?worker_id, taskset_name = ?taskset.name, "assigning taskset to worker");

                self.etcd
                    .put(PutRequest::new(key, taskset.encode_to_vec()))
                    .await?;

                Ok(())
            }
        }
    }

    #[tracing::instrument(name = "SimpleScheduler::remove_dead_workers", skip_all)]
    fn remove_dead_workers(&mut self) {
        let mut workers_to_remove = vec![];

        let now = Utc::now().timestamp_millis();
        for worker in self.workers.values() {
            if worker.timestamp - now > self.config.heartbeat_timeout_secs.as_millis() as i64 {
                workers_to_remove.push(worker.worker_id.clone());
            }
        }

        for worker_id in workers_to_remove {
            info!(?worker_id, "worker has not sent a heartbeat in a while");
            self.workers.remove(&worker_id);
        }
    }

    #[tracing::instrument(name = "SimpleScheduler::select_worker", skip_all)]
    fn select_worker(&self, taskset: &TaskSet) -> Result<Option<WorkerId>> {
        let memory_necessary_to_run_taskset = taskset.necessary_memory_in_bytes()?;

        let mut worker_with_the_minimum_amount_of_tasks = None;

        for (worker_id, worker_state) in self.workers.iter().filter(|(_worker_id, worker_state)| {
            worker_state.available_memory() >= memory_necessary_to_run_taskset
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
