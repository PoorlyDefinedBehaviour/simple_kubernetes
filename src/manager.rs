use anyhow::Result;
use etcd_rs::{Client, KeyValueOp, PutRequest};
use prost::Message;
use serde::Deserialize;

use std::path::Path;
use std::time::Duration;

use tracing::info;

use crate::definition::Definition;
use crate::simple_scheduler;
use crate::task_proto::TaskSet;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub worker: WorkerConfig,
}

#[derive(Debug, Deserialize)]
pub struct WorkerConfig {
    pub heartbeat_timeout_secs: u64,
}

impl From<&Config> for simple_scheduler::Config {
    fn from(input: &Config) -> Self {
        Self {
            heartbeat_timeout_secs: Duration::from_secs(input.worker.heartbeat_timeout_secs),
        }
    }
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

pub struct Manager {
    config: Config,
    etcd: Client,
}

impl Manager {
    #[tracing::instrument(name = "Manager::new", skip_all)]
    pub fn new(config: Config, etcd: Client) -> Self {
        Self { config, etcd }
    }

    #[tracing::instrument(name = "Manager::apply", skip_all, fields(
      definition = ?definition
    ))]
    pub async fn apply(&self, definition: Definition) -> Result<()> {
        let taskset = TaskSet::from(definition);

        self.store_tasks_desired_state(taskset).await?;

        Ok(())
    }

    /// Stores the tasks in etcd. The scheduler will pick the changes and schedule the tasks.
    #[tracing::instrument(name = "manager::store_tasks_desired_state", skip_all, fields(
        taskset = ?taskset
    ))]
    async fn store_tasks_desired_state(&self, taskset: TaskSet) -> Result<()> {
        let key = format!("tasks/desired_state/{}", taskset.name);

        info!(?key, "setting tasks desired state");

        let buffer = taskset.encode_to_vec();

        self.etcd.put(PutRequest::new(key, buffer)).await?;

        Ok(())
    }
}
