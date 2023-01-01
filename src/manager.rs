use crate::constants::TASK_LIST_KEY;
use crate::constants::WORKER_LIST_KEY;
use crate::definition::Definition;
use crate::manager_proto::manager_proto;
use crate::resources_proto::Task;
use anyhow::Result;
use etcd_rs::{Client, KeyValueOp, PutRequest};
use prost::Message;

pub struct Manager {
    etcd: Client,
}

impl Manager {
    #[tracing::instrument(name = "Manager::new", skip_all)]
    pub fn new(etcd: Client) -> Self {
        Self { etcd }
    }

    #[tracing::instrument(name = "Manager::apply", skip_all, fields(
      definition = ?definition
    ))]
    pub async fn apply(&self, definition: Definition) -> Result<()> {
        let task = Task::from(definition);

        self.store_tasks_desired_state(task).await?;

        Ok(())
    }

    #[tracing::instrument(name = "Manager::node_heartbeat", skip_all, fields(
        request = ?request
      ))]
    pub async fn node_heartbeat(&self, request: manager_proto::NodeHeartbeatRequest) -> Result<()> {
        let key = format!("{WORKER_LIST_KEY}/{}", request.worker_id);

        tracing::Span::current().record("key", &key);

        let buffer = request.encode_to_vec();

        self.etcd.put(PutRequest::new(key, buffer)).await?;

        Ok(())
    }

    /// Stores the tasks in etcd. The scheduler will pick the changes and schedule the tasks.
    #[tracing::instrument(name = "manager::store_tasks_desired_state", skip_all, fields(
        task = ?task
    ))]
    async fn store_tasks_desired_state(&self, task: Task) -> Result<()> {
        let key = format!("{TASK_LIST_KEY}/{}", task.name);

        tracing::Span::current().record("key", &key);

        let buffer = task.encode_to_vec();

        self.etcd.put(PutRequest::new(key, buffer)).await?;

        Ok(())
    }
}
