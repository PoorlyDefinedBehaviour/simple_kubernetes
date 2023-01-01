use anyhow::{Context, Result};
use chrono::Utc;
use docker_api::{
    opts::{ContainerCreateOpts, ContainerListOpts, ContainerRemoveOpts, PublishPort, PullOpts},
    Docker,
};
use etcd_rs::{Client, ClientConfig, KeyRange, KeyValueOp};
use futures_util::stream::StreamExt;

use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::Path,
    str::FromStr,
    time::Duration,
};
use tokio::select;
use tonic::transport::Channel;
use tracing::{error, info, warn};

use crate::{
    constants::WORKER_TASK_LIST_KEY,
    definition::ContainerName,
    list_watcher::ListWatcher,
    manager_proto,
    node::Node,
    resources_proto::{self, State, Task},
};

pub type WorkerId = String;

#[derive(Debug)]
enum ReconciliationAction {
    Remove {
        container_id: String,
    },
    Create {
        container: resources_proto::Container,
    },
}

/// Runs as a daemon on every node. Responsible for creating/stopping/etc containers.
pub struct Worker {
    config: Config,
    /// The node that the worker is running on.
    node: Node,
    /// The tasks currently running in this node.
    tasks: HashMap<ContainerName, resources_proto::Task>,
    /// Client used to communicate with the docker daemon.
    docker_client: Docker,
    /// Client used to communicate with etcd.
    etcd: Client,
    /// Client used to communicate with the cluster manager.
    manager_client: manager_proto::manager_client::ManagerClient<Channel>,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub id: WorkerId,
    pub manager: ManagerConfig,
    pub heartbeat: HeartbeatConfig,
    pub etcd: EtcdConfig,
}

#[derive(Debug, Deserialize)]
pub struct ManagerConfig {
    pub addr: SocketAddr,
}

#[derive(Debug, Deserialize)]
pub struct HeartbeatConfig {
    pub interval: u64,
}

#[derive(Debug, Deserialize)]
pub struct EtcdConfig {
    pub endpoints: Vec<String>,
}

impl Config {
    #[tracing::instrument(name = "worker::Config::from_file", skip_all, fields(
        file_path = ?file_path.as_ref()
    ))]
    pub async fn from_file(file_path: impl AsRef<Path>) -> Result<Self> {
        let file_contents = tokio::fs::read_to_string(file_path.as_ref()).await?;

        let config: Config = serde_yaml::from_str(&file_contents)?;

        Ok(config)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RunTaskError {
    #[error("error pulling image: {0:?}")]
    PullError(String),
}

impl Worker {
    #[tracing::instrument(name = "Worker::start", skip_all, fields(
        config = ?config
    ))]
    pub async fn start(config: Config) -> Result<()> {
        info!("starting worker control loop");

        let manager_client = manager_proto::manager_client::ManagerClient::connect(format!(
            "http://{}",
            config.manager.addr
        ))
        .await?;

        let etcd_endpoints: Vec<_> = config
            .etcd
            .endpoints
            .iter()
            .map(|endpoint| endpoint.into())
            .collect();

        let etcd = Client::connect(ClientConfig::new(etcd_endpoints)).await?;

        info!("connected to etcd");

        let docker_client = Docker::new("unix:///var/run/docker.sock")?;
        let mut worker = Self {
            config,
            node: Node::new(),
            tasks: HashMap::new(),
            docker_client,
            etcd,
            manager_client,
        };

        worker.build_state_from_existing_containers().await?;

        info!("spawning worker control loop");
        tokio::spawn(worker.watch_cluster_state_changes());

        Ok(())
    }

    #[tracing::instrument(name = "Worker::get_containers_running_in_node", skip_all)]
    async fn get_containers_running_in_node(
        &self,
    ) -> Result<Vec<docker_api::models::ContainerSummary>> {
        // List containers running in the mchine.
        let containers = self.docker_client.containers();

        let summaries = containers
            .list(&ContainerListOpts::builder().all(true).build())
            .await?;

        Ok(summaries)
    }

    #[tracing::instrument(name = "Worker::build_state_from_existing_containers", skip_all)]
    async fn build_state_from_existing_containers(&mut self) -> Result<()> {
        info!("building state from existing containers");

        let tasks = self.get_tasks_beloging_to_worker().await?;

        // Reset the local state.
        self.tasks.clear();

        let summaries: HashMap<_, _> = self
            .get_containers_running_in_node()
            .await?
            .into_iter()
            .map(|summary| (container_name(&summary).to_owned(), summary))
            .collect();

        for mut task in tasks.into_iter() {
            for container in task.containers.iter_mut() {
                if container.status.is_none() {
                    container.status = Some(resources_proto::Status {
                        state: resources_proto::State::Pending.as_i32(),
                        container_id: String::new(),
                    });
                }

                match summaries.get(&container.name) {
                    // Container does not exist in the node at the moment.
                    None => {
                        container.status.as_mut().unwrap().state = State::Pending.as_i32();
                    }
                    // Container exists in the node.
                    // Is the actual container state in sync with the stored state?
                    Some(summary) => {
                        let state = State::try_from(summary)?;
                        let status = container.status.as_mut().unwrap();
                        status.state = state.as_i32();
                        status.container_id = summary.id.clone().unwrap_or_default();
                    }
                };
            }

            self.tasks.insert(task.name.clone(), task);
        }

        Ok(())
    }

    #[tracing::instrument(name = "Worker::send_heartbeat", skip_all)]
    async fn send_heartbeat(&mut self) -> Result<()> {
        self.manager_client
            .node_heartbeat(manager_proto::NodeHeartbeatRequest {
                worker_id: self.config.id.to_string(),
                max_memory: self.node.max_memory(),
                memory_allocated: self.node.memory_allocated(),
                max_disk_size: self.node.max_disk_size(),
                disk_allocated: self.node.disk_allocated(),
                timestamp: Utc::now().timestamp_millis(),
                tasks: self
                    .tasks
                    .values()
                    .cloned()
                    .map(manager_proto::Task::from)
                    .collect(),
            })
            .await
            .context("sending node heartbeat")?;
        Ok(())
    }

    #[tracing::instrument(name = "Worker::watch_cluster_state_changes", skip_all)]
    async fn watch_cluster_state_changes(mut self) {
        // The desired state for this node. Changes to the desired state are
        // appended to the work queue before being processed.
        let (work_tx, mut work_rx) = tokio::sync::mpsc::channel::<Task>(100);
        let list_watcher = ListWatcher::new(
            // TODO: make configurable
            Duration::from_secs(15),
            work_tx,
            self.etcd.clone(),
        );

        let mut heartbeat_interval =
            tokio::time::interval(Duration::from_secs(self.config.heartbeat.interval));

        let mut build_state_from_existing_containers_interval =
            tokio::time::interval(Duration::from_secs(10));

        let desired_state_key = format!("{WORKER_TASK_LIST_KEY}/{}", self.config.id);

        info!(?desired_state_key, "spawning list watcher");
        tokio::spawn(list_watcher.list_and_watch(desired_state_key));

        loop {
            select! {
                _ = heartbeat_interval.tick() => {
                    if let Err(error) = self.send_heartbeat().await {
                        error!(?error, "unable to send heartbeat");
                    }
                },
                _ = build_state_from_existing_containers_interval.tick() => {
                    if let Err(error) = self.build_state_from_existing_containers().await {
                        error!(?error, "unable to build state from existing containers");
                    }
                },
                work_queue_item = work_rx.recv() => {
                    let desired_state_definition = match work_queue_item {
                        None => {
                            error!("unable to get work queue item because channel is closed");
                            return;
                        }
                        Some(v) => v,
                    };

                    let state_reconciliation_actions =
                        match self.state_reconciliation_actions(&desired_state_definition).await {
                            Err(error) => {
                                error!(?error, "unable to generate state reconciliation actions");
                                continue;
                            }
                            Ok(v) => v
                        };

                    for action in state_reconciliation_actions {
                        match action {
                            ReconciliationAction::Remove { container_id } => {
                                if let Err(error) = self.remove_container(&container_id).await {
                                    error!(?error, ?container_id, "unable to remove container");
                                }
                            }
                            ReconciliationAction::Create { container } => {
                                if let Err(error) = self.create_container(container).await {
                                    error!(?error, "unable to create container");
                                }
                            }

                        }
                    }
                }
            }
        }
    }

    async fn state_reconciliation_actions(
        &self,
        new_task: &Task,
    ) -> Result<Vec<ReconciliationAction>> {
        let mut actions = Vec::new();
        let mut containers_seen = HashSet::new();

        match self.tasks.get(&new_task.name) {
            // Task is new and so are its containers.
            None => {
                for container in new_task.containers.iter() {
                    // TODO: cloning strings, can this be avoided?
                    containers_seen.insert(container.name.clone());
                    actions.push(ReconciliationAction::Create {
                        container: container.clone(),
                    });
                }
            }
            Some(old_task) => {
                let mut new_containers: HashMap<_, _> = new_task
                    .containers
                    .iter()
                    .map(|container| (&container.name, container))
                    .collect();

                for old_container in old_task.containers.iter() {
                    match new_containers.remove(&old_container.name) {
                        None => {
                            actions.push(ReconciliationAction::Remove {
                                container_id: old_container.id().unwrap().to_owned(),
                            });
                        }
                        Some(new_container) => {
                            if container_desired_state_has_changed(
                                &new_task,
                                &new_container,
                                &old_task,
                                old_container,
                            ) {
                                actions.push(ReconciliationAction::Create {
                                    container: new_container.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }

        Ok(actions)
    }

    #[tracing::instrument(name = "Worker::remove_container", skip_all, fields(
        container_id = %container_id
    ))]
    async fn remove_container(&mut self, container_id: &str) -> Result<()> {
        let container =
            docker_api::container::Container::new(self.docker_client.clone(), container_id);

        let container_name = container.inspect().await?.name.unwrap();
        let container_name = format_container_name(&container_name);

        info!(?container_id, %container_name, "removing container");

        container
            .remove(
                &ContainerRemoveOpts::builder()
                    .force(true)
                    .volumes(true)
                    .build(),
            )
            .await?;

        self.tasks.remove(container_name);

        Ok(())
    }

    #[tracing::instrument(name = "Worker::create_container", skip_all, fields(
        container = ?container
    ))]
    async fn create_container(&mut self, container: resources_proto::Container) -> Result<()> {
        self.pull_image(&container.image)
            .await
            .context("pulling image")?;

        let containers = self.docker_client.containers();
        let mut create_opts = ContainerCreateOpts::builder()
            .image(&container.image)
            .name(&container.name);

        for port_binding in container.port_bindings.iter() {
            let port_and_protocol = format!(
                "{}/{}",
                port_binding.port,
                port_binding.protocol.to_lowercase()
            );

            let host_port = port_binding.port;
            create_opts = create_opts.expose(PublishPort::from_str(&port_and_protocol)?, host_port);
        }

        if let Err(_) = self.remove_container(&container.name).await { /* no-op */ }

        let created_container = containers
            .create(&create_opts.build())
            .await
            .context("creating container")?;

        info!(?created_container, "container created");

        created_container
            .start()
            .await
            .context("starting container")?;

        info!("container started");

        if let Err(error) = self.build_state_from_existing_containers().await {
            error!(
                ?error,
                "unable to build state from existing containers after creating new container"
            );
        }

        Ok(())
    }

    #[tracing::instrument(name = "Worker::pull_image", skip_all, fields(
        image = %image
    ))]
    pub async fn pull_image(&self, image: &str) -> Result<(), RunTaskError> {
        info!("pulling image");

        let pull_opts = PullOpts::builder().image(image).build();

        let images = self.docker_client.images();

        let mut stream = images.pull(&pull_opts);
        while let Some(result) = stream.next().await {
            if let Err(err) = result {
                return Err(RunTaskError::PullError(err.to_string()));
            }
        }

        Ok(())
    }

    /// Returns the tasks assigned to this node by the scheduler.
    #[tracing::instrument(name = "Worker::get_tasks_beloging_to_worker", skip_all)]
    async fn get_tasks_beloging_to_worker(&mut self) -> Result<Vec<resources_proto::Task>> {
        let key = format!("{WORKER_TASK_LIST_KEY}/{}", self.config.id);

        info!(?key, "fetching worker state");

        let range_response = self.etcd.get(KeyRange::prefix(key)).await?;

        let mut tasks = Vec::with_capacity(range_response.kvs.len());

        for kv in range_response.kvs {
            let task = resources_proto::Task::try_from(kv)?;
            tasks.push(task);
        }

        Ok(tasks)
    }
}

#[tracing::instrument(name = "worker::format_container_name", skip_all, fields(
    name = %name
))]
pub fn format_container_name(name: &str) -> &str {
    name.strip_prefix("/").unwrap_or_default()
}

fn container_name(summary: &docker_api::models::ContainerSummary) -> String {
    let container_name = &summary
        .names
        .as_ref()
        .expect("every container must have a name")[0]
        .clone();
    format_container_name(&container_name).to_owned()
}

#[tracing::instrument(name = "worker::task_desired_state_has_changed", skip_all, fields(
    new_container = ?new_container,
    old_container = ?old_container,
    changed
))]
fn container_desired_state_has_changed(
    new_task: &Task,
    new_container: &resources_proto::Container,
    old_task: &Task,
    old_container: &resources_proto::Container,
) -> bool {
    let changed = (old_container.is_completed() && new_task.timestamp > old_task.timestamp)
        || (!old_container.is_running() && !old_container.is_completed())
        || new_container.name != old_container.name
        || new_container.image != old_container.image
        || new_container.port_bindings != old_container.port_bindings
        || new_container.resources != old_container.resources;

    tracing::Span::current().record("changed", changed);

    if changed {
        info!("task desired state changed");
    }

    changed
}
