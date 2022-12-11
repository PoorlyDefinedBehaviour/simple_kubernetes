use anyhow::{Context, Result};
use docker_api::{
    opts::{ContainerCreateOpts, ContainerListOpts, ContainerRemoveOpts, PublishPort, PullOpts},
    Docker,
};
use etcd_rs::{Client, ClientConfig, KeyRange, KeyValueOp};
use futures_util::stream::StreamExt;
use prost::Message;
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::Path,
    str::FromStr,
    time::Duration,
};
use tokio::select;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    definition::ContainerName,
    list_watcher::ListWatcher,
    manager_proto::{self, RegisterWorkerRequest},
    node::Node,
    task_proto::{PortBinding, State, Task, Tasks},
};

pub type WorkerId = Uuid;

#[derive(Debug)]
enum ReconciliationAction {
    Remove { container_id: String },
    Recreate { container_id: String, task: Task },
    Create { task: Task },
}

/// Runs as a daemon on every node. Responsible for creating/stopping/etc containers.
pub struct Worker {
    config: Config,
    /// The node that the worker is running on.
    node: Node,
    /// The containers currently running in this node.
    state: HashMap<ContainerName, Task>,
    /// Client used to communicate with the docker daemon.
    docker_client: Docker,
    /// Client used to communicate with etcd.
    etcd: Client,
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

#[tracing::instrument(name = "worker::heartbeat", skip_all, fields(
    worker_id = %worker_id,
    manager_endpoint = %manager_endpoint
))]
async fn heartbeat(worker_id: WorkerId, manager_endpoint: String, heartbeat_interval: Duration) {
    let mut ticker = tokio::time::interval(heartbeat_interval);

    loop {
        ticker.tick().await;
        // TODO: do not recreate client
        match manager_proto::manager_client::ManagerClient::connect(manager_endpoint.clone()).await
        {
            Err(error) => {
                warn!(?error, "unable to connect to manager, will retry later");
            }
            Ok(mut client) => {
                if let Err(error) = client
                    .register_worker(RegisterWorkerRequest {
                        worker_id: worker_id.to_string(),
                    })
                    .await
                {
                    error!(?error, "unable to register worker");
                }
            }
        };
    }
}

impl Worker {
    #[tracing::instrument(name = "Worker::start", skip_all, fields(
        config = ?config
    ))]
    pub async fn start(config: Config) -> Result<()> {
        info!("starting worker control loop");

        let etcd_endpoints: Vec<_> = config
            .etcd
            .endpoints
            .iter()
            .map(|endpoint| endpoint.into())
            .collect();

        let etcd = Client::connect(ClientConfig::new(etcd_endpoints)).await?;

        info!("connected to etcd");

        let docker_client = Docker::new("unix:///var/run/docker.sock")?;
        let worker = Self {
            config,
            node: Node::new(),
            state: HashMap::new(),
            docker_client,
            etcd,
        };

        info!("spawning heartbeat control loop");
        tokio::spawn(heartbeat(
            worker.config.id,
            format!("http://{}", worker.config.manager.addr),
            Duration::from_secs(worker.config.heartbeat.interval),
        ));

        info!("spawning worker control loop");
        tokio::spawn(worker.watch_cluster_state_changes());

        Ok(())
    }

    #[tracing::instrument(name = "worker::build_state_from_existing_containers", skip_all)]
    async fn build_state_from_existing_containers(&mut self) -> Result<()> {
        info!("building state from existing containers");

        // State is unknown.
        self.state.clear();

        let containers = self.docker_client.containers();
        for container_summary in containers
            .list(&ContainerListOpts::builder().all(true).build())
            .await?
        {
            let task = Task::try_from(container_summary)
                .expect("summary conversion to task should never fail");

            self.state.insert(task.name.clone(), task);
        }

        Ok(())
    }

    #[tracing::instrument(name = "Worker::watch_cluster_state_changes", skip_all)]
    async fn watch_cluster_state_changes(mut self) {
        // The desired state for this node. Changes to the desired state are
        // appended to the work queue before being processed.
        let (work_tx, mut work_rx) = tokio::sync::mpsc::channel::<Tasks>(100);
        let list_watcher = ListWatcher::new(
            // TODO: make configurable
            Duration::from_secs(15),
            work_tx,
            self.etcd.clone(),
        );

        let mut build_state_from_existing_containers_interval =
            tokio::time::interval(Duration::from_secs(10));

        let desired_state_key = format!("node/{}/desired_state", self.config.id);

        info!(?desired_state_key, "spawning list watcher");
        tokio::spawn(list_watcher.list_and_watch(desired_state_key));

        loop {
            select! {
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
                        self.state_reconciliation_actions(&desired_state_definition);

                    for action in state_reconciliation_actions {
                        match action {
                            ReconciliationAction::Remove { container_id } => {
                                if let Err(error) = self.remove_container(&container_id).await {
                                    error!(?error, ?container_id, "unable to remove container");
                                }
                            }
                            ReconciliationAction::Create { task } => {
                                if let Err(error) = self.create_container(task).await {
                                    error!(?error, "unable to create container");
                                }
                            }
                            ReconciliationAction::Recreate { container_id, task } => {
                                if let Err(error) = self.recreate_container(&container_id, task).await {
                                    error!(?error, ?container_id, "unable to recreate container");
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn state_reconciliation_actions(&self, desired_state: &Tasks) -> Vec<ReconciliationAction> {
        let mut actions = Vec::new();
        let mut containers_seen = HashSet::new();

        for task in desired_state.tasks.iter() {
            // TODO: cloning strings, can this be avoided?
            containers_seen.insert(task.name.clone());

            match self.state.get(&task.name) {
                None => {
                    actions.push(ReconciliationAction::Create { task: task.clone() });
                }
                Some(entry) => {
                    if task_desired_state_has_changed(&task, entry) {
                        actions.push(ReconciliationAction::Recreate {
                            container_id: entry.container_id.clone(),
                            task: task.clone(),
                        });
                    }
                }
            }
        }

        // Remove containers that are currently running but are not in the
        // new specification.
        for (container_name, container_entry) in self.state.iter() {
            if !containers_seen.contains(container_name) {
                actions.push(ReconciliationAction::Remove {
                    container_id: container_entry.container_id.clone(),
                });
            }
        }

        actions
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

        self.state.remove(container_name);

        Ok(())
    }

    #[tracing::instrument(name = "Worker::create_container", skip_all, fields(
        task = ?task
    ))]
    async fn create_container(&mut self, mut task: Task) -> Result<()> {
        self.pull_image(&task.image)
            .await
            .context("pulling image")?;

        let containers = self.docker_client.containers();
        let mut create_opts = ContainerCreateOpts::builder()
            .image(&task.image)
            .name(&task.name);

        for port_binding in task.port_bindings.iter() {
            let port_and_protocol = format!(
                "{}/{}",
                port_binding.port,
                port_binding.protocol.to_lowercase()
            );

            let host_port = port_binding.port;
            create_opts = create_opts.expose(PublishPort::from_str(&port_and_protocol)?, host_port);
        }

        if let Err(_) = self.remove_container(&task.name).await { /* no-op */ }

        let created_container = containers
            .create(&create_opts.build())
            .await
            .context("creating container")?;

        info!(?created_container, "container created");

        created_container
            .start()
            .await
            .context("starting container")?;

        task.container_id = created_container.id().to_string();
        task.state = State::Running.as_i32();
        self.state.insert(task.name.clone(), task);
        info!("container started");

        Ok(())
    }

    #[tracing::instrument(name = "Worker::recreate_container", skip_all, fields(
        container_id = %container_id,
        task = ?task
    ))]
    async fn recreate_container(&mut self, container_id: &str, task: Task) -> Result<()> {
        info!(?container_id, name = %task.name, "recreating container");
        self.remove_container(container_id).await?;
        self.create_container(task).await?;
        Ok(())
    }

    #[tracing::instrument(name = "Worker::pull_image", skip_all, fields(
        image = %image
    ))]
    pub async fn pull_image(&self, image: &str) -> Result<(), RunTaskError> {
        info!("pulling image");

        let pull_opts = PullOpts::builder()
            .image("poorlydefinedbehaviour/kubia")
            .build();

        let images = self.docker_client.images();

        let mut stream = images.pull(&pull_opts);
        while let Some(result) = stream.next().await {
            if let Err(err) = result {
                return Err(RunTaskError::PullError(err.to_string()));
            }
        }

        Ok(())
    }

    /// Returns the containers currently executing in a worker.
    #[tracing::instrument(name = "manager::get_worker_current_state", skip_all, fields(
            worker_id = %worker_id
        ))]
    async fn get_worker_current_state(&self, worker_id: WorkerId) -> Result<Tasks> {
        let key = format!("node/{}/current_state", worker_id);
        info!(?key, "getting the current node state");
        let range_response = self.etcd.get(KeyRange::key(key)).await?;
        let tasks = Tasks::decode(range_response.kvs[0].value.as_ref())?;
        Ok(tasks)
    }
}

impl TryFrom<docker_api::models::ContainerSummary> for Task {
    type Error = anyhow::Error;

    fn try_from(input: docker_api::models::ContainerSummary) -> Result<Self, Self::Error> {
        let container_name = input.names.expect("every container must have a name")[0].clone();

        Ok(Task {
            id: Uuid::new_v4().to_string(),
            container_id: input.id.unwrap_or_default(),
            state: State::try_from(input.state.unwrap_or_default().as_ref())?.into(),
            image: input.image.expect("image should exist"),
            name: format_container_name(&container_name).to_owned(),
            port_bindings: input
                .ports
                .unwrap_or_default()
                .into_iter()
                .map(|port| PortBinding {
                    port: port.public_port.unwrap() as u32,
                    protocol: port.type_,
                })
                .collect(),
        })
    }
}

#[tracing::instrument(name = "worker::format_container_name", skip_all, fields(
    name = %name
))]
pub fn format_container_name(name: &str) -> &str {
    name.strip_prefix("/").unwrap_or_default()
}

#[tracing::instrument(name = "worker::task_desired_state_has_changed", skip_all, fields(
    desired_state = ?desired_state,
    current_state = ?current_state,
    changed
))]
fn task_desired_state_has_changed(desired_state: &Task, current_state: &Task) -> bool {
    let changed = (!current_state.is_running() && !current_state.is_completed())
        || desired_state.name != current_state.name
        || desired_state.image != current_state.image;

    tracing::Span::current().record("changed", changed);

    info!("task desired state changed");

    changed
}
