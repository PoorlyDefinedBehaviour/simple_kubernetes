use anyhow::Result;
use docker_api::{
    opts::{ContainerCreateOpts, ContainerRemoveOpts, PublishPort, PullOpts},
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
    sync::Arc,
    time::Duration,
};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    definition::{Container, ContainerName, Spec},
    list_watcher::ListWatcher,
    node::Node,
    work_queue::WorkQueue,
};

use self::manager_proto::RegisterWorkerRequest;

pub type WorkerId = Uuid;

pub mod manager_proto {
    tonic::include_proto!("manager");
}

#[derive(Debug)]
enum ReconciliationAction {
    Remove {
        container_id: docker_api::Id,
    },
    Update {
        container_id: docker_api::Id,
        container: Container,
    },
    Create {
        container: Container,
    },
}

#[derive(Debug)]
struct ContainerEntry {
    container_definition: Container,
    container_id: docker_api::Id,
}

/// Runs as a daemon on every node. Responsible for creating/stopping/etc containers.
pub struct Worker {
    config: Config,
    /// The node that the worker is running on.
    node: Node,
    /// The current specification being used to run containers in this node.
    spec: Option<Spec>,
    /// The containers currently running in this node.
    state: HashMap<ContainerName, ContainerEntry>,
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
    loop {
        // TODO: do not recreate client
        match manager_proto::manager_client::ManagerClient::connect(manager_endpoint.clone()).await
        {
            Err(error) => {
                warn!(?error, "unable to connect to manager");
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

        tokio::time::sleep(heartbeat_interval).await;
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

        let worker = Self {
            config,
            node: Node::new(),
            spec: None,
            state: HashMap::new(),
            docker_client: Docker::new("unix:///var/run/docker.sock")?,
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

    #[tracing::instrument(name = "Worker::watch_cluster_state_changes", skip_all)]
    async fn watch_cluster_state_changes(mut self) {
        // The desired state for this node. Changes to the desired state are
        // appended to the work queue before being processed.
        let work_queue: Arc<WorkQueue<Spec>> = Arc::new(WorkQueue::new());
        let list_watcher = ListWatcher::new(
            // TODO: make configurable
            Duration::from_secs(30),
            Arc::clone(&work_queue),
            self.etcd.clone(),
        );

        let desired_state_key = format!("node/{}/desired_state", self.config.id);

        info!(?desired_state_key, "spawning list watcher");
        tokio::spawn(list_watcher.list_and_watch(desired_state_key));

        loop {
            let desired_state_definition = match work_queue.next().await {
                None => {
                    error!("unable to get work queue item");
                    return;
                }
                Some(v) => v,
            };

            let state_reconciliation_actions =
                self.state_reconciliation_actions(&desired_state_definition);

            for action in state_reconciliation_actions {
                match action {
                    ReconciliationAction::Remove { container_id } => {
                        if let Err(error) = self.remove_container(container_id.clone()).await {
                            error!(?error, ?container_id, "unable to remove container");
                        }
                    }
                    ReconciliationAction::Create { container } => {
                        if let Err(error) = self.create_container(container).await {
                            error!(?error, "unable to create container");
                        }
                    }
                    ReconciliationAction::Update {
                        container_id,
                        container,
                    } => {
                        if let Err(error) =
                            self.update_container(container_id.clone(), container).await
                        {
                            error!(?error, ?container_id, "unable to update container");
                        }
                    }
                }
            }

            // TODO: the current state is not the desired state if an error happens.
            self.spec = Some(desired_state_definition);
        }
    }

    fn state_reconciliation_actions(&self, desired_state: &Spec) -> Vec<ReconciliationAction> {
        let mut actions = Vec::new();
        let mut containers_seen = HashSet::new();

        for container in desired_state.containers.iter() {
            // TODO: cloning strings, can this be avoided?
            containers_seen.insert(container.name.clone());

            if self.state.contains_key(&container.name) {
                let entry = self.state.get(&container.name).unwrap();
                actions.push(ReconciliationAction::Update {
                    container_id: entry.container_id.clone(),
                    container: container.clone(),
                });
            } else {
                actions.push(ReconciliationAction::Create {
                    container: container.clone(),
                });
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
    async fn remove_container(&mut self, container_id: docker_api::Id) -> Result<()> {
        info!(?container_id, "removing container");
        let container = {
            docker_api::container::Container::new(self.docker_client.clone(), container_id.clone())
        };
        if let Err(error) = container
            .remove(&ContainerRemoveOpts::builder().force(true).build())
            .await
        {
            error!(?error, ?container_id, "unable to remove container");
        }

        self.state.remove(&container.inspect().await?.name.unwrap());

        Ok(())
    }

    #[tracing::instrument(name = "Worker::create_container", skip_all, fields(
        container = ?container
    ))]
    async fn create_container(&mut self, container: Container) -> Result<()> {
        self.pull_image(&container.image)
            .await
            .expect("error pulling image");

        let containers = self.docker_client.containers();
        let mut create_opts = ContainerCreateOpts::builder().image(&container.image);

        for port in container.ports.iter() {
            let port_and_protocol =
                format!("{}/{}", port.container_port, port.protocol.to_lowercase());

            let host_port = 8001;
            create_opts = create_opts.expose(PublishPort::from_str(&port_and_protocol)?, host_port);
        }

        match containers.create(&create_opts.build()).await {
            Err(error) => {
                error!(?error, "unable to create container");
            }
            Ok(created_container) => {
                info!(?created_container, "container created");
                match created_container.start().await {
                    Err(error) => {
                        error!(?error, "unable to start container");
                    }
                    Ok(_) => {
                        self.state.insert(
                            container.name.clone(),
                            ContainerEntry {
                                container_id: created_container.id().clone(),
                                container_definition: container,
                            },
                        );
                        info!("container started");
                    }
                }

                // TODO: remove after debug.
                tokio::time::sleep(Duration::from_secs(600)).await;
            }
        }

        Ok(())
    }

    #[tracing::instrument(name = "Worker::update_container", skip_all, fields(
        container_id = %container_id,
        container = ?container
    ))]
    async fn update_container(
        &self,
        container_id: docker_api::Id,
        container: Container,
    ) -> Result<()> {
        todo!()
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
    async fn get_worker_current_state(&self, worker_id: WorkerId) -> Result<Spec> {
        let key = format!("node/{}/current_state", worker_id);
        info!(?key, "getting the current node state");
        let range_response = self.etcd.get(KeyRange::key(key)).await?;
        let spec = serde_json::from_slice(&range_response.kvs[0].value)?;
        Ok(spec)
    }
}
