use anyhow::Result;
use docker_api::{
    opts::{ContainerCreateOpts, PublishPort, PullOpts},
    Docker,
};
use etcd_rs::{Client, ClientConfig};
use futures_util::stream::StreamExt;
use serde::Deserialize;
use std::{net::SocketAddr, path::Path, str::FromStr, sync::Arc, time::Duration};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{definition::Definition, list_watcher::ListWatcher, node::Node, work_queue::WorkQueue};

use self::manager_proto::RegisterWorkerRequest;

pub type WorkerId = Uuid;

pub mod manager_proto {
    tonic::include_proto!("manager");
}

/// Runs as a daemon on every node. Responsible for handling tasks.
pub struct Worker {
    config: Config,
    /// The node that the worker is running on.
    node: Node,
    /// The current definition being used to run containers in this node.
    current_state: Option<Definition>,
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
            current_state: None,
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
        let work_queue: Arc<WorkQueue<Definition>> = Arc::new(WorkQueue::new());
        let list_watcher = ListWatcher::new(
            // TODO: make configurable
            Duration::from_secs(30),
            Arc::clone(&work_queue),
            self.etcd.clone(),
        );

        let current_state_key = format!("node/{}/current_state", self.config.id);

        info!(?current_state_key, "spawning list watcher");
        tokio::spawn(list_watcher.list_and_watch(current_state_key));

        info!("watching changes prefixed by worker id");

        loop {
            let desired_state_definition = match work_queue.next().await {
                None => {
                    error!("unable to get work queue item");
                    return;
                }
                Some(v) => v,
            };

            match &mut self.current_state {
                None => {
                    for container in desired_state_definition.spec.containers.iter() {
                        self.pull_image(&container.image)
                            .await
                            .expect("error pulling image");

                        let containers = self.docker_client.containers();
                        let mut create_opts =
                            ContainerCreateOpts::builder().image(&container.image);

                        for port in container.ports.iter() {
                            let port_and_protocol =
                                format!("{}/{}", port.container_port, port.protocol.to_lowercase());

                            let host_port = 8001;
                            create_opts = create_opts.expose(
                                PublishPort::from_str(&port_and_protocol).unwrap(),
                                host_port,
                            );
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
                                        info!("container started");
                                    }
                                }
                                tokio::time::sleep(Duration::from_secs(600)).await;
                            }
                        }
                    }

                    // TODO: the current state is not the desired statei if an error happens.
                    self.current_state = Some(desired_state_definition);
                }
                Some(current_state_definition) => {}
            }

            // TODO: if desired_state != current_state { modify_current_state() }
        }
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
}
