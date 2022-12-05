use std::{net::SocketAddr, path::Path, time::Duration};

use anyhow::Result;
use docker_api::{opts::PullOpts, Docker};
use etcd_rs::{
    Client, ClientConfig, ClusterOp, KeyRange, KeyValueOp, MemberAddRequest, PutRequest,
    WatchInbound, WatchOp,
};
use futures_util::stream::StreamExt;
use serde::Deserialize;
use tokio::select;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{definition::Definition, node::Node, worker::manager_proto::RegisterWorkerRequest};

pub type WorkerId = Uuid;

pub mod worker_proto {
    tonic::include_proto!("worker");
}
pub mod manager_proto {
    tonic::include_proto!("manager");
}

/// Runs as a daemon on every node. Responsible for handling tasks.
pub struct Worker {
    config: Config,
    /// The node that the worker is running on.
    node: Node,
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

impl Worker {
    #[tracing::instrument(name = "Worker::start", skip_all, fields(
        config = ?config
    ))]
    pub async fn start(config: Config) -> Result<()> {
        let etcd = Client::connect(ClientConfig::new([
            "http://127.0.0.1:2379".into(),
            "http://127.0.0.1:2380".into(),
        ]))
        .await?;

        info!("connected to etcd");

        let worker = Self {
            config,
            node: Node::new(),
            docker_client: Docker::new("unix:///var/run/docker.sock")?,
            etcd,
        };

        tokio::spawn(worker.watch_cluster_state_changes());

        Ok(())
    }

    #[tracing::instrument(name = "Worker::watch_cluster_state_changes", skip_all)]
    async fn watch_cluster_state_changes(self) {
        let (mut stream, _cancel) = self
            .etcd
            .watch(KeyRange::prefix(self.config.id.to_string()))
            .await
            .expect("watch by prefix");

        info!("watching changes prefixed by worker id");

        let mut interval = tokio::time::interval(Duration::from_secs(5));

        let manager_endpoint = format!("http://{}", self.config.manager.addr);

        let current_state_key = format!("node/{}/current_state", self.config.id);

        loop {
            select! {
                inbound = stream.inbound() => match inbound {
                    WatchInbound::Ready(resp) => {
                        println!("receive event: {:?}", resp);

                        if let Err(error) = self.etcd.put(PutRequest::new(current_state_key.clone(), "test")).await {
                          error!(?error, "unable to set current state");
                      }
                    }
                    WatchInbound::Interrupted(e) => {
                        eprintln!("encounter error: {:?}", e);
                        break;
                    }
                    WatchInbound::Closed => {
                        info!("watch stream closed");
                        break;
                    }
                },
                _ = interval.tick() => {
                    // TODO: do not recreate client
                    match manager_proto::manager_client::ManagerClient::connect(manager_endpoint.clone()).await {
                        Err(error) => {
                           warn!(?error, "unable to connect to manager") ;
                        }
                        Ok(mut client) => {
                            if let Err(error) = client.register_worker(RegisterWorkerRequest{ worker_id:self.config.id.to_string() }).await {
                                error!(?error, "unable to register worker");
                            }
                        }
                    };
                }
            }
        }
    }

    #[tracing::instrument(name = "Worker::run_task", skip_all, fields(
        definition = ?definition
    ))]
    pub async fn run_task(&mut self, definition: &Definition) -> Result<(), RunTaskError> {
        todo!()
    }

    #[tracing::instrument(name = "Worker::pull_image", skip_all, fields(
        image = %image
    ))]
    pub async fn pull_image(&self, image: &str) -> Result<(), RunTaskError> {
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

        info!("image pulled");

        Ok(())
    }
}
