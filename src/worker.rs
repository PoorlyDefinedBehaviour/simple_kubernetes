use std::{net::SocketAddr, path::Path};

use anyhow::Result;
use docker_api::{opts::PullOpts, Docker};
use futures_util::stream::StreamExt;
use serde::Deserialize;
use uuid::Uuid;

use crate::{definition::Definition, node::Node};

pub type WorkerId = Uuid;

/// Runs as a daemon on every node. Responsible for handling tasks.
#[derive(Debug)]
pub struct Worker {
    id: WorkerId,
    /// The node that the worker is running on.
    node: Node,
    /// Client used to communicate with the docker daemon.
    docker_client: Docker,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub id: WorkerId,
    pub manager: ManagerConfig,
    pub heartbeat: HeartbeatConfig,
}

#[derive(Debug, Deserialize)]
pub struct ManagerConfig {
    pub addr: SocketAddr,
}

#[derive(Debug, Deserialize)]
pub struct HeartbeatConfig {
    pub interval: u64,
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
    #[tracing::instrument(name = "Worker::new", skip_all, fields(
        config = ?config
    ))]
    pub fn new(config: Config) -> Result<Self> {
        Ok(Self {
            id: config.id,
            node: Node::new(),
            docker_client: Docker::new("unix:///var/run/docker.sock")?,
        })
    }
    pub fn collect_stats(&mut self) {
        todo!()
    }

    #[tracing::instrument(name = "Worker::run_task", skip_all, fields(
      definition = ?definition
    ))]
    pub async fn run_task(&mut self, definition: &Definition) -> Result<(), RunTaskError> {
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

    pub fn start_task(&mut self) {
        todo!()
    }

    pub fn stop_task(&mut self) {
        todo!()
    }
}
