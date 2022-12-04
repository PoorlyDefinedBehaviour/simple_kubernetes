use anyhow::Result;
use docker_api::{opts::PullOpts, Docker};
use futures_util::stream::StreamExt;
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

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum RunTaskError {
    #[error("error pulling image: {0:?}")]
    PullError(String),
}

impl Worker {
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
