pub mod manager_proto {
    tonic::include_proto!("manager");
}
use anyhow::Result;
use clap::{Parser, Subcommand};
use manager_proto::{ApplyReply, ApplyRequest, RegisterWorkerReply, RegisterWorkerRequest};
use simple_kubernetes::{
    definition::Definition,
    manager::{Config, Manager, RemoteWorker},
    simple_scheduler::SimpleScheduler,
};
use tokio::time::Instant;

use tonic::{transport::Server, Request, Response, Status};
use tracing::{error, info, Level};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Start the manager and configure it using a config file.
    Config {
        /// Path to the config file.
        #[arg(short)]
        file: String,
    },
}

pub struct ManagerService {
    manager: Manager,
}

impl ManagerService {
    fn new(manager: Manager) -> Self {
        Self { manager }
    }
}

impl TryFrom<RegisterWorkerRequest> for RemoteWorker {
    type Error = anyhow::Error;

    fn try_from(input: RegisterWorkerRequest) -> Result<RemoteWorker> {
        Ok(RemoteWorker {
            worker_id: Uuid::parse_str(&input.worker_id)?,
            heartbeat_received_at: Instant::now(),
        })
    }
}

#[tonic::async_trait]
impl manager_proto::manager_server::Manager for ManagerService {
    #[tracing::instrument(name = "ManagerService::register_worker", skip_all, fields(
        request = ?request
    ))]
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerReply>, Status> {
        async fn do_register_worker(
            request: Request<RegisterWorkerRequest>,
            manager: &Manager,
        ) -> Result<manager_proto::RegisterWorkerReply, Status> {
            let request = request.into_inner();

            let remote_worker = request
                .try_into()
                .map_err(|err: anyhow::Error| Status::invalid_argument(err.to_string()))?;

            manager.register_worker(remote_worker).await;

            Ok(manager_proto::RegisterWorkerReply {})
        }

        match do_register_worker(request, &self.manager).await {
            Err(error) => {
                error!(?error, "error registering worker");
                Err(error)
            }
            Ok(reply) => Ok(Response::new(reply)),
        }
    }

    #[tracing::instrument(name = "ManagerService::apply", skip_all, fields(
        request = ?request
    ))]
    async fn apply(&self, request: Request<ApplyRequest>) -> Result<Response<ApplyReply>, Status> {
        let request = request.into_inner();

        let definition: Definition = serde_yaml::from_str(&request.body)
            .map_err(|err| Status::failed_precondition(err.to_string()))?;

        self.manager
            .apply(definition)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        Ok(Response::new(manager_proto::ApplyReply {}))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Config { file } => {
            let addr = "[::1]:50051".parse().unwrap();

            info!(addr = ?addr, "starting manager server");

            let manager = Manager::new(
                Config::from_file(file).await?,
                Box::new(SimpleScheduler::new()),
            )
            .await?;
            let svc =
                manager_proto::manager_server::ManagerServer::new(ManagerService::new(manager));

            Server::builder().add_service(svc).serve(addr).await?;
        }
    }

    Ok(())
}
