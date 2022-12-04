mod worker_proto {
    tonic::include_proto!("worker");
}
mod manager_proto {
    tonic::include_proto!("manager");
}

use std::{net::SocketAddr, time::Duration};

use clap::{Parser, Subcommand};
use manager_proto::RegisterWorkerRequest;
use simple_kubernetes::worker::{self, Worker, WorkerId};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{error, info, log::LevelFilter, warn, Level};
use tracing_subscriber::EnvFilter;
use worker_proto::{RunTaskReply, RunTaskRequest};

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Start a worker and configure it using a config file.
    Config {
        /// Path to the config file.
        #[arg(short)]
        file: String,
    },
}

pub struct WorkerService {
    worker: Worker,
}

impl WorkerService {
    fn new(worker: Worker) -> Self {
        Self { worker }
    }
}

#[tonic::async_trait]
impl worker_proto::worker_server::Worker for WorkerService {
    #[tracing::instrument(name = "WorkerService::run_task", skip_all, fields(
        request = ?request
    ))]
    async fn run_task(
        &self,
        request: Request<RunTaskRequest>,
    ) -> Result<Response<RunTaskReply>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

        let reply = worker_proto::RunTaskReply {
            message: format!("Hello {}!", request.into_inner().name),
        };
        Ok(Response::new(reply))
    }
}

#[tracing::instrument(name = "worker::heartbeat", skip_all, fields(
    manager_endpoint = %manager_endpoint
))]
async fn heartbeat(
    heartbeat_interval: Duration,
    worker_id: WorkerId,
    worker_addr: SocketAddr,
    manager_endpoint: String,
) {
    loop {
        info!("sending heartbeat");

        // TODO: do not recreate client every time
        match manager_proto::manager_client::ManagerClient::connect(format!(
            "http://{manager_endpoint}",
        ))
        .await
        {
            Err(error) => {
                warn!(?error, "unable to connect to manager");
            }
            Ok(mut client) => match client
                .register_worker(RegisterWorkerRequest {
                    worker_id: worker_id.to_string(),
                    worker_addr: worker_addr.to_string(),
                })
                .await
            {
                Err(error) => {
                    error!(?error, "error registering worker");
                }
                Ok(response) => {
                    info!(?response, "worker registered");
                }
            },
        }

        tokio::time::sleep(heartbeat_interval).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Config { file } => {
            let addr = "[::1]:50052".parse().unwrap();

            info!(addr = ?addr, "starting worker server");

            let worker_config = worker::Config::from_file(file).await?;
            let worker_id = worker_config.id;
            let heartbeat_interval = Duration::from_secs(worker_config.heartbeat.interval);
            let worker = Worker::new(worker_config)?;

            let svc = worker_proto::worker_server::WorkerServer::new(WorkerService::new(worker));

            tokio::spawn(heartbeat(
                heartbeat_interval,
                worker_id,
                addr,
                "[::1]:50051".to_owned(),
            ));

            Server::builder().add_service(svc).serve(addr).await?;
        }
    }

    Ok(())
}
