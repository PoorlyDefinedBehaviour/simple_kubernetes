use clap::{Parser, Subcommand};
use simple_kubernetes::worker::{
    self,
    worker_proto::{self, RunTaskReply, RunTaskRequest},
    Worker,
};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, Level};

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

#[derive(Default)]
pub struct WorkerService {}

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Config { file } => {
            let addr = "[::1]:50052".parse().unwrap();

            info!(addr = ?addr, "starting worker server");

            let worker_config = worker::Config::from_file(file).await?;

            Worker::start(worker_config).await?;

            let svc = worker_proto::worker_server::WorkerServer::new(WorkerService::default());

            Server::builder().add_service(svc).serve(addr).await?;
        }
    }

    Ok(())
}
