use anyhow::Result;
use clap::{Parser, Subcommand};
use simple_kubernetes::manager_proto::{
    self, ApplyReply, ApplyRequest, NodeHeartbeatRequest, NodeHeartbeatResponse,
};
use simple_kubernetes::simple_scheduler;
use simple_kubernetes::{
    definition::Definition, manager::Manager, simple_scheduler::SimpleScheduler,
};
use tracing::{info, Level};

use tonic::{transport::Server, Request, Response, Status};

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

#[tonic::async_trait]
impl manager_proto::manager_server::Manager for ManagerService {
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

    #[tracing::instrument(name = "ManagerService::node_heartbeat", skip_all, fields(
        request = ?request
    ))]
    async fn node_heartbeat(
        &self,
        request: Request<NodeHeartbeatRequest>,
    ) -> Result<Response<NodeHeartbeatResponse>, Status> {
        let request = request.into_inner();

        self.manager
            .node_heartbeat(request)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        Ok(Response::new(manager_proto::NodeHeartbeatResponse {}))
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

            let etcd = etcd_rs::Client::connect(etcd_rs::ClientConfig::new([
                "http://127.0.0.1:2379".into(),
                "http://127.0.0.1:2380".into(),
            ]))
            .await?;

            info!("connected to etcd");

            let scheduler_config = simple_scheduler::Config::from_file(file).await?;

            let manager = Manager::new(etcd.clone());

            let scheduler = SimpleScheduler::new(scheduler_config, etcd);
            tokio::spawn(scheduler.watch_cluster_state_changes());

            let svc =
                manager_proto::manager_server::ManagerServer::new(ManagerService::new(manager));

            Server::builder().add_service(svc).serve(addr).await?;
        }
    }

    Ok(())
}
