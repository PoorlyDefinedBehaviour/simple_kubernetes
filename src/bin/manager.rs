pub mod manager_proto {
    tonic::include_proto!("manager");
}
use anyhow::Result;
use manager_proto::{RegisterWorkerReply, RegisterWorkerRequest};
use simple_kubernetes::{
    manager::{Manager, RemoteWorker},
    simple_scheduler::SimpleScheduler,
};
use std::{net::SocketAddr, str::FromStr};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{error, info, Level};
use uuid::Uuid;

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
            worker_addr: SocketAddr::from_str(&input.worker_addr)?,
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let addr = "[::1]:50051".parse().unwrap();

    info!(addr = ?addr, "starting manager server");

    let manager = Manager::new(Box::new(SimpleScheduler::new()));
    let svc = manager_proto::manager_server::ManagerServer::new(ManagerService::new(manager));

    Server::builder().add_service(svc).serve(addr).await?;

    // TODO: register workers so the manager knows which workers are alive.

    Ok(())
}
