pub mod manager_proto {
    tonic::include_proto!("manager");
}
use manager_proto::{RegisterWorkerReply, RegisterWorkerRequest};
use tonic::{transport::Server, Request, Response, Status};
use tracing::info;

#[derive(Default)]
pub struct ManagerService {}

#[tonic::async_trait]
impl manager_proto::manager_server::Manager for ManagerService {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerReply>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

        let reply = manager_proto::RegisterWorkerReply {};
        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();

    info!(addr = ?addr, "starting manager server");

    let svc = manager_proto::manager_server::ManagerServer::new(ManagerService::default());

    Server::builder().add_service(svc).serve(addr).await?;

    // TODO: register workers so the manager knows which workers are alive.

    Ok(())
}
