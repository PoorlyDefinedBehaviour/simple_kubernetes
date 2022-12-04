pub mod worker_proto {
    tonic::include_proto!("worker");
}
use tonic::{transport::Server, Request, Response, Status};
use tracing::info;
use worker_proto::{RunTaskReply, RunTaskRequest};

#[derive(Default)]
pub struct WorkerService {}

#[tonic::async_trait]
impl worker_proto::worker_server::Worker for WorkerService {
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
    let addr = "[::1]:50052".parse().unwrap();

    info!(addr = ?addr, "starting worker server");

    let svc = worker_proto::worker_server::WorkerServer::new(WorkerService::default());

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
