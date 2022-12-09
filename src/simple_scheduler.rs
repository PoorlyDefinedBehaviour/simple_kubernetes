use async_trait::async_trait;

use crate::scheduler::{CandidateSelectionError, CandidateSelectionInput, Scheduler};
use crate::worker::WorkerId;

pub struct SimpleScheduler {}

impl SimpleScheduler {
    #[tracing::instrument(name = "SimpleScheduler::new", skip_all)]
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Scheduler for SimpleScheduler {
    #[tracing::instrument(name= "SimpleScheduler::select_node_to_run_container", skip_all, fields(
    input = ?input
  ))]
    async fn select_node_to_run_container(
        &self,
        input: &CandidateSelectionInput,
    ) -> Result<WorkerId, CandidateSelectionError> {
        let worker_id = input.workers.keys().next().cloned().unwrap();
        Ok(worker_id)
    }
}
