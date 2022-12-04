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
    #[tracing::instrument(name= "SimpleScheduler::select_candidate_nodes", skip_all, fields(
    input = ?input
  ))]
    async fn select_candidate_nodes(
        &self,
        input: &CandidateSelectionInput,
    ) -> Result<Vec<WorkerId>, CandidateSelectionError> {
        let worker_id = input.workers.keys().next().cloned().unwrap();
        Ok(vec![worker_id])
    }
}
