use std::collections::HashMap;

use crate::{definition::Definition, manager::RemoteWorker, worker::WorkerId};
use async_trait::async_trait;

#[derive(Debug)]
pub struct CandidateSelectionInput<'a> {
    pub definition: &'a Definition,
    pub workers: &'a HashMap<WorkerId, RemoteWorker>,
}

#[derive(Debug, thiserror::Error)]
pub enum CandidateSelectionError {
    #[error("unable to find {num_workers} able to run the task")]
    NotEnoughWorkersMatched {
        num_workers: usize,
        rejection_reasons: Vec<String>,
    },
}

#[async_trait]
pub trait Scheduler: Send + Sync {
    async fn select_candidate_nodes(
        &self,
        input: &CandidateSelectionInput,
    ) -> Result<Vec<WorkerId>, CandidateSelectionError>;
}
