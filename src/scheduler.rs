pub trait Scheduler {
    fn select_candidate_nodes();
    fn score();
    fn pick();
}
