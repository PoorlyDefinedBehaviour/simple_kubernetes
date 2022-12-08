use anyhow::Result;
use tokio::sync::mpsc::{error::SendError, Receiver, Sender};
use tokio::sync::Mutex;

const WORK_QUEUE_CHANNEL_BUFFER_SIZE: usize = 100;

pub struct WorkQueue<T> {
    tx: Sender<T>,
    rx: Mutex<Receiver<T>>,
}

impl<T> WorkQueue<T> {
    #[tracing::instrument(name = "WorkQueue::new", skip_all)]
    pub fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(WORK_QUEUE_CHANNEL_BUFFER_SIZE);

        Self {
            tx,
            rx: Mutex::new(rx),
        }
    }

    #[tracing::instrument(name = "WorkQueue::next", skip_all)]
    pub async fn append(&self, value: T) -> Result<(), SendError<T>> {
        self.tx.send(value).await
    }

    #[tracing::instrument(name = "WorkQueue::next", skip_all)]
    pub async fn next(&self) -> Option<T> {
        let mut rx = self.rx.lock().await;
        rx.recv().await
    }
}
