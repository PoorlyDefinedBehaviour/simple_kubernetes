use crate::{
    task_proto::{Task, TaskSet},
    worker_proto,
};
use etcd_rs::{Client, KeyRange, KeyValueOp, WatchInbound, WatchOp};
use prost::{DecodeError, Message};
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::Sender;
use tracing::{error, info};

pub struct ListWatcher<T> {
    resync_interval: Duration,
    work_tx: Sender<T>,
    etcd: Client,
}

impl<T> ListWatcher<T>
where
    T: TryFrom<etcd_rs::KeyValue> + std::fmt::Debug,
    <T as TryFrom<etcd_rs::KeyValue>>::Error: std::fmt::Debug,
{
    pub fn new(resync_interval: Duration, work_tx: Sender<T>, etcd: Client) -> Self {
        Self {
            resync_interval,
            work_tx,
            etcd,
        }
    }

    #[tracing::instrument(name = "ListWatcher::list_and_watch", skip_all, fields(
        prefix = %prefix
    ))]
    pub async fn list_and_watch(self, prefix: String) {
        info!("listing and watching prefix");

        let (mut stream, _cancel) = self
            .etcd
            .watch(KeyRange::prefix(prefix.clone()))
            .await
            .expect("watch by prefix");

        let mut resync_interval = tokio::time::interval(self.resync_interval);

        loop {
            select! {
                _ = resync_interval.tick() => {
                    info!("fetching whole range for prefix");
                    let range_response = match self
                       .etcd
                        .get(KeyRange::range(prefix.clone(), vec![0]))
                        .await
                        {
                            Err(error) => {
                                error!(?prefix, ?error, "error listing prefix");
                                continue;
                            }
                            Ok(v) => v,
                        };

                    info!("fetched {} entries", range_response.kvs.len());

                    for kv in range_response.kvs {
                        let t = match T::try_from(kv) {
                            Err(error) => {
                                error!(?error, "unexpected data format, this is a bug.");
                                continue;
                            },
                            Ok(v) => v
                        };

                        if let Err(error) = self.work_tx.send(t).await {
                            error!(?error, "unable to send item to work channel")
                        };
                    }
                },
                // TODO: i think this may be broken, are events being received?
                message = stream.inbound() => {
                    match message {
                        WatchInbound::Ready(watch_response) => {
                            for event in watch_response.events {
                                let t = match T::try_from(event.kv) {
                                    Err(error) => {
                                        error!(?error, "unexpected data format, this is a bug.");
                                        continue;
                                    },
                                    Ok(v) => v
                                };

                                info!(value = ?t, "update received for prefix");

                                if let Err(error) = self.work_tx.send(t).await {
                                    error!(?error, "unable to send item to work channel")
                                };
                            }
                        },
                        WatchInbound::Interrupted(error) => {
                            error!(?error, "error watching prefix");
                            return;
                        },
                        WatchInbound::Closed => {
                            info!("prefix stream closed");
                            return;
                        }
                    }
                }
            }
        }
    }
}

impl TryFrom<etcd_rs::KeyValue> for TaskSet {
    type Error = DecodeError;

    fn try_from(value: etcd_rs::KeyValue) -> Result<Self, Self::Error> {
        TaskSet::decode(value.value.as_ref())
    }
}

impl TryFrom<etcd_rs::KeyValue> for Task {
    type Error = DecodeError;

    fn try_from(value: etcd_rs::KeyValue) -> Result<Self, Self::Error> {
        Task::decode(value.value.as_ref())
    }
}

impl TryFrom<etcd_rs::KeyValue> for worker_proto::CurrentState {
    type Error = DecodeError;

    fn try_from(value: etcd_rs::KeyValue) -> Result<Self, Self::Error> {
        worker_proto::CurrentState::decode(value.value.as_ref())
    }
}
