use crate::{definition::Definition, work_queue::WorkQueue};
use etcd_rs::{Client, KeyRange, KeyValueOp, WatchInbound, WatchOp};
use std::{sync::Arc, time::Duration};
use tokio::select;
use tracing::{error, info};

pub struct ListWatcher<T> {
    resync_interval: Duration,
    work_queue: Arc<WorkQueue<T>>,
    etcd: Client,
}

impl<T> ListWatcher<T>
where
    T: TryFrom<etcd_rs::KeyValue> + std::fmt::Debug,
    <T as TryFrom<etcd_rs::KeyValue>>::Error: std::fmt::Debug,
{
    pub fn new(resync_interval: Duration, work_queue: Arc<WorkQueue<T>>, etcd: Client) -> Self {
        Self {
            resync_interval,
            work_queue,
            etcd,
        }
    }

    #[tracing::instrument(name = "ListWatcher::list_and_watch", skip_all, fields(
        prefix = %prefix
    ))]
    pub async fn list_and_watch(self, prefix: String) {
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
                        let t = T::try_from(kv).expect("unexpected data format, this is a bug.");
                        self.work_queue.append(t).await.expect("work queue append failed");
                    }
                },
                // TODO: i think this may be broken, are events being received?
                message = stream.inbound() => {
                    info!(?message, "update received for prefix");
                    match message {
                        WatchInbound::Ready(watch_response) => {
                            for event in watch_response.events {
                                let t = T::try_from(event.kv).expect("unexpected data format, this is a bug.");
                                self.work_queue.append(t).await.expect("work queue append failed");
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

impl TryFrom<etcd_rs::KeyValue> for Definition {
    type Error = serde_json::Error;

    fn try_from(value: etcd_rs::KeyValue) -> Result<Self, Self::Error> {
        serde_json::from_slice(&value.value)
    }
}
