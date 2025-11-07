use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{mpsc, broadcast};
use tokio::time::{Duration, Instant};
use tracing::info;
use thiserror::Error;

use nostr_model::{NostrEvent, NostrFilter};
use graph_cozo::{CozoStore, types::{Worldview, GraphRankSettings, Scorecard, Timestamp}};

#[derive(Debug, Error)]
pub enum RepoError {
    #[error("store error")] Store,
    #[error("overloaded")] Overloaded,
}

pub type Result<T> = std::result::Result<T, RepoError>;

#[async_trait]
pub trait RelayRepo: Send + Sync {
    async fn write_event(&self, evt: &NostrEvent) -> Result<()>;
    async fn query(&self, sub_id: &str, filters: Vec<NostrFilter>, tx: mpsc::Sender<NostrEvent>) -> Result<()>;
    async fn abandon_query(&self, sub_id: &str);
    async fn health_check(&self) -> Result<()>;

    // one-shot query for HTTP
    async fn query_once(&self, filters: Vec<NostrFilter>, limit: usize) -> Result<Vec<NostrEvent>>;

    // global broadcast
    fn subscribe(&self) -> broadcast::Receiver<NostrEvent>;

    async fn worldview_get(&self, observer: &str, context: &str) -> Result<Option<Worldview>>;
    async fn worldview_put(&self, wv: &Worldview) -> Result<()>;
    async fn grapevine_generate(&self, observer: &str, context: &str, settings: GraphRankSettings) -> Result<Timestamp>;
    async fn scorecards_get(&self, observer: &str, context: &str, ts: Option<Timestamp>) -> Result<Option<Vec<Scorecard>>>;
    async fn scorecards_put(&self, observer: &str, context: &str, ts: Timestamp, list: &[Scorecard]) -> Result<()>;
}

pub struct CozoRepo {
    store: Arc<CozoStore>,
    tx: broadcast::Sender<NostrEvent>,
    persist_tx: mpsc::Sender<NostrEvent>,
}

impl CozoRepo {
    pub fn new(store: Arc<CozoStore>) -> Self {
        let (tx, _rx) = broadcast::channel(1024);
        // bounded queue for write batching
        let (persist_tx, mut persist_rx) = mpsc::channel::<NostrEvent>(4096);
        let store_bg = store.clone();
        tokio::spawn(async move {
            // batch parameters
            let max_batch: usize = 500;
            let max_wait = Duration::from_millis(50);
            let mut batch: Vec<NostrEvent> = Vec::with_capacity(max_batch);
            let mut next_deadline: Option<Instant> = None;
            loop {
                tokio::select! {
                    maybe_evt = persist_rx.recv() => {
                        match maybe_evt {
                            Some(ev) => {
                                batch.push(ev);
                                graph_cozo::record_persist_queue_depth(batch.len());
                                if batch.len() == 1 { next_deadline = Some(Instant::now() + max_wait); }
                                if batch.len() >= max_batch {
                                    let to_write = std::mem::take(&mut batch);
                                    let _ = store_bg.put_events_bulk(to_write.into_iter()).await;
                                    graph_cozo::record_persist_queue_depth(0);
                                    next_deadline = None;
                                }
                            }
                            None => {
                                // drain last batch then exit
                                if !batch.is_empty() { let to_write = std::mem::take(&mut batch); let _ = store_bg.put_events_bulk(to_write.into_iter()).await; }
                                graph_cozo::record_persist_queue_depth(0);
                                break;
                            }
                        }
                    }
                    _ = async {
                        if let Some(deadline) = next_deadline { tokio::time::sleep_until(deadline.into()).await; }
                    }, if next_deadline.is_some() => {
                        if !batch.is_empty() {
                            let to_write = std::mem::take(&mut batch);
                            let _ = store_bg.put_events_bulk(to_write.into_iter()).await;
                        }
                        graph_cozo::record_persist_queue_depth(0);
                        next_deadline = None;
                    }
                }
            }
        });
        info!("repo: write batcher started");
        Self { store, tx, persist_tx }
    }
}

#[async_trait]
impl RelayRepo for CozoRepo {
    async fn write_event(&self, evt: &NostrEvent) -> Result<()> {
        // enqueue for batched persistence; if queue is full, signal overload
        match self.persist_tx.try_send(evt.clone()) {
            Ok(_) => {
                // broadcast immediately to subscribers
                let _ = self.tx.send(evt.clone());
                Ok(())
            }
            Err(_e) => {
                Err(RepoError::Overloaded)
            }
        }
    }

    async fn query(&self, _sub_id: &str, filters: Vec<NostrFilter>, tx: mpsc::Sender<NostrEvent>) -> Result<()> {
        let events = self.store.query_events(&filters, 1000).await.map_err(|_| RepoError::Store)?;
        for e in events { let _ = tx.send(e).await; }
        Ok(())
    }

    async fn abandon_query(&self, _sub_id: &str) { /* no-op stub */ }

    async fn health_check(&self) -> Result<()> {
        self.store.health_check().await.map_err(|_| RepoError::Store)
    }

    async fn query_once(&self, filters: Vec<NostrFilter>, limit: usize) -> Result<Vec<NostrEvent>> {
        self.store
            .query_events(&filters, limit)
            .await
            .map_err(|_| RepoError::Store)
    }

    fn subscribe(&self) -> broadcast::Receiver<NostrEvent> { self.tx.subscribe() }

    async fn worldview_get(&self, observer: &str, context: &str) -> Result<Option<Worldview>> {
        self.store.worldview_get(&observer.to_string(), context).await.map_err(|_| RepoError::Store)
    }

    async fn worldview_put(&self, wv: &Worldview) -> Result<()> {
        self.store.worldview_put(wv).await.map_err(|_| RepoError::Store)
    }

    async fn grapevine_generate(&self, observer: &str, context: &str, settings: GraphRankSettings) -> Result<Timestamp> {
        self.store.grapevine_generate(&observer.to_string(), context, settings).await.map_err(|_| RepoError::Store)
    }

    async fn scorecards_get(&self, observer: &str, context: &str, ts: Option<Timestamp>) -> Result<Option<Vec<Scorecard>>> {
        self.store.scorecards_get(&observer.to_string(), context, ts).await.map_err(|_| RepoError::Store)
    }

    async fn scorecards_put(&self, observer: &str, context: &str, ts: Timestamp, list: &[Scorecard]) -> Result<()> {
        self.store.scorecards_put(&observer.to_string(), context, ts, list).await.map_err(|_| RepoError::Store)
    }
}
