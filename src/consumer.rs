use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use log::{debug, info, warn, error};
use rdkafka::types::RDKafkaErrorCode;
use tokio::sync::Mutex;
use tokio::task;

use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer as RdConsumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;

use trip::CircuitBreaker;

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        debug!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        debug!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        match offsets.elements().last() {
            Some(el) => {
                debug!("Committing offsets: {:?}, got {:?}", el.offset(), result);
            }
            None => {
                debug!("Committing offsets: None, got {:?}", result);
            }
        }
    }
}

type LoggingConsumer = StreamConsumer<CustomContext>;

pub struct Consumer {
    pub topics: Vec<String>,
    pub number_of_consumers: u8,

    config: ClientConfig,
    buffer_size: usize,
    kill_channel: tokio::sync::broadcast::Sender<bool>,
    stats: Arc<Mutex<Stats>>,
    stats_interval: Duration,
}

impl Consumer {
    pub fn new(
        config: ClientConfig,
        topics: Vec<String>,
        number_of_consumers: u8,
        buffer_size: usize,
        kill_channel: tokio::sync::broadcast::Sender<bool>,
    ) -> Self {
        let stats = Arc::new(Mutex::new(Stats::new()));
        Self {
            topics,
            number_of_consumers,

            config,
            buffer_size,
            kill_channel,
            stats,
            stats_interval: Duration::from_secs(5),
        }
    }

    pub fn run(
        &self,
        stats: bool,
    ) -> Result<Vec<task::JoinHandle<()>>, Box<dyn std::error::Error>> {
        let mut tasks: Vec<task::JoinHandle<()>> = (0..self.number_of_consumers)
            .map(|i| {
                let topics = self.topics.clone();
                let config = self.config.clone();
                let bf = self.buffer_size;
                let stats = self.stats.clone();
                let kill_rx = self.kill_channel.subscribe();
                task::spawn(async move {
                    consume(i, config, topics, bf, stats, kill_rx).await;
                })
            })
            .collect();

        if stats {
            tasks.push(self.start_stats()?)
        };

        Ok(tasks)
    }

    fn start_stats(&self) -> Result<task::JoinHandle<()>, Box<dyn std::error::Error>> {
        let stats = self.stats.clone();
        let interval = self.stats_interval;
        let mut kill_rx = self.kill_channel.subscribe();
        let stats_handle = task::spawn(async move {
            loop {
                tokio::select! {
                    kill = kill_rx.recv() => {
                        if kill.is_ok() {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(interval) => {
                        let stats = stats.lock().await;
                        let completed = stats.completed.swap(0, std::sync::atomic::Ordering::Relaxed);
                        let errored = stats.errored.swap(0, std::sync::atomic::Ordering::Relaxed);
                        let none = stats.none.swap(0, std::sync::atomic::Ordering::Relaxed);
                        let msg_count = completed + errored + none;
                        info!("Msgs/s: {}", msg_count / interval.as_secs());
                        debug!("Completed: {}, Errored: {}, None: {}", completed, errored, none);
                    }
                }
            }
        });
        Ok(stats_handle)
    }
}

async fn consume(
    id: u8,
    config: ClientConfig,
    topics: Vec<String>,
    buffer_size: usize,
    stats: Arc<Mutex<Stats>>,
    mut kill_channel: tokio::sync::broadcast::Receiver<bool>,
) {
    let mut msg_buffer = vec!["".to_string(); buffer_size];
    let mut circuit = CircuitBreaker::new(3, Duration::from_secs(3));
    let context = CustomContext;
    let consumer: LoggingConsumer = config
        .create_with_context::<CustomContext, LoggingConsumer>(context)
        .expect("Consumer creation failed");

    let topics: Vec<&str> = topics.iter().map(|t| t.as_str()).collect();
    consumer
        .subscribe(&topics)
        .expect("Can't subscribe to specified topics");

    let mut i = 0;
    info!("consumer {} is running", id);
    loop {
        tokio::select! {
            kill = kill_channel.recv() => {
                if kill.is_ok() {
                    debug!("consumer: {} recieved signal, shutting down", id);
                    break;
                }
            }
            msg = consumer.recv() => {
                match msg {
                    Err(_) => {
                        stats.lock().await.errored.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    Ok(m) => {
                        match m.payload_view::<str>() {
                            None => {
                                stats.lock().await.none.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                            Some(Ok(m)) => {
                                stats.lock().await.completed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                msg_buffer[i] = m.to_string();
                                i += 1;
                            }
                            Some(Err(e)) => {
                                error!("deserializing message payload: {:?}", e);
                                stats.lock().await.errored.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        };
                    }
                }

                if i >= buffer_size {
                    i = 0;
                    let _ = circuit.call_async(
                        |e: &bool| !(*e),
                        async {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        Ok(())
                    },
                    None).await;
                    match consumer.commit_consumer_state(CommitMode::Sync) {
                        Ok(_) => {}
                        Err(e) => {
                            match e {
                                rdkafka::error::KafkaError::ConsumerCommit(RDKafkaErrorCode::NoOffset) => {
                                    warn!("No offset to commit");
                                },
                                _ => error!("Error on consumer: {} while committing: {:?}", id, e),
                            }
                        }
                    }
                }
            }

        }
    }
    info!("consumer {} has shut down", id);
}

struct Stats {
    completed: AtomicU64,
    errored: AtomicU64,
    none: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Self {
            completed: 0.into(),
            errored: 0.into(),
            none: 0.into(),
        }
    }
}
