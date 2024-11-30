use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use log::{debug, error, info, warn};
use rdkafka::types::RDKafkaErrorCode;
use tokio::task;

use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer as RdConsumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;

use cushion::Cushion;
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

static STATS: Stats = Stats {
    completed: AtomicU64::new(0),
    errored: AtomicU64::new(0),
    none: AtomicU64::new(0),
};

pub struct Consumer {
    pub topics: Vec<String>,
    pub number_of_consumers: u8,

    config: ClientConfig,
    buffer_size: usize,
    kill_channel: tokio::sync::broadcast::Sender<bool>,
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
        Self {
            topics,
            number_of_consumers,

            config,
            buffer_size,
            kill_channel,
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
                let kill_rx = self.kill_channel.subscribe();
                let buffer_size = self.buffer_size;
                task::spawn(async move {
                    consume(i, config, topics, buffer_size, kill_rx).await;
                })
            })
            .collect();

        if stats {
            tasks.push(self.start_stats()?)
        };

        Ok(tasks)
    }

    fn start_stats(&self) -> Result<task::JoinHandle<()>, Box<dyn std::error::Error>> {
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
                        let (completed, errored, none, msg_count) = STATS.reset();
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
    mut kill_channel: tokio::sync::broadcast::Receiver<bool>,
) {
    let mut send_buffer = Cushion::with_capacity(buffer_size);
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
                    Err(e) => {
                        error!("Error on consumer: {} while receiving: {:?}", id, e);
                        STATS.add_errored();
                    }
                    Ok(m) => {
                        match m.payload_view::<str>() {
                            None => {
                                STATS.add_none();
                            }
                            Some(Ok(m)) => {
                                STATS.add_completed();
                                send_buffer.push_back(m.to_string());
                                i += 1;
                            }
                            Some(Err(e)) => {
                                STATS.add_errored();
                                error!("deserializing message payload: {:?}", e);
                            }
                        };
                    }
                }

                if i >= buffer_size {
                    i = 0;
                    let _ = circuit.call_async( |e: &bool| !(*e), async {
                        while let Some(_msg) = send_buffer.pop_front() {}
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        debug!("consumer: {} buffer cleared", id);
                        Ok(())
                    }, None).await;

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
    #[allow(dead_code)]
    fn new() -> Self {
        Self {
            completed: AtomicU64::new(0),
            errored: AtomicU64::new(0),
            none: AtomicU64::new(0),
        }
    }

    fn reset(&self) -> (u64, u64, u64, u64) {
        let completed = self.completed.swap(0, Ordering::Release);
        let errored = self.errored.swap(0, Ordering::Release);
        let none = self.none.swap(0, Ordering::Release);
        let msg_count = completed + errored + none;
        (completed, errored, none, msg_count)
    }

    fn add_completed(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }

    fn add_errored(&self) {
        self.errored.fetch_add(1, Ordering::Relaxed);
    }

    fn add_none(&self) {
        self.none.fetch_add(1, Ordering::Relaxed);
    }
}
