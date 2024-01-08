use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use log::{info, warn};
use tokio::task;

use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

type LoggingConsumer = StreamConsumer<CustomContext>;

pub fn run(
    config: ClientConfig,
    topic: &str,
    count: u8,
    recieved: Arc<AtomicBool>,
) -> Vec<task::JoinHandle<()>> {
    (0..count)
        .map(|i| {
            let recieved = Arc::clone(&recieved);
            let topic = topic.to_owned();
            let config = config.clone();
            task::spawn(async move {
                consume(&i, &config, &topic, recieved).await;
            })
        })
        .collect()
}

async fn consume(id: &u8, config: &ClientConfig, topic: &str, recieved: Arc<AtomicBool>) {
    let context = CustomContext;

    let consumer: LoggingConsumer = config
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[topic])
        .expect("Can't subscribe to specified topics");

    let mut completed = 0;
    let mut errored = 0;
    let mut none = 0;
    let mut loop_count = 0;
    loop {
        if recieved.load(Ordering::Relaxed) {
            info!("consumer: {} recieved signal, shutting down", id);
            break;
        }
        match consumer.recv().await {
            Err(_) => {
                errored += 1;
            }
            Ok(m) => {
                match m.payload_view::<str>() {
                    None => {
                        none += 1;
                    }
                    Some(Ok(_)) => {
                        completed += 1;
                    }
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        errored += 1;
                    }
                };
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
        if loop_count % 1000 == 0 && completed + errored + none > 0 {
            info!(
                "consumer: {} completed: {} errored: {} none: {}",
                id, completed, errored, none
            )
        }
        loop_count += 1;
    }
    info!("consumer {} has shut down", id);
}
