use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use log::{info, warn};
use tokio::task;

use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};

pub fn run(
    config: ClientConfig,
    topic: &str,
    looping: bool,
    delay_duration: Duration,
    received: Arc<AtomicBool>,
) -> task::JoinHandle<()> {
    let received = Arc::clone(&received);
    let topic = topic.to_owned();
    let config = config.clone();
    task::spawn(async move {
        produce(config, &topic, looping, delay_duration, received).await;
    })
}

pub async fn produce(
    config: ClientConfig,
    topic_name: &str,
    looping: bool,
    delay_duration: Duration,
    received: Arc<AtomicBool>,
) {
    let producer: &FutureProducer = &config.create().expect("Producer creation error");
    let deliver = |i: i32| async move {
        let delivery_status = producer
            .send(
                FutureRecord::to(topic_name)
                    .payload(&format!("Message {}", i))
                    .key(&format!("Key {}", i))
                    .headers(OwnedHeaders::new().insert(Header {
                        key: "header_key",
                        value: Some("header_value"),
                    })),
                Duration::from_secs(0),
            )
            .await;

        delivery_status
    };
    let mut loop_count = 0;
    let mut completed = 0;
    let mut errored = 0;
    loop {
        let futures = (0..100).map(|i| deliver(i)).collect::<Vec<_>>();

        if received.load(Ordering::Relaxed) {
            info!("Received signal to shut down");
            break;
        }

        for future in futures {
            match future.await {
                Ok(_) => completed += 1,
                Err(e) => {
                    warn!("{}", e.0);
                    errored += 1
                }
            }
        }
        if loop_count % 10 == 0 && completed + errored > 0 {
            info!(
                "loop: {}, completed: {}, errored: {}",
                loop_count, completed, errored
            );
        }

        loop_count += 1;

        if !looping {
            break;
        }
        tokio::time::sleep(delay_duration).await;
    }
    info!("producer has shut down");
}
