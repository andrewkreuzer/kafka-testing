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
    kill_channel: tokio::sync::broadcast::Sender<bool>,
) -> task::JoinHandle<()> {
    let topic = topic.to_owned();
    let config = config.clone();
    let rx = kill_channel.subscribe();
    task::spawn(async move {
        produce(config, &topic, looping, delay_duration, rx).await;
    })
}

pub async fn produce(
    config: ClientConfig,
    topic_name: &str,
    looping: bool,
    delay_duration: Duration,
    mut kill_channel: tokio::sync::broadcast::Receiver<bool>,
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
        let futures = futures::future::join_all(futures);

        tokio::select! {
            kill = kill_channel.recv() => {
                if let Ok(_) = kill {
                    info!("producer recieved signal, shutting down");
                    break;
                }
            }
            futures = futures => {
                for future in futures {
                    match future {
                        Ok(_) => completed += 1,
                        Err((kafka_err, _msg)) => {
                            warn!("{}", kafka_err);
                            errored += 1
                        }
                    }
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
