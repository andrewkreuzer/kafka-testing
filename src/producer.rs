use std::time::Duration;

use log::{info, warn, debug};
use rdkafka::error::KafkaError;
use tokio::task;

use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders, OwnedMessage};
use rdkafka::producer::{Producer, FutureProducer, FutureRecord};

use trip::CircuitBreaker;
use trip::Error as TripError;

pub fn run(
    config: ClientConfig,
    topic: String,
    oneshot: bool,
    delay_duration: Duration,
    message_count: i32,
    kill_channel: tokio::sync::broadcast::Sender<bool>,
) -> task::JoinHandle<()> {
    let topic = topic.clone();
    let config = config.clone();
    let rx = kill_channel.subscribe();
    task::spawn(async move {
        produce(config, &topic, oneshot, delay_duration, message_count, rx).await;
    })
}

pub async fn produce(
    config: ClientConfig,
    topic: &str,
    onseshot: bool,
    delay_duration: Duration,
    message_count: i32,
    mut kill_channel: tokio::sync::broadcast::Receiver<bool>,
) {
    let mut circuit = CircuitBreaker::new(3, Duration::from_secs(3));
    let message_count = if message_count > 0 {
        message_count
    } else {
        100
    };

    let producer: &FutureProducer = &config.create().expect("Producer creation error");
    let deliver = |i: i32| async move {
        producer
            .send(
                FutureRecord::to(topic)
                    .payload(&format!("Message {}", i))
                    .key(&format!("{}", i))
                    .headers(OwnedHeaders::new().insert(Header {
                        key: "header_key",
                        value: Some("header_value"),
                    })),
                Duration::from_secs(0),
            )
            .await
    };
    let mut loop_count = 0;
    let mut completed = 0;
    let mut errored = 0;
    loop {
        let futures = (0..message_count)
            .map(|i| circuit.call_async(|_e: &(KafkaError, OwnedMessage)| false, deliver(i), None))
            .collect::<Vec<_>>();
        let futures = futures::future::join_all(futures);

        tokio::select! {
            kill = kill_channel.recv() => {
                if kill.is_ok() {
                    debug!("producer recieved signal, shutting down");
                    if let Err(e) = producer.flush(Duration::from_secs(10)) {
                        warn!("flush error: {:?}", e);
                    }
                    break;
                }
            }
            futures = futures => {
                for future in futures {
                    match future {
                        Ok(_) => completed += 1,
                        Err(TripError::Inner((kafka_err, _msg))) => {
                            warn!("{}", kafka_err);
                            errored += 1
                        }
                        Err(_) => {
                            errored += 1;
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

        if onseshot {
            break;
        }
        tokio::time::sleep(delay_duration).await;
    }
    info!("producer has shut down");
}
