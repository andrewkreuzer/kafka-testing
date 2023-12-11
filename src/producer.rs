use std::time::Duration;

use log::info;
use tokio::task;

use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};

pub fn run(
    config: ClientConfig,
    topic: &str,
    looping: bool,
    delay_duration: Duration,
) -> task::JoinHandle<()> {
    let topic = topic.to_owned();
    let config = config.clone();
    task::spawn(async move {
        produce(config, &topic, looping, delay_duration).await;
    })
}

pub async fn produce(
    config: ClientConfig,
    topic_name: &str,
    looping: bool,
    delay_duration: Duration,
) {
    let producer: &FutureProducer = &config.create().expect("Producer creation error");
    loop {
        let futures = (0..50)
            .map(|i| async move {
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

                info!("Delivery status for message {} received", i);
                delivery_status
            })
            .collect::<Vec<_>>();

        for future in futures {
            info!("Future completed. Result: {:?}", future.await);
        }

        if !looping {
            break;
        }
        tokio::time::sleep(delay_duration).await;
    }
}
