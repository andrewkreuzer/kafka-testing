use rdkafka::config::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use std::boxed::Box;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;

#[derive(Debug)]
pub struct Config {
    pub topics: Vec<String>,
    pub producer: ClientConfig,
    pub consumer: ClientConfig,
}

impl Config {
    pub fn new(
        broker: Option<String>,
        topics: Vec<String>,
        group: Option<String>,
        producer_file: Option<String>,
        consumer_file: Option<String>,
    ) -> Self {
        let broker = broker.unwrap_or("kafka:9092".to_string());
        let group = group.unwrap_or("my-group".to_string());
        let producer_file = producer_file.unwrap_or("producer.properties".to_string());
        let consumer_file = consumer_file.unwrap_or("consumer.properties".to_string());
        let producer = read_config_file(producer_file).unwrap_or(default_producer_config(&broker));
        let consumer =
            read_config_file(consumer_file).unwrap_or(default_consumer_config(&broker, &group));
        Self {
            topics,
            producer,
            consumer,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        let topics = vec!["my-topic".to_string()];
        let broker = "kafka:9092".to_string();
        let group = "my-group".to_string();
        let producer = default_producer_config(&broker);
        let consumer = default_consumer_config(&broker, &group);
        Self {
            topics,
            producer,
            consumer,
        }
    }
}

fn default_producer_config(broker: &str) -> ClientConfig {
    let mut producer_config = ClientConfig::new();
    producer_config
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "5000");

    producer_config
}

fn default_consumer_config(broker: &str, group: &str) -> ClientConfig {
    let mut consumer_config = ClientConfig::new();
    consumer_config
        .set("bootstrap.servers", broker)
        .set("group.id", group)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set_log_level(RDKafkaLogLevel::Debug);

    consumer_config
}

pub fn read_config_file(config: String) -> Result<ClientConfig, Box<dyn std::error::Error>> {
    let mut kafka_config = ClientConfig::new();

    let file = File::open(config)?;
    for line in BufReader::new(&file).lines() {
        let cur_line: String = line?.trim().to_string();
        if cur_line.starts_with('#') || cur_line.len() < 1 {
            continue;
        }
        let (key, value): (&str, &str) = cur_line.split_once("=").unwrap();
        kafka_config.set(
            key.to_string().replace("\"", ""),
            value.to_string().replace("\"", ""),
        );
    }

    Ok(kafka_config)
}
