use rdkafka::config::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use std::boxed::Box;
use std::fmt::Display;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;

pub const DEFAULT_TOPIC: &str = "my-topic";
pub const DEFAULT_GROUP: &str = "my-group";
pub const DEFAULT_BROKER: &str = "localhost:9092";
pub const DEFAULT_CONSUMER_PROP_FILE: &str = "consumer.properties";
pub const DEFAULT_PRODUCER_PROP_FILE: &str = "producer.properties";

#[derive(Debug)]
pub struct Config {
    pub broker: String,
    pub topics: Vec<String>,
    pub group: String,
    pub producer: ClientConfig,
    pub consumer: ClientConfig,
}

impl Config {
    #[rustfmt::skip]
    pub fn new(
        broker: Option<String>,
        topics: Vec<String>,
        group: Option<String>,
        producer_file: Option<String>,
        consumer_file: Option<String>,
        verbosity: u8,
    ) -> Self {
        let broker = broker.as_deref().unwrap_or(DEFAULT_BROKER);
        let group = group.as_deref().unwrap_or(DEFAULT_GROUP);

        let mut producer = read_config_file(
            producer_file.as_deref().unwrap_or(DEFAULT_PRODUCER_PROP_FILE),
        ).unwrap_or(default_producer_config(broker));

        let mut consumer = read_config_file(
            consumer_file.as_deref().unwrap_or(DEFAULT_CONSUMER_PROP_FILE),
        ).unwrap_or(default_consumer_config(broker, group));

        producer.set_log_level(verbosity_to_rd_log_level(verbosity));
        consumer.set_log_level(verbosity_to_rd_log_level(verbosity));

        Self {
            broker: broker.to_string(),
            group: group.to_string(),
            topics,
            producer,
            consumer,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        let topics = vec![DEFAULT_TOPIC.to_string()];
        let producer = default_producer_config(DEFAULT_BROKER);
        let consumer = default_consumer_config(DEFAULT_BROKER, DEFAULT_GROUP);
        Self {
            broker: DEFAULT_BROKER.to_string(),
            group: DEFAULT_GROUP.to_string(),
            topics,
            producer,
            consumer,
        }
    }
}

impl Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let producer_properties: String = display_config_map(&self.producer);
        let consumer_properties: String = display_config_map(&self.consumer);
        fn display_config_map(config: &ClientConfig) -> String {
            config
                .config_map()
                .iter()
                .map(|(k, v)| format!("{}: {}", k, v))
                .collect::<Vec<String>>()
                .join("\n  ")
        }
        write!(
            f,
            r#"
Group: {}
Topics: {:?}
Producer Properties:
  {}
Consumer Properties:
  {}
            "#,
            self.group, self.topics, producer_properties, consumer_properties
        )
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
        .set("enable.auto.commit", "true");

    consumer_config
}

pub fn read_config_file(config: &str) -> Result<ClientConfig, Box<dyn std::error::Error>> {
    let mut kafka_config = ClientConfig::new();

    let file = File::open(config)?;
    for line in BufReader::new(&file).lines() {
        let line = line?;
        let cur_line = line.trim();
        if cur_line.starts_with('#') || cur_line.is_empty() {
            continue;
        }

        let (key, value): (&str, &str) = cur_line.split_once('=').unwrap();
        kafka_config.set(key.replace('\"', ""), value.replace('\"', ""));
    }

    Ok(kafka_config)
}

fn verbosity_to_rd_log_level(verbosity: u8) -> RDKafkaLogLevel {
    match verbosity {
        1 => RDKafkaLogLevel::Info,
        2 => RDKafkaLogLevel::Notice,
        3 => RDKafkaLogLevel::Debug,
        _ => RDKafkaLogLevel::Error,
    }
}
