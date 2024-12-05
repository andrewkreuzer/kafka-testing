use std::{
    process::exit,
    time::Duration,
};

use clap::{ArgAction, Parser};
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;

use futures::stream::StreamExt;
use tokio::sync::broadcast::{channel, Sender};

use kafka_testing::{
    config::{self, Config},
    consumer, producer,
};

#[derive(Parser, Debug)]
#[command(author, version, arg_required_else_help = true, verbatim_doc_comment)]
///| | ____ _ / _| | ____ _      | |_ ___  ___| |_(_)_ __   __ _ 
///| |/ / _` | |_| |/ / _` |_____| __/ _ \/ __| __| | '_ \ / _` |
///|   < (_| |  _|   < (_| |_____| ||  __/\__ \ |_| | | | | (_| |
///|_|\_\__,_|_| |_|\_\__,_|      \__\___||___/\__|_|_| |_|\__, |
///                                                        |___/ 
///
/// A simple tool to test Kafka infrastructure by producing and consuming messages.
/// It can be used to test Kafka clusters, brokers, topics, and consumers and
///  performance of Kafka clusters and brokers (tested up to 100k msg/s.
struct Args {
    /// Kafka broker address
    ///
    /// The address of the Kafka broker to connect to.
    #[arg(short, long, default_value = config::DEFAULT_BROKER)]
    broker: Option<String>,

    /// Kafka consumer group
    ///
    /// The name of the Kafka consumer group to connect as.
    #[arg(short, long, default_value = config::DEFAULT_GROUP)]
    group: Option<String>,

    /// Kafka topic
    ///
    /// The name of the Kafka topic to produce and consume messages to/from.
    #[arg(short, long, default_value = config::DEFAULT_TOPIC)]
    topic: Vec<String>,

    /// Delay between messages
    ///
    /// The delay between producing messages.
    #[clap(short, long, default_value = "1s")]
    delay: Option<String>,

    /// Start a producer
    #[clap(long)]
    producer: bool,

    /// Set producer properties file
    ///
    /// The path to a properties file containing producer configuration.
    /// When not present, default properties are used.
    #[clap(short, long)]
    producer_config: Option<String>,

    /// Run producer for a single iteration
    ///
    /// Sends <-message-count> messages and exits.
    #[arg( long, default_value = "false")]
    oneshot: bool,

    /// Number of messages to produce
    ///
    /// The number of messages to produce each iteration.
    #[arg( short, long, default_value_t = 100)]
    message_count: i32,

    /// Start a consumer
    #[clap(long)]
    consumer: bool,

    /// Number of consumers to start
    ///
    /// Should match the number of partitions in the topic (default to 3)
    #[clap(long, default_value_t = 3)]
    consumer_count: u8,

    /// Set consumer properties file
    ///
    /// The path to a properties file containing consumer configuration.
    /// Whne not present, default properties are used.
    #[clap(short, long)]
    consumer_config: Option<String>,

    /// Enable stats collection & printing
    ///
    /// This will print msg/s every 5 seconds.
    #[clap(long)]
    stats: bool,

    /// Verbosity level
    ///
    /// Will increase the verbosity level of the output based on the number of occurrences.
    #[clap(short, long, action = ArgAction::Count)]
    verbosity: u8,

    /// Print consumer/producer properties
    ///
    /// Will output the consummer and producer properties and exit.
    #[clap(long)]
    properties: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_max_level(verbosity_to_log_level(args.verbosity))
        .init();

    let signals = Signals::new([SIGHUP, SIGTERM, SIGINT, SIGQUIT])?;
    let handle = signals.handle();
    let (tx, _) = channel::<bool>(2);
    let signals_task = tokio::spawn(handle_signals(signals, tx.clone()));

    let config = Config::new(
        args.broker,
        args.topic.clone(),
        args.group,
        args.producer_config,
        args.consumer_config,
        args.verbosity,
    );

    if args.properties {
        println!("{}", config);
        exit(0);
    }

    let mut delay_duration: Duration = Duration::from_secs(1);
    if let Some(delay) = args.delay {
        delay_duration = parse_duration::parse(&delay)?;
    }

    let topic = match args.topic.first() {
        Some(topic) => topic.to_owned(),
        None => {
            println!("No topic specified");
            exit(1)
        }
    };

    let mut tasks = vec![];
    if args.producer {
        tasks.push(producer::run(
            config.producer,
            topic,
            args.oneshot,
            delay_duration,
            args.message_count,
            tx.clone(),
        ))
    }

    if args.consumer {
        let consumer = consumer::Consumer::new(
            config.consumer,
            args.topic,
            args.consumer_count,
            10000,
            tx.clone(),
        );

        tasks.extend(consumer.run(args.stats)?);
    }

    for task in tasks {
        let _ = task.await;
    }

    handle.close();
    signals_task.await?;

    Ok(())
}

async fn handle_signals(mut signals: Signals, tx: Sender<bool>) {
    while let Some(signal) = signals.next().await {
        match signal {
            SIGTERM | SIGINT | SIGQUIT => {
                println!("Recieved signal: {}", signal);
                let _ = tx.send(true);
            }
            _ => unreachable!(),
        }
    }
}

fn verbosity_to_log_level(verbosity: u8) -> tracing::Level {
    match verbosity {
        1 => tracing::Level::INFO,
        2 => tracing::Level::DEBUG,
        3 => tracing::Level::TRACE,
        _ => tracing::Level::ERROR,
    }
}
