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

const BANNER: &str = r"
| | ____ _ / _| | ____ _      | |_ ___  ___| |_(_)_ __   __ _ 
| |/ / _` | |_| |/ / _` |_____| __/ _ \/ __| __| | '_ \ / _` |
|   < (_| |  _|   < (_| |_____| ||  __/\__ \ |_| | | | | (_| |
|_|\_\__,_|_| |_|\_\__,_|      \__\___||___/\__|_|_| |_|\__, |
                                                        |___/ 
";

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, arg_required_else_help = true)]
struct Args {
    #[arg(short, long, default_value = config::DEFAULT_BROKER, help = "Kafka broker address")]
    broker: Option<String>,

    #[arg(short, long, default_value = config::DEFAULT_GROUP, help = "Kafka consumer group")]
    group: Option<String>,

    #[arg(short, long, default_value = config::DEFAULT_TOPIC, help = "Kafka topic")]
    topic: Vec<String>,

    #[clap(short, long, default_value = "1s", help = "Delay between messages")]
    delay: Option<String>,

    #[clap(long, help = "Starts a producer")]
    producer: bool,

    #[clap(short, long, help = "Set producer properties file")]
    producer_config: Option<String>,

    #[arg(
        long,
        default_value = "false",
        help = "Run producer for a single iteration"
    )]
    oneshot: bool,

    #[arg(
        short,
        long,
        default_value_t = 100,
        help = "Number of messages to produce"
    )]
    message_count: i32,

    #[clap(long, help = "Starts a consumer")]
    consumer: bool,

    #[clap(long, default_value_t = 3, help = "Number of consumers to start")]
    consumer_count: u8,

    #[clap(short, long, help = "Set consumer properties file")]
    consumer_config: Option<String>,

    #[clap(long, help = "Enable stats collection & printing")]
    stats: bool,

    #[clap(short, long, action = ArgAction::Count, help = "Increase output verbosity")]
    verbosity: u8,

    #[clap(long, help = "prints kafka properties and exits")]
    properties: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("{BANNER}");
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
