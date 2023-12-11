use parse_duration::parse;
use std::{process::exit, time::Duration};

use clap::Parser;

use kafka_testing::{config::Config, consumer, producer};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "kafka:9092")]
    broker: Option<String>,

    #[arg(short, long, default_value = "my-group")]
    group: Option<String>,

    #[arg(short, long, default_value = "my-topic")]
    topic: String,

    #[clap(short, long)]
    delay: Option<String>,

    #[clap(long)]
    producer: bool,

    #[clap(short, long)]
    producer_config: Option<String>,

    #[arg(long, default_value = "true")]
    looping: bool,

    #[arg(short, long, default_value_t = 100)]
    message_count: u8,

    #[clap(long)]
    consumer: bool,

    #[clap(long, default_value_t = 3)]
    consumer_count: u8,

    #[clap(short, long)]
    consumer_config: Option<String>,

    #[clap(long)]
    debug: bool,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let config = Config::new(
        args.broker,
        vec![args.topic.clone()],
        args.group,
        args.producer_config,
        args.consumer_config,
    );

    if args.debug {
        println!("{:?}", config);
        exit(1)
    }

    let mut delay_duration: Duration = Duration::from_secs(1);
    if let Some(delay) = args.delay {
        delay_duration = parse(&delay).expect("Invalid delay");
    }

    let mut tasks = vec![];
    if args.producer {
        println!("Starting producer");
        tasks.push(producer::run(
            config.producer,
            &args.topic,
            args.looping,
            delay_duration,
        ))
    }

    if args.consumer {
        println!("Starting consumer");
        let consumer_tasks = consumer::run(config.consumer, &args.topic, args.consumer_count);

        tasks.extend(consumer_tasks);
    }

    for task in tasks {
        let _ = task.await;
    }
}
