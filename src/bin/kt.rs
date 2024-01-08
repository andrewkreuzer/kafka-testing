use parse_duration::parse;
use std::{
    process::exit,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use clap::Parser;
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;

use futures::stream::StreamExt;

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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let signals = Signals::new(&[SIGHUP, SIGTERM, SIGINT, SIGQUIT])?;
    let handle = signals.handle();

    let recieved = Arc::new(AtomicBool::new(false));
    let signals_task = tokio::spawn(handle_signals(signals, Arc::clone(&recieved)));

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
            Arc::clone(&recieved),
        ))
    }

    if args.consumer {
        println!("Starting consumer");
        let consumer_tasks = consumer::run(
            config.consumer,
            &args.topic,
            args.consumer_count,
            Arc::clone(&recieved),
        );

        tasks.extend(consumer_tasks);
    }

    for task in tasks {
        let _ = task.await;
    }

    handle.close();
    signals_task.await?;

    Ok(())
}

async fn handle_signals(mut signals: Signals, recieved: Arc<AtomicBool>) {
    while let Some(signal) = signals.next().await {
        match signal {
            SIGTERM | SIGINT | SIGQUIT => {
                println!("Recieved signal: {}", signal);
                recieved.store(true, Ordering::Relaxed);
            }
            _ => unreachable!(),
        }
    }
}
