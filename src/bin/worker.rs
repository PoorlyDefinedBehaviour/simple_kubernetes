use clap::{Parser, Subcommand};

use simple_kubernetes::worker::{self, Worker};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Start a worker and configure it using a config file.
    Config {
        /// Path to the config file.
        #[arg(short)]
        file: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Config { file } => {
            let worker_config = worker::Config::from_file(file).await?;

            Worker::start(worker_config).await?;
            tokio::signal::ctrl_c().await?;
        }
    }

    Ok(())
}
