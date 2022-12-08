use anyhow::Result;
use clap::{Parser, Subcommand};
use simple_kubernetes::worker::manager_proto::{self, ApplyRequest};

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Apply a file containg a resource definition to create or update a resource.
    Apply {
        /// Path to file containing the resource definition.
        #[arg(short)]
        file: String,
        #[arg(short)]
        /// The manager's endpoint.
        endpoint: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Apply { file, endpoint } => {
            let definition = tokio::fs::read_to_string(file).await?;

            let mut client =
                manager_proto::manager_client::ManagerClient::connect(endpoint).await?;

            client.apply(ApplyRequest { body: definition }).await?;

            Ok(())
        }
    }
}
