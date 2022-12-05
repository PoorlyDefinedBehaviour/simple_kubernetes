use anyhow::Result;
use clap::{Parser, Subcommand};
use simple_kubernetes::{
    definition::Definition,
    manager::{Config, Manager},
    simple_scheduler::SimpleScheduler,
};

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
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let mut manager = Manager::new(
        Config::from_file("manager.yml".to_owned()).await?,
        Box::new(SimpleScheduler::new()),
    )
    .await?;

    match cli.command {
        Commands::Apply { file } => {
            let definition = Definition::from_file(&file).await?;
            manager.apply(definition).await?;
            Ok(())
        }
    }
}
