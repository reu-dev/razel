use anyhow::Result;
use clap::Parser;
use razel_server::config::Config;
use razel_server::Server;
use std::path::PathBuf;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
struct Cli {
    /// config file to read
    #[clap(short, long)]
    config: PathBuf,
    /// node name from config file
    #[clap(short, long)]
    name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(
                EnvFilter::builder()
                    .with_default_directive(LevelFilter::INFO.into())
                    .from_env_lossy(),
            )
            .finish(),
    )
    .unwrap();

    // exit on panic in any thread
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    let cli = Cli::parse();
    let config = Config::read(&cli.config)?;
    Server::new(config, cli.name)?.run().await?;
    Ok(())
}
