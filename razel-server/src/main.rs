use anyhow::Result;
use clap::Parser;
use razel_server::Server;
use razel_server::config::Config;
use std::path::PathBuf;

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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_new(
                std::env::var("RUST_LOG")
                    .unwrap_or("info,cranelift=info,wasmtime=info".to_string()),
            )
            .expect("failed to parse tracing directives"),
        )
        .with_writer(std::io::stderr)
        .init();

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
