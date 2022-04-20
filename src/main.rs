use log::LevelFilter;
use simplelog::*;

use razel::{parse_cli, Scheduler};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    TermLogger::init(
        LevelFilter::Info,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )
    .unwrap();

    let mut scheduler = Scheduler::new();
    parse_cli(
        std::env::args_os()
            .map(|x| x.into_string().unwrap())
            .collect(),
        &mut scheduler,
        None,
    )?;
    scheduler.run().await?;
    Ok(())
}
