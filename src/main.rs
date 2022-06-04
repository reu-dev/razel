use log::{info, LevelFilter};
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

    // exit on panic in any thread
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    let mut scheduler = Scheduler::new();
    parse_cli(
        std::env::args_os()
            .map(|x| x.into_string().unwrap())
            .collect(),
        &mut scheduler,
        None,
    )?;
    let result = scheduler.run().await?;
    info!(
        "Done. {} succeeded, {} failed, {} not run.",
        result.succeeded, result.failed, result.not_run
    );
    Ok(())
}
