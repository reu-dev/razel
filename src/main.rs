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

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use razel::{config, parse_cli, Scheduler, SchedulerResult};

    /// For simplification all tests use the same binary directory and therefore need to be run in serial
    async fn test_main(args: Vec<&str>, exp: SchedulerResult) {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Debug)
            .is_test(true)
            .try_init();
        let mut scheduler = Scheduler::new();
        scheduler.clean();
        parse_cli(
            args.iter().map(|&x| x.into()).collect(),
            &mut scheduler,
            args.get(2).map(|&x| x.into()),
        )
        .unwrap();
        let act = scheduler.run().await.unwrap();
        assert_eq!(act, exp);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    async fn command_ok() {
        test_main(
            vec![config::EXECUTABLE, "command", "--", "cmake", "-E", "true"],
            SchedulerResult {
                succeeded: 1,
                failed: 0,
                not_run: 0,
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    async fn command_fail() {
        test_main(
            vec![config::EXECUTABLE, "command", "--", "cmake", "-E", "false"],
            SchedulerResult {
                succeeded: 0,
                failed: 1,
                not_run: 0,
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    async fn task_ok() {
        test_main(
            vec![
                config::EXECUTABLE,
                "task",
                "ensure-not-equal",
                "test/data/a.csv",
                "test/data/f.csv",
            ],
            SchedulerResult {
                succeeded: 1,
                failed: 0,
                not_run: 0,
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    async fn task_fail() {
        test_main(
            vec![
                config::EXECUTABLE,
                "task",
                "ensure-equal",
                "test/data/a.csv",
                "test/data/f.csv",
            ],
            SchedulerResult {
                succeeded: 0,
                failed: 1,
                not_run: 0,
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    async fn build() {
        test_main(
            vec![config::EXECUTABLE, "build", "test/razel.jsonl"],
            SchedulerResult {
                succeeded: 7,
                failed: 0,
                not_run: 0,
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    async fn batch() {
        test_main(
            vec![config::EXECUTABLE, "batch", "test/batch.sh"],
            SchedulerResult {
                succeeded: 7,
                failed: 0,
                not_run: 0,
            },
        )
        .await;
    }
}
