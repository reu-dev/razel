use log::{debug, LevelFilter};
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
    let stats = scheduler.run().await?;
    debug!(
        "preparation: {:.3}s, execution: {:.3}s",
        stats.preparation_duration.as_secs_f32(),
        stats.execution_duration.as_secs_f32()
    );
    Ok(())
}

#[cfg(test)]
mod main {
    use serial_test::serial;

    use razel::{config, parse_cli, Scheduler, SchedulerExecStats};

    /// For simplification all tests use the same binary directory and therefore need to be run in serial
    async fn test_main(args: Vec<&str>, exp_stats: SchedulerExecStats) {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Debug)
            .is_test(true)
            .try_init();
        // run without reading cache
        {
            let mut scheduler = Scheduler::new();
            scheduler.read_cache = false;
            scheduler.clean();
            parse_cli(
                args.iter().map(|&x| x.into()).collect(),
                &mut scheduler,
                args.get(2).map(|&x| x.into()),
            )
            .unwrap();
            let act_stats = scheduler.run().await.unwrap();
            assert_eq!(act_stats.exec, exp_stats);
            assert_eq!(act_stats.cache_hits, 0);
        }
        // run with reading cache
        {
            let mut scheduler = Scheduler::new();
            scheduler.clean();
            parse_cli(
                args.iter().map(|&x| x.into()).collect(),
                &mut scheduler,
                args.get(2).map(|&x| x.into()),
            )
            .unwrap();
            let act_stats = scheduler.run().await.unwrap();
            assert_eq!(act_stats.exec, exp_stats);
            assert_eq!(act_stats.cache_hits, exp_stats.succeeded);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    async fn command_ok() {
        test_main(
            vec![config::EXECUTABLE, "command", "--", "cmake", "-E", "true"],
            SchedulerExecStats {
                succeeded: 1,
                ..Default::default()
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    async fn command_fail() {
        test_main(
            vec![config::EXECUTABLE, "command", "--", "cmake", "-E", "false"],
            SchedulerExecStats {
                failed: 1,
                ..Default::default()
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
            SchedulerExecStats {
                succeeded: 1,
                ..Default::default()
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
            SchedulerExecStats {
                failed: 1,
                ..Default::default()
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    async fn build() {
        test_main(
            vec![config::EXECUTABLE, "build", "test/razel.jsonl"],
            SchedulerExecStats {
                succeeded: 8,
                ..Default::default()
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    async fn batch() {
        test_main(
            vec![config::EXECUTABLE, "batch", "test/batch.sh"],
            SchedulerExecStats {
                succeeded: 8,
                ..Default::default()
            },
        )
        .await;
    }
}
