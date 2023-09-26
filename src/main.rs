use log::{debug, LevelFilter};
use simplelog::*;

use razel::{parse_cli, Razel};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    TermLogger::init(
        LevelFilter::Info,
        ConfigBuilder::new()
            .add_filter_ignore_str("cranelift_codegen::context")
            .build(),
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

    let mut razel = Razel::new();
    if let Some(run_args) = parse_cli(
        std::env::args_os()
            .map(|x| x.into_string().unwrap())
            .collect(),
        &mut razel,
    )? {
        if run_args.no_execution {
            razel.list_commands();
        } else {
            if !run_args.remote_cache.is_empty() {
                razel.connect_remote_cache(&run_args.remote_cache).await?;
            }
            let stats = razel.run(run_args.keep_going, run_args.verbose).await?;
            debug!(
                "preparation: {:.3}s, execution: {:.3}s",
                stats.preparation_duration.as_secs_f32(),
                stats.execution_duration.as_secs_f32()
            );
            if !stats.exec.finished_successfully() {
                std::process::exit(1);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod main {
    use serial_test::serial;

    use razel::metadata::Tag;
    use razel::{config, parse_cli, Razel, SchedulerExecStats};

    /// For simplification all tests use the same binary directory and therefore need to be run in serial
    async fn test_main(
        args: Vec<&str>,
        exp_stats: SchedulerExecStats,
        exp_cache_hits: usize,
        additional_tag: Option<(&str, Tag)>,
    ) {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Debug)
            .filter_module("cranelift_codegen", log::LevelFilter::Warn)
            .filter_module("wasmtime_cranelift", log::LevelFilter::Info)
            .filter_module("wasmtime_jit", log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        // exit on panic in any thread
        let default_panic = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            default_panic(info);
            std::process::exit(1);
        }));
        // run without reading cache
        {
            let mut razel = Razel::new();
            razel.read_cache = false;
            razel.clean();
            parse_cli(args.iter().map(|&x| x.into()).collect(), &mut razel)
                .unwrap()
                .unwrap();
            if let Some((name, tag)) = additional_tag.clone() {
                razel.add_tag_for_command(name, tag).unwrap();
            }
            let act_stats = razel.run(false, true).await.unwrap();
            assert_eq!(act_stats.exec, exp_stats);
            assert_eq!(act_stats.cache_hits, 0);
        }
        // run normally
        {
            let mut razel = Razel::new();
            razel.clean();
            parse_cli(args.iter().map(|&x| x.into()).collect(), &mut razel)
                .unwrap()
                .unwrap();
            if let Some((name, tag)) = additional_tag {
                razel.add_tag_for_command(name, tag).unwrap();
            }
            let act_stats = razel.run(false, true).await.unwrap();
            assert_eq!(act_stats.exec, exp_stats);
            assert_eq!(act_stats.cache_hits, exp_cache_hits);
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
            1,
            None,
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
            0,
            None,
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
            1,
            None,
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
            0,
            None,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    async fn exec_batch_sh() {
        test_main(
            vec![config::EXECUTABLE, "exec", "-f", "test/batch.sh"],
            SchedulerExecStats {
                succeeded: 7,
                ..Default::default()
            },
            7,
            None,
        )
        .await;
    }

    const RAZEL_JSONL_EXP_SUCCEEDED: usize = 9;
    const RAZEL_JSONL_EXP_CACHED: usize = 7;

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    async fn exec_razel_jsonl() {
        test_main(
            vec![config::EXECUTABLE, "exec", "-f", "test/razel.jsonl"],
            SchedulerExecStats {
                succeeded: RAZEL_JSONL_EXP_SUCCEEDED,
                ..Default::default()
            },
            RAZEL_JSONL_EXP_CACHED,
            None,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    async fn exec_razel_jsonl_with_no_cache_tag() {
        test_main(
            vec![config::EXECUTABLE, "exec", "-f", "test/razel.jsonl"],
            SchedulerExecStats {
                succeeded: RAZEL_JSONL_EXP_SUCCEEDED,
                ..Default::default()
            },
            RAZEL_JSONL_EXP_CACHED - 1,
            Some(("d.csv", Tag::NoCache)),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    async fn exec_razel_jsonl_with_no_sandbox_tag() {
        test_main(
            vec![config::EXECUTABLE, "exec", "-f", "test/razel.jsonl"],
            SchedulerExecStats {
                succeeded: RAZEL_JSONL_EXP_SUCCEEDED,
                ..Default::default()
            },
            RAZEL_JSONL_EXP_CACHED - 1, // no-sandbox should set no-cache
            Some(("d.csv", Tag::NoSandbox)),
        )
        .await;
    }
}
