use anyhow::{Context, Result};
use log::{debug, LevelFilter};
use razel::cli::parse_cli;
use razel::Razel;
use simplelog::*;

#[tokio::main]
async fn main() -> Result<()> {
    TermLogger::init(
        LevelFilter::Info,
        ConfigBuilder::new()
            .add_filter_ignore_str("cranelift_codegen")
            .add_filter_ignore_str("tracing::span")
            .add_filter_ignore_str("wasmtime_cranelift")
            .add_filter_ignore_str("wasmtime_jit")
            .add_filter_ignore_str("wasmtime_wasi")
            .set_target_level(LevelFilter::Error)
            .build(),
        TerminalMode::Stderr,
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
    dotenv_flow::dotenv_flow().context("Failed to read .env file")?;
    let Some(run_args) = parse_cli(
        std::env::args_os()
            .map(|x| x.into_string().unwrap())
            .collect(),
        &mut razel,
    )
    .await?
    else {
        return Ok(());
    };
    if run_args.info {
        razel.show_info(run_args.cache_dir)?;
        return Ok(());
    }
    if run_args.no_execution {
        razel.list_targets();
    } else {
        let stats = razel
            .run(
                run_args.keep_going,
                run_args.verbose,
                &run_args.group_by_tag,
                run_args.cache_dir,
                run_args.remote_cache,
                run_args.remote_cache_threshold,
                run_args.remote_exec,
            )
            .await?;
        debug!(
            "preparation: {:.3}s, execution: {:.3}s",
            stats.preparation_duration.as_secs_f32(),
            stats.execution_duration.as_secs_f32()
        );
        if !stats.exec.finished_successfully() {
            std::process::exit(1);
        }
    }
    Ok(())
}

#[cfg(test)]
mod main {
    use razel::cli::parse_cli;
    use razel::test_utils::ChangeDir;
    use razel::types::Tag;
    use razel::{config, Razel, SchedulerExecStats};
    use serial_test::serial;
    use std::path::Path;

    /// For simplification all tests use the same binary directory and therefore need to be run in serial
    async fn test_main(
        args: Vec<&str>,
        exp_stats: SchedulerExecStats,
        exp_cache_hits: usize,
        additional_tag: Option<(&str, Tag)>,
    ) {
        let cargo_workspace_dir = Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap())
            .parent()
            .unwrap()
            .to_path_buf();
        let _change_dir = ChangeDir::new(&cargo_workspace_dir);
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Debug)
            .filter_module("cranelift_codegen", log::LevelFilter::Warn)
            .filter_module("wasmtime", log::LevelFilter::Info)
            .filter_module("wasmtime_cranelift", log::LevelFilter::Info)
            .filter_module("serial_test", log::LevelFilter::Info)
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
                .await
                .unwrap()
                .unwrap();
            if let Some((name, tag)) = additional_tag.clone() {
                razel.add_tag_for_command(name, tag);
            }
            let act_stats = razel
                .run(false, true, "", None, vec![], None, vec![])
                .await
                .unwrap();
            assert_eq!(act_stats.exec, exp_stats);
            assert_eq!(act_stats.cache_hits, 0);
        }
        // run normally
        {
            let mut razel = Razel::new();
            razel.clean();
            parse_cli(args.iter().map(|&x| x.into()).collect(), &mut razel)
                .await
                .unwrap()
                .unwrap();
            if let Some((name, tag)) = additional_tag {
                razel.add_tag_for_command(name, tag);
            }
            let act_stats = razel
                .run(false, true, "", None, vec![], None, vec![])
                .await
                .unwrap();
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
                "examples/data/a.csv",
                "examples/data/f.csv",
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
                "examples/data/a.csv",
                "examples/data/f.csv",
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
            vec![config::EXECUTABLE, "exec", "-f", "examples/batch.sh"],
            SchedulerExecStats {
                succeeded: 9,
                ..Default::default()
            },
            9,
            None,
        )
        .await;
    }

    const RAZEL_JSONL_EXP_SUCCEEDED: usize = 12;
    const RAZEL_JSONL_EXP_CACHED: usize = 10;

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    async fn exec_razel_jsonl() {
        test_main(
            vec![config::EXECUTABLE, "exec", "-f", "examples/razel.jsonl"],
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
            vec![config::EXECUTABLE, "exec", "-f", "examples/razel.jsonl"],
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
            vec![config::EXECUTABLE, "exec", "-f", "examples/razel.jsonl"],
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
