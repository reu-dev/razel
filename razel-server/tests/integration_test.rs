use razel::cli::parse_cli;
use razel::test_utils::{ChangeDir, TempDir};
use razel::types::Tag;
use razel::{Razel, SchedulerExecStats, SchedulerStats, config, new_tmp_dir};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::time::sleep;
use tracing::{Level, instrument};
use url::Url;

const PORT: u16 = 4434;

async fn run_client_and_server(
    server_args: (&str, &str),
    client_args: Vec<&str>,
    exp_stats: SchedulerExecStats,
    additional_tag: Option<(&str, Tag)>,
) {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .with_target(false)
        .with_writer(std::io::stderr)
        .try_init()
        .ok();
    // exit on panic in any thread
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));
    let cargo_workspace_dir = Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap())
        .parent()
        .unwrap()
        .to_path_buf();
    let act_stats = {
        let _change_dir = ChangeDir::new(&cargo_workspace_dir);
        let (server, _server_dir) =
            spawn_server(PathBuf::from(server_args.0), server_args.1.to_string());
        // give server some time to start
        sleep(Duration::from_millis(100)).await;
        let act_stats = run_client(client_args, additional_tag).await;
        kill_server(server).await;
        act_stats
    };
    assert_eq!(act_stats.exec, exp_stats);
}

#[instrument(skip_all)]
async fn run_client(args: Vec<&str>, additional_tag: Option<(&str, Tag)>) -> SchedulerStats {
    let mut razel = Razel::new();
    razel.clean();
    parse_cli(args.iter().map(|&x| x.into()).collect(), &mut razel)
        .await
        .unwrap()
        .unwrap();
    if let Some((name, tag)) = additional_tag {
        razel.add_tag_for_command(name, tag);
    }
    razel
        .run(
            false,
            true,
            "",
            None,
            vec![],
            None,
            vec![Url::parse(&format!("http://localhost:{PORT}")).unwrap()],
        )
        .await
        .unwrap()
}

fn spawn_server(config: PathBuf, name: String) -> (Child, TempDir) {
    let target_dir = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let exe_suffix = if cfg!(windows) { ".exe" } else { "" };
    let server_path = target_dir.join(format!("razel-server{exe_suffix}"));
    let tmp_dir = new_tmp_dir!();
    let child = Command::new(server_path)
        .arg("-c")
        .arg(config.canonicalize().unwrap())
        .arg("-n")
        .arg(name)
        .current_dir(tmp_dir.dir())
        .env("RUST_LOG", "debug")
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    (child, tmp_dir)
}

async fn kill_server(mut child: Child) {
    child.kill().await.unwrap();
    child.wait().await.unwrap();
}

const RAZEL_JSONL_EXP_SUCCEEDED: usize = 12;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn razel_jsonl() {
    run_client_and_server(
        ("razel-server/examples/localhost.toml", "localhost"),
        vec![config::EXECUTABLE, "exec", "-f", "examples/razel.jsonl"],
        SchedulerExecStats {
            succeeded: RAZEL_JSONL_EXP_SUCCEEDED,
            ..Default::default()
        },
        None,
    )
    .await;
}
