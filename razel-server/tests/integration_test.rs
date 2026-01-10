use razel::cli::parse_cli;
use razel::test_utils::{ChangeDir, TempDir, setup_tracing};
use razel::types::Tag;
use razel::{Razel, SchedulerExecStats, config, new_tmp_dir};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::time::sleep;
use tracing::{info, instrument};
use url::Url;

const PORT: u16 = 4434;

fn prepare() -> ChangeDir {
    setup_tracing();
    // exit on panic in any thread
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));
    ChangeDir::new(&cargo_workspace_dir())
}

fn cargo_workspace_dir() -> PathBuf {
    Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap())
        .parent()
        .unwrap()
        .into()
}

#[instrument(skip_all)]
async fn run_client(
    args: &Vec<&str>,
    additional_tag: &Option<(&str, Tag)>,
    exp_stats: &SchedulerExecStats,
    exp_cache_hits: usize,
) {
    let mut razel = Razel::new();
    razel.clean();
    parse_cli(args.iter().map(|&x| x.into()).collect(), &mut razel)
        .await
        .unwrap()
        .unwrap();
    if let Some((name, tag)) = additional_tag {
        razel.add_tag_for_command(name, tag.clone());
    }
    let act_stats = razel
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
        .unwrap();
    assert_eq!(act_stats.exec, *exp_stats);
    assert_eq!(act_stats.cache_hits, exp_cache_hits);
}

async fn run_client_twice(
    args: Vec<&str>,
    additional_tag: Option<(&str, Tag)>,
    exp_stats: SchedulerExecStats,
    exp_cache_hits: usize,
) {
    println!("\nrun client with cold remote executor\n");
    run_client(&args, &additional_tag, &exp_stats, 0).await;
    println!("\nrun client with warm remote executor\n");
    run_client(&args, &additional_tag, &exp_stats, exp_cache_hits).await;
}

async fn spawn_server(config: &str, name: &str) -> (Child, TempDir) {
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
        .arg(PathBuf::from(config).canonicalize().unwrap())
        .arg("-n")
        .arg(name)
        .current_dir(tmp_dir.dir())
        .env("RUST_LOG", "debug,cranelift=info,wasmtime=info")
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    // give server some time to start
    sleep(Duration::from_millis(100)).await;
    (child, tmp_dir)
}

async fn kill_server(mut child: Child) {
    child.kill().await.unwrap();
    child.wait().await.unwrap();
}

const RAZEL_JSONL_EXP_SUCCEEDED: usize = 12;
const RAZEL_JSONL_EXP_CACHED: usize = 10;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn razel_jsonl() {
    let _change_dir = prepare();
    let (server, _server_dir) =
        spawn_server("razel-server/examples/localhost.toml", "localhost").await;
    run_client_twice(
        vec![config::EXECUTABLE, "exec", "-f", "examples/razel.jsonl"],
        None,
        SchedulerExecStats {
            succeeded: RAZEL_JSONL_EXP_SUCCEEDED,
            ..Default::default()
        },
        RAZEL_JSONL_EXP_CACHED,
    )
    .await;
    kill_server(server).await;
}
