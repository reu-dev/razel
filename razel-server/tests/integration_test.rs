use razel::cli::parse_cli;
use razel::test_utils::ChangeDir;
use razel::types::Tag;
use razel::{config, Razel, SchedulerExecStats};
use razel_server::config::Config;
use razel_server::Server;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, instrument, Level};
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
    let _change_dir = ChangeDir::new(&cargo_workspace_dir);
    tokio::spawn(run_server(
        PathBuf::from(server_args.0),
        server_args.1.to_string(),
    ));
    // give server some time to start
    sleep(Duration::from_millis(100)).await;
    run_client(client_args, exp_stats, additional_tag).await;
}

#[instrument(skip_all)]
async fn run_client(
    args: Vec<&str>,
    exp_stats: SchedulerExecStats,
    additional_tag: Option<(&str, Tag)>,
) {
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
    assert_eq!(act_stats.exec, exp_stats);
}

#[instrument(skip_all)]
async fn run_server(config: PathBuf, name: String) {
    let config = Config::read(&config).unwrap();
    let server = Server::new(config, name).unwrap();
    server.run().await.unwrap();
    info!("stopped server");
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
