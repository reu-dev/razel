use anyhow::Result;
use clap::Parser;
use razel::remote_exec::{rpc_recv_message, rpc_send_message, CreateJobResponse, Message};
use razel_server::rpc_utils_server::new_server_endpoint;
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing::{error, info, info_span, Instrument};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
struct Cli {
    /// Address to listen on
    #[clap(short, long, default_value = "127.0.0.1:4433")]
    listen: SocketAddr,
    /// TLS certificate in PEM format
    #[clap(long, requires = "key")]
    cert: Option<PathBuf>,
    /// TLS private key in PEM format
    #[clap(long, requires = "cert")]
    key: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(
                EnvFilter::builder()
                    .with_default_directive(LevelFilter::INFO.into())
                    .from_env_lossy(),
            )
            .finish(),
    )
    .unwrap();

    // exit on panic in any thread
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    let cli = Cli::parse();
    run(cli).await
}

async fn run(cli: Cli) -> Result<()> {
    let endpoint = new_server_endpoint(cli.listen, cli.cert, cli.key)?;
    info!("listening on {}", endpoint.local_addr()?);
    while let Some(conn) = endpoint.accept().await {
        if !conn.remote_address_validated() {
            info!("requiring connection to validate its address");
            conn.retry().unwrap();
        } else {
            info!("accepting connection");
            let fut = handle_connection(conn);
            tokio::spawn(async move {
                if let Err(e) = fut.await {
                    error!("connection failed: {e}");
                }
            });
        }
    }
    Ok(())
}

async fn handle_connection(conn: quinn::Incoming) -> Result<()> {
    let connection = conn.await?;
    let span = info_span!(
        "connection",
        remote = %connection.remote_address(),
    );
    async {
        info!("established");

        // Each stream initiated by the client constitutes a new request.
        loop {
            let stream = connection.accept_bi().await;
            let stream = match stream {
                Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
                    info!("connection closed");
                    return Ok(());
                }
                Err(e) => {
                    return Err(e);
                }
                Ok(s) => s,
            };
            let fut = handle_request(stream);
            tokio::spawn(
                async move {
                    if let Err(e) = fut.await {
                        error!("failed: {reason}", reason = e.to_string());
                    }
                }
                .instrument(info_span!("request")),
            );
        }
    }
    .instrument(span)
    .await?;
    Ok(())
}

async fn handle_request(
    (mut send, mut recv): (quinn::SendStream, quinn::RecvStream),
) -> Result<()> {
    let request: Message = rpc_recv_message(&mut recv).await?;
    match request {
        Message::CreateJobRequest(_) => {
            tracing::info!("CreateJobRequest");
            rpc_send_message(
                &mut send,
                &Message::CreateJobResponse(CreateJobResponse {
                    id: "1".into(),
                    url: "TODO".into(),
                }),
            )
            .await
            .unwrap();
        }
        Message::CreateJobResponse(_) => unreachable!("CreateJobResponse"),
        Message::ExecuteTargetsRequest(_) => {}
        Message::ExecuteTargetResult(_) => {}
        Message::UploadFilesRequest(_) => {}
    }
    // Gracefully terminate the stream
    send.finish().unwrap();
    info!("complete");
    Ok(())
}

#[cfg(test)]
mod main {
    use super::*;
    use razel::cli::parse_cli;
    use razel::test_utils::ChangeDir;
    use razel::types::Tag;
    use razel::{config, Razel, SchedulerExecStats};

    use std::path::Path;
    use std::str::FromStr;
    use url::Url;

    const PORT: u16 = 4433;

    /// For simplification all tests use the same binary directory and therefore need to be run in serial
    async fn test_main(
        args: Vec<&str>,
        exp_stats: SchedulerExecStats,
        additional_tag: Option<(&str, Tag)>,
    ) {
        tracing_subscriber::fmt().try_init().ok();
        // exit on panic in any thread
        let default_panic = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            default_panic(info);
            std::process::exit(1);
        }));
        tokio::spawn(run_server());
        run_client(args, exp_stats, additional_tag).await;
    }

    async fn run_client(
        args: Vec<&str>,
        exp_stats: SchedulerExecStats,
        additional_tag: Option<(&str, Tag)>,
    ) {
        let cargo_workspace_dir = Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap())
            .parent()
            .unwrap()
            .to_path_buf();
        let _change_dir = ChangeDir::new(&cargo_workspace_dir);
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

    async fn run_server() {
        run(Cli {
            listen: SocketAddr::from_str(&format!("127.0.0.1:{PORT}")).unwrap(),
            cert: None,
            key: None,
        })
        .await
        .unwrap();
    }

    const RAZEL_JSONL_EXP_SUCCEEDED: usize = 12;

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn exec_razel_jsonl() {
        test_main(
            vec![config::EXECUTABLE, "exec", "-f", "examples/razel.jsonl"],
            SchedulerExecStats {
                succeeded: RAZEL_JSONL_EXP_SUCCEEDED,
                ..Default::default()
            },
            None,
        )
        .await;
    }
}
