use anyhow::{Context, Result};
use clap::Parser;
use razel_bench::types::{Bench, CacheState, BENCHES_OUT_DIR};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::info;

const REMOTE_CACHE_CONTAINER_NAME: &str = "razel-bench-remote-cache";
const REMOTE_CACHE_GRPC_PORT: u16 = 9093;
const REMOTE_CACHE_STATUS_PORT: u16 = 8081;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// razel binary to benchmark
    #[clap(short, long, default_value = "razel")]
    bin: PathBuf,
    /// file to be executed by razel
    #[clap(short, long, default_value = "razel.jsonl")]
    file: PathBuf,
    /// benchmark title/name
    #[clap(short, long, default_value = "")]
    title: String,
    /// remote cache host name: ssh and run remote cache in podman
    #[clap(short = 'c', long)]
    remote_cache_host: Option<String>,
    /// number of runs
    #[clap(short, long, default_value = "3")]
    runs: usize,
}

#[derive(Clone)]
struct Config {
    title: String,
    bin: PathBuf,
    cwd: PathBuf,
    args: Vec<String>,
    remote_cache_host: Option<String>,
}

struct Bencher {}

impl Bencher {
    pub fn bench(config: &Config, cache_state: CacheState) -> Result<Bench> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let id = format!("{}.{:?}.{timestamp}", config.title, cache_state);
        let path = Path::new(BENCHES_OUT_DIR).join(format!("bench.{id}.summary.json"));
        let remote_cache_stats_before = Self::get_remote_cache_stats(config, &cache_state)?;
        let duration = Self::run(config, cache_state)?.as_secs_f32();
        let remote_cache_stats_after = Self::get_remote_cache_stats(config, &cache_state)?;
        let bench = Bench {
            id,
            path,
            title: config.title.clone(),
            cache_state,
            timestamp,
            duration,
            remote_cache_stats_before,
            remote_cache_stats_after,
        };
        Self::move_log_file(&config.cwd, &bench.log_file_path())?;
        bench.write()?;
        Ok(bench)
    }

    fn run(config: &Config, cache_state: CacheState) -> Result<Duration> {
        let mut args = config.args.clone();
        if let Some(host) = config
            .remote_cache_host
            .as_ref()
            .filter(|_| cache_state.is_remote_cache_used())
        {
            args.push(format!(
                "--remote-cache=grpc://{host}:{REMOTE_CACHE_GRPC_PORT}"
            ));
        }
        println!();
        info!(
            bin = config.bin.to_str().unwrap(),
            args = args.join(" "),
            "starting"
        );
        let execution_start = Instant::now();
        let status = Command::new(&config.bin)
            .current_dir(&config.cwd)
            .args(&args)
            .status()
            .unwrap();
        let duration = execution_start.elapsed();
        info!(
            bin = config.bin.to_str().unwrap(),
            args = args.join(" "),
            "finished: {}s",
            duration.as_secs_f32()
        );
        println!();
        assert!(status.success());
        Ok(duration)
    }

    fn move_log_file(cwd: &Path, target: &Path) -> Result<()> {
        fs::rename(
            cwd.join("razel-out")
                .join("razel-metadata")
                .join("log.json"),
            target,
        )?;
        Ok(())
    }

    fn get_remote_cache_stats(config: &Config, cache_state: &CacheState) -> Result<Option<Value>> {
        if let Some(host) = config
            .remote_cache_host
            .as_ref()
            .filter(|_| cache_state.is_remote_cache_used())
        {
            Ok(Some(get_remote_cache_stats(host).with_context(|| {
                format!("get_remote_cache_stats({host})")
            })?))
        } else {
            Ok(None)
        }
    }
}

fn start_remote_cache(host: &str) {
    let cmd = format!(
        "podman run -d  -p {REMOTE_CACHE_STATUS_PORT}:8080 -p {REMOTE_CACHE_GRPC_PORT}:9092 --name {REMOTE_CACHE_CONTAINER_NAME} docker.io/buchgr/bazel-remote-cache --max_size 100"
    );
    exec("ssh", &vec![host.to_owned(), cmd]);
    sleep(Duration::from_secs(2));
}

fn stop_remote_cache(host: &str) {
    let cmd = format!("podman rm --force --ignore {REMOTE_CACHE_CONTAINER_NAME}");
    exec("ssh", &vec![host.to_owned(), cmd]);
    sleep(Duration::from_secs(1));
}

fn get_remote_cache_stats(host: &str) -> Result<Value> {
    let url = format!("http://{host}:{REMOTE_CACHE_STATUS_PORT}/status");
    println!();
    info!("get_remote_cache_stats: {url}");
    let body = reqwest::blocking::get(url)?.text()?;
    let data = serde_json::from_str(&body)?;
    Ok(data)
}

fn exec(program: &str, args: &Vec<String>) {
    println!();
    info!("exec: {program} {}", args.join(" "));
    let status = Command::new(program).args(args).status().unwrap();
    assert!(status.success(), "{status:?}");
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    fs::create_dir_all(BENCHES_OUT_DIR)?;
    let file_abs = cli.file.canonicalize().unwrap();
    let workspace_dir = file_abs.parent().unwrap();
    let out_dir = workspace_dir.join("razel-out");
    let cache_dir = Path::new(BENCHES_OUT_DIR).join("tmp-cache");
    let config = Config {
        title: cli.title,
        bin: cli.bin,
        cwd: workspace_dir.to_path_buf(),
        args: [
            "exec",
            "-f",
            cli.file.to_str().unwrap(),
            "--cache-dir",
            cache_dir.to_str().unwrap(),
        ]
        .into_iter()
        .map(String::from)
        .collect(),
        remote_cache_host: cli.remote_cache_host,
    };
    for _ in 0..cli.runs {
        fs::remove_dir_all(&cache_dir).ok();
        fs::remove_dir_all(&out_dir).ok();
        Bencher::bench(&config, CacheState::LocalCold)?;
        Bencher::bench(&config, CacheState::LocalWarm)?;
        if let Some(host) = &config.remote_cache_host {
            fs::remove_dir_all(&cache_dir).ok();
            fs::remove_dir_all(&out_dir).ok();
            stop_remote_cache(host);
            start_remote_cache(host);
            Bencher::bench(&config, CacheState::LocalColdRemoteCold)?;
            fs::remove_dir_all(&cache_dir).unwrap();
            fs::remove_dir_all(&out_dir).unwrap();
            Bencher::bench(&config, CacheState::LocalColdRemoteWarm)?;
            stop_remote_cache(host);
        }
        fs::remove_dir_all(&cache_dir).ok();
    }
    Ok(())
}
