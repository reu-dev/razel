use anyhow::Result;
use clap::Parser;
use razel::cache::LocalCache;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::info;

const BENCHES_OUT_DIR: &str = "benches";
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

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
#[allow(clippy::enum_variant_names)]
enum CacheState {
    /// Local execution from scratch
    LocalCold,
    /// Local execution zero-check
    LocalWarm,
    /// CI execution from scratch
    LocalColdRemoteCold,
    /// CI execution on new node
    LocalColdRemoteWarm,
}

impl CacheState {
    fn is_remote_cache_used(&self) -> bool {
        match self {
            CacheState::LocalCold | CacheState::LocalWarm => false,
            CacheState::LocalColdRemoteCold | CacheState::LocalColdRemoteWarm => true,
        }
    }
}

#[derive(Clone)]
struct Config {
    title: String,
    bin: PathBuf,
    cwd: PathBuf,
    args: Vec<String>,
    remote_cache_host: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Bench {
    id: String,
    title: String,
    cache_state: CacheState,
    timestamp: u128,
    duration: f32,
    remote_cache_stats_before: Option<Value>,
    remote_cache_stats_after: Option<Value>,
}

impl Bench {
    fn new(config: &Config, cache_state: CacheState) -> Result<Self> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let id = format!("{}.{:?}.{timestamp}", config.title, cache_state);
        let remote_cache_stats_before = Self::get_remote_cache_stats(config, &cache_state)?;
        let duration = Self::run(config, cache_state)?.as_secs_f32();
        Self::move_log_file(&config.cwd, &id)?;
        let remote_cache_stats_after = Self::get_remote_cache_stats(config, &cache_state)?;
        Ok(Self {
            id,
            title: config.title.clone(),
            cache_state,
            timestamp,
            duration,
            remote_cache_stats_before,
            remote_cache_stats_after,
        })
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
        assert!(status.success());
        Ok(duration)
    }

    fn move_log_file(cwd: &Path, id: &str) -> Result<()> {
        fs::rename(
            cwd.join("razel-out")
                .join("razel-metadata")
                .join("log.json"),
            Path::new(BENCHES_OUT_DIR).join(format!("log.{id}.json")),
        )?;
        Ok(())
    }

    fn get_remote_cache_stats(config: &Config, cache_state: &CacheState) -> Result<Option<Value>> {
        if let Some(host) = config
            .remote_cache_host
            .as_ref()
            .filter(|_| cache_state.is_remote_cache_used())
        {
            Ok(Some(get_remote_cache_stats(host)?))
        } else {
            Ok(None)
        }
    }
}

fn start_remote_cache(host: &str) {
    let cmd = format!(
        "podman run -d  -p {REMOTE_CACHE_STATUS_PORT}:8080 -p {REMOTE_CACHE_GRPC_PORT}:9092 --name {REMOTE_CACHE_CONTAINER_NAME} buchgr/bazel-remote-cache --max_size 10"
    );
    let status = Command::new("ssh").args([host, &cmd]).status().unwrap();
    assert!(status.success());
    sleep(Duration::from_secs(1));
}

fn stop_remote_cache(host: &str) {
    let cmd = format!("podman rm --force --ignore {REMOTE_CACHE_CONTAINER_NAME}");
    let status = Command::new("ssh").args([host, &cmd]).status().unwrap();
    assert!(status.success());
}

fn get_remote_cache_stats(host: &str) -> Result<Value> {
    let url = format!("http://{host}:{REMOTE_CACHE_STATUS_PORT}/status");
    let body = reqwest::blocking::get(url)?.text()?;
    let data = serde_json::from_str(&body)?;
    Ok(data)
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    fs::create_dir_all(BENCHES_OUT_DIR)?;
    let workspace_dir = cli.file.parent().unwrap();
    let out_dir = workspace_dir.join("razel-out");
    let cache_dir = LocalCache::new(workspace_dir).unwrap().dir;
    let config = Config {
        title: cli.title,
        bin: cli.bin,
        cwd: workspace_dir.to_path_buf(),
        args: ["exec", "-f", cli.file.to_str().unwrap()]
            .into_iter()
            .map(String::from)
            .collect(),
        remote_cache_host: cli.remote_cache_host,
    };
    let benches_path = Path::new(BENCHES_OUT_DIR).join("benches.json");
    let mut benches: Vec<Bench> = if benches_path.is_file() {
        serde_json::from_reader(BufReader::new(File::open(&benches_path)?))?
    } else {
        vec![]
    };
    for _ in 0..cli.runs {
        fs::remove_dir_all(&cache_dir).unwrap();
        fs::remove_dir_all(&out_dir).ok();
        benches.push(Bench::new(&config, CacheState::LocalCold)?);
        benches.push(Bench::new(&config, CacheState::LocalWarm)?);
        if let Some(host) = &config.remote_cache_host {
            fs::remove_dir_all(&cache_dir).unwrap();
            fs::remove_dir_all(&out_dir).unwrap();
            stop_remote_cache(host);
            start_remote_cache(host);
            benches.push(Bench::new(&config, CacheState::LocalColdRemoteCold)?);
            fs::remove_dir_all(&cache_dir).unwrap();
            fs::remove_dir_all(&out_dir).unwrap();
            benches.push(Bench::new(&config, CacheState::LocalColdRemoteWarm)?);
            stop_remote_cache(host);
        }
    }
    fs::write(
        benches_path,
        serde_json::to_string_pretty(&benches).unwrap(),
    )?;
    Ok(())
}
