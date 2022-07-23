use clap::{AppSettings, Parser};
use std::process::ExitCode;
use std::thread::sleep;
use std::time::Duration;

#[derive(Parser)]
#[clap(name = "razel-test")]
#[clap(author, version, about, long_about = None)]
#[clap(global_setting(AppSettings::DeriveDisplayOrder))]
struct Args {
    /// Allocate memory [bytes]
    #[clap(short, long)]
    memory: Option<usize>,
    /// Keep running for some time [seconds]
    #[clap(short, long)]
    sleep: Option<u64>,
    #[clap(short, long, default_value_t = 0)]
    exit_code: u8,
}

fn main() -> ExitCode {
    let args = Args::parse();

    if let Some(x) = args.memory {
        let mut vec: Vec<u8> = Vec::new();
        vec.resize(x, 0);
    }

    if let Some(x) = args.sleep {
        sleep(Duration::from_secs(x));
    }

    ExitCode::from(args.exit_code)
}
