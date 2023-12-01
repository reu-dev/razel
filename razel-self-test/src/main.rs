use clap::Parser;
use std::process::ExitCode;
use std::thread::sleep;
use std::time::Duration;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Allocate memory [bytes]
    #[clap(short, long)]
    memory: Option<usize>,
    /// Keep running for some time [seconds]
    #[clap(short, long)]
    sleep: Option<f32>,
    #[clap(short, long, default_value_t = 0)]
    exit_code: u8,
}

fn main() -> ExitCode {
    let args = Args::parse();

    let mut _vec: Option<Vec<u8>> = args.memory.map(|x| vec![37; x]);

    if let Some(x) = args.sleep {
        sleep(Duration::from_secs_f32(x));
    }

    ExitCode::from(args.exit_code)
}
