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

    let mut vec: Vec<u8> = vec![];
    if let Some(x) = args.memory {
        println!("allocate {x} bytes...");
        vec.resize(x, x as u8);
    }

    if let Some(x) = args.sleep {
        println!("sleep for {x}s...");
        sleep(Duration::from_secs_f32(x));
    }

    if args.exit_code != 0 {
        eprintln!("exit with code {}", args.exit_code);
    }
    ExitCode::from(args.exit_code)
}
