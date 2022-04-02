use std::error::Error;
use std::ffi::OsString;

use clap::{AppSettings, Args, Parser, Subcommand};

use crate::{parse_batch_file, parse_command, Scheduler, tasks};

#[derive(Parser)]
#[clap(name = "razel")]
#[clap(author, version, about, long_about = None)]
#[clap(arg_required_else_help(true))]
#[clap(global_setting(AppSettings::DeriveDisplayOrder))]
struct Cli {
    #[clap(subcommand)]
    command: CliCommands,
}

#[derive(Subcommand)]
enum CliCommands {
    /// Execute a custom command
    Command {
        #[clap(last = true, required = true)]
        command: Vec<String>,
    },
    /// Execute commands from a batch file
    Batch {
        /// file with commands to execute
        file: String,
    },
    /// Execute a single task
    #[clap(subcommand)]
    Task(CliTasks),
}

#[derive(Subcommand)]
enum CliTasks {
    /// Concatenate multiple csv files - headers must match
    CsvConcat(CsvConcatTaskArgs),
    /// Filter a csv file - keeping only the specified cols
    CsvFilter(CsvFilterArgs),
    /// Write a text file
    Write {
        /// file to create
        file: String,
        /// lines to write
        lines: Vec<String>,
    },
    /// Ensure that two files are equal
    EnsureEqual(TwoFilesArgs),
    /// Ensure that two files are not equal
    EnsureNotEqual(TwoFilesArgs),
}

#[derive(Args, Debug)]
struct CsvConcatTaskArgs {
    /// input csv files
    #[clap(required = true)]
    input: Vec<String>,
    /// concatenated file to create
    output: String,
}

#[derive(Args, Debug)]
struct CsvFilterArgs {
    #[clap(short, long)]
    input: String,
    #[clap(short, long)]
    output: String,
    /// Col names to keep - all other cols are dropped
    #[clap(short, long = "col", multiple_values(true))]
    cols: Vec<String>,
    /// Fields to keep: Field=Value
    #[clap(short, long = "field", parse(try_from_str = parse_key_val), multiple_occurrences(true), multiple_values(true))]
    fields: Vec<(String, String)>,
}

#[derive(Args, Debug)]
struct TwoFilesArgs {
    file1: String,
    file2: String,
}

pub async fn parse_cli<I, T>(scheduler: &mut Scheduler, itr: I) -> Result<(), anyhow::Error>
    where
        I: IntoIterator<Item=T>,
        T: Into<OsString> + Clone {
    let cli = Cli::try_parse_from(itr)?;
    match cli.command {
        CliCommands::Command { command } => {
            parse_command(scheduler, command)
        }
        CliCommands::Batch { file } => {
            parse_batch_file(scheduler, file)
        }
        CliCommands::Task(task) => match task {
            CliTasks::CsvConcat(x) => {
                tasks::csv_concat(x.input, x.output).await
            }
            CliTasks::CsvFilter(x) => {
                tasks::csv_filter(x.input, x.output, x.cols, x.fields).await
            }
            CliTasks::EnsureEqual(_x) => {
                todo!() //tasks::ensure_equal(x.file1, x.file2)
            }
            CliTasks::EnsureNotEqual(_x) => {
                todo!() //tasks::ensure_not_equal(x.file1, x.file2)
            }
            CliTasks::Write { file, lines } => {
                tasks::write(file, lines).await
            }
        },
    }
}

/// Parse a single key-value pair
fn parse_key_val<T, U>(s: &str) -> Result<(T, U), Box<dyn Error + Send + Sync + 'static>>
    where
        T: std::str::FromStr,
        T::Err: Error + Send + Sync + 'static,
        U: std::str::FromStr,
        U::Err: Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{}`", s))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}
