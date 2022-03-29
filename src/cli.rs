use std::ffi::OsString;

use clap::{AppSettings, Args, Parser, Subcommand};

use crate::{parse_batch_file, parse_command, Scheduler, tasks};

pub fn parse_cli<I, T>(scheduler: &mut Scheduler, itr: I) -> Result<(), anyhow::Error>
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
                todo!() //tasks::csv_concat(x.input, x.output)
            }
            CliTasks::EnsureEqual(x) => {
                todo!() //tasks::ensure_equal(x.file1, x.file2)
            }
            CliTasks::EnsureNotEqual(x) => {
                todo!() //tasks::ensure_not_equal(x.file1, x.file2)
            }
            CliTasks::Write { file, lines } => {
                todo!() //tasks::write(file, lines)
            }
        },
    }
}

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
struct TwoFilesArgs {
    file1: String,
    file2: String,
}
