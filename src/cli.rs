use std::error::Error;

use clap::{AppSettings, Args, Parser, Subcommand};

use crate::parse_jsonl::parse_jsonl_file;
use crate::{parse_batch_file, parse_command, tasks, Scheduler, TaskFn};

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
    /// Execute a single task
    #[clap(subcommand)]
    Task(CliTasks),
    /// Execute commands from a batch file
    Batch {
        /// file with commands to execute
        file: String,
    },
    /// Execute commands from a razel.jsonl file
    Build,
}

#[derive(Subcommand)]
enum CliTasks {
    /// Concatenate multiple csv files - headers must match
    CsvConcat(CsvConcatTaskArgs),
    /// Filter a csv file - keeping only the specified cols
    CsvFilter(CsvFilterTaskArgs),
    /// Write a text file
    Write(WriteTaskArgs),
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
struct CsvFilterTaskArgs {
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
struct WriteTaskArgs {
    /// file to create
    file: String,
    /// lines to write
    lines: Vec<String>,
}

#[derive(Args, Debug)]
struct TwoFilesArgs {
    file1: String,
    file2: String,
}

pub fn parse_cli(
    args: Vec<String>,
    scheduler: &mut Scheduler,
    name: Option<String>,
) -> Result<(), anyhow::Error> {
    let cli = Cli::try_parse_from(args.iter())?;
    match cli.command {
        CliCommands::Command { command } => parse_command(scheduler, command),
        CliCommands::Task(task) => match_task(scheduler, name.unwrap(), task, args),
        CliCommands::Batch { file } => parse_batch_file(scheduler, file),
        CliCommands::Build => parse_jsonl_file(scheduler, "razel.jsonl".into()),
    }
}

fn match_task(
    scheduler: &mut Scheduler,
    name: String,
    task: CliTasks,
    args: Vec<String>,
) -> Result<(), anyhow::Error> {
    let (inputs, outputs, f): (Vec<String>, Vec<String>, TaskFn) = match task {
        // TODO use Scheduler to register files and map paths
        CliTasks::CsvConcat(x) => (
            x.input.clone(),
            vec![x.output.clone()],
            Box::new(move || tasks::csv_concat(x.input.clone(), x.output.clone())),
        ),
        CliTasks::CsvFilter(x) => (
            vec![x.input.clone()],
            vec![x.output.clone()],
            Box::new(move || {
                tasks::csv_filter(
                    x.input.clone(),
                    x.output.clone(),
                    x.cols.clone(),
                    x.fields.clone(),
                )
            }),
        ),
        CliTasks::EnsureEqual(x) => (
            vec![x.file1.clone(), x.file2.clone()],
            vec![],
            Box::new(move || tasks::ensure_equal(x.file1.clone(), x.file2.clone())),
        ),
        CliTasks::EnsureNotEqual(x) => (
            vec![x.file1.clone(), x.file2.clone()],
            vec![],
            Box::new(move || tasks::ensure_not_equal(x.file1.clone(), x.file2.clone())),
        ),
        CliTasks::Write(x) => (
            vec![],
            vec![x.file.clone()],
            Box::new(move || tasks::write(x.file.clone(), x.lines.clone())),
        ),
    };
    scheduler.push_task(name, args, f, inputs, outputs)
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
