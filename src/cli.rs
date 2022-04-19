use std::error::Error;
use std::ffi::OsString;

use clap::{AppSettings, Args, Parser, Subcommand};

use crate::parse_jsonl::parse_jsonl_file;
use crate::{parse_batch_file, parse_command, tasks, Command, Scheduler};

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

pub fn parse_cli<I, T>(
    scheduler: &mut Scheduler,
    itr: I,
    name: Option<String>,
) -> Result<(), anyhow::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = Cli::try_parse_from(itr)?;
    match cli.command {
        CliCommands::Command { command } => parse_command(scheduler, command),
        CliCommands::Task(task) => match task {
            CliTasks::CsvConcat(x) => new_csv_concat_task(scheduler, name.unwrap(), x),
            CliTasks::CsvFilter(x) => new_csv_filter_task(scheduler, name.unwrap(), x),
            CliTasks::EnsureEqual(_x) => {
                todo!() //tasks::ensure_equal(x.file1, x.file2)
            }
            CliTasks::EnsureNotEqual(_x) => {
                todo!() //tasks::ensure_not_equal(x.file1, x.file2)
            }
            CliTasks::Write(x) => new_write_task(scheduler, name.unwrap(), x),
        },
        CliCommands::Batch { file } => parse_batch_file(scheduler, file),
        CliCommands::Build => parse_jsonl_file(scheduler, "razel.jsonl".into()),
    }
}

fn new_csv_concat_task(
    scheduler: &mut Scheduler,
    name: String,
    args: CsvConcatTaskArgs,
) -> Result<(), anyhow::Error> {
    let inputs = args.input.clone();
    let outputs = vec![args.output.clone()];
    let command = Command::new_task(
        name,
        Box::new(move || tasks::csv_concat(args.input.clone(), args.output.clone())),
        inputs,
        outputs,
    );
    scheduler.push(Box::new(command));
    Ok(())
}

fn new_csv_filter_task(
    scheduler: &mut Scheduler,
    name: String,
    args: CsvFilterTaskArgs,
) -> Result<(), anyhow::Error> {
    let inputs = vec![args.input.clone()];
    let outputs = vec![args.output.clone()];
    let command = Command::new_task(
        name,
        Box::new(move || {
            tasks::csv_filter(
                args.input.clone(),
                args.output.clone(),
                args.cols.clone(),
                args.fields.clone(),
            )
        }),
        inputs,
        outputs,
    );
    scheduler.push(Box::new(command));
    Ok(())
}

fn new_write_task(
    scheduler: &mut Scheduler,
    name: String,
    args: WriteTaskArgs,
) -> Result<(), anyhow::Error> {
    let inputs = vec![];
    let outputs = vec![args.file.clone()];
    let command = Command::new_task(
        name,
        Box::new(move || tasks::write(args.file.clone(), args.lines.clone())),
        inputs,
        outputs,
    );
    scheduler.push(Box::new(command));
    Ok(())
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
