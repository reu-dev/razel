use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;

use clap::{AppSettings, Args, Parser, Subcommand};

use crate::parse_jsonl::parse_jsonl_file;
use crate::{parse_batch_file, parse_command, tasks, CommandBuilder, Razel};

#[derive(Parser)]
#[clap(name = "razel")]
#[clap(author, version, about, long_about = None)]
#[clap(global_setting(AppSettings::DeriveDisplayOrder))]
#[clap(infer_subcommands = true)]
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
    /// Execute commands from a razel.jsonl or batch file
    #[clap(visible_alias = "build", visible_alias = "test")]
    Exec(Exec),
    /// List commands from a razel.jsonl or batch file
    #[clap(visible_alias = "ls", visible_alias = "show-only")]
    ListCommands {
        /// file with commands to list
        #[clap(short, long, default_value = "razel.jsonl")]
        file: String,
    },
    // TODO add Debug subcommand
    /// Show info about configuration, cache, ...
    Info,
    // TODO add upgrade subcommand
}

#[derive(Args, Debug)]
struct Exec {
    /// file with commands to execute
    #[clap(short, long, default_value = "razel.jsonl")]
    file: String,
}

#[derive(Subcommand)]
enum CliTasks {
    /// Concatenate multiple csv files - headers must match
    CsvConcat(CsvConcatTask),
    /// Filter a csv file - keeping only the specified cols
    CsvFilter(CsvFilterTask),
    /// Write a text file
    WriteFile(WriteFileTask),
    /// Ensure that two files are equal
    EnsureEqual(EnsureEqualTask),
    /// Ensure that two files are not equal
    EnsureNotEqual(EnsureNotEqualTask),
}

#[derive(Args, Debug)]
struct CsvConcatTask {
    /// input csv files
    #[clap(required = true)]
    input: Vec<String>,
    /// concatenated file to create
    output: String,
}

impl CsvConcatTask {
    fn build(self, builder: &mut CommandBuilder, razel: &mut Razel) -> Result<(), anyhow::Error> {
        let inputs = builder.inputs(&self.input, razel)?;
        let output = builder.output(&self.output, razel)?;
        builder.task_executor(Arc::new(move || {
            tasks::csv_concat(inputs.clone(), output.clone())
        }));
        Ok(())
    }
}

#[derive(Args, Debug)]
struct CsvFilterTask {
    #[clap(short, long)]
    input: String,
    #[clap(short, long)]
    output: String,
    /// Col names to keep - all other cols are dropped
    #[clap(short, long = "col", multiple_values(true))]
    cols: Vec<String>,
}

impl CsvFilterTask {
    fn build(self, builder: &mut CommandBuilder, razel: &mut Razel) -> Result<(), anyhow::Error> {
        let input = builder.input(&self.input, razel)?;
        let output = builder.output(&self.output, razel)?;
        builder.task_executor(Arc::new(move || {
            tasks::csv_filter(input.clone(), output.clone(), self.cols.clone())
        }));
        Ok(())
    }
}

#[derive(Args, Debug)]
struct WriteFileTask {
    /// file to create
    file: String,
    /// lines to write
    lines: Vec<String>,
}

impl WriteFileTask {
    fn build(self, builder: &mut CommandBuilder, razel: &mut Razel) -> Result<(), anyhow::Error> {
        let output = builder.output(&self.file, razel)?;
        builder.task_executor(Arc::new(move || {
            tasks::write_file(output.clone(), self.lines.clone())
        }));
        Ok(())
    }
}

#[derive(Args, Debug)]
struct EnsureEqualTask {
    file1: String,
    file2: String,
}

impl EnsureEqualTask {
    fn build(self, builder: &mut CommandBuilder, razel: &mut Razel) -> Result<(), anyhow::Error> {
        let file1 = builder.input(&self.file1, razel)?;
        let file2 = builder.input(&self.file2, razel)?;
        builder.task_executor(Arc::new(move || {
            tasks::ensure_equal(file1.clone(), file2.clone())
        }));
        Ok(())
    }
}

#[derive(Args, Debug)]
struct EnsureNotEqualTask {
    file1: String,
    file2: String,
}

impl EnsureNotEqualTask {
    fn build(self, builder: &mut CommandBuilder, razel: &mut Razel) -> Result<(), anyhow::Error> {
        let file1 = builder.input(&self.file1, razel)?;
        let file2 = builder.input(&self.file2, razel)?;
        builder.task_executor(Arc::new(move || {
            tasks::ensure_not_equal(file1.clone(), file2.clone())
        }));
        Ok(())
    }
}

pub fn parse_cli(
    args: Vec<String>,
    razel: &mut Razel,
    name: Option<String>,
) -> Result<(), anyhow::Error> {
    let cli = Cli::try_parse_from(args.iter())?;
    match cli.command {
        CliCommands::Command { command } => parse_command(razel, command),
        CliCommands::Task(task) => match_task(razel, name.unwrap(), task, args),
        CliCommands::Exec(exec) => apply_file(razel, &exec.file),
        CliCommands::ListCommands { file } => {
            apply_file(razel, &file)?;
            razel.list_commands();
            std::process::exit(0);
        }
        CliCommands::Info => {
            razel.show_info();
            std::process::exit(0);
        }
    }
}

fn apply_file(razel: &mut Razel, file: &String) -> Result<(), anyhow::Error> {
    match Path::new(file).extension().and_then(OsStr::to_str) {
        Some("jsonl") => parse_jsonl_file(razel, file),
        _ => parse_batch_file(razel, file),
    }
}

fn match_task(
    razel: &mut Razel,
    name: String,
    task: CliTasks,
    args: Vec<String>,
) -> Result<(), anyhow::Error> {
    let mut builder = CommandBuilder::new(name, args);
    match task {
        CliTasks::CsvConcat(x) => x.build(&mut builder, razel),
        CliTasks::CsvFilter(x) => x.build(&mut builder, razel),
        CliTasks::EnsureEqual(x) => x.build(&mut builder, razel),
        CliTasks::EnsureNotEqual(x) => x.build(&mut builder, razel),
        CliTasks::WriteFile(x) => x.build(&mut builder, razel),
    }?;
    razel.push(builder)?;
    Ok(())
}
