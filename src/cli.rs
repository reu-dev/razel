use anyhow::bail;
use clap::{Args, Parser, Subcommand};
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;

use crate::metadata::Tag;
use crate::parse_jsonl::parse_jsonl_file;
use crate::tasks::DownloadFileTask;
use crate::{parse_batch_file, parse_command, tasks, CommandBuilder, FileType, Razel};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
#[clap(infer_subcommands = true)]
struct Cli {
    #[clap(subcommand)]
    command: CliCommands,
}

#[derive(Subcommand, Debug)]
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
        /// File with commands to list
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
    /// File with commands to execute
    #[clap(short, long, default_value = "razel.jsonl")]
    file: String,
    #[clap(flatten)]
    run_args: RunArgs,
}

#[derive(Args, Debug)]
pub struct RunArgs {
    /// No execution, just list commands
    #[clap(short, long, visible_alias = "ls")]
    pub no_execution: bool,
    /// Do not stop on first failure
    #[clap(short, long, visible_alias = "keep-running")]
    pub keep_going: bool,
    /// Show verbose output
    #[clap(short, long)]
    pub verbose: bool,
    /// Prefix of tags to group the report
    #[clap(long, default_value = "group")]
    pub group_by_tag: String,
    /// Comma seperated list of remote cache URLs
    #[clap(long, env = "RAZEL_REMOTE_CACHE", value_delimiter = ',')]
    pub remote_cache: Vec<String>,
    /// Only cache commands with: output size / exec time < threshold [kilobyte / s]
    #[clap(long, env = "RAZEL_REMOTE_CACHE_THRESHOLD")]
    pub remote_cache_threshold: Option<u32>,
}

impl Default for RunArgs {
    fn default() -> Self {
        Self {
            no_execution: false,
            keep_going: false,
            verbose: true,
            group_by_tag: "group".to_string(),
            remote_cache: vec![],
            remote_cache_threshold: None,
        }
    }
}

#[derive(Subcommand, Debug)]
enum CliTasks {
    /// Write a value captured with a regex to a file
    CaptureRegex(CaptureRegexTask),
    /// Concatenate multiple csv files - headers must match
    CsvConcat(CsvConcatTask),
    /// Filter a csv file - keeping only the specified cols
    CsvFilter(CsvFilterTask),
    /// Write a text file
    WriteFile(WriteFileTask),
    /// Download a file
    DownloadFile(DownloadFileTaskBuilder),
    /// Ensure that two files are equal
    EnsureEqual(EnsureEqualTask),
    /// Ensure that two files are not equal
    EnsureNotEqual(EnsureNotEqualTask),
}

impl CliTasks {
    pub fn build_command(
        self,
        razel: &mut Razel,
        name: String,
        args: Vec<String>,
        tags: Vec<Tag>,
    ) -> Result<(), anyhow::Error> {
        let mut builder = CommandBuilder::new(name, args, tags);
        match self {
            CliTasks::CaptureRegex(x) => x.build(&mut builder, razel),
            CliTasks::CsvConcat(x) => x.build(&mut builder, razel),
            CliTasks::CsvFilter(x) => x.build(&mut builder, razel),
            CliTasks::WriteFile(x) => x.build(&mut builder, razel),
            CliTasks::DownloadFile(x) => x.build(&mut builder, razel),
            CliTasks::EnsureEqual(x) => x.build(&mut builder, razel),
            CliTasks::EnsureNotEqual(x) => x.build(&mut builder, razel),
        }?;
        razel.push(builder)?;
        Ok(())
    }
}

trait TaskBuilder {
    fn build(self, builder: &mut CommandBuilder, razel: &mut Razel) -> Result<(), anyhow::Error>;
}

#[derive(Args, Debug)]
struct CaptureRegexTask {
    /// Input file to read
    input: String,
    /// File to write the captured value to
    output: String,
    /// Regex containing a single capturing group
    regex: String,
}

impl TaskBuilder for CaptureRegexTask {
    fn build(self, builder: &mut CommandBuilder, razel: &mut Razel) -> Result<(), anyhow::Error> {
        let input = builder.input(&self.input, razel)?;
        let output = builder.output(&self.output, FileType::OutputFile, razel)?;
        builder.blocking_task_executor(Arc::new(move || {
            tasks::capture_regex(input.clone(), output.clone(), self.regex.clone())
        }));
        Ok(())
    }
}

#[derive(Args, Debug)]
struct CsvConcatTask {
    /// Input csv files
    #[clap(required = true)]
    input: Vec<String>,
    /// Concatenated file to create
    output: String,
}

impl TaskBuilder for CsvConcatTask {
    fn build(self, builder: &mut CommandBuilder, razel: &mut Razel) -> Result<(), anyhow::Error> {
        let inputs = builder.inputs(&self.input, razel)?;
        let output = builder.output(&self.output, FileType::OutputFile, razel)?;
        builder.blocking_task_executor(Arc::new(move || {
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
    #[clap(short, long = "col", num_args = 0..)]
    cols: Vec<String>,
}

impl TaskBuilder for CsvFilterTask {
    fn build(self, builder: &mut CommandBuilder, razel: &mut Razel) -> Result<(), anyhow::Error> {
        let input = builder.input(&self.input, razel)?;
        let output = builder.output(&self.output, FileType::OutputFile, razel)?;
        builder.blocking_task_executor(Arc::new(move || {
            tasks::csv_filter(input.clone(), output.clone(), self.cols.clone())
        }));
        Ok(())
    }
}

#[derive(Args, Debug)]
struct WriteFileTask {
    /// File to create
    file: String,
    /// Lines to write
    lines: Vec<String>,
}

impl TaskBuilder for WriteFileTask {
    fn build(self, builder: &mut CommandBuilder, razel: &mut Razel) -> Result<(), anyhow::Error> {
        let output = builder.output(&self.file, FileType::OutputFile, razel)?;
        builder.blocking_task_executor(Arc::new(move || {
            tasks::write_file(output.clone(), self.lines.clone())
        }));
        Ok(())
    }
}

#[derive(Args, Debug)]
struct DownloadFileTaskBuilder {
    #[clap(short, long)]
    url: String,
    #[clap(short, long)]
    output: String,
    #[clap(short, long)]
    executable: bool,
}

impl TaskBuilder for DownloadFileTaskBuilder {
    fn build(self, builder: &mut CommandBuilder, razel: &mut Razel) -> Result<(), anyhow::Error> {
        let file_type = if self.executable {
            FileType::ExecutableInWorkspace
        } else {
            FileType::OutputFile
        };
        let output = builder.output(&self.output, file_type, razel)?;
        builder.async_task_executor(DownloadFileTask {
            url: self.url,
            output,
            executable: self.executable,
        });
        Ok(())
    }
}

#[derive(Args, Debug)]
struct EnsureEqualTask {
    file1: String,
    file2: String,
}

impl TaskBuilder for EnsureEqualTask {
    fn build(self, builder: &mut CommandBuilder, razel: &mut Razel) -> Result<(), anyhow::Error> {
        let file1 = builder.input(&self.file1, razel)?;
        let file2 = builder.input(&self.file2, razel)?;
        builder.blocking_task_executor(Arc::new(move || {
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

impl TaskBuilder for EnsureNotEqualTask {
    fn build(self, builder: &mut CommandBuilder, razel: &mut Razel) -> Result<(), anyhow::Error> {
        let file1 = builder.input(&self.file1, razel)?;
        let file2 = builder.input(&self.file2, razel)?;
        builder.blocking_task_executor(Arc::new(move || {
            tasks::ensure_not_equal(file1.clone(), file2.clone())
        }));
        Ok(())
    }
}

pub fn parse_cli(args: Vec<String>, razel: &mut Razel) -> Result<Option<RunArgs>, anyhow::Error> {
    let cli = Cli::parse_from(args.iter());
    Ok(match cli.command {
        CliCommands::Command { command } => {
            parse_command(razel, command)?;
            Some(Default::default())
        }
        CliCommands::Task(task) => {
            task.build_command(razel, "task".to_string(), args, vec![])?;
            Some(Default::default())
        }
        CliCommands::Exec(exec) => {
            apply_file(razel, &exec.file)?;
            Some(exec.run_args)
        }
        CliCommands::ListCommands { file } => {
            apply_file(razel, &file)?;
            Some(RunArgs {
                no_execution: true,
                ..Default::default()
            })
        }
        CliCommands::Info => {
            razel.show_info();
            None
        }
    })
}

pub fn parse_cli_within_file(
    razel: &mut Razel,
    args: Vec<String>,
    name: &str,
    tags: Vec<Tag>,
) -> Result<(), anyhow::Error> {
    let cli = Cli::try_parse_from(args.iter())?;
    match cli.command {
        CliCommands::Command { command } => {
            parse_command(razel, command)?;
        }
        CliCommands::Task(task) => {
            task.build_command(razel, name.to_owned(), args, tags)?;
        }
        _ => bail!("Razel subcommand not allowed within files"),
    }
    Ok(())
}

fn apply_file(razel: &mut Razel, file: &String) -> Result<(), anyhow::Error> {
    match Path::new(file).extension().and_then(OsStr::to_str) {
        Some("jsonl") => parse_jsonl_file(razel, file),
        _ => parse_batch_file(razel, file),
    }
}
