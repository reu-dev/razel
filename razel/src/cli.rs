use crate::executors::HttpRemoteExecConfig;
use crate::razel_jsonl::parse_jsonl_file;
use crate::tasks::DownloadFileTask;
use crate::types::Tag;
use crate::{parse_batch_file, parse_command, tasks, CommandBuilder, FileType, Razel};
use anyhow::bail;
use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use url::Url;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
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
    Task(CliTask),
    /// Execute commands from a razel.jsonl or batch file
    #[clap(visible_alias = "build", visible_alias = "test")]
    Exec(Exec),
    /// List commands from a razel.jsonl or batch file
    #[clap(visible_alias = "ls", visible_alias = "show-only")]
    ListCommands {
        /// File with commands to list
        #[clap(short, long, default_value = "razel.jsonl")]
        file: String,
        #[clap(flatten)]
        filter_args: FilterArgs,
    },
    /// Import commands from files and create razel.jsonl
    Import {
        /// razel.jsonl file to create
        #[clap(short, long, default_value = "razel.jsonl")]
        output: PathBuf,
        /// Input files to parse commands from
        #[clap(required = true)]
        files: Vec<String>,
    },
    /// Subcommands for Razel system management
    #[clap(subcommand)]
    System(SystemCommand),
    // TODO add Debug subcommand
    // TODO add upgrade subcommand
}

#[derive(Args, Debug)]
struct Exec {
    /// File with commands to execute
    #[clap(short, long, default_value = "razel.jsonl")]
    file: String,
    #[clap(flatten)]
    run_args: RunArgs,
    #[clap(flatten)]
    filter_args: FilterArgs,
}

#[derive(Args, Debug)]
pub struct RunArgs {
    /// No execution, just show info about configuration, cache, ...
    #[clap(short, long)]
    pub info: bool,
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
    /// Local cache directory (use --info to show default value)
    #[clap(long, env = "RAZEL_CACHE_DIR")]
    pub cache_dir: Option<PathBuf>,
    /// Comma seperated list of remote cache URLs
    #[clap(long, env = "RAZEL_REMOTE_CACHE", value_delimiter = ',')]
    pub remote_cache: Vec<String>,
    /// Only cache commands with: output size / exec time < threshold [kilobyte / s]
    #[clap(long, env = "RAZEL_REMOTE_CACHE_THRESHOLD")]
    pub remote_cache_threshold: Option<u32>,
    /// Http remote execution configuration
    #[clap(long, env = "RAZEL_HTTP_REMOTE_EXEC")]
    pub http_remote_exec: Option<HttpRemoteExecConfig>,
}

impl Default for RunArgs {
    fn default() -> Self {
        Self {
            info: false,
            no_execution: false,
            keep_going: false,
            verbose: true,
            group_by_tag: "group".to_string(),
            cache_dir: None,
            remote_cache: vec![],
            remote_cache_threshold: None,
            http_remote_exec: None,
        }
    }
}

#[derive(Args, Debug)]
#[group(multiple = false)]
pub struct FilterArgs {
    /// Filter commands by name or output file
    pub targets: Vec<String>,
    /// Filter commands by name or output file, include commands matching any pattern
    #[clap(short = 'r', long, num_args = 1..)]
    pub filter_regex: Vec<String>,
    /// Filter commands by name or output file, include commands matching all patterns
    #[clap(short = 'a', long, num_args = 1..)]
    pub filter_regex_all: Vec<String>,
    // TODO Filter commands by tags
    //#[clap(short = 't', long, num_args = 1..)]
    //pub filter_tags: Vec<String>,
}

#[derive(Subcommand, Debug)]
enum SystemCommand {
    /// Check remote cache availability
    CheckRemoteCache {
        /// Comma seperated list of remote cache URLs
        #[clap(env = "RAZEL_REMOTE_CACHE", value_delimiter = ',', required = true)]
        urls: Vec<String>,
    },
}

#[derive(Subcommand, Serialize, Deserialize)]
pub enum CliTask {
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
    /// Post a HTTP multipart form for remote execution
    HttpRemoteExec(HttpRemoteExecTask),
}

impl CliTask {
    pub fn build_command(
        self,
        razel: &mut Razel,
        name: String,
        args: Vec<String>,
        tags: Vec<Tag>,
    ) -> Result<(), anyhow::Error> {
        let mut builder = CommandBuilder::new(name, args, tags);
        match self {
            CliTask::CaptureRegex(x) => x.build(&mut builder, razel),
            CliTask::CsvConcat(x) => x.build(&mut builder, razel),
            CliTask::CsvFilter(x) => x.build(&mut builder, razel),
            CliTask::WriteFile(x) => x.build(&mut builder, razel),
            CliTask::DownloadFile(x) => x.build(&mut builder, razel),
            CliTask::EnsureEqual(x) => x.build(&mut builder, razel),
            CliTask::EnsureNotEqual(x) => x.build(&mut builder, razel),
            CliTask::HttpRemoteExec(x) => x.build(&mut builder, razel),
        }?;
        razel.push(builder)?;
        Ok(())
    }
}

trait TaskBuilder {
    fn build(self, builder: &mut CommandBuilder, razel: &mut Razel) -> Result<(), anyhow::Error>;
}

#[derive(Args, Serialize, Deserialize)]
pub struct CaptureRegexTask {
    /// Input file to read
    pub input: String,
    /// File to write the captured value to
    pub output: String,
    /// Regex containing a single capturing group
    pub regex: String,
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

#[derive(Args, Serialize, Deserialize)]
pub struct CsvConcatTask {
    /// Input csv files
    #[clap(required = true)]
    pub input: Vec<String>,
    /// Concatenated file to create
    pub output: String,
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

#[derive(Args, Serialize, Deserialize)]
pub struct CsvFilterTask {
    #[clap(short, long)]
    pub input: String,
    #[clap(short, long)]
    pub output: String,
    /// Col names to keep - all other cols are dropped
    #[clap(short, long = "col", num_args = 0..)]
    pub cols: Vec<String>,
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

#[derive(Args, Serialize, Deserialize)]
pub struct WriteFileTask {
    /// File to create
    pub file: String,
    /// Lines to write
    pub lines: Vec<String>,
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

#[derive(Args, Serialize, Deserialize)]
pub struct DownloadFileTaskBuilder {
    #[clap(short, long)]
    pub url: String,
    #[clap(short, long)]
    pub output: String,
    #[clap(short, long)]
    pub executable: bool,
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

#[derive(Args, Serialize, Deserialize)]
pub struct EnsureEqualTask {
    pub file1: String,
    pub file2: String,
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

#[derive(Args, Serialize, Deserialize)]
pub struct EnsureNotEqualTask {
    pub file1: String,
    pub file2: String,
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

#[derive(Args, Serialize, Deserialize)]
pub struct HttpRemoteExecTask {
    /// url for HTTP multipart form POST
    #[clap(short, long)]
    pub url: Url,
    /// files to attach to the form
    #[clap(short, long)]
    pub files: Vec<String>,
    /// file names to use in the form
    #[clap(short = 'n', long)]
    pub file_names: Vec<String>,
}

impl TaskBuilder for HttpRemoteExecTask {
    fn build(self, builder: &mut CommandBuilder, razel: &mut Razel) -> Result<(), anyhow::Error> {
        if self.file_names.len() != self.files.len() {
            bail!("number of file names and files must be equal");
        }
        let state = razel.http_remote_exec(&self.url);
        let mut files = Vec::with_capacity(self.files.len());
        for (i, name) in self.file_names.into_iter().enumerate() {
            let file = builder.input(&self.files[i], razel)?;
            files.push((name, file));
        }
        builder.http_remote_executor(state, self.url, files);
        Ok(())
    }
}

pub async fn parse_cli(
    args: Vec<String>,
    razel: &mut Razel,
) -> Result<Option<RunArgs>, anyhow::Error> {
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
            if let Some(x) = &exec.run_args.http_remote_exec {
                razel.set_http_remote_exec_config(x);
            }
            apply_file(razel, &exec.file)?;
            apply_filter(razel, &exec.filter_args)?;
            Some(exec.run_args)
        }
        CliCommands::ListCommands { file, filter_args } => {
            apply_file(razel, &file)?;
            apply_filter(razel, &filter_args)?;
            Some(RunArgs {
                no_execution: true,
                ..Default::default()
            })
        }
        CliCommands::Import { output, files } => {
            import(razel, &output, files)?;
            None
        }
        CliCommands::System(s) => {
            match s {
                SystemCommand::CheckRemoteCache { urls } => razel.check_remote_cache(urls).await?,
            }
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

pub fn parse_task(args: &Vec<String>) -> Result<CliTask, anyhow::Error> {
    let cli = Cli::try_parse_from(args)?;
    let CliCommands::Task(task) = cli.command else {
        unreachable!()
    };
    Ok(task)
}

fn apply_file(razel: &mut Razel, file: &String) -> Result<(), anyhow::Error> {
    match Path::new(file).extension().and_then(OsStr::to_str) {
        Some("jsonl") => parse_jsonl_file(razel, file),
        _ => parse_batch_file(razel, file),
    }
}

fn apply_filter(razel: &mut Razel, filter: &FilterArgs) -> Result<(), anyhow::Error> {
    if !filter.targets.is_empty() {
        razel.filter_targets(&filter.targets);
    } else if !filter.filter_regex.is_empty() {
        razel.filter_targets_regex(&filter.filter_regex)?;
    } else if !filter.filter_regex_all.is_empty() {
        razel.filter_targets_regex_all(&filter.filter_regex_all)?;
    }
    Ok(())
}

fn import(razel: &mut Razel, output: &Path, files: Vec<String>) -> Result<(), anyhow::Error> {
    for file in files {
        apply_file(razel, &file)?;
    }
    razel.write_jsonl(output)
}
