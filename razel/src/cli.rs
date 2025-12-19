use crate::types::{RazelJson, Tag, Task};
use crate::{parse_batch_file, parse_command, Razel};
use anyhow::{bail, Result};
use clap::{Args, Parser, Subcommand};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use url::Url;

mod http_remote_exec_config;
pub use http_remote_exec_config::*;

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
    Task(Task),
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
    /// Comma seperated list of remote execution server URLs
    #[clap(long, env = "RAZEL_REMOTE_EXEC", value_delimiter = ',')]
    pub remote_exec: Vec<Url>,
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
            remote_exec: vec![],
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
    /// Check availability of remote cache servers
    CheckRemoteCache {
        /// Comma seperated list of remote cache URLs
        #[clap(env = "RAZEL_REMOTE_CACHE", value_delimiter = ',', required = true)]
        urls: Vec<String>,
    },
    /// Check availability of remote execution servers
    CheckRemoteExec {
        /// Comma seperated list of remote execution server URLs
        #[clap(env = "RAZEL_REMOTE_EXEC", value_delimiter = ',', required = true)]
        urls: Vec<Url>,
    },
}

pub async fn parse_cli(args: Vec<String>, razel: &mut Razel) -> Result<Option<RunArgs>> {
    let cli = Cli::parse_from(args.iter());
    Ok(match cli.command {
        CliCommands::Command { command } => {
            parse_command(razel, command)?;
            Some(Default::default())
        }
        CliCommands::Task(task) => {
            razel.push_task("task".to_string(), args, task, vec![])?;
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
                SystemCommand::CheckRemoteCache { urls } => {
                    razel.check_remote_cache_servers(urls).await?
                }
                SystemCommand::CheckRemoteExec { urls } => {
                    razel.check_remote_exec_servers(urls).await?
                }
            }
            None
        }
    })
}

pub fn parse_cli_within_file(
    razel: &mut Razel,
    args: Vec<String>,
    name: String,
    tags: Vec<Tag>,
) -> Result<()> {
    let cli = Cli::try_parse_from(args.iter())?;
    match cli.command {
        CliCommands::Command { command } => {
            parse_command(razel, command)?;
        }
        CliCommands::Task(task) => {
            razel.push_task(name, args, task, tags)?;
        }
        _ => bail!("Razel subcommand not allowed within files"),
    }
    Ok(())
}

fn apply_file(razel: &mut Razel, file: &String) -> Result<()> {
    match Path::new(file).extension().and_then(OsStr::to_str) {
        Some("jsonl") => RazelJson::read(file, razel),
        _ => parse_batch_file(razel, file),
    }
}

fn apply_filter(razel: &mut Razel, filter: &FilterArgs) -> Result<()> {
    if !filter.targets.is_empty() {
        razel.filter_targets(&filter.targets);
    } else if !filter.filter_regex.is_empty() {
        razel.filter_targets_regex(&filter.filter_regex)?;
    } else if !filter.filter_regex_all.is_empty() {
        razel.filter_targets_regex_all(&filter.filter_regex_all)?;
    }
    Ok(())
}

fn import(razel: &mut Razel, output: &Path, files: Vec<String>) -> Result<()> {
    for file in files {
        apply_file(razel, &file)?;
    }
    razel.write_jsonl(output)
}
