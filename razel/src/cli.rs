use crate::types::{RazelJson, Tag, Task};
use crate::{Razel, Rules, parse_batch_file, parse_command};
use anyhow::{Result, bail};
use clap::{Args, Parser, Subcommand};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use url::Url;

mod http_remote_exec_config;
pub use http_remote_exec_config::*;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: CliCommands,
}

#[allow(clippy::large_enum_variant)]
#[derive(Subcommand)]
enum CliCommands {
    /// Execute a custom command
    Command {
        #[clap(flatten)]
        command: CommandArgs,
        #[clap(flatten)]
        cache_args: CacheArgs,
        #[clap(flatten)]
        remote_exec_args: RemoteExecCliArgs,
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
pub struct CommandArgs {
    /// Rule to parse input/output files from the command line, e.g. "cp <in> <out>"
    #[clap(short, long, conflicts_with_all = ["inputs", "outputs"])]
    pub rule: Option<String>,
    /// Input file
    #[clap(short = 'i', long = "input")]
    pub inputs: Vec<String>,
    /// Output file
    #[clap(short = 'o', long = "output")]
    pub outputs: Vec<String>,
    #[clap(last = true, required = true)]
    pub command: Vec<String>,
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
    /// Write a JUnit XML report to this path
    #[clap(long, env = "RAZEL_JUNIT", visible_alias = "output-junit")]
    pub junit: Option<PathBuf>,
    /// Override JUnit testcase classname (otherwise derived from --group-by-tag)
    #[clap(long, env = "RAZEL_JUNIT_CLASSNAME")]
    pub junit_classname: Option<String>,
    /// Max stdout/stderr bytes per failed testcase in JUnit report (0 = omit)
    #[clap(long, env = "RAZEL_JUNIT_FAILED_BYTES", default_value = "4096")]
    pub junit_failed_output_bytes: usize,
    /// Max stdout/stderr bytes per passed testcase in JUnit report (0 = omit)
    #[clap(long, env = "RAZEL_JUNIT_PASSED_BYTES", default_value = "0")]
    pub junit_passed_output_bytes: usize,
    #[clap(flatten)]
    pub cache_args: CacheArgs,
    #[clap(flatten)]
    pub remote_exec_args: RemoteExecCliArgs,
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
            junit: None,
            junit_classname: None,
            junit_failed_output_bytes: 4096,
            junit_passed_output_bytes: 0,
            cache_args: Default::default(),
            remote_exec_args: Default::default(),
            http_remote_exec: None,
        }
    }
}

#[derive(Args, Debug, Default)]
pub struct CacheArgs {
    /// Local cache directory (use `razel exec --info` to show default value)
    #[clap(long, env = "RAZEL_CACHE_DIR")]
    pub cache_dir: Option<PathBuf>,
    /// Comma separated list of remote cache URLs
    #[clap(long, env = "RAZEL_REMOTE_CACHE", value_delimiter = ',')]
    pub remote_cache: Vec<String>,
    /// Only cache commands with: output size / exec time < threshold [kilobyte / s]
    #[clap(long, env = "RAZEL_REMOTE_CACHE_THRESHOLD")]
    pub remote_cache_threshold: Option<u32>,
}

#[derive(Args, Debug, Default)]
pub struct RemoteExecCliArgs {
    /// Comma separated list of remote execution server URLs
    #[clap(long, env = "RAZEL_REMOTE_EXEC", value_delimiter = ',')]
    pub remote_exec: Vec<Url>,
    /// Auth token: JWT (audience `razel`) in CI, otherwise an opaque user-defined secret.
    #[clap(long, env = "RAZEL_TOKEN", hide_env_values = true)]
    pub remote_exec_token: Option<String>,
    /// Project label for interactive remote execution
    #[clap(long, env = "RAZEL_PROJECT")]
    pub remote_exec_project: Option<String>,
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
        /// Comma separated list of remote cache URLs
        #[clap(env = "RAZEL_REMOTE_CACHE", value_delimiter = ',', required = true)]
        urls: Vec<String>,
    },
    /// Check availability of remote execution servers
    CheckRemoteExec {
        /// Comma separated list of remote execution server URLs
        #[clap(env = "RAZEL_REMOTE_EXEC", value_delimiter = ',', required = true)]
        urls: Vec<Url>,
    },
}

pub async fn parse_cli(args: Vec<String>, razel: &mut Razel) -> Result<Option<RunArgs>> {
    let cli = Cli::parse_from(args.iter());
    Ok(match cli.command {
        CliCommands::Command {
            command,
            cache_args,
            remote_exec_args,
        } => {
            razel.single_target = true;
            parse_command(razel, &Rules::new(), "command".into(), command)?;
            Some(RunArgs {
                verbose: true,
                cache_args,
                remote_exec_args,
                ..Default::default()
            })
        }
        CliCommands::Task(task) => {
            razel.single_target = true;
            razel.push_task("task".to_string(), task_args(args), task, vec![])?;
            Some(RunArgs {
                verbose: true,
                ..Default::default()
            })
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
    rules: &Rules,
    args: Vec<String>,
    name: String,
    tags: Vec<Tag>,
) -> Result<()> {
    let cli = Cli::try_parse_from(args.iter())?;
    match cli.command {
        CliCommands::Command { command, .. } => {
            parse_command(razel, rules, name, command)?;
        }
        CliCommands::Task(task) => {
            razel.push_task(name, task_args(args), task, tags)?;
        }
        _ => bail!("Razel subcommand not allowed within files"),
    }
    Ok(())
}

/// Returns the args of a task in the razel.jsonl form `["task", <task>, <args>...]`.
///
/// They are used for the cache key, therefore must not depend on argv[0].
fn task_args(args: Vec<String>) -> Vec<String> {
    args.into_iter().skip(1).collect()
}

fn apply_file(razel: &mut Razel, file: &String) -> Result<()> {
    match Path::new(file).extension().and_then(OsStr::to_str) {
        Some("jsonl") => RazelJson::read(file, razel),
        _ => parse_batch_file(razel, file),
    }
}

fn apply_filter(razel: &mut Razel, filter: &FilterArgs) -> Result<()> {
    let builder = razel.targets_builder.as_mut().unwrap();
    if !filter.targets.is_empty() {
        builder.filter_targets(&filter.targets);
    } else if !filter.filter_regex.is_empty() {
        builder.filter_targets_regex(&filter.filter_regex)?;
    } else if !filter.filter_regex_all.is_empty() {
        builder.filter_targets_regex_all(&filter.filter_regex_all)?;
    }
    Ok(())
}

fn import(razel: &mut Razel, output: &Path, files: Vec<String>) -> Result<()> {
    for file in files {
        apply_file(razel, &file)?;
    }
    razel
        .targets_builder
        .as_ref()
        .unwrap()
        .write_jsonl_file(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::new_tmp_dir;

    fn args_with_executable(razel: &Razel) -> Vec<String> {
        let builder = razel.targets_builder.as_ref().unwrap();
        builder.targets[0].kind.args_with_executable()
    }

    #[tokio::test]
    async fn task_args_are_independent_of_source() {
        let dir = new_tmp_dir!();
        let output = dir.join("out.txt").to_str().unwrap().to_string();
        let expected = [
            "razel".to_string(),
            "task".into(),
            "write-file".into(),
            format!("razel-out/{output}"),
            "a".into(),
        ];
        for argv0 in ["razel", "razel.exe", "/path/to/razel"] {
            let mut razel = Razel::new();
            let args = [argv0, "task", "write-file", &output, "a"];
            parse_cli(args.map(String::from).to_vec(), &mut razel)
                .await
                .unwrap();
            assert_eq!(args_with_executable(&razel), expected, "{argv0}");
        }
        let file = dir.join_and_write_file("batch.sh", "razel task write-file out.txt a");
        let mut razel = Razel::new();
        apply_file(&mut razel, &file.to_str().unwrap().to_string()).unwrap();
        assert_eq!(args_with_executable(&razel), expected);
        let file = dir.join_and_write_file(
            "razel.jsonl",
            r#"{"name":"a","task":"write-file","args":["out.txt","a"]}"#,
        );
        let mut razel = Razel::new();
        apply_file(&mut razel, &file.to_str().unwrap().to_string()).unwrap();
        assert_eq!(args_with_executable(&razel), expected);
    }
}
