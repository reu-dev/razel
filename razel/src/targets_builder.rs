use crate::types::{
    CommandTarget, ExecutableType, File, FileId, Target, TargetId, TargetKind, TaskTarget,
};
use crate::{cli, config, CliTask, RazelJson, RazelJsonCommand, RazelJsonTask};
use anyhow::{anyhow, bail, Context, Result};
use itertools::{chain, Itertools};
use log::debug;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::{env, fs};
use which::which;

pub struct TargetsBuilder {
    /// current working directory, used to make paths relative
    pub current_dir: PathBuf,
    /// absolute directory to resolve relative paths of files
    pub workspace_dir: PathBuf,
    pub targets: Vec<Target>,
    pub files: Vec<File>,
    /// maps paths relative to current_dir (without out_dir prefix) to FileId
    pub file_by_path: HashMap<PathBuf, FileId>,
    pub creator_for_file: HashMap<FileId, TargetId>,
    pub target_by_name: HashMap<String, TargetId>,
    system_executable_by_name: HashMap<String, FileId>,
}

impl TargetsBuilder {
    pub fn new(mut workspace_dir: PathBuf) -> Self {
        let current_dir = env::current_dir().unwrap();
        if workspace_dir.is_relative() {
            workspace_dir = current_dir.join(workspace_dir);
        }
        Self {
            current_dir,
            workspace_dir,
            targets: vec![],
            files: vec![],
            file_by_path: Default::default(),
            creator_for_file: Default::default(),
            target_by_name: Default::default(),
            system_executable_by_name: Default::default(),
        }
    }

    pub fn push_command(&mut self, command: RazelJsonCommand) -> Result<()> {
        if self.target_by_name.contains_key(&command.name) {
            bail!("target already exists: {:?}", command.name);
        }
        let mut args = command.args;
        let executable_id = self.push_executable_file(&command.executable)?;
        let inputs = command
            .inputs
            .into_iter()
            .map(|x| self.push_input_file(&x, &mut args))
            .collect::<Result<Vec<_>>>()?;
        let outputs = command
            .outputs
            .into_iter()
            .map(|x| self.push_output_file(&x, &mut args))
            .collect::<Result<Vec<_>>>()?;
        let deps = command
            .deps
            .into_iter()
            .map(|x| {
                self.target_by_name
                    .get(&x)
                    .cloned()
                    .ok_or_else(|| anyhow!("no such target: {x:?}"))
            })
            .collect::<Result<Vec<_>>>()?;
        let target = Target {
            id: self.targets.len(),
            name: command.name,
            kind: TargetKind::Command(CommandTarget {
                executable: self.files[executable_id].path.to_str().unwrap().into(),
                args,
                env: command.env,
                stdout_file: command.stdout.map(|x| x.into()),
                stderr_file: command.stderr.map(|x| x.into()),
            }),
            executables: vec![executable_id],
            inputs,
            outputs,
            deps,
            tags: command.tags,
        };
        self.push_target(target);
        Ok(())
    }

    pub fn push_task(&mut self, json: RazelJsonTask, task: CliTask) -> Result<()> {
        if self.target_by_name.contains_key(&json.name) {
            bail!("target already exists: {:?}", json.name);
        }
        let mut args = json.args;
        let mut inputs = vec![];
        let mut outputs = vec![];
        macro_rules! input {
            ($arg:expr) => {{
                let id = self.push_input_file(&$arg, &mut args)?;
                inputs.push(id);
                id
            }};
        }
        macro_rules! output {
            ($arg:expr) => {{
                let id = self.push_output_file(&$arg, &mut args)?;
                outputs.push(id);
                id
            }};
        }
        match &task {
            CliTask::CaptureRegex(t) => {
                input!(&t.input);
                output!(&t.output);
            }
            CliTask::CsvConcat(t) => {
                inputs.reserve(t.input.len());
                for x in &t.input {
                    input!(&x);
                }
                output!(&t.output);
            }
            CliTask::CsvFilter(t) => {
                input!(&t.input);
                output!(&t.output);
            }
            CliTask::WriteFile(t) => {
                output!(&t.file);
            }
            CliTask::DownloadFile(t) => {
                let file = output!(&t.output);
                if t.executable {
                    self.files[file].executable = Some(ExecutableType::ExecutableInWorkspace);
                }
            }
            CliTask::EnsureEqual(t) => {
                input!(&t.file1);
                input!(&t.file2);
            }
            CliTask::EnsureNotEqual(t) => {
                input!(&t.file1);
                input!(&t.file2);
            }
            CliTask::HttpRemoteExec(t) => {
                inputs.reserve(t.files.len());
                for x in &t.files {
                    input!(x);
                }
            }
        }
        let target = Target {
            id: self.targets.len(),
            name: json.name,
            kind: TargetKind::Task(TaskTarget { args, task }),
            executables: vec![],
            inputs,
            outputs,
            deps: vec![],
            tags: json.tags,
        };
        self.push_target(target);
        Ok(())
    }

    fn push_target(&mut self, target: Target) {
        let old = self.target_by_name.insert(target.name.clone(), target.id);
        assert!(old.is_none());
        for output in target.outputs.iter().cloned() {
            let old = self.creator_for_file.insert(output, target.id);
            assert!(old.is_none());
        }
        self.targets.push(target);
    }

    fn push_executable_file(&mut self, arg: &str) -> Result<FileId> {
        let path = Path::new(&arg);
        if let Some(id) = self.file_by_path.get(path) {
            return Ok(*id);
        }
        let Some(file_name) = path.file_name().and_then(|x| x.to_str()) else {
            bail!(format!("executable is not a valid filename: {arg:?}"));
        };
        let (executable_type, abs_path) = if file_name == arg {
            if arg == "razel" || arg == "razel.exe" {
                (ExecutableType::RazelExecutable, path.to_path_buf())
            } else {
                return self.executable_which(arg);
            }
        } else if path.iter().contains(&OsStr::new("..")) {
            let canonicalized = path
                .canonicalize()
                .with_context(|| format!("canonicalize {arg:?}"))?;
            if canonicalized.starts_with(&self.workspace_dir) {
                (ExecutableType::ExecutableInWorkspace, canonicalized)
            } else {
                (ExecutableType::ExecutableOutsideWorkspace, canonicalized)
            }
        } else if path.is_relative() {
            (
                ExecutableType::ExecutableInWorkspace,
                self.workspace_dir.join(path),
            )
        } else if path.starts_with(&self.workspace_dir) {
            (ExecutableType::ExecutableInWorkspace, path.into())
        } else {
            (ExecutableType::ExecutableOutsideWorkspace, path.into())
        };
        let path = match executable_type {
            ExecutableType::ExecutableInWorkspace => {
                abs_path.strip_prefix(&self.workspace_dir)?.into()
            }
            ExecutableType::ExecutableOutsideWorkspace
            | ExecutableType::WasiModule
            | ExecutableType::SystemExecutable
            | ExecutableType::RazelExecutable => abs_path,
        };
        if let Some(id) = self.file_by_path.get(&path) {
            return Ok(*id);
        }
        let id = self.files.len();
        self.files.push(File::new(id, path, Some(executable_type)));
        Ok(id)
    }

    fn executable_which(&mut self, arg: &str) -> Result<FileId> {
        if let Some(id) = self.system_executable_by_name.get(arg) {
            return Ok(*id);
        }
        let path = which(arg).with_context(|| format!("executable not found: {arg:?}"))?;
        debug!("which({arg}) => {path:?}");
        let id = self.push_file(path, Some(ExecutableType::SystemExecutable));
        let old = self.system_executable_by_name.insert(arg.to_string(), id);
        assert!(old.is_none());
        Ok(id)
    }

    fn push_input_file(&mut self, arg: &str, args: &mut [String]) -> Result<FileId> {
        let id = self.push_file_with_relative_path(arg)?;
        let file = &self.files[id];
        for x in args.iter_mut().filter(|x| *x == arg) {
            *x = file.path.to_str().unwrap().into();
        }
        Ok(file.id)
    }

    fn push_output_file(&mut self, arg: &str, args: &mut [String]) -> Result<FileId> {
        let id = self.push_file_with_relative_path(arg)?;
        let file = &self.files[id];
        if let Some(target_id) = self.creator_for_file.get(&id) {
            bail!(
                "File {:?} cannot be output of multiple targets, already output of {:?}",
                file.path,
                self.targets[*target_id].name
            );
        }
        for x in args.iter_mut().filter(|x| *x == arg) {
            *x = file.path.to_str().unwrap().into();
        }
        Ok(file.id)
    }

    fn push_file(&mut self, path: PathBuf, executable: Option<ExecutableType>) -> FileId {
        if let Some(id) = self.file_by_path.get(&path) {
            return *id;
        }
        let id = self.files.len();
        self.files.push(File::new(id, path.clone(), executable));
        let old = self.file_by_path.insert(path, id);
        assert!(old.is_none());
        id
    }

    fn push_file_with_relative_path(&mut self, arg: &str) -> Result<FileId> {
        let path = self.rel_path(arg)?;
        Ok(self.push_file(path, None))
    }

    /// Maps a relative path from workspace dir to cwd, allow absolute path
    fn rel_path(&self, arg: &str) -> Result<PathBuf> {
        let path = Path::new(arg);
        if path.is_absolute() {
            Ok(PathBuf::from(
                path.strip_prefix(&self.current_dir).unwrap_or(path),
            ))
        } else {
            self.workspace_dir
                .join(path)
                .strip_prefix(&self.current_dir)
                .map(PathBuf::from)
                .with_context(|| {
                    format!(
                        "File is not within cwd ({:?}): {:?}",
                        self.current_dir, path
                    )
                })
        }
    }
}

impl File {
    fn new(id: FileId, path: PathBuf, executable: Option<ExecutableType>) -> Self {
        Self {
            id,
            path,
            digest: None,
            executable,
        }
    }
}

pub fn parse_jsonl_file(path: &str) -> Result<TargetsBuilder> {
    let mut builder = TargetsBuilder::new(Path::new(path).parent().unwrap().into());
    let file =
        BufReader::new(fs::File::open(path).with_context(|| anyhow!("failed to open {path:?}"))?);
    let mut len: usize = 0;
    for (line_number, line_result) in file.lines().enumerate() {
        let line = line_result?;
        let line_trimmed = line.trim();
        if line_trimmed.is_empty() || line_trimmed.starts_with("//") {
            continue;
        }
        let json: RazelJson = serde_json::from_str(line_trimmed).with_context(|| {
            format!(
                "failed to parse {}:{}\n{}",
                path,
                line_number + 1,
                line_trimmed
            )
        })?;
        match json {
            RazelJson::Command(command) => {
                builder.push_command(command)?;
            }
            RazelJson::Task(mut json) => {
                json.args = chain!(
                    [
                        config::EXECUTABLE.to_string(),
                        "task".to_string(),
                        json.task.clone()
                    ],
                    json.args.into_iter()
                )
                .collect();
                let task = cli::parse_task(&json.args)?;
                builder.push_task(json, task)?;
            }
        }
        len += 1;
    }
    debug!("Added {len} commands from {path}");
    Ok(builder)
}
