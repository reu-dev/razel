use crate::config;
use crate::types::{
    CommandTarget, ExecutableType, File, FileId, RazelJson, RazelJsonCommand, RazelJsonHandler,
    RazelJsonTask, Tag, Target, TargetId, TargetKind, Task, TaskTarget,
};
use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
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
    /// directory of output files - relative to current_dir
    pub out_dir: PathBuf,
    pub targets: Vec<Target>,
    pub files: Vec<File>,
    /// maps paths relative to current_dir (without out_dir prefix) to FileId
    pub file_by_path: HashMap<PathBuf, FileId>,
    pub creator_for_file: HashMap<FileId, TargetId>,
    pub target_by_name: HashMap<String, TargetId>,
    system_executable_by_name: HashMap<String, FileId>,
}

impl Default for TargetsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TargetsBuilder {
    pub fn new() -> Self {
        let current_dir = env::current_dir().unwrap();
        let workspace_dir = current_dir.clone();
        Self {
            current_dir,
            workspace_dir,
            out_dir: PathBuf::from(config::OUT_DIR),
            targets: vec![],
            files: vec![],
            file_by_path: Default::default(),
            creator_for_file: Default::default(),
            target_by_name: Default::default(),
            system_executable_by_name: Default::default(),
        }
    }

    pub fn read_jsonl_file(&mut self, path: &str) -> Result<()> {
        self.set_workspace_dir(Path::new(path).parent().unwrap());
        let file = BufReader::new(
            fs::File::open(path).with_context(|| anyhow!("failed to open {path:?}"))?,
        );
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
                RazelJson::Command(command) => self.push_json_command(command)?,
                RazelJson::Task(json) => self.push_json_task(json)?,
            };
            len += 1;
        }
        debug!("Added {len} commands from {path}");
        Ok(())
    }

    pub fn push_json_command(&mut self, command: RazelJsonCommand) -> Result<TargetId> {
        if self.target_by_name.contains_key(&command.name) {
            bail!("target already exists: {:?}", command.name);
        }
        let mut args = command.args;
        let executable_id = self.push_executable_file(&command.executable)?;
        let inputs = command
            .inputs
            .into_iter()
            .map(|mut x| self.push_input_file(&mut x, &mut args))
            .collect::<Result<Vec<_>>>()?;
        let outputs = command
            .outputs
            .into_iter()
            .map(|mut x| self.push_output_file(&mut x, &mut args))
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
        let command_target = CommandTarget {
            executable: self.files[executable_id].path.to_str().unwrap().into(),
            args,
            env: command.env,
            stdout_file: command.stdout.map(|x| x.into()),
            stderr_file: command.stderr.map(|x| x.into()),
        };
        let target = Target {
            id: Default::default(),
            name: command.name,
            kind: if command_target.executable.ends_with(".wasm") {
                TargetKind::Wasi(command_target)
            } else {
                TargetKind::Command(command_target)
            },
            executables: vec![executable_id],
            inputs,
            outputs,
            deps,
            tags: command.tags,
            is_excluded: false,
        };
        self.push_target(target)
    }

    pub fn push_json_task(&mut self, json: RazelJsonTask) -> Result<TargetId> {
        let args = chain!(
            ["task".to_string(), json.task.clone()],
            json.args.into_iter()
        )
        .collect();
        let task = TaskParser::try_parse_from(&args)?.task;
        self.push_task(json.name, args, task, json.tags)
    }

    pub fn push_task(
        &mut self,
        name: String,
        mut args: Vec<String>,
        mut task: Task,
        tags: Vec<Tag>,
    ) -> Result<TargetId> {
        if self.target_by_name.contains_key(&name) {
            bail!("target already exists: {name:?}");
        }
        let mut inputs = vec![];
        let mut outputs = vec![];
        macro_rules! input {
            ($arg:expr) => {{
                let id = self.push_input_file(&mut $arg, &mut args)?;
                inputs.push(id);
                id
            }};
        }
        macro_rules! output {
            ($arg:expr) => {{
                let id = self.push_output_file(&mut $arg, &mut args)?;
                outputs.push(id);
                id
            }};
        }
        match task {
            Task::CaptureRegex(ref mut t) => {
                input!(&mut t.input);
                output!(&mut t.output);
            }
            Task::CsvConcat(ref mut t) => {
                inputs.reserve(t.input.len());
                for x in t.input.iter_mut() {
                    input!(*x);
                }
                output!(&mut t.output);
            }
            Task::CsvFilter(ref mut t) => {
                input!(&mut t.input);
                output!(&mut t.output);
            }
            Task::WriteFile(ref mut t) => {
                output!(&mut t.file);
            }
            Task::DownloadFile(ref mut t) => {
                let file_id = output!(&mut t.output);
                if t.executable {
                    self.files[file_id].executable = Some(ExecutableType::ExecutableInWorkspace);
                }
            }
            Task::EnsureEqual(ref mut t) => {
                input!(&mut t.file1);
                input!(&mut t.file2);
            }
            Task::EnsureNotEqual(ref mut t) => {
                input!(&mut t.file1);
                input!(&mut t.file2);
            }
            Task::HttpRemoteExec(ref mut t) => {
                if t.file_names.len() != t.files.len() {
                    bail!("number of file names and files must be equal: {name:?}");
                }
                inputs.reserve(t.files.len());
                for x in t.files.iter_mut() {
                    input!(*x);
                }
            }
        }
        let id = self.targets.len();
        let target = Target {
            id,
            name,
            kind: match task {
                Task::HttpRemoteExec(_) => {
                    TargetKind::HttpRemoteExecTask(TaskTarget { args, task })
                }
                _ => TargetKind::Task(TaskTarget { args, task }),
            },
            executables: vec![],
            inputs,
            outputs,
            deps: vec![],
            tags,
            is_excluded: false,
        };
        self.push_target(target)
    }

    fn push_target(&mut self, mut target: Target) -> Result<TargetId> {
        Self::check_tags(&mut target)?;
        let old = self.target_by_name.insert(target.name.clone(), target.id);
        assert!(old.is_none());
        let id = self.targets.len();
        target.id = id;
        for output in target.outputs.iter().cloned() {
            let old = self.creator_for_file.insert(output, target.id);
            assert!(old.is_none());
        }
        self.targets.push(target);
        Ok(id)
    }

    pub fn check_tags(target: &mut Target) -> Result<()> {
        match &target.kind {
            TargetKind::Command(_) | TargetKind::Task(_) | TargetKind::HttpRemoteExecTask(_) => {
                if target.tags.contains(&Tag::NoSandbox) && !target.tags.contains(&Tag::NoCache) {
                    // executing a command without sandbox is not reliable, therefore don't cache it
                    debug!("push Tag::NoCache: {}", target.name);
                    target.tags.push(Tag::NoCache);
                }
            }
            TargetKind::Wasi(_) => {
                if target.tags.contains(&Tag::NoSandbox) {
                    bail!(
                        "Tag is not supported for WASI executor: {}",
                        serde_json::to_string(&Tag::NoSandbox).unwrap()
                    );
                }
            }
        }
        Ok(())
    }

    fn push_executable_file(&mut self, arg: &str) -> Result<FileId> {
        let path = Path::new(&arg);
        if path.is_relative() {
            let abs = self.workspace_dir.join(path);
            let cwd_path = abs.strip_prefix(&self.current_dir).unwrap().to_path_buf();
            if let Some(id) = self.file_by_path.get(&cwd_path) {
                return Ok(*id);
            }
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
            ExecutableType::ExecutableInWorkspace
            | ExecutableType::ExecutableOutsideWorkspace
            | ExecutableType::WasiModule => abs_path.strip_prefix(&self.current_dir)?.into(),
            ExecutableType::SystemExecutable | ExecutableType::RazelExecutable => abs_path,
        };
        if let Some(id) = self.file_by_path.get(&path) {
            return Ok(*id);
        }
        let id = self.files.len();
        self.files
            .push(File::new(id, path.clone(), Some(executable_type)));
        self.file_by_path.insert(path, id);
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

    fn push_input_file(&mut self, arg: &mut String, args: &mut [String]) -> Result<FileId> {
        let id = self.push_input_file_with_relative_path(arg)?;
        let file = &self.files[id];
        let path = file.path.to_str().unwrap().to_string();
        for x in args.iter_mut().filter(|x| *x == arg) {
            *x = path.clone();
        }
        *arg = path;
        Ok(file.id)
    }

    fn push_input_file_with_relative_path(&mut self, arg: &str) -> Result<FileId> {
        let path = self.rel_path(arg)?;
        Ok(self.push_file(path, None))
    }

    fn push_output_file(&mut self, arg: &mut String, args: &mut [String]) -> Result<FileId> {
        let id = self.push_output_file_with_relative_path(arg)?;
        let file = &self.files[id];
        if let Some(target_id) = self.creator_for_file.get(&id) {
            bail!(
                "File {:?} cannot be output of multiple targets, already output of {:?}",
                file.path,
                self.targets[*target_id].name
            );
        }
        let path = file.path.to_str().unwrap().to_string();
        for x in args.iter_mut().filter(|x| *x == arg) {
            *x = path.clone();
        }
        *arg = path;
        Ok(file.id)
    }

    fn push_output_file_with_relative_path(&mut self, arg: &str) -> Result<FileId> {
        let path = self.rel_path(arg)?;
        if let Some(id) = self.file_by_path.get(&path) {
            return Ok(*id);
        }
        let id = self.files.len();
        let path_with_out_dir = self.out_dir.join(&path);
        self.files.push(File::new(id, path_with_out_dir, None));
        let old = self.file_by_path.insert(path, id);
        assert!(old.is_none());
        Ok(id)
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

impl RazelJsonHandler for TargetsBuilder {
    /// Set the directory to resolve relative paths of input/output files
    fn set_workspace_dir(&mut self, dir: &Path) {
        if dir == Path::new("") {
            self.workspace_dir = self.current_dir.clone();
        } else if dir.is_relative() {
            self.workspace_dir = self.current_dir.join(dir);
        } else {
            self.workspace_dir = dir.into();
        }
    }

    fn push_json(&mut self, json: RazelJson) -> Result<TargetId> {
        match json {
            RazelJson::Command(command) => self.push_json_command(command),
            RazelJson::Task(json) => self.push_json_task(json),
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
            is_excluded: false,
        }
    }
}

#[derive(Parser)]
struct TaskParser {
    #[clap(subcommand)]
    task: Task,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_parser() {
        let task = TaskParser::parse_from(["task", "write-file", "out.txt", "line1", "line2"]).task;
        match task {
            Task::WriteFile(t) => {
                assert_eq!(t.file, "out.txt");
                assert_eq!(t.lines, vec!["line1", "line2"]);
            }
            _ => panic!(),
        }
    }
}
