use anyhow::{anyhow, Context};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use url::Url;

use crate::executors::{
    AsyncTask, AsyncTaskExecutor, BlockingTaskExecutor, CustomCommandExecutor, Executor,
    HttpRemoteExecDomain, HttpRemoteExecutor, TaskFn, WasiExecutor,
};
use crate::types::Tag;
use crate::{ArenaId, FileId, FileType, Razel, ScheduleState};

pub struct Command {
    pub id: CommandId,
    pub name: String,
    /// user specified executable and optionally runtimes, e.g. razel for WASI
    pub executables: Vec<FileId>,
    /// input files excluding <Self::executables>
    pub inputs: Vec<FileId>,
    pub outputs: Vec<FileId>,
    /// dependencies on other commands in addition to input files
    pub deps: Vec<CommandId>,
    pub executor: Executor,
    pub tags: Vec<Tag>,
    pub is_excluded: bool,
    /// dependencies which are not yet finished successfully
    pub unfinished_deps: Vec<CommandId>,
    /// commands which depend on this command
    pub reverse_deps: Vec<CommandId>,
    /// TODO remove, Scheduler should keep track of states
    pub schedule_state: ScheduleState,
}

pub type CommandId = ArenaId<Command>;

pub struct CommandBuilder {
    name: String,
    args_with_out_paths: Vec<String>,
    executables: Vec<FileId>,
    inputs: Vec<FileId>,
    outputs: Vec<FileId>,
    stdout_file: Option<PathBuf>,
    stderr_file: Option<PathBuf>,
    deps: Vec<CommandId>,
    executor: Option<Executor>,
    tags: Vec<Tag>,
}

impl CommandBuilder {
    pub fn new(name: String, args: Vec<String>, tags: Vec<Tag>) -> CommandBuilder {
        CommandBuilder {
            name,
            args_with_out_paths: args,
            executables: vec![],
            inputs: vec![],
            outputs: vec![],
            stdout_file: None,
            stderr_file: None,
            deps: vec![],
            executor: None,
            tags,
        }
    }

    fn map_out_path(&mut self, original: &String, mapped: &str) {
        self.args_with_out_paths.iter_mut().for_each(|x| {
            if x == original {
                *x = mapped.to_owned()
            }
        });
    }

    pub fn input(&mut self, path: &String, razel: &mut Razel) -> Result<PathBuf, anyhow::Error> {
        razel.input_file(path.clone()).map(|file| {
            self.map_out_path(path, file.path.to_str().unwrap());
            self.inputs.push(file.id);
            file.path.clone()
        })
    }

    pub fn inputs(
        &mut self,
        paths: &[String],
        razel: &mut Razel,
    ) -> Result<Vec<PathBuf>, anyhow::Error> {
        self.inputs.reserve(paths.len());
        paths
            .iter()
            .map(|path| {
                let file = razel.input_file(path.clone())?;
                self.map_out_path(path, file.path.to_str().unwrap());
                self.inputs.push(file.id);
                Ok(file.path.clone())
            })
            .collect()
    }

    pub fn output(
        &mut self,
        path: &String,
        file_type: FileType,
        razel: &mut Razel,
    ) -> Result<PathBuf, anyhow::Error> {
        razel.output_file(path, file_type).map(|file| {
            self.map_out_path(path, file.path.to_str().unwrap());
            self.outputs.push(file.id);
            file.path.clone()
        })
    }

    pub fn outputs(
        &mut self,
        paths: &[String],
        razel: &mut Razel,
    ) -> Result<Vec<PathBuf>, anyhow::Error> {
        self.outputs.reserve(paths.len());
        paths
            .iter()
            .map(|path| {
                let file = razel.output_file(path, FileType::OutputFile)?;
                self.map_out_path(path, file.path.to_str().unwrap());
                self.outputs.push(file.id);
                Ok(file.path.clone())
            })
            .collect()
    }

    pub fn stdout(&mut self, path: &String, razel: &mut Razel) -> Result<(), anyhow::Error> {
        let file = razel.output_file(path, FileType::OutputFile)?;
        self.outputs.push(file.id);
        self.stdout_file = Some(file.path.clone());
        Ok(())
    }

    pub fn stderr(&mut self, path: &String, razel: &mut Razel) -> Result<(), anyhow::Error> {
        let file = razel.output_file(path, FileType::OutputFile)?;
        self.outputs.push(file.id);
        self.stderr_file = Some(file.path.clone());
        Ok(())
    }

    pub fn dep(&mut self, command_name: &String, razel: &mut Razel) -> Result<(), anyhow::Error> {
        let command_id = razel
            .get_command_by_name(command_name)
            .with_context(|| anyhow!("unknown command for dep: {command_name}"))?;
        self.deps.push(command_id.id);
        Ok(())
    }

    pub fn custom_command_executor(
        &mut self,
        executable: String,
        env: HashMap<String, String>,
        razel: &mut Razel,
    ) -> Result<(), anyhow::Error> {
        let file = razel.executable(executable)?;
        self.executables.push(file.id);
        self.executor = Some(Executor::CustomCommand(CustomCommandExecutor {
            executable: file.executable_for_command_line(),
            args: self.args_with_out_paths.clone(),
            env,
            stdout_file: self.stdout_file.clone(),
            stderr_file: self.stderr_file.clone(),
            timeout: self.tags.iter().find_map(|t| {
                if let Tag::Timeout(x) = t {
                    Some(*x)
                } else {
                    None
                }
            }),
        }));
        Ok(())
    }

    pub fn wasi_executor(
        &mut self,
        executable: String,
        env: HashMap<String, String>,
        razel: &mut Razel,
    ) -> Result<(), anyhow::Error> {
        let mut read_dirs = vec![];
        for id in &self.inputs {
            let dir = razel.get_file_path(*id).parent().unwrap().to_path_buf();
            if !read_dirs.contains(&dir) {
                read_dirs.push(dir);
            }
        }
        let file = razel.wasi_module(executable)?;
        let write_dir = (self.outputs.len()
            - self.stdout_file.is_some() as usize
            - self.stderr_file.is_some() as usize)
            != 0;
        self.executables.push(file.id);
        self.executor = Some(Executor::Wasi(WasiExecutor {
            module: None,
            module_file_id: Some(file.id),
            executable: file.executable_for_command_line(),
            args: self.args_with_out_paths.clone(),
            env,
            stdout_file: self.stdout_file.clone(),
            stderr_file: self.stderr_file.clone(),
            read_dirs,
            write_dir,
        }));
        Ok(())
    }

    pub fn async_task_executor(&mut self, task: impl AsyncTask + Send + Sync + 'static) {
        self.executor = Some(Executor::AsyncTask(AsyncTaskExecutor {
            task: Arc::new(task),
            args: self.args_with_out_paths.clone(),
        }));
    }

    pub fn blocking_task_executor(&mut self, f: TaskFn) {
        self.executor = Some(Executor::BlockingTask(BlockingTaskExecutor {
            f,
            args: self.args_with_out_paths.clone(),
        }));
    }

    pub fn http_remote_executor(
        &mut self,
        state: Option<Arc<HttpRemoteExecDomain>>,
        url: Url,
        files: Vec<(String, PathBuf)>,
    ) {
        self.executor = Some(Executor::HttpRemote(HttpRemoteExecutor {
            args: self.args_with_out_paths.clone(),
            state,
            url,
            files,
        }));
    }

    pub fn build(self, id: CommandId) -> Command {
        Command {
            id,
            name: self.name,
            executables: self.executables,
            inputs: self.inputs,
            outputs: self.outputs,
            deps: self.deps,
            executor: self.executor.unwrap(),
            tags: self.tags,
            is_excluded: false,
            unfinished_deps: vec![],
            reverse_deps: vec![],
            schedule_state: ScheduleState::New,
        }
    }
}
