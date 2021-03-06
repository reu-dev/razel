use std::collections::HashMap;
use std::path::PathBuf;

use crate::executors::{CustomCommandExecutor, Executor, TaskExecutor, TaskFn};
use crate::{ArenaId, FileId, ScheduleState, Scheduler};

pub struct Command {
    pub id: CommandId,
    pub name: String,
    pub inputs: Vec<FileId>,
    pub outputs: Vec<FileId>,
    pub executor: Executor,
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
    args_with_exec_paths: Vec<String>,
    args_with_out_paths: Vec<String>,
    inputs: Vec<FileId>,
    outputs: Vec<FileId>,
    executor: Option<Executor>,
}

impl CommandBuilder {
    pub fn new(name: String, args: Vec<String>) -> CommandBuilder {
        CommandBuilder {
            name,
            args_with_exec_paths: args.clone(),
            args_with_out_paths: args,
            inputs: vec![],
            outputs: vec![],
            executor: None,
        }
    }

    fn map_exec_path(&mut self, original: &String, mapped: &String) {
        self.args_with_exec_paths.iter_mut().for_each(|x| {
            if x == original {
                *x = mapped.clone()
            }
        });
    }

    fn map_out_path(&mut self, original: &String, mapped: &String) {
        self.args_with_out_paths.iter_mut().for_each(|x| {
            if x == original {
                *x = mapped.clone()
            }
        });
    }

    pub fn input(
        &mut self,
        path: &String,
        scheduler: &mut Scheduler,
    ) -> Result<PathBuf, anyhow::Error> {
        scheduler.input_file(path.clone()).map(|file| {
            self.map_exec_path(path, &file.exec_path.to_str().unwrap().into());
            self.map_out_path(path, &file.out_path.to_str().unwrap().into());
            self.inputs.push(file.id);
            file.out_path.clone()
        })
    }

    pub fn inputs(
        &mut self,
        paths: &Vec<String>,
        scheduler: &mut Scheduler,
    ) -> Result<Vec<PathBuf>, anyhow::Error> {
        self.inputs.reserve(paths.len());
        paths
            .iter()
            .map(|path| {
                let file = scheduler.input_file(path.clone())?;
                self.map_exec_path(path, &file.exec_path.to_str().unwrap().into());
                self.map_out_path(path, &file.out_path.to_str().unwrap().into());
                self.inputs.push(file.id);
                Ok(file.out_path.clone())
            })
            .collect()
    }

    pub fn output(
        &mut self,
        path: &String,
        scheduler: &mut Scheduler,
    ) -> Result<PathBuf, anyhow::Error> {
        scheduler.output_file(path).map(|file| {
            self.map_exec_path(path, &file.exec_path.to_str().unwrap().into());
            self.map_out_path(path, &file.out_path.to_str().unwrap().into());
            self.outputs.push(file.id);
            file.out_path.clone()
        })
    }

    pub fn outputs(
        &mut self,
        paths: &Vec<String>,
        scheduler: &mut Scheduler,
    ) -> Result<Vec<PathBuf>, anyhow::Error> {
        self.outputs.reserve(paths.len());
        paths
            .iter()
            .map(|path| {
                let file = scheduler.output_file(path)?;
                self.map_exec_path(path, &file.exec_path.to_str().unwrap().into());
                self.map_out_path(path, &file.out_path.to_str().unwrap().into());
                self.outputs.push(file.id);
                Ok(file.out_path.clone())
            })
            .collect()
    }

    pub fn custom_command_executor(
        &mut self,
        executable: String,
        env: HashMap<String, String>,
        scheduler: &mut Scheduler,
    ) -> Result<(), anyhow::Error> {
        let file = scheduler.executable(executable)?;
        self.inputs.push(file.id);
        self.executor = Some(Executor::CustomCommand(CustomCommandExecutor {
            executable: file.exec_path.to_str().unwrap().into(),
            args: self.args_with_out_paths.clone(),
            env,
        }));
        Ok(())
    }

    pub fn task_executor(&mut self, f: TaskFn) {
        self.executor = Some(Executor::Task(TaskExecutor {
            f,
            args: self.args_with_out_paths.clone(),
        }));
    }

    pub fn build(self, id: CommandId) -> Command {
        Command {
            id,
            name: self.name,
            inputs: self.inputs,
            outputs: self.outputs,
            executor: self.executor.unwrap(),
            unfinished_deps: vec![],
            reverse_deps: vec![],
            schedule_state: ScheduleState::New,
        }
    }
}
