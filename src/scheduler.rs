use std::collections::HashMap;
use std::rc::Rc;

use anyhow::Context;
use itertools::{chain, join};
use log::info;

use crate::{Command, CustomCommandExecutor, Executor, StringOrFileArg, TaskExecutor, TaskFn};

pub struct Scheduler {
    files: HashMap<String, Rc<File>>,
    queue: Vec<Box<Command>>,
}

impl Scheduler {
    pub fn new() -> Scheduler {
        Scheduler {
            files: Default::default(),
            queue: Default::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn push_custom_command(
        &mut self,
        name: String,
        executable: String,
        args: Vec<String>,
        inputs: Vec<String>,
        outputs: Vec<String>,
    ) -> Result<(), anyhow::Error> {
        let input_files: Vec<Rc<File>> = inputs.iter().map(|x| self.input_file(x)).collect();
        let output_files: Vec<Rc<File>> = outputs.iter().map(|x| self.output_file(x)).collect();
        let string_or_file_args = args
            .iter()
            .map(|a| {
                input_files
                    .iter()
                    .find(|f| f.arg == *a)
                    .or_else(|| output_files.iter().find(|f| f.arg == *a))
                    .map(|f| StringOrFileArg::File(f.clone()))
                    .unwrap_or_else(|| StringOrFileArg::String(a.clone()))
            })
            .collect();

        self.push(Box::new(Command {
            name,
            command_line: join(chain([executable.clone()].iter(), args.iter()), " "),
            inputs,
            outputs,
            executor: Executor::CustomCommand(CustomCommandExecutor {
                executable,
                string_or_file_args,
            }),
        }))?;
        Ok(())
    }

    pub fn push_task(
        &mut self,
        name: String,
        args: Vec<String>,
        f: TaskFn,
        inputs: Vec<String>,
        outputs: Vec<String>,
    ) -> Result<(), anyhow::Error> {
        self.push(Box::new(Command {
            name,
            command_line: args.join(" "),
            inputs,
            outputs,
            executor: Executor::Task(TaskExecutor { f }),
        }))?;
        Ok(())
    }

    fn push(&mut self, command: Box<Command>) -> Result<(), anyhow::Error> {
        // TODO check if name is unique
        // TODO patch outputs.creating_command
        self.queue.push(command);
        Ok(())
    }

    pub async fn run(&mut self) -> Result<(), anyhow::Error> {
        for command in self.queue.iter_mut() {
            info!("Execute {}: {}", command.name, command.command_line);
            command
                .exec()
                .await
                .with_context(|| format!("{}\n{}", command.name, command.command_line))?
        }
        Ok(())
    }

    fn input_file(&mut self, path: &String) -> Rc<File> {
        Rc::new(File {
            arg: path.clone(),
            is_data: false, // TODO
            creating_command: None,
            path: "TODO".into(), // TODO
        })
    }

    fn output_file(&mut self, path: &String) -> Rc<File> {
        Rc::new(File {
            arg: path.clone(),
            is_data: false,
            creating_command: None,
            path: format!("{}/{}", crate::config::BIN_DIR, path),
        })
    }
}

pub struct File {
    /// string argument from original command line
    arg: String,
    /// data files can only be used as inputs and must exist before running any commands
    is_data: bool,
    creating_command: Option<Box<Command>>,
    /// path to be used for exec
    pub path: String,
}
