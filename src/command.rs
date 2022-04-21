use std::rc::Rc;

use anyhow::bail;
use log::warn;

use crate::File;

pub enum StringOrFileArg {
    String(String),
    File(Rc<File>),
}

pub type TaskFn = Box<dyn Fn() -> Result<(), anyhow::Error>>;

pub struct Command {
    pub name: String,
    /// for showing errors
    pub command_line: String,
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
    pub executor: Executor,
    //exit_status: Option<ExitStatus>,
    //error: Option<io::Error>,
}

impl Command {
    pub async fn exec(&mut self) -> Result<(), anyhow::Error> {
        match &self.executor {
            Executor::CustomCommand(c) => c.exec().await,
            Executor::Task(t) => t.exec().await,
        }
    }
}

pub enum Executor {
    CustomCommand(CustomCommandExecutor),
    Task(TaskExecutor),
}

pub struct CustomCommandExecutor {
    pub executable: String,
    pub string_or_file_args: Vec<StringOrFileArg>,
}

impl CustomCommandExecutor {
    async fn exec(&self) -> Result<(), anyhow::Error> {
        // TODO add sandbox dir
        let args: Vec<&String> = self
            .string_or_file_args
            .iter()
            .map(|a| match a {
                StringOrFileArg::String(x) => x,
                StringOrFileArg::File(x) => &x.path,
            })
            .collect();
        let mut child = match tokio::process::Command::new(&self.executable)
            .env_clear()
            .args(&args)
            .spawn()
        {
            Ok(child) => child,
            Err(e) => {
                warn!("command failed to start: {}\n{}", self.executable, e);
                //self.error = Some(e);
                bail!("command failed to start: {}\n{}", self.executable, e);
            }
        };
        match child.wait().await {
            Ok(exit_status) => {
                //self.exit_status = Some(exit_status);
                if exit_status.success() {
                    Ok(())
                } else {
                    bail!(
                        "command exit status {:?}\n{}",
                        exit_status.code(),
                        self.executable
                    );
                }
            }
            Err(e) => {
                warn!("command failed: {}\n{}", self.executable, e);
                //self.error = Some(e);
                bail!("command failed: {:?}\n{}", self.executable, e);
            }
        }
    }
}

pub struct TaskExecutor {
    pub f: TaskFn,
}

impl TaskExecutor {
    pub async fn exec(&self) -> Result<(), anyhow::Error> {
        (self.f)()
    }
}
