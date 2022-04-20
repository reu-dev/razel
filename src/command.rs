use anyhow::bail;
use itertools::{chain, join};
use log::{info, warn};

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
    pub fn new_custom_command(
        name: String,
        executable: String,
        args: Vec<String>,
        inputs: Vec<String>,
        outputs: Vec<String>,
    ) -> Command {
        Command {
            name,
            command_line: join(chain([executable.clone()].iter(), args.iter()), " "),
            inputs,
            outputs,
            executor: Executor::CustomCommand(CustomCommandExecutor { executable, args }),
        }
    }

    pub fn new_task(
        name: String,
        args: Vec<String>,
        f: TaskFn,
        inputs: Vec<String>,
        outputs: Vec<String>,
    ) -> Command {
        Command {
            name,
            command_line: args.join(" "),
            inputs,
            outputs,
            executor: Executor::Task(TaskExecutor { f }),
        }
    }

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
    executable: String,
    args: Vec<String>,
}

impl CustomCommandExecutor {
    async fn exec(&self) -> Result<(), anyhow::Error> {
        info!("exec command: {} {}", self.executable, self.args.join(" "));
        let mut child = match tokio::process::Command::new(&self.executable)
            .env_clear()
            .args(&self.args)
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
    f: TaskFn,
}

impl TaskExecutor {
    pub async fn exec(&self) -> Result<(), anyhow::Error> {
        (self.f)()
    }
}

/*
pub struct File {
    path: String,
    is_data: bool,
    creating_command: Option<Box<Command>>,
}
*/
