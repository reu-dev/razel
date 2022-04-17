use anyhow::bail;
use async_trait::async_trait;
use log::{info, warn};

pub struct File {
    path: String,
    is_data: bool,
    creating_command: Option<Box<dyn Command>>,
}

#[async_trait]
// TODO add file handling
pub trait Command {
    async fn exec(&mut self) -> Result<(), anyhow::Error>;
    //fn get_input_files(&self) -> Vec<&File>;
}

// TODO how to execute tasks
pub struct Task {}

pub struct CustomCommand {
    program: String,
    args: Vec<String>,
    //exit_status: Option<ExitStatus>,
    //error: Option<io::Error>,
}

impl CustomCommand {
    pub fn new(program: String, args: Vec<String>) -> CustomCommand {
        assert!(!program.is_empty());
        CustomCommand {
            program,
            args,
            //exit_status: None,
            //error: None,
        }
    }
}

#[async_trait]
impl Command for CustomCommand {
    async fn exec(&mut self) -> Result<(), anyhow::Error> {
        info!("exec command: {} {}", self.program, self.args.join(" "));
        let mut child = match tokio::process::Command::new(&self.program)
            .env_clear()
            .args(&self.args)
            .spawn()
        {
            Ok(child) => child,
            Err(e) => {
                warn!("command failed to start: {}\n{}", self.program, e);
                //self.error = Some(e);
                bail!("command failed to start: {}\n{}", self.program, e);
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
                        self.program
                    );
                }
            }
            Err(e) => {
                warn!("command failed: {}\n{}", self.program, e);
                //self.error = Some(e);
                bail!("command failed: {:?}\n{}", self.program, e);
            }
        }
    }
}
