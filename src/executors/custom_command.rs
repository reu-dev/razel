use anyhow::bail;
use itertools::{chain, join};
use log::warn;

pub struct CustomCommandExecutor {
    pub executable: String,
    pub args: Vec<String>,
}

impl CustomCommandExecutor {
    pub async fn exec(&self) -> Result<(), anyhow::Error> {
        // TODO add sandbox dir
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

    pub fn command_line(&self) -> String {
        join(
            chain([self.executable.clone()].iter(), self.args.iter()),
            " ",
        )
    }
}
