use super::Razel;
use crate::executors::Executor;
use crate::types::{RazelJson, RazelJsonCommand, RazelJsonTask};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::slice::Iter;

impl Razel {
    pub fn write_jsonl(&self, output: &Path) -> Result<(), anyhow::Error> {
        let mut writer = BufWriter::new(File::create(output)?);
        for command in self.commands.iter() {
            let json = match &command.executor {
                Executor::CustomCommand(_) | Executor::Wasi(_) => {
                    RazelJson::Command(RazelJsonCommand {
                        name: command.name.clone(),
                        executable: self.files[*command.executables.first().unwrap()]
                            .arg
                            .clone(),
                        args: args_wo_out_dir(&self.out_dir, command.executor.args().iter()),
                        env: command.executor.env().cloned().unwrap_or_default(),
                        inputs: command
                            .inputs
                            .iter()
                            .map(|x| self.files[*x].arg.clone())
                            .collect(),
                        outputs: command
                            .outputs
                            .iter()
                            .map(|x| self.files[*x].arg.clone())
                            .collect(),
                        stdout: command
                            .executor
                            .stdout_file()
                            .map(|x| x.to_str().unwrap().into()),
                        stderr: command
                            .executor
                            .stderr_file()
                            .map(|x| x.to_str().unwrap().into()),
                        deps: command
                            .deps
                            .iter()
                            .map(|x| self.commands[*x].name.clone())
                            .collect(),
                        tags: command.tags.clone(),
                    })
                }
                Executor::Task(_) | Executor::HttpRemote(_) => {
                    let mut i = command.executor.args().iter();
                    i.next();
                    i.next();
                    RazelJson::Task(RazelJsonTask {
                        name: command.name.clone(),
                        task: i.next().unwrap().to_string(),
                        args: args_wo_out_dir(&self.out_dir, i),
                        tags: command.tags.clone(),
                    })
                }
            };
            writer.write_all(&serde_json::to_vec(&json)?)?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
        Ok(())
    }
}

fn args_wo_out_dir(out_dir: &Path, i: Iter<String>) -> Vec<String> {
    i.into_iter()
        .map(|x| {
            Path::new(x)
                .strip_prefix(out_dir)
                .ok()
                .map_or_else(|| x.into(), |x| x.to_str().unwrap().into())
        })
        .collect()
}
