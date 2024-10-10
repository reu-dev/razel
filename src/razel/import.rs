use super::Razel;
use crate::executors::Executor;
use crate::RazelCustomCommandJson;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

impl Razel {
    pub fn write_jsonl(&self, output: &PathBuf) -> Result<(), anyhow::Error> {
        let mut writer = BufWriter::new(File::create(output)?);
        for command in self.commands.iter() {
            let json = match &command.executor {
                Executor::CustomCommand(exec) => RazelCustomCommandJson {
                    name: command.name.clone(),
                    executable: exec.executable.clone(),
                    args: exec.args.clone(),
                    env: exec.env.clone(),
                    inputs: command
                        .inputs
                        .iter()
                        .map(|x| self.files[*x].path.to_str().unwrap().into())
                        .collect(),
                    outputs: command
                        .outputs
                        .iter()
                        .map(|x| self.files[*x].path.to_str().unwrap().into())
                        .collect(),
                    stdout: None,
                    stderr: None,
                    deps: vec![],
                    tags: vec![],
                },
                _ => todo!(),
            };
            writer.write_all(&serde_json::to_vec(&json)?)?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
        Ok(())
    }
}
