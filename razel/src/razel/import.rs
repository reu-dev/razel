use super::Razel;
use crate::types::{RazelJson, RazelJsonCommand, RazelJsonTask, TargetKind};
use anyhow::Result;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::slice::Iter;

impl Razel {
    pub fn write_jsonl(&self, output: &Path) -> Result<()> {
        let mut writer = BufWriter::new(File::create(output)?);
        for target in self.dep_graph.targets.iter() {
            let json = match &target.kind {
                TargetKind::Command(c) | TargetKind::Wasi(c) => {
                    RazelJson::Command(RazelJsonCommand {
                        name: target.name.clone(),
                        executable: c.executable.clone(),
                        args: args_wo_out_dir(&self.out_dir, c.args.iter()),
                        env: c.env.clone(),
                        inputs: target
                            .inputs
                            .iter()
                            .map(|x| self.dep_graph.files[*x].path.to_str().unwrap().into())
                            .collect(),
                        outputs: target
                            .outputs
                            .iter()
                            .map(|x| self.dep_graph.files[*x].path.to_str().unwrap().into())
                            .collect(),
                        stdout: c.stdout_file.as_ref().map(|x| x.to_str().unwrap().into()),
                        stderr: c.stderr_file.as_ref().map(|x| x.to_str().unwrap().into()),
                        deps: target
                            .deps
                            .iter()
                            .map(|x| self.dep_graph.targets[*x].name.clone())
                            .collect(),
                        tags: target.tags.clone(),
                    })
                }
                TargetKind::Task(t) | TargetKind::HttpRemoteExecTask(t) => {
                    let mut i = t.args.iter();
                    i.next(); // "task"
                    let task = i.next().unwrap().to_string();
                    RazelJson::Task(RazelJsonTask {
                        name: target.name.clone(),
                        task,
                        args: args_wo_out_dir(&self.out_dir, i),
                        tags: target.tags.clone(),
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
