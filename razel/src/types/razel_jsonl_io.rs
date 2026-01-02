use crate::types::{
    ExecutableType, File, RazelJson, RazelJsonCommand, RazelJsonTask, Target, TargetId, TargetKind,
};
use anyhow::{anyhow, Context, Result};
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::slice::Iter;

pub trait RazelJsonHandler {
    /// Set the directory to resolve relative paths of input/output files
    fn set_workspace_dir(&mut self, dir: &Path);
    fn push_json(&mut self, json: RazelJson) -> Result<TargetId>;
}

impl RazelJson {
    pub fn read(path: &str, handler: &mut impl RazelJsonHandler) -> Result<()> {
        handler.set_workspace_dir(Path::new(path).parent().unwrap());
        let file = BufReader::new(
            fs::File::open(path).with_context(|| anyhow!("failed to open {path:?}"))?,
        );
        for (line_number, line_result) in file.lines().enumerate() {
            let line = line_result?;
            let line_trimmed = line.trim();
            if line_trimmed.is_empty() || line_trimmed.starts_with("//") {
                continue;
            }
            let json: RazelJson = serde_json::from_str(line_trimmed).with_context(|| {
                format!("failed to parse {path}:{}\n{line_trimmed}", line_number + 1)
            })?;
            handler.push_json(json).with_context(|| {
                format!("failed to push {path}:{}\n{line_trimmed}", line_number + 1)
            })?;
        }
        Ok(())
    }

    pub fn write(
        targets: &Vec<Target>,
        files: &Vec<File>,
        out_dir: &Path,
        output: &Path,
    ) -> Result<()> {
        let mut writer = BufWriter::new(fs::File::create(output)?);
        for target in targets.iter() {
            let json = match &target.kind {
                TargetKind::Command(c) | TargetKind::Wasi(c) => {
                    let executable = if files[target.executables[0]].executable
                        == Some(ExecutableType::SystemExecutable)
                    {
                        files[target.executables[0]]
                            .path
                            .file_name()
                            .unwrap()
                            .to_string_lossy()
                            .to_string()
                    } else {
                        c.executable.clone()
                    };
                    RazelJson::Command(RazelJsonCommand {
                        name: target.name.clone(),
                        executable,
                        args: args_wo_out_dir(out_dir, c.args.iter()),
                        env: c.env.clone(),
                        inputs: target
                            .inputs
                            .iter()
                            .map(|x| maybe_strip_prefix(&files[*x].path, out_dir))
                            .collect(),
                        outputs: target
                            .outputs
                            .iter()
                            .map(|x| strip_prefix(&files[*x].path, out_dir))
                            .collect(),
                        stdout: c.stdout_file.as_ref().map(|x| x.to_str().unwrap().into()),
                        stderr: c.stderr_file.as_ref().map(|x| x.to_str().unwrap().into()),
                        deps: target
                            .deps
                            .iter()
                            .map(|x| targets[*x].name.clone())
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
                        args: args_wo_out_dir(out_dir, i),
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

fn strip_prefix(path: &Path, prefix: &Path) -> String {
    path.strip_prefix(prefix)
        .unwrap()
        .to_string_lossy()
        .to_string()
}

fn maybe_strip_prefix(path: &Path, prefix: &Path) -> String {
    path.strip_prefix(prefix)
        .unwrap_or(path)
        .to_string_lossy()
        .to_string()
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
