use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::Context;
use log::debug;
use serde::Deserialize;

use crate::metadata::Tag;
use crate::{config, parse_cli_within_file, Razel};

pub fn parse_jsonl_file(razel: &mut Razel, file_name: &String) -> Result<(), anyhow::Error> {
    razel.set_workspace_dir(Path::new(file_name).parent().unwrap())?;
    let file = File::open(file_name).with_context(|| file_name.clone())?;
    let file_buffered = BufReader::new(file);
    for (line_number, line_result) in file_buffered.lines().enumerate() {
        let line = line_result?;
        let line_trimmed = line.trim();
        if line_trimmed.is_empty() || line_trimmed.starts_with("//") {
            continue;
        }
        let json: RazelJson = serde_json::from_str(line_trimmed).with_context(|| {
            format!(
                "failed to parse {}:{}\n{}",
                file_name,
                line_number + 1,
                line_trimmed
            )
        })?;
        match json {
            RazelJson::CustomCommand(c) => {
                razel.push_custom_command(
                    c.name,
                    c.executable,
                    c.args,
                    c.env,
                    c.inputs,
                    c.outputs,
                    c.stdout,
                    c.stderr,
                    c.deps,
                    c.tags,
                )?;
            }
            RazelJson::Task(t) => {
                let mut args: Vec<String> = vec![config::EXECUTABLE.into(), "task".into(), t.task];
                args.extend(&mut t.args.iter().map(|x| x.into()));
                parse_cli_within_file(razel, args.clone(), &t.name, t.tags)
                    .with_context(|| format!("{}\n{}", t.name, args.join(" ")))?
            }
        }
    }
    debug!("Added {} commands from {file_name}", razel.len());
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, untagged)]
pub enum RazelJson {
    CustomCommand(RazelCustomCommandJson),
    Task(RazelTaskJson),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RazelCustomCommandJson {
    pub name: String,
    pub executable: String,
    pub args: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub inputs: Vec<String>,
    #[serde(default)]
    pub outputs: Vec<String>,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    #[serde(default)]
    pub deps: Vec<String>,
    #[serde(default)]
    pub tags: Vec<Tag>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RazelTaskJson {
    pub name: String,
    pub task: String,
    pub args: Vec<String>,
    #[serde(default)]
    pub tags: Vec<Tag>,
}
