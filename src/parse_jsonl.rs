use std::fs::File;
use std::io::{BufRead, BufReader};

use anyhow::Context;
use log::info;
use serde::Deserialize;

use crate::{config, parse_cli, Scheduler};

pub fn parse_jsonl_file(scheduler: &mut Scheduler, file_name: String) -> Result<(), anyhow::Error> {
    let file = File::open(&file_name)?;
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
                scheduler.push_custom_command(c.name, c.executable, c.args, c.inputs, c.outputs)?;
            }
            RazelJson::Task(t) => {
                let mut args: Vec<String> =
                    vec![config::EXECUTABLE.into(), "task".into(), t.task.into()];
                args.extend(&mut t.args.iter().map(|x| x.into()));
                parse_cli(args.clone(), scheduler, Some(t.name.clone()))
                    .with_context(|| format!("{}\n{}", t.name, args.join(" ")))?
            }
        }
    }
    info!("Added {} commands from {}", scheduler.len(), file_name);
    Ok(())
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RazelJson {
    CustomCommand(RazelCustomCommandJson),
    Task(RazelTaskJson),
}

#[derive(Deserialize)]
struct RazelCustomCommandJson {
    name: String,
    executable: String,
    args: Vec<String>,
    inputs: Vec<String>,
    outputs: Vec<String>,
}

#[derive(Deserialize)]
struct RazelTaskJson {
    name: String,
    task: String,
    args: Vec<String>,
}
