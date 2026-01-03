use crate::cli::parse_cli_within_file;
use crate::types::RazelJsonCommand;
use crate::types::RazelJsonHandler;
use crate::{Razel, Rules, config};
use anyhow::{Context, Result, bail};
use itertools::Itertools;
use log::debug;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

pub fn parse_command(razel: &mut Razel, command_line: Vec<String>) -> Result<()> {
    let rules = Rules::new();
    create_command(razel, &rules, "command".into(), command_line.clone())
        .with_context(|| command_line.join(" "))
}

pub fn parse_batch_file(razel: &mut Razel, file_name: &String) -> Result<()> {
    razel.set_workspace_dir(Path::new(file_name).parent().unwrap());
    let mut rules = Rules::new();
    let file = File::open(file_name).with_context(|| file_name.clone())?;
    let file_buffered = BufReader::new(file);
    let mut len: usize = 0;
    let mut next_name = None;
    for (line_number, line) in file_buffered.lines().enumerate() {
        if let Ok(line) = line {
            let line_trimmed = line.trim();
            if let Some(comment) = line_trimmed.strip_prefix("#").map(str::trim_start) {
                if let Some(rule) = comment.strip_prefix("razel:rule") {
                    rules.add(rule.trim())?;
                } else if !comment.contains(' ') {
                    next_name = Some(comment.to_string());
                }
                continue;
            } else if line_trimmed.is_empty() {
                next_name = None;
                continue;
            }
            let name = next_name
                .take()
                .unwrap_or_else(|| format!("{}:{}", file_name, line_number + 1));
            let command_line: Vec<String> =
                line.split_whitespace().map(|x| x.to_string()).collect();
            create_command(razel, &rules, name.clone(), command_line.clone())
                .with_context(|| command_line.join(" "))
                .with_context(|| format!("Failed to add command: {name}"))?;
            len += 1;
        }
    }
    debug!("Added {len} commands from {file_name}");
    Ok(())
}

fn create_command(
    razel: &mut Razel,
    rules: &Rules,
    name: String,
    mut command_line: Vec<String>,
) -> Result<()> {
    if command_line.first().unwrap() == config::EXECUTABLE {
        parse_cli_within_file(razel, command_line, name, vec![])?
    } else {
        let (stdout, stderr) = parse_redirects(&mut command_line)?;
        let mut i = command_line.into_iter();
        let executable = i.next().unwrap();
        let args = i.collect_vec();
        let (inputs, outputs) = if let Some(files) = rules.eval_command(&executable, &args)? {
            (files.inputs, files.outputs)
        } else {
            (Default::default(), Default::default())
        };
        razel.push_json_command(RazelJsonCommand {
            name,
            executable,
            args,
            env: Default::default(),
            inputs,
            outputs,
            stdout,
            stderr,
            deps: vec![],
            tags: vec![],
        })?;
    }
    Ok(())
}

/// Parse and drop stdout/stderr redirects from a command line
fn parse_redirects(cmd: &mut Vec<String>) -> Result<(Option<String>, Option<String>)> {
    let mut stdout = None;
    let mut stderr = None;
    // [executable, redirect, file]
    let mut r = 1;
    while r < cmd.len() {
        let arg = cmd[cmd.len() - r].as_str();
        match arg {
            ">" | "1>" => {
                if r != 2 {
                    bail!("Redirect in wrong position: {arg}");
                }
                if stdout.replace(cmd.pop().unwrap()).is_some() {
                    bail!("Multiple stdout redirects are not supported.")
                }
            }
            "2>" => {
                if r != 2 {
                    bail!("Redirect in wrong position: {arg}");
                }
                if stderr.replace(cmd.pop().unwrap()).is_some() {
                    bail!("Multiple stderr redirects are not supported.")
                }
            }
            ">>" | "1>>" | "2>>" | "&2>" => bail!("Redirect is not supported: {arg}"),
            _ => {
                r += 1;
                continue;
            }
        }
        cmd.pop();
        r = 1;
    }
    Ok((stdout, stderr))
}
