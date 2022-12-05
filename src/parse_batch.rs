use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::Context;
use log::info;

use crate::{config, parse_cli_within_file, Razel, Rules};

pub fn parse_command(razel: &mut Razel, command_line: Vec<String>) -> Result<(), anyhow::Error> {
    let rules = Rules::new();
    create_command(razel, &rules, "command".into(), command_line.clone())
        .with_context(|| command_line.join(" "))
}

pub fn parse_batch_file(razel: &mut Razel, file_name: &String) -> Result<(), anyhow::Error> {
    razel.set_workspace_dir(Path::new(file_name).parent().unwrap())?;
    let rules = Rules::new();
    let file = File::open(file_name).with_context(|| file_name.clone())?;
    let file_buffered = BufReader::new(file);
    for (line_number, line) in file_buffered.lines().enumerate() {
        if let Ok(line) = line {
            let line_trimmed = line.trim();
            if line_trimmed.is_empty() || line_trimmed.starts_with('#') {
                continue;
            }
            let name = format!("{}:{}", file_name, line_number + 1);
            let command_line: Vec<String> =
                line.split_whitespace().map(|x| x.to_string()).collect();
            create_command(razel, &rules, name.clone(), command_line.clone())
                .with_context(|| command_line.join(" "))
                .with_context(|| format!("Failed to add command: {name}"))?;
        }
    }
    info!("Added {} commands from {}", razel.len(), file_name);
    Ok(())
}

fn create_command(
    razel: &mut Razel,
    rules: &Rules,
    name: String,
    command_line: Vec<String>,
) -> Result<(), anyhow::Error> {
    if command_line.first().unwrap() == config::EXECUTABLE {
        parse_cli_within_file(razel, command_line, &name)?
    } else {
        let (stdout, stderr) = (None, None); // TODO parse redirects
        let (inputs, outputs) = if let Some(files) = rules.parse_command(&command_line)? {
            (files.inputs, files.outputs)
        } else {
            (Default::default(), Default::default())
        };
        let mut i = command_line.into_iter();
        let program = i.next().unwrap();
        let args = i.collect();
        razel.push_custom_command(
            name,
            program,
            args,
            Default::default(),
            inputs,
            outputs,
            stdout,
            stderr,
        )?;
    }
    Ok(())
}
