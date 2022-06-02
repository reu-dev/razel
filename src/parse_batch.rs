use std::fs::File;
use std::io::{BufRead, BufReader};

use anyhow::Context;
use log::info;

use crate::{config, parse_cli, Rules, Scheduler};

pub fn parse_command(
    scheduler: &mut Scheduler,
    command_line: Vec<String>,
) -> Result<(), anyhow::Error> {
    let rules = Rules::new();
    create_command(scheduler, &rules, "command".into(), command_line.clone())
        .with_context(|| command_line.join(" "))
}

pub fn parse_batch_file(scheduler: &mut Scheduler, file_name: String) -> Result<(), anyhow::Error> {
    let rules = Rules::new();
    let file = File::open(&file_name)?;
    let file_buffered = BufReader::new(file);
    for (line_number, line) in file_buffered.lines().enumerate() {
        if let Ok(line) = line {
            let line_trimmed = line.trim();
            if line_trimmed.is_empty() || line_trimmed.starts_with("#") {
                continue;
            }
            let name = format!("{}:{}", &file_name, line_number + 1);
            let command_line: Vec<String> =
                line.split_whitespace().map(|x| x.to_string()).collect();
            create_command(scheduler, &rules, name.clone(), command_line.clone())
                .with_context(|| command_line.join(" "))
                .with_context(|| format!("Failed to add command: {name}"))?;
        }
    }
    info!("Added {} commands from {}", scheduler.len(), file_name);
    Ok(())
}

fn create_command(
    scheduler: &mut Scheduler,
    rules: &Rules,
    name: String,
    command_line: Vec<String>,
) -> Result<(), anyhow::Error> {
    if command_line.first().unwrap() == config::EXECUTABLE {
        parse_cli(command_line, scheduler, Some(name))?
    } else {
        if let Some(x) = rules.parse_command(&command_line)? {
            let mut i = command_line.into_iter();
            let program = i.next().unwrap();
            let args = i.collect();
            scheduler.push_custom_command(name, program, args, x.inputs, x.outputs)?;
        }
    }
    Ok(())
}
