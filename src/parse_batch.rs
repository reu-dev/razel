use anyhow::Context;
use log::info;
use std::fs::File;
use std::io::{BufRead, BufReader};

use crate::{config, parse_cli, Scheduler};

pub fn parse_command(
    scheduler: &mut Scheduler,
    mut command_line: Vec<String>,
) -> Result<(), anyhow::Error> {
    let program = command_line.drain(1..).collect();
    create_command(scheduler, "command".into(), program, command_line)
}

pub fn parse_batch_file(scheduler: &mut Scheduler, file_name: String) -> Result<(), anyhow::Error> {
    let file = File::open(&file_name)?;
    let file_buffered = BufReader::new(file);
    for (line_number, line) in file_buffered.lines().enumerate() {
        if let Ok(line) = line {
            let line_trimmed = line.trim();
            if line_trimmed.starts_with("#") {
                continue;
            }
            let name = format!("{}:{}", &file_name, line_number + 1);
            let mut split = line.split_whitespace().map(|x| x.to_string());
            let program = split.next().unwrap();
            let args = split.collect();
            create_command(scheduler, name, program, args)?;
        }
    }
    info!("Added {} commands from {}", scheduler.len(), file_name);
    Ok(())
}

fn create_command(
    scheduler: &mut Scheduler,
    name: String,
    program: String,
    mut args: Vec<String>,
) -> Result<(), anyhow::Error> {
    if program == config::EXECUTABLE {
        args.insert(0, config::EXECUTABLE.to_string());
        parse_cli(args.clone(), scheduler, Some(name.clone()))
            .with_context(|| format!("{}\n{}", name, args.join(" ")))?
    } else {
        scheduler.push_custom_command(name, program, args, vec![], vec![])?
    }
    Ok(())
}
