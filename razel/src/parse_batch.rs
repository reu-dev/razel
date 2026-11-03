use crate::cli::{CommandArgs, parse_cli_within_file};
use crate::types::RazelJsonCommand;
use crate::types::RazelJsonHandler;
use crate::{ParseCommandResult, Razel, Rules, config};
use anyhow::{Context, Result, bail};
use itertools::Itertools;
use std::borrow::Cow;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use tracing::debug;

pub fn parse_command(
    razel: &mut Razel,
    rules: &Rules,
    name: String,
    args: CommandArgs,
) -> Result<()> {
    let CommandArgs {
        rule,
        inputs,
        outputs,
        command,
    } = args;
    let context = command.join(" ");
    let files = (!inputs.is_empty() || !outputs.is_empty())
        .then_some(ParseCommandResult { inputs, outputs });
    let mut rules = Cow::Borrowed(rules);
    if let Some(rule) = rule {
        rules
            .to_mut()
            .add(&rule)
            .with_context(|| format!("Invalid rule: {rule}"))?;
    }
    create_command(razel, &rules, name, command, files).context(context)
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
            shlex::split(line_trimmed)
                .context("Invalid quoting")
                .and_then(|command_line| {
                    create_command(razel, &rules, name.clone(), command_line, None)
                })
                .with_context(|| line_trimmed.to_string())
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
    files: Option<ParseCommandResult>,
) -> Result<()> {
    if command_line.first().unwrap() == config::EXECUTABLE {
        if files.is_some() {
            bail!("input/output files can't be specified for nested razel command");
        }
        return parse_cli_within_file(razel, rules, command_line, name, vec![]);
    }
    let (stdout, stderr) = parse_redirects(&mut command_line)?;
    let mut i = command_line.into_iter();
    let executable = i.next().unwrap();
    let args = i.collect_vec();
    let ParseCommandResult { inputs, outputs } = match files {
        Some(x) => x,
        None => rules.eval_command(&executable, &args)?.unwrap_or_default(),
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
        worker: vec![],
    })?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::new_tmp_dir;

    #[test]
    fn batch_file_rule_applies_to_razel_command() {
        let dir = new_tmp_dir!();
        let file = dir.join_and_write_file(
            "batch.sh",
            "# razel:rule copy <in> <out>\n# copy-in\nrazel command -- copy.wasm in out\n",
        );
        let mut razel = Razel::new();
        parse_batch_file(&mut razel, &file.to_str().unwrap().to_string()).unwrap();
        let target = &razel.targets_builder.as_ref().unwrap().targets[0];
        assert_eq!(target.name, "copy-in");
        assert_eq!(target.inputs.len(), 1);
        assert_eq!(target.outputs.len(), 1);
    }

    #[test]
    fn wasi_command_line_round_trip() {
        let mut razel = Razel::new();
        razel
            .push_json_command(RazelJsonCommand {
                name: "a".into(),
                executable: "x.wasm".into(),
                args: vec!["-i".into(), "in".into(), "out".into()],
                env: Default::default(),
                inputs: vec!["in".into()],
                outputs: vec!["out".into()],
                stdout: Some("stdout".into()),
                stderr: None,
                deps: vec![],
                tags: vec![],
                worker: vec![],
            })
            .unwrap();
        let builder = razel.targets_builder.as_ref().unwrap();
        let command_line = builder.targets[0].command_line_with_redirects(&builder.files);
        assert_eq!(
            command_line.join(" "),
            "razel command -i in -o razel-out/out -- ./x.wasm -i in razel-out/out > razel-out/stdout"
        );
        let mut reparsed = Razel::new();
        parse_cli_within_file(
            &mut reparsed,
            &Rules::new(),
            command_line,
            "b".into(),
            vec![],
        )
        .unwrap();
        let files_of = |razel: &Razel| {
            let builder = razel.targets_builder.as_ref().unwrap();
            let target = &builder.targets[0];
            let paths = |ids: &[usize]| {
                ids.iter()
                    .map(|x| builder.files[*x].path.clone())
                    .collect_vec()
            };
            (
                target.kind.args_with_executable(),
                paths(&target.inputs),
                paths(&target.outputs),
            )
        };
        assert_eq!(files_of(&reparsed), files_of(&razel));
    }

    #[test]
    fn batch_file_quoted_rule_arg() {
        let dir = new_tmp_dir!();
        let file = dir.join_and_write_file(
            "batch.sh",
            "razel command -r \"copy <in> <out>\" -- copy.wasm in out\n",
        );
        let mut razel = Razel::new();
        parse_batch_file(&mut razel, &file.to_str().unwrap().to_string()).unwrap();
        let target = &razel.targets_builder.as_ref().unwrap().targets[0];
        assert_eq!(target.inputs.len(), 1);
        assert_eq!(target.outputs.len(), 1);
    }
}
