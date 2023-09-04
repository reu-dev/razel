use std::collections::HashMap;
use std::path::Path;

use anyhow::{bail, Context};
use log::warn;

/// Rules to parse input/output file arguments from command lines
///
/// Specifying rules:
/// * first word is the program
/// * only input (<in>) and output (<out>) file arguments need to be specified, other arguments (<>) will be ignored
/// * multiple file arguments are marked with ...
///
/// Parsing commands using the rules:
/// * the first command line argument (program) selects the rule(s)
/// * positional arguments are parsed backwards from the command line
/// * then named arguments are parsed
/// * other arguments are ignored
pub struct Rules {
    rules: HashMap<String, Rule>,
}

impl Rules {
    pub fn new() -> Self {
        let mut s = Self {
            rules: Default::default(),
        };
        s.set_defaults();
        s
    }

    pub fn add(&mut self, spec: &str) -> Result<(), anyhow::Error> {
        let rule = Rule::new(spec)?;
        self.rules.insert(rule.executable.clone(), rule);
        Ok(())
    }

    pub fn parse_command(
        &self,
        command: &Vec<String>,
    ) -> Result<Option<CommandFileArgIndices>, anyhow::Error> {
        let executable_stem: String = Path::new(command.first().unwrap())
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .into();
        if let Some(rule) = self.rules.get(&executable_stem) {
            Ok(Some(rule.parse_command(command)?))
        } else {
            warn!("no rule for executable: {}", executable_stem);
            Ok(None)
        }
    }

    fn set_defaults(&mut self) {
        [
            "razel-test",
            "cp <in> <out>",
            "ar <out> <in>...",
            "c++ -MF <out> -o <out> <in>...",
            "cc  -MF <out> -o <out> <in>...",
            "sox <in>... <out>",
            // TODO cmake -E copy <in> <out>
        ]
        .iter()
        .for_each(|x| self.add(x).unwrap());
    }
}

impl Default for Rules {
    fn default() -> Self {
        Self::new()
    }
}

struct Rule {
    executable: String,
    options: HashMap<String, Arg>,
    positional_args: Vec<Arg>,
}

impl Rule {
    pub fn new(spec: &str) -> Result<Self, anyhow::Error> {
        let mut items = spec.split(' ').filter(|x| !x.is_empty());
        let executable = items.next().context("Rule is incomplete")?.into();
        let mut named_args: HashMap<String, Arg> = Default::default();
        let mut positional_args: Vec<Arg> = Default::default();
        let mut name: Option<&str> = None;
        for item in items {
            let arg = Self::parse_arg(item)?;
            match (name, arg) {
                (None, None) => name = Some(item),
                (Some(n), Some(a)) => {
                    named_args.insert(n.into(), a);
                    name = None;
                }
                (None, Some(a)) => positional_args.push(a),
                (Some(n), None) => bail!(format!("named argument without file spec: {n}")),
            }
        }
        if positional_args.iter().filter(|x| x.multiple).count() > 1 {
            bail!("only one positional argument might take multiple values");
        }
        Ok(Self {
            executable,
            options: named_args,
            positional_args,
        })
    }

    fn parse_arg(item: &str) -> Result<Option<Arg>, anyhow::Error> {
        let open = item.chars().filter(|x| *x == '<').count();
        let close = item.chars().filter(|x| *x == '>').count();
        Ok(if open == 0 && close == 0 {
            None
        } else if item == "<in>" {
            Some(Arg {
                file_type: ArgFileType::Input,
                multiple: false,
            })
        } else if item == "<in>..." {
            Some(Arg {
                file_type: ArgFileType::Input,
                multiple: true,
            })
        } else if item == "<out>" {
            Some(Arg {
                file_type: ArgFileType::Output,
                multiple: false,
            })
        } else if item == "<out>..." {
            Some(Arg {
                file_type: ArgFileType::Output,
                multiple: true,
            })
        } else if item == "<>" {
            Some(Arg {
                file_type: ArgFileType::NoFile,
                multiple: false,
            })
        } else {
            bail!(format!("syntax error: {item}"))
        })
    }

    pub fn parse_command(
        &self,
        command: &Vec<String>,
    ) -> Result<CommandFileArgIndices, anyhow::Error> {
        let mut command_files: CommandFileArgIndices = Default::default();
        let mut prev_option: Option<&Arg> = None;
        let mut i_positional = 1;
        for (i, item) in command.iter().enumerate().skip(1) {
            let mut is_option = true;
            let curr_option = self.options.get(item);
            match (prev_option, curr_option) {
                (None, None) => is_option = item.starts_with('-'),
                (None, Some(arg)) => prev_option = Some(arg),
                (Some(arg), None) => {
                    command_files.push(arg.file_type, item.clone());
                    if !arg.multiple {
                        prev_option = None;
                    }
                }
                (Some(_), Some(arg)) => {
                    prev_option = Some(arg);
                }
            }
            if is_option {
                i_positional = i + 1;
            }
        }
        if command.len() - i_positional < self.positional_args.len() {
            bail!("missing positional args");
        }
        for (i, arg) in self.positional_args.iter().enumerate() {
            if arg.multiple {
                let needed_later = self.positional_args.len() - 1 - i;
                while command.len() - i_positional > needed_later {
                    command_files.push(arg.file_type, command[i_positional].clone());
                    i_positional += 1;
                }
            } else {
                command_files.push(arg.file_type, command[i_positional].clone());
                i_positional += 1;
            }
        }
        Ok(command_files)
    }
}

struct Arg {
    file_type: ArgFileType,
    multiple: bool,
}

#[derive(Clone, Copy, PartialEq)]
enum ArgFileType {
    Input,
    Output,
    NoFile,
}

#[derive(Debug, Default)]
pub struct CommandFileArgIndices {
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
}

impl CommandFileArgIndices {
    fn push(&mut self, arg_file_type: ArgFileType, path: String) {
        match arg_file_type {
            ArgFileType::Input => self.inputs.push(path),
            ArgFileType::Output => self.outputs.push(path),
            ArgFileType::NoFile => {}
        }
    }
}
