use crate::CommandBuilder;
use std::collections::HashMap;

use anyhow::{bail, Context};

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
struct Rules {
    rules: HashMap<String, Rule>,
}

impl Rules {
    pub fn new() -> Self {
        Self {
            rules: Default::default(),
        }
    }

    pub fn add(&mut self, rule: &str) {}

    fn set_defaults(&mut self) {
        vec![
            "cp <in> <out>",
            "ar <out> <in>...",
            "c++ -MF <out> -o <out> <in>...",
            "cc  -MF <out> -o <out> <in>...",
        ]
        .iter()
        .for_each(|x| self.add(x));
    }
}

struct Rule {
    executable: String,
    named_args: HashMap<String, Arg>,
    positional_args: Vec<Arg>,
}

impl Rule {
    pub fn from_string(spec: &str) -> Result<Self, anyhow::Error> {
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
            named_args,
            positional_args,
        })
    }

    fn parse_arg(item: &str) -> Result<Option<Arg>, anyhow::Error> {
        let open = item.chars().into_iter().filter(|x| *x == '<').count();
        let close = item.chars().into_iter().filter(|x| *x == '>').count();
        Ok(if open == 0 && close == 0 {
            None
        } else if item == "<in>" {
            Some(Arg {
                file_type: FileType::Input,
                multiple: false,
            })
        } else if item == "<in>..." {
            Some(Arg {
                file_type: FileType::Input,
                multiple: true,
            })
        } else if item == "<out>" {
            Some(Arg {
                file_type: FileType::Output,
                multiple: false,
            })
        } else if item == "<out>..." {
            Some(Arg {
                file_type: FileType::Output,
                multiple: true,
            })
        } else if item == "<>" {
            Some(Arg {
                file_type: FileType::NoFile,
                multiple: false,
            })
        } else {
            bail!(format!("syntax error: {item}"))
        })
    }

    pub fn parse(command_line: &str) -> CommandBuilder {
        todo!()
    }
}

struct Arg {
    file_type: FileType,
    multiple: bool,
}

enum FileType {
    Input,
    Output,
    NoFile,
}
