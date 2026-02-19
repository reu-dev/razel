use anyhow::{Context, Result, anyhow, bail};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use tracing::warn;

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
/// * stdout/stderr redirects should be handled beforehand
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

    pub fn read(&mut self, path: &Path) -> Result<()> {
        let file = File::open(path).with_context(|| anyhow!("{path:?}"))?;
        let file_buffered = BufReader::new(file);
        for (line_number, line) in file_buffered.lines().enumerate() {
            if let Ok(line) = line {
                let line_trimmed = line.trim();
                if let Some(comment) = line_trimmed.strip_prefix("#").map(str::trim_start) {
                    if let Some(rule) = comment.strip_prefix("razel:rule") {
                        self.add(rule.trim()).with_context(|| {
                            anyhow!("{}:{}", path.to_string_lossy(), line_number + 1)
                        })?;
                    }
                    continue;
                } else if line_trimmed.is_empty() {
                    continue;
                }
                self.add(line_trimmed)
                    .with_context(|| anyhow!("{}:{}", path.to_string_lossy(), line_number + 1))?;
            }
        }
        Ok(())
    }

    pub fn add(&mut self, spec: &str) -> Result<()> {
        let rule = Rule::new(spec)?;
        self.rules.insert(rule.executable.clone(), rule);
        Ok(())
    }

    pub fn eval_command(
        &self,
        executable: &str,
        args: &[String],
    ) -> Result<Option<ParseCommandResult>> {
        if args.is_empty() {
            return Ok(None);
        }
        let executable_stem: String = Path::new(executable)
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .into();
        if let Some(rule) = self.rules.get(&executable_stem) {
            Ok(Some(rule.eval_args(args)?))
        } else if executable_stem == "sox" {
            Ok(Some(Self::eval_sox(args)?))
        } else {
            warn!("no rule for executable: {executable_stem}");
            Ok(None)
        }
    }

    /// Hardcoded parser for sox, whose general form is:
    ///   sox [global-opts] [format-opts] infile... outfile [effect [effect-opts]...]
    ///
    /// Strategy:
    /// - Walk args left-to-right, skipping flags and their values.
    /// - Collect plain positional items until the first recognised effect keyword.
    /// - Everything collected is a file: all but the last are inputs, the last is output.
    fn eval_sox(args: &[String]) -> Result<ParseCommandResult> {
        /// Options that consume the next token as their value.
        const VALUE_FLAGS: &[&str] = &[
            "-t",
            "--type",
            "-b",
            "--bits",
            "-e",
            "--encoding",
            "-r",
            "--rate",
            "-c",
            "--channels",
            "-L",
            "-B",
            "-x", // endianness (no value, but harmless to list here)
            "--combine",
            "-v",
            "--volume",
            "--ignore-length",
        ];
        /// Known effect names; seeing one of these ends the file-argument section.
        const EFFECTS: &[&str] = &[
            "trim",
            "rate",
            "vol",
            "norm",
            "gain",
            "remix",
            "channels",
            "silence",
            "pad",
            "fade",
            "speed",
            "pitch",
            "reverb",
            "echo",
            "equalizer",
            "bass",
            "treble",
            "highpass",
            "lowpass",
            "bandpass",
            "bandreject",
            "allpass",
            "flanger",
            "phaser",
            "overdrive",
            "stat",
            "stats",
            "spectrogram",
            "noiseprof",
            "noisered",
            "compand",
            "dither",
            "repeat",
            "reverse",
            "swap",
            "synth",
            "newfile",
            "restart",
        ];
        let mut files: Vec<String> = Vec::new();
        let mut skip_next = false;
        for arg in args {
            if skip_next {
                skip_next = false;
                continue;
            }
            if VALUE_FLAGS.contains(&arg.as_str()) {
                skip_next = true;
                continue;
            }
            // Any remaining flag (starts with '-') is a boolean global option — skip it.
            if arg.starts_with('-') {
                continue;
            }
            // First recognised effect keyword ends the file section.
            if EFFECTS.contains(&arg.as_str()) {
                break;
            }
            files.push(arg.clone());
        }
        if files.len() < 2 {
            bail!(
                "sox: expected at least one input and one output file, got {:?}",
                files
            );
        }
        let output = files.pop().unwrap();
        Ok(ParseCommandResult {
            inputs: files,
            outputs: vec![output],
        })
    }

    fn set_defaults(&mut self) {
        [
            "razel-self-test",
            "ar <out> <in>...",
            "c++ -MF <out> -o <out> <in>...",
            "cc  -MF <out> -o <out> <in>...",
            "clang -o <out> <in>...",
            "cp <in> <out>",
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
    pub fn new(spec: &str) -> Result<Self> {
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

    fn parse_arg(item: &str) -> Result<Option<Arg>> {
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

    pub fn eval_args(&self, items: &[String]) -> Result<ParseCommandResult> {
        let mut files: ParseCommandResult = Default::default();
        let mut prev_option: Option<&Arg> = None;
        let mut positionals_missing = self.positional_args.len();
        let mut first_positional = 0;
        for (i, item) in items
            .iter()
            .enumerate()
            .take(items.len().saturating_sub(positionals_missing))
        {
            let mut is_option = true;
            let curr_option = self.options.get(item);
            match (prev_option, curr_option) {
                (None, None) => is_option = item.starts_with('-'),
                (None, Some(arg)) => prev_option = Some(arg),
                (Some(arg), None) => {
                    files.push(arg.file_type, item.clone());
                    if !arg.multiple {
                        prev_option = None;
                    }
                }
                (Some(_), Some(arg)) => {
                    prev_option = Some(arg);
                }
            }
            if is_option {
                first_positional = i + 1;
            }
        }
        if items.len().saturating_sub(first_positional) < positionals_missing {
            bail!(
                "expected {positionals_missing} positional arguments, found only {}",
                items.len().saturating_sub(first_positional)
            );
        }
        let mut positional_files: ParseCommandResult = Default::default();
        let mut len = items.len();
        for arg in self.positional_args.iter().rev() {
            positionals_missing -= 1;
            if arg.multiple {
                while len - positionals_missing > first_positional {
                    len -= 1;
                    positional_files.push(arg.file_type, items[len].clone());
                }
            } else {
                len -= 1;
                positional_files.push(arg.file_type, items[len].clone());
            }
        }
        assert_eq!(positionals_missing, 0);
        files
            .inputs
            .extend(positional_files.inputs.into_iter().rev());
        files
            .outputs
            .extend(positional_files.outputs.into_iter().rev());
        Ok(files)
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

#[derive(Debug, Default, PartialEq)]
pub struct ParseCommandResult {
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
}

impl ParseCommandResult {
    fn push(&mut self, arg_file_type: ArgFileType, path: String) {
        match arg_file_type {
            ArgFileType::Input => self.inputs.push(path),
            ArgFileType::Output => self.outputs.push(path),
            ArgFileType::NoFile => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::check;
    use itertools::Itertools;

    impl Rules {
        fn test(&self, command: &str, exp_inputs: &[&str], exp_outputs: &[&str]) {
            let command = command
                .split_whitespace()
                .map(|x| x.to_string())
                .collect_vec();
            let (executable, args) = command.split_first().unwrap();
            let files = self
                .eval_command(executable, args)
                .unwrap()
                .unwrap_or_default();
            check!(files.inputs == exp_inputs);
            check!(files.outputs == exp_outputs);
        }

        fn test_fail(&self, command: &str) {
            let command = command
                .split_whitespace()
                .map(|x| x.to_string())
                .collect_vec();
            let (executable, args) = command.split_first().unwrap();
            let result = self.eval_command(executable, args);
            check!(result.is_err());
        }
    }

    #[test]
    fn rules_should_pass() {
        let rules = Rules::new();
        check!(
            rules
                .eval_command("some-executable-without-rule", &[])
                .unwrap()
                .is_none()
        );
        rules.test("some-executable-without-files", &[], &[]);
        rules.test("cp in out", &["in"], &["out"]);
        rules.test("c++ -MF out1 -o out2 in1", &["in1"], &["out1", "out2"]);
        rules.test(
            "c++ -MF out1 -o out2 in1 in2",
            &["in1", "in2"],
            &["out1", "out2"],
        );
        rules.test("clang -O3 -o out.a src.c", &["src.c"], &["out.a"]);
    }

    #[test]
    fn rules_should_fail() {
        let rules = Rules::new();
        rules.test_fail("cp a");
    }

    #[test]
    fn rules_sox() {
        let rules = Rules::new();
        rules.test("sox in.wav out.wav", &["in.wav"], &["out.wav"]);
        rules.test(
            "sox in1.wav in2.wav in3.wav out.wav",
            &["in1.wav", "in2.wav", "in3.wav"],
            &["out.wav"],
        );
        rules.test("sox -t raw in.raw out.wav", &["in.raw"], &["out.wav"]);
        rules.test(
            "sox -r 44100 -b 16 -e signed in.raw out.wav",
            &["in.raw"],
            &["out.wav"],
        );
        rules.test(
            "sox in1.wav in2.wav --combine merge out.wav",
            &["in1.wav", "in2.wav"],
            &["out.wav"],
        );
        rules.test("sox in.wav out.wav trim 1", &["in.wav"], &["out.wav"]);
        rules.test("sox in.wav out.wav trim 0 1", &["in.wav"], &["out.wav"]);
        rules.test("sox in.wav out.wav rate 22050", &["in.wav"], &["out.wav"]);
        rules.test(
            "sox in1.wav in2.wav --combine merge out.wav trim 0 10",
            &["in1.wav", "in2.wav"],
            &["out.wav"],
        );
    }
}
