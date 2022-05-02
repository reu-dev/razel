use crate::CommandBuilder;
use std::collections::HashMap;

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
            "ar <out:.a> <in:.o>",
            "c++ -MF <out> -o <out> -c <in>",
            "c++ -o <out> <in:.o> <in:.a>",
            "cc -MF <out> -o <out> -c <in>",
        ]
        .iter()
        .map(|x| self.add(x));
    }
}

struct Rule {
    executable: String,
    named_args: HashMap<String, Arg>,
}

impl Rule {
    pub fn from_string(s: &str) -> Self {}

    pub fn parse(command: &str) -> CommandBuilder {}
}

struct Arg {
    file_type: FileType,
}

enum FileType {
    Input,
    Output,
}
