use super::Razel;
use crate::config::OUT_DIR;
use anyhow::Result;
use itertools::chain;
use regex::RegexSet;

impl Razel {
    pub fn filter_targets(&mut self, targets: &[String]) {
        self.exclude_all();
        self.include_matching(|x| targets.iter().any(|t| t == x));
    }

    pub fn filter_targets_regex(&mut self, patterns: &[String]) -> Result<()> {
        self.exclude_all();
        let regex = RegexSet::new(patterns)?;
        self.include_matching(|x| regex.is_match(x));
        Ok(())
    }

    pub fn filter_targets_regex_all(&mut self, patterns: &[String]) -> Result<()> {
        self.exclude_all();
        let regex = RegexSet::new(patterns)?;
        self.include_matching(|x| regex.matches(x).matched_all());
        Ok(())
    }

    fn exclude_all(&mut self) {
        for x in self.dep_graph.targets.iter_mut() {
            x.is_excluded = true;
        }
        for x in self.dep_graph.files.iter_mut() {
            x.is_excluded = true;
        }
    }

    fn include_matching(&mut self, is_match: impl Fn(&str) -> bool) {
        let dep_graph = &mut self.dep_graph;
        let mut matching_len: usize = 0;
        let mut to_include = vec![];
        for command in dep_graph.targets.iter().filter(|c| {
            is_match(&c.name)
                || c.outputs.iter().any(|x| {
                    let path = &dep_graph.files[*x].path;
                    let path_wo_out_dir = path.strip_prefix(OUT_DIR).unwrap();
                    is_match(path.to_str().unwrap()) || is_match(path_wo_out_dir.to_str().unwrap())
                })
        }) {
            matching_len += 1;
            to_include.push(command.id);
        }
        let mut included: usize = 0;
        while let Some(id) = to_include.pop() {
            let command = &mut dep_graph.targets[id];
            if !command.is_excluded {
                continue;
            }
            command.is_excluded = false;
            for x in chain!(&command.executables, &command.inputs, &command.outputs) {
                dep_graph.files[*x].is_excluded = false;
            }
            included += 1;
            for input_id in chain!(&command.executables, &command.inputs) {
                if let Some(dep) = dep_graph.creator_for_file.get(input_id) {
                    to_include.push(*dep);
                }
            }
            for dep in &command.deps {
                to_include.push(*dep);
            }
        }
        let deps_len = included - matching_len;
        tracing::info!("included {matching_len} targets with {deps_len} dependencies");
        self.excluded_targets_len = dep_graph.targets.len() - included;
    }
}
