use super::TargetsBuilder;
use crate::config::OUT_DIR;
use anyhow::Result;
use itertools::chain;
use regex::RegexSet;

impl TargetsBuilder {
    pub fn filter_targets(&mut self, targets: &[String]) {
        self.filter(|x| targets.iter().any(|t| t == x));
    }

    pub fn filter_targets_regex(&mut self, patterns: &[String]) -> Result<()> {
        let regex = RegexSet::new(patterns)?;
        self.filter(|x| regex.is_match(x));
        Ok(())
    }

    pub fn filter_targets_regex_all(&mut self, patterns: &[String]) -> Result<()> {
        let regex = RegexSet::new(patterns)?;
        self.filter(|x| regex.matches(x).matched_all());
        Ok(())
    }

    fn filter(&mut self, is_match: impl Fn(&str) -> bool) {
        let mut included_targets = vec![false; self.targets.len()];
        let mut included_files = vec![false; self.files.len()];
        let mut to_include = vec![];
        let mut matching_len: usize = 0;
        for target in &self.targets {
            if is_match(&target.name)
                || target.outputs.iter().any(|x| {
                    let path = &self.files[*x].path;
                    let path_wo_out_dir = path.strip_prefix(OUT_DIR).unwrap();
                    is_match(path.to_str().unwrap()) || is_match(path_wo_out_dir.to_str().unwrap())
                })
            {
                matching_len += 1;
                to_include.push(target.id);
            }
        }
        let mut included: usize = 0;
        while let Some(id) = to_include.pop() {
            if included_targets[id] {
                continue;
            }
            included_targets[id] = true;
            included += 1;
            let target = &self.targets[id];
            for x in chain!(&target.executables, &target.inputs, &target.outputs) {
                included_files[*x] = true;
            }
            for input_id in chain!(&target.executables, &target.inputs) {
                if let Some(dep) = self.creator_for_file.get(input_id) {
                    to_include.push(*dep);
                }
            }
            for dep in &target.deps {
                to_include.push(*dep);
            }
        }
        let deps_len = included - matching_len;
        tracing::info!("included {matching_len} targets with {deps_len} dependencies");
        self.compact_and_remap(&included_targets, &included_files);
    }

    fn compact_and_remap(&mut self, included_targets: &[bool], included_files: &[bool]) {
        let target_remap = build_remap(included_targets);
        let file_remap = build_remap(included_files);
        self.targets.retain_mut(|target| {
            let Some(new_id) = target_remap[target.id] else {
                return false;
            };
            target.id = new_id;
            for x in &mut target.executables {
                *x = file_remap[*x].unwrap();
            }
            for x in &mut target.inputs {
                *x = file_remap[*x].unwrap();
            }
            for x in &mut target.outputs {
                *x = file_remap[*x].unwrap();
            }
            for x in &mut target.deps {
                *x = target_remap[*x].unwrap();
            }
            true
        });
        self.files.retain_mut(|file| {
            let Some(new_id) = file_remap[file.id] else {
                return false;
            };
            file.id = new_id;
            true
        });
        self.file_by_path
            .retain(|_, id| remap_in_place(id, &file_remap));
        self.creator_for_file = std::mem::take(&mut self.creator_for_file)
            .into_iter()
            .filter_map(|(file, target)| Some((file_remap[file]?, target_remap[target]?)))
            .collect();
        self.target_by_name
            .retain(|_, id| remap_in_place(id, &target_remap));
        self.system_executable_by_name
            .retain(|_, id| remap_in_place(id, &file_remap));
    }
}

fn build_remap(included: &[bool]) -> Vec<Option<usize>> {
    let mut remap = vec![None; included.len()];
    for (new_id, old_id) in included
        .iter()
        .enumerate()
        .filter_map(|(old_id, &is_included)| is_included.then_some(old_id))
        .enumerate()
    {
        remap[old_id] = Some(new_id);
    }
    remap
}

fn remap_in_place(id: &mut usize, remap: &[Option<usize>]) -> bool {
    match remap[*id] {
        Some(new_id) => {
            *id = new_id;
            true
        }
        None => false,
    }
}
