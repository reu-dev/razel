use crate::targets_builder::TargetsBuilder;
use crate::types::*;
use anyhow::{bail, Result};
use itertools::{chain, Itertools};
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub struct DependencyGraph {
    pub targets: Vec<Target>,
    pub files: Vec<File>,
    pub creator_for_file: HashMap<FileId, TargetId>,
    pub target_by_name: HashMap<String, TargetId>,
    pub deps: Vec<Vec<TargetId>>,
    pub reverse_deps: HashMap<TargetId, Vec<TargetId>>,
    pub ready: Vec<TargetId>,
    pub waiting: HashSet<TargetId>,
}

impl DependencyGraph {
    pub fn from_builder(builder: TargetsBuilder) -> Self {
        let mut instance = Self {
            targets: builder.targets,
            files: builder.files,
            creator_for_file: builder.creator_for_file,
            target_by_name: builder.target_by_name,
            deps: vec![],
            reverse_deps: Default::default(),
            ready: vec![],
            waiting: Default::default(),
        };
        instance.create();
        instance
    }

    pub fn get_target_with_deps(&self, name: &str) -> Result<Vec<TargetId>> {
        let Some(target_id) = self.target_by_name.get(name).cloned() else {
            bail!("no such target: {name:?}")
        };
        let mut targets = vec![];
        let mut included = HashSet::new();
        self.get_target_with_deps_impl(target_id, &mut targets, &mut included);
        Ok(targets)
    }

    fn get_target_with_deps_impl(
        &self,
        target_id: TargetId,
        cmds: &mut Vec<TargetId>,
        included: &mut HashSet<TargetId>,
    ) {
        included.insert(target_id);
        for dep in self.deps[target_id].iter().cloned() {
            if !included.contains(&dep) {
                self.get_target_with_deps_impl(dep, cmds, included);
            }
        }
        cmds.push(target_id);
    }

    pub fn get_command_line_for_target(&self, target_id: TargetId) -> Vec<String> {
        let target = &self.targets[target_id];
        target.kind.command_line_with_redirects()
    }

    pub fn get_command_lines_for_target_with_deps(&self, name: &str) -> Result<Vec<String>> {
        self.get_target_with_deps(name).map(|t| {
            t.into_iter()
                .map(|id| {
                    self.get_command_line_for_target(id)
                        .iter()
                        .map(|x| {
                            if x.contains(" ") {
                                format!("{x:?}")
                            } else {
                                x.clone()
                            }
                        })
                        .join(" ")
                })
                .collect()
        })
    }

    pub fn create(&mut self) {
        self.waiting.reserve(self.targets.len());
        for target in &self.targets {
            assert_eq!(target.id, self.deps.len());
            if target.is_excluded {
                continue;
            }
            let mut target_deps = vec![];
            for input_id in chain!(&target.executables, &target.inputs) {
                if let Some(dep) = self.creator_for_file.get(input_id).cloned() {
                    target_deps.push(dep);
                    self.reverse_deps.entry(dep).or_default().push(target.id);
                }
            }
            for dep in target.deps.iter().cloned() {
                target_deps.push(dep);
                self.reverse_deps.entry(dep).or_default().push(target.id);
            }
            if target_deps.is_empty() {
                self.ready.push(target.id);
            } else {
                self.waiting.insert(target.id);
            }
            self.deps.push(target_deps);
        }
        self.check_for_circular_dependencies();
    }

    fn check_for_circular_dependencies(&self) {
        // TODO
    }
}
