use crate::targets_builder::TargetsBuilder;
use crate::types::*;
use anyhow::{Result, bail};
use itertools::{Itertools, chain};
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub struct DependencyGraph {
    pub targets: Vec<Target>,
    pub files: Vec<File>,
    /// a file can be job input or is created by exactly one target
    pub creator_for_file: HashMap<FileId, TargetId>,
    pub target_by_name: HashMap<String, TargetId>,
    pub deps: Vec<Vec<TargetId>>,
    pub reverse_deps: Vec<Vec<TargetId>>,
    pub ready: Vec<TargetId>,
    pub waiting: HashSet<TargetId>,
    pub skipped: HashSet<TargetId>,
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
            skipped: Default::default(),
        };
        instance.create();
        instance
    }

    pub fn push_targets(&mut self, targets: Vec<Target>, files: Vec<File>) {
        assert!(self.targets.is_empty());
        for target in &targets {
            for output in target.outputs.iter().cloned() {
                let old = self.creator_for_file.insert(output, target.id);
                assert!(old.is_none());
            }
        }
        self.targets = targets;
        self.files = files;
        self.create();
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

    /// Returns the list of newly ready targets
    pub fn set_succeeded(&mut self, id: TargetId) -> Vec<TargetId> {
        self.ready
            .remove(self.ready.iter().find_position(|x| **x == id).unwrap().0);
        let mut ready = vec![];
        for rdep_id in self.reverse_deps[id].clone() {
            let deps = self.deps.get_mut(rdep_id).unwrap();
            assert!(!deps.is_empty());
            deps.swap_remove(deps.iter().position(|x| *x == id).unwrap());
            if deps.is_empty() {
                self.waiting.remove(&rdep_id);
                ready.push(rdep_id);
            }
        }
        self.ready.extend_from_slice(&ready);
        ready
    }

    /// Returns the list of newly skipped targets
    pub fn set_failed(&mut self, id: TargetId) -> Vec<TargetId> {
        self.ready
            .remove(self.ready.iter().find_position(|x| **x == id).unwrap().0);
        let mut skipped = vec![];
        let mut to_skip = self.reverse_deps[id].clone();
        while let Some(id) = to_skip.pop() {
            if self.skipped.contains(&id) {
                continue;
            }
            skipped.push(id);
            assert!(!self.deps[id].is_empty());
            self.waiting.remove(&id);
            self.skipped.insert(id);
            to_skip.extend(self.reverse_deps[id].iter());
        }
        skipped
    }

    pub fn is_finished(&self) -> bool {
        self.waiting.is_empty() && self.ready.is_empty()
    }

    fn create(&mut self) {
        assert!(self.deps.is_empty());
        self.deps.resize(self.targets.len(), Vec::new());
        self.reverse_deps.resize(self.targets.len(), Vec::new());
        self.waiting.reserve(self.targets.len());
        for target in &self.targets {
            if target.is_excluded {
                continue;
            }
            let target_deps = &mut self.deps[target.id];
            for input_id in chain!(&target.executables, &target.inputs) {
                if let Some(dep) = self.creator_for_file.get(input_id).cloned() {
                    target_deps.push(dep);
                    self.reverse_deps[dep].push(target.id);
                }
            }
            for dep in target.deps.iter().cloned() {
                target_deps.push(dep);
                self.reverse_deps[dep].push(target.id);
            }
            if target_deps.is_empty() {
                self.ready.push(target.id);
            } else {
                self.waiting.insert(target.id);
            }
        }
        self.check_for_circular_dependencies();
    }

    fn check_for_circular_dependencies(&self) {
        // TODO
    }
}
