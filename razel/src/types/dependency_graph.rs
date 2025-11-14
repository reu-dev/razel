use crate::targets_builder::TargetsBuilder;
use crate::types::{FileId, Target, TargetId};
use itertools::chain;
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub struct DependencyGraph {
    reverse_deps: HashMap<TargetId, Vec<TargetId>>,
    ready: Vec<TargetId>,
    waiting: HashSet<TargetId>,
    unfinished_deps: Vec<Vec<TargetId>>,
}

impl DependencyGraph {
    pub fn from_builder(builder: &TargetsBuilder) -> Self {
        let mut instance = Self::default();
        instance.create(&builder.targets, &builder.creator_for_file);
        instance
    }

    fn create(&mut self, targets: &Vec<Target>, creator_for_file: &HashMap<FileId, TargetId>) {
        self.waiting.reserve(targets.len());
        for target in targets {
            assert_eq!(target.id, self.unfinished_deps.len());
            let mut unfinished_deps = vec![];
            for input_id in chain!(&target.executables, &target.inputs) {
                if let Some(dep) = creator_for_file.get(input_id).cloned() {
                    unfinished_deps.push(dep);
                    self.reverse_deps.entry(dep).or_default().push(target.id);
                }
            }
            for dep in target.deps.iter().cloned() {
                unfinished_deps.push(dep);
                self.reverse_deps.entry(dep).or_default().push(target.id);
            }
            if unfinished_deps.is_empty() {
                self.ready.push(target.id);
            } else {
                self.waiting.insert(target.id);
            }
            self.unfinished_deps.push(unfinished_deps);
        }
        self.check_for_circular_dependencies();
    }

    fn check_for_circular_dependencies(&self) {
        // TODO
    }
}
