use crate::executors::{HttpRemoteExecDomain, HttpRemoteExecState};
use crate::types::{Target, TargetId, TargetKind, Task};
use itertools::Itertools;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

type Cpus = f32;
type Group = String;

struct ReadyItem {
    id: TargetId,
    group: Group,
    slots: Cpus,
    locks: Vec<String>,
}

/// Keeps track of ready/running targets and selects next to run depending on resources
pub struct Scheduler {
    max_cpus: Cpus,
    cpus: Cpus,
    http_remote_exec_state: HttpRemoteExecState,
    // TODO sort by weight, e.g. recursive number of rdeps
    ready_items: Vec<ReadyItem>,
    ready_for_remote_exec: Vec<(Arc<HttpRemoteExecDomain>, VecDeque<TargetId>)>,
    ready_for_remote_exec_len: usize,
    running_items: HashMap<TargetId, (Group, Cpus)>,
    running_with_remote_exec: usize,
    /// groups targets by estimated resource requirement
    group_to_slots: HashMap<String, Cpus>,
    locks: HashSet<String>,
}

impl Scheduler {
    pub fn new(available_slots: usize) -> Self {
        Self {
            max_cpus: available_slots as Cpus,
            cpus: 0.0,
            http_remote_exec_state: Default::default(),
            ready_items: Default::default(),
            ready_for_remote_exec: Default::default(),
            ready_for_remote_exec_len: 0,
            running_items: Default::default(),
            running_with_remote_exec: 0,
            group_to_slots: Default::default(),
            locks: Default::default(),
        }
    }

    pub fn set_http_remote_exec_config(&mut self, http_remote_exec_state: HttpRemoteExecState) {
        self.http_remote_exec_state = http_remote_exec_state;
        self.ready_for_remote_exec = self
            .http_remote_exec_state
            .domains
            .iter()
            .map(|x| (x.clone(), Default::default()))
            .collect();
    }

    pub fn ready(&self) -> usize {
        self.ready_items.len() + self.ready_for_remote_exec_len
    }

    pub fn ready_ids(&self) -> Vec<TargetId> {
        self.ready_items
            .iter()
            .map(|x| x.id)
            .chain(
                self.ready_for_remote_exec
                    .iter()
                    .flat_map(|(_, x)| x.iter().cloned()),
            )
            .collect()
    }

    pub fn running(&self) -> usize {
        self.running_items.len() + self.running_with_remote_exec
    }

    pub fn len(&self) -> usize {
        self.ready() + self.running()
    }

    pub fn is_empty(&self) -> bool {
        self.ready_items.is_empty()
            && self.ready_for_remote_exec_len == 0
            && self.running_items.is_empty()
            && self.running_with_remote_exec == 0
    }

    pub fn push_ready(&mut self, target: &Target) {
        if self.push_ready_for_remote_exec(target) {
            return;
        }
        let group = Self::group_for_command(target);
        let slots = target.cpus().max(self.slots_for_group(&group));
        let locks = target.locks().map(String::from).collect();
        self.ready_items.push(ReadyItem {
            id: target.id,
            group,
            slots,
            locks,
        });
    }

    fn push_ready_for_remote_exec(&mut self, target: &Target) -> bool {
        let TargetKind::HttpRemoteExecTask(task_target) = &target.kind else {
            return false;
        };
        let Task::HttpRemoteExec(task) = &task_target.task else {
            unreachable!()
        };
        let Some(domain) = self.http_remote_exec_state.for_url(&task.url) else {
            return false;
        };
        let Some(ready) = self
            .ready_for_remote_exec
            .iter_mut()
            .find(|(x, _)| Arc::ptr_eq(x, &domain))
        else {
            unreachable!()
        };
        ready.1.push_back(target.id);
        self.ready_for_remote_exec_len += 1;
        true
    }

    pub fn pop_ready_and_run(&mut self) -> Option<TargetId> {
        if let Some(x) = self.pop_ready_and_run_remote_exec() {
            return Some(x);
        }
        if self.cpus + 1.0 > self.max_cpus || self.ready_items.is_empty() {
            return None;
        }
        let free_slots = self.max_cpus - self.cpus;
        if let Some((index, _)) = self.ready_items.iter().find_position(|x| {
            x.slots <= free_slots && !x.locks.iter().any(|l| self.locks.contains(l))
        }) {
            let item = self.ready_items.remove(index);
            self.locks.extend(item.locks);
            self.running_items.insert(item.id, (item.group, item.slots));
            self.cpus += item.slots;
            Some(item.id)
        } else {
            None
        }
    }

    fn pop_ready_and_run_remote_exec(&mut self) -> Option<TargetId> {
        if self.ready_for_remote_exec_len == 0 {
            return None;
        }
        let id = self
            .ready_for_remote_exec
            .iter_mut()
            .find(|(domain, targets)| !targets.is_empty() && domain.try_schedule())
            .and_then(|(_, targets)| targets.pop_front())?;
        self.ready_for_remote_exec_len -= 1;
        self.running_with_remote_exec += 1;
        Some(id)
    }

    pub fn set_finished_and_get_retry_flag(&mut self, target: &Target, oom_killed: bool) -> bool {
        if self.unschedule_remote_exec(target) {
            return false;
        }
        let id = target.id;
        let (group, cpus) = self.running_items.remove(&id).unwrap();
        self.cpus -= cpus;
        assert!(self.cpus > -0.01);
        for lock in target.locks() {
            self.locks.remove(lock);
        }
        if oom_killed && self.scale_up_memory_requirement(&group, cpus) {
            let slots = self.slots_for_group(&group);
            let locks = target.locks().map(String::from).collect();
            self.ready_items.push(ReadyItem {
                id,
                group,
                slots,
                locks,
            });
            true
        } else {
            false
        }
    }

    fn unschedule_remote_exec(&mut self, target: &Target) -> bool {
        let TargetKind::HttpRemoteExecTask(task_target) = &target.kind else {
            return false;
        };
        let Task::HttpRemoteExec(task) = &task_target.task else {
            unreachable!()
        };
        let Some(domain) = self.http_remote_exec_state.for_url(&task.url) else {
            return false;
        };
        assert!(self.running_with_remote_exec > 0);
        domain.unschedule();
        self.running_with_remote_exec -= 1;
        true
    }

    fn scale_up_memory_requirement(&mut self, group: &Group, target_cpus: Cpus) -> bool {
        let group_cpus = self.slots_for_group(group);
        let new = (target_cpus * 2.0).max(group_cpus).min(self.max_cpus);
        if new > group_cpus {
            self.group_to_slots.insert(group.clone(), new);
            self.ready_items
                .iter_mut()
                .filter(|x| x.group == *group)
                .for_each(|x| x.slots = new);
        }
        new > target_cpus
    }

    fn slots_for_group(&self, group: &Group) -> Cpus {
        *self.group_to_slots.get(group).unwrap_or(&0.0)
    }

    fn group_for_command(target: &Target) -> Group {
        // assume resource requirements depends just on executable
        // could also use the command line with file arguments stripped
        match &target.kind {
            TargetKind::Command(c) => c.executable.clone(),
            TargetKind::Wasi(c) => c.executable.clone(),
            TargetKind::Task(_) => String::new(),
            TargetKind::HttpRemoteExecTask(_) => String::new(),
        }
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        assert_eq!(
            self.ready_for_remote_exec
                .iter()
                .map(|(_, x)| x.len())
                .sum::<usize>(),
            self.ready_for_remote_exec_len
        );
    }
}

#[cfg(test)]
#[allow(clippy::bool_assert_comparison)]
mod tests {
    use super::*;
    use crate::types::CommandTarget;

    fn create(available_slots: usize, executables: Vec<&str>) -> (Scheduler, Vec<Target>) {
        let mut scheduler = Scheduler::new(available_slots);
        let mut targets = vec![];
        for (id, executable) in executables.iter().enumerate() {
            targets.push(Target {
                id,
                name: format!("cmd_{id}"),
                kind: TargetKind::Command(CommandTarget {
                    executable: executable.to_string(),
                    ..Default::default()
                }),
                executables: vec![],
                inputs: vec![],
                outputs: vec![],
                deps: vec![],
                tags: vec![],
                worker: vec![],
                is_excluded: false,
            });
            scheduler.push_ready(&targets[id]);
        }
        assert_eq!(scheduler.ready(), executables.len());
        (scheduler, targets)
    }

    #[test]
    fn simple() {
        let (mut s, targets) = create(3, vec!["a", "a", "b", "b"]);
        let t0 = s.pop_ready_and_run().unwrap();
        let t1 = s.pop_ready_and_run().unwrap();
        let t2 = s.pop_ready_and_run().unwrap();
        assert_eq!(s.pop_ready_and_run(), None);
        assert_eq!(s.cpus, 3.0);
        assert_eq!(
            s.set_finished_and_get_retry_flag(&targets[t1], false),
            false
        );
        let t3 = s.pop_ready_and_run().unwrap();
        assert_eq!(
            s.set_finished_and_get_retry_flag(&targets[t0], false),
            false
        );
        assert_eq!(
            s.set_finished_and_get_retry_flag(&targets[t2], false),
            false
        );
        assert_eq!(
            s.set_finished_and_get_retry_flag(&targets[t3], false),
            false
        );
        assert_eq!(s.len(), 0);
        assert_eq!(s.cpus, 0.0);
    }

    #[test]
    fn killed() {
        let (mut s, targets) = create(3, vec!["a", "a", "b", "b"]);
        let t0 = s.pop_ready_and_run().unwrap();
        let t1 = s.pop_ready_and_run().unwrap();
        let t2 = s.pop_ready_and_run().unwrap();
        assert_eq!(s.pop_ready_and_run(), None);
        assert_eq!(s.cpus, 3.0); // 0 1 2
        assert_eq!(s.group_to_slots.get("a"), None);
        assert_eq!(s.set_finished_and_get_retry_flag(&targets[t1], true), true);
        assert_eq!(s.cpus, 2.0); // 0 2
        assert_eq!(s.group_to_slots["a"], 2.0);
        let t3 = s.pop_ready_and_run().unwrap();
        assert_eq!(s.pop_ready_and_run(), None);
        assert_eq!(s.cpus, 3.0); // 0 2 3
        assert_eq!(s.set_finished_and_get_retry_flag(&targets[t0], true), true);
        assert_eq!(s.cpus, 2.0); // 2 3
        assert_eq!(s.group_to_slots["a"], 2.0);
        assert_eq!(
            s.set_finished_and_get_retry_flag(&targets[t2], false),
            false
        );
        assert_eq!(s.cpus, 1.0); // 3
        assert_eq!(
            s.set_finished_and_get_retry_flag(&targets[t3], false),
            false
        );
        assert_eq!(s.cpus, 0.0);
        let t0_or_1 = s.pop_ready_and_run().unwrap();
        assert_eq!(s.cpus, 2.0);
        assert_eq!(s.pop_ready_and_run(), None);
        assert_eq!(
            s.set_finished_and_get_retry_flag(&targets[t0_or_1], false),
            false
        );
        let t0_or_1 = s.pop_ready_and_run().unwrap();
        assert_eq!(
            s.set_finished_and_get_retry_flag(&targets[t0_or_1], true),
            true
        );
        assert_eq!(s.group_to_slots["a"], 3.0);
        let x = s.pop_ready_and_run().unwrap();
        assert_eq!(s.cpus, 3.0);
        assert_eq!(s.set_finished_and_get_retry_flag(&targets[x], false), false);
        assert_eq!(s.len(), 0);
        assert_eq!(s.cpus, 0.0);
    }
}
