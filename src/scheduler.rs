use crate::executors::{Executor, HttpRemoteExecDomain};
use crate::{Command, CommandId};
use itertools::Itertools;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

type Group = String;

struct ReadyItem {
    id: CommandId,
    group: Group,
    slots: usize,
}

/// Keeps track of ready/running commands and selects next to run depending on resources
pub struct Scheduler {
    available_slots: usize,
    used_slots: usize,
    // TODO sort by weight, e.g. recursive number of rdeps
    ready_items: Vec<ReadyItem>,
    ready_for_remote_exec: Vec<(Arc<HttpRemoteExecDomain>, VecDeque<CommandId>)>,
    ready_for_remote_exec_len: usize,
    running_items: HashMap<CommandId, Group>,
    running_with_remote_exec: usize,
    /// groups commands by estimated resource requirement
    group_to_slots: HashMap<String, usize>,
}

impl Scheduler {
    pub fn new(available_slots: usize) -> Self {
        Self {
            available_slots,
            used_slots: 0,
            ready_items: Default::default(),
            ready_for_remote_exec: Default::default(),
            ready_for_remote_exec_len: 0,
            running_items: Default::default(),
            running_with_remote_exec: 0,
            group_to_slots: Default::default(),
        }
    }

    pub fn ready(&self) -> usize {
        self.ready_items.len() + self.ready_for_remote_exec_len
    }

    pub fn ready_ids(&self) -> Vec<CommandId> {
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

    pub fn push_ready(&mut self, command: &Command) {
        if self.push_ready_for_remote_exec(command) {
            return;
        }
        let group = Self::group_for_command(command);
        let slots = self.slots_for_group(&group);
        self.ready_items.push(ReadyItem {
            id: command.id,
            group,
            slots,
        });
    }

    fn push_ready_for_remote_exec(&mut self, command: &Command) -> bool {
        let Executor::HttpRemote(executor) = &command.executor else {
            return false;
        };
        let Some(domain) = &executor.state else {
            return false;
        };
        let ready = match self
            .ready_for_remote_exec
            .iter_mut()
            .find(|(x, _)| Arc::ptr_eq(x, domain))
        {
            Some(x) => &mut x.1,
            _ => {
                self.ready_for_remote_exec
                    .push((domain.clone(), Default::default()));
                &mut self.ready_for_remote_exec.last_mut().unwrap().1
            }
        };
        ready.push_back(command.id);
        self.ready_for_remote_exec_len += 1;
        true
    }

    pub fn pop_ready_and_run(&mut self) -> Option<CommandId> {
        if let Some(x) = self.pop_ready_and_run_remote_exec() {
            return Some(x);
        }
        if self.used_slots >= self.available_slots || self.ready_items.is_empty() {
            return None;
        }
        let free_slots = self.available_slots - self.used_slots;
        if let Some((index, _)) = self
            .ready_items
            .iter()
            .find_position(|x| x.slots <= free_slots)
        {
            let item = self.ready_items.remove(index);
            self.running_items.insert(item.id, item.group);
            self.used_slots += item.slots;
            Some(item.id)
        } else {
            None
        }
    }

    fn pop_ready_and_run_remote_exec(&mut self) -> Option<CommandId> {
        if self.ready_for_remote_exec_len == 0 {
            return None;
        }
        let id = self
            .ready_for_remote_exec
            .iter_mut()
            .find(|(domain, commands)| !commands.is_empty() && domain.try_schedule())
            .and_then(|(_, commands)| commands.pop_front())?;
        self.ready_for_remote_exec_len -= 1;
        self.running_with_remote_exec += 1;
        Some(id)
    }

    pub fn set_finished_and_get_retry_flag(&mut self, command: &Command, killed: bool) -> bool {
        if self.unschedule_remote_exec(command) {
            return false;
        }
        let id = command.id;
        let group = self.running_items.remove(&id).unwrap();
        self.used_slots -= self.slots_for_group(&group);
        if killed {
            self.scale_up_memory_requirement(&group);
            // stop retry only when command was run exclusively
            if !self.running_items.is_empty() {
                let slots = self.slots_for_group(&group);
                self.ready_items.push(ReadyItem { id, group, slots });
                return true;
            }
        }
        false
    }

    fn unschedule_remote_exec(&mut self, command: &Command) -> bool {
        let Executor::HttpRemote(executor) = &command.executor else {
            return false;
        };
        let Some(domain) = &executor.state else {
            return false;
        };
        assert!(self.running_with_remote_exec > 0);
        domain.unschedule();
        self.running_with_remote_exec -= 1;
        true
    }

    fn scale_up_memory_requirement(&mut self, group: &Group) -> bool {
        let slots_old = self.slots_for_group(group);
        let slots_new = (slots_old * 2).min(self.available_slots);
        if slots_new == slots_old {
            return false;
        }
        self.group_to_slots.insert(group.clone(), slots_new);
        let running_in_group = self
            .running_items
            .iter()
            .filter(|(_, x)| *x == group)
            .count();
        self.used_slots += running_in_group * (slots_new - slots_old);
        self.ready_items
            .iter_mut()
            .filter(|x| x.group == *group)
            .for_each(|x| x.slots = slots_new);
        true
    }

    fn slots_for_group(&self, group: &Group) -> usize {
        *self.group_to_slots.get(group).unwrap_or(&1)
    }

    fn group_for_command(command: &Command) -> Group {
        // assume resource requirements depends just on executable
        // could also use the command line with file arguments stripped
        match &command.executor {
            Executor::CustomCommand(c) => c.executable.clone(),
            Executor::Wasi(x) => x.executable.clone(),
            Executor::AsyncTask(_) => String::new(),
            Executor::BlockingTask(_) => String::new(),
            Executor::HttpRemote(_) => String::new(),
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
    use crate::executors::CustomCommandExecutor;
    use crate::{Arena, ScheduleState};

    fn create(available_slots: usize, executables: Vec<&str>) -> (Scheduler, Arena<Command>) {
        let mut scheduler = Scheduler::new(available_slots);
        let mut commands: Arena<Command> = Default::default();
        for executable in &executables {
            let id = commands.alloc_with_id(|id| Command {
                id,
                name: format!("cmd_{id}"),
                executables: vec![],
                inputs: vec![],
                outputs: vec![],
                deps: vec![],
                executor: Executor::CustomCommand(CustomCommandExecutor {
                    executable: executable.to_string(),
                    ..Default::default()
                }),
                tags: vec![],
                unfinished_deps: vec![],
                reverse_deps: vec![],
                schedule_state: ScheduleState::New,
            });
            scheduler.push_ready(&commands[id]);
        }
        assert_eq!(scheduler.ready(), executables.len());
        (scheduler, commands)
    }

    #[test]
    fn simple() {
        let (mut s, commands) = create(3, vec!["exec_0", "exec_0", "exec_1", "exec_1"]);
        let c0 = s.pop_ready_and_run().unwrap();
        let c1 = s.pop_ready_and_run().unwrap();
        let c2 = s.pop_ready_and_run().unwrap();
        assert_eq!(s.pop_ready_and_run(), None);
        assert_eq!(s.used_slots, 3);
        assert_eq!(
            s.set_finished_and_get_retry_flag(&commands[c1], false),
            false
        );
        let c3 = s.pop_ready_and_run().unwrap();
        assert_eq!(
            s.set_finished_and_get_retry_flag(&commands[c0], false),
            false
        );
        assert_eq!(
            s.set_finished_and_get_retry_flag(&commands[c2], false),
            false
        );
        assert_eq!(
            s.set_finished_and_get_retry_flag(&commands[c3], false),
            false
        );
        assert_eq!(s.len(), 0);
        assert_eq!(s.used_slots, 0);
    }

    #[test]
    fn killed() {
        let (mut s, commands) = create(3, vec!["exec_0", "exec_0", "exec_1", "exec_1"]);
        let c0 = s.pop_ready_and_run().unwrap();
        let c1 = s.pop_ready_and_run().unwrap();
        let c2 = s.pop_ready_and_run().unwrap();
        assert_eq!(s.pop_ready_and_run(), None);
        assert_eq!(s.used_slots, 3);
        assert_eq!(s.set_finished_and_get_retry_flag(&commands[c1], true), true); // -> exec_0: 2 slots
        assert_eq!(s.used_slots, 3); // c0 (2), c2 (1)
        assert_eq!(s.pop_ready_and_run(), None);
        assert_eq!(s.set_finished_and_get_retry_flag(&commands[c0], true), true); // -> exec_0: 3 slots
        assert_eq!(s.used_slots, 1); // c2 (1)
        assert_eq!(
            s.set_finished_and_get_retry_flag(&commands[c2], false),
            false
        );
        assert_eq!(s.used_slots, 0);
        let c3 = s.pop_ready_and_run().unwrap();
        assert_eq!(s.used_slots, 1); // c4 (1)
        assert_eq!(s.pop_ready_and_run(), None);
        assert_eq!(
            s.set_finished_and_get_retry_flag(&commands[c3], false),
            false
        );
        assert_eq!(s.used_slots, 0);
        let c0_or_c1 = s.pop_ready_and_run().unwrap();
        assert_eq!(s.used_slots, 3);
        assert_eq!(s.pop_ready_and_run(), None);
        assert_eq!(
            s.set_finished_and_get_retry_flag(&commands[c0_or_c1], false),
            false
        );
        let c0_or_c1 = s.pop_ready_and_run().unwrap();
        assert_eq!(
            s.set_finished_and_get_retry_flag(&commands[c0_or_c1], true),
            false
        );
        assert_eq!(s.len(), 0);
        assert_eq!(s.used_slots, 0);
    }
}
