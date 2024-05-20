use crate::executors::{Executor, HttpRemoteExecConfig};
use crate::{Command, CommandId};
use itertools::Itertools;
use std::collections::HashMap;

type Group = String;

struct ReadyItem {
    id: CommandId,
    group: Group,
    slots: usize,
}

struct HttpDomainData {
    domain: String,
    ready_items: Vec<ReadyItem>,
    hosts: Vec<HttpHostData>,
}

struct HttpHostData {
    host: String,
    client: reqwest::Client,
    available_slots: usize,
    used_slots: usize,
}

/// Keeps track of ready/running commands and selects next to run depending on resources
pub struct Scheduler {
    available_slots: usize,
    used_slots: usize,
    // TODO sort by weight, e.g. recursive number of rdeps
    ready_items: Vec<ReadyItem>,
    running_items: HashMap<CommandId, Group>,
    /// groups commands by estimated resource requirement
    group_to_slots: HashMap<String, usize>,
    http_domains: Vec<HttpDomainData>,
}

impl Scheduler {
    pub fn new(available_slots: usize) -> Self {
        Self {
            available_slots,
            used_slots: 0,
            ready_items: Default::default(),
            running_items: Default::default(),
            group_to_slots: Default::default(),
            http_domains: vec![],
        }
    }

    pub fn set_http_remote_exec_config(&mut self, http_remote_exec: HttpRemoteExecConfig) {
        for (domain, host_and_slot) in http_remote_exec.0 {
            if host_and_slot.is_empty() {
                continue;
            }
            let domain_data = HttpDomainData {
                domain,
                ready_items: vec![],
                hosts: host_and_slot
                    .into_iter()
                    .map(|(host, available_slots)| HttpHostData {
                        host,
                        client: Default::default(),
                        available_slots,
                        used_slots: 0,
                    })
                    .collect(),
            };
            self.http_domains.push(domain_data);
        }
    }

    pub fn ready(&self) -> usize {
        self.ready_items.len()
    }

    pub fn ready_ids(&self) -> Vec<CommandId> {
        self.ready_items.iter().map(|x| x.id).collect()
    }

    pub fn running(&self) -> usize {
        self.running_items.len()
    }

    pub fn len(&self) -> usize {
        self.ready_items.len() + self.running_items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ready_items.is_empty() && self.running_items.is_empty()
    }

    pub fn push_ready(&mut self, command: &Command) {
        if let Executor::HttpRemote(_) = &command.executor {
            todo!();
        }
        let group = Self::group_for_command(command);
        let slots = self.slots_for_group(&group);
        self.ready_items.push(ReadyItem {
            id: command.id,
            group,
            slots,
        });
    }

    pub fn pop_ready_and_run(&mut self) -> Option<(CommandId, Option<Executor>)> {
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
            Some((item.id, None))
        } else {
            None
        }
    }

    pub fn set_finished_and_get_retry_flag(&mut self, id: CommandId, killed: bool) -> bool {
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

#[cfg(test)]
#[allow(clippy::bool_assert_comparison)]
mod tests {
    use super::*;
    use crate::executors::CustomCommandExecutor;
    use crate::{Arena, ScheduleState};

    fn create(available_slots: usize, executables: Vec<&str>) -> Scheduler {
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
        scheduler
    }

    #[test]
    fn simple() {
        let mut ror = create(3, vec!["exec_0", "exec_0", "exec_1", "exec_1"]);
        let c0 = ror.pop_ready_and_run().unwrap().0;
        let c1 = ror.pop_ready_and_run().unwrap().0;
        let c2 = ror.pop_ready_and_run().unwrap().0;
        assert_eq!(ror.pop_ready_and_run().map(|x| x.0), None);
        assert_eq!(ror.used_slots, 3);
        assert_eq!(ror.set_finished_and_get_retry_flag(c1, false), false);
        let c3 = ror.pop_ready_and_run().unwrap().0;
        assert_eq!(ror.set_finished_and_get_retry_flag(c0, false), false);
        assert_eq!(ror.set_finished_and_get_retry_flag(c2, false), false);
        assert_eq!(ror.set_finished_and_get_retry_flag(c3, false), false);
        assert_eq!(ror.len(), 0);
        assert_eq!(ror.used_slots, 0);
    }

    #[test]
    fn killed() {
        let mut ror = create(3, vec!["exec_0", "exec_0", "exec_1", "exec_1"]);
        let c0 = ror.pop_ready_and_run().unwrap().0;
        let c1 = ror.pop_ready_and_run().unwrap().0;
        let c2 = ror.pop_ready_and_run().unwrap().0;
        assert_eq!(ror.pop_ready_and_run().map(|x| x.0), None);
        assert_eq!(ror.used_slots, 3);
        assert_eq!(ror.set_finished_and_get_retry_flag(c1, true), true); // -> exec_0: 2 slots
        assert_eq!(ror.used_slots, 3); // c0 (2), c2 (1)
        assert_eq!(ror.pop_ready_and_run().map(|x| x.0), None);
        assert_eq!(ror.set_finished_and_get_retry_flag(c0, true), true); // -> exec_0: 3 slots
        assert_eq!(ror.used_slots, 1); // c2 (1)
        assert_eq!(ror.set_finished_and_get_retry_flag(c2, false), false);
        assert_eq!(ror.used_slots, 0);
        let c3 = ror.pop_ready_and_run().unwrap().0;
        assert_eq!(ror.used_slots, 1); // c4 (1)
        assert_eq!(ror.pop_ready_and_run().map(|x| x.0), None);
        assert_eq!(ror.set_finished_and_get_retry_flag(c3, false), false);
        assert_eq!(ror.used_slots, 0);
        let c0_or_c1 = ror.pop_ready_and_run().unwrap().0;
        assert_eq!(ror.used_slots, 3);
        assert_eq!(ror.pop_ready_and_run().map(|x| x.0), None);
        assert_eq!(ror.set_finished_and_get_retry_flag(c0_or_c1, false), false);
        let c0_or_c1 = ror.pop_ready_and_run().unwrap().0;
        assert_eq!(ror.set_finished_and_get_retry_flag(c0_or_c1, true), false);
        assert_eq!(ror.len(), 0);
        assert_eq!(ror.used_slots, 0);
    }
}
