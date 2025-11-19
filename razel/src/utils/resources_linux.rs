use crate::config;
use anyhow::{bail, Context, Result};
use log::debug;
use procfs::{Current, Meminfo};
use std::fs;
use std::fs::{read_to_string, File};
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::str::FromStr;

pub fn create_cgroup() -> Result<Option<CGroup>> {
    let available = get_available_memory()?;
    let mut limit = available;
    let existing_limit = CGroup::new("".into()).read::<u64>("memory", "memory.limit_in_bytes");
    if let Ok(x) = existing_limit {
        limit = limit.min(x); // memory.limit_in_bytes will be infinite if not set
    }
    limit = (limit as f64 * 0.95) as u64;
    let cgroup = CGroup::new(config::EXECUTABLE.into());
    cgroup.create("memory")?;
    cgroup.write("memory", "memory.limit_in_bytes", limit)?;
    cgroup.write("memory", "memory.swappiness", 0)?;
    debug!(
        "create_cgroup(): available: {}MiB, limit: {:?}MiB -> set limit {}MiB",
        available / 1024 / 1024,
        existing_limit.ok().map(|x| x / 1024 / 1024),
        limit / 1024 / 1024
    );
    Ok(Some(cgroup))
}

/// Reproduces what the K8s kubelet does to calculate memory.available relative to root cgroup.
///
/// see https://kubernetes.io/docs/concepts/scheduling-eviction/node-pressure-eviction/
fn get_available_memory() -> Result<u64> {
    let memory_capacity = Meminfo::current()?.mem_total;
    let cgroup = CGroup::new("".into());
    let memory_usage = cgroup.read::<u64>("memory", "memory.usage_in_bytes")?;
    let memory_total_inactive_file =
        cgroup.read_field::<u64>("memory", "memory.stat", "total_inactive_file")?;
    let memory_working_set = memory_usage.saturating_sub(memory_total_inactive_file);
    let memory_available = memory_capacity - memory_working_set;
    Ok(memory_available)
}

#[derive(Clone)]
pub struct CGroup {
    group: String,
}

impl CGroup {
    pub fn new(group: String) -> Self {
        Self { group }
    }

    pub fn create(&self, controller: &str) -> Result<()> {
        let path = self.path(controller, "x");
        let dir = path.parent().unwrap();
        fs::create_dir_all(dir).with_context(|| format!("Failed to create dir {dir:?}"))?;
        Ok(())
    }

    pub fn add_task(&self, controller: &str, pid: u32) -> Result<()> {
        self.write(controller, "tasks", pid)
    }

    pub fn read<T>(&self, controller: &str, file: &str) -> Result<T>
    where
        T: FromStr,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    {
        let path = self.path(controller, file);
        let string = read_to_string(&path).with_context(|| format!("Failed to read {path:?}"))?;
        let value = string
            .trim()
            .parse::<T>()
            .with_context(|| format!("Failed to parse {path:?}"))?;
        Ok(value)
    }

    pub fn read_field<T>(&self, controller: &str, file: &str, field: &str) -> Result<T>
    where
        T: FromStr,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    {
        let path = self.path(controller, file);
        let file = File::open(&path).with_context(|| format!("Failed to read {path:?}"))?;
        let lines = BufReader::new(file).lines();
        for line in lines {
            if let Some(string) = line?.strip_prefix(field) {
                let value = string.trim().parse::<T>().with_context(|| {
                    format!("Failed to parse field {field} from line: {string}")
                })?;
                return Ok(value);
            }
        }
        bail!("Failed to parse field {} from {:?}", field, path);
    }

    pub fn write<T>(&self, controller: &str, file: &str, value: T) -> Result<()>
    where
        T: std::fmt::Display,
    {
        let path = self.path(controller, file);
        fs::write(&path, value.to_string()).with_context(|| format!("Failed to write {path:?}"))?;
        Ok(())
    }

    fn path(&self, controller: &str, file: &str) -> PathBuf {
        PathBuf::from("/sys/fs/cgroup")
            .join(controller)
            .join(&self.group)
            .join(file)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[ignore]
    fn available_memory() {
        println!("available_memory: {}", get_available_memory().unwrap());
    }

    #[test]
    #[serial]
    #[ignore]
    fn cgroup_razel() {
        let cgroup = CGroup::new("razel".into());
        cgroup.create("memory").unwrap();
        cgroup
            .write("memory", "memory.limit_in_bytes", 150 * 1024 * 1024)
            .unwrap();
        cgroup.write("memory", "memory.swappiness", 0).unwrap();
        println!(
            "memory.limit_in_bytes: {:?}",
            cgroup.read::<u64>("memory", "memory.limit_in_bytes")
        );
        println!(
            "memory.swappiness: {:?}",
            cgroup.read::<i32>("memory", "memory.swappiness")
        );

        println!(
            "tasks before: {:?}",
            cgroup.read::<String>("memory", "tasks")
        );
        cgroup.add_task("memory", std::process::id()).unwrap();
        println!(
            "tasks after: {:?}",
            cgroup.read::<String>("memory", "tasks")
        );
    }
}
