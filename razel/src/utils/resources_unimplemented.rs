pub fn create_cgroup() -> Result<Option<CGroup>, anyhow::Error> {
    // no error, just not supported
    Ok(None)
}

#[derive(Clone)]
pub struct CGroup {}

impl CGroup {
    pub fn add_task(&self, _controller: &str, _pid: u32) -> Result<(), anyhow::Error> {
        unreachable!()
    }
}
