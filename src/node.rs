use sysinfo::{DiskExt, System, SystemExt};

/// Represents a physical machine used to run tasks.
#[derive(Debug)]
pub struct Node {
    sys: System,
}

impl Node {
    pub fn new() -> Self {
        Self {
            sys: System::new_all(),
        }
    }

    /// The name of the node.
    pub fn hostname(&self) -> Option<String> {
        self.sys.host_name()
    }

    /// The amount of memory available.
    pub fn max_memory(&mut self) -> u64 {
        self.sys.refresh_memory();
        self.sys.total_memory()
    }

    /// The amount of memory allocated.
    pub fn memory_allocated(&mut self) -> u64 {
        self.sys.refresh_memory();
        self.sys.used_memory()
    }

    /// The disk size available.
    pub fn max_disk_size(&mut self) -> u64 {
        self.sys.refresh_disks();
        self.sys.disks().iter().map(|disk| disk.total_space()).sum()
    }

    /// The disk size allocated.
    pub fn disk_allocated(&mut self) -> u64 {
        // Disks are refreshed by max_disk_size.
        let max_disk_size = self.max_disk_size();
        let total_available: u64 = self
            .sys
            .disks()
            .iter()
            .map(|disk| disk.available_space())
            .sum();
        max_disk_size - total_available
    }
}
