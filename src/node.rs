use anyhow::Result;

use sysinfo::{System, SystemExt};

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
    pub fn hostname() -> Result<String> {
        todo!()
    }

    /// The ip address of the node.
    pub fn ip() -> Result<String> {
        todo!()
    }

    /// The amount of memory available.
    pub fn max_memory() -> Result<String> {
        todo!()
    }

    /// The amount of memory allocated.
    pub fn memory_allocated() -> Result<String> {
        todo!()
    }

    /// The disk size available.
    pub fn max_disk_size() -> Result<String> {
        todo!()
    }

    /// The disk size allocated.
    pub fn disk_allocated() -> Result<String> {
        todo!()
    }
}
