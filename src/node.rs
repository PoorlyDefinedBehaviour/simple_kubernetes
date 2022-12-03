//! Represents a physical machine used to run tasks.

use std::net::Ipv6Addr;

pub struct Node {
    name: String,
    ip: Ipv6Addr,
    max_memory: usize,
    memory_allocated: usize,
    max_disk_size: usize,
    disk_allocated: usize,
    task_count: usize,
}
