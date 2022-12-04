use std::net::Ipv6Addr;

/// Represents a physical machine used to run tasks.
#[derive(Debug)]
pub struct Node {
    /// The name of the node.
    name: String,
    /// The ip address of the node.
    ip: Ipv6Addr,
    /// The amount of memory available.
    max_memory: usize,
    /// The amount of memory allocated.
    memory_allocated: usize,
    /// The disk size available.
    max_disk_size: usize,
    /// The disk size allocated.
    disk_allocated: usize,
}
