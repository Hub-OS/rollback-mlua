use std::collections::VecDeque;
use std::os::raw::c_void;

use super::memory::Memory;

pub struct PhotographicMemory {
    active_memory: Memory,
    max_snapshots: usize,
    snapshots: VecDeque<Memory>,
}

impl PhotographicMemory {
    pub fn new(space: usize, max_snapshots: usize) -> Self {
        Self {
            active_memory: Memory::new(space),
            max_snapshots,
            snapshots: VecDeque::with_capacity(max_snapshots),
        }
    }

    pub fn max_snapshots(&self) -> usize {
        self.max_snapshots
    }

    pub fn len(&self) -> usize {
        self.active_memory.len()
    }

    pub fn unused_memory(&self) -> usize {
        self.active_memory.unused_memory()
    }

    pub fn used_memory(&self) -> usize {
        self.active_memory.used_memory()
    }

    pub fn realloc(&mut self, ptr: *mut c_void, osize: usize, nsize: usize) -> *mut c_void {
        self.active_memory.realloc(ptr, osize, nsize)
    }

    pub fn snap(&mut self) {
        if self.max_snapshots == 0 {
            return;
        } else if self.snapshots.len() >= self.max_snapshots {
            self.snapshots.pop_front();
        }

        self.snapshots.push_back(self.active_memory.clone());
    }

    pub fn rollback(&mut self, n: usize) {
        if self.snapshots.len() < n || n == 0 {
            log::error!(
                "Not enough snapshots to roll back: existing: {}, required: {}",
                self.snapshots.len(),
                n
            );
            return;
        }

        let index = self.snapshots.len() - n;
        let snapshot = &self.snapshots[index];

        self.active_memory.copy_from(snapshot);

        self.snapshots.resize_with(index + 1, || unreachable!());
    }
}
