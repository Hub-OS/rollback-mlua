use std::os::raw::c_void;

#[derive(Debug, PartialEq, Eq, Clone)]
struct MemoryGap {
    offset: usize,
    size: usize,
}

// naive allocator
#[derive(Clone)]
pub struct Memory {
    heap: Vec<u8>,
    gaps: Vec<MemoryGap>,
}

impl Memory {
    pub fn new(initial_size: usize) -> Self {
        Self {
            heap: vec![0; initial_size],
            gaps: vec![MemoryGap {
                offset: 0,
                size: initial_size,
            }],
        }
    }

    pub fn copy_from(&mut self, other: &Self) {
        self.gaps = other.gaps.clone();
        self.heap.copy_from_slice(&other.heap);
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn unused_memory(&self) -> usize {
        let mut remaining_space = 0;

        for gap in &self.gaps {
            remaining_space += gap.size;
        }

        remaining_space
    }

    pub fn used_memory(&self) -> usize {
        self.heap.len() - self.unused_memory()
    }

    pub fn realloc(&mut self, ptr: *mut c_void, osize: usize, nsize: usize) -> *mut c_void {
        // adds clarity to this fn
        #![allow(clippy::needless_return)]

        let osize = align_up(osize);
        let nsize = align_up(nsize);

        if ptr.is_null() {
            // alloc
            return self.alloc(nsize);
        } else if nsize == 0 {
            // free
            self.free(ptr, osize);
            return std::ptr::null_mut();
        } else {
            // realloc

            if nsize <= osize {
                // free space to the right
                // if nsize == osize this does nothing, as it should
                self.free(unsafe { ptr.add(nsize) }, osize - nsize);
                return ptr;
            }

            let new_address = self.alloc(nsize);

            if new_address.is_null() {
                return std::ptr::null_mut();
            }

            self.free(ptr, osize);

            let original_offset = ptr as usize - self.heap.as_ptr() as usize;
            let copy_size = osize.min(nsize);
            let copy_range = original_offset..original_offset + copy_size;

            let new_offset = new_address as usize - self.heap.as_ptr() as usize;

            self.heap.copy_within(copy_range, new_offset);

            new_address
        }
    }

    fn alloc(&mut self, size: usize) -> *mut c_void {
        if size == 0 {
            return std::ptr::null_mut();
        }

        let mut closest_size = usize::MAX;
        let mut best_i: Option<usize> = None;

        for (i, gap) in self.gaps.iter().enumerate() {
            if size <= gap.size && gap.size < closest_size {
                best_i = Some(i);
                closest_size = gap.size;

                if size == gap.size {
                    break;
                }
            }
        }

        let best_i = match best_i {
            Some(i) => i,
            // failed to find space
            None => return std::ptr::null_mut(),
        };

        let gap = &mut self.gaps[best_i];

        let address = unsafe { self.heap.as_ptr().add(gap.offset) };
        gap.offset += size;
        gap.size -= size;

        if gap.size == 0 {
            self.gaps.remove(best_i);
        }

        address as *mut c_void
    }

    fn free(&mut self, ptr: *mut c_void, osize: usize) {
        if osize == 0 {
            return;
        }

        // find a left + right neighbor
        // if there's no neighbors make a new gap
        let mut left_neighbor = false;
        let mut right_neighbor = false;
        let mut insertion_index = 0;

        let offset = ptr as usize - self.heap.as_ptr() as usize;

        for (i, gap) in self.gaps.iter().enumerate() {
            if offset < gap.offset {
                if offset + osize == gap.offset {
                    right_neighbor = true;
                }

                break;
            }

            if offset == gap.offset + gap.size {
                left_neighbor = true;
            }

            insertion_index = i + 1;
        }

        if left_neighbor && right_neighbor {
            // two neighbors, join them
            let right_gap = &self.gaps[insertion_index];
            let increase = osize + right_gap.size;

            let left_gap = &mut self.gaps[insertion_index - 1];
            left_gap.size += increase;

            self.gaps.remove(insertion_index);
        } else if left_neighbor {
            // extend left gap
            let gap = &mut self.gaps[insertion_index - 1];

            gap.size += osize;
        } else if right_neighbor {
            // extend right gap
            let gap = &mut self.gaps[insertion_index];

            gap.offset -= osize;
            gap.size += osize;
        } else {
            // no neighbors, make a new gap
            self.gaps.insert(
                insertion_index,
                MemoryGap {
                    offset,
                    size: osize,
                },
            );
        }
    }
}

// alignment must be a power of two
// assuming 16 byte alignment for 64 bit system
const ALIGNMENT: usize = 16;

fn align_up(addr: usize) -> usize {
    (addr + ALIGNMENT - 1) & !(ALIGNMENT - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SPACE: usize = 64;

    #[test]
    fn continuous_region() {
        let mut mem = Memory::new(TEST_SPACE);

        mem.realloc(std::ptr::null_mut(), 0, 1);
        mem.realloc(std::ptr::null_mut(), 0, 1);
        mem.realloc(std::ptr::null_mut(), 0, 1);

        assert_eq!(
            mem.used_memory(),
            ALIGNMENT * 3,
            "three chunks of memory used"
        );
        assert_eq!(
            mem.gaps.len(),
            1,
            "no fragmentation, gap at the end representing unused memory"
        );
    }

    #[test]
    fn create_gap() {
        let mut mem = Memory::new(TEST_SPACE);

        mem.realloc(std::ptr::null_mut(), 0, 1);
        let mid = mem.realloc(std::ptr::null_mut(), 0, 1);
        mem.realloc(std::ptr::null_mut(), 0, 1);

        // free
        mem.realloc(mid, 1, 0);

        assert_eq!(mem.used_memory(), ALIGNMENT * 2, "freed one chunk");
        assert_eq!(mem.gaps.len(), 2, "created a gap");
        assert_eq!(
            mem.gaps[0].clone(),
            MemoryGap {
                offset: ALIGNMENT,
                size: ALIGNMENT
            },
            "created a gap"
        );

        mem.realloc(std::ptr::null_mut(), 0, ALIGNMENT);
        assert_eq!(mem.gaps.len(), 1, "sealed gap");
        assert_eq!(
            mem.gaps[0].clone(),
            MemoryGap {
                offset: ALIGNMENT * 3,
                size: TEST_SPACE - ALIGNMENT * 3
            },
        );
    }

    #[test]
    fn find_space() {
        let mut mem = Memory::new(TEST_SPACE);

        mem.realloc(std::ptr::null_mut(), 0, 1);
        let mid = mem.realloc(std::ptr::null_mut(), 0, 1);
        mem.realloc(std::ptr::null_mut(), 0, 1);

        // free
        mem.realloc(mid, 1, 0);

        mem.realloc(std::ptr::null_mut(), 0, ALIGNMENT * 2);
        assert_eq!(mem.gaps.len(), 2, "ignored gap");
    }

    #[test]
    fn merging_gaps() {
        let mut mem = Memory::new(TEST_SPACE);

        mem.realloc(std::ptr::null_mut(), 0, 1);
        let mid = mem.realloc(std::ptr::null_mut(), 0, 1);
        let right = mem.realloc(std::ptr::null_mut(), 0, 1);

        // free
        mem.realloc(mid, 1, 0);
        mem.realloc(right, 1, 0);

        assert_eq!(mem.used_memory(), ALIGNMENT, "freed two chunks");
        assert_eq!(mem.gaps.len(), 1, "continuous memory");
    }
}
