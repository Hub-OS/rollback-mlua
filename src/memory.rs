use std::os::raw::c_void;

#[derive(Debug, PartialEq, Eq, Clone)]
struct MemoryGap {
    offset: usize,
    size: usize,
}

// naive allocator
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

    pub fn compressed_clone(&self) -> Self {
        let mut end = self.heap.len();

        if let Some(gap) = self.gaps.last() {
            if gap.offset + gap.size == self.heap.len() {
                end = gap.offset
            }
        }

        Self {
            heap: self.heap[..end].to_vec(),
            gaps: self.gaps.clone(),
        }
    }

    pub fn copy_from(&mut self, other: &Self) {
        self.gaps.clone_from(&other.gaps);
        let slice = &mut self.heap[..other.heap.len()];

        slice.copy_from_slice(&other.heap);
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

        let Some((i, gap)) = self
            .gaps
            .iter_mut()
            .enumerate()
            .find(|(_, gap)| gap.size >= size)
        else {
            return std::ptr::null_mut();
        };

        let address = unsafe { self.heap.as_ptr().add(gap.offset) };
        gap.offset += size;
        gap.size -= size;

        if gap.size == 0 {
            self.gaps.remove(i);
        }

        address as *mut c_void
    }

    fn free(&mut self, ptr: *mut c_void, osize: usize) {
        if osize == 0 {
            return;
        }

        let offset = ptr as usize - self.heap.as_ptr() as usize;

        let Err(insertion_index) = self.gaps.binary_search_by_key(&offset, |gap| gap.offset) else {
            // the memory is already free?
            return;
        };

        // finding neighbors to extend or merge gaps
        let (left_gaps, right_gaps) = self.gaps.split_at_mut(insertion_index);
        let mut left_gap = left_gaps.last_mut();
        let mut right_gap = right_gaps.first_mut();

        if let Some(gap) = &left_gap {
            if gap.offset + gap.size != offset {
                left_gap = None;
            }
        }

        if let Some(gap) = &right_gap {
            if gap.offset != offset + osize {
                right_gap = None;
            }
        }

        match (left_gap, right_gap) {
            (Some(left_gap), Some(right_gap)) => {
                // two neighbors, join them
                let increase = osize + right_gap.size;

                left_gap.size += increase;

                self.gaps.remove(insertion_index);
            }

            (Some(left_gap), None) => {
                // extend left gap
                left_gap.size += osize;
            }

            (None, Some(right_gap)) => {
                // extend right gap
                right_gap.offset -= osize;
                right_gap.size += osize;
            }

            (None, None) => {
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

    #[test]
    fn left_gap_merge() {
        let mut mem = Memory::new(TEST_SPACE);

        let left = mem.realloc(std::ptr::null_mut(), 0, 1);

        // free
        mem.realloc(left, 1, 0);

        assert_eq!(mem.gaps.len(), 1, "continuous memory");
    }

    #[test]
    fn right_gap_merge() {
        let mut mem = Memory::new(TEST_SPACE);

        mem.realloc(std::ptr::null_mut(), 0, TEST_SPACE - ALIGNMENT);
        let right = mem.realloc(std::ptr::null_mut(), 0, ALIGNMENT);

        assert_eq!(mem.gaps.len(), 0, "no gaps");
        assert_eq!(mem.used_memory(), TEST_SPACE, "all memory is in use");

        // free
        mem.realloc(right, 1, 0);

        assert_eq!(mem.gaps.len(), 1, "continuous memory");
    }

    #[test]
    fn maintained_order() {
        let mut mem = Memory::new(TEST_SPACE);

        let left = mem.realloc(std::ptr::null_mut(), 0, ALIGNMENT);
        mem.realloc(std::ptr::null_mut(), 0, TEST_SPACE - ALIGNMENT * 2);
        let right = mem.realloc(std::ptr::null_mut(), 0, ALIGNMENT);

        assert_eq!(mem.gaps.len(), 0, "no gaps");
        assert_eq!(mem.used_memory(), TEST_SPACE, "all memory is in use");

        // free right first
        mem.realloc(right, 1, 0);

        // free left
        mem.realloc(left, 1, 0);

        assert!(mem.gaps[0].offset < mem.gaps[1].offset, "order maintained");
    }

    #[test]
    fn maintained_order_flipped() {
        let mut mem = Memory::new(TEST_SPACE);

        let left = mem.realloc(std::ptr::null_mut(), 0, ALIGNMENT);
        mem.realloc(std::ptr::null_mut(), 0, TEST_SPACE - ALIGNMENT * 2);
        let right = mem.realloc(std::ptr::null_mut(), 0, ALIGNMENT);

        assert_eq!(mem.gaps.len(), 0, "no gaps");
        assert_eq!(mem.used_memory(), TEST_SPACE, "all memory is in use");

        // free left first
        mem.realloc(left, 1, 0);

        // free right
        mem.realloc(right, 1, 0);

        assert!(mem.gaps[0].offset < mem.gaps[1].offset, "order maintained");
    }
}
