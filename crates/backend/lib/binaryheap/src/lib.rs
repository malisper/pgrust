// src/common/binaryheap.c: fixed-capacity binary max-heap over word-sized
// values. Sift orders are C-exact so the array layout — and therefore
// consumer visit order on comparator ties — matches C for the same op
// sequence. T: Copy mirrors C's bh_node_type word semantics.

pub struct BinaryHeap<T: Copy, C: Fn(&T, &T) -> i32> {
    // One fixed allocation at create, mirroring C's single palloc; the only
    // drop glue is the buffer deallocation itself (T: Copy has none).
    nodes: Box<[T]>,
    size: usize,
    has_heap_property: bool,
    compare: C,
}

impl<T: Copy + Default, C: Fn(&T, &T) -> i32> BinaryHeap<T, C> {
    pub fn allocate(capacity: usize, compare: C) -> Self {
        BinaryHeap {
            nodes: vec![T::default(); capacity].into_boxed_slice(),
            size: 0,
            has_heap_property: true,
            compare,
        }
    }
}

impl<T: Copy, C: Fn(&T, &T) -> i32> BinaryHeap<T, C> {
    pub fn reset(&mut self) {
        self.size = 0;
        self.has_heap_property = true;
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn get(&self, n: usize) -> T {
        debug_assert!(n < self.size);
        self.nodes[n]
    }

    pub fn add_unordered(&mut self, d: T) {
        if self.size >= self.nodes.len() {
            panic!("out of binary heap slots");
        }
        self.has_heap_property = false;
        self.nodes[self.size] = d;
        self.size += 1;
    }

    pub fn build(&mut self) {
        if self.size > 1 {
            let mut i = parent_offset(self.size - 1) as isize;
            while i >= 0 {
                self.sift_down(i as usize);
                i -= 1;
            }
        }
        self.has_heap_property = true;
    }

    pub fn add(&mut self, d: T) {
        if self.size >= self.nodes.len() {
            panic!("out of binary heap slots");
        }
        self.nodes[self.size] = d;
        self.size += 1;
        self.sift_up(self.size - 1);
    }

    pub fn first(&self) -> T {
        debug_assert!(!self.is_empty() && self.has_heap_property);
        self.nodes[0]
    }

    pub fn remove_first(&mut self) -> T {
        debug_assert!(!self.is_empty() && self.has_heap_property);
        let result = self.nodes[0];
        if self.size == 1 {
            self.size -= 1;
            return result;
        }
        self.size -= 1;
        self.nodes[0] = self.nodes[self.size];
        self.sift_down(0);
        result
    }

    pub fn remove_node(&mut self, n: usize) {
        debug_assert!(!self.is_empty() && self.has_heap_property);
        debug_assert!(n < self.size);
        self.size -= 1;
        let cmp = (self.compare)(&self.nodes[self.size], &self.nodes[n]);
        self.nodes[n] = self.nodes[self.size];
        if cmp > 0 {
            self.sift_up(n);
        } else if cmp < 0 {
            self.sift_down(n);
        }
    }

    pub fn replace_first(&mut self, d: T) {
        debug_assert!(!self.is_empty() && self.has_heap_property);
        self.nodes[0] = d;
        if self.size > 1 {
            self.sift_down(0);
        }
    }

    fn sift_up(&mut self, mut node_off: usize) {
        let node_val = self.nodes[node_off];
        while node_off != 0 {
            let parent_off = parent_offset(node_off);
            let parent_val = self.nodes[parent_off];
            if (self.compare)(&node_val, &parent_val) <= 0 {
                break;
            }
            self.nodes[node_off] = parent_val;
            node_off = parent_off;
        }
        self.nodes[node_off] = node_val;
    }

    fn sift_down(&mut self, mut node_off: usize) {
        let node_val = self.nodes[node_off];
        loop {
            let left_off = 2 * node_off + 1;
            let right_off = 2 * node_off + 2;
            let mut swap_off = left_off;
            if right_off < self.size
                && (self.compare)(&self.nodes[left_off], &self.nodes[right_off]) < 0
            {
                swap_off = right_off;
            }
            if left_off >= self.size || (self.compare)(&node_val, &self.nodes[swap_off]) >= 0 {
                break;
            }
            self.nodes[node_off] = self.nodes[swap_off];
            node_off = swap_off;
        }
        self.nodes[node_off] = node_val;
    }
}

#[inline]
fn parent_offset(i: usize) -> usize {
    (i - 1) / 2
}

#[cfg(test)]
mod tests {
    use super::*;

    fn min_heap(cap: usize) -> BinaryHeap<i64, fn(&i64, &i64) -> i32> {
        // Max-heap with inverted comparator = min-heap, the C consumers' idiom.
        BinaryHeap::allocate(cap, |a, b| {
            if a < b {
                1
            } else if a > b {
                -1
            } else {
                0
            }
        })
    }

    #[test]
    fn build_and_drain_sorted() {
        let mut h = min_heap(64);
        let vals = [5i64, 3, 9, 1, 7, 7, 0, -4, 100, 42];
        for v in vals {
            h.add_unordered(v);
        }
        h.build();
        let mut sorted = vals.to_vec();
        sorted.sort();
        let mut out = Vec::new();
        while !h.is_empty() {
            out.push(h.remove_first());
        }
        assert_eq!(out, sorted);
    }

    #[test]
    fn drain_matches_model() {
        let mut h = min_heap(1000);
        let mut model = Vec::new();
        let mut x: u64 = 999;
        for _ in 0..500 {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let v = (x >> 40) as i64;
            h.add(v);
            model.push(v);
        }
        model.sort();
        let mut out = Vec::new();
        while !h.is_empty() {
            out.push(h.remove_first());
        }
        assert_eq!(out, model);
    }

    #[test]
    fn replace_first_and_remove_node() {
        let mut h = min_heap(16);
        for v in [10i64, 20, 30, 40, 50] {
            h.add(v);
        }
        h.replace_first(35);
        assert_eq!(h.remove_first(), 20);
        let n = h.len();
        h.remove_node(n - 1);
        let mut out = Vec::new();
        while !h.is_empty() {
            out.push(h.remove_first());
        }
        assert!(out.windows(2).all(|w| w[0] <= w[1]));
        assert_eq!(out.len(), 3);
    }

    #[test]
    #[should_panic(expected = "out of binary heap slots")]
    fn overflow_panics() {
        let mut h = min_heap(2);
        h.add(1);
        h.add(2);
        h.add(3);
    }
}
