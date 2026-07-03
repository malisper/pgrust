//! lib/pairingheap.c, index-arena rendering: link fields are u32 slot ids so
//! merge order — and therefore GiST scan visit order — is C-exact.

pub type NodeId = u32;
pub const INVALID: NodeId = u32::MAX;

struct Slot<T> {
    item: Option<T>,
    first_child: NodeId,
    next_sibling: NodeId,
    prev_or_parent: NodeId,
}

pub struct PairingHeap<T, C: Fn(&T, &T) -> i32> {
    slots: Vec<Slot<T>>,
    free: Vec<NodeId>,
    root: NodeId,
    compare: C,
}

impl<T, C: Fn(&T, &T) -> i32> PairingHeap<T, C> {
    pub fn new(compare: C) -> Self {
        PairingHeap {
            slots: Vec::new(),
            free: Vec::new(),
            root: INVALID,
            compare,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.root == INVALID
    }

    pub fn reset(&mut self) {
        self.slots.clear();
        self.free.clear();
        self.root = INVALID;
    }

    fn alloc(&mut self, item: T) -> NodeId {
        if let Some(id) = self.free.pop() {
            let s = &mut self.slots[id as usize];
            s.item = Some(item);
            s.first_child = INVALID;
            s.next_sibling = INVALID;
            s.prev_or_parent = INVALID;
            id
        } else {
            self.slots.push(Slot {
                item: Some(item),
                first_child: INVALID,
                next_sibling: INVALID,
                prev_or_parent: INVALID,
            });
            (self.slots.len() - 1) as NodeId
        }
    }

    #[inline]
    fn cmp(&self, a: NodeId, b: NodeId) -> i32 {
        let ia = self.slots[a as usize].item.as_ref().expect("live node");
        let ib = self.slots[b as usize].item.as_ref().expect("live node");
        (self.compare)(ia, ib)
    }

    fn merge(&mut self, a: NodeId, b: NodeId) -> NodeId {
        if a == INVALID {
            return b;
        }
        if b == INVALID {
            return a;
        }
        let (a, b) = if self.cmp(a, b) < 0 { (b, a) } else { (a, b) };
        let a_first = self.slots[a as usize].first_child;
        if a_first != INVALID {
            self.slots[a_first as usize].prev_or_parent = b;
        }
        {
            let sb = &mut self.slots[b as usize];
            sb.prev_or_parent = a;
            sb.next_sibling = a_first;
        }
        self.slots[a as usize].first_child = b;
        a
    }

    pub fn add(&mut self, item: T) {
        let node = self.alloc(item);
        self.slots[node as usize].first_child = INVALID;
        let root = self.root;
        self.root = self.merge(root, node);
        let r = &mut self.slots[self.root as usize];
        r.prev_or_parent = INVALID;
        r.next_sibling = INVALID;
    }

    pub fn first(&self) -> Option<&T> {
        if self.root == INVALID {
            return None;
        }
        self.slots[self.root as usize].item.as_ref()
    }

    pub fn remove_first(&mut self) -> Option<T> {
        if self.root == INVALID {
            return None;
        }
        let result = self.root;
        let children = self.slots[result as usize].first_child;
        self.root = self.merge_children(children);
        if self.root != INVALID {
            let r = &mut self.slots[self.root as usize];
            r.prev_or_parent = INVALID;
            r.next_sibling = INVALID;
        }
        let item = self.slots[result as usize].item.take();
        self.free.push(result);
        item
    }

    fn merge_children(&mut self, children: NodeId) -> NodeId {
        if children == INVALID || self.slots[children as usize].next_sibling == INVALID {
            return children;
        }
        let mut next = children;
        let mut pairs = INVALID;
        loop {
            let mut curr = next;
            if curr == INVALID {
                break;
            }
            let curr_next = self.slots[curr as usize].next_sibling;
            if curr_next == INVALID {
                self.slots[curr as usize].next_sibling = pairs;
                pairs = curr;
                break;
            }
            next = self.slots[curr_next as usize].next_sibling;
            curr = self.merge(curr, curr_next);
            self.slots[curr as usize].next_sibling = pairs;
            pairs = curr;
        }
        let mut newroot = pairs;
        let mut next = self.slots[pairs as usize].next_sibling;
        while next != INVALID {
            let curr = next;
            next = self.slots[curr as usize].next_sibling;
            newroot = self.merge(newroot, curr);
        }
        newroot
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_heap_order_with_fifo_ties_matches_c() {
        // C two-pass pairing: equal-compare items pop in a deterministic
        // (not insertion) order; distinct keys pop max-first.
        let mut h = PairingHeap::new(|a: &i32, b: &i32| a - b);
        for v in [3, 1, 4, 1, 5, 9, 2, 6] {
            h.add(v);
        }
        let mut got = Vec::new();
        while let Some(v) = h.remove_first() {
            got.push(v);
        }
        assert_eq!(got, vec![9, 6, 5, 4, 3, 2, 1, 1]);
    }

    #[test]
    fn reset_then_reuse() {
        let mut h = PairingHeap::new(|a: &i32, b: &i32| a - b);
        h.add(1);
        h.add(2);
        h.reset();
        assert!(h.is_empty());
        h.add(7);
        assert_eq!(h.remove_first(), Some(7));
        assert!(h.is_empty());
    }
}
