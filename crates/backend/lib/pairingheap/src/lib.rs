// lib/pairingheap.c, generic slot-arena rendering (same scheme as the GiST
// scan-local copy in _support/types/gist): link fields are u32 slot ids, and
// merge / two-pass merge_children orders are C-exact, so pop order — including
// on comparator ties — matches C's intrusive version for the same op sequence.
// add() returns the slot id, standing in for C's pairingheap_node* so callers
// (C's pairingheap_remove consumers) can remove interior nodes.

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
        PairingHeap { slots: Vec::new(), free: Vec::new(), root: INVALID, compare }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.root == INVALID
    }

    // pairingheap_is_singular: root exists and has no children.
    #[inline]
    pub fn is_singular(&self) -> bool {
        self.root != INVALID && self.slots[self.root as usize].first_child == INVALID
    }

    pub fn reset(&mut self) {
        self.slots.clear();
        self.free.clear();
        self.root = INVALID;
    }

    #[inline]
    pub fn get(&self, id: NodeId) -> &T {
        self.slots[id as usize].item.as_ref().expect("live node")
    }

    #[inline]
    pub fn get_mut(&mut self, id: NodeId) -> &mut T {
        self.slots[id as usize].item.as_mut().expect("live node")
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

    pub fn add(&mut self, item: T) -> NodeId {
        let node = self.alloc(item);
        let root = self.root;
        self.root = self.merge(root, node);
        let r = &mut self.slots[self.root as usize];
        r.prev_or_parent = INVALID;
        r.next_sibling = INVALID;
        node
    }

    pub fn first(&self) -> Option<&T> {
        if self.root == INVALID {
            return None;
        }
        self.slots[self.root as usize].item.as_ref()
    }

    pub fn first_id(&self) -> NodeId {
        self.root
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

    // pairingheap_remove: unlink an interior node, splicing a merged subheap
    // of its children into its place.
    pub fn remove(&mut self, node: NodeId) -> T {
        if node == self.root {
            return self.remove_first().expect("live root");
        }
        let children = self.slots[node as usize].first_child;
        let next_sibling = self.slots[node as usize].next_sibling;
        let prev = self.slots[node as usize].prev_or_parent;
        debug_assert!(prev != INVALID);
        let prev_is_parent = self.slots[prev as usize].first_child == node;

        if children != INVALID {
            let replacement = self.merge_children(children);
            {
                let r = &mut self.slots[replacement as usize];
                r.prev_or_parent = prev;
                r.next_sibling = next_sibling;
            }
            if prev_is_parent {
                self.slots[prev as usize].first_child = replacement;
            } else {
                self.slots[prev as usize].next_sibling = replacement;
            }
            if next_sibling != INVALID {
                self.slots[next_sibling as usize].prev_or_parent = replacement;
            }
        } else {
            if prev_is_parent {
                self.slots[prev as usize].first_child = next_sibling;
            } else {
                self.slots[prev as usize].next_sibling = next_sibling;
            }
            if next_sibling != INVALID {
                self.slots[next_sibling as usize].prev_or_parent = prev;
            }
        }
        let item = self.slots[node as usize].item.take().expect("live node");
        self.free.push(node);
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

    fn max_heap() -> PairingHeap<i64, fn(&i64, &i64) -> i32> {
        PairingHeap::new(|a, b| {
            if a > b {
                1
            } else if a < b {
                -1
            } else {
                0
            }
        })
    }

    #[test]
    fn pop_order_max_first() {
        let mut h = max_heap();
        for v in [3i64, 1, 4, 1, 5, 9, 2, 6] {
            h.add(v);
        }
        assert!(!h.is_empty());
        let mut got = Vec::new();
        while let Some(v) = h.remove_first() {
            got.push(v);
        }
        assert_eq!(got, vec![9, 6, 5, 4, 3, 2, 1, 1]);
    }

    #[test]
    fn singular_flag() {
        let mut h = max_heap();
        assert!(!h.is_singular());
        h.add(1);
        assert!(h.is_singular());
        h.add(2);
        assert!(!h.is_singular());
    }

    #[test]
    fn interior_remove_random_matches_model() {
        let mut h = max_heap();
        let mut ids = Vec::new();
        let mut model = Vec::new();
        let mut x: u64 = 42;
        for _ in 0..300 {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let v = (x >> 40) as i64;
            ids.push((h.add(v), v));
            model.push(v);
        }
        // Remove every third inserted node via remove(id), root or interior.
        let mut k = 0;
        ids.retain(|&(id, v)| {
            k += 1;
            if k % 3 == 0 {
                let got = h.remove(id);
                assert_eq!(got, v);
                let pos = model.iter().position(|&m| m == v).unwrap();
                model.remove(pos);
                false
            } else {
                true
            }
        });
        model.sort_by(|a, b| b.cmp(a));
        let mut got = Vec::new();
        while let Some(v) = h.remove_first() {
            got.push(v);
        }
        assert_eq!(got, model);
    }

    #[test]
    fn reset_then_reuse() {
        let mut h = max_heap();
        h.add(1);
        h.add(2);
        h.reset();
        assert!(h.is_empty());
        h.add(7);
        assert_eq!(h.remove_first(), Some(7));
        assert!(h.is_empty());
    }
}
