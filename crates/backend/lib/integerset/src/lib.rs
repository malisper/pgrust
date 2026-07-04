#![no_std]

use mcx::{vec_new_in, Mcx, PgVec};
use types_error::{PgError, PgResult};

const SIMPLE8B_MAX_VALUES_PER_CODEWORD: usize = 240;

// If you change the fanouts, you must recalculate MAX_TREE_LEVELS too:
// MAX_LEAF_ITEMS * MAX_INTERNAL_ITEMS ^ (MAX_TREE_LEVELS - 1) >= 2^64.
const MAX_INTERNAL_ITEMS: usize = 64;
const MAX_LEAF_ITEMS: usize = 64;
const MAX_TREE_LEVELS: usize = 11;

const MAX_VALUES_PER_LEAF_ITEM: usize = 1 + SIMPLE8B_MAX_VALUES_PER_CODEWORD;

// Must exceed MAX_VALUES_PER_LEAF_ITEM so the encoder can always fill a leaf item.
const MAX_BUFFERED_VALUES: usize = MAX_VALUES_PER_LEAF_ITEM * 2;

// Arena-index "NULL". C links nodes with raw pointers; here every node lives in
// one of two per-set arenas (nodes are never freed or moved out from under an
// index, matching C's never-pfreed dedicated context) and links are u32
// indices. A downlink held by a level-L internal node resolves in the leaf
// arena iff L == 1, else in the internal arena.
const NIL: u32 = u32::MAX;

#[derive(Clone, Copy)]
struct LeafItem {
    first: u64,
    codeword: u64,
}

struct InternalNode {
    level: u16,
    num_items: u16,
    values: [u64; MAX_INTERNAL_ITEMS],
    downlinks: [u32; MAX_INTERNAL_ITEMS],
}

struct LeafNode {
    num_items: u16,
    next: u32,
    items: [LeafItem; MAX_LEAF_ITEMS],
}

const _: () = assert!(!core::mem::needs_drop::<InternalNode>());
const _: () = assert!(!core::mem::needs_drop::<LeafNode>());

#[derive(Clone, Copy, PartialEq, Eq)]
enum IterSource {
    DecodeBuf,
    Buffered,
}

pub struct IntegerSet<'mcx> {
    mcx: Mcx<'mcx>,
    mem_used: u64,

    num_entries: u64,
    highest_value: u64,

    num_levels: i32,
    // Root is a leaf-arena index iff num_levels == 1, else an internal-arena index.
    root: u32,
    rightmost_nodes: [u32; MAX_TREE_LEVELS],
    leftmost_leaf: u32,

    internal_nodes: PgVec<'mcx, InternalNode>,
    leaf_nodes: PgVec<'mcx, LeafNode>,

    buffered_values: [u64; MAX_BUFFERED_VALUES],
    num_buffered_values: i32,

    iter_active: bool,
    iter_source: IterSource,
    iter_num_values: i32,
    iter_valueno: i32,
    iter_node: u32,
    iter_itemno: i32,
    iter_values_buf: [u64; MAX_VALUES_PER_LEAF_ITEM],
}

impl<'mcx> IntegerSet<'mcx> {
    pub fn create(mcx: Mcx<'mcx>) -> IntegerSet<'mcx> {
        IntegerSet {
            mcx,
            mem_used: core::mem::size_of::<IntegerSet>() as u64,
            num_entries: 0,
            highest_value: 0,
            num_levels: 0,
            root: NIL,
            rightmost_nodes: [NIL; MAX_TREE_LEVELS],
            leftmost_leaf: NIL,
            internal_nodes: vec_new_in(mcx),
            leaf_nodes: vec_new_in(mcx),
            buffered_values: [0; MAX_BUFFERED_VALUES],
            num_buffered_values: 0,
            iter_active: false,
            iter_source: IterSource::DecodeBuf,
            iter_num_values: 0,
            iter_valueno: 0,
            iter_node: NIL,
            iter_itemno: 0,
            iter_values_buf: [0; MAX_VALUES_PER_LEAF_ITEM],
        }
    }

    fn new_internal_node(&mut self) -> PgResult<u32> {
        self.internal_nodes
            .try_reserve(1)
            .map_err(|_| self.mcx.oom(core::mem::size_of::<InternalNode>()))?;
        self.internal_nodes.push(InternalNode {
            level: 0,
            num_items: 0,
            values: [0; MAX_INTERNAL_ITEMS],
            downlinks: [NIL; MAX_INTERNAL_ITEMS],
        });
        self.mem_used += core::mem::size_of::<InternalNode>() as u64;
        Ok((self.internal_nodes.len() - 1) as u32)
    }

    fn new_leaf_node(&mut self) -> PgResult<u32> {
        self.leaf_nodes
            .try_reserve(1)
            .map_err(|_| self.mcx.oom(core::mem::size_of::<LeafNode>()))?;
        self.leaf_nodes.push(LeafNode {
            num_items: 0,
            next: NIL,
            items: [LeafItem { first: 0, codeword: 0 }; MAX_LEAF_ITEMS],
        });
        self.mem_used += core::mem::size_of::<LeafNode>() as u64;
        Ok((self.leaf_nodes.len() - 1) as u32)
    }

    pub fn num_entries(&self) -> u64 {
        self.num_entries
    }

    pub fn memory_usage(&self) -> u64 {
        self.mem_used
    }

    pub fn add_member(&mut self, x: u64) -> PgResult<()> {
        if self.iter_active {
            return Err(PgError::error(
                "cannot add new values to integer set while iteration is in progress",
            )
            .into());
        }
        if x <= self.highest_value && self.num_entries > 0 {
            return Err(PgError::error("cannot add value to integer set out of order").into());
        }

        if self.num_buffered_values as usize >= MAX_BUFFERED_VALUES {
            self.flush_buffered_values()?;
            debug_assert!((self.num_buffered_values as usize) < MAX_BUFFERED_VALUES);
        }

        self.buffered_values[self.num_buffered_values as usize] = x;
        self.num_buffered_values += 1;
        self.num_entries += 1;
        self.highest_value = x;
        Ok(())
    }

    fn flush_buffered_values(&mut self) -> PgResult<()> {
        let num_values = self.num_buffered_values;
        let mut num_packed: i32 = 0;

        let mut leaf: u32 = self.rightmost_nodes[0];
        if leaf == NIL {
            leaf = self.new_leaf_node()?;
            self.root = leaf;
            self.leftmost_leaf = leaf;
            self.rightmost_nodes[0] = leaf;
            self.num_levels = 1;
        }

        // Stop once fewer than MAX_VALUES_PER_LEAF_ITEM values remain, so the
        // encoder never runs out of input mid-codeword.
        while (num_values - num_packed) as usize >= MAX_VALUES_PER_LEAF_ITEM {
            let first = self.buffered_values[num_packed as usize];
            let (codeword, num_encoded) =
                simple8b_encode(&self.buffered_values[(num_packed as usize + 1)..], first);
            let item = LeafItem { first, codeword };

            if self.leaf_nodes[leaf as usize].num_items as usize >= MAX_LEAF_ITEMS {
                let old_leaf = leaf;
                leaf = self.new_leaf_node()?;
                self.leaf_nodes[old_leaf as usize].next = leaf;
                self.rightmost_nodes[0] = leaf;
                self.update_upper(1, leaf, item.first)?;
            }
            let node = &mut self.leaf_nodes[leaf as usize];
            node.items[node.num_items as usize] = item;
            node.num_items += 1;

            num_packed += 1 + num_encoded as i32;
        }

        if num_packed < self.num_buffered_values {
            self.buffered_values
                .copy_within(num_packed as usize..self.num_buffered_values as usize, 0);
        }
        self.num_buffered_values -= num_packed;
        Ok(())
    }

    fn update_upper(&mut self, level: i32, child: u32, child_key: u64) -> PgResult<()> {
        debug_assert!(level > 0);

        if level >= self.num_levels {
            if self.num_levels as usize == MAX_TREE_LEVELS {
                return Err(PgError::error(
                    "could not expand integer set, maximum number of levels reached",
                )
                .into());
            }
            let oldroot = self.root;
            let root_is_leaf = self.num_levels == 1;
            self.num_levels += 1;

            let downlink_key = if root_is_leaf {
                self.leaf_nodes[oldroot as usize].items[0].first
            } else {
                self.internal_nodes[oldroot as usize].values[0]
            };

            let parent = self.new_internal_node()?;
            let n = &mut self.internal_nodes[parent as usize];
            n.level = level as u16;
            n.values[0] = downlink_key;
            n.downlinks[0] = oldroot;
            n.num_items = 1;

            self.root = parent;
            self.rightmost_nodes[level as usize] = parent;
        }

        let parent = self.rightmost_nodes[level as usize];

        if (self.internal_nodes[parent as usize].num_items as usize) < MAX_INTERNAL_ITEMS {
            let n = &mut self.internal_nodes[parent as usize];
            let idx = n.num_items as usize;
            n.values[idx] = child_key;
            n.downlinks[idx] = child;
            n.num_items += 1;
        } else {
            let new_parent = self.new_internal_node()?;
            let n = &mut self.internal_nodes[new_parent as usize];
            n.level = level as u16;
            n.values[0] = child_key;
            n.downlinks[0] = child;
            n.num_items = 1;

            self.rightmost_nodes[level as usize] = new_parent;
            self.update_upper(level + 1, new_parent, child_key)?;
        }
        Ok(())
    }

    pub fn is_member(&self, x: u64) -> bool {
        // Ordered-insert invariant: anything >= buffered_values[0] cannot be in
        // the tree, so the buffer probe alone decides.
        if self.num_buffered_values > 0 && x >= self.buffered_values[0] {
            let arr = &self.buffered_values[..self.num_buffered_values as usize];
            let itemno = binsrch_uint64(x, arr, false);
            if itemno >= arr.len() {
                return false;
            }
            return arr[itemno] == x;
        }

        if self.root == NIL {
            return false;
        }
        let mut node = self.root;
        let mut level = self.num_levels - 1;
        while level > 0 {
            let n = &self.internal_nodes[node as usize];
            debug_assert_eq!(n.level as i32, level);

            let itemno = binsrch_uint64(x, &n.values[..n.num_items as usize], true);
            if itemno == 0 {
                return false;
            }
            node = n.downlinks[itemno - 1];
            level -= 1;
        }
        let leaf = &self.leaf_nodes[node as usize];

        let itemno = binsrch_leaf(x, &leaf.items[..leaf.num_items as usize], true);
        if itemno == 0 {
            return false;
        }
        let item = &leaf.items[itemno - 1];

        if item.first == x {
            return true;
        }
        debug_assert!(x > item.first);

        simple8b_contains(item.codeword, x, item.first)
    }

    pub fn begin_iterate(&mut self) {
        // An in-progress iteration may be abandoned midway.
        self.iter_active = true;
        self.iter_node = self.leftmost_leaf;
        self.iter_itemno = 0;
        self.iter_valueno = 0;
        self.iter_num_values = 0;
        self.iter_source = IterSource::DecodeBuf;
    }

    pub fn iterate_next(&mut self) -> Option<u64> {
        debug_assert!(self.iter_active);
        loop {
            if self.iter_valueno < self.iter_num_values {
                let v = match self.iter_source {
                    IterSource::DecodeBuf => self.iter_values_buf[self.iter_valueno as usize],
                    IterSource::Buffered => self.buffered_values[self.iter_valueno as usize],
                };
                self.iter_valueno += 1;
                return Some(v);
            }

            if self.iter_node != NIL {
                let node = &self.leaf_nodes[self.iter_node as usize];
                if self.iter_itemno < node.num_items as i32 {
                    let item = node.items[self.iter_itemno as usize];
                    self.iter_itemno += 1;

                    self.iter_values_buf[0] = item.first;
                    let num_decoded =
                        simple8b_decode(item.codeword, &mut self.iter_values_buf[1..], item.first);
                    self.iter_num_values = num_decoded as i32 + 1;
                    self.iter_valueno = 0;
                    self.iter_source = IterSource::DecodeBuf;
                    continue;
                }

                self.iter_node = node.next;
                self.iter_itemno = 0;
                continue;
            }

            if self.iter_source == IterSource::DecodeBuf {
                self.iter_source = IterSource::Buffered;
                self.iter_num_values = self.num_buffered_values;
                self.iter_valueno = 0;
                continue;
            }

            break;
        }

        self.iter_active = false;
        None
    }
}

// Returns the insert position for `item`. With nextkey, an equal key yields the
// position immediately after it; without, the equal key's own position.
fn binsrch_uint64(item: u64, arr: &[u64], nextkey: bool) -> usize {
    let mut low = 0;
    let mut high = arr.len();
    while high > low {
        let mid = low + (high - low) / 2;
        let go_right = if nextkey { item >= arr[mid] } else { item > arr[mid] };
        if go_right {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low
}

fn binsrch_leaf(item: u64, arr: &[LeafItem], nextkey: bool) -> usize {
    let mut low = 0;
    let mut high = arr.len();
    while high > low {
        let mid = low + (high - low) / 2;
        let go_right = if nextkey { item >= arr[mid].first } else { item > arr[mid].first };
        if go_right {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low
}

struct Simple8bMode {
    bits_per_int: u32,
    num_ints: u32,
}

const SIMPLE8B_MODES: [Simple8bMode; 17] = [
    Simple8bMode { bits_per_int: 0, num_ints: 240 },
    Simple8bMode { bits_per_int: 0, num_ints: 120 },
    Simple8bMode { bits_per_int: 1, num_ints: 60 },
    Simple8bMode { bits_per_int: 2, num_ints: 30 },
    Simple8bMode { bits_per_int: 3, num_ints: 20 },
    Simple8bMode { bits_per_int: 4, num_ints: 15 },
    Simple8bMode { bits_per_int: 5, num_ints: 12 },
    Simple8bMode { bits_per_int: 6, num_ints: 10 },
    Simple8bMode { bits_per_int: 7, num_ints: 8 },
    Simple8bMode { bits_per_int: 8, num_ints: 7 },
    Simple8bMode { bits_per_int: 10, num_ints: 6 },
    Simple8bMode { bits_per_int: 12, num_ints: 5 },
    Simple8bMode { bits_per_int: 15, num_ints: 4 },
    Simple8bMode { bits_per_int: 20, num_ints: 3 },
    Simple8bMode { bits_per_int: 30, num_ints: 2 },
    Simple8bMode { bits_per_int: 60, num_ints: 1 },
    Simple8bMode { bits_per_int: 0, num_ints: 0 },
];

// Looks like a mode-0 codeword, but a real mode-0 codeword has zeroes in the
// unused bits, so the two are distinguishable.
const EMPTY_CODEWORD: u64 = 0x0FFF_FFFF_FFFF_FFFF;

// Encodes the deltas-minus-one of `ints` relative to `base`; requires
// ints.len() >= SIMPLE8B_MAX_VALUES_PER_CODEWORD and strictly increasing input
// with ints[0] > base. Returns (codeword, count encoded); count is 0 (with
// EMPTY_CODEWORD) only when the first delta is >= 2^60.
fn simple8b_encode(ints: &[u64], base: u64) -> (u64, usize) {
    debug_assert!(ints.len() >= SIMPLE8B_MAX_VALUES_PER_CODEWORD);
    debug_assert!(ints[0] > base);

    // Every codeword must be "full" for its mode: widen the mode until the
    // pending delta fits, accept deltas until the mode's slot count is reached.
    // The sentinel mode (nints == 0) stops the widening after mode 15.
    let mut selector = 0usize;
    let mut nints = SIMPLE8B_MODES[0].num_ints as usize;
    let mut bits = SIMPLE8B_MODES[0].bits_per_int;
    let mut diff = ints[0] - base - 1;
    let mut last_val = ints[0];
    let mut i = 0usize;
    loop {
        if diff >= (1u64 << bits) {
            selector += 1;
            nints = SIMPLE8B_MODES[selector].num_ints as usize;
            bits = SIMPLE8B_MODES[selector].bits_per_int;
            if i >= nints {
                break;
            }
        } else {
            i += 1;
            if i >= nints {
                break;
            }
            debug_assert!(ints[i] > last_val);
            diff = ints[i] - last_val - 1;
            last_val = ints[i];
        }
    }

    if nints == 0 {
        debug_assert_eq!(i, 0);
        return (EMPTY_CODEWORD, 0);
    }

    // Shift values in reverse order so the decoder emits them in order.
    let mut codeword: u64 = 0;
    if bits > 0 {
        let mut j = nints - 1;
        while j > 0 {
            diff = ints[j] - ints[j - 1] - 1;
            codeword |= diff;
            codeword <<= bits;
            j -= 1;
        }
        diff = ints[0] - base - 1;
        codeword |= diff;
    }

    codeword |= (selector as u64) << 60;
    (codeword, nints)
}

fn simple8b_decode(codeword: u64, decoded: &mut [u64], base: u64) -> usize {
    let selector = (codeword >> 60) as usize;
    let nints = SIMPLE8B_MODES[selector].num_ints as usize;
    let bits = SIMPLE8B_MODES[selector].bits_per_int;
    let mask = (1u64 << bits) - 1;

    if codeword == EMPTY_CODEWORD {
        return 0;
    }

    let mut cw = codeword;
    let mut curr_value = base;
    for slot in decoded.iter_mut().take(nints) {
        let diff = cw & mask;
        curr_value += 1 + diff;
        *slot = curr_value;
        cw >>= bits;
    }
    nints
}

fn simple8b_contains(codeword: u64, key: u64, base: u64) -> bool {
    let selector = (codeword >> 60) as usize;
    let nints = SIMPLE8B_MODES[selector].num_ints as usize;
    let bits = SIMPLE8B_MODES[selector].bits_per_int;

    if codeword == EMPTY_CODEWORD {
        return false;
    }

    if bits == 0 {
        return (key - base) <= nints as u64;
    }

    let mask = (1u64 << bits) - 1;
    let mut cw = codeword;
    let mut curr_value = base;
    for _ in 0..nints {
        let diff = cw & mask;
        curr_value += 1 + diff;
        if curr_value >= key {
            return curr_value == key;
        }
        cw >>= bits;
    }
    false
}

#[cfg(test)]
extern crate std;

#[cfg(test)]
mod tests;
