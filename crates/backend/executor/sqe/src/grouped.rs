//! Shared machinery for the GROUPED band: open-addressed count
//! tables (u64/u128 keys), the global text-group registry (dict codes →
//! dense global group ids), radix partition helpers, and canonical top-k
//! rendering.
//!
//! Design laws (phase-1 lessons, applied):
//! - cnt==0 is the empty sentinel everywhere (first insert always sets ≥1).
//! - Partition ownership (radix by key/user hash, one owner per partition,
//!   NO merge) is the default shape at high NDV; thread-local tables +
//!   merge exist to MEASURE the merge tax, and dense gid arrays make merge
//!   a vector add (the dict-dense exception).

use crate::bank::Bank;
use crate::fold::AccumCell;
use crate::scan::{open_cursor, varlena_payload, Scratch};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// hashes
// ---------------------------------------------------------------------------

#[inline(always)]
pub fn hash64(x: u64) -> u64 {
    // splitmix64 finisher — full avalanche, cheap.
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[inline(always)]
pub fn hash128(key: u128) -> u64 {
    hash64((key as u64) ^ ((key >> 64) as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

/// FNV-1a over bytes (string registry hashing; dict entries hash once per
/// ENTRY, so hash quality > speed here anyway).
#[inline(always)]
pub fn hash_bytes(b: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for &c in b {
        h = (h ^ c as u64).wrapping_mul(0x1000_0000_01b3);
    }
    h
}

pub const RADIX_P: usize = 256;

#[inline(always)]
pub fn radix_of(h: u64) -> usize {
    (h >> 56) as usize
}

// ---------------------------------------------------------------------------
// open-addressed count tables
// ---------------------------------------------------------------------------

/// u64 key → u64 count, linear probe, grow at 3/4 load.
pub struct Cnt64 {
    pub keys: Vec<u64>,
    pub cnt: Vec<u32>,
    mask: usize,
    pub len: usize,
}

impl Cnt64 {
    pub fn new(expect: usize) -> Cnt64 {
        let cap = (expect * 2).next_power_of_two().max(16);
        Cnt64 {
            keys: vec![0; cap],
            cnt: vec![0; cap],
            mask: cap - 1,
            len: 0,
        }
    }
    #[inline(always)]
    pub fn add(&mut self, key: u64, by: u32) {
        if self.len * 4 >= (self.mask + 1) * 3 {
            self.grow();
        }
        let mut slot = (hash64(key) as usize) & self.mask;
        loop {
            if self.cnt[slot] == 0 {
                self.keys[slot] = key;
                self.cnt[slot] = by;
                self.len += 1;
                return;
            }
            if self.keys[slot] == key {
                self.cnt[slot] += by;
                return;
            }
            slot = (slot + 1) & self.mask;
        }
    }
    fn grow(&mut self) {
        let mut big = Cnt64::new(self.mask + 2);
        for s in 0..=self.mask {
            if self.cnt[s] != 0 {
                big.add(self.keys[s], self.cnt[s]);
            }
        }
        *self = big;
    }
    pub fn drain_into(&self, out: &mut Vec<(u64, u64)>) {
        for s in 0..=self.mask {
            if self.cnt[s] != 0 {
                out.push((self.keys[s], self.cnt[s] as u64));
            }
        }
    }
}

/// The Cnt64 sibling for grouped FOLDS (sqe-grpfold): u64 key → per-group
/// row count + a block of `naggs` fold cells (fold.rs `AccumCell` — the
/// one scatter-fold law). rows==0 is the empty sentinel (a group exists
/// only once a row touched it, so first insert always sets rows >= 1);
/// linear probe, grow at 3/4 load.
pub struct Cells64 {
    pub keys: Vec<u64>,
    pub rows: Vec<u64>,
    /// Cell blocks, `naggs` per slot (slot s owns `cells[s*naggs..]`).
    pub cells: Vec<AccumCell>,
    pub naggs: usize,
    mask: usize,
    pub len: usize,
}

impl Cells64 {
    pub fn new(expect: usize, naggs: usize) -> Cells64 {
        let cap = (expect * 2).next_power_of_two().max(16);
        Cells64 {
            keys: vec![0; cap],
            rows: vec![0; cap],
            cells: vec![AccumCell::default(); cap * naggs],
            naggs,
            mask: cap - 1,
            len: 0,
        }
    }
    /// Pre-size for `n` more keys: growth REHASHES (slots move), so a
    /// batch user recording slot indices MUST reserve the whole batch
    /// first — after this, `n` touches cannot grow the table.
    pub fn reserve_batch(&mut self, n: usize) {
        while (self.len + n) * 4 >= (self.mask + 1) * 3 {
            self.grow();
        }
    }
    /// Count one row into `key`'s group; returns the group's SLOT (its
    /// cell block starts at `slot * naggs`). The slot is stable only
    /// until the next growth (see `reserve_batch`).
    #[inline(always)]
    pub fn touch(&mut self, key: u64) -> usize {
        if self.len * 4 >= (self.mask + 1) * 3 {
            self.grow();
        }
        let mut slot = (hash64(key) as usize) & self.mask;
        loop {
            if self.rows[slot] == 0 {
                self.keys[slot] = key;
                self.rows[slot] = 1;
                self.len += 1;
                return slot;
            }
            if self.keys[slot] == key {
                self.rows[slot] += 1;
                return slot;
            }
            slot = (slot + 1) & self.mask;
        }
    }
    fn grow(&mut self) {
        let mut big = Cells64::new(self.mask + 2, self.naggs);
        for s in 0..=self.mask {
            if self.rows[s] != 0 {
                let slot = big.touch(self.keys[s]);
                big.rows[slot] = self.rows[s];
                big.cells[slot * big.naggs..(slot + 1) * big.naggs]
                    .copy_from_slice(&self.cells[s * self.naggs..(s + 1) * self.naggs]);
            }
        }
        *self = big;
    }
    /// Visit every live group as `(key, rows, cell block)`.
    pub fn for_each(&self, mut f: impl FnMut(u64, u64, &[AccumCell])) {
        for s in 0..=self.mask {
            if self.rows[s] != 0 {
                f(self.keys[s], self.rows[s], &self.cells[s * self.naggs..(s + 1) * self.naggs]);
            }
        }
    }
}

/// `Cells64` on u128 keys (the packed 1-2-byval-column group key of the
/// hash-plane foundation shape). rows==0 is the empty sentinel.
pub struct Cells128 {
    pub keys: Vec<u128>,
    pub rows: Vec<u64>,
    pub cells: Vec<AccumCell>,
    pub naggs: usize,
    mask: usize,
    pub len: usize,
}

impl Cells128 {
    pub fn new(expect: usize, naggs: usize) -> Cells128 {
        let cap = (expect * 2).next_power_of_two().max(16);
        Cells128 {
            keys: vec![0; cap],
            rows: vec![0; cap],
            cells: vec![AccumCell::default(); cap * naggs],
            naggs,
            mask: cap - 1,
            len: 0,
        }
    }
    /// Pre-size for `n` more keys (see `Cells64::reserve_batch`: growth
    /// rehashes, so batch users reserve BEFORE recording slots).
    pub fn reserve_batch(&mut self, n: usize) {
        while (self.len + n) * 4 >= (self.mask + 1) * 3 {
            self.grow();
        }
    }
    /// Count `by` rows into `key`'s group; returns the group's SLOT. The
    /// slot is stable only until the next growth (see `reserve_batch`).
    #[inline(always)]
    pub fn touch(&mut self, key: u128, by: u64) -> usize {
        if self.len * 4 >= (self.mask + 1) * 3 {
            self.grow();
        }
        let mut slot = (hash128(key) as usize) & self.mask;
        loop {
            if self.rows[slot] == 0 {
                self.keys[slot] = key;
                self.rows[slot] = by;
                self.len += 1;
                return slot;
            }
            if self.keys[slot] == key {
                self.rows[slot] += by;
                return slot;
            }
            slot = (slot + 1) & self.mask;
        }
    }
    fn grow(&mut self) {
        let mut big = Cells128::new(self.mask + 2, self.naggs);
        for s in 0..=self.mask {
            if self.rows[s] != 0 {
                let slot = big.touch(self.keys[s], self.rows[s]);
                big.cells[slot * big.naggs..(slot + 1) * big.naggs]
                    .copy_from_slice(&self.cells[s * self.naggs..(s + 1) * self.naggs]);
            }
        }
        *self = big;
    }
    /// Visit every live group as `(key, rows, cell block)`.
    pub fn for_each(&self, mut f: impl FnMut(u128, u64, &[AccumCell])) {
        for s in 0..=self.mask {
            if self.rows[s] != 0 {
                f(self.keys[s], self.rows[s], &self.cells[s * self.naggs..(s + 1) * self.naggs]);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// [sqe-tpch-mech] direct-array atomic lanes. Zero-initialized lanes ride
// the allocator's zeroed pages (alloc_zeroed): a 48MB lane must not be
// TOUCHED at construction — first use faults only the pages real groups
// live on. AtomicU64/AtomicI64 are documented same-size/same-bit-validity
// as their integer twins, so the zeroed buffer reinterprets soundly.
// ---------------------------------------------------------------------------

pub fn atomic_zeros_u64(n: usize) -> Vec<std::sync::atomic::AtomicU64> {
    let mut v = vec![0u64; n];
    let (p, len, cap) = (v.as_mut_ptr(), v.len(), v.capacity());
    std::mem::forget(v);
    unsafe { Vec::from_raw_parts(p as *mut std::sync::atomic::AtomicU64, len, cap) }
}

pub fn atomic_fill_i64(n: usize, x: i64) -> Vec<std::sync::atomic::AtomicI64> {
    let mut v = vec![x; n];
    let (p, len, cap) = (v.as_mut_ptr(), v.len(), v.capacity());
    std::mem::forget(v);
    unsafe { Vec::from_raw_parts(p as *mut std::sync::atomic::AtomicI64, len, cap) }
}

/// u128 key → u32 count. `add` returns true on FIRST insert (drives
/// distinct counting through the same table).
pub struct Cnt128 {
    pub keys: Vec<u128>,
    pub cnt: Vec<u32>,
    mask: usize,
    pub len: usize,
}

impl Cnt128 {
    pub fn new(expect: usize) -> Cnt128 {
        let cap = (expect * 2).next_power_of_two().max(16);
        Cnt128 {
            keys: vec![0; cap],
            cnt: vec![0; cap],
            mask: cap - 1,
            len: 0,
        }
    }
    #[inline(always)]
    pub fn add(&mut self, key: u128, by: u32) -> bool {
        if self.len * 4 >= (self.mask + 1) * 3 {
            self.grow();
        }
        let mut slot = (hash128(key) as usize) & self.mask;
        loop {
            if self.cnt[slot] == 0 {
                self.keys[slot] = key;
                self.cnt[slot] = by;
                self.len += 1;
                return true;
            }
            if self.keys[slot] == key {
                self.cnt[slot] += by;
                return false;
            }
            slot = (slot + 1) & self.mask;
        }
    }
    /// Active table capacity (reset-aware; buffers may be larger).
    pub fn cap(&self) -> usize {
        self.mask + 1
    }
    /// [sqe-m2] Re-arm a parked table for a new expected size: grow-only
    /// buffers, active window cleared (fill beats fresh page faults).
    pub fn reset(&mut self, expect: usize) {
        let cap = (expect * 2).next_power_of_two().max(16);
        if self.keys.len() < cap {
            self.keys.resize(cap, 0);
            self.cnt.resize(cap, 0);
        }
        self.cnt[..cap].fill(0);
        self.mask = cap - 1;
        self.len = 0;
    }
    fn grow(&mut self) {
        let mut big = Cnt128::new(self.mask + 2);
        for s in 0..=self.mask {
            if self.cnt[s] != 0 {
                big.add(self.keys[s], self.cnt[s]);
            }
        }
        *self = big;
    }
    pub fn drain_into(&self, out: &mut Vec<(u128, u64)>) {
        for s in 0..=self.mask {
            if self.cnt[s] != 0 {
                out.push((self.keys[s], self.cnt[s] as u64));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// text-group registry (dict codes → dense global gids)
// ---------------------------------------------------------------------------

/// Global dense group ids for one text column: per part, code → gid; the
/// string work is per DICT ENTRY (once per distinct value per part), never
/// per row. Non-dict parts keep `None` and fall back to the per-row map.
pub struct TextReg {
    pub strings: Vec<Vec<u8>>,
    pub map: HashMap<Vec<u8>, u32>,
    pub per_part: Vec<Option<Vec<u32>>>,
    pub empty_gid: u32,
    pub dict_parts: usize,
}

impl TextReg {
    /// Build the registry: string work per DICT ENTRY on dict parts; on
    /// non-dict parts every row is interned HERE (once, build-time) so all
    /// later gid streams are read-only — that is what makes the parallel
    /// kernels' gid domain globally consistent with zero synchronization.
    pub fn build(bank: &Bank, attno: u32) -> TextReg {
        let mut strings: Vec<Vec<u8>> = Vec::new();
        let mut map: HashMap<Vec<u8>, u32> = HashMap::new();
        fn intern(
            b: &[u8],
            strings: &mut Vec<Vec<u8>>,
            map: &mut HashMap<Vec<u8>, u32>,
        ) -> u32 {
            if let Some(&g) = map.get(b) {
                return g;
            }
            let g = strings.len() as u32;
            strings.push(b.to_vec());
            map.insert(b.to_vec(), g);
            g
        }
        let empty_gid = intern(b"", &mut strings, &mut map);
        let mut per_part: Vec<Option<Vec<u32>>> = Vec::with_capacity(bank.parts.len());
        let mut dict_parts = 0usize;
        let mut scratch = crate::scan::scratch_fetch();
        for pi in 0..bank.parts.len() {
            if crate::scan::is_dict(bank, pi, attno) {
                dict_parts += 1;
                let dh = crate::scan::dict_handle(bank, pi, attno);
                let n = dh.ncodes();
                let mut c2g = vec![0u32; n as usize];
                if crate::engine::bulk_entries_on() {
                    // [bulkentries] Bulk sequential decode of the full dict.
                    let mut cur = dh.entries(0, n).expect("dict cursor");
                    while let Some((c, e)) = cur.next_entry().expect("dict entry") {
                        c2g[c as usize] = intern(e.bytes, &mut strings, &mut map);
                    }
                } else {
                    for c in 0..n {
                        let e = dh.entry(c).expect("dict entry");
                        c2g[c as usize] = intern(e.bytes, &mut strings, &mut map);
                    }
                }
                per_part.push(Some(c2g));
            } else {
                let mut cur = open_cursor(bank, pi, attno);
                for g in 0..cur.granule_count() {
                    let rows = cur.rows_in_granule(g) as usize;
                    let d = scratch.decode_full(&mut cur, g, rows);
                    for &x in d {
                        let p = unsafe { varlena_payload(x) };
                        intern(p, &mut strings, &mut map);
                    }
                }
                per_part.push(None);
            }
        }
        crate::scan::scratch_park(scratch);
        TextReg {
            strings,
            map,
            per_part,
            empty_gid,
            dict_parts,
        }
    }

    pub fn ngids(&self) -> usize {
        self.strings.len()
    }

    /// gid of a raw payload (non-dict-part row path; always present after
    /// build).
    #[inline(always)]
    pub fn gid_of(&self, b: &[u8]) -> u32 {
        *self.map.get(b).expect("registry covers every value by construction")
    }

    /// Parallel registry build (the Umbra-directive push: serial interning
    /// of ~1.6M dict entries was the dominant cold cost of every text
    /// kernel). Shape: phase A scatters (hash, part, code, bytes) per dict
    /// ENTRY into 256 hash partitions; phase B owners sort + dedupe their
    /// partition and assign partition-local ids (deterministic: sorted
    /// order); phase C stitches global gids by prefix sum. NO string map
    /// is built — this arm requires every part dict-published (true for
    /// every text column of this bank; falls back to serial build else).
    pub fn build_par(bank: &Bank, attno: u32, pool: &crate::pool::Pool) -> TextReg {
        if !(0..bank.parts.len()).all(|pi| crate::scan::is_dict(bank, pi, attno)) {
            return TextReg::build(bank, attno);
        }
        let nparts = bank.parts.len();
        // phase A: per-part entry scatter.
        let pass_a = pool.run(
            nparts,
            |_| (0..RADIX_P)
                .map(|_| Vec::new())
                .collect::<Vec<Vec<(u64, u32, u32, Vec<u8>)>>>(),
            |buckets, pi| {
                let dh = crate::scan::dict_handle(bank, pi, attno);
                if crate::engine::bulk_entries_on() {
                    // [bulkentries] Bulk sequential decode of the full dict.
                    let mut cur = dh.entries(0, dh.ncodes()).expect("dict cursor");
                    while let Some((c, e)) = cur.next_entry().expect("dict entry") {
                        let h = hash_bytes(e.bytes);
                        buckets[radix_of(h)].push((h, pi as u32, c, e.bytes.to_vec()));
                    }
                } else {
                    for c in 0..dh.ncodes() {
                        let e = dh.entry(c).expect("dict entry");
                        let h = hash_bytes(e.bytes);
                        buckets[radix_of(h)].push((h, pi as u32, c, e.bytes.to_vec()));
                    }
                }
            },
        );
        let scattered: Vec<&Vec<Vec<(u64, u32, u32, Vec<u8>)>>> = pass_a.iter().collect();
        // phase B: partition-owned dedupe + local id assignment.
        let owned = pool.run(
            RADIX_P,
            |_| Vec::new(),
            |out: &mut Vec<(usize, Vec<Vec<u8>>, Vec<(u32, u32, u32)>)>, p| {
                let mut items: Vec<(u64, u32, u32, &[u8])> = Vec::new();
                for b in &scattered {
                    for (h, pi, c, s) in &b[p] {
                        items.push((*h, *pi, *c, s.as_slice()));
                    }
                }
                items.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.3.cmp(b.3)));
                let mut strings: Vec<Vec<u8>> = Vec::new();
                let mut assign: Vec<(u32, u32, u32)> = Vec::with_capacity(items.len());
                let mut prev: Option<&[u8]> = None;
                for (_, pi, c, s) in items {
                    if prev != Some(s) {
                        strings.push(s.to_vec());
                        prev = Some(s);
                    }
                    assign.push((pi, c, strings.len() as u32 - 1));
                }
                out.push((p, strings, assign));
            },
        );
        // phase C: stitch global gids.
        let mut parts_sorted: Vec<(usize, Vec<Vec<u8>>, Vec<(u32, u32, u32)>)> =
            owned.into_iter().flatten().collect();
        parts_sorted.sort_unstable_by_key(|e| e.0);
        let mut offsets = vec![0u32; RADIX_P + 1];
        for e in &parts_sorted {
            offsets[e.0 + 1] = e.1.len() as u32;
        }
        for p in 0..RADIX_P {
            offsets[p + 1] += offsets[p];
        }
        let mut strings: Vec<Vec<u8>> = Vec::with_capacity(offsets[RADIX_P] as usize);
        let mut per_part: Vec<Option<Vec<u32>>> = (0..nparts)
            .map(|pi| {
                let dh = crate::scan::dict_handle(bank, pi, attno);
                Some(vec![0u32; dh.ncodes() as usize])
            })
            .collect();
        for (p, ps, assign) in parts_sorted {
            let base = offsets[p];
            strings.extend(ps);
            for (pi, c, local) in assign {
                per_part[pi as usize].as_mut().unwrap()[c as usize] = base + local;
            }
        }
        let empty_gid = {
            // "" must exist in some part's dict (empty strings dominate
            // these columns); locate via any part's code 0.. scan cheap:
            let h = hash_bytes(b"");
            let p = radix_of(h);
            let base = offsets[p];
            let end = offsets[p + 1];
            (base..end)
                .find(|&g| strings[g as usize].is_empty())
                .unwrap_or_else(|| {
                    let g = strings.len() as u32;
                    strings.push(Vec::new());
                    g
                })
        };
        TextReg {
            strings,
            map: HashMap::new(),
            per_part,
            empty_gid,
            dict_parts: nparts,
        }
    }

    /// [r3b, coordinator rule 2026-08-15] SORTED-MERGE registry: the
    /// cross-part identity of a dict entry is resolved by a k-way MERGE of
    /// the parts' byte-rank-sorted dicts (owners by title-byte range,
    /// binary-searched per part, memcmp at the merge front) — no hash
    /// anywhere. Consequence: gids come out in BYTES order (gid ASC ==
    /// bytes ASC), so a (count DESC, gid ASC) top-k is already the
    /// canonical (count DESC, key ASC) order. Requires every part
    /// dict-published (falls back to the serial build otherwise).
    pub fn build_merge(bank: &Bank, attno: u32, pool: &crate::pool::Pool) -> TextReg {
        if !(0..bank.parts.len()).all(|pi| crate::scan::is_dict(bank, pi, attno)) {
            return TextReg::build(bank, attno);
        }
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;
        let nparts = bank.parts.len();
        let dhs: Vec<pgrc2_read::dicthandle::DictHandle> = (0..nparts)
            .map(|pi| crate::scan::dict_handle(bank, pi, attno))
            .collect();
        let dhs = &dhs;
        let t = pool.threads();
        let nown = (t * 4).max(8);
        let big = (0..nparts).max_by_key(|&pi| dhs[pi].ncodes()).unwrap_or(0);
        let nbig = dhs[big].ncodes();
        let bound = |k: usize| -> Option<&[u8]> {
            if k == 0 || k >= nown {
                None
            } else {
                let c = ((k as u64 * nbig as u64) / nown as u64) as u32;
                Some(dhs[big].entry(c.min(nbig.saturating_sub(1))).expect("dict entry").bytes)
            }
        };
        let lower_bound = |dh: &pgrc2_read::dicthandle::DictHandle, b: &[u8]| -> u32 {
            let (mut lo, mut hi) = (0u32, dh.ncodes());
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                if dh.entry(mid).expect("dict entry").bytes < b {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            lo
        };
        // owner output: (k, strings, assign (pi, code, local id))
        let owned = pool.run(
            nown,
            |_| Vec::new(),
            |out: &mut Vec<(usize, Vec<Vec<u8>>, Vec<(u32, u32, u32)>)>, k| {
                let blo = bound(k);
                let bhi = bound(k + 1);
                let mut curs: Vec<(u32, u32)> = Vec::with_capacity(nparts); // (i, hi)
                let mut heap: BinaryHeap<Reverse<(&[u8], u32)>> = BinaryHeap::with_capacity(nparts);
                for (pi, dh) in dhs.iter().enumerate() {
                    let lo = match blo {
                        None => 0,
                        Some(b) => lower_bound(dh, b),
                    };
                    let hi = match bhi {
                        None => dh.ncodes(),
                        Some(b) => lower_bound(dh, b).max(lo),
                    };
                    curs.push((lo, hi));
                    if lo < hi {
                        heap.push(Reverse((dh.entry(lo).expect("dict entry").bytes, pi as u32)));
                    }
                }
                let mut strings: Vec<Vec<u8>> = Vec::new();
                let mut assign: Vec<(u32, u32, u32)> = Vec::new();
                while let Some(Reverse((mb, p0))) = heap.pop() {
                    let local = strings.len() as u32;
                    strings.push(mb.to_vec());
                    let mut pi = p0;
                    loop {
                        let cu = &mut curs[pi as usize];
                        assign.push((pi, cu.0, local));
                        cu.0 += 1;
                        if cu.0 < cu.1 {
                            heap.push(Reverse((
                                dhs[pi as usize].entry(cu.0).expect("dict entry").bytes,
                                pi,
                            )));
                        }
                        match heap.peek() {
                            Some(Reverse((nb, np))) if *nb == mb => {
                                pi = *np;
                                heap.pop();
                            }
                            _ => break,
                        }
                    }
                }
                out.push((k, strings, assign));
            },
        );
        let mut parts_sorted: Vec<(usize, Vec<Vec<u8>>, Vec<(u32, u32, u32)>)> =
            owned.into_iter().flatten().collect();
        parts_sorted.sort_unstable_by_key(|e| e.0);
        let mut strings: Vec<Vec<u8>> = Vec::new();
        let mut per_part: Vec<Option<Vec<u32>>> = (0..nparts)
            .map(|pi| Some(vec![0u32; dhs[pi].ncodes() as usize]))
            .collect();
        for (_, ps, assign) in parts_sorted {
            let base = strings.len() as u32;
            strings.extend(ps);
            for (pi, c, local) in assign {
                per_part[pi as usize].as_mut().unwrap()[c as usize] = base + local;
            }
        }
        // "" sorts first: gid 0 iff present.
        let empty_gid = if strings.first().map(|s| s.is_empty()).unwrap_or(false) {
            0
        } else {
            let g = strings.len() as u32;
            strings.push(Vec::new());
            g
        };
        TextReg {
            strings,
            map: HashMap::new(),
            per_part,
            empty_gid,
            dict_parts: nparts,
        }
    }
}

// ---------------------------------------------------------------------------
// per-row gid stream: the common "decode this text column as global group
// ids" loop (dict parts: decode_codes + table walk; non-dict: hash rows).
// ---------------------------------------------------------------------------

/// Per-thread scratch for a gid stream over one text column.
pub struct GidStream {
    pub attno: u32,
    pub cc: crate::scan::CurCache,
    pub scratch: Scratch,
    codes: Vec<u32>,
    gids: Vec<u32>,
}

impl GidStream {
    /// Depot-riding constructor (the scratch-init discipline): the decode
    /// arena comes reset from the calling worker's depot; the cursor is
    /// per-engagement — never parked.
    pub fn fetch(attno: u32) -> GidStream {
        GidStream {
            attno,
            cc: crate::scan::CurCache::new(attno),
            scratch: crate::scan::scratch_fetch(),
            codes: vec![0; 8192],
            gids: vec![0; 8192],
        }
    }

    /// Park the decode arena back on this worker's depot (cursor drops).
    pub fn park(self) {
        crate::scan::scratch_park(self.scratch);
    }

    /// Decode granule (pi, g) as GLOBAL gids; returns the gid slice.
    pub fn granule<'s>(
        &'s mut self,
        bank: &Bank,
        reg: &TextReg,
        pi: usize,
        g: u32,
        rows: usize,
    ) -> &'s [u32] {
        if self.codes.len() < rows {
            self.codes.resize(rows, 0);
        }
        if self.gids.len() < rows {
            self.gids.resize(rows, 0);
        }
        match &reg.per_part[pi] {
            Some(c2g) => {
                let cur = self.cc.get(bank, pi);
                cur.decode_codes(g, &mut self.codes[..rows]).expect("codes");
                for r in 0..rows {
                    self.gids[r] = c2g[self.codes[r] as usize];
                }
            }
            None => {
                let cur = self.cc.get(bank, pi);
                let d = self.scratch.decode_full(cur, g, rows);
                // SAFETY: gids and datums are disjoint fields.
                let d: &[u64] = unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) };
                for (r, &x) in d.iter().enumerate() {
                    let p = unsafe { varlena_payload(x) };
                    self.gids[r] = reg.gid_of(p);
                }
            }
        }
        &self.gids[..rows]
    }
}

// ---------------------------------------------------------------------------
// canonical top-k rendering
// ---------------------------------------------------------------------------

/// ORDER BY count DESC, key ASC, LIMIT k — the canonical total order every
/// variant of a query must render identically (ties beyond the SQL's ORDER
/// BY are canonicalized on key; stated in the results doc).
pub fn top_by_count<K: Ord + Copy>(rows: &mut Vec<(K, u64)>, k: usize) {
    rows.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    rows.truncate(k);
}

/// Top-k under (count DESC, tie(key) ASC) where `tie` supplies the
/// canonical key order (string bytes for text groups — matches the banked
/// probes' `ORDER BY c DESC, <col>`). Threshold selection: find the k-th
/// count, keep only candidates at/above it, sort those. Exact, and the
/// full-table sort never happens.
pub fn topk_by_count_tie<K: Copy>(
    mut rows: Vec<(K, u64)>,
    k: usize,
    tie: impl Fn(&K, &K) -> std::cmp::Ordering,
) -> Vec<(K, u64)> {
    if rows.len() > k {
        let mut counts: Vec<u64> = rows.iter().map(|r| r.1).collect();
        let idx = counts.len() - k;
        let (_, &mut thresh, _) = counts.select_nth_unstable(idx);
        rows.retain(|r| r.1 >= thresh);
    }
    rows.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| tie(&a.0, &b.0)));
    rows.truncate(k);
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fold::{scatter_cell_fold, AggFoldOp};

    /// Cells64/Cells128 growth preserves rows AND cell blocks (the fold
    /// state moves with its key), and rows==0 stays the only sentinel.
    #[test]
    fn cells_tables_grow_preserving_cells() {
        let mut t = Cells64::new(1, 2);
        for i in 0..500u64 {
            let slot = t.touch(i % 37);
            let base = slot * 2;
            scatter_cell_fold(AggFoldOp::Sum, &mut t.cells[base], i as i64, true);
            scatter_cell_fold(AggFoldOp::Max, &mut t.cells[base + 1], (i % 37) as i64, true);
        }
        assert_eq!(t.len, 37);
        let (mut rows, mut total) = (0u64, 0i128);
        t.for_each(|k, r, cells| {
            rows += r;
            total += cells[0].a;
            assert_eq!(cells[1].a as u64, k, "max(key) == key per group");
            assert_eq!(r as i64, cells[0].b, "sum count tracks group rows");
        });
        assert_eq!(rows, 500);
        assert_eq!(total, (0..500i128).sum::<i128>());

        // batch law: after reserve_batch(n), n touches cannot grow, so
        // slots recorded across the whole batch stay live.
        let mut t = Cells64::new(1, 1);
        t.reserve_batch(1000);
        let slots: Vec<usize> = (0..1000u64).map(|i| t.touch(i)).collect();
        for (i, &s) in slots.iter().enumerate() {
            assert_eq!(t.keys[s], i as u64, "slot must survive the reserved batch");
        }

        let mut t = Cells128::new(1, 1);
        for i in 0..300u64 {
            let slot = t.touch(((i % 23) as u128) | 1 << 100, 1);
            scatter_cell_fold(AggFoldOp::Min, &mut t.cells[slot], -(i as i64), true);
        }
        assert_eq!(t.len, 23);
        let mut n = 0;
        t.for_each(|k, r, cells| {
            n += 1;
            assert_eq!(k >> 100, 1);
            assert!(r > 0 && cells[0].valid != 0);
        });
        assert_eq!(n, 23);
    }
}
