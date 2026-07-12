//! lanetable — the lane-native compact-row aggregation table (cbstore-v2
//! plan Stage 2.2, closing the measured 2.4–3.3×/6.6× aggregation-table gap
//! vs ClickHouse's HashMap/StringHashMap on the Stage-0 shim parity rig).
//!
//! Design provenance (the executor catalogs, stolen deliberately):
//!   * DuckDB `ht_entry_t`: 8-byte entries = 16-bit salt + 48-bit payload
//!     reference; zero = empty; salt compare is one masked u64 compare.
//!     Salt checks are DISABLED while the table holds ≤ 8192 entries
//!     (cache-resident: the extra compare is pure overhead) — DuckDB's
//!     documented threshold.
//!   * ClickHouse HashMap core: open addressing, single-step linear probing,
//!     fill < 0.5, 4×-growth until 2^23 buckets then 2×, integer hash of
//!     CRC32 class (here: the murmur3 64-bit finalizer — 5 cycles, full
//!     avalanche, no platform intrinsics).
//!   * ClickHouse StringHashMap idiom: keys ≤ 8 B pack into one u64 (length
//!     recovered from the zero padding via leading_zeros — PG text carries no
//!     NUL bytes, so packing is injective); longer keys go to a byte arena
//!     with the full 64-bit hash saved in the row (HashMapWithSavedHash:
//!     hash early-out before memcmp, rehash without touching key bytes).
//!   * ClickHouse two-level tables: above `TWO_LEVEL_THRESHOLD` entries the
//!     table converts to 256 sub-tables bucketed by the hash's top byte —
//!     the Stage-4 merge structure (bucket-parallel merge), built here so the
//!     parallel program only adds the claim loop.
//!   * ClickHouse PrefetchingHelper: batched probes prefetch the bucket for
//!     row i+lookahead, lookahead MEASURED from the first iterations and
//!     clamped to [4, 32]; prefetch engages only when the entry array
//!     exceeds L2 (below that the table is cache-resident and prefetching is
//!     pure overhead). The DuckDB alternative (no prefetch; a branchless
//!     pre-touch pass over the batch's buckets) is implemented alongside —
//!     [`PrefetchMode`] — and the shim microbench decides the default.
//!
//! Payload rows are compact packed [key words][state bytes] rows in chunked,
//! allocation-stable storage — no MinimalTuple headers, no per-entry
//! allocations. The table owns LAYOUT AND PROBE ONLY: aggregate transition /
//! finalize semantics stay with the caller, which treats the state bytes as
//! its own (nodeagg stores its `AggPerGroup` array there, exactly as the
//! C-ported tuplehash's `additionalsize` area, zero-initialized).
//!
//! Iteration is ROW (insertion) order — divergent from the C table's bucket
//! order, legal under the 2026-07-13 order-relaxation policy (same rows /
//! values / errors; group order free unless SQL mandates it).
//!
//! No dependencies: pure std, usable from bench rigs and the executor alike.

/// CH `group_by_two_level_threshold`: convert to 256 buckets above this many
/// entries.
pub const TWO_LEVEL_THRESHOLD: usize = 100_000;

/// DuckDB: salt compares are skipped while the table is at most this many
/// entries (cache-resident probes; the salt branch is pure overhead).
pub const SALT_DISABLE_MAX_ENTRIES: usize = 8192;

/// CH `min_bytes_for_prefetch` idea: prefetch only when the entry array
/// exceeds the L2 slice (Graviton3/4: 1–2 MiB per core; 1 MiB is the
/// conservative engage point).
pub const PREFETCH_MIN_TABLE_BYTES: usize = 1 << 20;

const SALT_SHIFT: u32 = 48;
const REF_MASK: u64 = (1 << SALT_SHIFT) - 1;
/// Salt bits 32..48 of the hash — DISJOINT from the two-level bucket byte
/// (hash bits 56..64) so intra-bucket salts keep their full 16 bits of
/// discrimination after conversion.
#[inline(always)]
fn salt_of(hash: u64) -> u64 {
    ((hash >> 32) & 0xFFFF) << SALT_SHIFT
}

/// Two-level bucket = the hash's top byte (CH: `hash >> (32 - 8)` on their
/// 32-bit bucket hash; ours is 64-bit).
#[inline(always)]
fn bucket_of(hash: u64) -> usize {
    (hash >> 56) as usize
}

/// murmur3 fmix64 — the integer key hash (CRC32-class cost, full avalanche).
#[inline(always)]
pub fn hash_int(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
    k ^= k >> 33;
    k = k.wrapping_mul(0xC4CE_B9FE_1A85_EC53);
    k ^= k >> 33;
    k
}

/// Byte-key hash: 8-byte-word mix loop (CH hashes strings CRC32-by-8B-words;
/// this is the same shape with multiply-mix combining). Tail bytes pack into
/// one final word. Internal-table use only — no semantic constraint on hash
/// choice (order already relaxed).
#[inline]
pub fn hash_bytes(b: &[u8]) -> u64 {
    // Short-key fast path (the GROUP BY-dominant case: SearchEngineID-class
    // 1-8 byte strings): one packed word, two mix rounds, no loop setup.
    if b.len() <= 8 {
        return hash_int(hash_int(0x9E37_79B9_7F4A_7C15 ^ (b.len() as u64) ^ pack8(b)));
    }
    let mut h: u64 = 0x9E37_79B9_7F4A_7C15 ^ (b.len() as u64);
    let mut chunks = b.chunks_exact(8);
    for c in &mut chunks {
        let w = u64::from_le_bytes(c.try_into().unwrap());
        h = hash_int(h ^ w);
    }
    let rem = chunks.remainder();
    if !rem.is_empty() {
        h = hash_int(h ^ pack8(rem));
    }
    hash_int(h)
}

/// Pack a ≤8-byte key into one u64, low byte first (injective for NUL-free
/// byte strings; PG text carries no NULs). Length recovery:
/// [`packed_len`].
#[inline(always)]
pub fn pack8(b: &[u8]) -> u64 {
    debug_assert!(b.len() <= 8);
    // Shift-composed byte loads, NOT copy_from_slice: a dynamic-length
    // sub-8-byte copy lowers to a real memcpy call — measured dominating the
    // str8 probe loop on the pod rig.
    let mut w = 0u64;
    let mut i = 0;
    while i < b.len() {
        // SAFETY: i < b.len().
        w |= (unsafe { *b.get_unchecked(i) } as u64) << (8 * i);
        i += 1;
    }
    w
}

/// Length of a [`pack8`]-packed key — CH's clz recovery (`toStringView`): the
/// padding is the zero high bytes.
#[inline(always)]
pub fn packed_len(w: u64) -> usize {
    8 - (w.leading_zeros() as usize) / 8
}

/// Prefetch idiom for the batched probes — the CH-vs-DuckDB A/B the shim
/// microbench decides (see module doc).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrefetchMode {
    /// No look-ahead of any kind (baseline).
    None,
    /// DuckDB: branchless pre-touch pass loading every row's bucket entry
    /// before the probe loop.
    PreTouch,
    /// ClickHouse PrefetchingHelper: measured look-ahead in [4, 32], engaged
    /// only when the entry array exceeds [`PREFETCH_MIN_TABLE_BYTES`].
    Adaptive,
}

/// Key representation of a table, fixed at build.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyRepr {
    /// One 64-bit integer key word (int2/int4/int8 keys canonicalized to i64
    /// by the caller).
    Int,
    /// Byte-string keys: 3 key words = [packed8-or-arena-offset, len,
    /// saved 64-bit hash].
    Bytes,
}

const INT_KEY_WORDS: usize = 1;
const BYTES_KEY_WORDS: usize = 3;

/// Row storage: fixed-stride rows in chunked, allocation-stable u64 arrays
/// (pointers into a row never move — resize allocates new chunks only).
struct RowStore {
    chunks: Vec<Box<[u64]>>,
    stride_words: usize,
    // Power-of-two rows per chunk as shift/mask — row_ptr is the probe hot
    // path's dependent load chain; a runtime div here costs ~2x on the
    // DRAM-bound curve.
    chunk_shift: u32,
    chunk_mask: usize,
    nrows: usize,
}

impl RowStore {
    fn new(stride_words: usize) -> RowStore {
        debug_assert!(stride_words >= 1);
        // ~256 KiB chunks; power-of-two row counts keep the index math to
        // shift/mask.
        let rows_per_chunk = ((1usize << 15) / stride_words).next_power_of_two().max(64);
        RowStore {
            chunks: Vec::new(),
            stride_words,
            chunk_shift: rows_per_chunk.trailing_zeros(),
            chunk_mask: rows_per_chunk - 1,
            nrows: 0,
        }
    }

    #[inline(always)]
    fn rows_per_chunk(&self) -> usize {
        self.chunk_mask + 1
    }

    #[inline(always)]
    fn row_ptr(&self, row: usize) -> *mut u64 {
        debug_assert!(row < self.nrows);
        let c = row >> self.chunk_shift;
        let s = row & self.chunk_mask;
        // SAFETY: row < nrows ⇒ chunk exists and slot is within the chunk.
        unsafe { self.chunks.get_unchecked(c).as_ptr().add(s * self.stride_words) as *mut u64 }
    }

    /// Allocate one zeroed row; returns its index (stable forever).
    #[inline]
    fn alloc(&mut self) -> usize {
        let row = self.nrows;
        if row == self.chunks.len() * self.rows_per_chunk() {
            self.chunks
                .push(vec![0u64; self.rows_per_chunk() * self.stride_words].into_boxed_slice());
        }
        self.nrows += 1;
        row
    }

    fn mem_used(&self) -> usize {
        self.chunks.len() * self.rows_per_chunk() * self.stride_words * 8
    }

    fn clear(&mut self) {
        // Keep one chunk's allocation (rescan warmth), zero it for the
        // zero-initialized state contract.
        self.chunks.truncate(1);
        if let Some(c) = self.chunks.first_mut() {
            c.fill(0);
        }
        self.nrows = 0;
    }
}

/// One open-addressing entry array (the whole table single-level, or one of
/// the 256 two-level buckets).
struct EntrySet {
    entries: Vec<u64>,
    mask: usize,
    members: usize,
}

impl EntrySet {
    fn with_capacity_pow2(cap: usize) -> EntrySet {
        let cap = cap.next_power_of_two().max(64);
        EntrySet { entries: vec![0u64; cap], mask: cap - 1, members: 0 }
    }

    /// CH grower: fill < 0.5.
    #[inline(always)]
    fn needs_grow(&self) -> bool {
        self.members * 2 >= self.entries.len()
    }

    /// CH growth: ×4 below 2^23 buckets, ×2 after.
    fn grown_capacity(&self) -> usize {
        if self.entries.len() < (1 << 23) {
            self.entries.len() * 4
        } else {
            self.entries.len() * 2
        }
    }

    fn mem_used(&self) -> usize {
        self.entries.len() * 8
    }
}

/// The compact-row lane aggregation table. See the module doc for the
/// design; state bytes are the caller's (zero-initialized at group birth).
pub struct LaneAggTable {
    repr: KeyRepr,
    state_bytes: usize,
    key_words: usize,
    rows: RowStore,
    /// Long byte keys (> 8 B) live here contiguously; rows reference
    /// (offset, len). Never shrinks until reset.
    arena: Vec<u8>,
    /// Single-level entry set (empty and unused once `buckets` exists).
    single: EntrySet,
    /// Two-level: 256 sub-tables bucketed by the hash top byte.
    buckets: Option<Vec<EntrySet>>,
    /// The NULL group's row (out-of-band — CH ZeroValueStorage idiom).
    null_row: Option<usize>,
    total_members: usize,
}

/// Probe outcome: pointer to the row's state bytes + whether the group is
/// new (states zeroed; the caller runs its group initialization).
pub struct Probe {
    pub states: *mut u8,
    pub is_new: bool,
}

impl LaneAggTable {
    /// `state_bytes` is rounded up to 8-byte alignment (rows are u64 arrays,
    /// so states start 8-aligned — the tuplehash `maxalign(additionalsize)`
    /// contract).
    pub fn new(repr: KeyRepr, state_bytes: usize, capacity_hint: usize) -> LaneAggTable {
        let state_bytes = (state_bytes + 7) & !7;
        let key_words = match repr {
            KeyRepr::Int => INT_KEY_WORDS,
            KeyRepr::Bytes => BYTES_KEY_WORDS,
        };
        let stride = key_words + state_bytes / 8;
        LaneAggTable {
            repr,
            state_bytes,
            key_words,
            rows: RowStore::new(stride),
            arena: Vec::new(),
            single: EntrySet::with_capacity_pow2(capacity_hint.saturating_mul(2)),
            buckets: None,
            null_row: None,
            total_members: 0,
        }
    }

    #[inline]
    pub fn repr(&self) -> KeyRepr {
        self.repr
    }

    /// The (maxaligned) per-row state size the table was built with.
    #[inline]
    pub fn state_bytes(&self) -> usize {
        self.state_bytes
    }

    /// Live groups (NULL group included).
    #[inline]
    pub fn len(&self) -> usize {
        self.total_members + self.null_row.is_some() as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Conservative bytes owned by the table (entry arrays + row chunks +
    /// key arena) — the caller feeds this into its memory-limit accounting.
    pub fn mem_used(&self) -> usize {
        let entries = match &self.buckets {
            Some(bs) => bs.iter().map(EntrySet::mem_used).sum::<usize>(),
            None => self.single.mem_used(),
        };
        entries + self.rows.mem_used() + self.arena.capacity()
    }

    /// DuckDB salt-disable gate, resolved per probe call (cheap load).
    #[inline(always)]
    fn salt_enabled(&self) -> bool {
        self.total_members > SALT_DISABLE_MAX_ENTRIES
    }

    #[inline(always)]
    fn set_for(&self, hash: u64) -> &EntrySet {
        match &self.buckets {
            Some(bs) => {
                // SAFETY: bucket_of yields < 256 and bs has exactly 256 sets.
                unsafe { bs.get_unchecked(bucket_of(hash)) }
            }
            None => &self.single,
        }
    }

    #[inline(always)]
    fn set_for_mut(&mut self, hash: u64) -> &mut EntrySet {
        match &mut self.buckets {
            Some(bs) => {
                // SAFETY: bucket_of yields < 256 and bs has exactly 256 sets.
                unsafe { bs.get_unchecked_mut(bucket_of(hash)) }
            }
            None => &mut self.single,
        }
    }

    // -- Int keys ----------------------------------------------------------

    /// Probe/insert one canonical i64 key with its [`hash_int`] hash. The
    /// hit path carries NO growth checks (CH shape: grow only on emplace) —
    /// the insert leg checks/grows first and re-probes.
    #[inline]
    pub fn probe_int(&mut self, key: i64, hash: u64) -> Probe {
        debug_assert_eq!(self.repr, KeyRepr::Int);
        let salted = if self.salt_enabled() { salt_of(hash) } else { 0 };
        let set = self.set_for(hash);
        let mask = set.mask;
        let mut pos = (hash as usize) & mask;
        loop {
            // SAFETY: pos is masked into the entry array.
            let e = unsafe { *set.entries.get_unchecked(pos) };
            if e == 0 {
                return self.insert_int(key, hash, pos);
            }
            // Salt check only when enabled; entry refs are 48-bit and salts
            // occupy the top 16, so the masked compare is exact.
            if salted == 0 || (e & !REF_MASK) == salted {
                let row = ((e & REF_MASK) - 1) as usize;
                let p = self.rows.row_ptr(row);
                // SAFETY: live row; word 0 is the key word.
                if unsafe { *p } as i64 == key {
                    // SAFETY: states start at key_words within the row.
                    return Probe {
                        states: unsafe { p.add(self.key_words).cast() },
                        is_new: false,
                    };
                }
            }
            pos = (pos + 1) & mask;
        }
    }

    #[inline(never)]
    fn insert_int(&mut self, key: i64, hash: u64, mut pos: usize) -> Probe {
        if self.grow_if_needed(hash) {
            // Layout changed: recompute the insert position (the key is
            // known absent — this probe already missed).
            let set = self.set_for(hash);
            pos = (hash as usize) & set.mask;
            while unsafe { *set.entries.get_unchecked(pos) } != 0 {
                pos = (pos + 1) & set.mask;
            }
        }
        let row = self.rows.alloc();
        let p = self.rows.row_ptr(row);
        // SAFETY: fresh zeroed row of stride key_words + state words.
        unsafe { *p = key as u64 };
        // Salt is ALWAYS stored (only the probe-side CHECK is gated on
        // table size), so entries born under a small table stay findable
        // after the salt check enables.
        let set = self.set_for_mut(hash);
        set.entries[pos] = salt_of(hash) | (row as u64 + 1);
        set.members += 1;
        self.total_members += 1;
        // SAFETY: states follow the key word in the fresh zeroed row.
        Probe { states: unsafe { p.add(self.key_words).cast() }, is_new: true }
    }

    /// Batched int-key probe: hashes computed in one pass, then the probe
    /// loop under the chosen prefetch idiom. Appends one state pointer per
    /// key to `out` (order = input order) and pushes the batch ordinal of
    /// each NEW group to `new_out` (the caller initializes those states).
    pub fn probe_int_batch(
        &mut self,
        keys: &[i64],
        mode: PrefetchMode,
        hashes: &mut Vec<u64>,
        out: &mut Vec<*mut u8>,
        new_out: &mut Vec<u32>,
    ) {
        debug_assert_eq!(self.repr, KeyRepr::Int);
        hashes.clear();
        hashes.reserve(keys.len());
        for &k in keys {
            hashes.push(hash_int(k as u64));
        }
        out.reserve(keys.len());
        match mode {
            PrefetchMode::None => {
                for (i, (&k, &h)) in keys.iter().zip(hashes.iter()).enumerate() {
                    let pr = self.probe_int(k, h);
                    out.push(pr.states);
                    if pr.is_new {
                        new_out.push(i as u32);
                    }
                }
            }
            PrefetchMode::PreTouch => {
                // DuckDB: branchless pre-touch of every row's bucket entry —
                // gated to tables larger than L2 (DuckDB's thread-local
                // tables are cache-sized so it never needs this gate; ours
                // grows unbounded and a cache-resident pre-touch is pure
                // overhead — CH's own prefetch-gate reasoning).
                if self.entry_bytes() > PREFETCH_MIN_TABLE_BYTES {
                    let mut sink = 0u64;
                    for &h in hashes.iter() {
                        let set = self.set_for(h);
                        // SAFETY: masked index.
                        sink ^= unsafe { *set.entries.get_unchecked((h as usize) & set.mask) };
                    }
                    std::hint::black_box(sink);
                }
                for (i, (&k, &h)) in keys.iter().zip(hashes.iter()).enumerate() {
                    let pr = self.probe_int(k, h);
                    out.push(pr.states);
                    if pr.is_new {
                        new_out.push(i as u32);
                    }
                }
            }
            PrefetchMode::Adaptive => {
                let engage = self.entry_bytes() > PREFETCH_MIN_TABLE_BYTES;
                if !engage {
                    for (i, (&k, &h)) in keys.iter().zip(hashes.iter()).enumerate() {
                        let pr = self.probe_int(k, h);
                        out.push(pr.states);
                        if pr.is_new {
                            new_out.push(i as u32);
                        }
                    }
                    return;
                }
                // CH PrefetchingHelper: time the first iterations, solve the
                // look-ahead, clamp to [4, 32].
                let sample = keys.len().min(100);
                let t0 = std::time::Instant::now();
                for i in 0..sample {
                    let pr = self.probe_int(keys[i], hashes[i]);
                    out.push(pr.states);
                    if pr.is_new {
                        new_out.push(i as u32);
                    }
                }
                let lookahead = if sample == 0 {
                    8
                } else {
                    // CH: lookahead ≈ 4 · (100ns / t_iter), clamped.
                    let per_iter_ns = (t0.elapsed().as_nanos() as u64 / sample as u64).max(1);
                    ((400 / per_iter_ns) as usize).clamp(4, 32)
                };
                for i in sample..keys.len() {
                    let j = i + lookahead;
                    if j < keys.len() {
                        let h = hashes[j];
                        let set = self.set_for(h);
                        prefetch(unsafe {
                            set.entries.as_ptr().add((h as usize) & set.mask)
                        });
                    }
                    let pr = self.probe_int(keys[i], hashes[i]);
                    out.push(pr.states);
                    if pr.is_new {
                        new_out.push(i as u32);
                    }
                }
            }
        }
    }

    // -- Byte-string keys ---------------------------------------------------

    /// Probe/insert one byte-string key with its [`hash_bytes`] hash. Keys
    /// ≤ 8 B compare as one packed word (StringHashMap idiom); longer keys
    /// compare saved-hash-then-memcmp against the arena.
    #[inline]
    pub fn probe_bytes(&mut self, key: &[u8], hash: u64) -> Probe {
        debug_assert_eq!(self.repr, KeyRepr::Bytes);
        let salted = if self.salt_enabled() { salt_of(hash) } else { 0 };
        let klen = key.len() as u64;
        let packed = if key.len() <= 8 { pack8(key) } else { 0 };
        let set = self.set_for(hash);
        let mask = set.mask;
        let mut pos = (hash as usize) & mask;
        loop {
            // SAFETY: masked index.
            let e = unsafe { *set.entries.get_unchecked(pos) };
            if e == 0 {
                return self.insert_bytes(key, hash, pos, packed);
            }
            if salted == 0 || (e & !REF_MASK) == salted {
                let row = ((e & REF_MASK) - 1) as usize;
                let p = self.rows.row_ptr(row);
                // SAFETY: live Bytes row: [word0, len, hash][states...].
                let (w0, rlen, rhash) =
                    unsafe { (*p, *p.add(1), *p.add(2)) };
                let matched = if rlen == klen {
                    if klen <= 8 {
                        w0 == packed
                    } else {
                        rhash == hash && {
                            let off = w0 as usize;
                            &self.arena[off..off + key.len()] == key
                        }
                    }
                } else {
                    false
                };
                if matched {
                    // SAFETY: states follow the 3 key words.
                    return Probe {
                        states: unsafe { p.add(self.key_words).cast() },
                        is_new: false,
                    };
                }
            }
            pos = (pos + 1) & mask;
        }
    }

    #[inline(never)]
    fn insert_bytes(&mut self, key: &[u8], hash: u64, mut pos: usize, packed: u64) -> Probe {
        if self.grow_if_needed(hash) {
            // Layout changed: recompute (key known absent — probe missed).
            let set = self.set_for(hash);
            pos = (hash as usize) & set.mask;
            while unsafe { *set.entries.get_unchecked(pos) } != 0 {
                pos = (pos + 1) & set.mask;
            }
        }
        let w0 = if key.len() <= 8 {
            packed
        } else {
            let off = self.arena.len() as u64;
            self.arena.extend_from_slice(key);
            off
        };
        let row = self.rows.alloc();
        let p = self.rows.row_ptr(row);
        // SAFETY: fresh zeroed row with 3 key words + state words.
        unsafe {
            *p = w0;
            *p.add(1) = key.len() as u64;
            *p.add(2) = hash;
        }
        // Salt always stored; see insert_int.
        let set = self.set_for_mut(hash);
        set.entries[pos] = salt_of(hash) | (row as u64 + 1);
        set.members += 1;
        self.total_members += 1;
        // SAFETY: states follow the key words in the fresh zeroed row.
        Probe { states: unsafe { p.add(self.key_words).cast() }, is_new: true }
    }

    // -- NULL group ---------------------------------------------------------

    /// The NULL key's group (out-of-band; SQL GROUP BY treats NULLs as one
    /// group).
    #[inline]
    pub fn probe_null(&mut self) -> Probe {
        match self.null_row {
            Some(row) => Probe {
                // SAFETY: live row.
                states: unsafe { self.rows.row_ptr(row).add(self.key_words).cast() },
                is_new: false,
            },
            None => {
                let row = self.rows.alloc();
                self.null_row = Some(row);
                Probe {
                    // SAFETY: fresh zeroed row.
                    states: unsafe { self.rows.row_ptr(row).add(self.key_words).cast() },
                    is_new: true,
                }
            }
        }
    }

    // -- Growth / two-level conversion ---------------------------------------

    fn entry_bytes(&self) -> usize {
        match &self.buckets {
            Some(bs) => bs.iter().map(EntrySet::mem_used).sum(),
            None => self.single.mem_used(),
        }
    }

    /// Pre-INSERT growth gate (never on the hit path): grow the target
    /// entry set when its fill would cross 0.5 (CH's bet: low fill keeps
    /// probes ~1 step), converting to two-level at the CH threshold.
    /// Returns true when any layout changed (caller re-derives positions).
    fn grow_if_needed(&mut self, hash: u64) -> bool {
        let mut changed = false;
        if self.buckets.is_none() && self.total_members + 1 > TWO_LEVEL_THRESHOLD {
            self.convert_two_level();
            changed = true;
        }
        let set = self.set_for(hash);
        if set.needs_grow() {
            self.grow_set(hash);
            changed = true;
        }
        changed
    }

    #[cold]
    fn grow_set(&mut self, hash: u64) {
        let repr = self.repr;
        let newcap = {
            let set = self.set_for(hash);
            set.grown_capacity()
        };
        let mut newset = EntrySet::with_capacity_pow2(newcap);
        newset.members = {
            let set = self.set_for(hash);
            set.members
        };
        {
            let old_entries: Vec<u64> = {
                let set = self.set_for_mut(hash);
                std::mem::take(&mut set.entries)
            };
            for e in old_entries {
                if e == 0 {
                    continue;
                }
                let row = ((e & REF_MASK) - 1) as usize;
                let h = self.row_hash(repr, row);
                let mut pos = (h as usize) & newset.mask;
                // Fill < 0.5 by construction — a free slot exists.
                while newset.entries[pos] != 0 {
                    pos = (pos + 1) & newset.mask;
                }
                newset.entries[pos] = salt_of(h) | (row as u64 + 1);
            }
        }
        *self.set_for_mut(hash) = newset;
    }

    /// Recompute a row's hash: ints re-fmix (cheaper than storing), byte
    /// keys read their saved hash word.
    #[inline]
    fn row_hash(&self, repr: KeyRepr, row: usize) -> u64 {
        let p = self.rows.row_ptr(row);
        match repr {
            // SAFETY: live Int row, word 0 = key.
            KeyRepr::Int => hash_int(unsafe { *p }),
            // SAFETY: live Bytes row, word 2 = saved hash.
            KeyRepr::Bytes => unsafe { *p.add(2) },
        }
    }

    /// CH two-level conversion: split the single entry set into 256 buckets
    /// by hash top byte. Salt bits (32..48) are disjoint from the bucket
    /// byte, so per-bucket discrimination is preserved.
    #[cold]
    fn convert_two_level(&mut self) {
        debug_assert!(self.buckets.is_none());
        let repr = self.repr;
        let per_bucket = (self.total_members / 128).next_power_of_two().max(64);
        let mut bs: Vec<EntrySet> =
            (0..256).map(|_| EntrySet::with_capacity_pow2(per_bucket)).collect();
        let old = std::mem::replace(&mut self.single, EntrySet::with_capacity_pow2(0));
        for e in old.entries {
            if e == 0 {
                continue;
            }
            let row = ((e & REF_MASK) - 1) as usize;
            let h = self.row_hash(repr, row);
            let set = &mut bs[bucket_of(h)];
            if set.needs_grow() {
                grow_in_place(set, |r| self.row_hash(repr, r));
            }
            let mut pos = (h as usize) & set.mask;
            while set.entries[pos] != 0 {
                pos = (pos + 1) & set.mask;
            }
            set.entries[pos] = salt_of(h) | (row as u64 + 1);
            set.members += 1;
        }
        self.buckets = Some(bs);
    }

    /// Whether the table has converted to the two-level (256-bucket)
    /// structure — the Stage-4 bucket-parallel merge precondition.
    pub fn is_two_level(&self) -> bool {
        self.buckets.is_some()
    }

    // -- Read-back -----------------------------------------------------------

    /// Rows in insertion order (the NULL group occupies its allocated row
    /// position). Total = `nrows()`.
    #[inline]
    pub fn nrows(&self) -> usize {
        self.rows.nrows
    }

    /// Row `i`'s key: `None` = the NULL group.
    #[inline]
    pub fn row_key_int(&self, i: usize) -> Option<i64> {
        debug_assert_eq!(self.repr, KeyRepr::Int);
        if self.null_row == Some(i) {
            return None;
        }
        // SAFETY: i < nrows (caller iterates 0..nrows).
        Some(unsafe { *self.rows.row_ptr(i) } as i64)
    }

    /// Row `i`'s byte key: `None` = the NULL group. Short keys are returned
    /// from the caller's scratch buffer (packed-word unpack).
    #[inline]
    pub fn row_key_bytes<'a>(&'a self, i: usize, scratch: &'a mut [u8; 8]) -> Option<&'a [u8]> {
        debug_assert_eq!(self.repr, KeyRepr::Bytes);
        if self.null_row == Some(i) {
            return None;
        }
        let p = self.rows.row_ptr(i);
        // SAFETY: live Bytes row.
        let (w0, len) = unsafe { (*p, *p.add(1) as usize) };
        if len <= 8 {
            scratch.copy_from_slice(&w0.to_le_bytes());
            Some(&scratch[..len])
        } else {
            let off = w0 as usize;
            Some(&self.arena[off..off + len])
        }
    }

    /// Row `i`'s state bytes.
    #[inline]
    pub fn row_states(&self, i: usize) -> *mut u8 {
        // SAFETY: states follow the key words; caller keeps i < nrows.
        unsafe { self.rows.row_ptr(i).add(self.key_words).cast() }
    }

    /// Drop all groups, keeping (bounded) allocations for reuse (rescan).
    pub fn reset(&mut self) {
        self.rows.clear();
        self.arena.clear();
        self.single = EntrySet::with_capacity_pow2(64);
        self.buckets = None;
        self.null_row = None;
        self.total_members = 0;
    }
}

/// Grow one entry set in place during two-level conversion (borrow-friendly
/// free function: `row_hash` needs `&self`, the set is local).
fn grow_in_place(set: &mut EntrySet, row_hash: impl Fn(usize) -> u64) {
    let mut newset = EntrySet::with_capacity_pow2(set.grown_capacity());
    newset.members = set.members;
    for &e in &set.entries {
        if e == 0 {
            continue;
        }
        let row = ((e & REF_MASK) - 1) as usize;
        let h = row_hash(row);
        let mut pos = (h as usize) & newset.mask;
        while newset.entries[pos] != 0 {
            pos = (pos + 1) & newset.mask;
        }
        newset.entries[pos] = salt_of(h) | (row as u64 + 1);
    }
    *set = newset;
}

/// Best-effort read prefetch (L1). No-op on targets without a stable idiom.
#[inline(always)]
fn prefetch(p: *const u64) {
    #[cfg(target_arch = "aarch64")]
    // SAFETY: prfm is a hint; any address is allowed.
    unsafe {
        core::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, preserves_flags));
    }
    #[cfg(target_arch = "x86_64")]
    // SAFETY: prefetch is a hint; any address is allowed.
    unsafe {
        core::arch::x86_64::_mm_prefetch::<{ core::arch::x86_64::_MM_HINT_T0 }>(p as *const i8);
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    let _ = p;
}

#[cfg(test)]
mod tests;
