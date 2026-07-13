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

/// Fixed probe-prefetch look-ahead for the engaged batched drivers. The CH
/// PrefetchingHelper MEASURES a look-ahead in [4, 32] per batch; at the
/// executor's 1024-row batches that sampling left the first 100 rows of
/// EVERY batch unprefetched (~10% of all probes) and paid two clock reads
/// per batch — both visible in the in-situ Q16 profile (2026-07-15,
/// serialgap2: vdso/clock_gettime/Timespec::now lines). Engaged probes are
/// DRAM-bound (~60-120ns/iter vs ~100ns DRAM), so the solved look-ahead is
/// always a handful; a fixed mid-clamp distance covers the whole batch with
/// zero measurement overhead.
pub const PREFETCH_LOOKAHEAD: usize = 8;

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

/// Integer-key hash function, fixed per table at build (internal-only choice:
/// read-back is insertion order and migration re-hashes through the C path,
/// so nothing byte-visible depends on it — see the Stage-2.2 note).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HashKind {
    /// murmur3 fmix64 (portable baseline; 2 multiplies + 3 xor-shifts).
    Fmix,
    /// Hardware CRC32C (aarch64 `crc32cx`, the ClickHouse `intHashCRC32`
    /// idiom) spread to 64 bits by one Fibonacci multiply — the high bits
    /// feed our salt (32..48) and two-level bucket (56..64), which CH's
    /// 32-bit hash consumers don't need but ours do. Falls back to Fmix
    /// where the instruction is unavailable.
    Crc,
}

impl HashKind {
    /// The production pick: hardware CRC32C when the CPU has it (universal
    /// on Graviton and Apple Silicon; checked via HWCAP once per process).
    #[inline]
    pub fn best() -> HashKind {
        if crc_supported() {
            HashKind::Crc
        } else {
            HashKind::Fmix
        }
    }
}

/// One-time HWCAP check for the aarch64 CRC32 extension.
#[inline(always)]
fn crc_supported() -> bool {
    #[cfg(target_arch = "aarch64")]
    {
        static OK: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *OK.get_or_init(|| std::arch::is_aarch64_feature_detected!("crc"))
    }
    #[cfg(not(target_arch = "aarch64"))]
    false
}

/// `crc32cx` via inline asm (NOT `#[target_feature]` intrinsics: a
/// target-feature function cannot inline into generic callers, and this sits
/// in the probe hot loop). The `.arch_extension` directive makes the
/// integrated assembler accept the instruction on baseline armv8 targets;
/// callers gate on [`crc_supported`] (SIGILL otherwise).
#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn crc32cx(crc: u32, data: u64) -> u32 {
    let out: u32;
    // SAFETY: single register-to-register instruction, no memory access.
    unsafe {
        core::arch::asm!(
            ".arch_extension crc",
            "crc32cx {out:w}, {crc:w}, {data}",
            out = lateout(reg) out,
            crc = in(reg) crc,
            data = in(reg) data,
            options(pure, nomem, nostack, preserves_flags),
        );
    }
    out
}

/// Spread a 32-bit CRC to 64 bits: one Fibonacci multiply. Low bits keep the
/// CRC's distribution (position bits); high bits mix in every CRC bit (salt
/// + two-level bucket bits). 2^32 hash space matches CH's own CRC tables —
/// full-key compares resolve the residual collisions.
#[inline(always)]
#[cfg(target_arch = "aarch64")]
fn spread32(c: u32) -> u64 {
    (c as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Hardware-CRC32C integer hash (CH `intHashCRC32`, 64-bit-spread). Callers
/// MUST hold [`crc_supported`]; non-aarch64 builds alias [`hash_int`].
#[inline(always)]
pub fn hash_int_crc(k: u64) -> u64 {
    #[cfg(target_arch = "aarch64")]
    {
        spread32(crc32cx(u32::MAX, k))
    }
    #[cfg(not(target_arch = "aarch64"))]
    hash_int(k)
}

/// Hardware-CRC32C 128-bit-key hash: the CH `UInt128HashCRC32` idiom —
/// crc32cx chained over the two words, then the 64-bit spread (salt +
/// two-level bits). Callers MUST hold [`crc_supported`]; non-aarch64 builds
/// alias [`hash_i128`].
#[inline(always)]
pub fn hash_i128_crc(k: [u64; 2]) -> u64 {
    #[cfg(target_arch = "aarch64")]
    {
        spread32(crc32cx(crc32cx(u32::MAX, k[0]), k[1]))
    }
    #[cfg(not(target_arch = "aarch64"))]
    hash_i128(k)
}

/// fmix64-chain 128-bit-key hash (portable baseline for [`hash_i128_crc`]).
#[inline(always)]
pub fn hash_i128(k: [u64; 2]) -> u64 {
    hash_int(hash_int(k[0]) ^ k[1])
}

/// Hardware-CRC32C byte-string hash (CH hashes strings CRC-by-8B-words; same
/// shape). Callers MUST hold [`crc_supported`]; non-aarch64 aliases
/// [`hash_bytes`].
#[inline]
pub fn hash_bytes_crc(b: &[u8]) -> u64 {
    #[cfg(target_arch = "aarch64")]
    {
        let mut c = crc32cx(u32::MAX, b.len() as u64);
        if b.len() <= 8 {
            return spread32(crc32cx(c, pack8(b)));
        }
        let mut chunks = b.chunks_exact(8);
        for w in &mut chunks {
            c = crc32cx(c, u64::from_le_bytes(w.try_into().unwrap()));
        }
        let rem = chunks.remainder();
        if !rem.is_empty() {
            c = crc32cx(c, pack8(rem));
        }
        spread32(c)
    }
    #[cfg(not(target_arch = "aarch64"))]
    hash_bytes(b)
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
    /// Two 64-bit key words (a packed multi-key composite ≤ 16 B, low word
    /// first — the CH `keys128` idiom). Salt8 entries only.
    Int128,
    /// Byte-string keys: 3 key words = [packed8-or-arena-offset, len,
    /// saved 64-bit hash].
    Bytes,
}

/// Entry-array layout, fixed at build (Int keys only; Bytes always Salt8).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryLayout {
    /// DuckDB `ht_entry_t`: 8-byte entries = 16-bit salt + 48-bit row ref.
    /// Key compare chases the row pointer (entry→row two-load chain).
    Salt8,
    /// CH single-load idiom: 16-byte entries = [key word, row ref]; the
    /// probe compares the key straight out of the entry line, touching the
    /// payload row only on a hit (for the states, which the fold loads
    /// anyway). 2× entry-array memory at the same fill.
    Inline16,
}

const INT_KEY_WORDS: usize = 1;
const INT128_KEY_WORDS: usize = 2;
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
/// the 256 two-level buckets). `slot_words` is 1 (Salt8) or 2 (Inline16);
/// slot count = `mask + 1`, `entries.len() == (mask + 1) * slot_words`.
struct EntrySet {
    entries: Vec<u64>,
    mask: usize,
    members: usize,
    slot_words: usize,
}

impl EntrySet {
    fn with_capacity_pow2(cap: usize, slot_words: usize) -> EntrySet {
        let cap = cap.next_power_of_two().max(64);
        EntrySet { entries: vec![0u64; cap * slot_words], mask: cap - 1, members: 0, slot_words }
    }

    /// CH grower: fill < 0.5.
    #[inline(always)]
    fn needs_grow(&self) -> bool {
        self.members * 2 >= self.mask + 1
    }

    /// CH growth (in slots): ×4 below 2^23 buckets, ×2 after.
    fn grown_capacity(&self) -> usize {
        if self.mask + 1 < (1 << 23) {
            (self.mask + 1) * 4
        } else {
            (self.mask + 1) * 2
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
    hash: HashKind,
    /// Entry slot words: 1 = Salt8, 2 = Inline16 (Int repr only).
    slot_words: usize,
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
        // Production defaults, per the pod A/B (Stage-2.2 tableresidual
        // note): hardware CRC32C where available; Salt8 entries.
        LaneAggTable::with_config(repr, state_bytes, capacity_hint, HashKind::best(), EntryLayout::Salt8)
    }

    /// Explicit-config constructor (the bench A/B entry point; `new` picks
    /// the production winners).
    pub fn with_config(
        repr: KeyRepr,
        state_bytes: usize,
        capacity_hint: usize,
        hash: HashKind,
        layout: EntryLayout,
    ) -> LaneAggTable {
        let state_bytes = (state_bytes + 7) & !7;
        let key_words = match repr {
            KeyRepr::Int => INT_KEY_WORDS,
            KeyRepr::Int128 => INT128_KEY_WORDS,
            KeyRepr::Bytes => BYTES_KEY_WORDS,
        };
        // Crc falls back where unsupported (single dispatch point: the
        // stored kind IS the executed kind everywhere downstream).
        let hash = if hash == HashKind::Crc && !crc_supported() { HashKind::Fmix } else { hash };
        let slot_words = match layout {
            EntryLayout::Salt8 => 1,
            EntryLayout::Inline16 => {
                debug_assert_eq!(repr, KeyRepr::Int, "Inline16 is Int-only");
                2
            }
        };
        let stride = key_words + state_bytes / 8;
        // Honor the caller's group estimate THROUGH the two-level structure
        // (CH sizes from its size-hint prealloc events; the C tuplehash sizes
        // nbuckets from numGroups): a hint above the conversion threshold
        // builds the 256-bucket table at birth, per-bucket presized with 2×
        // headroom over the 0.5-fill minimum (planner ndistinct estimates
        // run low; one ×4 bucket grow bounds any residual underestimate).
        // The in-situ alternative — presize single-level, then THROW THE
        // ARRAY AWAY at the 100K-member conversion and regrow 256 buckets
        // from 1024 slots — cost Q16 ~12% in rehash walks (grow_set +
        // convert_two_level + insert_int, serialgap2 profile).
        let (single, buckets) = if capacity_hint > TWO_LEVEL_THRESHOLD {
            let per_bucket = (capacity_hint.saturating_mul(4) / 256).next_power_of_two().max(64);
            let bs =
                (0..256).map(|_| EntrySet::with_capacity_pow2(per_bucket, slot_words)).collect();
            (EntrySet::with_capacity_pow2(0, slot_words), Some(bs))
        } else {
            (EntrySet::with_capacity_pow2(capacity_hint.saturating_mul(2), slot_words), None)
        };
        LaneAggTable {
            repr,
            hash,
            slot_words,
            state_bytes,
            key_words,
            rows: RowStore::new(stride),
            arena: Vec::new(),
            single,
            buckets,
            null_row: None,
            total_members: 0,
        }
    }

    #[inline]
    pub fn repr(&self) -> KeyRepr {
        self.repr
    }

    /// This table's integer-key hash — callers of [`Self::probe_int`] MUST
    /// hash through this (kind-consistent with grow/convert re-hashing).
    #[inline(always)]
    pub fn hash_key_int(&self, k: u64) -> u64 {
        hash_int_kind(self.hash, k)
    }

    /// This table's 128-bit-key hash — callers of [`Self::probe_i128`] MUST
    /// hash through this (kind-consistent with grow/convert re-hashing).
    #[inline(always)]
    pub fn hash_key_i128(&self, k: [u64; 2]) -> u64 {
        hash_i128_kind(self.hash, k)
    }

    /// This table's byte-key hash — callers of [`Self::probe_bytes`] MUST
    /// hash through this.
    #[inline(always)]
    pub fn hash_key_bytes(&self, b: &[u8]) -> u64 {
        match self.hash {
            HashKind::Fmix => hash_bytes(b),
            HashKind::Crc => hash_bytes_crc(b),
        }
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

    /// Probe/insert one canonical i64 key with its [`Self::hash_key_int`]
    /// hash. The hit path carries NO growth checks (CH shape: grow only on
    /// emplace) — the insert leg checks/grows first and re-probes.
    #[inline]
    pub fn probe_int(&mut self, key: i64, hash: u64) -> Probe {
        debug_assert_eq!(self.repr, KeyRepr::Int);
        if self.slot_words == 2 {
            return self.probe_int_inline(key, hash);
        }
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

    /// Inline16 probe: entry slot = [key, ref]; the key compare never
    /// touches the payload row (single-load probe; CH HashMap cell idiom).
    #[inline]
    fn probe_int_inline(&mut self, key: i64, hash: u64) -> Probe {
        let set = self.set_for(hash);
        let mask = set.mask;
        let mut pos = (hash as usize) & mask;
        loop {
            // SAFETY: pos is masked; slots are 2 words.
            let sp = unsafe { set.entries.as_ptr().add(pos * 2) };
            // SAFETY: in-bounds slot words.
            let (k, r) = unsafe { (*sp, *sp.add(1)) };
            if r == 0 {
                return self.insert_int(key, hash, pos);
            }
            if k as i64 == key {
                let p = self.rows.row_ptr((r - 1) as usize);
                // SAFETY: states start at key_words within the live row.
                return Probe { states: unsafe { p.add(self.key_words).cast() }, is_new: false };
            }
            pos = (pos + 1) & mask;
        }
    }

    /// Fused probe+fold driver — the CH raw-emplace loop shape (hash inline,
    /// one pass, no places[] round trip). `fold(states, ordinal, is_new)`
    /// runs once per key IN INPUT ORDER; new groups' states are zeroed.
    ///
    /// Why this exists (pod perf, tableresidual note): the per-row
    /// [`Self::probe_int`] shape makes LLVM reload the table's hash-kind /
    /// layout / mask / base pointers from memory EVERY row — the caller's
    /// fold stores through `*mut u8` may alias `self` as far as alias
    /// analysis can prove. This driver hoists the entry-array and row-store
    /// raw parts into locals (SSA values, immune to stores through unknown
    /// pointers) and re-hoists only after an insert (which may grow or
    /// convert the table). Inserts are once-per-group, so the steady-state
    /// loop is pure register traffic + the two data-dependent loads.
    pub fn probe_fold_int(&mut self, keys: &[i64], mut fold: impl FnMut(*mut u8, u32, bool)) {
        debug_assert_eq!(self.repr, KeyRepr::Int);
        // hash_int / hash_int_crc are zero-sized fn items: each arm
        // monomorphizes the loop with the hash inlined.
        match (self.hash, self.slot_words == 2) {
            (HashKind::Fmix, false) => self.probe_fold_run::<false>(keys, hash_int, &mut fold),
            (HashKind::Fmix, true) => self.probe_fold_run::<true>(keys, hash_int, &mut fold),
            (HashKind::Crc, false) => self.probe_fold_run::<false>(keys, hash_int_crc, &mut fold),
            (HashKind::Crc, true) => self.probe_fold_run::<true>(keys, hash_int_crc, &mut fold),
        }
    }

    fn probe_fold_run<const INLINE: bool>(
        &mut self,
        keys: &[i64],
        hf: impl Fn(u64) -> u64 + Copy,
        fold: &mut impl FnMut(*mut u8, u32, bool),
    ) {
        let kw = self.key_words;
        let mut i = 0usize;
        'rehoist: while i < keys.len() {
            // Hoisted raw parts (re-derived after every insert: grow /
            // two-level conversion / row-chunk allocation all happen there).
            let bp: *mut EntrySet = match &mut self.buckets {
                Some(bs) => bs.as_mut_ptr(),
                None => core::ptr::null_mut(),
            };
            let (sp_entries, sp_mask) = {
                let s = &mut self.single;
                (s.entries.as_mut_ptr(), s.mask)
            };
            let rows_chunks = self.rows.chunks.as_ptr();
            let rows_shift = self.rows.chunk_shift;
            let rows_cmask = self.rows.chunk_mask;
            let rows_stride = self.rows.stride_words;
            let members = self.total_members;
            let salt_on = !INLINE && members > SALT_DISABLE_MAX_ENTRIES;
            while i < keys.len() {
                // SAFETY: i < keys.len().
                let key = unsafe { *keys.get_unchecked(i) };
                let hash = hf(key as u64);
                let (e_ptr, mask) = if bp.is_null() {
                    (sp_entries, sp_mask)
                } else {
                    // SAFETY: two-level tables have exactly 256 buckets.
                    let set = unsafe { &mut *bp.add(bucket_of(hash)) };
                    (set.entries.as_mut_ptr(), set.mask)
                };
                let salted = if salt_on { salt_of(hash) } else { 0 };
                let mut pos = (hash as usize) & mask;
                let hit: Option<*mut u64> = loop {
                    if INLINE {
                        // SAFETY: masked slot index, 2 words per slot.
                        let sp = unsafe { e_ptr.add(pos * 2) };
                        // SAFETY: in-bounds slot words.
                        let (k, r) = unsafe { (*sp, *sp.add(1)) };
                        if r == 0 {
                            break None;
                        }
                        if k as i64 == key {
                            let row = (r - 1) as usize;
                            // SAFETY: live row (chunked storage; parts
                            // hoisted above, stable since the last insert).
                            break Some(unsafe {
                                row_ptr_raw(rows_chunks, rows_shift, rows_cmask, rows_stride, row)
                            });
                        }
                    } else {
                        // SAFETY: masked entry index.
                        let e = unsafe { *e_ptr.add(pos) };
                        if e == 0 {
                            break None;
                        }
                        if salted == 0 || (e & !REF_MASK) == salted {
                            let row = ((e & REF_MASK) - 1) as usize;
                            // SAFETY: live row.
                            let p = unsafe {
                                row_ptr_raw(rows_chunks, rows_shift, rows_cmask, rows_stride, row)
                            };
                            // SAFETY: word 0 is the key word.
                            if unsafe { *p } as i64 == key {
                                break Some(p);
                            }
                        }
                    }
                    pos = (pos + 1) & mask;
                };
                match hit {
                    Some(p) => {
                        // SAFETY: states follow the key words.
                        fold(unsafe { p.add(kw).cast() }, i as u32, false);
                        i += 1;
                    }
                    None => {
                        // Insert leg (cold; may grow/convert/allocate) —
                        // fold, then re-hoist every raw part.
                        let pr = self.insert_int(key, hash, pos);
                        fold(pr.states, i as u32, true);
                        i += 1;
                        continue 'rehoist;
                    }
                }
            }
        }
    }

    /// Engaged-path fused driver: the [`Self::probe_fold_run`] hoisted-locals
    /// probe loop over a PRE-HASHED batch, issuing the entry-slot prefetch
    /// [`PREFETCH_LOOKAHEAD`] rows ahead. This replaces the engaged
    /// per-row-`probe_int` shape (which reloaded the table's mask/base
    /// pointers from memory every row — 41% of in-situ Q16) and the CH
    /// per-batch look-ahead measurement (see [`PREFETCH_LOOKAHEAD`]).
    fn probe_fold_hashed_run<const INLINE: bool>(
        &mut self,
        keys: &[i64],
        hashes: &[u64],
        fold: &mut impl FnMut(*mut u8, u32, bool),
    ) {
        debug_assert_eq!(keys.len(), hashes.len());
        let kw = self.key_words;
        let sw = if INLINE { 2 } else { 1 };
        let mut i = 0usize;
        'rehoist: while i < keys.len() {
            // Hoisted raw parts (re-derived after every insert — see
            // probe_fold_run's rationale).
            let bp: *mut EntrySet = match &mut self.buckets {
                Some(bs) => bs.as_mut_ptr(),
                None => core::ptr::null_mut(),
            };
            let (sp_entries, sp_mask) = {
                let s = &mut self.single;
                (s.entries.as_mut_ptr(), s.mask)
            };
            let rows_chunks = self.rows.chunks.as_ptr();
            let rows_shift = self.rows.chunk_shift;
            let rows_cmask = self.rows.chunk_mask;
            let rows_stride = self.rows.stride_words;
            let salt_on = !INLINE && self.total_members > SALT_DISABLE_MAX_ENTRIES;
            while i < keys.len() {
                let j = i + PREFETCH_LOOKAHEAD;
                if j < keys.len() {
                    // SAFETY: j < keys.len() == hashes.len().
                    let h = unsafe { *hashes.get_unchecked(j) };
                    let (e_ptr_j, mask_j) = if bp.is_null() {
                        (sp_entries, sp_mask)
                    } else {
                        // SAFETY: two-level tables have exactly 256 buckets.
                        let set = unsafe { &mut *bp.add(bucket_of(h)) };
                        (set.entries.as_mut_ptr(), set.mask)
                    };
                    // SAFETY: masked slot index × slot words (hint only).
                    prefetch(unsafe { e_ptr_j.add(((h as usize) & mask_j) * sw) });
                }
                // SAFETY: i < keys.len() == hashes.len().
                let key = unsafe { *keys.get_unchecked(i) };
                let hash = unsafe { *hashes.get_unchecked(i) };
                let (e_ptr, mask) = if bp.is_null() {
                    (sp_entries, sp_mask)
                } else {
                    // SAFETY: two-level tables have exactly 256 buckets.
                    let set = unsafe { &mut *bp.add(bucket_of(hash)) };
                    (set.entries.as_mut_ptr(), set.mask)
                };
                let salted = if salt_on { salt_of(hash) } else { 0 };
                let mut pos = (hash as usize) & mask;
                let hit: Option<*mut u64> = loop {
                    if INLINE {
                        // SAFETY: masked slot index, 2 words per slot.
                        let sp = unsafe { e_ptr.add(pos * 2) };
                        // SAFETY: in-bounds slot words.
                        let (k, r) = unsafe { (*sp, *sp.add(1)) };
                        if r == 0 {
                            break None;
                        }
                        if k as i64 == key {
                            let row = (r - 1) as usize;
                            // SAFETY: live row (parts stable since last insert).
                            break Some(unsafe {
                                row_ptr_raw(rows_chunks, rows_shift, rows_cmask, rows_stride, row)
                            });
                        }
                    } else {
                        // SAFETY: masked entry index.
                        let e = unsafe { *e_ptr.add(pos) };
                        if e == 0 {
                            break None;
                        }
                        if salted == 0 || (e & !REF_MASK) == salted {
                            let row = ((e & REF_MASK) - 1) as usize;
                            // SAFETY: live row.
                            let p = unsafe {
                                row_ptr_raw(rows_chunks, rows_shift, rows_cmask, rows_stride, row)
                            };
                            // SAFETY: word 0 is the key word.
                            if unsafe { *p } as i64 == key {
                                break Some(p);
                            }
                        }
                    }
                    pos = (pos + 1) & mask;
                };
                match hit {
                    Some(p) => {
                        // SAFETY: states follow the key words.
                        fold(unsafe { p.add(kw).cast() }, i as u32, false);
                        i += 1;
                    }
                    None => {
                        // Insert leg (cold; may grow/allocate) — fold, then
                        // re-hoist every raw part.
                        let pr = self.insert_int(key, hash, pos);
                        fold(pr.states, i as u32, true);
                        i += 1;
                        continue 'rehoist;
                    }
                }
            }
        }
    }

    #[inline(never)]
    fn insert_int(&mut self, key: i64, hash: u64, mut pos: usize) -> Probe {
        let sw = self.slot_words;
        if self.grow_if_needed(hash) {
            // Layout changed: recompute the insert position (the key is
            // known absent — this probe already missed).
            let set = self.set_for(hash);
            pos = (hash as usize) & set.mask;
            // SAFETY: masked slot; the ref word is slot word sw-1 (word 0
            // for Salt8, word 1 for Inline16).
            while unsafe { *set.entries.get_unchecked(pos * sw + (sw - 1)) } != 0 {
                pos = (pos + 1) & set.mask;
            }
        }
        let row = self.rows.alloc();
        let p = self.rows.row_ptr(row);
        // SAFETY: fresh zeroed row of stride key_words + state words.
        unsafe { *p = key as u64 };
        let set = self.set_for_mut(hash);
        if sw == 2 {
            set.entries[pos * 2] = key as u64;
            set.entries[pos * 2 + 1] = row as u64 + 1;
        } else {
            // Salt is ALWAYS stored (only the probe-side CHECK is gated on
            // table size), so entries born under a small table stay findable
            // after the salt check enables.
            set.entries[pos] = salt_of(hash) | (row as u64 + 1);
        }
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
        out.reserve(keys.len());
        // Below the prefetch engage point (or with prefetch off) the table
        // is cache-resident and per-probe LOOP OVERHEAD dominates — route
        // through the fused hoisted-locals driver (tableresidual note: the
        // per-row probe shape reloads table fields from memory every row).
        // Both prefetch idioms are L2-gated off there by construction, so
        // this changes no prefetch behavior.
        let engaged = mode != PrefetchMode::None
            && self.entry_bytes() > PREFETCH_MIN_TABLE_BYTES;
        if !engaged {
            self.probe_fold_int(keys, |states, i, is_new| {
                out.push(states);
                if is_new {
                    new_out.push(i);
                }
            });
            return;
        }
        hashes.clear();
        hashes.reserve(keys.len());
        // Hash-kind branch hoisted out of the loop (the OnceLock/enum test
        // must not sit per-key in a 2-instruction hash loop).
        match self.hash {
            HashKind::Fmix => {
                for &k in keys {
                    hashes.push(hash_int(k as u64));
                }
            }
            HashKind::Crc => {
                for &k in keys {
                    hashes.push(hash_int_crc(k as u64));
                }
            }
        }
        match mode {
            PrefetchMode::None => unreachable!("handled by the fused driver"),
            PrefetchMode::PreTouch => {
                // DuckDB: branchless pre-touch of every row's bucket entry —
                // engaged-only (a cache-resident pre-touch is pure overhead;
                // CH's own prefetch-gate reasoning).
                {
                    let sw = self.slot_words;
                    let mut sink = 0u64;
                    for &h in hashes.iter() {
                        let set = self.set_for(h);
                        // SAFETY: masked slot index × slot words.
                        sink ^= unsafe {
                            *set.entries.get_unchecked(((h as usize) & set.mask) * sw)
                        };
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
                // Fused hoisted-locals probe over the pre-hashed batch with a
                // fixed look-ahead prefetch (PREFETCH_LOOKAHEAD): full batch
                // coverage, no per-batch clock reads, no per-row table-field
                // reloads (the engaged per-row probe_int shape was 41% of
                // in-situ Q16).
                let hs: &[u64] = hashes;
                if self.slot_words == 2 {
                    self.probe_fold_hashed_run::<true>(keys, hs, &mut |states, i, is_new| {
                        out.push(states);
                        if is_new {
                            new_out.push(i);
                        }
                    });
                } else {
                    self.probe_fold_hashed_run::<false>(keys, hs, &mut |states, i, is_new| {
                        out.push(states);
                        if is_new {
                            new_out.push(i);
                        }
                    });
                }
            }
        }
    }

    // -- Int128 keys (packed multi-key composites) ---------------------------

    /// Probe/insert one 2-word packed key with its [`Self::hash_key_i128`]
    /// hash. Salt8 entries only (Inline16 slots hold one key word); same
    /// grow-on-emplace shape as [`Self::probe_int`].
    #[inline]
    pub fn probe_i128(&mut self, key: [u64; 2], hash: u64) -> Probe {
        debug_assert_eq!(self.repr, KeyRepr::Int128);
        debug_assert_eq!(self.slot_words, 1, "Int128 is Salt8-only");
        let salted = if self.salt_enabled() { salt_of(hash) } else { 0 };
        let set = self.set_for(hash);
        let mask = set.mask;
        let mut pos = (hash as usize) & mask;
        loop {
            // SAFETY: pos is masked into the entry array.
            let e = unsafe { *set.entries.get_unchecked(pos) };
            if e == 0 {
                return self.insert_i128(key, hash, pos);
            }
            if salted == 0 || (e & !REF_MASK) == salted {
                let row = ((e & REF_MASK) - 1) as usize;
                let p = self.rows.row_ptr(row);
                // SAFETY: live Int128 row; words 0..2 are the key words.
                if unsafe { *p == key[0] && *p.add(1) == key[1] } {
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

    /// Fused probe+fold driver over a packed 2-word key lane — the
    /// [`Self::probe_fold_int`] hoisted-locals shape for [`KeyRepr::Int128`]
    /// tables. `fold(states, ordinal, is_new)` runs once per key IN INPUT
    /// ORDER; new groups' states are zeroed.
    pub fn probe_fold_i128(&mut self, keys: &[[u64; 2]], mut fold: impl FnMut(*mut u8, u32, bool)) {
        debug_assert_eq!(self.repr, KeyRepr::Int128);
        match self.hash {
            HashKind::Fmix => self.probe_fold_i128_run(keys, hash_i128, &mut fold),
            HashKind::Crc => self.probe_fold_i128_run(keys, hash_i128_crc, &mut fold),
        }
    }

    fn probe_fold_i128_run(
        &mut self,
        keys: &[[u64; 2]],
        hf: impl Fn([u64; 2]) -> u64 + Copy,
        fold: &mut impl FnMut(*mut u8, u32, bool),
    ) {
        let kw = self.key_words;
        let mut i = 0usize;
        'rehoist: while i < keys.len() {
            // Hoisted raw parts (re-derived after every insert — see
            // probe_fold_run's rationale).
            let bp: *mut EntrySet = match &mut self.buckets {
                Some(bs) => bs.as_mut_ptr(),
                None => core::ptr::null_mut(),
            };
            let (sp_entries, sp_mask) = {
                let s = &mut self.single;
                (s.entries.as_mut_ptr(), s.mask)
            };
            let rows_chunks = self.rows.chunks.as_ptr();
            let rows_shift = self.rows.chunk_shift;
            let rows_cmask = self.rows.chunk_mask;
            let rows_stride = self.rows.stride_words;
            let salt_on = self.total_members > SALT_DISABLE_MAX_ENTRIES;
            while i < keys.len() {
                // SAFETY: i < keys.len().
                let key = unsafe { *keys.get_unchecked(i) };
                let hash = hf(key);
                let (e_ptr, mask) = if bp.is_null() {
                    (sp_entries, sp_mask)
                } else {
                    // SAFETY: two-level tables have exactly 256 buckets.
                    let set = unsafe { &mut *bp.add(bucket_of(hash)) };
                    (set.entries.as_mut_ptr(), set.mask)
                };
                let salted = if salt_on { salt_of(hash) } else { 0 };
                let mut pos = (hash as usize) & mask;
                let hit: Option<*mut u64> = loop {
                    // SAFETY: masked entry index.
                    let e = unsafe { *e_ptr.add(pos) };
                    if e == 0 {
                        break None;
                    }
                    if salted == 0 || (e & !REF_MASK) == salted {
                        let row = ((e & REF_MASK) - 1) as usize;
                        // SAFETY: live row.
                        let p = unsafe {
                            row_ptr_raw(rows_chunks, rows_shift, rows_cmask, rows_stride, row)
                        };
                        // SAFETY: words 0..2 are the key words.
                        if unsafe { *p == key[0] && *p.add(1) == key[1] } {
                            break Some(p);
                        }
                    }
                    pos = (pos + 1) & mask;
                };
                match hit {
                    Some(p) => {
                        // SAFETY: states follow the key words.
                        fold(unsafe { p.add(kw).cast() }, i as u32, false);
                        i += 1;
                    }
                    None => {
                        // Insert leg (cold; may grow/convert/allocate) —
                        // fold, then re-hoist every raw part.
                        let pr = self.insert_i128(key, hash, pos);
                        fold(pr.states, i as u32, true);
                        i += 1;
                        continue 'rehoist;
                    }
                }
            }
        }
    }

    /// [`Self::probe_fold_hashed_run`]'s Int128 twin (Salt8-only): fused
    /// hoisted-locals probe over a pre-hashed 2-word-key batch with the fixed
    /// look-ahead entry prefetch.
    fn probe_fold_i128_hashed_run(
        &mut self,
        keys: &[[u64; 2]],
        hashes: &[u64],
        fold: &mut impl FnMut(*mut u8, u32, bool),
    ) {
        debug_assert_eq!(keys.len(), hashes.len());
        let kw = self.key_words;
        let mut i = 0usize;
        'rehoist: while i < keys.len() {
            let bp: *mut EntrySet = match &mut self.buckets {
                Some(bs) => bs.as_mut_ptr(),
                None => core::ptr::null_mut(),
            };
            let (sp_entries, sp_mask) = {
                let s = &mut self.single;
                (s.entries.as_mut_ptr(), s.mask)
            };
            let rows_chunks = self.rows.chunks.as_ptr();
            let rows_shift = self.rows.chunk_shift;
            let rows_cmask = self.rows.chunk_mask;
            let rows_stride = self.rows.stride_words;
            let salt_on = self.total_members > SALT_DISABLE_MAX_ENTRIES;
            while i < keys.len() {
                let j = i + PREFETCH_LOOKAHEAD;
                if j < keys.len() {
                    // SAFETY: j < keys.len() == hashes.len().
                    let h = unsafe { *hashes.get_unchecked(j) };
                    let (e_ptr_j, mask_j) = if bp.is_null() {
                        (sp_entries, sp_mask)
                    } else {
                        // SAFETY: two-level tables have exactly 256 buckets.
                        let set = unsafe { &mut *bp.add(bucket_of(h)) };
                        (set.entries.as_mut_ptr(), set.mask)
                    };
                    // SAFETY: masked entry index (Salt8: 1 slot word) — hint.
                    prefetch(unsafe { e_ptr_j.add((h as usize) & mask_j) });
                }
                // SAFETY: i < keys.len() == hashes.len().
                let key = unsafe { *keys.get_unchecked(i) };
                let hash = unsafe { *hashes.get_unchecked(i) };
                let (e_ptr, mask) = if bp.is_null() {
                    (sp_entries, sp_mask)
                } else {
                    // SAFETY: two-level tables have exactly 256 buckets.
                    let set = unsafe { &mut *bp.add(bucket_of(hash)) };
                    (set.entries.as_mut_ptr(), set.mask)
                };
                let salted = if salt_on { salt_of(hash) } else { 0 };
                let mut pos = (hash as usize) & mask;
                let hit: Option<*mut u64> = loop {
                    // SAFETY: masked entry index.
                    let e = unsafe { *e_ptr.add(pos) };
                    if e == 0 {
                        break None;
                    }
                    if salted == 0 || (e & !REF_MASK) == salted {
                        let row = ((e & REF_MASK) - 1) as usize;
                        // SAFETY: live row.
                        let p = unsafe {
                            row_ptr_raw(rows_chunks, rows_shift, rows_cmask, rows_stride, row)
                        };
                        // SAFETY: words 0..2 are the key words.
                        if unsafe { *p == key[0] && *p.add(1) == key[1] } {
                            break Some(p);
                        }
                    }
                    pos = (pos + 1) & mask;
                };
                match hit {
                    Some(p) => {
                        // SAFETY: states follow the key words.
                        fold(unsafe { p.add(kw).cast() }, i as u32, false);
                        i += 1;
                    }
                    None => {
                        let pr = self.insert_i128(key, hash, pos);
                        fold(pr.states, i as u32, true);
                        i += 1;
                        continue 'rehoist;
                    }
                }
            }
        }
    }

    #[inline(never)]
    fn insert_i128(&mut self, key: [u64; 2], hash: u64, mut pos: usize) -> Probe {
        if self.grow_if_needed(hash) {
            // Layout changed: recompute (key known absent — probe missed).
            let set = self.set_for(hash);
            pos = (hash as usize) & set.mask;
            while unsafe { *set.entries.get_unchecked(pos) } != 0 {
                pos = (pos + 1) & set.mask;
            }
        }
        let row = self.rows.alloc();
        let p = self.rows.row_ptr(row);
        // SAFETY: fresh zeroed row of stride key_words + state words.
        unsafe {
            *p = key[0];
            *p.add(1) = key[1];
        }
        // Salt is ALWAYS stored (see insert_int).
        let set = self.set_for_mut(hash);
        set.entries[pos] = salt_of(hash) | (row as u64 + 1);
        set.members += 1;
        self.total_members += 1;
        // SAFETY: states follow the key words in the fresh zeroed row.
        Probe { states: unsafe { p.add(self.key_words).cast() }, is_new: true }
    }

    /// Batched Int128 probe — [`Self::probe_int_batch`]'s twin over a packed
    /// 2-word key lane: fused hoisted-locals driver below the prefetch engage
    /// point, adaptive prefetch above it.
    pub fn probe_i128_batch(
        &mut self,
        keys: &[[u64; 2]],
        mode: PrefetchMode,
        hashes: &mut Vec<u64>,
        out: &mut Vec<*mut u8>,
        new_out: &mut Vec<u32>,
    ) {
        debug_assert_eq!(self.repr, KeyRepr::Int128);
        out.reserve(keys.len());
        let engaged =
            mode != PrefetchMode::None && self.entry_bytes() > PREFETCH_MIN_TABLE_BYTES;
        if !engaged {
            self.probe_fold_i128(keys, |states, i, is_new| {
                out.push(states);
                if is_new {
                    new_out.push(i);
                }
            });
            return;
        }
        hashes.clear();
        hashes.reserve(keys.len());
        match self.hash {
            HashKind::Fmix => {
                for &k in keys {
                    hashes.push(hash_i128(k));
                }
            }
            HashKind::Crc => {
                for &k in keys {
                    hashes.push(hash_i128_crc(k));
                }
            }
        }
        match mode {
            PrefetchMode::None => unreachable!("handled by the fused driver"),
            PrefetchMode::PreTouch => {
                {
                    let mut sink = 0u64;
                    for &h in hashes.iter() {
                        let set = self.set_for(h);
                        // SAFETY: masked entry index (Salt8: 1 slot word).
                        sink ^= unsafe { *set.entries.get_unchecked((h as usize) & set.mask) };
                    }
                    std::hint::black_box(sink);
                }
                for (i, (&k, &h)) in keys.iter().zip(hashes.iter()).enumerate() {
                    let pr = self.probe_i128(k, h);
                    out.push(pr.states);
                    if pr.is_new {
                        new_out.push(i as u32);
                    }
                }
            }
            PrefetchMode::Adaptive => {
                // Fused hoisted-locals probe over the pre-hashed batch with
                // the fixed look-ahead prefetch (see probe_int_batch).
                let hs: &[u64] = hashes;
                self.probe_fold_i128_hashed_run(keys, hs, &mut |states, i, is_new| {
                    out.push(states);
                    if is_new {
                        new_out.push(i);
                    }
                });
            }
        }
    }

    // -- Byte-string keys ---------------------------------------------------

    /// Probe/insert one byte-string key with its [`Self::hash_key_bytes`]
    /// hash. Keys
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
        let sw = self.slot_words;
        let newcap = {
            let set = self.set_for(hash);
            set.grown_capacity()
        };
        let mut newset = EntrySet::with_capacity_pow2(newcap, sw);
        newset.members = {
            let set = self.set_for(hash);
            set.members
        };
        let old_entries: Vec<u64> = {
            let set = self.set_for_mut(hash);
            std::mem::take(&mut set.entries)
        };
        let (repr, hk, rows) = (self.repr, self.hash, &self.rows);
        reinsert_all(&old_entries, sw, &mut newset, |kw, row| {
            slot_hash(repr, hk, rows, sw, kw, row)
        });
        *self.set_for_mut(hash) = newset;
    }

    /// CH two-level conversion: split the single entry set into 256 buckets
    /// by hash top byte. Salt bits (32..48) are disjoint from the bucket
    /// byte, so per-bucket discrimination is preserved.
    #[cold]
    fn convert_two_level(&mut self) {
        debug_assert!(self.buckets.is_none());
        let sw = self.slot_words;
        let per_bucket = (self.total_members / 128).next_power_of_two().max(64);
        let mut bs: Vec<EntrySet> =
            (0..256).map(|_| EntrySet::with_capacity_pow2(per_bucket, sw)).collect();
        let old = std::mem::replace(&mut self.single, EntrySet::with_capacity_pow2(0, sw));
        let (repr, hk, rows) = (self.repr, self.hash, &self.rows);
        let hash_of = |kw: u64, row: usize| slot_hash(repr, hk, rows, sw, kw, row);
        let n_slots = old.entries.len() / sw;
        for s in 0..n_slots {
            let (kw, rw) = slot_words_at(&old.entries, sw, s);
            if rw == 0 {
                continue;
            }
            let row = ((rw & REF_MASK) - 1) as usize;
            let h = hash_of(kw, row);
            let set = &mut bs[bucket_of(h)];
            if set.needs_grow() {
                grow_in_place(set, &hash_of);
            }
            insert_slot(set, h, kw, rw);
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

    /// Row `i`'s packed 2-word key: `None` = the NULL group (unused by the
    /// multi-key hosting, which encodes NULLs in the packed key — kept for
    /// layout symmetry with [`Self::row_key_int`]).
    #[inline]
    pub fn row_key_i128(&self, i: usize) -> Option<[u64; 2]> {
        debug_assert_eq!(self.repr, KeyRepr::Int128);
        if self.null_row == Some(i) {
            return None;
        }
        let p = self.rows.row_ptr(i);
        // SAFETY: i < nrows (caller iterates 0..nrows); words 0..2 are keys.
        Some(unsafe { [*p, *p.add(1)] })
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
        self.single = EntrySet::with_capacity_pow2(64, self.slot_words);
        self.buckets = None;
        self.null_row = None;
        self.total_members = 0;
    }
}

/// Raw-parts row pointer (probe_fold_run's hoisted twin of
/// [`RowStore::row_ptr`]).
///
/// SAFETY: caller holds `row < nrows` for the store whose parts these are,
/// and no chunk allocation happened since the parts were read.
#[inline(always)]
unsafe fn row_ptr_raw(
    chunks: *const Box<[u64]>,
    shift: u32,
    cmask: usize,
    stride: usize,
    row: usize,
) -> *mut u64 {
    let c = row >> shift;
    let s = row & cmask;
    // SAFETY: per the function contract.
    unsafe { (*chunks.add(c)).as_ptr().add(s * stride) as *mut u64 }
}

/// Kind-dispatched integer hash (the single dispatch point for probes and
/// grow-side re-hashing).
#[inline(always)]
fn hash_int_kind(hk: HashKind, k: u64) -> u64 {
    match hk {
        HashKind::Fmix => hash_int(k),
        HashKind::Crc => hash_int_crc(k),
    }
}

/// Kind-dispatched 128-bit-key hash ([`hash_int_kind`]'s Int128 twin).
#[inline(always)]
fn hash_i128_kind(hk: HashKind, k: [u64; 2]) -> u64 {
    match hk {
        HashKind::Fmix => hash_i128(k),
        HashKind::Crc => hash_i128_crc(k),
    }
}

/// Slot re-hash for grow/convert: Inline16 hashes the key straight out of
/// the entry (never touches rows); Salt8 re-derives from the row (ints
/// re-hash — cheaper than storing; byte keys read their saved hash word,
/// kind-agnostic). Kind-consistent with the probe side by construction.
#[inline]
fn slot_hash(repr: KeyRepr, hk: HashKind, rows: &RowStore, sw: usize, kw: u64, row: usize) -> u64 {
    if sw == 2 {
        return hash_int_kind(hk, kw);
    }
    let p = rows.row_ptr(row);
    match repr {
        // SAFETY: live Int row, word 0 = key.
        KeyRepr::Int => hash_int_kind(hk, unsafe { *p }),
        // SAFETY: live Int128 row, words 0..2 = key (re-hash: cheaper than a
        // saved-hash word, exactly like the Int arm).
        KeyRepr::Int128 => hash_i128_kind(hk, unsafe { [*p, *p.add(1)] }),
        // SAFETY: live Bytes row, word 2 = saved hash.
        KeyRepr::Bytes => unsafe { *p.add(2) },
    }
}

/// Read slot `s`'s (key word, ref word): Salt8 slots have no key word
/// (returned 0, unused); Inline16 slots are [key, ref].
#[inline(always)]
fn slot_words_at(entries: &[u64], sw: usize, s: usize) -> (u64, u64) {
    if sw == 2 {
        (entries[2 * s], entries[2 * s + 1])
    } else {
        (0, entries[s])
    }
}

/// Insert one live slot into `set` at hash `h` (grow/convert reinsertion —
/// fill < 0.5 by construction, a free slot exists). Salt8 recomputes the
/// salt from `h` (identical bits: same hash); Inline16 copies the pair.
#[inline]
fn insert_slot(set: &mut EntrySet, h: u64, kw: u64, rw: u64) {
    let sw = set.slot_words;
    let mut pos = (h as usize) & set.mask;
    while set.entries[pos * sw + (sw - 1)] != 0 {
        pos = (pos + 1) & set.mask;
    }
    if sw == 2 {
        set.entries[pos * 2] = kw;
        set.entries[pos * 2 + 1] = rw;
    } else {
        set.entries[pos] = salt_of(h) | (rw & REF_MASK);
    }
}

/// Rehash every live slot of `old_entries` into `newset` (same layout).
fn reinsert_all(
    old_entries: &[u64],
    sw: usize,
    newset: &mut EntrySet,
    hash_of: impl Fn(u64, usize) -> u64,
) {
    let n_slots = old_entries.len() / sw;
    for s in 0..n_slots {
        let (kw, rw) = slot_words_at(old_entries, sw, s);
        if rw == 0 {
            continue;
        }
        let row = ((rw & REF_MASK) - 1) as usize;
        insert_slot(newset, hash_of(kw, row), kw, rw);
    }
}

/// Grow one entry set in place during two-level conversion (borrow-friendly
/// free function: the hasher needs `&self`, the set is local).
fn grow_in_place(set: &mut EntrySet, hash_of: impl Fn(u64, usize) -> u64) {
    let mut newset = EntrySet::with_capacity_pow2(set.grown_capacity(), set.slot_words);
    newset.members = set.members;
    let old_entries = std::mem::take(&mut set.entries);
    reinsert_all(&old_entries, newset.slot_words, &mut newset, hash_of);
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
