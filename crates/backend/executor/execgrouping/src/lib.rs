// execGrouping.c: entries-in-a-PgVec + a hashbrown table of u32 indexes
// (C: simplehash; the index side is rule-2 arena+handle sharing); hash and
// match run through execexpr programs resolved once at build. Cross-type
// FindTupleHashEntry and the parallel variable-IV arm are loud.
#![allow(non_snake_case)]

use core::ptr::NonNull;
use std::rc::Rc;

use ::datum::Datum;
use ::execexpr::{
    exec_build_grouping_equal, exec_build_hash32_from_attrs, exec_eval_expr, exec_qual,
    EvalSlots, ExprState,
};
use ::mcx::{vec_with_capacity_in, Mcx, PgBox, PgVec};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::MinimalTupleData;
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

#[cfg(test)]
mod tests;

/// C `execTuplesHashPrepare` (fmgr carriers live in the built exprs).
pub fn exec_tuples_hash_prepare<'mcx>(
    mcx: Mcx<'mcx>,
    eq_operators: &[Oid],
) -> PgResult<(PgVec<'mcx, Oid>, PgVec<'mcx, Oid>)> {
    let mut eqfuncoids = vec_with_capacity_in(mcx, eq_operators.len())?;
    let mut hashfunctions = vec_with_capacity_in(mcx, eq_operators.len())?;
    for &eq_opr in eq_operators {
        let eq_function = lsyscache::get_opcode(eq_opr)?;
        let Some((left, right)) = lsyscache::get_op_hash_functions(eq_opr)? else {
            return Err(no_hash_function(eq_opr));
        };
        debug_assert_eq!(left, right);
        eqfuncoids.push(eq_function);
        hashfunctions.push(right);
    }
    Ok((eqfuncoids, hashfunctions))
}

#[cold]
#[inline(never)]
fn no_hash_function(eq_opr: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "could not find hash function for hash operator {eq_opr}"
    )))
}

// key/key_isnull: first key datum cached at insert (datum1 idea); valid only
// under a probe kernel, whose match skips the stored-tuple deform. Byval
// kernels cache the value; the Text kernel caches a pointer INTO the stored
// `first_tuple` image (stable until reset; relocations must go through
// `TupleHashTable::relocate_entry`, which rebases it).
#[derive(Clone, Copy)]
pub struct TupleHashEntryData {
    first_tuple: NonNull<MinimalTupleData>,
    hash: u32,
    key_isnull: bool,
    key: Datum,
}

impl TupleHashEntryData {
    #[inline]
    pub fn hash(&self) -> u32 {
        self.hash
    }

    #[inline]
    pub fn tuple(&self) -> NonNull<MinimalTupleData> {
        self.first_tuple
    }

    /// Repoint at a relocated image (table-handoff copy; the new image must
    /// carry the same additionalsize prefix). Callers relocating entries of a
    /// table whose kernel may cache a by-ref key must go through
    /// [`TupleHashTable::relocate_entry`], which rebases the cache.
    #[inline]
    pub fn set_tuple(&mut self, tuple: NonNull<MinimalTupleData>) {
        self.first_tuple = tuple;
    }

    /// The entry's pergroup prefix (entry_additional's per-entry form).
    #[inline]
    pub fn additional(&self, additionalsize: usize) -> Option<NonNull<u8>> {
        if additionalsize == 0 {
            return None;
        }
        let t = self.first_tuple.as_ptr().cast::<u8>();
        // SAFETY: the tuple sits additionalsize bytes into its allocation
        // (exec_copy_slot_minimal_tuple contract).
        unsafe { Some(NonNull::new_unchecked(t.sub(additionalsize))) }
    }
}

const _: () = assert!(core::mem::size_of::<TupleHashEntryData>() == 24);

// Monomorphized single-byval-key probe kernel selected at build from the
// hash/eq fn oids (execexpr CmpOp precedent): C-exact hash + NOT DISTINCT
// inline, no compiled-program walk (C only has the interpreted path non-JIT).
#[derive(Clone, Copy, PartialEq, Eq)]
enum ProbeKernel {
    Expr,
    Int2 { att: u16 },
    Int4 { att: u16 },
    Int8 { att: u16 },
    /// Single text/varchar key under a DETERMINISTIC collation (resolved once
    /// at build — `varlena::text_collation_is_raw_bytes`): C-exact
    /// hashtext = hash_any(raw bytes) and texteq = length + memcmp, inline
    /// (detoast included), no compiled-program walk and no per-row collation
    /// probing. Entries cache the key datum INSIDE the stored `first_tuple`
    /// image (deformed once per new group), so matches skip the per-probe
    /// stored-tuple store + deform entirely. Nondeterministic or invalid
    /// collations keep the Expr path (bit-identical semantics + C's errors).
    Text { att: u16 },
}

impl ProbeKernel {
    fn select(
        key_col_idx: &[i16],
        eqfuncoids: &[Oid],
        hashfunctions: &[Oid],
        collations: &[Oid],
    ) -> ProbeKernel {
        if let ([col], [eq], [hash], [collid]) =
            (key_col_idx, eqfuncoids, hashfunctions, collations)
        {
            let att = (col - 1) as u16;
            match (*hash, *eq) {
                // hashint2 / int2eq
                (449, 63) => return ProbeKernel::Int2 { att },
                (450, 65) => return ProbeKernel::Int4 { att },
                (949, 467) => return ProbeKernel::Int8 { att },
                // hashtext / texteq (text and varchar keys), raw-bytes
                // collations only. A determinism-probe error falls back to
                // Expr, whose per-row program raises C's error at C's row.
                (400, 67) => {
                    if ::varlena::text_collation_is_raw_bytes(*collid).unwrap_or(false) {
                        return ProbeKernel::Text { att };
                    }
                }
                _ => {}
            }
        }
        ProbeKernel::Expr
    }
}


// simplehash.h-parity open-addressing index over `entries`: robin-hood
// insertion, doubling growth, backward iteration from the first free bucket.
// Byte-identical hashed-output ROW ORDER depends on reproducing C's bucket
// layout exactly (SH_INSERT_HASH_INTERNAL / SH_GROW / SH_ITERATE).
const SH_EMPTY: u32 = u32::MAX;
const SH_FILLFACTOR: f64 = 0.9;
const SH_MAX_FILLFACTOR: f64 = 0.98;
const SH_GROW_MAX_DIB: u32 = 25;
const SH_GROW_MAX_MOVE: u32 = 150;
const SH_GROW_MIN_FILLFACTOR: f64 = 0.1;
const SH_MAX_SIZE: u64 = (u32::MAX as u64) + 1;

struct SimpleHashIndex {
    // bucket -> entry index, SH_EMPTY when free.
    buckets: Vec<u32>,
    sizemask: u32,
    members: u64,
    grow_threshold: u64,
}

impl SimpleHashIndex {
    fn compute_size(newsize: u64) -> u64 {
        newsize.max(2).next_power_of_two()
    }

    fn with_nelements(nelements: usize) -> Self {
        let size = Self::compute_size(
            (SH_MAX_SIZE as f64).min(nelements as f64 / SH_FILLFACTOR) as u64,
        );
        let mut t = SimpleHashIndex {
            buckets: vec![SH_EMPTY; size as usize],
            sizemask: (size - 1) as u32,
            members: 0,
            grow_threshold: 0,
        };
        t.update_grow_threshold();
        t
    }

    fn size(&self) -> u64 {
        self.buckets.len() as u64
    }

    fn update_grow_threshold(&mut self) {
        let size = self.size();
        self.grow_threshold = if size == SH_MAX_SIZE {
            (size as f64 * SH_MAX_FILLFACTOR) as u64
        } else {
            (size as f64 * SH_FILLFACTOR) as u64
        };
    }

    #[inline]
    fn initial_bucket(&self, hash: u32) -> u32 {
        hash & self.sizemask
    }

    #[inline]
    fn distance(&self, optimal: u32, bucket: u32) -> u32 {
        if optimal <= bucket {
            bucket - optimal
        } else {
            (self.size() as u32).wrapping_add(bucket) - optimal
        }
    }

    fn grow(&mut self, newsize: u64, entry_hash: impl Fn(u32) -> u32) {
        let oldsize = self.size() as u32;
        let old = core::mem::replace(
            &mut self.buckets,
            vec![SH_EMPTY; Self::compute_size(newsize) as usize],
        );
        self.sizemask = (self.buckets.len() - 1) as u32;
        self.update_grow_threshold();
        // Start copying at the first bucket that is free or optimally placed
        // in the OLD table, so runs move over without conflicts (SH_GROW).
        let mut startelem = 0u32;
        for i in 0..oldsize {
            if old[i as usize] == SH_EMPTY {
                startelem = i;
                break;
            }
            let optimal = entry_hash(old[i as usize]) & ((oldsize - 1) as u32);
            if optimal == i {
                startelem = i;
                break;
            }
        }
        let mut copyelem = startelem;
        for _ in 0..oldsize {
            let ix = old[copyelem as usize];
            if ix != SH_EMPTY {
                let mut cur = self.initial_bucket(entry_hash(ix));
                while self.buckets[cur as usize] != SH_EMPTY {
                    cur = (cur + 1) & self.sizemask;
                }
                self.buckets[cur as usize] = ix;
            }
            copyelem += 1;
            if copyelem >= oldsize {
                copyelem = 0;
            }
        }
    }

    /// SH_INSERT_HASH_INTERNAL: probe for a match via `matches`; on miss,
    /// robin-hood place `new_ix`. Returns (entry index, found-existing).
    fn insert_or_find(
        &mut self,
        hash: u32,
        new_ix: u32,
        entry_hash: impl Fn(u32) -> u32 + Copy,
        mut matches: impl FnMut(u32) -> PgResult<bool>,
    ) -> PgResult<(u32, bool)> {
        'restart: loop {
            let mut insertdist = 0u32;
            if self.members >= self.grow_threshold {
                assert!(self.size() != SH_MAX_SIZE, "hash table size exceeded");
                self.grow(self.size() * 2, entry_hash);
            }
            let startelem = self.initial_bucket(hash);
            let mut curelem = startelem;
            loop {
                let occupant = self.buckets[curelem as usize];
                if occupant == SH_EMPTY {
                    self.members += 1;
                    self.buckets[curelem as usize] = new_ix;
                    return Ok((new_ix, false));
                }
                if entry_hash(occupant) == hash && matches(occupant)? {
                    return Ok((occupant, true));
                }
                let curoptimal = self.initial_bucket(entry_hash(occupant));
                let curdist = self.distance(curoptimal, curelem);
                if insertdist > curdist {
                    // Shift the colliding run forward one bucket and take
                    // the vacated spot.
                    let mut emptyelem = curelem;
                    let mut emptydist = 0u32;
                    loop {
                        emptyelem = (emptyelem + 1) & self.sizemask;
                        if self.buckets[emptyelem as usize] == SH_EMPTY {
                            break;
                        }
                        emptydist += 1;
                        if emptydist > SH_GROW_MAX_MOVE
                            && (self.members as f64 / self.size() as f64)
                                >= SH_GROW_MIN_FILLFACTOR
                        {
                            self.grow_threshold = 0;
                            continue 'restart;
                        }
                    }
                    let mut moveelem = emptyelem;
                    while moveelem != curelem {
                        let prev = moveelem.wrapping_sub(1) & self.sizemask;
                        self.buckets[moveelem as usize] = self.buckets[prev as usize];
                        moveelem = prev;
                    }
                    self.members += 1;
                    self.buckets[curelem as usize] = new_ix;
                    return Ok((new_ix, false));
                }
                curelem = (curelem + 1) & self.sizemask;
                insertdist += 1;
                if insertdist > SH_GROW_MAX_DIB
                    && (self.members as f64 / self.size() as f64) >= SH_GROW_MIN_FILLFACTOR
                {
                    self.grow_threshold = 0;
                    continue 'restart;
                }
            }
        }
    }

    /// Find-only probe (SH_LOOKUP shape).
    fn find(
        &self,
        hash: u32,
        entry_hash: impl Fn(u32) -> u32,
        mut matches: impl FnMut(u32) -> PgResult<bool>,
    ) -> PgResult<Option<u32>> {
        let startelem = self.initial_bucket(hash);
        let mut curelem = startelem;
        loop {
            let occupant = self.buckets[curelem as usize];
            if occupant == SH_EMPTY {
                return Ok(None);
            }
            if entry_hash(occupant) == hash && matches(occupant)? {
                return Ok(Some(occupant));
            }
            curelem = (curelem + 1) & self.sizemask;
            if curelem == startelem {
                return Ok(None);
            }
        }
    }

    /// SH_START_ITERATE + SH_ITERATE: backward walk from the first free
    /// bucket. `cursor` starts at 0; it packs the start bucket (high 32
    /// bits) and visited count + 1 (low 32 bits).
    fn iterate(&self, cursor: &mut usize) -> Option<u32> {
        let size = self.size() as usize;
        let (start, mut visited) = if *cursor == 0 {
            let start =
                (0..size).find(|&i| self.buckets[i] == SH_EMPTY).expect("free bucket exists")
                    as u32;
            (start, 0usize)
        } else {
            ((*cursor >> 32) as u32, (*cursor & 0xffff_ffff) - 1)
        };
        let mut result = None;
        while visited < size {
            let bucket = start.wrapping_sub(1).wrapping_sub(visited as u32) & self.sizemask;
            visited += 1;
            let ix = self.buckets[bucket as usize];
            if ix != SH_EMPTY {
                result = Some(ix);
                break;
            }
        }
        *cursor = ((start as usize) << 32) | (visited + 1);
        result
    }

    fn clear(&mut self) {
        self.buckets.fill(SH_EMPTY);
        self.members = 0;
    }
}

pub struct TupleHashTable<'mcx> {
    entries: PgVec<'mcx, TupleHashEntryData>,
    hashtab: SimpleHashIndex,
    additionalsize: usize,
    kernel: ProbeKernel,
    tab_hash_expr: PgBox<'mcx, ExprState<'mcx>>,
    tab_eq_func: PgBox<'mcx, ExprState<'mcx>>,
    tableslot: SlotData<'mcx>,
}

/// C `BuildTupleHashTable`; entry tuples go to the per-lookup `table_mcx`
/// the caller resets wholesale, paired with [`TupleHashTable::reset`].
#[allow(clippy::too_many_arguments)]
pub fn build_tuple_hash_table<'mcx>(
    metacxt: Mcx<'mcx>,
    input_desc: &Rc<TupleDescData<'mcx>>,
    key_col_idx: &[i16],
    eqfuncoids: &[Oid],
    hashfunctions: &[Oid],
    collations: &[Oid],
    mut nbuckets: usize,
    additionalsize: usize,
    use_variable_hash_iv: bool,
) -> PgResult<TupleHashTable<'mcx>> {
    if use_variable_hash_iv {
        panic!("BuildTupleHashTable (execGrouping.c): parallel hash-IV arm not ported");
    }
    debug_assert!(nbuckets > 0);
    let additionalsize = maxalign(additionalsize);
    let entrysize = core::mem::size_of::<TupleHashEntryData>() + additionalsize;
    let hash_mem_limit = get_hash_memory_limit() / entrysize;
    if nbuckets > hash_mem_limit {
        nbuckets = hash_mem_limit.max(1);
    }

    let mut tab_hash_expr = exec_build_hash32_from_attrs(
        metacxt,
        input_desc,
        hashfunctions,
        collations,
        key_col_idx,
        0,
    )?;
    let mut tab_eq_func = exec_build_grouping_equal(
        metacxt,
        input_desc,
        input_desc,
        key_col_idx,
        eqfuncoids,
        collations,
    )?;
    // DIVERGENCE: C runs hash/eq fns in the caller-reset tempcxt; here their
    // allocations (record keys deform) live in metacxt to teardown.
    tab_hash_expr.arm_result_mcx(metacxt);
    tab_eq_func.arm_result_mcx(metacxt);
    let tableslot = exectuples::make_tuple_table_slot(
        metacxt,
        TupleSlotKind::MinimalTuple,
        Some(input_desc.clone()),
    );

    Ok(TupleHashTable {
        entries: vec_with_capacity_in(metacxt, nbuckets)?,
        hashtab: SimpleHashIndex::with_nelements(nbuckets),
        additionalsize,
        kernel: ProbeKernel::select(key_col_idx, eqfuncoids, hashfunctions, collations),
        tab_hash_expr,
        tab_eq_func,
        tableslot,
    })
}

#[inline]
const fn maxalign(n: usize) -> usize {
    (n + 7) & !7
}

/// C `get_hash_memory_limit` (nodeHash.c; no hash-AM executor crate yet).
pub fn get_hash_memory_limit() -> usize {
    let work_mem = guc_tables::vars::work_mem.read() as f64;
    let mult = guc_tables::vars::hash_mem_multiplier.read();
    let bytes = work_mem * mult * 1024.0;
    if bytes < usize::MAX as f64 {
        bytes as usize
    } else {
        usize::MAX
    }
}

impl<'mcx> TupleHashTable<'mcx> {
    /// Global-heap + fn_extra release; the table is then safe to forget.
    pub fn release(&mut self) {
        self.hashtab = SimpleHashIndex::with_nelements(0);
        self.hashtab.buckets = Vec::new();
        self.tab_hash_expr.release_frames();
        self.tab_eq_func.release_frames();
        self.tableslot.base_mut().tts_tupleDescriptor = None;
    }

    // C MemoryContextMemAllocated(hash_metacxt).
    pub fn meta_mem(&self) -> usize {
        self.entries.capacity() * core::mem::size_of::<TupleHashEntryData>()
            + self.hashtab.buckets.capacity() * core::mem::size_of::<u32>()
    }

    /// C `TupleHashTableHash`; the caller resets its per-tuple context.
    pub fn hash_slot(&mut self, input_slot: &mut SlotData<'mcx>) -> PgResult<u32> {
        // NULL hashes as 0, as EEOP_HASHDATUM_FIRST does.
        match self.kernel {
            ProbeKernel::Int2 { att } => {
                let (key, isnull) = kernel_key(input_slot, att);
                let h = if isnull {
                    0
                } else {
                    // C hashint2: hash_uint32((int32) int16-value).
                    ::hashfn::hash_bytes_uint32(key.as_i16() as i32 as u32)
                };
                Ok(::hashfn::murmurhash32(h))
            }
            ProbeKernel::Int4 { att } => {
                let (key, isnull) = kernel_key(input_slot, att);
                let h = if isnull { 0 } else { ::hashfn::hash_bytes_uint32(key.as_u32()) };
                Ok(::hashfn::murmurhash32(h))
            }
            ProbeKernel::Int8 { att } => {
                let (key, isnull) = kernel_key(input_slot, att);
                let h = if isnull { 0 } else { ::hashfn::hash_bytes_uint32(hashint8_fold(key)) };
                Ok(::hashfn::murmurhash32(h))
            }
            ProbeKernel::Text { att } => {
                let (key, isnull) = kernel_key(input_slot, att);
                let h = if isnull {
                    0
                } else {
                    text_kernel_hash(key, *self.entries.allocator())?
                };
                Ok(::hashfn::murmurhash32(h))
            }
            ProbeKernel::Expr => {
                let mut slots = EvalSlots { scan: None, inner: Some(input_slot), outer: None };
                let r = exec_eval_expr(&mut self.tab_hash_expr, &mut slots)?;
                debug_assert!(!r.isnull);
                Ok(::hashfn::murmurhash32(r.value.as_u32()))
            }
        }
    }

    /// C `LookupTupleHashEntryHash`; None `table_mcx` = C's find-only mode.
    pub fn lookup(
        &mut self,
        input_slot: &mut SlotData<'mcx>,
        hash: u32,
        table_mcx: Option<Mcx<'_>>,
        slot_mcx: Mcx<'mcx>,
    ) -> PgResult<(Option<u32>, bool)> {
        let TupleHashTable { entries, hashtab, tab_eq_func, tableslot, kernel, .. } = self;
        let mut eq_err: Option<Box<PgError>> = None;
        let input_slot = input_slot;
        // Kernel match = NOT DISTINCT over the entry's cached key datum.
        let entry_hash = |ix: u32| entries[ix as usize].hash;
        let found = match *kernel {
            ProbeKernel::Int2 { att } => {
                let (key, isnull) = kernel_key(input_slot, att);
                hashtab.find(hash, entry_hash, |ix| {
                    let e = &entries[ix as usize];
                    Ok(match (isnull, e.key_isnull) {
                        (false, false) => e.key.as_i16() == key.as_i16(),
                        (a, b) => a & b,
                    })
                })?
            }
            ProbeKernel::Int4 { att } => {
                let (key, isnull) = kernel_key(input_slot, att);
                hashtab.find(hash, entry_hash, |ix| {
                    let e = &entries[ix as usize];
                    Ok(match (isnull, e.key_isnull) {
                        (false, false) => e.key.as_i32() == key.as_i32(),
                        (a, b) => a & b,
                    })
                })?
            }
            ProbeKernel::Int8 { att } => {
                let (key, isnull) = kernel_key(input_slot, att);
                hashtab.find(hash, entry_hash, |ix| {
                    let e = &entries[ix as usize];
                    Ok(match (isnull, e.key_isnull) {
                        (false, false) => e.key.as_i64() == key.as_i64(),
                        (a, b) => a & b,
                    })
                })?
            }
            ProbeKernel::Text { att } => {
                let (key, isnull) = kernel_key(input_slot, att);
                let det_mcx = *entries.allocator();
                // Detoast the input side once per probe, not per candidate.
                // SAFETY: non-null live text varlena (key column type is
                // text/varchar by kernel selection).
                let a = if isnull {
                    None
                } else {
                    Some(unsafe { ::types_fmgr::datum_varlena_packed(key, det_mcx) }?)
                };
                hashtab.find(hash, entry_hash, |ix| {
                    let e = &entries[ix as usize];
                    // Cached-key match: NOT DISTINCT over the datum cached
                    // inside the stored image (raw-bytes collation → texteq
                    // is length + memcmp).
                    match (&a, e.key_isnull) {
                        (Some(a), false) => {
                            // SAFETY: e.key points into the live stored image
                            // (insert caches it; relocate_entry rebases it).
                            let b =
                                unsafe { ::types_fmgr::datum_varlena_packed(e.key, det_mcx) }?;
                            Ok(a.data() == b.data())
                        }
                        (None, true) => Ok(true),
                        _ => Ok(false),
                    }
                })?
            }
            ProbeKernel::Expr => hashtab.find(hash, entry_hash, |ix| {
                let e = &entries[ix as usize];
                // SAFETY: entry images live in table_mcx until reset().
                unsafe {
                    exectuples::exec_store_minimal_tuple_ptr(tableslot, slot_mcx, e.first_tuple)
                };
                let mut slots = EvalSlots {
                    scan: None,
                    inner: Some(&mut *input_slot),
                    outer: Some(&mut *tableslot),
                };
                match exec_qual(Some(tab_eq_func), &mut slots) {
                    Ok(m) => Ok(m),
                    Err(e) => {
                        eq_err = Some(e);
                        Ok(false)
                    }
                }
            })?,
        };
        if let Some(e) = eq_err {
            return Err(e);
        }
        if let Some(ix) = found {
            return Ok((Some(ix), false));
        }
        let Some(table_mcx) = table_mcx else {
            return Ok((None, false));
        };

        // Bulk-freed at reset: forget, never drop (docs/no-drop.md).
        let tup = exectuples::exec_copy_slot_minimal_tuple(
            input_slot,
            slot_mcx,
            table_mcx,
            self.additionalsize,
        )?;
        let first_tuple = NonNull::new(tup.as_ptr().cast_mut().cast::<MinimalTupleData>())
            .expect("minimal tuple image is non-null");
        core::mem::forget(tup);

        let (key, key_isnull) = match self.kernel {
            ProbeKernel::Int2 { att } | ProbeKernel::Int4 { att } | ProbeKernel::Int8 { att } => {
                kernel_key(input_slot, att)
            }
            // Text caches the key datum INSIDE the stored image (a pointer
            // into the input slot would dangle once the slot advances):
            // deform the just-created copy once per NEW GROUP; stable in
            // table_mcx until reset, rebased by relocate_entry on handoff.
            ProbeKernel::Text { att } => {
                // SAFETY: first_tuple is the live image created just above.
                unsafe {
                    exectuples::exec_store_minimal_tuple_ptr(
                        &mut self.tableslot,
                        slot_mcx,
                        first_tuple,
                    )
                };
                kernel_key(&mut self.tableslot, att)
            }
            ProbeKernel::Expr => (Datum::null(), true),
        };
        let ix = self.entries.len() as u32;
        if self.entries.len() == self.entries.capacity() {
            let add = self.entries.capacity().max(16);
            self.entries
                .try_reserve(add)
                .map_err(|_| oom_entries(*self.entries.allocator(), add))?;
        }
        self.entries.push(TupleHashEntryData { first_tuple, hash, key_isnull, key });
        let entries = &self.entries;
        // The match closure never fires: the find above proved absence.
        let (got, found) =
            self.hashtab
                .insert_or_find(hash, ix, |i| entries[i as usize].hash, |_| Ok(false))?;
        debug_assert!(!found && got == ix);
        Ok((Some(ix), true))
    }

    #[inline]
    pub fn entries(&self) -> &[TupleHashEntryData] {
        &self.entries
    }

    #[inline]
    pub fn additionalsize(&self) -> usize {
        self.additionalsize
    }

    /// Stored-tuple equality against the slot's tuple, via this table's key
    /// kernel / grouping-equal program (the `lookup` match arm, entry-free).
    pub fn match_tuple(
        &mut self,
        input_slot: &mut SlotData<'mcx>,
        input_key: (Datum, bool),
        entry: &TupleHashEntryData,
        slot_mcx: Mcx<'mcx>,
    ) -> PgResult<bool> {
        match self.kernel {
            ProbeKernel::Int2 { .. } => Ok(match (input_key.1, entry.key_isnull) {
                (false, false) => input_key.0.as_i16() == entry.key.as_i16(),
                (a, b) => a & b,
            }),
            ProbeKernel::Int4 { .. } => Ok(match (input_key.1, entry.key_isnull) {
                (false, false) => input_key.0.as_i32() == entry.key.as_i32(),
                (a, b) => a & b,
            }),
            ProbeKernel::Int8 { .. } => Ok(match (input_key.1, entry.key_isnull) {
                (false, false) => input_key.0.as_i64() == entry.key.as_i64(),
                (a, b) => a & b,
            }),
            ProbeKernel::Text { .. } => match (input_key.1, entry.key_isnull) {
                (false, false) => {
                    let det_mcx = *self.entries.allocator();
                    // SAFETY: both sides are non-null live text varlenas —
                    // the input key per `kernel_key_of`'s caller contract,
                    // the entry's cached key inside its live stored image.
                    let a = unsafe { ::types_fmgr::datum_varlena_packed(input_key.0, det_mcx) }?;
                    let b = unsafe { ::types_fmgr::datum_varlena_packed(entry.key, det_mcx) }?;
                    Ok(a.data() == b.data())
                }
                (a, b) => Ok(a & b),
            },
            ProbeKernel::Expr => {
                // SAFETY: caller keeps entry images live (insert contract).
                unsafe {
                    exectuples::exec_store_minimal_tuple_ptr(
                        &mut self.tableslot,
                        slot_mcx,
                        entry.first_tuple,
                    )
                };
                let mut slots = EvalSlots {
                    scan: None,
                    inner: Some(input_slot),
                    outer: Some(&mut self.tableslot),
                };
                exec_qual(Some(&mut self.tab_eq_func), &mut slots)
            }
        }
    }

    /// The kernel's cached-key extraction for `match_tuple` callers. For the
    /// Text kernel the returned datum points into the slot's tuple — the
    /// caller must keep the slot live across the `match_tuple` call.
    pub fn kernel_key_of(&self, input_slot: &mut SlotData<'mcx>) -> (Datum, bool) {
        match self.kernel {
            ProbeKernel::Int2 { att }
            | ProbeKernel::Int4 { att }
            | ProbeKernel::Int8 { att }
            | ProbeKernel::Text { att, .. } => kernel_key(input_slot, att),
            ProbeKernel::Expr => (Datum::null(), true),
        }
    }

    /// C `FindTupleHashEntry`: find-only probe with a caller-supplied
    /// (potentially cross-type) equality; the hash must come from the
    /// caller's own input-side hash functions.
    pub fn find_entry_with(
        &self,
        hash: u32,
        eq: impl FnMut(u32) -> PgResult<bool>,
    ) -> PgResult<Option<u32>> {
        self.hashtab.find(hash, |ix| self.entries[ix as usize].hash, eq)
    }

    /// C SH_START_ITERATE/SH_ITERATE bucket-order drain; `cursor` starts 0.
    pub fn iterate(&self, cursor: &mut usize) -> Option<u32> {
        self.hashtab.iterate(cursor)
    }

    #[inline]
    pub fn num_entries(&self) -> usize {
        self.entries.len()
    }

    /// C `TupleHashEntryGetTuple`.
    #[inline]
    pub fn entry_tuple(&self, ix: u32) -> NonNull<MinimalTupleData> {
        self.entries[ix as usize].first_tuple
    }

    /// C `TupleHashEntryGetAdditional` (maxaligned, zero-initialized;
    /// None is C's NULL for additionalsize-0 tables, e.g. hashed DISTINCT).
    #[inline]
    pub fn entry_additional(&self, ix: u32) -> Option<NonNull<u8>> {
        if self.additionalsize == 0 {
            return None;
        }
        let t = self.entries[ix as usize].first_tuple.as_ptr().cast::<u8>();
        // SAFETY: the tuple sits additionalsize bytes into its allocation.
        unsafe { Some(NonNull::new_unchecked(t.sub(self.additionalsize))) }
    }

    /// K2 slot-free find over a staged key (kernel tables only): the `lookup`
    /// find leg with the key already in hand — no hashslot presentation, no
    /// slot deform. `None` = miss (the caller presents the key in a slot and
    /// runs the full `lookup` for the insert/spill leg — rare per batch).
    /// Bit-identical match semantics to `lookup`'s kernel arms.
    ///
    /// Contract (like `hash_staged`): a non-null staged datum is a live value
    /// of the kernel's key type.
    pub fn find_staged(&self, key: Datum, isnull: bool, hash: u32) -> PgResult<Option<u32>> {
        let TupleHashTable { entries, hashtab, kernel, .. } = self;
        let entry_hash = |ix: u32| entries[ix as usize].hash;
        match *kernel {
            ProbeKernel::Int2 { .. } => hashtab.find(hash, entry_hash, |ix| {
                let e = &entries[ix as usize];
                Ok(match (isnull, e.key_isnull) {
                    (false, false) => e.key.as_i16() == key.as_i16(),
                    (a, b) => a & b,
                })
            }),
            ProbeKernel::Int4 { .. } => hashtab.find(hash, entry_hash, |ix| {
                let e = &entries[ix as usize];
                Ok(match (isnull, e.key_isnull) {
                    (false, false) => e.key.as_i32() == key.as_i32(),
                    (a, b) => a & b,
                })
            }),
            ProbeKernel::Int8 { .. } => hashtab.find(hash, entry_hash, |ix| {
                let e = &entries[ix as usize];
                Ok(match (isnull, e.key_isnull) {
                    (false, false) => e.key.as_i64() == key.as_i64(),
                    (a, b) => a & b,
                })
            }),
            ProbeKernel::Text { .. } => {
                let det_mcx = *entries.allocator();
                // Detoast the input side once per probe, not per candidate.
                // SAFETY: non-null live text varlena (fn contract).
                let a = if isnull {
                    None
                } else {
                    Some(unsafe { ::types_fmgr::datum_varlena_packed(key, det_mcx) }?)
                };
                hashtab.find(hash, entry_hash, |ix| {
                    let e = &entries[ix as usize];
                    match (&a, e.key_isnull) {
                        (Some(a), false) => {
                            // SAFETY: e.key points into the live stored image.
                            let b =
                                unsafe { ::types_fmgr::datum_varlena_packed(e.key, det_mcx) }?;
                            Ok(a.data() == b.data())
                        }
                        (None, true) => Ok(true),
                        _ => Ok(false),
                    }
                })
            }
            ProbeKernel::Expr => unreachable!("staged find requires a probe kernel"),
        }
    }

    /// Repoint an entry at a relocated VERBATIM copy of its image
    /// (table-handoff install), rebasing the Text kernel's cached key pointer
    /// into the new image (the copy preserves byte layout, so the key's
    /// offset from the tuple start is invariant). Byval caches are untouched.
    pub fn relocate_entry(
        &self,
        e: &mut TupleHashEntryData,
        new_tuple: NonNull<MinimalTupleData>,
    ) {
        if matches!(self.kernel, ProbeKernel::Text { .. }) && !e.key_isnull {
            let off = e
                .key
                .as_usize()
                .wrapping_sub(e.first_tuple.as_ptr() as usize);
            e.key = Datum::from_usize((new_tuple.as_ptr() as usize).wrapping_add(off));
        }
        e.set_tuple(new_tuple);
    }

    /// True when this table's probe kernel supports the lane-v2 K2 staged
    /// batched hash pre-pass: a single-column Int4/Int8/Text kernel whose
    /// hash needs no compiled-program walk (Expr tables refuse — batching
    /// their per-row program would win nothing).
    pub fn staged_probe_supported(&self) -> bool {
        !matches!(self.kernel, ProbeKernel::Expr)
    }

    /// The single grouping key's integer width in bytes (2/4/8) when this
    /// table probes through an integer kernel — the lane-v2 compact-table
    /// (lanetable) admission input. `None` = Text/Expr kernel.
    pub fn staged_probe_int_width(&self) -> Option<u8> {
        match self.kernel {
            ProbeKernel::Int2 { .. } => Some(2),
            ProbeKernel::Int4 { .. } => Some(4),
            ProbeKernel::Int8 { .. } => Some(8),
            ProbeKernel::Text { .. } | ProbeKernel::Expr => None,
        }
    }

    /// K2 batched hashing: `TupleHashTableHash` over a staged key lane in one
    /// tight loop, bit-identical per element to [`Self::hash_slot`] over a
    /// slot carrying the same value. Kernel tables only
    /// ([`Self::staged_probe_supported`]).
    ///
    /// Safety contract (inline, like `kernel_key`): non-null staged datums
    /// must be live values of the kernel's key type.
    pub fn hash_staged(
        &self,
        keys: &[Datum],
        isnull: &[bool],
        out: &mut Vec<u32>,
    ) -> PgResult<()> {
        debug_assert_eq!(keys.len(), isnull.len());
        out.clear();
        out.reserve(keys.len());
        match self.kernel {
            ProbeKernel::Int2 { .. } => {
                for (&k, &n) in keys.iter().zip(isnull) {
                    let h = if n {
                        0
                    } else {
                        ::hashfn::hash_bytes_uint32(k.as_i16() as i32 as u32)
                    };
                    out.push(::hashfn::murmurhash32(h));
                }
            }
            ProbeKernel::Int4 { .. } => {
                for (&k, &n) in keys.iter().zip(isnull) {
                    let h = if n { 0 } else { ::hashfn::hash_bytes_uint32(k.as_u32()) };
                    out.push(::hashfn::murmurhash32(h));
                }
            }
            ProbeKernel::Int8 { .. } => {
                for (&k, &n) in keys.iter().zip(isnull) {
                    let h = if n { 0 } else { ::hashfn::hash_bytes_uint32(hashint8_fold(k)) };
                    out.push(::hashfn::murmurhash32(h));
                }
            }
            ProbeKernel::Text { .. } => {
                let mcx = *self.entries.allocator();
                for (&k, &n) in keys.iter().zip(isnull) {
                    let h = if n { 0 } else { text_kernel_hash(k, mcx)? };
                    out.push(::hashfn::murmurhash32(h));
                }
            }
            ProbeKernel::Expr => unreachable!("staged hashing requires a probe kernel"),
        }
        Ok(())
    }

    /// C `ResetTupleHashTable`; the caller resets the entry context.
    pub fn reset(&mut self) {
        self.entries.clear();
        self.hashtab.clear();
    }
}

/// The Text kernel's hashtext core over a live text datum: detoast
/// (`pg_detoast_datum_packed`; external/compressed images land in `mcx` — the
/// same metacxt the Expr path's armed result mcx uses, per the divergence
/// note in `build_tuple_hash_table`) + raw-bytes hash_any — bit-identical to
/// the fmgr `hashtext` under the kernel's resolved-once raw-bytes collation
/// gate.
///
/// Safety contract (inline, not `unsafe fn`, mirroring `kernel_key`): the
/// datum must be a non-null live text/varchar varlena — kernel selection
/// (hashtext/texteq operator pair) proves the key column's type.
#[inline]
fn text_kernel_hash(key: Datum, mcx: Mcx<'_>) -> PgResult<u32> {
    // SAFETY: non-null live text varlena (fn contract above).
    let v = unsafe { ::types_fmgr::datum_varlena_packed(key, mcx) }?;
    Ok(::hashfn::hash_bytes(v.data()))
}

#[inline(always)]
fn kernel_key(input_slot: &mut SlotData<'_>, att: u16) -> (Datum, bool) {
    exectuples::slot_getsomeattrs(input_slot, att as i32 + 1);
    let base = input_slot.base();
    (base.tts_values[att as usize], base.tts_isnull[att as usize])
}

// hashfunc.c hashint8's cross-type-compatible fold to 32 bits.
#[inline(always)]
fn hashint8_fold(key: Datum) -> u32 {
    let val = key.as_i64();
    let lohalf = val as u32;
    let hihalf = (val >> 32) as u32;
    lohalf ^ if val >= 0 { hihalf } else { !hihalf }
}

#[cold]
#[inline(never)]
fn oom_entries(mcx: Mcx<'_>, add: usize) -> Box<PgError> {
    Box::new(mcx.oom(add * core::mem::size_of::<TupleHashEntryData>()))
}
