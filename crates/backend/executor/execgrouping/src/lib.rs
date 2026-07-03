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
// under a byval ProbeKernel, whose match skips the stored-tuple deform.
#[derive(Clone, Copy)]
pub struct TupleHashEntryData {
    first_tuple: NonNull<MinimalTupleData>,
    hash: u32,
    key_isnull: bool,
    key: Datum,
}

const _: () = assert!(core::mem::size_of::<TupleHashEntryData>() == 24);

// Monomorphized single-byval-key probe kernel selected at build from the
// hash/eq fn oids (execexpr CmpOp precedent): C-exact hash + NOT DISTINCT
// inline, no compiled-program walk (C only has the interpreted path non-JIT).
#[derive(Clone, Copy, PartialEq, Eq)]
enum ProbeKernel {
    Expr,
    Int4 { att: u16 },
    Int8 { att: u16 },
}

impl ProbeKernel {
    fn select(key_col_idx: &[i16], eqfuncoids: &[Oid], hashfunctions: &[Oid]) -> ProbeKernel {
        if let ([col], [eq], [hash]) = (key_col_idx, eqfuncoids, hashfunctions) {
            let att = (col - 1) as u16;
            match (*hash, *eq) {
                (450, 65) => return ProbeKernel::Int4 { att },
                (949, 467) => return ProbeKernel::Int8 { att },
                _ => {}
            }
        }
        ProbeKernel::Expr
    }
}

pub struct TupleHashTable<'mcx> {
    entries: PgVec<'mcx, TupleHashEntryData>,
    hashtab: hashbrown::HashTable<u32>,
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

    let tab_hash_expr = exec_build_hash32_from_attrs(
        metacxt,
        input_desc,
        hashfunctions,
        collations,
        key_col_idx,
        0,
    )?;
    let tab_eq_func = exec_build_grouping_equal(
        metacxt,
        input_desc,
        input_desc,
        key_col_idx,
        eqfuncoids,
        collations,
    )?;
    let tableslot = exectuples::make_tuple_table_slot(
        metacxt,
        TupleSlotKind::MinimalTuple,
        Some(input_desc.clone()),
    );

    Ok(TupleHashTable {
        entries: vec_with_capacity_in(metacxt, nbuckets)?,
        hashtab: hashbrown::HashTable::with_capacity(nbuckets),
        additionalsize,
        kernel: ProbeKernel::select(key_col_idx, eqfuncoids, hashfunctions),
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
    // C MemoryContextMemAllocated(hash_metacxt); 5 = swiss-table slot+control.
    pub fn meta_mem(&self) -> usize {
        self.entries.capacity() * core::mem::size_of::<TupleHashEntryData>()
            + self.hashtab.capacity() * 5
    }

    /// C `TupleHashTableHash`; the caller resets its per-tuple context.
    pub fn hash_slot(&mut self, input_slot: &mut SlotData<'mcx>) -> PgResult<u32> {
        // NULL hashes as 0, as EEOP_HASHDATUM_FIRST does.
        match self.kernel {
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
        let found = match *kernel {
            ProbeKernel::Int4 { att } => {
                let (key, isnull) = kernel_key(input_slot, att);
                hashtab
                    .find(hash as u64, |ix: &u32| {
                        let e = &entries[*ix as usize];
                        e.hash == hash
                            && match (isnull, e.key_isnull) {
                                (false, false) => e.key.as_i32() == key.as_i32(),
                                (a, b) => a & b,
                            }
                    })
                    .copied()
            }
            ProbeKernel::Int8 { att } => {
                let (key, isnull) = kernel_key(input_slot, att);
                hashtab
                    .find(hash as u64, |ix: &u32| {
                        let e = &entries[*ix as usize];
                        e.hash == hash
                            && match (isnull, e.key_isnull) {
                                (false, false) => e.key.as_i64() == key.as_i64(),
                                (a, b) => a & b,
                            }
                    })
                    .copied()
            }
            ProbeKernel::Expr => hashtab
                .find(hash as u64, |ix: &u32| {
                    let e = &entries[*ix as usize];
                    if e.hash != hash {
                        return false;
                    }
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
                        Ok(m) => m,
                        Err(e) => {
                            eq_err = Some(e);
                            false
                        }
                    }
                })
                .copied(),
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
            ProbeKernel::Int4 { att } | ProbeKernel::Int8 { att } => kernel_key(input_slot, att),
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
        self.hashtab
            .insert_unique(hash as u64, ix, |i| entries[*i as usize].hash as u64);
        Ok((Some(ix), true))
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

    /// C `ResetTupleHashTable`; the caller resets the entry context.
    pub fn reset(&mut self) {
        self.entries.clear();
        self.hashtab.clear();
    }
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
