// nodeHash.c single-batch build side; multi-batch/skew/parallel are loud.
// Bucket chains are arena+u32-handle (rule 2), not raw pointer chains.
#![allow(non_snake_case)]

use core::ptr::NonNull;
use std::rc::Rc;

use ::execexpr::{exec_build_hash32_from_attrs, exec_eval_expr, EvalSlots, ExprState};
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{vec_with_capacity_in, Mcx, PgBox, PgVec};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::types_nodes::plannodes::Hash;
use ::types_slot::TupleSlotKind;
use ::types_tuple::{MinimalTupleData, TupleDescData, SizeofMinimalTupleHeader};

pub fn init_seams() {}

/// The build side pulls its tuples from this child (C's `outerPlan(hashNode)`).
pub trait HashBuildInput<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>;
}

// C HashJoinTupleData; matched = HeapTupleHeaderSetMatch; u32::MAX ends chains.
#[derive(Clone, Copy)]
pub struct HashJoinTupleEntry {
    pub next: u32,
    pub hashvalue: u32,
    pub matched: bool,
    pub tuple: NonNull<MinimalTupleData>,
}

const END: u32 = u32::MAX;

pub struct HashJoinTable<'mcx> {
    bucket_mask: u32,
    buckets: PgVec<'mcx, u32>,
    entries: PgVec<'mcx, HashJoinTupleEntry>,
    total_tuples: f64,
}

impl<'mcx> HashJoinTable<'mcx> {
    fn create(mcx: Mcx<'mcx>, nbuckets: u32) -> PgResult<HashJoinTable<'mcx>> {
        debug_assert!(nbuckets.is_power_of_two());
        let mut buckets = vec_with_capacity_in(mcx, nbuckets as usize)?;
        buckets.resize(nbuckets as usize, END);
        Ok(HashJoinTable {
            bucket_mask: nbuckets - 1,
            buckets,
            entries: PgVec::new_in(mcx),
            total_tuples: 0.0,
        })
    }

    fn insert(
        &mut self,
        slot: &mut ::types_slot::SlotData<'mcx>,
        slot_mcx: Mcx<'mcx>,
        table_mcx: Mcx<'mcx>,
        hashvalue: u32,
    ) -> PgResult<()> {
        let tup = exectuples::exec_copy_slot_minimal_tuple(slot, slot_mcx, table_mcx, 0)?;
        let tuple = NonNull::new(tup.as_ptr().cast_mut().cast::<MinimalTupleData>())
            .expect("minimal tuple image is non-null");
        // Bulk-freed at query-context reset: forget, never drop (docs/no-drop.md).
        core::mem::forget(tup);

        let bucketno = (hashvalue & self.bucket_mask) as usize;
        let ix = self.entries.len() as u32;
        if self.entries.len() == self.entries.capacity() {
            let add = self.entries.capacity().max(16);
            self.entries
                .try_reserve(add)
                .map_err(|_| oom_entries(*self.entries.allocator(), add))?;
        }
        self.entries.push(HashJoinTupleEntry {
            next: self.buckets[bucketno],
            hashvalue,
            matched: false,
            tuple,
        });
        self.buckets[bucketno] = ix;
        self.total_tuples += 1.0;
        Ok(())
    }

    #[inline]
    pub fn total_tuples(&self) -> f64 {
        self.total_tuples
    }

    #[inline]
    pub fn bucket_head(&self, bucketno: u32) -> u32 {
        self.buckets[bucketno as usize]
    }

    #[inline]
    pub fn bucket_of(&self, hashvalue: u32) -> u32 {
        hashvalue & self.bucket_mask
    }

    #[inline]
    pub fn entry(&self, ix: u32) -> HashJoinTupleEntry {
        self.entries[ix as usize]
    }

    #[inline]
    pub const fn chain_end() -> u32 {
        END
    }

    #[inline]
    pub fn set_matched(&mut self, ix: u32) {
        self.entries[ix as usize].matched = true;
    }

    #[inline]
    pub fn nbuckets(&self) -> u32 {
        self.bucket_mask + 1
    }

    /// ExecHashTableResetMatchFlags.
    pub fn reset_match_flags(&mut self) {
        for e in self.entries.iter_mut() {
            e.matched = false;
        }
    }
}

pub struct HashState<'mcx> {
    hash_expr: PgBox<'mcx, ExprState<'mcx>>,
    pub table: Option<HashJoinTable<'mcx>>,
    pub hash_tuple_slot: ExecSlotId,
    ntuples_est: f64,
    tupwidth: i32,
    #[allow(dead_code)]
    inner_desc: Rc<TupleDescData<'static>>,
}

/// `ExecInitHash`: the inner hash program + the slot the probe stores bucket
/// tuples into.
pub fn exec_init_hash<'mcx>(
    node: &'mcx Hash<'mcx>,
    estate: &mut EStateData<'mcx>,
    inner_desc: Rc<TupleDescData<'static>>,
    inner_attnums: &[i16],
    inner_hashfn_oids: &[Oid],
    collations: &[Oid],
) -> PgResult<HashState<'mcx>> {
    let mcx = estate.es_query_cxt;
    let hash_expr = exec_build_hash32_from_attrs(
        mcx,
        &inner_desc,
        inner_hashfn_oids,
        collations,
        inner_attnums,
        0,
    )?;
    let hash_tuple_slot =
        estate.exec_init_extra_tuple_slot(Some(inner_desc.clone()), TupleSlotKind::MinimalTuple);

    let child = node
        .plan
        .lefttree
        .expect("Hash without an outer plan")
        .as_plan()
        .expect("Hash outer is a plan node");
    let ntuples_est = if node.plan.parallel_aware {
        panic!("ExecHashTableCreate (nodeHash.c): parallel-aware Hash not ported")
    } else {
        child.plan_rows
    };

    Ok(HashState {
        hash_expr,
        table: None,
        hash_tuple_slot,
        ntuples_est,
        tupwidth: child.plan_width,
        inner_desc,
    })
}

/// `MultiExecHash`/`MultiExecPrivateHash`: build the single-batch table.
pub fn multi_exec_hash<'mcx, C: HashBuildInput<'mcx>>(
    hs: &mut HashState<'mcx>,
    child: &mut C,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let (nbuckets, nbatch, _skew) = exec_choose_hash_table_size(hs.ntuples_est, hs.tupwidth, true);
    assert_eq!(
        nbatch, 1,
        "MultiExecHash (nodeHash.c): nbatch>1 multi-batch spill; work_mem overflow lane not ported"
    );
    let mut table = HashJoinTable::create(mcx, nbuckets)?;

    loop {
        let Some(slot_id) = child.exec_proc(estate)? else {
            break;
        };
        let hashvalue = {
            let slot = &mut estate.es_tupleTable[slot_id.0 as usize];
            let mut slots = EvalSlots { scan: None, inner: Some(slot), outer: None };
            let r = exec_eval_expr(&mut hs.hash_expr, &mut slots)?;
            // Non-strict fold keeps NULL-key tuples; they never match the
            // inner-join hashqual recheck, so the result equals C's strict drop.
            r.value.as_u32()
        };
        let slot = &mut estate.es_tupleTable[slot_id.0 as usize];
        table.insert(slot, mcx, mcx, hashvalue)?;
    }

    hs.table = Some(table);
    Ok(())
}

/// `ExecEndHash`: the table lives in the query arena (wholesale reset).
pub fn exec_end_hash(_hs: &mut HashState<'_>) {}

// C constants (hashjoin.h / htup_details.h), 64-bit build.
const HJTUPLE_OVERHEAD: usize = 16; // MAXALIGN(sizeof(HashJoinTupleData))
const SIZEOF_HASHJOINTUPLE: usize = 8; // pointer
const NTUP_PER_BUCKET: f64 = 1.0;
const MAX_ALLOC_SIZE: usize = 0x3fff_ffff;
const SKEW_HASH_MEM_PERCENT: usize = 2;
const SKEW_BUCKET_OVERHEAD: usize = 16; // MAXALIGN(sizeof(HashSkewBucket))

#[inline]
const fn maxalign(n: usize) -> usize {
    (n + 7) & !7
}

/// C `get_hash_memory_limit` (nodeHash.c): `work_mem * hash_mem_multiplier * 1024`.
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

/// C `ExecChooseHashTableSize` serial path -> (numbuckets, numbatches, num_skew_mcvs).
pub fn exec_choose_hash_table_size(ntuples: f64, tupwidth: i32, useskew: bool) -> (u32, i32, i32) {
    let ntuples = if ntuples <= 0.0 { 1000.0 } else { ntuples };

    let tupsize = HJTUPLE_OVERHEAD + maxalign(SizeofMinimalTupleHeader) + maxalign(tupwidth as usize);
    let inner_rel_bytes = ntuples * tupsize as f64;

    let mut hash_table_bytes = get_hash_memory_limit();
    let mut space_allowed = hash_table_bytes;

    let mut num_skew_mcvs: i64 = 0;
    if useskew {
        let bytes_per_mcv =
            tupsize + (8 * core::mem::size_of::<usize>()) + core::mem::size_of::<i32>() + SKEW_BUCKET_OVERHEAD;
        let mut skew_mcvs = hash_table_bytes / bytes_per_mcv;
        skew_mcvs = (skew_mcvs * SKEW_HASH_MEM_PERCENT) / 100;
        skew_mcvs = skew_mcvs.min(i32::MAX as usize);
        num_skew_mcvs = skew_mcvs as i64;
        if skew_mcvs > 0 {
            hash_table_bytes -= skew_mcvs * bytes_per_mcv;
        }
    }

    let mut max_pointers = hash_table_bytes / SIZEOF_HASHJOINTUPLE;
    max_pointers = max_pointers.min(MAX_ALLOC_SIZE / SIZEOF_HASHJOINTUPLE);
    max_pointers = prevpower2(max_pointers);
    max_pointers = max_pointers.min(i32::MAX as usize / 2 + 1);

    let mut dbuckets = (ntuples / NTUP_PER_BUCKET).ceil();
    dbuckets = dbuckets.min(max_pointers as f64);
    let mut nbuckets = dbuckets as usize;
    nbuckets = nbuckets.max(1024);
    nbuckets = nextpower2_32(nbuckets as u32) as usize;

    let mut nbatch: i64 = 1;
    let bucket_bytes = SIZEOF_HASHJOINTUPLE * nbuckets;
    if inner_rel_bytes + bucket_bytes as f64 > hash_table_bytes as f64 {
        let bucket_size = tupsize * (NTUP_PER_BUCKET as usize) + SIZEOF_HASHJOINTUPLE;
        let mut sbuckets = if hash_table_bytes <= bucket_size {
            1
        } else {
            nextpower2_size(hash_table_bytes / bucket_size)
        };
        sbuckets = sbuckets.min(max_pointers);
        nbuckets = nextpower2_32(sbuckets as u32) as usize;
        let bucket_bytes2 = nbuckets * SIZEOF_HASHJOINTUPLE;
        let dbatch = (inner_rel_bytes / (hash_table_bytes - bucket_bytes2) as f64)
            .ceil()
            .min(max_pointers as f64);
        let minbatch = dbatch as i64;
        nbatch = nextpower2_32(2i64.max(minbatch) as u32) as i64;
    }

    const BLCKSZ: usize = 8192;
    while nbatch > 1 {
        if nbuckets > (MAX_ALLOC_SIZE / SIZEOF_HASHJOINTUPLE / 2) {
            break;
        }
        if space_allowed > usize::MAX / 2 {
            break;
        }
        if (nbatch as usize) < space_allowed / BLCKSZ {
            break;
        }
        nbuckets *= 2;
        num_skew_mcvs *= 2;
        space_allowed *= 2;
        nbatch /= 2;
    }

    (nbuckets as u32, nbatch as i32, num_skew_mcvs as i32)
}

#[inline]
fn nextpower2_32(n: u32) -> u32 {
    if n <= 1 {
        1
    } else {
        1u32 << (32 - (n - 1).leading_zeros())
    }
}

#[inline]
fn nextpower2_size(n: usize) -> usize {
    if n <= 1 {
        1
    } else {
        1usize << (usize::BITS - (n - 1).leading_zeros())
    }
}

#[inline]
fn prevpower2(n: usize) -> usize {
    if n == 0 {
        1
    } else {
        1usize << (usize::BITS - 1 - n.leading_zeros())
    }
}

#[cold]
#[inline(never)]
fn oom_entries(mcx: Mcx<'_>, add: usize) -> Box<PgError> {
    Box::new(mcx.oom(add * core::mem::size_of::<HashJoinTupleEntry>()))
}
