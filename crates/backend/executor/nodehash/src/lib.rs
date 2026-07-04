// nodeHash.c serial build side; skew and parallel absent (sizing still
// reserves skew memory like C). Tuples are C's HashJoinTupleData: 16-byte
// header before the image in the per-batch aux arena, pointer bucket chains.
#![allow(non_snake_case)]

use core::ptr::NonNull;
use std::rc::Rc;

use ::execexpr::{exec_build_hash32_from_attrs, exec_eval_expr, EvalSlots, ExprState};
use ::executils::{AuxCxtId, EStateData, EcxtId, ExecSlotId};
use ::fd::buffile::BufFile;
use ::mcx::{vec_with_capacity_in, Mcx, PgBox, PgVec};
use ::types_core::instrument::HashInstrumentation;
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::types_nodes::plannodes::Hash;
use ::types_slot::TupleSlotKind;
use ::types_tuple::{MinimalTupleData, TupleDescData, SizeofMinimalTupleHeader};

pub fn init_seams() {}

/// The build side pulls its tuples from this child (C's `outerPlan(hashNode)`).
pub trait HashBuildInput<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>;

    /// `MultiExecHash` entry; the dispatcher overrides this to batch-consume
    /// fusible children (same per-row hash+insert, node recursion elided).
    fn multi_exec(
        &mut self,
        hs: &mut HashState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
    where
        Self: Sized,
    {
        multi_exec_hash(hs, self, estate)
    }
}

/// Page-batch feed for the fused hash-build drive; same contract as the agg
/// drive's source (store staged tuple `i` into the slot, apply the scan qual).
pub trait HashBuildBatchSource<'mcx> {
    fn next_batch(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32>;
    fn fetch_tuple(&mut self, i: u32, estate: &mut EStateData<'mcx>) -> PgResult<bool>;
    fn slot(&self) -> ExecSlotId;
}

/// C `HashJoinTupleData` (16 = HJTUPLE_OVERHEAD; image at +16).
#[repr(C)]
pub struct HashJoinTupleHdr {
    next: *mut HashJoinTupleHdr,
    hashvalue: u32,
    _pad: u32,
}

impl HashJoinTupleHdr {
    #[inline(always)]
    pub fn next(&self) -> *mut HashJoinTupleHdr {
        self.next
    }

    #[inline(always)]
    pub fn hashvalue(&self) -> u32 {
        self.hashvalue
    }

    /// C `HJTUPLE_MINTUPLE`.
    /// # Safety
    /// `this` is a live header from `insert`.
    #[inline(always)]
    pub unsafe fn mintuple(this: *mut HashJoinTupleHdr) -> NonNull<MinimalTupleData> {
        // SAFETY: caller contract; the image starts HJTUPLE_OVERHEAD past the header.
        unsafe { NonNull::new_unchecked(this.cast::<u8>().add(HJTUPLE_OVERHEAD)).cast() }
    }
}

#[derive(Clone, Copy)]
struct ChunkMeta {
    next: u32,
    used: u32,
    maxlen: u32,
}

const END: u32 = u32::MAX;

pub struct HashJoinTable<'mcx> {
    nbuckets: u32,
    log2_nbuckets: u32,
    nbuckets_original: u32,
    nbuckets_optimal: u32,
    log2_nbuckets_optimal: u32,
    buckets: PgVec<'mcx, *mut HashJoinTupleHdr>,
    tuples: PgVec<'mcx, NonNull<HashJoinTupleHdr>>,
    pub nbatch: i32,
    pub curbatch: i32,
    pub nbatch_original: i32,
    pub nbatch_outstart: i32,
    grow_enabled: bool,
    total_tuples: f64,
    space_used: usize,
    space_peak: usize,
    space_allowed: usize,
    pub inner_batch_file: PgVec<'mcx, Option<BufFile<'mcx>>>,
    pub outer_batch_file: PgVec<'mcx, Option<BufFile<'mcx>>>,
    batch_cxt: AuxCxtId,
}

impl<'mcx> HashJoinTable<'mcx> {
    fn create(
        mcx: Mcx<'mcx>,
        estate: &mut EStateData<'mcx>,
        nbuckets: u32,
        nbatch: i32,
        space_allowed: usize,
    ) -> PgResult<HashJoinTable<'mcx>> {
        debug_assert!(nbuckets.is_power_of_two());
        let mut buckets = vec_with_capacity_in(mcx, nbuckets as usize)?;
        buckets.resize(nbuckets as usize, core::ptr::null_mut());
        let mut table = HashJoinTable {
            nbuckets,
            log2_nbuckets: nbuckets.trailing_zeros(),
            nbuckets_original: nbuckets,
            nbuckets_optimal: nbuckets,
            log2_nbuckets_optimal: nbuckets.trailing_zeros(),
            buckets,
            tuples: PgVec::new_in(mcx),
            nbatch,
            curbatch: 0,
            nbatch_original: nbatch,
            nbatch_outstart: nbatch,
            grow_enabled: true,
            total_tuples: 0.0,
            space_used: 0,
            space_peak: 0,
            space_allowed,
            inner_batch_file: PgVec::new_in(mcx),
            outer_batch_file: PgVec::new_in(mcx),
            batch_cxt: estate.create_aux_context("HashBatchContext"),
        };
        if nbatch > 1 {
            table.inner_batch_file.resize_with(nbatch as usize, || None);
            table.outer_batch_file.resize_with(nbatch as usize, || None);
            ::fd::buffile::PrepareTempTablespaces();
        }
        Ok(table)
    }

    #[inline]
    fn tuple_size(hdr: NonNull<HashJoinTupleHdr>) -> usize {
        let t_len = unsafe { (*HashJoinTupleHdr::mintuple(hdr.as_ptr()).as_ptr()).t_len };
        HJTUPLE_OVERHEAD + t_len as usize
    }

    // Cold. C's dense_alloc chunk list replayed from insertion-order sizes;
    // unmatched-scan and spill order depend on the walk permutation.
    fn chunk_walk_order(&self, mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u32>> {
        let mut chunks: PgVec<'mcx, ChunkMeta> = PgVec::new_in(mcx);
        let mut chunk_head = END;
        let mut chunk_of: PgVec<'mcx, u32> = vec_with_capacity_in(mcx, self.tuples.len())?;
        for &hdr in self.tuples.iter() {
            let size = maxalign(Self::tuple_size(hdr)) as u32;
            let id = if size as usize > HASH_CHUNK_THRESHOLD {
                let id = chunks.len() as u32;
                if chunk_head != END {
                    let head_next = chunks[chunk_head as usize].next;
                    chunks.push(ChunkMeta { next: head_next, used: size, maxlen: size });
                    chunks[chunk_head as usize].next = id;
                } else {
                    chunks.push(ChunkMeta { next: END, used: size, maxlen: size });
                    chunk_head = id;
                }
                id
            } else if chunk_head == END
                || chunks[chunk_head as usize].maxlen - chunks[chunk_head as usize].used < size
            {
                let id = chunks.len() as u32;
                chunks.push(ChunkMeta {
                    next: chunk_head,
                    used: size,
                    maxlen: HASH_CHUNK_SIZE as u32,
                });
                chunk_head = id;
                id
            } else {
                chunks[chunk_head as usize].used += size;
                chunk_head
            };
            chunk_of.push(id);
        }

        let nchunks = chunks.len();
        let mut pos: PgVec<'mcx, u32> = vec_with_capacity_in(mcx, nchunks)?;
        pos.resize(nchunks, 0);
        let mut npos = 0u32;
        let mut c = chunk_head;
        while c != END {
            pos[c as usize] = npos;
            npos += 1;
            c = chunks[c as usize].next;
        }
        let mut counts: PgVec<'mcx, u32> = vec_with_capacity_in(mcx, npos as usize + 1)?;
        counts.resize(npos as usize + 1, 0);
        for &cid in chunk_of.iter() {
            counts[pos[cid as usize] as usize + 1] += 1;
        }
        for i in 1..counts.len() {
            let prev = counts[i - 1];
            counts[i] += prev;
        }
        let mut order: PgVec<'mcx, u32> = vec_with_capacity_in(mcx, self.tuples.len())?;
        order.resize(self.tuples.len(), 0);
        for (ix, &cid) in chunk_of.iter().enumerate() {
            let p = pos[cid as usize] as usize;
            order[counts[p] as usize] = ix as u32;
            counts[p] += 1;
        }
        Ok(order)
    }

    /// `ExecHashTableInsert`.
    pub fn insert(
        &mut self,
        estate: &mut EStateData<'mcx>,
        slot_id: ExecSlotId,
        scratch_ecxt: EcxtId,
        hashvalue: u32,
    ) -> PgResult<()> {
        let query_mcx = estate.es_query_cxt;
        let (bucketno, batchno) = self.get_bucket_and_batch(hashvalue);
        if batchno == self.curbatch {
            let (slot, batch_mcx) = estate.slot_and_aux_mcx(slot_id, self.batch_cxt);
            let mut tup = exectuples::exec_copy_slot_minimal_tuple(
                slot,
                query_mcx,
                batch_mcx,
                HJTUPLE_OVERHEAD,
            )?;
            tup.data_mut().clear_match();
            let t_len = tup.t_len();
            let hdr = tup.forget_base().as_ptr().cast::<HashJoinTupleHdr>();

            let hash_tuple_size = HJTUPLE_OVERHEAD + t_len as usize;
            let ntuples = self.total_tuples;
            if self.tuples.len() == self.tuples.capacity() {
                let add = self.tuples.capacity().max(256);
                self.tuples
                    .try_reserve(add)
                    .map_err(|_| oom_tuples(*self.tuples.allocator(), add))?;
            }
            // SAFETY: hdr = the forgotten allocation's prefix; bucketno masked.
            unsafe {
                let head = self.buckets.get_unchecked_mut(bucketno as usize);
                (*hdr).next = *head;
                (*hdr).hashvalue = hashvalue;
                *head = hdr;
                self.tuples.push(NonNull::new_unchecked(hdr));
            }

            if self.nbatch == 1 && ntuples > (self.nbuckets_optimal as f64) * NTUP_PER_BUCKET {
                if self.nbuckets_optimal <= i32::MAX as u32 / 2
                    && (self.nbuckets_optimal as usize) * 2
                        <= MAX_ALLOC_SIZE / core::mem::size_of::<usize>()
                {
                    self.nbuckets_optimal *= 2;
                    self.log2_nbuckets_optimal += 1;
                }
            }

            self.space_used += hash_tuple_size;
            if self.space_used > self.space_peak {
                self.space_peak = self.space_used;
            }
            if self.space_used
                + self.nbuckets_optimal as usize * core::mem::size_of::<usize>()
                > self.space_allowed
            {
                self.increase_num_batches(query_mcx)?;
            }
        } else {
            debug_assert!(batchno > self.curbatch);
            let (slot, scratch_mcx) = estate.slot_and_per_tuple_mcx(slot_id, scratch_ecxt);
            let fetched =
                exectuples::exec_fetch_slot_minimal_tuple(slot, query_mcx, scratch_mcx)?;
            let (ptr, t_len): (*const u8, u32) = match &fetched {
                exectuples::FetchedMinimalTuple::Slot(m, _) => {
                    // SAFETY: live stored image; header read.
                    (m.as_ptr().cast_const().cast(), unsafe { m.as_ref().t_len })
                }
                exectuples::FetchedMinimalTuple::Copied(t) => (t.as_ptr(), t.t_len()),
            };
            // SAFETY: a minimal tuple image is t_len readable bytes.
            let bytes = unsafe { core::slice::from_raw_parts(ptr, t_len as usize) };
            save_tuple(
                &mut self.inner_batch_file[batchno as usize],
                hashvalue,
                bytes,
                query_mcx,
            )?;
        }
        Ok(())
    }

    /// `ExecHashIncreaseNumBatches` + `ExecHashIncreaseBatchSize`.
    fn increase_num_batches(&mut self, mcx: Mcx<'mcx>) -> PgResult<()> {
        if !self.grow_enabled {
            return Ok(());
        }
        let oldnbatch = self.nbatch;
        if oldnbatch as usize
            > (i32::MAX as usize / 2).min(MAX_ALLOC_SIZE / (core::mem::size_of::<usize>() * 2))
        {
            return Ok(());
        }
        let batch_space = self.nbatch as usize * 2 * BLCKSZ;
        if self.space_allowed <= batch_space {
            self.space_allowed *= 2;
            return Ok(());
        }
        let nbatch = oldnbatch * 2;
        if self.inner_batch_file.is_empty() {
            ::fd::buffile::PrepareTempTablespaces();
        }
        self.inner_batch_file.resize_with(nbatch as usize, || None);
        self.outer_batch_file.resize_with(nbatch as usize, || None);
        self.nbatch = nbatch;

        if self.nbuckets_optimal != self.nbuckets {
            debug_assert!(self.nbuckets_optimal > self.nbuckets);
            self.nbuckets = self.nbuckets_optimal;
            self.log2_nbuckets = self.log2_nbuckets_optimal;
        }
        self.buckets.clear();
        self.buckets.resize(self.nbuckets as usize, core::ptr::null_mut());

        let order = self.chunk_walk_order(mcx)?;
        // Relisting kept tuples in walk order reproduces C's re-dense_alloc order.
        let mut kept: PgVec<'mcx, NonNull<HashJoinTupleHdr>> =
            vec_with_capacity_in(mcx, self.tuples.len())?;
        let ninmemory = order.len();
        let mut nfreed = 0usize;
        for &ix in order.iter() {
            let hdr = self.tuples[ix as usize];
            // SAFETY: headers/images live in the batch arena until reset.
            let hashvalue = unsafe { hdr.as_ref().hashvalue };
            let hash_tuple_size = Self::tuple_size(hdr);
            let (bucketno, batchno) = self.get_bucket_and_batch(hashvalue);
            if batchno == self.curbatch {
                // SAFETY: as above; bucketno < buckets.len() by mask.
                unsafe {
                    let head = &mut self.buckets[bucketno as usize];
                    (*hdr.as_ptr()).next = *head;
                    *head = hdr.as_ptr();
                }
                kept.push(hdr);
            } else {
                debug_assert!(batchno > self.curbatch);
                let tuple = unsafe { HashJoinTupleHdr::mintuple(hdr.as_ptr()) };
                let t_len = unsafe { (*tuple.as_ptr()).t_len };
                // SAFETY: entry images live in the batch arena until reset.
                let bytes = unsafe {
                    core::slice::from_raw_parts(tuple.as_ptr().cast::<u8>(), t_len as usize)
                };
                save_tuple(
                    &mut self.inner_batch_file[batchno as usize],
                    hashvalue,
                    bytes,
                    mcx,
                )?;
                self.space_used -= hash_tuple_size;
                nfreed += 1;
            }
        }
        self.tuples = kept;

        // All or none moved: more batches can't subdivide this key set.
        if nfreed == 0 || nfreed == ninmemory {
            self.grow_enabled = false;
        }
        Ok(())
    }

    /// `ExecHashIncreaseNumBuckets`.
    fn increase_num_buckets(&mut self, mcx: Mcx<'mcx>) -> PgResult<()> {
        if self.nbuckets >= self.nbuckets_optimal {
            return Ok(());
        }
        self.nbuckets = self.nbuckets_optimal;
        self.log2_nbuckets = self.log2_nbuckets_optimal;
        self.buckets.clear();
        self.buckets.resize(self.nbuckets as usize, core::ptr::null_mut());
        let order = self.chunk_walk_order(mcx)?;
        for &ix in order.iter() {
            let hdr = self.tuples[ix as usize];
            // SAFETY: headers live in the batch arena until reset.
            unsafe {
                let hashvalue = hdr.as_ref().hashvalue;
                let (bucketno, _batchno) = self.get_bucket_and_batch(hashvalue);
                let head = &mut self.buckets[bucketno as usize];
                (*hdr.as_ptr()).next = *head;
                *head = hdr.as_ptr();
            }
        }
        Ok(())
    }

    fn finish_build(&mut self, mcx: Mcx<'mcx>) -> PgResult<()> {
        if self.nbuckets != self.nbuckets_optimal {
            self.increase_num_buckets(mcx)?;
        }
        self.space_used += self.nbuckets as usize * core::mem::size_of::<usize>();
        if self.space_used > self.space_peak {
            self.space_peak = self.space_used;
        }
        Ok(())
    }

    /// `ExecHashTableReset`.
    pub fn reset(&mut self, estate: &mut EStateData<'mcx>) {
        estate.reset_aux_context(self.batch_cxt);
        self.tuples.clear();
        self.buckets.clear();
        self.buckets.resize(self.nbuckets as usize, core::ptr::null_mut());
        self.space_used = 0;
    }

    /// `ExecHashTableDestroy`: batch 0 never has files.
    pub fn destroy(&mut self) -> PgResult<()> {
        for i in 1..self.inner_batch_file.len() {
            if let Some(f) = self.inner_batch_file[i].take() {
                f.close()?;
            }
            if let Some(f) = self.outer_batch_file[i].take() {
                f.close()?;
            }
        }
        Ok(())
    }

    /// `ExecHashGetBucketAndBatch`.
    #[inline]
    pub fn get_bucket_and_batch(&self, hashvalue: u32) -> (u32, i32) {
        let nbuckets = self.nbuckets;
        let nbatch = self.nbatch as u32;
        if nbatch > 1 {
            (
                hashvalue & (nbuckets - 1),
                (hashvalue.rotate_right(self.log2_nbuckets) & (nbatch - 1)) as i32,
            )
        } else {
            (hashvalue & (nbuckets - 1), 0)
        }
    }

    #[inline]
    pub fn total_tuples(&self) -> f64 {
        self.total_tuples
    }

    #[inline]
    pub fn bucket_head(&self, bucketno: u32) -> *mut HashJoinTupleHdr {
        self.buckets[bucketno as usize]
    }

    #[inline]
    pub fn nbuckets(&self) -> u32 {
        self.nbuckets
    }

    pub fn instrumentation(&self) -> HashInstrumentation {
        HashInstrumentation {
            nbuckets: self.nbuckets as i32,
            nbuckets_original: self.nbuckets_original as i32,
            nbatch: self.nbatch,
            nbatch_original: self.nbatch_original,
            space_peak: self.space_peak as u64,
        }
    }

    /// `ExecHashTableResetMatchFlags`.
    pub fn reset_match_flags(&mut self) {
        for &head in self.buckets.iter() {
            let mut cur = head;
            while !cur.is_null() {
                // SAFETY: chain headers/images live in the batch arena.
                unsafe {
                    (*HashJoinTupleHdr::mintuple(cur).as_ptr()).clear_match();
                    cur = (*cur).next;
                }
            }
        }
    }
}

/// `ExecHashJoinSaveTuple` (nodeHashjoin.c): hashvalue then the image.
pub fn save_tuple<'mcx>(
    fileptr: &mut Option<BufFile<'mcx>>,
    hashvalue: u32,
    tuple: &[u8],
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    if fileptr.is_none() {
        *fileptr = Some(::fd::buffile::BufFileCreateTemp(mcx, false)?);
    }
    let file = fileptr.as_mut().expect("batch file just ensured");
    file.write(&hashvalue.to_ne_bytes())?;
    file.write(tuple)?;
    Ok(())
}

/// `ExecHashJoinGetSavedTuple` (nodeHashjoin.c): None at EOF; the image in
/// `scratch` (u64-backed for MAXALIGN) is valid until the next call.
pub fn get_saved_tuple<'mcx>(
    file: &mut BufFile<'mcx>,
    scratch: &mut PgVec<'mcx, u64>,
) -> PgResult<Option<(u32, NonNull<MinimalTupleData>)>> {
    if init_small::globals::InterruptPending() {
        postgres_seams::check_for_interrupts::call()?;
    }
    let mut header = [0u8; 8];
    let nread = file.read_maybe_eof(&mut header, true)?;
    if nread == 0 {
        return Ok(None);
    }
    let hashvalue = u32::from_ne_bytes(header[0..4].try_into().unwrap());
    let t_len = u32::from_ne_bytes(header[4..8].try_into().unwrap());
    debug_assert!(t_len as usize >= SizeofMinimalTupleHeader);
    scratch.clear();
    scratch.resize((t_len as usize).div_ceil(8), 0);
    // SAFETY: u64 backing reinterpreted as bytes; length covers t_len.
    let image: &mut [u8] =
        unsafe { core::slice::from_raw_parts_mut(scratch.as_mut_ptr().cast(), t_len as usize) };
    image[0..4].copy_from_slice(&t_len.to_ne_bytes());
    file.read_exact(&mut image[4..])?;
    let ptr = NonNull::new(image.as_mut_ptr().cast::<MinimalTupleData>())
        .expect("scratch image is non-null");
    Ok(Some((hashvalue, ptr)))
}

pub struct HashState<'mcx> {
    hash_expr: PgBox<'mcx, ExprState<'mcx>>,
    pub table: Option<HashJoinTable<'mcx>>,
    pub hash_tuple_slot: ExecSlotId,
    pub ps_ExprContext: EcxtId,
    ntuples_est: f64,
    tupwidth: i32,
    #[allow(dead_code)]
    inner_desc: Option<Rc<TupleDescData<'static>>>,
}

impl<'mcx> HashState<'mcx> {
    /// Slot deform prefix the build-side hash reads per row (its FETCHSOME
    /// bound); None = shape unknown to the batch-deform planner.
    pub fn build_prefix(&self) -> Option<i32> {
        self.hash_expr.max_fetch(::execexpr::SlotSrc::Inner)
    }
}

/// `ExecInitHash`.
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
    let ps_ExprContext = estate.exec_assign_expr_context();

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
        ps_ExprContext,
        ntuples_est,
        tupwidth: child.plan_width,
        inner_desc: Some(inner_desc),
    })
}

/// `ExecHashTableCreate`; useskew mirrors C's OidIsValid(node->skewTable) for
/// these plan shapes (the skew table itself only forms from MCV stats).
pub fn exec_hash_table_create<'mcx>(
    hs: &HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<HashJoinTable<'mcx>> {
    let mcx = estate.es_query_cxt;
    let (nbuckets, nbatch, _num_skew_mcvs, space_allowed) =
        exec_choose_hash_table_size_full(hs.ntuples_est, hs.tupwidth, true);
    HashJoinTable::create(mcx, estate, nbuckets, nbatch, space_allowed)
}

#[inline(always)]
fn hash_insert_slot<'mcx>(
    hs: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot_id: ExecSlotId,
) -> PgResult<()> {
    estate.reset_expr_context(hs.ps_ExprContext);
    let hashvalue = {
        let slot = &mut estate.es_tupleTable[slot_id.0 as usize];
        let mut slots = EvalSlots { scan: None, inner: Some(slot), outer: None };
        let r = exec_eval_expr(&mut hs.hash_expr, &mut slots)?;
        // Non-strict fold keeps NULL-key tuples: they never match the
        // recheck, so results equal C for every jointype.
        r.value.as_u32()
    };
    let ecxt = hs.ps_ExprContext;
    let table = hs.table.as_mut().expect("hash table created");
    table.insert(estate, slot_id, ecxt, hashvalue)?;
    table.total_tuples += 1.0;
    Ok(())
}

/// `MultiExecHash`/`MultiExecPrivateHash`.
pub fn multi_exec_hash<'mcx, C: HashBuildInput<'mcx>>(
    hs: &mut HashState<'mcx>,
    child: &mut C,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    debug_assert!(hs.table.is_some(), "table created in HJ_BUILD_HASHTABLE");
    loop {
        let Some(slot_id) = child.exec_proc(estate)? else {
            break;
        };
        hash_insert_slot(hs, estate, slot_id)?;
    }
    hs.table.as_mut().expect("hash table created").finish_build(mcx)?;
    Ok(())
}

/// `MultiExecPrivateHash` over a page-batch source: identical per-row hash +
/// `ExecHashTableInsert` order (spill/growth arms included), minus the
/// per-tuple node recursion.
pub fn multi_exec_hash_batched<'mcx, S: HashBuildBatchSource<'mcx>>(
    hs: &mut HashState<'mcx>,
    mut src: S,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    debug_assert!(hs.table.is_some(), "table created in HJ_BUILD_HASHTABLE");
    let slot_id = src.slot();
    loop {
        let n = src.next_batch(estate)?;
        if n == 0 {
            break;
        }
        for i in 0..n {
            if !src.fetch_tuple(i, estate)? {
                continue;
            }
            hash_insert_slot(hs, estate, slot_id)?;
        }
    }
    hs.table.as_mut().expect("hash table created").finish_build(mcx)?;
    Ok(())
}

/// `ExecEndHash`: the table lives in the query arena (wholesale reset).
pub fn exec_end_hash(hs: &mut HashState<'_>) {
    hs.hash_expr.release_frames();
    hs.inner_desc = None;
}

// C constants (hashjoin.h / htup_details.h), 64-bit build.
pub const HJTUPLE_OVERHEAD: usize = 16; // MAXALIGN(sizeof(HashJoinTupleData))
const SIZEOF_HASHJOINTUPLE: usize = 8; // pointer
const NTUP_PER_BUCKET: f64 = 1.0;
const MAX_ALLOC_SIZE: usize = 0x3fff_ffff;
const SKEW_HASH_MEM_PERCENT: usize = 2;
const SKEW_BUCKET_OVERHEAD: usize = 16; // MAXALIGN(sizeof(HashSkewBucket))
const HASH_CHUNK_SIZE: usize = 32 * 1024;
const HASH_CHUNK_THRESHOLD: usize = HASH_CHUNK_SIZE / 4;
const BLCKSZ: usize = 8192;

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

/// `ExecChooseHashTableSize` -> (numbuckets, numbatches, num_skew_mcvs).
pub fn exec_choose_hash_table_size(ntuples: f64, tupwidth: i32, useskew: bool) -> (u32, i32, i32) {
    let (b, n, s, _) = exec_choose_hash_table_size_full(ntuples, tupwidth, useskew);
    (b, n, s)
}

/// As above plus `*space_allowed`.
pub fn exec_choose_hash_table_size_full(
    ntuples: f64,
    tupwidth: i32,
    useskew: bool,
) -> (u32, i32, i32, usize) {
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

    (nbuckets as u32, nbatch as i32, num_skew_mcvs as i32, space_allowed)
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
fn oom_tuples(mcx: Mcx<'_>, add: usize) -> Box<PgError> {
    Box::new(mcx.oom(add * core::mem::size_of::<NonNull<HashJoinTupleHdr>>()))
}

// Exempt: released in exec_end_hash_join/exec_end_hash — table (BufFile fds)
// is destroyed and taken there, hash_expr via release_frames, inner_desc taken.
mcx::forget_safe_struct!(
    HashState<'_> { hash_tuple_slot, ps_ExprContext, ntuples_est, tupwidth;
        table, hash_expr, inner_desc },
);
