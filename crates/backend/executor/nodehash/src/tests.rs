use super::*;

use ::datum::Datum;
use ::mcx::MemoryContext;

fn with_mcx<R>(f: impl for<'m> FnOnce(Mcx<'m>) -> R) -> R {
    let ctx = MemoryContext::new("nodehash-test");
    f(ctx.mcx())
}

#[test]
fn probe_bloom_no_false_negatives() {
    with_mcx(|mcx| {
        let mut bf = ProbeBloom::new_in(mcx, 10_000.0);
        let mut h: u32 = 0x9e37_79b9;
        let mut inserted = Vec::new();
        for _ in 0..10_000 {
            h = h.wrapping_mul(0x0019_660d).wrapping_add(0x3c6e_f35f);
            bf.insert(h);
            inserted.push(h);
        }
        for h in inserted {
            assert!(bf.test(h));
        }
    });
}

#[test]
fn probe_bloom_rejects_and_density() {
    with_mcx(|mcx| {
        let mut bf = ProbeBloom::new_in(mcx, 1_000.0);
        for v in 0..1_000i32 {
            bf.insert(::hashfn::hash_bytes_uint32(v as u32));
        }
        assert!(bf.density() <= 0.25);
        let misses = (100_000..110_000i32)
            .filter(|v| !bf.test(::hashfn::hash_bytes_uint32(*v as u32)))
            .count();
        assert!(misses > 9_000, "filter admits too much: {misses} misses of 10000");
        let full = ProbeBloom {
            words: ::mcx::vec_from_elem_in(mcx, u64::MAX, 64),
            wmask: 63,
        };
        assert!(full.density() > 0.25);
    });
}

#[test]
fn sel_hash32_low32_matches_scalar_semantics() {
    with_mcx(|mcx| {
        let mut bf = ProbeBloom::new_in(mcx, 64.0);
        for v in [7i32, -3, 0, 123_456] {
            bf.insert(::hashfn::hash_bytes_uint32(v as u32));
        }
        let values: Vec<Datum> = (-8..120i32).map(Datum::from_i32).collect();
        let mut isnull = vec![false; values.len()];
        isnull[3] = true;
        let mut sel = [0u64; 4];
        bf.sel_hash32_low32(&values, &isnull, &mut sel);
        for (i, v) in (-8..120i32).enumerate() {
            let expect = if isnull[i] {
                bf.test(0)
            } else {
                bf.test(::hashfn::hash_bytes_uint32(v as u32))
            };
            let got = sel[i / 64] & (1u64 << (i % 64)) != 0;
            assert_eq!(got, expect, "row {i} value {v}");
        }
    });
}

#[test]
fn dense_chain_reverse_insertion_and_bounds() {
    let ctx = MemoryContext::new("nodehash-test");
    let mcx = ctx.mcx();
    let keys: [i64; 6] = [5, 7, 5, NULL_KEY, 6, 5];
    let min = 5i32;
    let range = 3usize;
    let mut heads: PgVec<'_, u32> = vec_with_capacity_in(mcx, range).unwrap();
    heads.resize(range, DENSE_END);
    let mut next: PgVec<'_, u32> = vec_with_capacity_in(mcx, keys.len()).unwrap();
    next.resize(keys.len(), DENSE_END);
    for (i, &k) in keys.iter().enumerate() {
        if k == NULL_KEY {
            continue;
        }
        let idx = (k - min as i64) as usize;
        next[i] = heads[idx];
        heads[idx] = i as u32;
    }
    let d = DenseTable { min, heads, next };
    assert_eq!(d.head_for(5), 5);
    assert_eq!(d.next(5), 2);
    assert_eq!(d.next(2), 0);
    assert_eq!(d.next(0), DENSE_END);
    assert_eq!(d.head_for(6), 4);
    assert_eq!(d.next(4), DENSE_END);
    assert_eq!(d.head_for(7), 1);
    assert_eq!(d.head_for(4), DENSE_END);
    assert_eq!(d.head_for(8), DENSE_END);
    assert_eq!(d.head_for(i32::MIN), DENSE_END);
    assert_eq!(d.head_for(i32::MAX), DENSE_END);
}

// ---------------------------------------------------------------------------
// audit-18.6 b148: the private-table growth walks are cancellable like C.
// nodeHash.c ExecHashIncreaseNumBatches (:1161) and ExecHashIncreaseNumBuckets
// (:1645) run CHECK_FOR_INTERRUPTS() inside their in-memory tuple walks; a
// pending cancel must surface as 57014 from inside the walk, not after it.
// ---------------------------------------------------------------------------

mod audit_b148 {
    use super::*;

    use std::sync::Once;

    use ::executils::EStateData;
    use ::mcx::MemoryContext;
    use ::types_error::ERRCODE_QUERY_CANCELED;
    use ::types_slot::TupleSlotKind;
    use ::types_tuple::{
        CompactAttribute, FormData_pg_attribute, TupleDescData, TYPALIGN_INT, TYPSTORAGE_PLAIN,
    };

    const INT4OID: u32 = 23;

    static CFI_SEAM: Once = Once::new();

    // CHECK_FOR_INTERRUPTS() -> ProcessInterrupts (postgres.c): the seam only
    // runs while InterruptPending is set; a pending cancel is C's 57014.
    // Shared with the skew rig (a seam installs once per test process).
    pub(super) fn install_cfi_seam() {
        CFI_SEAM.call_once(|| {
            postgres_seams::check_for_interrupts::set(|| {
                Err(Box::new(
                    PgError::error("canceling statement due to user request")
                        .with_sqlstate(ERRCODE_QUERY_CANCELED),
                ))
            });
        });
    }

    fn leaked_mcx() -> Mcx<'static> {
        let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("nodehash-b148")));
        m.mcx()
    }

    fn int4_desc(mcx: Mcx<'static>) -> Rc<TupleDescData<'static>> {
        let att = FormData_pg_attribute {
            attnum: 1,
            atttypid: INT4OID,
            attlen: 4,
            attbyval: true,
            attalign: TYPALIGN_INT,
            attstorage: TYPSTORAGE_PLAIN,
            ..Default::default()
        };
        let mut attrs = PgVec::new_in(mcx);
        let mut compact = PgVec::new_in(mcx);
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
        Rc::new(TupleDescData {
            natts: 1,
            tdtypeid: 2249,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs: compact,
            attrs,
        })
    }

    struct Rig {
        estate: EStateData<'static>,
        slot: ExecSlotId,
        ecxt: EcxtId,
        table: HashJoinTable<'static>,
    }

    // A single-batch private table (1024 buckets, a generous allowance so
    // inserts never spill) holding `n` int4 tuples whose hash values keep
    // every tuple in batch 0 under any later batch split.
    fn rig(n: u32) -> Rig {
        install_cfi_seam();
        // The batch split's PrepareTempTablespaces() consults the
        // temp_tablespaces GUC only while no list is set; an empty list is
        // C's default-GUC outcome and keeps the unit off the GUC plane.
        ::fd::temp::SetTempTablespaces(&[]);
        let mcx = leaked_mcx();
        let mut estate = EStateData::new_in(mcx);
        let slot = estate.exec_init_extra_tuple_slot(Some(int4_desc(mcx)), TupleSlotKind::Virtual);
        let ecxt = estate.exec_assign_expr_context();
        let mut table = HashJoinTable::create(mcx, &mut estate, 1024, 1, 1 << 20, None, None)
            .expect("hash table create");
        for i in 0..n {
            {
                let s = estate.slot_mut(slot);
                exectuples::exec_clear_tuple(s, mcx);
                let base = s.base_mut();
                base.tts_values[0] = Datum::from_i32(i as i32);
                base.tts_isnull[0] = false;
                exectuples::exec_store_virtual_tuple(s);
            }
            // low hash bits only: batchno = (hash >> log2_nbuckets) & (nbatch-1) = 0
            table.insert(&mut estate, slot, ecxt, i & 0x3ff).expect("insert");
            // MultiExecPrivateHash counts the tuple after ExecHashTableInsert.
            table.total_tuples += 1.0;
        }
        Rig { estate, slot, ecxt, table }
    }

    fn with_cancel_pending<R>(f: impl FnOnce() -> R) -> R {
        init_small::globals::SetInterruptPending(true);
        let r = f();
        init_small::globals::SetInterruptPending(false);
        r
    }

    #[test]
    fn increase_num_buckets_walk_is_cancellable() {
        let Rig { estate, table: mut t, .. } = rig(2048);
        let mcx = estate.es_query_cxt;
        // 2048 tuples over 1024 buckets: the insert path doubled the optimal
        // bucket count, so the rebucketing walk has work to do.
        assert!(t.nbuckets_optimal > t.nbuckets, "rig precondition: growth pending");
        let err = with_cancel_pending(|| t.increase_num_buckets(mcx))
            .expect_err("ExecHashIncreaseNumBuckets must honour a pending cancel (nodeHash.c:1645)");
        assert_eq!(err.sqlstate(), ERRCODE_QUERY_CANCELED);
    }

    #[test]
    fn increase_num_batches_walk_is_cancellable() {
        let Rig { estate, table: mut t, .. } = rig(256);
        let mcx = estate.es_query_cxt;
        assert!(t.grow_enabled && t.nbatch == 1, "rig precondition: batch growth allowed");
        let err = with_cancel_pending(|| t.increase_num_batches(mcx))
            .expect_err("ExecHashIncreaseNumBatches must honour a pending cancel (nodeHash.c:1161)");
        assert_eq!(err.sqlstate(), ERRCODE_QUERY_CANCELED);
    }

    #[test]
    fn growth_walks_complete_without_a_pending_interrupt() {
        let Rig { estate, table: mut t, slot: _, ecxt: _ } = rig(2048);
        let mcx = estate.es_query_cxt;
        t.increase_num_buckets(mcx).expect("no interrupt pending: rebucket completes");
        assert_eq!(t.nbuckets, t.nbuckets_optimal);
        t.increase_num_batches(mcx).expect("no interrupt pending: batch split completes");
        assert_eq!(t.nbatch, 2);
        // every tuple kept batch 0, so C disables further growth (nfreed == 0)
        assert!(!t.grow_enabled);
        assert_eq!(t.tuples.len(), 2048);
    }
}

// ---------------------------------------------------------------------------
// audit-18.6 w2-011 (row nodeHash-p2-915e64a2): the skew optimization plane
// — ExecHashBuildSkewHash's bucket table (nodeHash.c:2467), ExecHashGetSkewBucket
// (:2555), ExecHashSkewTableInsert (:2601) and ExecHashRemoveNextSkewBucket
// (:2647) — over a two-batch private table.
// ---------------------------------------------------------------------------

mod rem_w2_011_skew {
    use super::*;

    use ::executils::EStateData;
    use ::mcx::MemoryContext;
    use ::types_error::ERRCODE_QUERY_CANCELED;
    use ::types_slot::TupleSlotKind;
    use ::types_tuple::{
        CompactAttribute, FormData_pg_attribute, TupleDescData, TYPALIGN_INT, TYPSTORAGE_PLAIN,
    };

    const INT4OID: u32 = 23;
    // nbuckets 1024 -> log2 10; two batches -> batchno = bit 10 of the hash.
    const NBUCKETS: u32 = 1024;
    const BATCH1: u32 = 1 << 10;

    // The audit_b148 rig's CHECK_FOR_INTERRUPTS seam (57014 while
    // InterruptPending is set); a seam installs once per test process.
    fn install_cfi_seam() {
        super::audit_b148::install_cfi_seam();
    }

    fn leaked_mcx() -> Mcx<'static> {
        let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("nodehash-skew")));
        m.mcx()
    }

    fn int4_desc(mcx: Mcx<'static>) -> Rc<TupleDescData<'static>> {
        let att = FormData_pg_attribute {
            attnum: 1,
            atttypid: INT4OID,
            attlen: 4,
            attbyval: true,
            attalign: TYPALIGN_INT,
            attstorage: TYPSTORAGE_PLAIN,
            ..Default::default()
        };
        let mut attrs = PgVec::new_in(mcx);
        let mut compact = PgVec::new_in(mcx);
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
        Rc::new(TupleDescData {
            natts: 1,
            tdtypeid: 2249,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs: compact,
            attrs,
        })
    }

    struct Rig {
        estate: EStateData<'static>,
        slot: ExecSlotId,
        ecxt: EcxtId,
        table: HashJoinTable<'static>,
    }

    // A two-batch private table with `space_allowed` bytes (the skew plane
    // gets SKEW_HASH_MEM_PERCENT of it) and no tuples yet.
    fn rig(space_allowed: usize) -> Rig {
        install_cfi_seam();
        ::fd::temp::SetTempTablespaces(&[]);
        let mcx = leaked_mcx();
        let mut estate = EStateData::new_in(mcx);
        let slot = estate.exec_init_extra_tuple_slot(Some(int4_desc(mcx)), TupleSlotKind::Virtual);
        let ecxt = estate.exec_assign_expr_context();
        let table = HashJoinTable::create(mcx, &mut estate, NBUCKETS, 2, space_allowed, None, None)
            .expect("hash table create");
        Rig { estate, slot, ecxt, table }
    }

    // MultiExecPrivateHash's per-tuple arm (nodeHash.c:181-195) for one int4
    // build row with hash value `h`.
    fn build_row(r: &mut Rig, v: i32, h: u32) {
        let mcx = r.estate.es_query_cxt;
        {
            let s = r.estate.slot_mut(r.slot);
            exectuples::exec_clear_tuple(s, mcx);
            let base = s.base_mut();
            base.tts_values[0] = Datum::from_i32(v);
            base.tts_isnull[0] = false;
            exectuples::exec_store_virtual_tuple(s);
        }
        let bucket = r.table.get_skew_bucket(h);
        if bucket != INVALID_SKEW_BUCKET_NO {
            r.table.skew_table_insert(&mut r.estate, r.slot, h, bucket).expect("skew insert");
            r.table.skew_tuples += 1.0;
        } else {
            r.table.insert(&mut r.estate, r.slot, r.ecxt, h).expect("insert");
        }
        r.table.total_tuples += 1.0;
    }

    fn chain_values(mut cur: *mut HashJoinTupleHdr) -> Vec<(u32, i32)> {
        let mut out = Vec::new();
        while !cur.is_null() {
            // SAFETY: chain headers/images live in the batch arena; the int4
            // payload sits at t_hoff - MINIMAL_TUPLE_OFFSET (t_hoff counts
            // from the full heap-tuple header the minimal form drops).
            unsafe {
                let mt = HashJoinTupleHdr::mintuple(cur).as_ptr();
                let hoff = (*mt).t_hoff as usize - ::types_tuple::htup::MINIMAL_TUPLE_OFFSET;
                let v = *(mt.cast::<u8>().add(hoff).cast::<i32>());
                out.push(((*cur).hashvalue(), v));
                cur = (*cur).next();
            }
        }
        out
    }

    #[test]
    fn skew_buckets_are_created_in_mcv_order_and_probed_by_open_addressing() {
        let mut r = rig(1 << 20);
        let mcx = r.estate.es_query_cxt;
        // Three MCV hash values, two of which collide on the low bits.
        let h_a = 0x11;
        let h_b = 0x24;
        let h_c = 0x11 | (1 << 8); // same low bits as h_a -> linear probe
        r.table.build_skew_buckets(mcx, &[h_a, h_b, h_c]).unwrap();
        assert!(r.table.skew_enabled());
        // nextpower2(3 + 1) << 2 = 16 slots.
        assert_eq!(r.table.skew_bucket.len(), 16);
        assert_eq!(r.table.n_skew_buckets(), 3);
        assert_eq!(r.table.get_skew_bucket(h_a), (h_a & 15) as i32);
        assert_eq!(r.table.get_skew_bucket(h_b), (h_b & 15) as i32);
        assert_eq!(r.table.get_skew_bucket(h_c), ((h_a & 15) + 1) as i32, "probe past the collision");
        assert_eq!(r.table.get_skew_bucket(0x33), INVALID_SKEW_BUCKET_NO);
        // skewBucketNums lists them in creation (MCV frequency) order.
        assert_eq!(
            (0..3).map(|i| r.table.skew_bucket_num(i)).collect::<Vec<_>>(),
            vec![(h_a & 15) as i32, (h_b & 15) as i32, ((h_a & 15) + 1) as i32]
        );
        // Space accounting: 16 pointers + 3 ints + 3 bucket structs.
        let expect = 16 * 8 + 3 * 4 + 3 * SKEW_BUCKET_OVERHEAD;
        assert_eq!(r.table.space_used, expect);
        assert_eq!(r.table.space_used_skew, expect);
        assert_eq!(r.table.space_peak, expect);
        // A duplicate MCV hash value shares its bucket.
        let mut r2 = rig(1 << 20);
        r2.table.build_skew_buckets(mcx, &[h_a, h_a]).unwrap();
        assert_eq!(r2.table.n_skew_buckets(), 1);
    }

    #[test]
    fn mcv_build_rows_land_in_their_skew_bucket_newest_first() {
        let mut r = rig(1 << 20);
        let mcx = r.estate.es_query_cxt;
        let h_mcv = 0x5 | BATCH1; // an MCV whose main-table batch is 1
        r.table.build_skew_buckets(mcx, &[h_mcv]).unwrap();
        build_row(&mut r, 1, h_mcv);
        build_row(&mut r, 2, h_mcv);
        build_row(&mut r, 3, 0x7); // not an MCV: main table, batch 0
        assert_eq!(r.table.skew_tuples(), 2.0);
        assert_eq!(r.table.total_tuples(), 3.0);
        let b = r.table.get_skew_bucket(h_mcv);
        // Batch-1 rows served from the skew bucket in batch 0, newest first.
        assert_eq!(chain_values(r.table.skew_bucket_head(b)), vec![(h_mcv, 2), (h_mcv, 1)]);
        // Skew tuples are not in the dense list nor any main bucket.
        assert_eq!(r.table.tuples.len(), 1);
        assert_eq!(chain_values(r.table.bucket_head(0x7)), vec![(0x7, 3)]);
        assert!(r.table.inner_batch_file[1].is_none());
        // Batch-0 probes served by the skew plane: nbuckets_optimal grows from
        // main-table rows only (ntuples = totalTuples - skewTuples).
        assert_eq!(r.table.nbuckets_optimal, NBUCKETS);
    }

    #[test]
    fn skew_space_overflow_evicts_least_common_bucket_into_the_main_table() {
        // spaceAllowedSkew = 2% of 64kB = 1310 bytes: the arrays + a few
        // dozen ~40-byte tuples fit, then the least common bucket goes.
        // (Both MCVs hash to batch 0: the batch-file eviction arm needs a
        // temp tablespace and is exercised by the e2e corpus.)
        let mut r = rig(64 * 1024);
        let mcx = r.estate.es_query_cxt;
        let h_first = 0x3; // most common MCV
        let h_last = 0x6; // least common MCV
        r.table.build_skew_buckets(mcx, &[h_first, h_last]).unwrap();
        let allowed = r.table.space_allowed_skew;
        assert_eq!(allowed, 64 * 1024 * 2 / 100);
        let mut n = 0;
        let mut n_last = 0;
        while r.table.n_skew_buckets() == 2 {
            if n % 2 == 0 {
                build_row(&mut r, n, h_first);
            } else {
                build_row(&mut r, n, h_last);
                n_last += 1;
            }
            n += 1;
            assert!(n < 100, "skew space never overflowed");
        }
        // The LAST-created bucket (least common MCV) went first: its rows
        // moved into the main table's bucket chain (dense list included) and
        // the bucket's space left the skew account.
        assert_eq!(r.table.n_skew_buckets(), 1);
        assert_eq!(r.table.get_skew_bucket(h_last), INVALID_SKEW_BUCKET_NO);
        assert_ne!(r.table.get_skew_bucket(h_first), INVALID_SKEW_BUCKET_NO);
        let (last_bucketno, _) = r.table.get_bucket_and_batch(h_last);
        let last_rows = chain_values(r.table.bucket_head(last_bucketno));
        assert_eq!(last_rows.len(), n_last);
        assert!(last_rows.iter().all(|(h, _)| *h == h_last));
        assert_eq!(r.table.tuples.len(), n_last);
        assert!(r.table.space_used_skew <= allowed);
        assert!(r.table.skew_enabled());
        // Keep going: the remaining bucket's rows are batch-0 rows, so its
        // eviction moves them into the main table's bucket chain (dense
        // list included) and disables the optimization.
        while r.table.skew_enabled() {
            build_row(&mut r, n, h_first);
            n += 1;
            assert!(n < 200, "skew plane never drained");
        }
        assert_eq!(r.table.n_skew_buckets(), 0);
        assert_eq!(r.table.space_used_skew, 0);
        assert!(r.table.skew_bucket.is_empty() && r.table.skew_bucket_nums.is_empty());
        let (bucketno, batchno) = r.table.get_bucket_and_batch(h_first);
        assert_eq!(batchno, 0);
        let moved = chain_values(r.table.bucket_head(bucketno));
        assert!(moved.len() >= 2 && moved.iter().all(|(h, _)| *h == h_first));
        assert_eq!(r.table.tuples.len(), moved.len() + n_last);
        // Later rows with that hash value take the ordinary insert path.
        build_row(&mut r, 999, h_first);
        assert_eq!(chain_values(r.table.bucket_head(bucketno))[0], (h_first, 999));
        // The space accounting stays exact: every live main-table tuple plus
        // nothing for the retired skew plane.
        let live: usize = r.table.tuples.iter().map(|&h| HashJoinTable::tuple_size(h)).sum();
        assert_eq!(r.table.space_used, live);
    }

    #[test]
    fn eviction_walk_is_cancellable_and_match_flags_cover_skew_chains() {
        let mut r = rig(64 * 1024);
        let mcx = r.estate.es_query_cxt;
        let h = 0x4;
        r.table.build_skew_buckets(mcx, &[h]).unwrap();
        build_row(&mut r, 1, h);
        build_row(&mut r, 2, h);
        // ExecHashTableResetMatchFlags walks skew chains too (nodeHash.c:2377).
        let head = r.table.skew_bucket_head(r.table.get_skew_bucket(h));
        unsafe { (*HashJoinTupleHdr::mintuple(head).as_ptr()).set_match() };
        r.table.reset_match_flags();
        assert!(unsafe { !(*HashJoinTupleHdr::mintuple(head).as_ptr()).has_match() });
        // nodeHash.c:2717: a pending cancel surfaces from inside the eviction.
        init_small::globals::SetInterruptPending(true);
        let err = r.table.remove_next_skew_bucket(mcx).expect_err("cancellable eviction");
        init_small::globals::SetInterruptPending(false);
        assert_eq!(err.sqlstate(), ERRCODE_QUERY_CANCELED);
    }

    #[test]
    fn first_batch_end_retires_the_skew_plane() {
        let mut r = rig(1 << 20);
        let mcx = r.estate.es_query_cxt;
        r.table.build_skew_buckets(mcx, &[0x1, 0x2]).unwrap();
        build_row(&mut r, 1, 0x1);
        r.table.end_skew_after_first_batch();
        assert!(!r.table.skew_enabled());
        assert_eq!(r.table.n_skew_buckets(), 0);
        assert_eq!(r.table.get_skew_bucket(0x1), INVALID_SKEW_BUCKET_NO);
        assert_eq!(r.table.space_used_skew, 0);
    }
}
