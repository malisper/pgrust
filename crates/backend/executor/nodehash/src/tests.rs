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
    fn install_cfi_seam() {
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
