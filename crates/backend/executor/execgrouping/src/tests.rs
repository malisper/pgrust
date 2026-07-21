use std::rc::Rc;
use std::sync::Once;

use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_core::INT4OID;
use ::types_slot::TupleSlotKind;
use ::types_tuple::{CompactAttribute, FormData_pg_attribute, TupleDescData, TYPALIGN_INT, TYPSTORAGE_PLAIN};

use crate::build_tuple_hash_table;

static SEAMS: Once = Once::new();

fn install() {
    SEAMS.call_once(|| {
        miscinit_seams::get_user_id::set(|| 10);
        aclchk_seams::object_aclcheck::set(|_, _, _, _| Ok(0));
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
    });
}

fn one_int4_desc(mcx: Mcx<'_>) -> Rc<TupleDescData<'_>> {
    let att = FormData_pg_attribute {
        attnum: 1,
        atttypid: INT4OID,
        atttypmod: -1,
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
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

#[test]
fn lookup_groups_and_isolates_keys() {
    install();
    let ctx = MemoryContext::new("execgrouping-test");
    let mcx = ctx.mcx();
    let table_ctx = MemoryContext::new("entries");
    let desc = one_int4_desc(mcx);
    // hashint4 (450) / int4eq (65) passed as resolved oids, as nodeAgg does.
    let mut table = build_tuple_hash_table(
        mcx,
        &desc,
        &[1],
        &[65],
        &[450],
        &[0],
        16,
        16,
        false,
    )
    .unwrap();
    let mut slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc));

    fn put<'mcx>(
        table: &mut crate::TupleHashTable<'mcx>,
        slot: &mut ::types_slot::SlotData<'mcx>,
        table_ctx: &MemoryContext,
        mcx: Mcx<'mcx>,
        v: i32,
        is_null: bool,
    ) -> (u32, bool) {
        exectuples::exec_clear_tuple(slot, mcx);
        slot.base_mut().tts_values[0] = Datum::from_i32(v);
        slot.base_mut().tts_isnull[0] = is_null;
        exectuples::exec_store_virtual_tuple(slot);
        let hash = table.hash_slot(slot).unwrap();
        let (ix, isnew) = table.lookup(slot, hash, Some(table_ctx.mcx()), mcx).unwrap();
        (ix.unwrap(), isnew)
    }

    let (i1, new1) = put(&mut table, &mut slot, &table_ctx, mcx, 7, false);
    let (i2, new2) = put(&mut table, &mut slot, &table_ctx, mcx, 8, false);
    let (i3, new3) = put(&mut table, &mut slot, &table_ctx, mcx, 7, false);
    let (i4, new4) = put(&mut table, &mut slot, &table_ctx, mcx, 0, true);
    let (i5, new5) = put(&mut table, &mut slot, &table_ctx, mcx, 0, true);
    assert!(new1 && new2 && !new3 && new4 && !new5);
    assert_eq!(i1, i3);
    assert_ne!(i1, i2);
    assert_eq!(i4, i5, "NULL keys are NOT DISTINCT");
    assert_eq!(table.num_entries(), 3);

    // The additional block precedes the stored tuple, zeroed.
    let add = table.entry_additional(i1).unwrap();
    // SAFETY: 16 zeroed additional bytes per entry (build arg above).
    let bytes = unsafe { core::slice::from_raw_parts(add.as_ptr(), 16) };
    assert_eq!(bytes, &[0u8; 16]);

    table.reset();
    assert_eq!(table.num_entries(), 0);
    let (_, renew) = put(&mut table, &mut slot, &table_ctx, mcx, 7, false);
    assert!(renew);
}

// Hashed DISTINCT: no per-group transition state, additionalsize 0.
#[test]
fn lookup_zero_additionalsize() {
    install();
    let ctx = MemoryContext::new("execgrouping-test0");
    let mcx = ctx.mcx();
    let table_ctx = MemoryContext::new("entries0");
    let desc = one_int4_desc(mcx);
    let mut table =
        build_tuple_hash_table(mcx, &desc, &[1], &[65], &[450], &[0], 16, 0, false).unwrap();
    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc));

    fn put<'mcx>(
        table: &mut crate::TupleHashTable<'mcx>,
        slot: &mut ::types_slot::SlotData<'mcx>,
        table_ctx: &MemoryContext,
        mcx: Mcx<'mcx>,
        v: i32,
    ) -> (u32, bool) {
        exectuples::exec_clear_tuple(slot, mcx);
        slot.base_mut().tts_values[0] = Datum::from_i32(v);
        slot.base_mut().tts_isnull[0] = false;
        exectuples::exec_store_virtual_tuple(slot);
        let hash = table.hash_slot(slot).unwrap();
        let (ix, isnew) = table.lookup(slot, hash, Some(table_ctx.mcx()), mcx).unwrap();
        (ix.unwrap(), isnew)
    }

    let (i1, new1) = put(&mut table, &mut slot, &table_ctx, mcx, 5);
    let (i2, new2) = put(&mut table, &mut slot, &table_ctx, mcx, 6);
    let (i3, new3) = put(&mut table, &mut slot, &table_ctx, mcx, 5);
    assert!(new1 && new2 && !new3);
    assert_eq!(i1, i3);
    assert_ne!(i1, i2);
    assert_eq!(table.num_entries(), 2);
    assert!(table.entry_additional(i1).is_none());
    assert!(table.entry_additional(i2).is_none());
}

fn two_int4_desc(mcx: Mcx<'_>) -> Rc<TupleDescData<'_>> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for attnum in 1..=2 {
        let att = FormData_pg_attribute {
            attnum,
            atttypid: INT4OID,
            atttypmod: -1,
            attlen: 4,
            attbyval: true,
            attalign: TYPALIGN_INT,
            attstorage: TYPSTORAGE_PLAIN,
            ..Default::default()
        };
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
    }
    Rc::new(TupleDescData {
        natts: 2,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

// Multi-column keys take the compiled-program path, not the probe kernel.
#[test]
fn lookup_two_col_expr_path() {
    install();
    let ctx = MemoryContext::new("execgrouping-test2");
    let mcx = ctx.mcx();
    let table_ctx = MemoryContext::new("entries2");
    let desc = two_int4_desc(mcx);
    let mut table = build_tuple_hash_table(
        mcx,
        &desc,
        &[1, 2],
        &[65, 65],
        &[450, 450],
        &[0, 0],
        16,
        16,
        false,
    )
    .unwrap();
    assert!(matches!(table.kernel, crate::ProbeKernel::Expr));
    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc));

    fn put<'mcx>(
        table: &mut crate::TupleHashTable<'mcx>,
        slot: &mut ::types_slot::SlotData<'mcx>,
        table_ctx: &MemoryContext,
        mcx: Mcx<'mcx>,
        a: i32,
        b: i32,
        bn: bool,
    ) -> (u32, bool) {
        exectuples::exec_clear_tuple(slot, mcx);
        slot.base_mut().tts_values[0] = Datum::from_i32(a);
        slot.base_mut().tts_isnull[0] = false;
        slot.base_mut().tts_values[1] = Datum::from_i32(b);
        slot.base_mut().tts_isnull[1] = bn;
        exectuples::exec_store_virtual_tuple(slot);
        let hash = table.hash_slot(slot).unwrap();
        let (ix, isnew) = table.lookup(slot, hash, Some(table_ctx.mcx()), mcx).unwrap();
        (ix.unwrap(), isnew)
    }

    let (i1, new1) = put(&mut table, &mut slot, &table_ctx, mcx, 1, 2, false);
    let (i2, new2) = put(&mut table, &mut slot, &table_ctx, mcx, 1, 3, false);
    let (i3, new3) = put(&mut table, &mut slot, &table_ctx, mcx, 1, 2, false);
    let (i4, new4) = put(&mut table, &mut slot, &table_ctx, mcx, 1, 0, true);
    let (i5, new5) = put(&mut table, &mut slot, &table_ctx, mcx, 1, 0, true);
    assert!(new1 && new2 && !new3 && new4 && !new5);
    assert_eq!(i1, i3);
    assert_ne!(i1, i2);
    assert_eq!(i4, i5);
    assert_eq!(table.num_entries(), 3);
}

#[test]
fn lookup_int8_kernel() {
    use ::types_core::INT8OID;
    use ::types_tuple::TYPALIGN_DOUBLE;
    install();
    let ctx = MemoryContext::new("execgrouping-test8");
    let mcx = ctx.mcx();
    let table_ctx = MemoryContext::new("entries8");
    let att = FormData_pg_attribute {
        attnum: 1,
        atttypid: INT8OID,
        atttypmod: -1,
        attlen: 8,
        attbyval: true,
        attalign: TYPALIGN_DOUBLE,
        attstorage: TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    compact.push(CompactAttribute::populate_from(&att));
    attrs.push(att);
    let desc = Rc::new(TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    });
    // hashint8 (949) / int8eq (467).
    let mut table =
        build_tuple_hash_table(mcx, &desc, &[1], &[467], &[949], &[0], 16, 16, false).unwrap();
    assert!(matches!(table.kernel, crate::ProbeKernel::Int8 { .. }));
    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc));

    fn put<'mcx>(
        table: &mut crate::TupleHashTable<'mcx>,
        slot: &mut ::types_slot::SlotData<'mcx>,
        table_ctx: &MemoryContext,
        mcx: Mcx<'mcx>,
        v: i64,
        isnull: bool,
    ) -> (u32, bool) {
        exectuples::exec_clear_tuple(slot, mcx);
        slot.base_mut().tts_values[0] = Datum::from_i64(v);
        slot.base_mut().tts_isnull[0] = isnull;
        exectuples::exec_store_virtual_tuple(slot);
        let hash = table.hash_slot(slot).unwrap();
        let (ix, isnew) = table.lookup(slot, hash, Some(table_ctx.mcx()), mcx).unwrap();
        (ix.unwrap(), isnew)
    }

    let (i1, new1) = put(&mut table, &mut slot, &table_ctx, mcx, i64::MAX - 1, false);
    let (i2, new2) = put(&mut table, &mut slot, &table_ctx, mcx, -42, false);
    let (i3, new3) = put(&mut table, &mut slot, &table_ctx, mcx, i64::MAX - 1, false);
    let (i4, new4) = put(&mut table, &mut slot, &table_ctx, mcx, 0, true);
    let (i5, new5) = put(&mut table, &mut slot, &table_ctx, mcx, 0, true);
    assert!(new1 && new2 && !new3 && new4 && !new5);
    assert_eq!(i1, i3);
    assert_ne!(i1, i2);
    assert_eq!(i4, i5);
    assert_eq!(table.num_entries(), 3);
}

// q18fin diagnostic (fix/parallel-finalize-stall): the 15M-group finalize
// does 15M all-miss int8 inserts through lookup() at ~19us each while the
// serial agg's identical call site pays ~140ns. This probe isolates the
// variable: N distinct int8 keys inserted (a) in ascending arrival order
// (serial scan shape) vs (b) in shuffled arrival order (Gather-funnel shape),
// same keys, same hashes, same growth trajectory. Run explicitly:
//   cargo test -p execgrouping --release -- --ignored --nocapture probe_cost
#[test]
#[ignore]
fn probe_cost_insert_order() {
    install();
    let n: usize = std::env::var("PROBE_N").ok().and_then(|v| v.parse().ok()).unwrap_or(4_000_000);
    let init_buckets: usize =
        std::env::var("PROBE_BUCKETS").ok().and_then(|v| v.parse().ok()).unwrap_or(250_000);

    // Deterministic shuffle (splitmix-based Fisher-Yates).
    let mut keys: Vec<i64> = (1..=n as i64).collect();
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut rng = move || {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    let mut shuffled = keys.clone();
    for i in (1..shuffled.len()).rev() {
        let j = (rng() % (i as u64 + 1)) as usize;
        shuffled.swap(i, j);
    }

    let mut run = |label: &str, order: &[i64]| {
        let ctx = MemoryContext::new("probe-cost");
        let mcx = ctx.mcx();
        let table_ctx = MemoryContext::new("probe-entries");
        let att = FormData_pg_attribute {
            attnum: 1,
            atttypid: ::types_core::INT8OID,
            atttypmod: -1,
            attlen: 8,
            attbyval: true,
            attalign: ::types_tuple::TYPALIGN_DOUBLE,
            attstorage: TYPSTORAGE_PLAIN,
            ..Default::default()
        };
        let mut attrs = PgVec::new_in(mcx);
        let mut compact = PgVec::new_in(mcx);
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
        let desc = Rc::new(TupleDescData {
            natts: 1,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs: compact,
            attrs,
        });
        let mut table = build_tuple_hash_table(
            mcx, &desc, &[1], &[467], &[949], &[0], init_buckets, 16, false,
        )
        .unwrap();
        let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc));
        let t0 = std::time::Instant::now();
        for &v in order {
            exectuples::exec_clear_tuple(&mut slot, mcx);
            slot.base_mut().tts_values[0] = Datum::from_i64(v);
            slot.base_mut().tts_isnull[0] = false;
            exectuples::exec_store_virtual_tuple(&mut slot);
            let hash = table.hash_slot(&mut slot).unwrap();
            let (ix, _isnew) = table.lookup(&mut slot, hash, Some(table_ctx.mcx()), mcx).unwrap();
            ix.unwrap();
        }
        let el = t0.elapsed();
        eprintln!(
            "probe_cost[{label}]: n={} init_buckets={} wall={:.3}s ns/insert={:.0} entries={}",
            order.len(),
            init_buckets,
            el.as_secs_f64(),
            el.as_nanos() as f64 / order.len() as f64,
            table.num_entries()
        );
        assert!(table.num_entries() <= order.len());
        table.release();
    };

    run("ascending", &keys);
    run("shuffled", &shuffled);
    // Serial-scan facsimile: 4 consecutive rows per key (3 hits per insert).
    let mut serial_like: Vec<i64> = Vec::with_capacity(n * 4);
    for &k in &keys {
        serial_like.extend_from_slice(&[k, k, k, k]);
    }
    keys.clear();
    run("ascending-x4", &serial_like);
}

// t26 merge-1 revert regression (staged-hash IV parity): tables built with
// use_variable_hash_iv fold a per-participant IV into every kernel hash.
// hash_staged is contractually bit-identical to hash_slot (nodeagg's staged
// probe debug-asserts exactly that; a release-build mismatch means
// find_staged misses live groups = silent duplicate groups). The uninstalled
// parallel_worker_number seam makes the IV deterministic here: worker -1 =>
// IV = murmurhash32(u32::MAX) != 0.
#[test]
fn staged_hash_matches_slot_under_variable_iv() {
    install();
    let ctx = MemoryContext::new("execgrouping-iv");
    let mcx = ctx.mcx();
    let desc = one_int4_desc(mcx);

    let hash_both = |variable_iv: bool| -> (Vec<u32>, Vec<u32>) {
        // hashint4 (450) / int4eq (65), as nodeAgg passes them.
        let mut table = build_tuple_hash_table(
            mcx,
            &desc,
            &[1],
            &[65],
            &[450],
            &[0],
            16,
            16,
            variable_iv,
        )
        .unwrap();
        assert!(matches!(table.kernel, crate::ProbeKernel::Int4 { .. }));
        let mut slot =
            exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc.clone()));
        let keys = [Datum::from_i32(0), Datum::from_i32(1), Datum::from_i32(-7), Datum::from_i32(i32::MAX)];
        let isnull = [false, false, false, true];
        let mut slot_hashes = Vec::new();
        for (&k, &n) in keys.iter().zip(&isnull) {
            exectuples::exec_clear_tuple(&mut slot, mcx);
            slot.base_mut().tts_values[0] = k;
            slot.base_mut().tts_isnull[0] = n;
            exectuples::exec_store_virtual_tuple(&mut slot);
            slot_hashes.push(table.hash_slot(&mut slot).unwrap());
        }
        let mut staged_hashes = Vec::new();
        table.hash_staged(&keys, &isnull, &mut staged_hashes).unwrap();
        (slot_hashes, staged_hashes)
    };

    let (slot0, staged0) = hash_both(false);
    assert_eq!(slot0, staged0, "IV=0 staged/slot parity");
    let (slot1, staged1) = hash_both(true);
    assert_eq!(slot1, staged1, "variable-IV staged/slot parity (t26 merge-1 revert class)");
    assert_ne!(slot0, slot1, "the variable IV must actually engage the kernels");
}

// q18fin r3 (t26 "q18fin-t26-r2 re-earn verdict" defect): the byref finalize
// merge compares STORED hashes across participant tables and the finalize's
// IV=0 table, so the handoff export rebases handed hashes onto the IV=0
// mapping through `hash_to_iv0`. This unit pins the rebase algebra: for any
// participant IV, hash_to_iv0(variable-IV hash) must equal the IV=0 twin's
// hash for the same key — on the word kernels (rot(iv,1) pre-fmix), on the
// staged kernels (bit-identical to hash_slot), and on the multi-column Expr
// arm (the IV rides the hash chain as rot(iv, ncols) pre-fmix).
#[test]
fn hash_to_iv0_rebases_variable_iv_onto_leader_mapping() {
    install();
    let ctx = MemoryContext::new("execgrouping-iv0");
    let mcx = ctx.mcx();

    // Participant IVs as production derives them: leader -1, workers 0..
    // (worker 0 is C's quirk — murmurhash32(0) == 0, an IV-0 participant).
    let ivs: [u32; 4] = [
        ::hashfn::murmurhash32(-1i32 as u32),
        ::hashfn::murmurhash32(0),
        ::hashfn::murmurhash32(1),
        ::hashfn::murmurhash32(7),
    ];

    // Word kernel (single int4) + staged lane.
    {
        let desc = one_int4_desc(mcx);
        let keys = [Datum::from_i32(6), Datum::from_i32(0), Datum::from_i32(-7)];
        let isnull = [false, false, true];
        let hash_all = |iv: u32| -> (Vec<u32>, Vec<u32>) {
            let mut table = crate::build_tuple_hash_table_with_iv(
                mcx, &desc, &[1], &[65], &[450], &[0], 16, 16, iv,
            )
            .unwrap();
            let mut slot = exectuples::make_tuple_table_slot(
                mcx,
                TupleSlotKind::Virtual,
                Some(desc.clone()),
            );
            let mut slot_hashes = Vec::new();
            for (&k, &n) in keys.iter().zip(&isnull) {
                exectuples::exec_clear_tuple(&mut slot, mcx);
                slot.base_mut().tts_values[0] = k;
                slot.base_mut().tts_isnull[0] = n;
                exectuples::exec_store_virtual_tuple(&mut slot);
                let h = table.hash_slot(&mut slot).unwrap();
                slot_hashes.push(table.hash_to_iv0(h));
            }
            let mut staged = Vec::new();
            table.hash_staged(&keys, &isnull, &mut staged).unwrap();
            let staged_rebased = staged.iter().map(|&h| table.hash_to_iv0(h)).collect();
            (slot_hashes, staged_rebased)
        };
        let (base, base_staged) = hash_all(0);
        assert_eq!(base, base_staged);
        for iv in ivs {
            let (rebased, rebased_staged) = hash_all(iv);
            assert_eq!(rebased, base, "word-kernel rebase (iv={iv:#x})");
            assert_eq!(rebased_staged, base, "staged rebase (iv={iv:#x})");
        }
    }

    // Expr arm (two int4 key columns — multi-col shapes ARE byref-admitted).
    {
        let desc = two_int4_desc(mcx);
        let keyset: [((i32, bool), (i32, bool)); 4] = [
            ((6, false), (42, false)),
            ((0, false), (0, false)),
            ((123, false), (0, true)),
            ((0, true), (0, true)),
        ];
        let hash_all = |iv: u32| -> Vec<u32> {
            let mut table = crate::build_tuple_hash_table_with_iv(
                mcx,
                &desc,
                &[1, 2],
                &[65, 65],
                &[450, 450],
                &[0, 0],
                16,
                16,
                iv,
            )
            .unwrap();
            assert!(matches!(table.kernel, crate::ProbeKernel::Expr));
            let mut slot = exectuples::make_tuple_table_slot(
                mcx,
                TupleSlotKind::Virtual,
                Some(desc.clone()),
            );
            let mut hashes = Vec::new();
            for ((v1, n1), (v2, n2)) in keyset {
                exectuples::exec_clear_tuple(&mut slot, mcx);
                slot.base_mut().tts_values[0] = Datum::from_i32(v1);
                slot.base_mut().tts_isnull[0] = n1;
                slot.base_mut().tts_values[1] = Datum::from_i32(v2);
                slot.base_mut().tts_isnull[1] = n2;
                exectuples::exec_store_virtual_tuple(&mut slot);
                let h = table.hash_slot(&mut slot).unwrap();
                hashes.push(table.hash_to_iv0(h));
            }
            hashes
        };
        let base = hash_all(0);
        for iv in ivs {
            assert_eq!(hash_all(iv), base, "Expr-arm rebase (iv={iv:#x})");
        }
    }
}
