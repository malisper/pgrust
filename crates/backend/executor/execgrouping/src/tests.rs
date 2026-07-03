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
    let add = table.entry_additional(i1);
    // SAFETY: 16 zeroed additional bytes per entry (build arg above).
    let bytes = unsafe { core::slice::from_raw_parts(add.as_ptr(), 16) };
    assert_eq!(bytes, &[0u8; 16]);

    table.reset();
    assert_eq!(table.num_entries(), 0);
    let (_, renew) = put(&mut table, &mut slot, &table_ctx, mcx, 7, false);
    assert!(renew);
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
