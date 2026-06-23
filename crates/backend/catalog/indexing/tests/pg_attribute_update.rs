//! Round-trip test for the `ALTER TABLE` per-`Anum` `pg_attribute` field-modify
//! path (`catalog_tuple_update_pg_attribute`).
//!
//! The seam's body is `heap_modify_tuple` over the selectively-replaced columns
//! carried in `PgAttributeUpdateRow`, then `CatalogTupleUpdate` (which needs a
//! live catalog relation). This test exercises the form/modify/deform core the
//! seam is built on directly: form a small pg_attribute-shaped tuple with
//! `attnotnull = true`, flip it to `false` via the same `replaces[]` mechanism
//! the seam uses, deform, and assert the flip landed while the untouched
//! columns round-trip unchanged.

use ::heaptuple::{
    heap_deform_tuple, heap_form_tuple, heap_modify_tuple, Datum,
};
use ::mcx::{slice_in, Mcx, MemoryContext, PgVec};
use ::types_tuple::heaptuple::{CompactAttribute, TupleDescData};

/// A by-value `CompactAttribute` of the given `attlen`/alignment.
fn byval(attlen: i16, attalignby: u8) -> CompactAttribute {
    CompactAttribute {
        attcacheoff: -1,
        attlen,
        attbyval: true,
        attispackable: false,
        atthasmissing: false,
        attisdropped: false,
        attgenerated: false,
        attnullability: 0,
        attalignby,
    }
}

fn tupdesc<'mcx>(mcx: Mcx<'mcx>, attrs: &[CompactAttribute]) -> TupleDescData<'mcx> {
    TupleDescData {
        natts: attrs.len() as i32,
        tdtypeid: 2249, // RECORDOID
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: slice_in(mcx, attrs).unwrap(),
        attrs: PgVec::new_in(mcx),
    }
}

#[test]
fn attnotnull_flip_round_trips() {
    let ctx = MemoryContext::new("pg_attribute_update_test");
    let mcx = ctx.mcx();

    // A 3-column slice of pg_attribute's by-value layout, in field order:
    //   atttypid (oid, 4 bytes), attnum (int2, 2 bytes), attnotnull (bool, 1).
    let td = tupdesc(mcx, &[byval(4, 4), byval(2, 2), byval(1, 1)]);

    // The original on-disk row: atttypid = 23 (int4), attnum = 5,
    // attnotnull = true.
    let values = [Datum::from_oid(23), Datum::from_i16(5), Datum::from_bool(true)];
    let isnull = [false, false, false];
    let orig = heap_form_tuple(mcx, &td, &values, &isnull).unwrap();

    // Sanity: the formed tuple deforms back to the inputs.
    let before = heap_deform_tuple(mcx, &orig.tuple, &td, &orig.data).unwrap();
    assert_eq!(before[2].0.as_bool(), true, "attnotnull starts true");

    // The ALTER `DROP NOT NULL` write: replace only attnotnull (column 3),
    // exactly as PgAttributeUpdateRow { attnotnull: Some(false), .. } drives the
    // seam's replaces[]/values[] fill.
    let repl_values = [Datum::null(), Datum::null(), Datum::from_bool(false)];
    let repl_isnull = [false, false, false];
    let do_replace = [false, false, true];
    let modified =
        heap_modify_tuple(mcx, &orig, &td, &repl_values, &repl_isnull, &do_replace).unwrap();

    // Deform the modified tuple: attnotnull flipped to false, the untouched
    // columns preserved.
    let after = heap_deform_tuple(mcx, &modified.tuple, &td, &modified.data).unwrap();
    assert_eq!(after[0].0.as_oid(), 23, "atttypid preserved");
    assert_eq!(after[1].0.as_i16(), 5, "attnum preserved");
    assert_eq!(after[2].0.as_bool(), false, "attnotnull flipped to false");
    assert!(!after[0].1 && !after[1].1 && !after[2].1, "no nulls introduced");

    // The tuple identity (t_self) is copied from the old tuple, so the
    // CatalogTupleUpdate the seam performs would address the original row.
    assert_eq!(
        modified.tuple.t_self, orig.tuple.t_self,
        "modified tuple keeps the original t_self for in-place update"
    );
}
