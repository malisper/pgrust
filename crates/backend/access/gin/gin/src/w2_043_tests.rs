//! audit-18.6 remediation batch w2-043-ginutil-c-1 witnesses
//! (backend/access/gin, ginutil.c initGinState).
//!
//! C's initGinState (ginutil.c:139-206) resolves every support-proc slot on
//! its own through index_getprocid / index_getprocinfo, so an opclass may
//! pair any extractValue with any extractQuery, register a comparePartial
//! proc next to array procs, or name any comparator as FUNCTION 1. pgrust
//! resolved the whole column to one closed opclass tag from the extract
//! procs, refusing (0A000) or asserting on every other pairing. The rig here
//! is rem_b084's fake index relation with the pg_amproc projection keyed by
//! opfamily, one custom opclass shape per witness.

use std::cell::Cell;
use std::rc::Rc;

use ::gin_vocab::*;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_core::{Oid, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
use ::types_error::PgResult;
use ::types_rel::{
    FormData_pg_class, FormData_pg_index, LockInfoData, LockRelId, Relation, RelationData, LOCKMODE,
    RELKIND_INDEX, REPLICA_IDENTITY_DEFAULT,
};
use ::types_tuple::tupdesc::{CompactAttribute, FormData_pg_attribute};
use ::types_tuple::TupleDescData;

use crate::util::initGinState;

/// Opfamily oids the rig's pg_amproc projection (rem_b084_tests::install)
/// answers for; each is one custom opclass shape.
///
/// Row a186-verified-fp-gin-ginutil-20afcd34cff6cd5d3ebf-1: array procs
/// with FUNCTION 3 gin_extract_tsquery (the verified fp4_odd_arr_ops).
pub(crate) const OPFAMILY_ODD_ARRAY: Oid = 16401;
/// Row a186-verified-fp-gin-ginutil-918e21828a298d32e5ee-1: array_ops shape
/// plus FUNCTION 5 gin_cmp_prefix (the verified fp1_arr_ops).
pub(crate) const OPFAMILY_ARRAY_PREFIX: Oid = 16402;
/// A text-keyed array opclass naming bttextcmp as FUNCTION 1 (the live-pair
/// w2_textarr_ops arm: pgrust asserted on the proc-1 / opclass pairing).
pub(crate) const OPFAMILY_TEXT_ARRAY_CMP: Oid = 16403;
/// Array procs with only the tri-state consistent proc (FUNCTION 6, no
/// FUNCTION 4): C's shimBoolConsistentFn wiring.
pub(crate) const OPFAMILY_TRI_ONLY: Oid = 16404;

/// pg_amproc rows of the shapes above (procnum -> proc oid; 0 = no row).
pub(crate) fn amproc_of(opfamily: Oid, procnum: u16) -> Option<Oid> {
    Some(match (opfamily, procnum) {
        (OPFAMILY_ODD_ARRAY, GIN_EXTRACTVALUE_PROC) => 2743, // ginarrayextract
        (OPFAMILY_ODD_ARRAY, GIN_EXTRACTQUERY_PROC) => 3657, // gin_extract_tsquery
        (OPFAMILY_ODD_ARRAY, GIN_CONSISTENT_PROC) => 2744,   // ginarrayconsistent
        (OPFAMILY_ODD_ARRAY, _) => 0,

        (OPFAMILY_ARRAY_PREFIX, GIN_EXTRACTVALUE_PROC) => 2743, // ginarrayextract
        (OPFAMILY_ARRAY_PREFIX, GIN_EXTRACTQUERY_PROC) => 2774, // ginqueryarrayextract
        (OPFAMILY_ARRAY_PREFIX, GIN_CONSISTENT_PROC) => 2744,   // ginarrayconsistent
        (OPFAMILY_ARRAY_PREFIX, GIN_COMPARE_PARTIAL_PROC) => 2700, // gin_cmp_prefix
        (OPFAMILY_ARRAY_PREFIX, _) => 0,

        (OPFAMILY_TEXT_ARRAY_CMP, GIN_COMPARE_PROC) => 360,          // bttextcmp
        (OPFAMILY_TEXT_ARRAY_CMP, GIN_EXTRACTVALUE_PROC) => 2743,    // ginarrayextract
        (OPFAMILY_TEXT_ARRAY_CMP, GIN_EXTRACTQUERY_PROC) => 2774,    // ginqueryarrayextract
        (OPFAMILY_TEXT_ARRAY_CMP, GIN_CONSISTENT_PROC) => 2744,      // ginarrayconsistent
        (OPFAMILY_TEXT_ARRAY_CMP, GIN_COMPARE_PARTIAL_PROC) => 2700, // gin_cmp_prefix
        (OPFAMILY_TEXT_ARRAY_CMP, _) => 0,

        (OPFAMILY_TRI_ONLY, GIN_EXTRACTVALUE_PROC) => 2743,  // ginarrayextract
        (OPFAMILY_TRI_ONLY, GIN_EXTRACTQUERY_PROC) => 2774,  // ginqueryarrayextract
        (OPFAMILY_TRI_ONLY, GIN_TRICONSISTENT_PROC) => 3920, // ginarraytriconsistent
        (OPFAMILY_TRI_ONLY, _) => 0,

        _ => return None,
    })
}

fn one_col_tupdesc(mcx: Mcx<'_>, text_keys: bool) -> TupleDescData<'_> {
    let mut compact = PgVec::new_in(mcx);
    compact.push(CompactAttribute {
        attcacheoff: Cell::new(-1),
        attlen: if text_keys { -1 } else { 4 },
        attbyval: !text_keys,
        attispackable: text_keys,
        atthasmissing: false,
        attisdropped: false,
        attgenerated: false,
        attnullability: 0,
        attalignby: 4,
    });
    // initGinState's typcache fallback reads the key attribute's type when
    // the opclass has no FUNCTION 1 (ginutil.c:147).
    let mut attrs = PgVec::new_in(mcx);
    attrs.push(FormData_pg_attribute {
        atttypid: if text_keys { ::types_core::TEXTOID } else { ::types_core::INT4OID },
        attlen: if text_keys { -1 } else { 4 },
        attnum: 1,
        atttypmod: -1,
        attbyval: !text_keys,
        attalign: b'i' as i8,
        attstorage: if text_keys { b'x' as i8 } else { b'p' as i8 },
        ..Default::default()
    });
    TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: 1,
        constr: None,
        compact_attrs: compact,
        attrs,
    }
}

fn noop_close(_oid: Oid, _mode: LOCKMODE) -> PgResult<()> {
    Ok(())
}

/// A permanent one-column GIN index relation over int4[] (or text[]) keyed
/// by the custom opfamily `opfamily` (opcintype anyarray, no collation).
fn index_rel(mcx: Mcx<'_>, opfamily: Oid, text_keys: bool) -> Relation<'_> {
    let mut relname = ::types_tuple::NameData::default();
    relname.namestrcpy("w2_043_gin");
    let mut indkey = PgVec::new_in(mcx);
    indkey.push(1);
    let one = |v: Oid| {
        let mut vec = PgVec::new_in(mcx);
        vec.push(v);
        vec
    };
    let mut indoption = PgVec::new_in(mcx);
    indoption.push(0i16);
    let data = RelationData {
        rd_locator: Cell::new(::types_storage::RelFileLocator::new(1663, 5, 6143)),
        rd_smgr: Default::default(),
        rd_id: 6143,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId { relId: 6143, dbId: 5 },
        },
        rd_rel: FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: ::types_core::catalog::GIN_AM_OID,
            relfilenode: 6143,
            reltablespace: 0,
            relpages: 0,
            reltuples: -1.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence: RELPERSISTENCE_PERMANENT,
            relkind: RELKIND_INDEX,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: REPLICA_IDENTITY_DEFAULT,
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        },
        rd_att: Rc::new(one_col_tupdesc(mcx, text_keys)),
        rd_index: Some(FormData_pg_index {
            indexrelid: 6143,
            indrelid: 6142,
            indnatts: 1,
            indnkeyatts: 1,
            indisunique: false,
            indnullsnotdistinct: false,
            indisprimary: false,
            indisexclusion: false,
            indimmediate: true,
            indisvalid: true,
            indisready: true,
            indcheckxmin: false,
            indxmin: 0,
            indkey,
            has_indpred: false,
            indexprs_src: None,
            indpred_src: None,
        }),
        rd_opcintype: one(::types_core::ANYARRAYOID),
        rd_opfamily: one(opfamily),
        rd_indoption: indoption,
        rd_indcollation: one(0),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        pgstat_link: Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(),
        rd_amcache_gin: Default::default(),
        rd_amcache_spgist: Default::default(),
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
        rd_trigdesc: Default::default(),
        rd_hastriggers: false,
        rd_hasrules: false,
    };
    Relation::open(data, Some(noop_close))
}

// ginutil.c:160-165: extractValue and extractQuery are two independent
// fmgr_info_copy's of index_getprocinfo; C never pairs them, so array procs
// with gin_extract_tsquery as FUNCTION 3 build an index (the verified row's
// C transcript: CREATE INDEX). pgrust refused with 0A000 "GIN operator class
// with extractQuery support function 3657 is not supported".
#[test]
fn extract_procs_resolve_independently_like_c() {
    crate::rem_b084_tests::install();
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx(), OPFAMILY_ODD_ARRAY, false);
    let state = initGinState(&rel)
        .unwrap_or_else(|e| panic!("C's initGinState accepts this opclass: {}", e.message()));
    let col = state.col(1);
    assert!(!col.can_partial_match, "no FUNCTION 5: canPartialMatch is false");
}

// ginutil.c:196-206: canPartialMatch is "proc 5 is registered", whatever
// the other procs are; C builds and scans the verified fp1_arr_ops index
// (array procs + gin_cmp_prefix). pgrust refused with 0A000 "GIN operator
// class with comparePartial support function 2700 is not supported".
#[test]
fn compare_partial_proc_is_recorded_on_any_opclass_shape() {
    crate::rem_b084_tests::install();
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx(), OPFAMILY_ARRAY_PREFIX, false);
    let state = initGinState(&rel)
        .unwrap_or_else(|e| panic!("C's initGinState accepts this opclass: {}", e.message()));
    assert!(state.col(1).can_partial_match, "FUNCTION 5 registered: canPartialMatch is true");
}

// ginutil.c:139-146: FUNCTION 1 is fmgr_info_copy'd from index_getprocinfo
// on its own; a text-keyed array opclass naming bttextcmp builds in C.
// pgrust asserted ("assertion failed: matches!(opclass, ...)", XX000 with a
// panic in the server log) on the proc-1 / opclass pairing.
#[test]
fn compare_proc_resolves_independently_of_the_extract_procs() {
    crate::rem_b084_tests::install();
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx(), OPFAMILY_TEXT_ARRAY_CMP, true);
    let state = initGinState(&rel)
        .unwrap_or_else(|e| panic!("C's initGinState accepts this opclass: {}", e.message()));
    assert!(state.col(1).can_partial_match);
}

// ginutil.c:171-193 + ginlogic.c ginInitConsistentFunction: an opclass with
// only the tri-state proc is valid; the binary check is shimmed over it.
#[test]
fn tri_consistent_only_opclass_is_accepted() {
    crate::rem_b084_tests::install();
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx(), OPFAMILY_TRI_ONLY, false);
    let state = initGinState(&rel)
        .unwrap_or_else(|e| panic!("C's initGinState accepts this opclass: {}", e.message()));
    assert!(!state.col(1).can_partial_match);
}
