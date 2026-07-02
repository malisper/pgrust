use super::*;

use std::sync::Once;

use ::datum::Datum;
use ::executils::EStateData;
use ::mcx::{MemoryContext, PgVec};
use ::types_core::{Oid, RECORDOID};
use ::types_tuple::{
    CompactAttribute, FormData_pg_attribute, PgTypeShape, TYPALIGN_INT, TYPSTORAGE_PLAIN,
};

const INT4OID: Oid = 23;

static SEAMS: Once = Once::new();

fn with_mcx<R>(f: impl for<'m> FnOnce(Mcx<'m>) -> R) -> R {
    SEAMS.call_once(|| {
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok(match typid {
                INT4OID => Some(PgTypeShape {
                    typlen: 4,
                    typbyval: true,
                    typalign: TYPALIGN_INT,
                    typstorage: TYPSTORAGE_PLAIN,
                    typcollation: 0,
                }),
                _ => None,
            })
        });
    });
    let ctx = MemoryContext::new("execscan-test");
    f(ctx.mcx())
}

fn int4_desc<'mcx>(mcx: Mcx<'mcx>, natts: i32) -> Rc<TupleDescData<'mcx>> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for i in 0..natts {
        let att = FormData_pg_attribute {
            attnum: (i + 1) as i16,
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
        natts,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn var_tle<'mcx>(mcx: Mcx<'mcx>, attno: i16, resno: i16, typmod: i32) -> Node<'mcx> {
    let var = Node::mk_var(mcx, 1, attno, INT4OID, typmod, 0, 0).unwrap();
    Node::mk_target_entry(mcx, var, resno, None, false).unwrap()
}

fn assign<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: &NodeList<'mcx>,
    natts: i32,
) -> Option<ProjectionInfo<'mcx>> {
    let mut estate = EStateData::new_in(mcx);
    let desc = int4_desc(mcx, natts);
    exec_conditional_assign_projection_info(mcx, &mut estate, tlist, 1, &desc).unwrap()
}

#[test]
fn matching_tlist_needs_no_projection() {
    with_mcx(|mcx| {
        let tlist =
            NodeList::make2(mcx, var_tle(mcx, 1, 1, -1), var_tle(mcx, 2, 2, -1)).unwrap();
        assert!(assign(mcx, &tlist, 2).is_none());
    });
}

#[test]
fn out_of_order_vars_project() {
    with_mcx(|mcx| {
        let tlist =
            NodeList::make2(mcx, var_tle(mcx, 2, 1, -1), var_tle(mcx, 1, 2, -1)).unwrap();
        assert!(assign(mcx, &tlist, 2).is_some());
    });
}

#[test]
fn short_and_long_tlists_project() {
    with_mcx(|mcx| {
        let short = NodeList::make1(mcx, var_tle(mcx, 1, 1, -1)).unwrap();
        assert!(assign(mcx, &short, 2).is_some());
        let long =
            NodeList::make2(mcx, var_tle(mcx, 1, 1, -1), var_tle(mcx, 2, 2, -1)).unwrap();
        assert!(assign(mcx, &long, 1).is_some());
    });
}

#[test]
fn non_var_tle_projects() {
    with_mcx(|mcx| {
        let c = Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(1), false, true).unwrap();
        let tle = Node::mk_target_entry(mcx, c, 1, None, false).unwrap();
        let tlist = NodeList::make1(mcx, tle).unwrap();
        assert!(assign(mcx, &tlist, 1).is_some());
    });
}

#[test]
fn generic_var_typmod_matches_specific_tupdesc() {
    // C allows vartypmod -1 against any tupdesc typmod (union-of-typmods case).
    with_mcx(|mcx| {
        let tlist = NodeList::make1(mcx, var_tle(mcx, 1, 1, -1)).unwrap();
        assert!(assign(mcx, &tlist, 1).is_none());
        let mismatched = NodeList::make1(mcx, var_tle(mcx, 1, 1, 7)).unwrap();
        assert!(assign(mcx, &mismatched, 1).is_some());
    });
}

#[test]
fn type_from_tl_builds_record_desc() {
    with_mcx(|mcx| {
        let c = Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(9), false, true).unwrap();
        let tle = Node::mk_target_entry(mcx, c, 1, Some("answer"), false).unwrap();
        let tlist = NodeList::make1(mcx, tle).unwrap();
        let desc = exec_type_from_tl(mcx, &tlist).unwrap();
        assert_eq!(desc.natts, 1);
        assert_eq!(desc.tdtypeid, RECORDOID);
        assert_eq!(desc.tdtypmod, -1);
        let att = &desc.attrs[0];
        assert_eq!(att.atttypid, INT4OID);
        assert_eq!(att.atttypmod, -1);
        assert_eq!(att.attlen, 4);
        assert!(att.attbyval);
        assert_eq!(att.attnum, 1);
    });
}

#[test]
fn projection_slot_is_registered_in_tuple_table() {
    with_mcx(|mcx| {
        let mut estate = EStateData::new_in(mcx);
        let desc = int4_desc(mcx, 1);
        let c = Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(3), false, true).unwrap();
        let tle = Node::mk_target_entry(mcx, c, 1, None, false).unwrap();
        let tlist = NodeList::make1(mcx, tle).unwrap();
        let proj = exec_conditional_assign_projection_info(mcx, &mut estate, &tlist, 1, &desc)
            .unwrap()
            .unwrap();
        let slot = estate.slot(proj.pi_result_slot);
        assert_eq!(slot.base().tts_tupleDescriptor.as_ref().unwrap().natts, 1);
    });
}
