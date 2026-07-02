use mcx::{Mcx, MemoryContext};
use parser_small1::make_parsestate;
use types_core::catalog::{INT4OID, TEXTOID, UNKNOWNOID};
use types_core::InvalidOid;
use types_nodes::{CoercionForm, Node, NodeTag};

use crate::{
    can_coerce_type, coerce_type, enforce_generic_type_consistency, find_coercion_pathway,
    IsBinaryCoercible, COERCION_ASSIGNMENT, COERCION_IMPLICIT, COERCION_PATH_COERCEVIAIO,
    COERCION_PATH_NONE, COERCION_PATH_RELABELTYPE,
};

const VARCHAROID: types_core::Oid = 1043;
const TEXT_TO_VARCHAR_CAST: types_core::Oid = 10058;

fn install_fixture() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        syscache_seams::pg_type_base_shape::set(|typid| {
            Ok(Some(syscache_seams::PgTypeBaseShape {
                typtype: if typid == UNKNOWNOID { b'p' as i8 } else { b'b' as i8 },
                typbasetype: InvalidOid,
                typtypmod: -1,
                typelem: InvalidOid,
                typsubscript: InvalidOid,
            }))
        });
        syscache_seams::pg_type_io_shape::set(|typid| {
            Ok((typid == TEXTOID).then_some(syscache_seams::PgTypeIoShape {
                oid: TEXTOID,
                typinput: 46,
                typoutput: 47,
                typreceive: 2414,
                typsend: 2415,
                typmodin: InvalidOid,
                typmodout: InvalidOid,
                typelem: InvalidOid,
                typlen: -1,
                typbyval: false,
                typalign: b'i' as i8,
                typdelim: b',' as i8,
                typisdefined: true,
            }))
        });
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok(Some(types_tuple::PgTypeShape {
                typlen: if typid == TEXTOID || typid == VARCHAROID { -1 } else { 4 },
                typbyval: !(typid == TEXTOID || typid == VARCHAROID),
                typalign: b'i' as i8,
                typstorage: b'p' as i8,
                typcollation: if typid == TEXTOID || typid == VARCHAROID { 100 } else { InvalidOid },
            }))
        });
        // pg_cast.dat: text -> varchar is binary-coercible, implicit.
        syscache_seams::lookup_pg_cast_shape::set(|src, tgt| {
            Ok((src == TEXTOID && tgt == VARCHAROID).then_some(syscache_seams::PgCastShape {
                oid: TEXT_TO_VARCHAR_CAST,
                castfunc: InvalidOid,
                castcontext: b'i' as i8,
                castmethod: b'b' as i8,
            }))
        });
        syscache_seams::pg_type_element_shape::set(|_| {
            Ok(Some(syscache_seams::PgTypeElementShape {
                typelem: InvalidOid,
                typsubscript: InvalidOid,
            }))
        });
        syscache_seams::pg_type_category::set(|typid| {
            Ok(Some(if typid == TEXTOID || typid == VARCHAROID {
                (b'S' as i8, typid == TEXTOID)
            } else {
                (b'N' as i8, false)
            }))
        });
        syscache_seams::pg_type_typrelid::set(|_| Ok(Some(InvalidOid)));
    });
}

fn unknown_const<'mcx>(mcx: Mcx<'mcx>, s: &str) -> Node<'mcx> {
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, s.len() + 1).unwrap();
    mcx::vec_append_bytes(&mut buf, s.as_bytes()).unwrap();
    buf.push(0);
    let d = datum::Datum::from_usize(buf.leak().as_ptr() as usize);
    Node::mk_const(mcx, UNKNOWNOID, -1, InvalidOid, -2, d, false, false).unwrap()
}

#[test]
fn unknown_const_coerces_to_text_via_textin() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let pstate = make_parsestate(mcx, None);
    let node = unknown_const(mcx, "hello");

    let out = coerce_type(
        mcx,
        &pstate,
        node,
        UNKNOWNOID,
        TEXTOID,
        -1,
        COERCION_IMPLICIT,
        CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )
    .unwrap();

    let c = out.as_const().unwrap();
    assert_eq!(c.consttype, TEXTOID);
    assert_eq!((c.consttypmod, c.constcollid), (-1, 100));
    assert_eq!((c.constlen, c.constbyval, c.constisnull), (-1, false, false));
    // SAFETY: the datum points at a flat 4B-header text varlena owned by mcx.
    let v = unsafe { datum::varlena::VarlenaRef::from_ptr(c.constvalue.as_usize() as *const u8) };
    assert_eq!(v.data(), b"hello");
}

#[test]
fn null_unknown_const_coerces_without_calling_input() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let pstate = make_parsestate(mcx, None);
    let node =
        Node::mk_const(mcx, UNKNOWNOID, -1, InvalidOid, -2, datum::Datum::null(), true, false)
            .unwrap();

    let out = coerce_type(
        mcx,
        &pstate,
        node,
        UNKNOWNOID,
        TEXTOID,
        -1,
        COERCION_IMPLICIT,
        CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )
    .unwrap();

    let c = out.as_const().unwrap();
    assert_eq!((c.consttype, c.constisnull), (TEXTOID, true));
    assert_eq!(c.constvalue, datum::Datum::null());
}

#[test]
fn same_type_is_identity() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let pstate = make_parsestate(mcx, None);
    let node = Node::mk_const(mcx, INT4OID, -1, InvalidOid, 4, datum::Datum::from_i32(7), false, true)
        .unwrap();

    let out = coerce_type(
        mcx,
        &pstate,
        node,
        INT4OID,
        INT4OID,
        -1,
        COERCION_IMPLICIT,
        CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )
    .unwrap();
    assert_eq!(out.node_tag(), NodeTag::T_Const);
    assert_eq!(out.as_const().unwrap().constvalue, datum::Datum::from_i32(7));
}

#[test]
fn binary_compatible_cast_wraps_relabel() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let pstate = make_parsestate(mcx, None);
    let node = Node::mk_const(mcx, TEXTOID, -1, 100, -1, datum::Datum::null(), true, false).unwrap();

    let out = coerce_type(
        mcx,
        &pstate,
        node,
        TEXTOID,
        VARCHAROID,
        -1,
        COERCION_IMPLICIT,
        CoercionForm::COERCE_IMPLICIT_CAST,
        11,
    )
    .unwrap();

    let r = out.as_relabel_type().unwrap();
    assert_eq!(r.resulttype, VARCHAROID);
    assert_eq!(r.resulttypmod, -1);
    assert_eq!(r.relabelformat, CoercionForm::COERCE_IMPLICIT_CAST);
    assert_eq!(r.location, 11);
    assert_eq!(r.arg.node_tag(), NodeTag::T_Const);
}

#[test]
fn pathways_and_predicates() {
    install_fixture();
    assert_eq!(
        find_coercion_pathway(VARCHAROID, TEXTOID, COERCION_IMPLICIT).unwrap().0,
        COERCION_PATH_RELABELTYPE
    );
    assert_eq!(
        find_coercion_pathway(TEXTOID, INT4OID, COERCION_IMPLICIT).unwrap().0,
        COERCION_PATH_NONE
    );
    // assignment-to-string CoerceViaIO fallback (find_coercion_pathway tail).
    assert_eq!(
        find_coercion_pathway(TEXTOID, INT4OID, COERCION_ASSIGNMENT).unwrap().0,
        COERCION_PATH_COERCEVIAIO
    );

    assert!(IsBinaryCoercible(TEXTOID, TEXTOID).unwrap());
    assert!(IsBinaryCoercible(TEXTOID, VARCHAROID).unwrap());
    assert!(!IsBinaryCoercible(VARCHAROID, TEXTOID).unwrap());

    assert!(can_coerce_type(&[UNKNOWNOID], &[INT4OID], COERCION_IMPLICIT).unwrap());
    assert!(can_coerce_type(&[TEXTOID], &[VARCHAROID], COERCION_IMPLICIT).unwrap());
    assert!(!can_coerce_type(&[INT4OID], &[TEXTOID], COERCION_IMPLICIT).unwrap());

    let mut declared = [INT4OID, INT4OID];
    assert_eq!(
        enforce_generic_type_consistency(&[INT4OID, INT4OID], &mut declared, INT4OID, false),
        INT4OID
    );
}
