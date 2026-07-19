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
const BPCHAROID: types_core::Oid = 1042;
const BPCHAR_LEN_COERCION_FUNC: types_core::Oid = 668;
const MISSING_TYPE: types_core::Oid = 999_999;
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
            Ok(match typid {
                TEXTOID => Some(syscache_seams::PgTypeIoShape {
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
                }),
                INT4OID => Some(syscache_seams::PgTypeIoShape {
                    oid: INT4OID,
                    typinput: 42,
                    typoutput: 43,
                    typreceive: 2406,
                    typsend: 2407,
                    typmodin: InvalidOid,
                    typmodout: InvalidOid,
                    typelem: InvalidOid,
                    typlen: 4,
                    typbyval: true,
                    typalign: b'i' as i8,
                    typdelim: b',' as i8,
                    typisdefined: true,
                }),
                _ => None,
            })
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
            if src == BPCHAROID && tgt == BPCHAROID {
                return Ok(Some(syscache_seams::PgCastShape {
                    oid: 10096,
                    castfunc: BPCHAR_LEN_COERCION_FUNC,
                    castcontext: b'i' as i8,
                    castmethod: b'f' as i8,
                }));
            }
            Ok((src == TEXTOID && tgt == VARCHAROID).then_some(syscache_seams::PgCastShape {
                oid: TEXT_TO_VARCHAR_CAST,
                castfunc: InvalidOid,
                castcontext: b'i' as i8,
                castmethod: b'b' as i8,
            }))
        });
        syscache_seams::pg_type_element_shape::set(|typid| {
            if typid == MISSING_TYPE {
                return Ok(None);
            }
            Ok(Some(syscache_seams::PgTypeElementShape {
                typelem: InvalidOid,
                typsubscript: InvalidOid,
            }))
        });
        syscache_seams::lookup_pg_proc_shape::set(|funcid| {
            Ok((funcid == BPCHAR_LEN_COERCION_FUNC).then_some(syscache_seams::PgProcShape {
                prolang: 12,
                prosecdef: false,
                proconfig_isnull: true,
                pronamespace: 11,
                prorettype: BPCHAROID,
                provariadic: InvalidOid,
                prosupport: InvalidOid,
                pronargs: 3,
                prokind: b'f' as i8,
                provolatile: b'i' as i8,
                proparallel: b's' as i8,
                proretset: false,
                proisstrict: true,
                proleakproof: false,
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
        pg_inherits_seams::type_inherits_from::set(|_, _| Ok(false));
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
fn unknown_const_coercion_error_carries_cursor_position() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_sourcetext = Some(b"SELECT 'notanint'::int4");

    let mut buf: mcx::PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, 9).unwrap();
    mcx::vec_append_bytes(&mut buf, b"notanint\0").unwrap();
    let d = datum::Datum::from_usize(buf.leak().as_ptr() as usize);
    let node = Node::mk(
        mcx,
        types_nodes::Const {
            consttype: UNKNOWNOID,
            consttypmod: -1,
            constcollid: InvalidOid,
            constlen: -2,
            constvalue: d,
            constisnull: false,
            constbyval: false,
            location: 7,
        },
    )
    .unwrap();

    let err = coerce_type(
        mcx,
        &pstate,
        node,
        UNKNOWNOID,
        INT4OID,
        -1,
        COERCION_IMPLICIT,
        CoercionForm::COERCE_IMPLICIT_CAST,
        17,
    )
    .unwrap_err();

    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_TEXT_REPRESENTATION);
    // C: setup_parser_errposition_callback(con->location=7) -> char pos 8.
    assert_eq!(err.cursor_position(), Some(8));
}

#[test]
fn own_datum_flattens_short_varlena() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut short = [0u8; 4];
    // SAFETY: local buffer, len 4 <= VARATT_SHORT_MAX.
    unsafe { types_tuple::varatt::set_varsize_short(short.as_mut_ptr(), 4) };
    short[1] = b'h';
    short[2] = b'i';
    short[3] = b'!';
    let d = datum::Datum::from_usize(short.as_ptr() as usize);

    let out = crate::own_datum(mcx, d, -1, false).unwrap();
    // SAFETY: own_datum returned a flat varlena owned by mcx.
    let v = unsafe { datum::varlena::VarlenaRef::from_ptr(out.as_usize() as *const u8) };
    assert_eq!(v.data(), b"hi!");
    // SAFETY: header byte of the returned varlena.
    assert!(unsafe { types_tuple::varatt::varatt_is_4b_u(out.as_usize() as *const u8) });
}

#[repr(C)]
struct FakeExpanded {
    hdr: datum::ExpandedObjectHeader,
    payload: [u8; 4],
}

static FAKE_METHODS: datum::ExpandedObjectMethods = datum::ExpandedObjectMethods {
    get_flat_size: |_| 8,
    flatten_into: |eohptr, result, n| unsafe {
        assert_eq!(n, 8);
        let obj = eohptr as *mut FakeExpanded;
        core::ptr::copy_nonoverlapping(((n as u32) << 2).to_ne_bytes().as_ptr(), result, 4);
        core::ptr::copy_nonoverlapping((*obj).payload.as_ptr(), result.add(4), 4);
    },
};

#[test]
fn own_datum_flattens_expanded_datum() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let obj = Box::into_raw(Box::new(FakeExpanded {
        hdr: datum::ExpandedObjectHeader::empty(),
        payload: *b"flat",
    }));
    let d = unsafe {
        let hdr = core::ptr::addr_of_mut!((*obj).hdr);
        datum::expandeddatum::eoh_init_header(hdr, &FAKE_METHODS, core::ptr::null());
        datum::expandeddatum::eohp_get_ro_datum(hdr)
    };
    let out = crate::own_datum(mcx, d, -1, false).unwrap();
    let image =
        unsafe { core::slice::from_raw_parts(out.as_usize() as *const u8, 8) };
    assert_eq!(&image[4..], b"flat");
    drop(unsafe { Box::from_raw(obj) });
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
        enforce_generic_type_consistency(&[INT4OID, INT4OID], &mut declared, INT4OID, false)
            .unwrap(),
        INT4OID
    );
}

#[test]
fn typmod_coercion_function_paths() {
    install_fixture();
    assert_eq!(
        crate::find_typmod_coercion_function(BPCHAROID).unwrap(),
        (crate::COERCION_PATH_FUNC, BPCHAR_LEN_COERCION_FUNC)
    );
    assert_eq!(
        crate::find_typmod_coercion_function(TEXTOID).unwrap(),
        (COERCION_PATH_NONE, InvalidOid)
    );
}

#[test]
fn typmod_coercion_builds_three_arg_funcexpr() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let node =
        Node::mk_const(mcx, BPCHAROID, -1, 100, -1, datum::Datum::null(), true, false).unwrap();

    let out = crate::coerce_type_typmod(
        mcx,
        node,
        BPCHAROID,
        9,
        COERCION_IMPLICIT,
        CoercionForm::COERCE_IMPLICIT_CAST,
        7,
        false,
    )
    .unwrap();
    let f = out.as_func_expr().unwrap();
    assert_eq!(f.funcid, BPCHAR_LEN_COERCION_FUNC);
    assert_eq!(f.funcresulttype, BPCHAROID);
    assert_eq!(f.args.len(), 3);
    let mut args = f.args.iter();
    assert!(args.next().unwrap().ptr_eq(node));
    let typmod_arg = args.next().unwrap().as_const().unwrap();
    assert_eq!(typmod_arg.constvalue, datum::Datum::from_i32(9));
    let explicit_arg = args.next().unwrap().as_const().unwrap();
    assert_eq!(explicit_arg.constvalue, datum::Datum::from_bool(false));

    let out = crate::coerce_type_typmod(
        mcx,
        node,
        BPCHAROID,
        9,
        crate::COERCION_EXPLICIT,
        CoercionForm::COERCE_EXPLICIT_CAST,
        7,
        false,
    )
    .unwrap();
    let f = out.as_func_expr().unwrap();
    let explicit_arg = f.args.iter().nth(2).unwrap().as_const().unwrap();
    assert_eq!(explicit_arg.constvalue, datum::Datum::from_bool(true));
}

#[test]
fn unknown_param_to_typmod_target_wraps_without_hiding() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let parstate = parser_small1::VarParamState::new();
    parstate.param_types.borrow_mut().push(UNKNOWNOID);
    parser_small1::setup_parse_variable_parameters(&mut pstate, parstate);

    let param = Node::mk(
        mcx,
        types_nodes::primnodes::Param {
            paramkind: types_nodes::primnodes::ParamKind::PARAM_EXTERN,
            paramid: 1,
            paramtype: UNKNOWNOID,
            paramtypmod: -1,
            paramcollid: InvalidOid,
            location: -1,
        },
    )
    .unwrap();

    // coerce_type retypes the unknown Param in place and returns it, so only
    // the length coercion wraps it; the hide flag must stay false (the input
    // node was not generated by coerce_type).
    let out = crate::coerce_to_target_type(
        mcx,
        &pstate,
        param,
        UNKNOWNOID,
        BPCHAROID,
        9,
        COERCION_IMPLICIT,
        CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )
    .unwrap()
    .unwrap();

    let f = out.as_func_expr().unwrap();
    assert_eq!(f.funcid, BPCHAR_LEN_COERCION_FUNC);
    let arg0 = f.args.iter().next().unwrap();
    assert!(arg0.ptr_eq(param));
    assert_eq!(arg0.as_param().unwrap().paramtype, BPCHAROID);
    assert_eq!(arg0.as_param().unwrap().paramtypmod, -1);
}

#[test]
fn typmod_coercion_skips_when_typmod_matches() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let node =
        Node::mk_const(mcx, BPCHAROID, 9, 100, -1, datum::Datum::null(), true, false).unwrap();
    let out = crate::coerce_type_typmod(
        mcx,
        node,
        BPCHAROID,
        9,
        COERCION_IMPLICIT,
        CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
        false,
    )
    .unwrap();
    assert!(out.ptr_eq(node));
}

#[test]
fn negative_typmod_retypes_const_to_unspecified_typmod() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let node =
        Node::mk_const(mcx, BPCHAROID, 9, 100, -1, datum::Datum::null(), true, false).unwrap();
    let out = crate::coerce_type_typmod(
        mcx,
        node,
        BPCHAROID,
        -1,
        crate::COERCION_EXPLICIT,
        CoercionForm::COERCE_EXPLICIT_CAST,
        -1,
        false,
    )
    .unwrap();
    // C coerce_type_typmod: targetTypMod < 0 takes the applyRelabelType leg,
    // which rewrites a Const in place of wrapping it.
    let c = out.as_const().unwrap();
    assert_eq!(c.consttype, BPCHAROID);
    assert_eq!(c.consttypmod, -1);
    assert_eq!(c.constcollid, 100);
}

#[test]
fn negative_typmod_relabels_var_to_unspecified_typmod() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let var = Node::mk_var(mcx, 1, 1, BPCHAROID, 9, 100, 0).unwrap();
    let out = crate::coerce_type_typmod(
        mcx,
        var,
        BPCHAROID,
        -1,
        crate::COERCION_EXPLICIT,
        CoercionForm::COERCE_EXPLICIT_CAST,
        -1,
        false,
    )
    .unwrap();
    let r = out.as_relabel_type().unwrap();
    assert!(r.arg.ptr_eq(var));
    assert_eq!(r.resulttype, BPCHAROID);
    assert_eq!(r.resulttypmod, -1);
    assert_eq!(r.resultcollid, 100);
    assert_eq!(r.relabelformat, CoercionForm::COERCE_EXPLICIT_CAST);
}
