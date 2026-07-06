use mcx::MemoryContext;
use wchar::{PG_LATIN1, PG_UTF8};

use crate::{
    downcase_identifier, downcase_truncate_identifier, parser_errposition_source,
    scanner_isspace, truncate_identifier, NAMEDATALEN,
};

#[test]
fn downcase_ascii() {
    let ctx = MemoryContext::new("t");
    let out = downcase_truncate_identifier(ctx.mcx(), b"FooBar_123", false, PG_UTF8).unwrap();
    assert_eq!(&out[..], b"foobar_123");
}

#[test]
fn downcase_leaves_high_bit_in_multibyte_encoding() {
    let ctx = MemoryContext::new("t");
    let ident = "S\u{00c9}L".as_bytes();
    let out = downcase_truncate_identifier(ctx.mcx(), ident, false, PG_UTF8).unwrap();
    assert_eq!(&out[..], "s\u{00c9}l".as_bytes());
}

#[test]
fn downcase_high_bit_single_byte_c_locale() {
    // C locale: isupper never true for high-bit bytes, so 0xC9 passes through.
    let ctx = MemoryContext::new("t");
    let out = downcase_truncate_identifier(ctx.mcx(), &[b'A', 0xC9, b'Z'], false, PG_LATIN1).unwrap();
    assert_eq!(&out[..], &[b'a', 0xC9, b'z']);
}

#[test]
fn downcase_truncates_at_namedatalen() {
    let ctx = MemoryContext::new("t");
    let long = [b'X'; 100];
    let out = downcase_truncate_identifier(ctx.mcx(), &long, false, PG_UTF8).unwrap();
    assert_eq!(out.len(), NAMEDATALEN - 1);
    assert!(out.iter().all(|&b| b == b'x'));
}

#[test]
fn downcase_no_truncate_flag() {
    let ctx = MemoryContext::new("t");
    let long = [b'y'; 100];
    let out = downcase_identifier(ctx.mcx(), &long, false, false, PG_UTF8).unwrap();
    assert_eq!(out.len(), 100);
}

#[test]
fn truncate_short_ident_untouched() {
    let ctx = MemoryContext::new("t");
    let mut v = mcx::slice_in(ctx.mcx(), b"short".as_slice()).unwrap();
    truncate_identifier(&mut v, false, PG_UTF8).unwrap();
    assert_eq!(&v[..], b"short");
}

#[test]
fn truncate_respects_multibyte_boundary() {
    let ctx = MemoryContext::new("t");
    let mut ident = alloc_ident(&ctx, 62);
    ident.extend_from_slice("\u{00e9}\u{00e9}".as_bytes());
    assert_eq!(ident.len(), 66);
    truncate_identifier(&mut ident, false, PG_UTF8).unwrap();
    // limit 63: 62 ascii + one 2-byte char would hit 64, so clip stays at 62.
    assert_eq!(ident.len(), 62);
}

#[test]
fn truncate_single_byte_encoding_stops_at_nul() {
    let ctx = MemoryContext::new("t");
    let mut ident = alloc_ident(&ctx, 70);
    ident[10] = 0;
    truncate_identifier(&mut ident, false, PG_LATIN1).unwrap();
    assert_eq!(ident.len(), 10);
}

fn alloc_ident<'a>(ctx: &'a MemoryContext, n: usize) -> mcx::PgVec<'a, u8> {
    mcx::vec_from_elem_in(ctx.mcx(), b'a', n)
}

#[test]
fn scanner_isspace_matches_scan_l() {
    for ch in [b' ', b'\t', b'\n', b'\r', 0x0b, 0x0c] {
        assert!(scanner_isspace(ch));
    }
    for ch in [b'a', b'0', 0x00, 0xA0, b'_'] {
        assert!(!scanner_isspace(ch));
    }
}

#[test]
fn errposition_missing_inputs() {
    assert_eq!(parser_errposition_source(Some(b"select 1"), -1, PG_UTF8), 0);
    assert_eq!(parser_errposition_source(None, 3, PG_UTF8), 0);
}

#[test]
fn errposition_counts_characters_not_bytes() {
    let src = "s\u{00e9}lect 1".as_bytes();
    assert_eq!(parser_errposition_source(Some(src), 3, PG_UTF8), 3);
    assert_eq!(parser_errposition_source(Some(src), 0, PG_UTF8), 1);
    assert_eq!(parser_errposition_source(Some(src), 3, PG_LATIN1), 4);
}

extern crate std;

use types_core::catalog::{
    BOOLOID, INT2ARRAYOID, INT2VECTOROID, INT4OID, INT8OID, TEXTOID, UNKNOWNOID, VOIDOID,
};
use types_core::{InvalidOid, Oid};
use types_error::{ERRCODE_TOO_MANY_COLUMNS, ERRCODE_UNDEFINED_PARAMETER};
use types_nodes::node_tree::{Boolean, Float, Integer};
use types_nodes::{A_Const, ParamKind, ParamRef, ValUnion};
use types_tuple::htup::MaxTupleAttributeNumber;

use crate::parse_param::VarParamState;
use crate::{
    check_variable_parameters, fixed_paramref_hook, free_parsestate, get_visible_ENR, make_const,
    make_parsestate, name_matches_visible_ENR, query_contains_extern_params,
    setup_parse_fixed_parameters, setup_parse_variable_parameters, transformContainerSubscripts,
    transformContainerType, variable_coerce_param_hook, variable_paramref_hook, ParseExprKind,
    ParseRefHookState,
};

const DOMAIN_OID: Oid = 90000;
const DOMAIN_BASE_TYPMOD: i32 = 7;

fn install_type_fixture() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // The rig models a live session over pg_catalog-band builtins:
        // always visible, so error strings stay unqualified (C TypeIsVisible).
        namespace_seams::type_is_visible::set(|_| Ok(true));
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok(Some(types_tuple::PgTypeShape {
                typlen: -2,
                typbyval: false,
                typalign: b'i' as i8,
                typstorage: b'p' as i8,
                typcollation: if typid == TEXTOID { 100 } else { InvalidOid },
            }))
        });
        syscache_seams::lookup_pg_type_typcache_shape::set(|typid| {
            let mut typname = types_tuple::NameData::default();
            typname.namestrcpy(if typid == TEXTOID { "text" } else { "int4" });
            Ok(Some(syscache_seams::PgTypeTypcacheShape {
                typname,
                typlen: 4,
                typbyval: true,
                typalign: b'i' as i8,
                typstorage: b'p' as i8,
                typtype: b'b' as i8,
                typisdefined: true,
                typrelid: InvalidOid,
                typsubscript: InvalidOid,
                typelem: InvalidOid,
                typarray: InvalidOid,
                typcollation: InvalidOid,
            }))
        });
        syscache_seams::pg_type_base_shape::set(|typid| {
            Ok(Some(if typid == DOMAIN_OID {
                syscache_seams::PgTypeBaseShape {
                    typtype: b'd' as i8,
                    typbasetype: TEXTOID,
                    typtypmod: DOMAIN_BASE_TYPMOD,
                    typelem: InvalidOid,
                    typsubscript: InvalidOid,
                }
            } else {
                syscache_seams::PgTypeBaseShape {
                    typtype: b'b' as i8,
                    typbasetype: InvalidOid,
                    typtypmod: -1,
                    typelem: InvalidOid,
                    typsubscript: InvalidOid,
                }
            }))
        });
    });
}

#[test]
fn parsestate_defaults_and_inheritance() {
    let ctx = MemoryContext::new("t");
    let mut parent = make_parsestate(ctx.mcx(), None);
    assert_eq!(parent.p_next_resno, 1);
    assert!(parent.p_resolve_unknowns);
    assert!(parent.parentParseState.is_none());

    parent.p_sourcetext = Some(b"select $1");
    let carrier = VarParamState::new();
    setup_parse_variable_parameters(&mut parent, carrier.clone());

    let child = make_parsestate(ctx.mcx(), Some(&parent));
    assert_eq!(child.p_sourcetext, Some(b"select $1".as_slice()));
    let child_carrier = child.p_ref_hook_state.as_var_params().unwrap();
    // C aliases p_ref_hook_state into the child: same shared array.
    assert!(alloc::rc::Rc::ptr_eq(&carrier.param_types, &child_carrier.param_types));
}

#[test]
fn free_parsestate_checks_resno_limit() {
    let ctx = MemoryContext::new("t");
    let pstate = make_parsestate(ctx.mcx(), None);
    free_parsestate(pstate).unwrap();

    let mut pstate = make_parsestate(ctx.mcx(), None);
    pstate.p_next_resno = MaxTupleAttributeNumber + 2;
    let err = free_parsestate(pstate).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_TOO_MANY_COLUMNS);
}

#[test]
fn parser_errposition_via_pstate() {
    let ctx = MemoryContext::new("t");
    let mut pstate = make_parsestate(ctx.mcx(), None);
    assert_eq!(crate::parser_errposition(&pstate, 3, PG_UTF8), 0);
    pstate.p_sourcetext = Some("s\u{00e9}lect 1".as_bytes());
    assert_eq!(crate::parser_errposition(&pstate, 3, PG_UTF8), 3);
    assert_eq!(crate::parser_errposition(&pstate, -1, PG_UTF8), 0);
}

#[test]
fn make_const_natural_types() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let pstate = make_parsestate(mcx, None);

    let null = A_Const { val: None, location: 5 };
    let con = make_const(mcx, &pstate, &null).unwrap();
    let con = con.as_const().unwrap();
    assert!(con.constisnull);
    assert_eq!(
        (con.consttype, con.consttypmod, con.constlen, con.constbyval, con.location),
        (UNKNOWNOID, -1, -2, false, 5)
    );

    let int = A_Const { val: Some(ValUnion::Integer(Integer { ival: -42 })), location: 1 };
    let con = make_const(mcx, &pstate, &int).unwrap();
    let con = con.as_const().unwrap();
    assert_eq!((con.consttype, con.constlen, con.constbyval), (INT4OID, 4, true));
    assert_eq!(con.constvalue.as_i32(), -42);

    // "Float" that is an oversize integer fitting int32 / int64.
    let f32fit =
        A_Const { val: Some(ValUnion::Float(Float { fval: "2147483647" })), location: 2 };
    let con = make_const(mcx, &pstate, &f32fit).unwrap();
    assert_eq!(con.as_const().unwrap().consttype, INT4OID);

    let f64fit =
        A_Const { val: Some(ValUnion::Float(Float { fval: "3000000000" })), location: 2 };
    let con = make_const(mcx, &pstate, &f64fit).unwrap();
    let con = con.as_const().unwrap();
    assert_eq!((con.consttype, con.constlen, con.constbyval), (INT8OID, 8, true));
    assert_eq!(con.constvalue.as_i64(), 3_000_000_000);

    let b = A_Const { val: Some(ValUnion::Boolean(Boolean { boolval: true })), location: 3 };
    let con = make_const(mcx, &pstate, &b).unwrap();
    let con = con.as_const().unwrap();
    assert_eq!((con.consttype, con.constlen, con.constbyval), (BOOLOID, 1, true));
    assert!(con.constvalue.as_bool());

    let s = A_Const {
        val: Some(ValUnion::String(types_nodes::node_tree::String { sval: "abc" })),
        location: 4,
    };
    let con = make_const(mcx, &pstate, &s).unwrap();
    let con = con.as_const().unwrap();
    assert_eq!((con.consttype, con.constlen, con.constbyval), (UNKNOWNOID, -2, false));
    let bytes =
        unsafe { core::slice::from_raw_parts(con.constvalue.as_usize() as *const u8, 4) };
    assert_eq!(bytes, b"abc\0");
}

#[test]
fn make_const_numeric_literal_uses_numeric_in() {
    let ctx = MemoryContext::new("t");
    let pstate = make_parsestate(ctx.mcx(), None);
    let f = A_Const { val: Some(ValUnion::Float(Float { fval: "1.5" })), location: 0 };
    let node = make_const(ctx.mcx(), &pstate, &f).unwrap();
    let c = node.as_const().unwrap();
    assert_eq!(c.consttype, types_core::catalog::NUMERICOID);
    assert_eq!(c.constlen, -1);
    assert!(!c.constbyval && !c.constisnull);
    let expect = adt_numeric::io::numeric_in("1.5", -1, None).unwrap().unwrap();
    let img = expect.as_bytes();
    // SAFETY: the const datum points at a live numeric varlena of img.len() bytes.
    let got = unsafe {
        core::slice::from_raw_parts(c.constvalue.as_usize() as *const u8, img.len())
    };
    assert_eq!(got, img);
}

#[test]
fn make_const_bitstring_literal() {
    let ctx = MemoryContext::new("t");
    let pstate = make_parsestate(ctx.mcx(), None);
    let b = A_Const {
        val: Some(ValUnion::BitString(types_nodes::node_tree::BitString { bsval: "b101" })),
        location: 0,
    };
    let con = make_const(ctx.mcx(), &pstate, &b).unwrap();
    let con = con.as_const().unwrap();
    assert_eq!((con.consttype, con.constlen, con.constbyval), (1560, -1, false));
    assert!(!con.constisnull);
}

#[test]
fn fixed_paramref_hook_resolves_and_rejects() {
    install_type_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let types = [TEXTOID, InvalidOid];
    setup_parse_fixed_parameters(&mut pstate, &types);

    let node =
        fixed_paramref_hook(mcx, &pstate, &ParamRef { number: 1, location: 7 }, PG_UTF8).unwrap();
    let param = node.as_param().unwrap();
    assert_eq!(param.paramkind, ParamKind::PARAM_EXTERN);
    assert_eq!(
        (param.paramid, param.paramtype, param.paramtypmod, param.paramcollid, param.location),
        (1, TEXTOID, -1, 100, 7)
    );

    for number in [0, 2, 3] {
        let err = fixed_paramref_hook(
            mcx,
            &pstate,
            &ParamRef { number, location: -1 },
            PG_UTF8,
        )
        .unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_PARAMETER);
    }
}

#[test]
fn variable_paramref_hook_grows_shared_array() {
    install_type_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let carrier = VarParamState::new();
    setup_parse_variable_parameters(&mut pstate, carrier.clone());

    let node =
        variable_paramref_hook(mcx, &pstate, &ParamRef { number: 3, location: 2 }, PG_UTF8)
            .unwrap();
    let param = node.as_param().unwrap();
    assert_eq!((param.paramid, param.paramtype), (3, UNKNOWNOID));
    assert_eq!(&*carrier.param_types.borrow(), &[InvalidOid, InvalidOid, UNKNOWNOID]);

    let err = variable_paramref_hook(mcx, &pstate, &ParamRef { number: 0, location: -1 }, PG_UTF8)
        .unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_PARAMETER);

    // JDBC hack: VOID param in a CALL argument reads as unknown.
    carrier.param_types.borrow_mut()[0] = VOIDOID;
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_CALL_ARGUMENT;
    let node =
        variable_paramref_hook(mcx, &pstate, &ParamRef { number: 1, location: 0 }, PG_UTF8)
            .unwrap();
    assert_eq!(node.as_param().unwrap().paramtype, UNKNOWNOID);
    assert_eq!(carrier.param_types.borrow()[0], UNKNOWNOID);
}

#[test]
fn variable_coerce_param_hook_backwrites_type() {
    install_type_fixture();
    let ctx = MemoryContext::new("t");
    let mut pstate = make_parsestate(ctx.mcx(), None);
    let carrier = VarParamState::new();
    carrier.param_types.borrow_mut().extend_from_slice(&[UNKNOWNOID]);
    setup_parse_variable_parameters(&mut pstate, carrier.clone());

    let mut param = types_nodes::Param {
        paramkind: ParamKind::PARAM_EXTERN,
        paramid: 1,
        paramtype: UNKNOWNOID,
        paramtypmod: -1,
        paramcollid: InvalidOid,
        location: 9,
    };
    assert!(variable_coerce_param_hook(&pstate, &mut param, TEXTOID, 44, 4, PG_UTF8).unwrap());
    assert_eq!(carrier.param_types.borrow()[0], TEXTOID);
    assert_eq!((param.paramtype, param.paramtypmod, param.paramcollid), (TEXTOID, -1, 100));
    // Leftmost of the param's and coercion's locations.
    assert_eq!(param.location, 4);

    // Re-coercion to the same type is accepted.
    param.paramtype = UNKNOWNOID;
    assert!(variable_coerce_param_hook(&pstate, &mut param, TEXTOID, -1, -1, PG_UTF8).unwrap());

    // Non-extern / known params fall through to normal coercion.
    param.paramkind = ParamKind::PARAM_EXEC;
    assert!(!variable_coerce_param_hook(&pstate, &mut param, TEXTOID, -1, -1, PG_UTF8).unwrap());

    // A conflicting re-coercion is the C ereport, detail via format_type_be.
    param.paramkind = ParamKind::PARAM_EXTERN;
    param.paramtype = UNKNOWNOID;
    let err =
        variable_coerce_param_hook(&pstate, &mut param, INT4OID, -1, -1, PG_UTF8).unwrap_err();
    assert_eq!(err.message(), "inconsistent types deduced for parameter $1");
    assert_eq!(err.detail(), Some("text versus integer"));
}

#[test]
fn transform_container_type_smashes_domains_and_vectors() {
    install_type_fixture();
    let mut ty = INT2VECTOROID;
    let mut typmod = -1;
    transformContainerType(&mut ty, &mut typmod).unwrap();
    assert_eq!((ty, typmod), (INT2ARRAYOID, -1));

    let mut ty = DOMAIN_OID;
    let mut typmod = -1;
    transformContainerType(&mut ty, &mut typmod).unwrap();
    assert_eq!((ty, typmod), (TEXTOID, DOMAIN_BASE_TYPMOD));
}

#[test]
fn enr_lookup_through_pstate() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut env = queryenvironment::create_queryEnv(mcx);
    queryenvironment::register_ENR(
        &mut env,
        queryenvironment::EphemeralNamedRelationData {
            md: queryenvironment::EphemeralNamedRelationMetadataData {
                name: mcx::PgString::from_str_in("new_rows", mcx).unwrap(),
                reliddesc: 1234,
                tupdesc: None,
                enrtype: queryenvironment::ENR_NAMED_TUPLESTORE,
                enrtuples: 3.0,
            },
            reldata: types_portal::TuplestoreHandle(1),
        },
    )
    .unwrap();

    let mut pstate = make_parsestate(mcx, None);
    assert!(!name_matches_visible_ENR(&pstate, "new_rows"));
    pstate.p_queryEnv = Some(&env);
    assert!(name_matches_visible_ENR(&pstate, "new_rows"));
    assert!(!name_matches_visible_ENR(&pstate, "old_rows"));
    assert_eq!(get_visible_ENR(&pstate, "new_rows").unwrap().reliddesc, 1234);
}

fn query_with_param<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    paramtype: Oid,
    paramkind: ParamKind,
) -> &'mcx types_nodes::parsenodes::Query<'mcx> {
    use types_nodes::{Node, NodeList};
    let param = Node::mk(
        mcx,
        types_nodes::Param {
            paramkind,
            paramid: 1,
            paramtype,
            paramtypmod: -1,
            paramcollid: 0,
            location: 3,
        },
    )
    .unwrap();
    let te = Node::mk_target_entry(mcx, param, 1, None, false).unwrap();
    let query = types_nodes::parsenodes::Query {
        targetList: NodeList::from_slice(mcx, &[te]).unwrap(),
        ..Default::default()
    };
    Node::mk_mut(mcx, query).unwrap().seal_ref()
}

#[test]
fn check_variable_parameters_passes_and_flags_mismatch() {
    use types_error::ERRCODE_AMBIGUOUS_PARAMETER;

    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let carrier = VarParamState::new();
    setup_parse_variable_parameters(&mut pstate, carrier.clone());

    // No params generated: vacuous.
    let q = query_with_param(mcx, INT4OID, ParamKind::PARAM_EXTERN);
    check_variable_parameters(&pstate, q, PG_UTF8).unwrap();

    carrier.param_types.borrow_mut().push(INT4OID);
    check_variable_parameters(&pstate, q, PG_UTF8).unwrap();

    let mismatched = query_with_param(mcx, TEXTOID, ParamKind::PARAM_EXTERN);
    let err = check_variable_parameters(&pstate, mismatched, PG_UTF8).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_AMBIGUOUS_PARAMETER);
}

#[test]
fn check_variable_parameters_rejects_unknown_paramno() {

    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let carrier = VarParamState::new();
    setup_parse_variable_parameters(&mut pstate, carrier.clone());
    carrier.param_types.borrow_mut().push(INT4OID);

    let q = query_with_param(mcx, INT4OID, ParamKind::PARAM_EXTERN);
    // Shrink after building so paramid 1 is out of range.
    carrier.param_types.borrow_mut().clear();
    carrier.param_types.borrow_mut().push(INT4OID);
    let two = {
        use types_nodes::{Node, NodeList};
        let param = Node::mk(
            mcx,
            types_nodes::Param {
                paramkind: ParamKind::PARAM_EXTERN,
                paramid: 2,
                paramtype: INT4OID,
                paramtypmod: -1,
                paramcollid: 0,
                location: -1,
            },
        )
        .unwrap();
        let te = Node::mk_target_entry(mcx, param, 1, None, false).unwrap();
        let query = types_nodes::parsenodes::Query {
            targetList: NodeList::from_slice(mcx, &[te]).unwrap(),
            ..Default::default()
        };
        Node::mk_mut(mcx, query).unwrap().seal_ref()
    };
    check_variable_parameters(&pstate, q, PG_UTF8).unwrap();
    let err = check_variable_parameters(&pstate, two, PG_UTF8).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_PARAMETER);
}

#[test]
fn query_contains_extern_params_distinguishes_kinds() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let with_extern = query_with_param(mcx, INT4OID, ParamKind::PARAM_EXTERN);
    assert!(query_contains_extern_params(with_extern).unwrap());
    let with_exec = query_with_param(mcx, INT4OID, ParamKind::PARAM_EXEC);
    assert!(!query_contains_extern_params(with_exec).unwrap());
}

#[test]
#[should_panic(expected = "SubscriptingRef")]
fn transform_container_subscripts_is_deferred() {
    transformContainerSubscripts();
}

// Keep the ref-hook enum honest: default is None (no hooks installed).
#[test]
fn ref_hook_state_defaults_none() {
    let ctx = MemoryContext::new("t");
    let pstate = make_parsestate(ctx.mcx(), None);
    assert!(matches!(pstate.p_ref_hook_state, ParseRefHookState::None));
    assert!(pstate.p_ref_hook_state.as_fixed_params().is_none());
    assert!(pstate.p_ref_hook_state.as_var_params().is_none());
}
