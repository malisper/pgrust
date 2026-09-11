use super::*;
use datum::Datum;
use mcx::MemoryContext;
use std::sync::Once;
use types_core::{BOOLOID, FUNC_MAX_ARGS, INT4OID, TEXTOID};
use types_error::{
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_TOO_MANY_ARGUMENTS,
};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_tuple::{PgTypeShape, TYPALIGN_INT, TYPSTORAGE_EXTENDED, TYPSTORAGE_PLAIN};

const VARCHAROID: Oid = 1043;
const REL_OID: Oid = 5001;
const F_INT4OUT: Oid = 43;
const F_BOOLOUT: Oid = 1244;
const F_TEXTOUT: Oid = 47;
const F_VARCHAROUT: Oid = 1045;

std::thread_local! {
    static OUT: core::cell::RefCell<Vec<u8>> = const { core::cell::RefCell::new(Vec::new()) };
    static REWRITE_LOCK_CALLS: core::cell::RefCell<Vec<(bool, bool)>> =
        const { core::cell::RefCell::new(Vec::new()) };
}

fn record_rewrite_locks<'mcx>(
    _mcx: Mcx<'mcx>,
    _query: &types_nodes::parsenodes::Query<'mcx>,
    for_execute: bool,
    for_update_pushed_down: bool,
) -> PgResult<()> {
    REWRITE_LOCK_CALLS.with(|c| c.borrow_mut().push((for_execute, for_update_pushed_down)));
    Ok(())
}

fn cstring_out(bytes: &[u8]) -> Datum {
    OUT.with(|c| {
        let mut b = c.borrow_mut();
        b.clear();
        b.extend_from_slice(bytes);
        b.push(0);
        Datum::from_usize(b.as_ptr() as usize)
    })
}

fn fake_int4out(_f: Option<&mut FmgrInfo>, fc: &mut Fcinfo) -> PgResult<Datum> {
    Ok(cstring_out(fc.arg_i32(0).to_string().as_bytes()))
}

fn fake_boolout(_f: Option<&mut FmgrInfo>, fc: &mut Fcinfo) -> PgResult<Datum> {
    Ok(cstring_out(if fc.arg_bool(0) { b"t" } else { b"f" }))
}

fn fake_varlena_out(_f: Option<&mut FmgrInfo>, fc: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: test consts carry inline uncompressed varlena images.
    let data = unsafe { fc.arg_varlena_packed(0) }.unwrap().data().to_vec();
    Ok(cstring_out(&data))
}

fn name(s: &str) -> types_tuple::NameData {
    let mut n = types_tuple::NameData::default();
    n.namestrcpy(s);
    n
}

fn type_shape(typid: Oid) -> Option<PgTypeShape> {
    match typid {
        INT4OID => Some(PgTypeShape {
            typlen: 4,
            typbyval: true,
            typalign: TYPALIGN_INT,
            typstorage: TYPSTORAGE_PLAIN,
            typcollation: 0,
        }),
        BOOLOID => Some(PgTypeShape {
            typlen: 1,
            typbyval: true,
            typalign: b'c' as i8,
            typstorage: TYPSTORAGE_PLAIN,
            typcollation: 0,
        }),
        TEXTOID | VARCHAROID => Some(PgTypeShape {
            typlen: -1,
            typbyval: false,
            typalign: TYPALIGN_INT,
            typstorage: TYPSTORAGE_EXTENDED,
            typcollation: 100,
        }),
        _ => None,
    }
}

static SEAMS: Once = Once::new();

fn install() {
    // simple_quote_literal (reached via get_const_expr for string constants)
    // reads the standard_conforming_strings GUC; make it available.
    install_scs();
    SEAMS.call_once(|| {
        use syscache_seams as s;
        s::lookup_pg_type_shape::set(|typid| Ok(type_shape(typid)));
        s::pg_type_typtype::set(|typid| Ok(type_shape(typid).map(|_| b'b' as i8)));
        s::pg_type_io_shape::set(|typid| {
            let out = match typid {
                INT4OID => F_INT4OUT,
                BOOLOID => F_BOOLOUT,
                TEXTOID => F_TEXTOUT,
                VARCHAROID => F_VARCHAROUT,
                _ => return Ok(None),
            };
            let sh = type_shape(typid).unwrap();
            Ok(Some(s::PgTypeIoShape {
                oid: typid,
                typinput: 1,
                typoutput: out,
                typreceive: 1,
                typsend: 1,
                typmodin: 0,
                typmodout: 0,
                typelem: 0,
                typlen: sh.typlen,
                typbyval: sh.typbyval,
                typalign: sh.typalign,
                typdelim: b',' as i8,
                typisdefined: true,
            }))
        });
        s::lookup_pg_type_typcache_shape::set(|typid| {
            let sh = match type_shape(typid) {
                Some(sh) => sh,
                None => return Ok(None),
            };
            let tyname = match typid {
                INT4OID => "int4",
                BOOLOID => "bool",
                TEXTOID => "text",
                VARCHAROID => "varchar",
                _ => unreachable!(),
            };
            Ok(Some(syscache_seams::PgTypeTypcacheShape {
                typname: name(tyname),
                typlen: sh.typlen,
                typbyval: sh.typbyval,
                typalign: sh.typalign,
                typstorage: sh.typstorage,
                typtype: b'b' as i8,
                typisdefined: true,
                typrelid: 0,
                typsubscript: 0,
                typelem: 0,
                typarray: 0,
                typcollation: sh.typcollation,
            }))
        });
        s::lookup_pg_attribute_shape::set(|relid, attnum| {
            if relid != REL_OID {
                return Ok(None);
            }
            let attname = match attnum {
                1 => "id",
                2 => "qty",
                _ => return Ok(None),
            };
            Ok(Some(syscache_seams::PgAttributeLsShape {
                attname: name(attname),
                atttypid: INT4OID,
                atttypmod: -1,
                attcollation: 0,
                attgenerated: 0,
                attisdropped: false,
            }))
        });
        s::pg_class_relname::set(|relid| {
            Ok((relid == REL_OID).then(|| name("orders")))
        });
        s::lookup_pg_class_ls_shape::set(|relid| {
            Ok((relid == REL_OID).then(|| syscache_seams::PgClassLsShape {
                relnamespace: 2200,
                reltype: 0,
                relam: 0,
                reltablespace: 0,
                relnatts: 2,
                relkind: b'r' as i8,
                relpersistence: b'p' as i8,
                relispartition: false,
                relhassubclass: false,
            }))
        });
        namespace_seams::type_is_visible::set(|_| Ok(true));
        // AcquireRewriteLocks (rewriteHandler.c) as the deparser reaches it:
        // record every call and its (forExecute, forUpdatePushedDown) flags.
        rewrite_handler_seams::acquire_rewrite_locks::set(record_rewrite_locks);
        // CHECK_FOR_INTERRUPTS(): a pending cancel raises 57014, as
        // ProcessInterrupts does for QueryCancelPending.
        postgres_seams::check_for_interrupts::set(|| {
            if init_small::globals::InterruptPending() {
                Err(Box::new(
                    PgError::error("canceling statement due to user request")
                        .with_sqlstate(types_error::ERRCODE_QUERY_CANCELED),
                ))
            } else {
                Ok(())
            }
        });
        fmgr_seams::fmgr_info::set(|foid| {
            let f = match foid {
                F_INT4OUT => fake_int4out,
                F_BOOLOUT => fake_boolout,
                F_TEXTOUT | F_VARCHAROUT => fake_varlena_out,
                other => panic!("test fmgr_info: unexpected function {other}"),
            };
            Ok(FmgrInfo::new(f, foid, 1, true, false))
        });
    });
}

fn expr(nodetree: &str, relid: Oid, pretty: bool) -> String {
    install();
    let ctx = MemoryContext::new("ruleutils test");
    pg_get_expr_worker(ctx.mcx(), nodetree, relid, get_pretty_flags(pretty))
        .unwrap()
        .unwrap()
}

// Node-tree fixtures and expected strings captured from live C PG 18.3
// (initdb scratch cluster; see the crate's audit notes).
const CONST_NEG5: &str = "{CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 \
     :constbyval true :constisnull false :location -1 :constvalue 4 [ -5 -1 -1 -1 -1 -1 -1 -1 ]}";
const CONST_TRUE: &str = "{CONST :consttype 16 :consttypmod -1 :constcollid 0 :constlen 1 \
     :constbyval true :constisnull false :location -1 :constvalue 1 [ 1 0 0 0 0 0 0 0 ]}";
const CONST_ITS: &str = "{CONST :consttype 25 :consttypmod -1 :constcollid 100 :constlen -1 \
     :constbyval false :constisnull false :location -1 :constvalue 8 [ 32 0 0 0 105 116 39 115 ]}";
const CONST_AB_VARCHAR: &str = "{CONST :consttype 1043 :consttypmod -1 :constcollid 100 \
     :constlen -1 :constbyval false :constisnull false :location -1 \
     :constvalue 6 [ 24 0 0 0 97 98 ]}";
const COERCE_42_TEXT: &str = "{COERCEVIAIO :arg {CONST :consttype 23 :consttypmod -1 \
     :constcollid 0 :constlen 4 :constbyval true :constisnull false :location -1 \
     :constvalue 4 [ 42 0 0 0 0 0 0 0 ]} :resulttype 25 :resultcollid 100 :coerceformat 1 \
     :location -1}";
const BOOL_NULLTEST: &str = "{BOOLEXPR :boolop and :args ({NULLTEST :arg {VAR :varno 1 \
     :varattno 1 :vartype 23 :vartypmod -1 :varcollid 0 :varnullingrels (b) :varlevelsup 0 \
     :varreturningtype 0 :varnosyn 1 :varattnosyn 1 :location -1} :nulltesttype 1 \
     :argisrow false :location -1} {BOOLEXPR :boolop not :args ({NULLTEST :arg {VAR :varno 1 \
     :varattno 2 :vartype 23 :vartypmod -1 :varcollid 0 :varnullingrels (b) :varlevelsup 0 \
     :varreturningtype 0 :varnosyn 1 :varattnosyn 2 :location -1} :nulltesttype 0 \
     :argisrow false :location -1}) :location -1}) :location -1}";

#[test]
fn const_deparse_matches_c() {
    assert_eq!(expr(CONST_NEG5, 0, false), "'-5'::integer");
    assert_eq!(expr(CONST_NEG5, 0, true), "'-5'::integer");
    assert_eq!(expr(CONST_TRUE, 0, false), "true");
    assert_eq!(expr(CONST_ITS, 0, false), "'it''s'::text");
    assert_eq!(expr(CONST_AB_VARCHAR, 0, false), "'ab'::character varying");
}

#[test]
fn coercion_deparse_matches_c() {
    assert_eq!(expr(COERCE_42_TEXT, 0, false), "(42)::text");
    assert_eq!(expr(COERCE_42_TEXT, 0, true), "42::text");
}

// upstream 0ddd9098a310 (18.6): the parser accepts any string as an EXTRACT()
// field name (extract_arg: IDENT | ... | Sconst), so deparse quotes it like an
// identifier; a bare emit let a crafted field inject SQL into pg_get_viewdef().
// Expected strings captured from live C PG 18.6 (pg_get_viewdef over a view
// built with each field spelling).
const EXTRACT_FUNCEXPR_HEAD: &str = "{FUNCEXPR :funcid 6202 :funcresulttype 1700 \
     :funcretset false :funcvariadic false :funcformat 3 :funccollid 0 :inputcollid 100 :args (";
const CONST_TEXT_YEAR: &str = "{CONST :consttype 25 :consttypmod -1 :constcollid 100 :constlen -1 \
     :constbyval false :constisnull false :location -1 :constvalue 8 [ 32 0 0 0 121 101 97 114 ]}";
const CONST_TEXT_YEAR_MIXED: &str = "{CONST :consttype 25 :consttypmod -1 :constcollid 100 \
     :constlen -1 :constbyval false :constisnull false :location -1 \
     :constvalue 8 [ 32 0 0 0 89 101 97 114 ]}";
const CONST_TEXT_INJECT: &str = "{CONST :consttype 25 :consttypmod -1 :constcollid 100 :constlen -1 \
     :constbyval false :constisnull false :location -1 \
     :constvalue 19 [ 76 0 0 0 121 101 97 114 32 70 82 79 77 32 120 41 32 45 45 ]}";
const CONST_TEXT_DQUOTE: &str = "{CONST :consttype 25 :consttypmod -1 :constcollid 100 :constlen -1 \
     :constbyval false :constisnull false :location -1 :constvalue 7 [ 28 0 0 0 97 34 98 ]}";

fn extract_expr(field: &str) -> String {
    expr(&format!("{EXTRACT_FUNCEXPR_HEAD}{field} {CONST_NEG5}) :location -1}}"), 0, false)
}

#[test]
fn extract_field_name_deparses_as_quoted_identifier() {
    assert_eq!(extract_expr(CONST_TEXT_YEAR), "EXTRACT(year FROM '-5'::integer)");
    assert_eq!(extract_expr(CONST_TEXT_YEAR_MIXED), "EXTRACT(\"Year\" FROM '-5'::integer)");
    assert_eq!(
        extract_expr(CONST_TEXT_INJECT),
        "EXTRACT(\"year FROM x) --\" FROM '-5'::integer)"
    );
    assert_eq!(extract_expr(CONST_TEXT_DQUOTE), "EXTRACT(\"a\"\"b\" FROM '-5'::integer)");
}

// deparse_expression_pretty directly: pg_get_expr_worker's relation probe
// needs a live catcache the unit tests don't boot.
fn expr_with_rel(nodetree: &str, pretty: bool) -> String {
    install();
    let ctx = MemoryContext::new("ruleutils test");
    let node = readfuncs::stringToNode(ctx.mcx(), nodetree).unwrap();
    deparse_expression_pretty(ctx.mcx(), node, REL_OID, false, get_pretty_flags(pretty)).unwrap()
}

#[test]
fn booltest_nulltest_deparse_matches_c() {
    assert_eq!(expr_with_rel(BOOL_NULLTEST, false), "((id IS NOT NULL) AND (NOT (qty IS NULL)))");
    assert_eq!(expr_with_rel(BOOL_NULLTEST, true), "id IS NOT NULL AND NOT qty IS NULL");
}

#[test]
fn expr_with_var_and_no_relation_errors() {
    install();
    let ctx = MemoryContext::new("ruleutils test");
    let err = pg_get_expr_worker(ctx.mcx(), BOOL_NULLTEST, 0, PRETTYFLAG_INDENT).err().unwrap();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
}

#[test]
fn pretty_flags_match_c() {
    assert_eq!(get_pretty_flags(false), PRETTYFLAG_INDENT);
    assert_eq!(get_pretty_flags(true), PRETTYFLAG_PAREN | PRETTYFLAG_INDENT | PRETTYFLAG_SCHEMA);
}

#[test]
fn quote_qualified() {
    assert_eq!(quote_qualified_identifier(None, "foo"), "foo");
    assert_eq!(quote_qualified_identifier(Some("public"), "t"), "public.t");
    assert_eq!(quote_qualified_identifier(Some("Wei rd"), "a\"b"), "\"Wei rd\".\"a\"\"b\"");
    assert_eq!(quote_qualified_identifier(None, "select"), "\"select\"");
}

// simple_quote_literal reads the standard_conforming_strings GUC; install a
// test-controllable accessor once, backed by a static the tests can flip.
static SCS: core::sync::atomic::AtomicBool = core::sync::atomic::AtomicBool::new(true);

fn install_scs() {
    use core::sync::atomic::Ordering;
    guc_tables::vars::standard_conforming_strings.install_if_absent(guc_tables::GucVarAccessors {
        get: || SCS.load(Ordering::Relaxed),
        set: |v| SCS.store(v, Ordering::Relaxed),
    });
}

#[test]
fn simple_quote_literal_doubles_quotes() {
    install_scs();
    let mut buf = String::new();
    deparse::simple_quote_literal(&mut buf, "it's");
    assert_eq!(buf, "'it''s'");
}

#[test]
fn simple_quote_literal_backslash_follows_standard_conforming_strings() {
    install_scs();
    let saved = guc_tables::vars::standard_conforming_strings.read();

    // standard_conforming_strings=on (the common case): backslashes are left
    // alone and no E'' prefix is emitted -- output must be unchanged.
    guc_tables::vars::standard_conforming_strings.write(true);
    let mut on = String::new();
    deparse::simple_quote_literal(&mut on, "a\\b'c");
    assert_eq!(on, "'a\\b''c'");

    // standard_conforming_strings=off: backslashes are doubled so the literal is
    // unambiguous to a reader that treats backslashes as escapes, closing the
    // SQL-injection hole. C never uses E'' here either.
    guc_tables::vars::standard_conforming_strings.write(false);
    let mut off = String::new();
    deparse::simple_quote_literal(&mut off, "a\\b'c");
    assert_eq!(off, "'a\\\\b''c'");

    guc_tables::vars::standard_conforming_strings.write(saved);
}

// ev_action fixtures + expected strings captured from live C PG 18.3
// (Homebrew, 2026-07-03): CREATE VIEW then pg_rewrite.ev_action /
// pg_get_viewdef.
fn deparse_view_action(action: &str, attnames: &[&str]) -> String {
    install();
    let ctx = MemoryContext::new("ruleutils viewdef test");
    let mcx = ctx.mcx();
    let node = readfuncs::stringToNode(mcx, action.trim_end()).unwrap();
    let q = node.as_list().unwrap().nth(0).as_query().unwrap();
    let mut dctx = deparse::DeparseContext::new(mcx, PRETTYFLAG_INDENT);
    dctx.wrap_column = 0;
    let rd = std::rc::Rc::new(attnames.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    query::get_query_def(q, &mut dctx, Some(rd), true).unwrap();
    dctx.buf.push(';');
    dctx.buf.into_inner()
}

#[test]
fn viewdef_select_1_matches_c() {
    let action = include_str!("fixtures/v1_action.txt");
    assert_eq!(deparse_view_action(action, &["?column?"]), " SELECT 1 AS \"?column?\";");
}

#[test]
fn viewdef_union_all_limit_matches_c() {
    let action = include_str!("fixtures/v11_action.txt");
    assert_eq!(
        deparse_view_action(action, &["two"]),
        " SELECT 2 AS two\nUNION ALL\n SELECT 3 AS two\n OFFSET 1\n LIMIT 1;"
    );
}

#[test]
fn rtable_name_dedup_appends_digits() {
    install();
    let ctx = MemoryContext::new("ruleutils dedup test");
    let mcx = ctx.mcx();
    let dpns = query::deparse_context_for(mcx, "orders", REL_OID).unwrap();
    assert_eq!(dpns.rtable_names, vec![Some("orders".to_string())]);
    assert_eq!(dpns.rtable_columns[0].colnames.len(), 2);
    assert_eq!(dpns.rtable_columns[0].colnames[0].as_deref(), Some("id"));
}

// Public issue #18: pg_get_expr over pg_rewrite.ev_qual's "<>" null-node
// marker. C's stringToNode returns NULL and get_rule_expr deparses NULL as
// nothing, so pg_get_expr returns the EMPTY STRING (not SQL NULL) — verified
// live on C 18.3 (146/147 fresh-catalog rows: is_null=f, is_empty=t). Base
// pgrust panicked at readfuncs lib.rs:37 instead.
#[test]
fn null_node_marker_deparses_to_empty_string() {
    install();
    let ctx = MemoryContext::new("ruleutils test");
    assert_eq!(
        pg_get_expr_worker(ctx.mcx(), "<>", 0, PRETTYFLAG_INDENT).unwrap(),
        Some(String::new())
    );
    // pg_get_expr_ext's pretty arm takes the same NULL-node path.
    assert_eq!(
        pg_get_expr_worker(ctx.mcx(), "<>", 0, get_pretty_flags(true)).unwrap(),
        Some(String::new())
    );
}

#[test]
fn unsupported_rule_event_type_is_ereport_0a000() {
    for ev in [b'1', b'2', b'3', b'4'] {
        assert!(super::ruledef::rule_event_keyword("r", ev).is_ok());
    }
    let err = super::ruledef::rule_event_keyword("r", b'9').err().unwrap();
    assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    assert_eq!(err.message(), "rule \"r\" has unsupported event type 57");
}

#[test]
fn deparse_too_many_arguments_is_ereport_54023() {
    super::deparse::check_deparse_nargs(FUNC_MAX_ARGS).unwrap();
    let err = super::deparse::check_deparse_nargs(FUNC_MAX_ARGS + 1).err().unwrap();
    assert_eq!(err.sqlstate(), ERRCODE_TOO_MANY_ARGUMENTS);
    assert_eq!(err.message(), "too many arguments");
}

// An Aggref as a stored view would carry it (C outfuncs field order), with
// `nargs` int4 aggargtypes and no argument expressions.
fn aggref_nodetree(nargs: usize) -> String {
    let types = vec!["23"; nargs].join(" ");
    format!(
        "{{AGGREF :aggfnoid 5002 :aggtype 20 :aggcollid 0 :inputcollid 0 :aggtranstype 0 \
         :aggargtypes (o {types}) :aggdirectargs <> :args <> :aggorder <> :aggdistinct <> \
         :aggfilter <> :aggstar false :aggvariadic false :aggkind n :aggpresorted false \
         :agglevelsup 0 :aggsplit 0 :aggno -1 :aggtransno -1 :location -1}}"
    )
}

// upstream 2a03f21daf59 (18.6): get_aggregate_argtypes' arity guard is reached
// from ruleutils too, so a stored view whose Aggref carries FUNC_MAX_ARGS
// argument types (made by an executable that did not enforce the limit)
// deparses to 54023 rather than into fixed-size argument arrays. Pre-fix the
// name lookup runs and reports the (unstubbed) function as a cache miss.
#[test]
fn agg_deparse_with_too_many_arguments_is_ereport_54023() {
    install();
    syscache_seams::pg_proc_proname::set(|_| Ok(None));
    let ctx = MemoryContext::new("ruleutils test");
    let deparse_err = |nargs: usize| {
        let node = readfuncs::stringToNode(ctx.mcx(), &aggref_nodetree(nargs)).unwrap();
        deparse_expression_pretty(ctx.mcx(), node, REL_OID, false, PRETTYFLAG_INDENT)
            .err()
            .unwrap()
    };
    let err = deparse_err(FUNC_MAX_ARGS);
    assert_eq!(err.sqlstate(), ERRCODE_TOO_MANY_ARGUMENTS);
    assert_eq!(err.message(), "aggregates cannot have more than 99 arguments");
    // FUNC_MAX_ARGS-1 passes the guard and reaches the function-name lookup.
    let err = deparse_err(FUNC_MAX_ARGS - 1);
    assert_eq!(err.message(), "cache lookup failed for function 5002");
}

// The tablespace probes in pg_get_indexdef_worker / pg_get_constraintdef and
// the ON CONSTRAINT probe in get_insert_query_def used to panic!.  C reports
// the constraint miss with elog(ERROR, "cache lookup failed for constraint
// %u") (ruleutils.c:7115) and dereferences get_tablespace_name's result
// unchecked; either way it never aborts the backend, so both are catchable
// XX000 here.
#[test]
fn ruleutils_cache_lookup_failures_are_catchable_xx000() {
    for (what, oid, expected) in [
        ("tablespace", 1663u32, "cache lookup failed for tablespace 1663"),
        ("constraint", 16384, "cache lookup failed for constraint 16384"),
    ] {
        let e = cache_lookup_failed(what, oid);
        assert_eq!(e.message(), expected);
        assert_eq!(e.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
        assert_eq!(e.level(), types_error::ERROR);
    }
}

mod serial_sequence {
    use super::*;

    const NS_PUBLIC: Oid = 2200;
    const REL_T_SER: Oid = 5002;
    const REL_T_SER_MIXED: Oid = 5003;

    fn install_name_lookups() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            miscinit_seams::get_user_id::set(|| 10);
            aclchk_seams::object_aclcheck::set(|_classid, _objid, _roleid, _mode| Ok(0));
            syscache_seams::lookup_pg_namespace_oid_by_name::set(|nspname| {
                Ok(if nspname == "public" { NS_PUBLIC } else { InvalidOid })
            });
            syscache_seams::lookup_pg_class_relid_by_name::set(|relname, nsp| {
                Ok(match (relname, nsp) {
                    ("t_ser", NS_PUBLIC) => REL_T_SER,
                    ("T_Ser", NS_PUBLIC) => REL_T_SER_MIXED,
                    _ => InvalidOid,
                })
            });
            syscache_seams::lookup_pg_attribute_attnum_by_name::set(|_relid, _attname| Ok(0));
        });
    }

    // C prints tablerv->relname (the dequoted, downcased last part), not the
    // raw text argument.
    #[test]
    fn undefined_column_names_the_parsed_relation() {
        install_name_lookups();
        let ctx = MemoryContext::new("t");
        let cases: [(&[u8], &[u8], &str); 5] = [
            (b"public.t_ser", b"nope", "column \"nope\" of relation \"t_ser\" does not exist"),
            (b" public . t_ser ", b"nope", "column \"nope\" of relation \"t_ser\" does not exist"),
            (b"PUBLIC.T_SER", b"nope", "column \"nope\" of relation \"t_ser\" does not exist"),
            (b"public.\"T_Ser\"", b"nope", "column \"nope\" of relation \"T_Ser\" does not exist"),
            (b"public.t_ser", b"\xE9", "column \"\u{FFFD}\" of relation \"t_ser\" does not exist"),
        ];
        for (table, column, msg) in cases {
            let err = pg_get_serial_sequence_worker(ctx.mcx(), table, column).unwrap_err();
            assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_COLUMN);
            assert_eq!(err.message(), msg);
        }
        let err = pg_get_serial_sequence_worker(ctx.mcx(), b"public.gone", b"x").unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_TABLE);
        assert_eq!(err.message(), "relation \"public.gone\" does not exist");
    }
}

// audit-18.6 b057 a186-candidate-fp-adt-ruleutils-p3-d749acd8397898cfc803-1 /
// p4-d5718746d4fe34cb7043-1: get_query_def (ruleutils.c:5635), get_setop_query
// (6422) and get_rule_expr (9263) CHECK_FOR_INTERRUPTS(), so a pending cancel
// stops a long deparse instead of being noticed only after it completes.
#[test]
fn deparse_checks_for_interrupts() {
    install();
    let ctx = MemoryContext::new("ruleutils test");
    let mcx = ctx.mcx();
    let node = readfuncs::stringToNode(mcx, CONST_NEG5).unwrap();
    let action = include_str!("fixtures/v11_action.txt");
    let q = readfuncs::stringToNode(mcx, action.trim_end()).unwrap();
    let q = q.as_list().unwrap().nth(0).as_query().unwrap();
    init_small::globals::SetInterruptPending(true);
    let expr = deparse_expression_pretty(mcx, node, REL_OID, false, PRETTYFLAG_INDENT);
    let mut dctx = deparse::DeparseContext::new(mcx, PRETTYFLAG_INDENT);
    dctx.wrap_column = 0;
    let query = query::get_query_def(q, &mut dctx, None, true);
    init_small::globals::SetInterruptPending(false);
    let err = expr.err().expect("pending interrupt cancels get_rule_expr");
    assert_eq!(err.sqlstate(), types_error::ERRCODE_QUERY_CANCELED);
    let err = query.err().expect("pending interrupt cancels get_query_def");
    assert_eq!(err.sqlstate(), types_error::ERRCODE_QUERY_CANCELED);
    // With no interrupt pending the same trees deparse.
    assert!(deparse_expression_pretty(mcx, node, REL_OID, false, PRETTYFLAG_INDENT).is_ok());
}

// audit-18.6 w2-009 a186-candidate-fp-adt-ruleutils-p3-ef9ce4957f7488166a94-1:
// get_query_def (ruleutils.c:5663) calls AcquireRewriteLocks(query, false,
// false) before deparsing -- AccessShareLock on every relation the query
// mentions and the dropped-column fix-up of JOIN joinaliasvars -- so the
// catalog reads that follow are consistent under concurrent DDL. pgrust
// deparsed the tree with no lock walk at all.
#[test]
fn get_query_def_acquires_rewrite_locks() {
    install();
    let ctx = MemoryContext::new("ruleutils test");
    let mcx = ctx.mcx();
    // v1: a single SELECT with no subqueries, so exactly one get_query_def
    // runs (a subquery RTE re-enters get_query_def and re-locks, as C does).
    let action = include_str!("fixtures/v1_action.txt");
    let q = readfuncs::stringToNode(mcx, action.trim_end()).unwrap();
    let q = q.as_list().unwrap().nth(0).as_query().unwrap();
    REWRITE_LOCK_CALLS.with(|c| c.borrow_mut().clear());
    let mut dctx = deparse::DeparseContext::new(mcx, PRETTYFLAG_INDENT);
    dctx.wrap_column = 0;
    query::get_query_def(q, &mut dctx, None, true).unwrap();
    let calls = REWRITE_LOCK_CALLS.with(|c| c.borrow().clone());
    assert_eq!(
        calls,
        vec![(false, false)],
        "get_query_def must AcquireRewriteLocks(query, false, false) exactly once per query"
    );
}

fn query_def_err(action: &str) -> Box<PgError> {
    install();
    let ctx = MemoryContext::new("ruleutils test");
    let mcx = ctx.mcx();
    let node = readfuncs::stringToNode(mcx, action.trim_end()).unwrap();
    let q = node.as_list().unwrap().nth(0).as_query().unwrap();
    let mut dctx = deparse::DeparseContext::new(mcx, PRETTYFLAG_INDENT);
    dctx.wrap_column = 0;
    query::get_query_def(q, &mut dctx, None, true).err().expect("get_query_def raises")
}

// audit-18.6 b057 a186-candidate-fp-adt-ruleutils-p3-586a69256ed80efd5a97-1:
// get_utility_query_def (ruleutils.c:7584) elog(ERROR)s on a non-NOTIFY
// utility statement in a stored rule action; never a panic.
#[test]
fn utility_query_other_than_notify_is_elog_error() {
    let action = include_str!("fixtures/v1_action.txt")
        .replacen(":commandType 1", ":commandType 6", 1)
        .replacen(":utilityStmt <>", &format!(":utilityStmt {CONST_NEG5}"), 1);
    let err = query_def_err(&action);
    assert_eq!(err.message(), "unexpected utility statement type");
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
}

// audit-18.6 b057 a186-candidate-fp-adt-ruleutils-p3-90468a68265716418828-1:
// get_select_query_def (ruleutils.c:6003) on LCS_NONE and get_setop_query
// (6513) on SETOP_NONE are elog(ERROR)s with C's messages, not panics.
#[test]
fn lcs_none_and_setop_none_are_elog_errors() {
    let action = include_str!("fixtures/v1_action.txt")
        .replacen(":hasForUpdate false", ":hasForUpdate true", 1)
        .replacen(
            ":rowMarks <>",
            ":rowMarks ({ROWMARKCLAUSE :rti 1 :strength 0 :waitPolicy 0 :pushedDown false})",
            1,
        );
    let err = query_def_err(&action);
    assert_eq!(err.message(), "unrecognized LockClauseStrength 0");
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);

    let action = include_str!("fixtures/v11_action.txt")
        .replacen("SETOPERATIONSTMT :op 1", "SETOPERATIONSTMT :op 0", 1);
    let err = query_def_err(&action);
    assert_eq!(err.message(), "unrecognized set op: 0");
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
}

// audit-18.6 b168 a186-candidate-fp-adt-ruleutils-p2-63752ef20d65ef4898f7-1:
// an unexpected node in the jointree fromlist is elog(ERROR, "unrecognized
// node type in jointree: %d") (ruleutils.c:4190 has_dangerous_join_using),
// never a panic. T_Const = 7 in nodetags.h.
#[test]
fn unexpected_jointree_node_is_elog_error() {
    let action = include_str!("fixtures/v1_action.txt")
        .replacen(":jointree {FROMEXPR :fromlist <>", &format!(":jointree {{FROMEXPR :fromlist ({CONST_NEG5})"), 1);
    assert!(action.contains(":fromlist ({CONST"), "fixture jointree not rewritten");
    let err = query_def_err(&action);
    assert_eq!(err.message(), "unrecognized node type in jointree: 7");
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
}

// audit-18.6 b168 a186-candidate-fp-adt-ruleutils-p3-c2298cc0c5be26a9d5b6-1:
// make_ruledef (ruleutils.c:5395-5397): stringToNode("<>") is NIL and an
// empty ev_action list is elog(ERROR, "invalid empty ev_action list").
#[test]
fn empty_ev_action_list_is_elog_error() {
    install();
    let ctx = MemoryContext::new("ruleutils empty ev_action");
    let mcx = ctx.mcx();
    let row = super::ruledef::PgRewriteRow {
        rulename: "r".to_string(),
        ev_class: REL_OID,
        ev_type: b'1',
        is_instead: false,
        ev_qual: "<>".to_string(),
        ev_action: "<>".to_string(),
    };
    let err = super::ruledef::make_ruledef(mcx, &row, 0).err().expect("make_ruledef raises");
    assert_eq!(err.message(), "invalid empty ev_action list");
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
}
