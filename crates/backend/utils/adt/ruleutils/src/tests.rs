use super::*;
use datum::Datum;
use mcx::MemoryContext;
use std::sync::Once;
use types_core::{BOOLOID, INT4OID, TEXTOID};
use types_error::ERRCODE_INVALID_PARAMETER_VALUE;
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
    let err = pg_get_expr_worker(ctx.mcx(), BOOL_NULLTEST, 0, PRETTYFLAG_INDENT).unwrap_err();
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

#[test]
fn simple_quote_literal_doubles_quotes() {
    let mut buf = String::new();
    deparse::simple_quote_literal(&mut buf, "it's");
    assert_eq!(buf, "'it''s'");
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
    dctx.buf
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
