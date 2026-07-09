use std::sync::Once;

use mcx::MemoryContext;
use types_core::BOOTSTRAP_SUPERUSERID;
use types_nodes::list::NodeList;
use types_nodes::node_tree::Node;
use types_nodes::parsenodes::{DefElem, VariableSetKind, VariableSetStmt};
use types_nodes::rawnodes::ValUnion;

use crate::*;

fn test_parse_bool(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "on" | "true" | "yes" | "1" => Some(true),
        "off" | "false" | "no" | "0" => Some(false),
        _ => None,
    }
}

fn setup() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        std::env::set_var("PGRUST_TZDIR", "/usr/share/zoneinfo");
        guc_tables::init_seams();
        // SHOW ALL touches every enum GUC; stub the option sets whose owning
        // units (transam_xlog/dsm/aio) are not linked into this test binary.
        guc_tables::option_sets::archive_mode_options.install(&[]);
        guc_tables::option_sets::dynamic_shared_memory_options.install(&[]);
        guc_tables::option_sets::io_method_options.install(&[]);
        guc_tables::option_sets::wal_sync_method_options.install(&[]);
        elog::init_seams();
        guc::init_seams();
        variable::init_seams();
        pgtz::init_seams();
        xact_seams::is_in_parallel_mode::set(|| false);
        xact_seams::get_current_sub_transaction_id::set(|| 1);
        scalar_seams::parse_bool::set(test_parse_bool);
        aclchk_seams::pg_parameter_aclcheck_set::set(|_, _| Ok(true));
        mbutils_seams::get_database_encoding::set(mbutils::GetDatabaseEncoding);
        pqcomm_seams::pq_putmessage::set(|_, _| Ok(0));
        timestamp_seams::get_current_timestamp::set(|| 42);
        syscache_seams::lookup_pg_type_shape::set(|typid| match typid {
            types_core::TEXTOID => Ok(Some(types_tuple::PgTypeShape {
                typlen: -1,
                typbyval: false,
                typalign: b'i' as i8,
                typstorage: b'x' as i8,
                typcollation: 100,
            })),
            _ => Ok(None),
        });
    });
    // superuser_arg's bootstrap escape path makes user 10 a superuser here.
    miscinit::SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, 0);
    guc::initialize_guc_options().unwrap();
}

fn set_stmt<'mcx>(
    kind: VariableSetKind,
    name: Option<&'mcx str>,
    args: NodeList<'mcx>,
    is_local: bool,
) -> VariableSetStmt<'mcx> {
    VariableSetStmt { kind, name, args, jumble_args: false, is_local, location: -1 }
}

fn string_const<'mcx>(mcx: mcx::Mcx<'mcx>, s: &'mcx str) -> Node<'mcx> {
    Node::mk_a_const(
        mcx,
        Some(ValUnion::String(types_nodes::node_tree::String { sval: s })),
        -1,
    )
    .unwrap()
}

fn attr_name(desc: &types_tuple::TupleDescData<'_>, i: usize) -> String {
    std::str::from_utf8(desc.attr(i).attname.name_str()).unwrap().to_string()
}

#[test]
fn exec_set_variable_stmt_sets_and_resets() {
    setup();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    let mut args = NodeList::nil();
    args.lappend(mcx, string_const(mcx, "German, YMD")).unwrap();
    let stmt = set_stmt(VariableSetKind::VAR_SET_VALUE, Some("DateStyle"), args, false);
    ExecSetVariableStmt(&stmt, true).unwrap();
    assert_eq!(
        guc::store::get_string("DateStyle").unwrap().as_deref(),
        Some("German, YMD")
    );

    let stmt = set_stmt(VariableSetKind::VAR_RESET, Some("DateStyle"), NodeList::nil(), false);
    ExecSetVariableStmt(&stmt, true).unwrap();
    assert_eq!(
        guc::store::get_string("DateStyle").unwrap().as_deref(),
        Some("ISO, MDY")
    );

    let mut args = NodeList::nil();
    args.lappend(mcx, Node::mk_a_const(mcx, Some(ValUnion::Integer(types_nodes::node_tree::Integer { ival: 30000 })), -1).unwrap()).unwrap();
    let stmt = set_stmt(VariableSetKind::VAR_SET_VALUE, Some("statement_timeout"), args, false);
    ExecSetVariableStmt(&stmt, true).unwrap();
    assert_eq!(guc::store::get_int("statement_timeout"), Some(30000));

    let stmt = set_stmt(VariableSetKind::VAR_RESET_ALL, None, NodeList::nil(), false);
    ExecSetVariableStmt(&stmt, true).unwrap();
    assert_eq!(guc::store::get_int("statement_timeout"), Some(0));
}

#[test]
fn set_transaction_multi_routes_def_elems() {
    setup();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    let arg = string_const(mcx, "serializable");
    let item = Node::mk(
        mcx,
        DefElem {
            defnamespace: None,
            defname: Some("transaction_isolation"),
            arg: Some(arg),
            defaction: Default::default(),
            location: -1,
        },
    )
    .unwrap();
    let mut args = NodeList::nil();
    args.lappend(mcx, item).unwrap();
    let stmt = set_stmt(
        VariableSetKind::VAR_SET_MULTI,
        Some("SESSION CHARACTERISTICS"),
        args,
        false,
    );
    ExecSetVariableStmt(&stmt, true).unwrap();
    assert_eq!(
        guc::store::get_enum("default_transaction_isolation"),
        Some(types_core::XACT_SERIALIZABLE)
    );
    guc::ResetAllOptions();
}

#[test]
fn flatten_rejects_multiple_args_for_scalar_guc() {
    setup();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let args = [string_const(mcx, "a"), string_const(mcx, "b")];
    let err = flatten_set_variable_args("work_mem", &args).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_PARAMETER_VALUE);

    // DateStyle is GUC_LIST_INPUT: multiple args join with ", ".
    let flat = flatten_set_variable_args("DateStyle", &args).unwrap();
    assert_eq!(flat.as_deref(), Some("a, b"));
    assert_eq!(flatten_set_variable_args("work_mem", &[]).unwrap(), None);
}

#[test]
fn flatten_quotes_list_quote_identifiers() {
    setup();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let args = [
        string_const(mcx, "user"),
        string_const(mcx, "MixedCase"),
        string_const(mcx, "public"),
    ];
    // search_path is GUC_LIST_QUOTE: reserved keywords and non-vanilla
    // identifiers get quoted, plain ones pass through.
    let flat = flatten_set_variable_args("search_path", &args).unwrap();
    assert_eq!(flat.as_deref(), Some("\"user\", \"MixedCase\", public"));

    // DateStyle lacks GUC_LIST_QUOTE: no quoting even for keywords.
    let flat = flatten_set_variable_args("DateStyle", &[string_const(mcx, "user")]).unwrap();
    assert_eq!(flat.as_deref(), Some("user"));
}

#[test]
fn extract_set_current_reads_live_value() {
    setup();
    let stmt = set_stmt(
        VariableSetKind::VAR_SET_CURRENT,
        Some("DateStyle"),
        NodeList::nil(),
        false,
    );
    assert_eq!(ExtractSetVariableArgs(&stmt).unwrap().as_deref(), Some("ISO, MDY"));
    let stmt = set_stmt(VariableSetKind::VAR_RESET, Some("DateStyle"), NodeList::nil(), false);
    assert_eq!(ExtractSetVariableArgs(&stmt).unwrap(), None);
}

#[test]
fn result_desc_uses_canonical_name() {
    setup();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = GetPGVariableResultDesc(mcx, "datestyle").unwrap();
    assert_eq!(desc.natts, 1);
    assert_eq!(attr_name(&desc, 0), "DateStyle");

    let desc = GetPGVariableResultDesc(mcx, "ALL").unwrap();
    assert_eq!(desc.natts, 3);
    assert_eq!(attr_name(&desc, 0), "name");
    assert_eq!(attr_name(&desc, 2), "description");

    assert!(GetPGVariableResultDesc(mcx, "no_such_guc").is_err());
}

#[test]
fn show_all_rows_are_sorted_and_visible() {
    setup();
    let rows = show_all_guc_config_rows().unwrap();
    assert!(rows.len() > 300);
    for w in rows.windows(2) {
        assert_ne!(
            guc::guc_name_compare(&w[0].0, &w[1].0),
            std::cmp::Ordering::Greater
        );
    }
    assert!(rows.iter().any(|(n, v, _)| n == "DateStyle" && v.as_deref() == Some("ISO, MDY")));
    // NO_SHOW_ALL options are filtered.
    assert!(!rows.iter().any(|(n, _, _)| n == "default_with_oids"));
}

#[test]
fn show_builds_builtin_tupdesc_without_syscache() {
    setup();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    // ShowGUCConfigOption/ShowAllGUCConfig build their tupdesc with
    // TupleDescInitBuiltinEntry, not TupleDescInitEntry: a database-less
    // walsender has no pg_type syscache to look TEXTOID up in. Reproduce the
    // exact call each makes and check the ancillary fields land as
    // TupleDescInitBuiltinEntry's hardcoded TEXTOID shape, not merely as
    // whatever the (catalog-backed) syscache stub happens to return.
    let mut tupdesc = tupdesc::CreateTemplateTupleDesc(mcx, 1).unwrap();
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 1, "DateStyle", TEXTOID, -1, 0).unwrap();
    let attr = tupdesc.attr(0);
    assert_eq!(attr.atttypid, TEXTOID);
    assert_eq!(attr.attlen, -1);
    assert!(!attr.attbyval);
    assert_eq!(attr.attalign, b'i' as i8);
    assert_eq!(attr.atttypmod, -1);
    assert_eq!(attr.attcollation, types_core::DEFAULT_COLLATION_OID);

    // Same three TEXTOID columns for SHOW ALL.
    let mut tupdesc = tupdesc::CreateTemplateTupleDesc(mcx, 3).unwrap();
    for (i, name) in ["name", "setting", "description"].iter().enumerate() {
        tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, (i + 1) as i16, name, TEXTOID, -1, 0)
            .unwrap();
    }
    for i in 0..3 {
        let attr = tupdesc.attr(i);
        assert_eq!(attr.atttypid, TEXTOID);
        assert_eq!(attr.attlen, -1);
        assert!(!attr.attbyval);
        assert_eq!(attr.attalign, b'i' as i8);
        assert_eq!(attr.attcollation, types_core::DEFAULT_COLLATION_OID);
    }
}

#[test]
fn show_emits_through_tup_output() {
    setup();
    let ctx = MemoryContext::new("t");
    let mut dest = tcop_dest::CreateDestReceiver(types_dest::CommandDest::None);
    GetPGVariable(ctx.mcx(), "DateStyle", &mut dest).unwrap();
    GetPGVariable(ctx.mcx(), "all", &mut dest).unwrap();
}

fn text_image(s: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + s.len());
    v.extend_from_slice(&datum::varlena::set_varsize_4b(4 + s.len()));
    v.extend_from_slice(s.as_bytes());
    v
}

fn flags_of(mcx: mcx::Mcx<'_>, name: &str) -> Option<Vec<String>> {
    use types_fmgr::LocalFcinfo;
    let img = text_image(name);
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    // SAFETY: mcx outlives this call.
    unsafe { fcinfo.set_result_mcx(mcx) };
    fcinfo.set_arg(0, datum::Datum::from_usize(img.as_ptr() as usize));
    let d = fc_pg_settings_get_flags(None, &mut fcinfo).unwrap();
    if fcinfo.isnull {
        return None;
    }
    let p = d.as_usize() as *const u8;
    let img = unsafe { core::slice::from_raw_parts(p, arrayfuncs::foundation::varsize_any(p)) };
    let (elems, _) = arrayfuncs::deconstruct_array_builtin(mcx, img, types_core::TEXTOID, true)
        .unwrap();
    Some(
        elems
            .iter()
            .map(|d| {
                let p = d.as_usize() as *const u8;
                let bytes =
                    unsafe { datum::VarlenaRef::from_ptr(p) }.data().to_vec();
                String::from_utf8(bytes).unwrap()
            })
            .collect(),
    )
}

#[test]
fn pg_settings_get_flags_cases() {
    setup();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(flags_of(mcx, "enable_seqscan").as_deref(), Some(&["EXPLAIN".to_string()][..]));
    assert_eq!(flags_of(mcx, "no_such_guc_xyz"), None);
}
