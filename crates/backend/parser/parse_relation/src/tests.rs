use std::rc::Rc;
use std::sync::Once;

use mcx::{Mcx, MemoryContext, PgVec};
use parser_small1::{make_parsestate, ParseNamespaceItem, ParseState};
use types_core::catalog::{INT4OID, TEXTOID};
use types_core::{InvalidOid, Oid, RELPERSISTENCE_PERMANENT, INVALID_PROC_NUMBER};
use types_error::{
    PgResult, ERRCODE_AMBIGUOUS_ALIAS, ERRCODE_AMBIGUOUS_COLUMN, ERRCODE_DUPLICATE_ALIAS,
    ERRCODE_INVALID_COLUMN_REFERENCE, ERRCODE_UNDEFINED_COLUMN, ERRCODE_UNDEFINED_TABLE,
};
use types_nodes::parsenodes::ACL_SELECT;
use types_nodes::{Alias, Node, NodeList, RTEKind, RTEPermissionInfo};
use types_rel::{
    AccessShareLock, FormData_pg_class, LockInfoData, LockRelId, Relation, RelationData,
    LOCKMODE, RELKIND_RELATION, REPLICA_IDENTITY_DEFAULT,
};
use types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
use types_tuple::{FormData_pg_attribute, NameData};

use crate::*;

const T_OID: Oid = 101;
const U_OID: Oid = 102;
const D_OID: Oid = 103;

struct Col {
    name: &'static str,
    typid: Oid,
    typmod: i32,
    collation: Oid,
    dropped: bool,
}

const fn col(name: &'static str, typid: Oid, collation: Oid) -> Col {
    Col { name, typid, typmod: -1, collation, dropped: false }
}

static T_COLS: [Col; 2] = [col("x", INT4OID, InvalidOid), col("y", TEXTOID, 100)];
static U_COLS: [Col; 1] = [col("x", INT4OID, InvalidOid)];
static D_COLS: [Col; 3] = [
    col("a", INT4OID, InvalidOid),
    Col { name: "", typid: InvalidOid, typmod: -1, collation: InvalidOid, dropped: true },
    col("b", TEXTOID, 100),
];

fn entry(oid: Oid) -> Option<(&'static str, &'static [Col])> {
    match oid {
        T_OID => Some(("t", &T_COLS)),
        U_OID => Some(("u", &U_COLS)),
        D_OID => Some(("d", &D_COLS)),
        _ => None,
    }
}

fn make<'mcx>(mcx: Mcx<'mcx>, oid: Oid, name: &str, cols: &[Col]) -> Relation<'mcx> {
    let mut relname = NameData::default();
    relname.namestrcpy(name);
    let mut attrs = Vec::new();
    for (i, c) in cols.iter().enumerate() {
        let mut a = FormData_pg_attribute {
            attrelid: oid,
            atttypid: c.typid,
            attlen: if c.typid == INT4OID { 4 } else { -1 },
            attnum: i as i16 + 1,
            atttypmod: c.typmod,
            attbyval: c.typid == INT4OID,
            attalign: b'i' as i8,
            attstorage: b'p' as i8,
            attislocal: true,
            attisdropped: c.dropped,
            attcollation: c.collation,
            ..Default::default()
        };
        a.attname.namestrcpy(c.name);
        attrs.push(a);
    }
    let data = RelationData { rd_locator: Default::default(), rd_smgr: Default::default(),
        rd_id: oid,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: std::cell::Cell::new(true),
        rd_createSubid: std::cell::Cell::new(0),
        rd_newRelfilelocatorSubid: std::cell::Cell::new(0),
        rd_firstRelfilelocatorSubid: std::cell::Cell::new(0),
        rd_droppedSubid: std::cell::Cell::new(0),
        rd_lockInfo: LockInfoData { lockRelId: LockRelId { relId: oid, dbId: 5 } },
        rd_rel: FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: 2,
            relfilenode: oid,
            reltablespace: 0,
            relpages: 0,
            reltuples: -1.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence: RELPERSISTENCE_PERMANENT,
            relkind: RELKIND_RELATION,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: REPLICA_IDENTITY_DEFAULT,
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        },
        rd_att: Rc::new(tupdesc::CreateTupleDesc(mcx, &attrs).unwrap()),
        rd_index: None,
        rd_opcintype: PgVec::new_in(mcx),
        rd_opfamily: PgVec::new_in(mcx),
        rd_indoption: PgVec::new_in(mcx),
        rd_indcollation: PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: std::cell::Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
    };
    Relation::open(data, None)
}

fn by_name(relname: &str) -> Option<Oid> {
    [T_OID, U_OID, D_OID].into_iter().find(|&oid| entry(oid).unwrap().0 == relname)
}

fn fake_relation_open(mcx: Mcx<'_>, oid: Oid, _lockmode: LOCKMODE) -> PgResult<Relation<'_>> {
    let (name, cols) = entry(oid).expect("open of unknown oid");
    Ok(make(mcx, oid, name, cols))
}

fn fake_relation_openrv_extended<'mcx>(
    mcx: Mcx<'mcx>,
    rv: &rel_vocab::RangeVar,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<Option<Relation<'mcx>>> {
    match by_name(rv.relname) {
        Some(oid) => fake_relation_open(mcx, oid, lockmode).map(Some),
        None if missing_ok => Ok(None),
        None => Err(types_error::PgError::error("no such relation").into()),
    }
}

fn fake_range_var_get_relid(
    _mcx: Mcx<'_>,
    rv: &rel_vocab::RangeVar,
    _lockmode: LOCKMODE,
    _missing_ok: bool,
) -> PgResult<Oid> {
    Ok(by_name(rv.relname).unwrap_or(InvalidOid))
}

fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        relation_seams::relation_open::set(fake_relation_open);
        relation_seams::relation_openrv_extended::set(fake_relation_openrv_extended);
        namespace_seams::range_var_get_relid::set(fake_range_var_get_relid);
        mbutils_seams::pg_mbstrlen_with_len::set(mbutils::pg_mbstrlen_with_len);
        mbutils_seams::pg_mblen_range::set(mbutils::pg_mblen_range);
        // System columns exist in pg_attribute for every fixture relation but U_OID.
        syscache_seams::search_syscache_exists_attnum::set(|relid, attnum| {
            Ok(attnum < 0 && relid != U_OID)
        });
        table::init_seams();
    });
}

fn rv<'mcx>(mcx: Mcx<'mcx>, relname: &'mcx str, alias: Option<&'mcx Alias<'mcx>>) -> &'mcx types_nodes::RangeVar<'mcx> {
    Node::mk_mut(
        mcx,
        types_nodes::RangeVar {
            catalogname: None,
            schemaname: None,
            relname: Some(relname),
            inh: true,
            relpersistence: RELPERSISTENCE_PERMANENT,
            alias,
            location: 14,
        },
    )
    .unwrap()
    .seal_ref()
}

fn add<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    relname: &'mcx str,
    alias: Option<&'mcx Alias<'mcx>>,
) -> &'mcx mut parser_small1::ParseNamespaceItem<'mcx> {
    let r = rv(mcx, relname, alias);
    addRangeTableEntry(mcx, pstate, r, alias, r.inh, true).unwrap()
}

fn perminfo_of<'mcx>(nsitem: &parser_small1::ParseNamespaceItem<'mcx>) -> &'mcx RTEPermissionInfo<'mcx> {
    nsitem.p_perminfo.unwrap().as_rte_permission_info().unwrap()
}

#[test]
fn add_range_table_entry_builds_relation_rte() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let nsitem = add(mcx, &mut pstate, "t", None);

    assert_eq!(nsitem.p_rtindex, 1);
    let rte = nsitem.rte();
    assert_eq!(rte.rtekind, RTEKind::RTE_RELATION);
    assert_eq!(rte.relid, T_OID);
    assert!(rte.inh);
    assert_eq!(rte.relkind, RELKIND_RELATION);
    assert_eq!(rte.rellockmode, AccessShareLock);
    assert_eq!(rte.perminfoindex, 1);
    assert!(rte.alias.is_none());
    assert!(rte.inFromCl);
    assert!(!rte.lateral);

    let eref = rte.eref.unwrap();
    assert_eq!(eref.aliasname, Some("t"));
    let names: Vec<_> = eref.colnames.iter().map(|n| n.as_string().unwrap().sval).collect();
    assert_eq!(names, ["x", "y"]);

    assert_eq!(pstate.p_rtable.len(), 1);
    assert_eq!(pstate.p_rteperminfos.len(), 1);
    let perminfo = perminfo_of(nsitem);
    assert_eq!(perminfo.relid, T_OID);
    assert!(perminfo.inh);
    assert_eq!(perminfo.requiredPerms, ACL_SELECT);
    assert!(perminfo.selectedCols.is_empty());

    let cols = nsitem.p_nscolumns;
    assert_eq!(cols.len(), 2);
    assert_eq!((cols[0].p_varno, cols[0].p_varattno, cols[0].p_vartype), (1, 1, INT4OID));
    assert_eq!((cols[1].p_vartype, cols[1].p_varcollid), (TEXTOID, 100));
    assert!(nsitem.p_rel_visible && nsitem.p_cols_visible);
}

#[test]
fn alias_overrides_refname_and_colnames() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let mut colnames = NodeList::nil();
    colnames.lappend(mcx, Node::mk_string(mcx, "a").unwrap()).unwrap();
    let alias =
        Node::mk_mut(mcx, Alias { aliasname: Some("c"), colnames }).unwrap().seal_ref();

    let nsitem = add(mcx, &mut pstate, "t", Some(alias));
    let eref = nsitem.rte().eref.unwrap();
    assert_eq!(eref.aliasname, Some("c"));
    let names: Vec<_> = eref.colnames.iter().map(|n| n.as_string().unwrap().sval).collect();
    assert_eq!(names, ["a", "y"]);
    assert_eq!(nsitem.rte().alias.unwrap().aliasname, Some("c"));
}

#[test]
fn too_many_column_aliases_is_42p10() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let mut colnames = NodeList::nil();
    for n in ["a", "b"] {
        colnames.lappend(mcx, Node::mk_string(mcx, n).unwrap()).unwrap();
    }
    let alias =
        Node::mk_mut(mcx, Alias { aliasname: Some("c"), colnames }).unwrap().seal_ref();

    let r = rv(mcx, "u", Some(alias));
    let err =
        addRangeTableEntry(mcx, &mut pstate, r, Some(alias), true, true).map(|_| ()).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_COLUMN_REFERENCE);
    assert_eq!(err.message, "table \"c\" has 1 columns available but 2 columns specified");
}

#[test]
fn missing_relation_is_42p01() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_sourcetext = Some(b"SELECT x FROM nope");

    let r = rv(mcx, "nope", None);
    let err =
        addRangeTableEntry(mcx, &mut pstate, r, None, true, true).map(|_| ()).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_TABLE);
    assert_eq!(err.message, "relation \"nope\" does not exist");
    assert_eq!(err.cursor_position(), Some(15));
}

#[test]
fn col_name_to_var_builds_var_and_marks_select_priv() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let nsitem = add(mcx, &mut pstate, "t", None);
    pstate.p_namespace.push(nsitem);

    let node = colNameToVar(mcx, &pstate, "y", false, 7).unwrap().unwrap();
    let var = node.as_var().unwrap();
    assert_eq!((var.varno, var.varattno), (1, 2));
    assert_eq!((var.vartype, var.vartypmod, var.varcollid), (TEXTOID, -1, 100));
    assert_eq!(var.varlevelsup, 0);
    assert_eq!((var.varnosyn, var.varattnosyn), (1, 2));
    assert_eq!(var.location, 7);

    let perminfo = pstate.p_rteperminfos.nth(0).as_rte_permission_info().unwrap();
    assert_eq!(perminfo.requiredPerms, ACL_SELECT);
    assert!(perminfo.selectedCols.is_member(2 - FirstLowInvalidHeapAttributeNumber));

    assert!(colNameToVar(mcx, &pstate, "nope", false, 7).unwrap().is_none());
}

#[test]
fn ambiguous_column_across_tables_is_42702() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let n1 = add(mcx, &mut pstate, "t", None);
    pstate.p_namespace.push(n1);
    let n2 = add(mcx, &mut pstate, "u", None);
    pstate.p_namespace.push(n2);

    let err = colNameToVar(mcx, &pstate, "x", false, 7).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_AMBIGUOUS_COLUMN);
    assert_eq!(err.message, "column reference \"x\" is ambiguous");
}

#[test]
fn ambiguous_table_alias_is_42p09() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let alias = Node::mk_mut(mcx, Alias { aliasname: Some("z"), colnames: NodeList::nil() })
        .unwrap()
        .seal_ref();
    let n1 = add(mcx, &mut pstate, "t", Some(alias));
    pstate.p_namespace.push(n1);
    let n2 = add(mcx, &mut pstate, "u", Some(alias));
    pstate.p_namespace.push(n2);

    let err =
        refnameNamespaceItem(&pstate, None, "z", -1, None).map(|_| ()).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_AMBIGUOUS_ALIAS);
    assert_eq!(err.message, "table reference \"z\" is ambiguous");
}

#[test]
fn refname_namespace_item_finds_by_name() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let n1 = add(mcx, &mut pstate, "t", None);
    pstate.p_namespace.push(n1);

    let mut sublevels_up = -1;
    let found = refnameNamespaceItem(&pstate, None, "t", -1, Some(&mut sublevels_up))
        .unwrap()
        .unwrap();
    assert_eq!(found.p_rtindex, 1);
    assert_eq!(sublevels_up, 0);
    assert!(refnameNamespaceItem(&pstate, None, "nope", -1, None).unwrap().is_none());
}

#[test]
fn duplicate_alias_is_42712() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let alias = Node::mk_mut(mcx, Alias { aliasname: Some("z"), colnames: NodeList::nil() })
        .unwrap()
        .seal_ref();
    let n1 = add(mcx, &mut pstate, "t", Some(alias));
    pstate.p_namespace.push(n1);
    let n2 = add(mcx, &mut pstate, "u", Some(alias));

    let err = checkNameSpaceConflicts(pstate.p_namespace.as_slice(), &[n2])
        .unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DUPLICATE_ALIAS);
    assert_eq!(err.message, "table name \"z\" specified more than once");

    // Alias-less same-name relations in different schemas do not conflict;
    // same relid does.
    let mut p2 = make_parsestate(mcx, None);
    let a = add(mcx, &mut p2, "t", None);
    p2.p_namespace.push(a);
    let b = add(mcx, &mut p2, "t", None);
    let err = checkNameSpaceConflicts(p2.p_namespace.as_slice(), &[b]).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DUPLICATE_ALIAS);
}

#[test]
fn error_missing_column_exact_match_arm() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    // RTE exists in the rangetable but is not visible in the namespace.
    let _ = add(mcx, &mut pstate, "t", None);

    let err = errorMissingColumn(mcx, &pstate, None, "x", 7);
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_COLUMN);
    assert_eq!(err.message, "column \"x\" does not exist");
    assert_eq!(
        err.detail(),
        Some(
            "There is a column named \"x\" in table \"t\", but it cannot be referenced \
             from this part of the query."
        )
    );
}

#[test]
fn error_missing_rte_bald_arm() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let pstate = make_parsestate(mcx, None);

    let r = rv(mcx, "u", None);
    let err = errorMissingRTE(mcx, &pstate, r);
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_TABLE);
    assert_eq!(err.message, "missing FROM-clause entry for table \"u\"");
}

#[test]
fn error_missing_rte_alias_hint_arm() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let alias = Node::mk_mut(mcx, Alias { aliasname: Some("f"), colnames: NodeList::nil() })
        .unwrap()
        .seal_ref();
    let n1 = add(mcx, &mut pstate, "t", Some(alias));
    pstate.p_namespace.push(n1);

    let r = rv(mcx, "t", None);
    let err = errorMissingRTE(mcx, &pstate, r);
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_TABLE);
    assert_eq!(err.message, "invalid reference to FROM-clause entry for table \"t\"");
    assert_eq!(err.hint(), Some("Perhaps you meant to reference the table alias \"f\"."));
}

#[test]
fn expand_ns_item_attrs_builds_target_entries() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let nsitem = add(mcx, &mut pstate, "t", None);
    pstate.p_namespace.push(nsitem);
    let nsitem = pstate.p_namespace[0];

    let tes = expandNSItemAttrs(mcx, &mut pstate, nsitem, 0, true, 7).unwrap();
    assert_eq!(tes.len(), 2);
    let te0 = tes.nth(0).as_target_entry().unwrap();
    assert_eq!((te0.resno, te0.resname), (1, Some("x")));
    let v0 = te0.expr.as_var().unwrap();
    assert_eq!((v0.varno, v0.varattno, v0.vartype), (1, 1, INT4OID));
    let te1 = tes.nth(1).as_target_entry().unwrap();
    assert_eq!((te1.resno, te1.resname), (2, Some("y")));
    assert_eq!(pstate.p_next_resno, 3);

    let perminfo = pstate.p_rteperminfos.nth(0).as_rte_permission_info().unwrap();
    assert!(perminfo.selectedCols.is_member(1 - FirstLowInvalidHeapAttributeNumber));
    assert!(perminfo.selectedCols.is_member(2 - FirstLowInvalidHeapAttributeNumber));
}

#[test]
fn dropped_columns_skip_expansion_and_get_empty_eref_cells() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let nsitem = add(mcx, &mut pstate, "d", None);
    let eref_names: Vec<_> =
        nsitem.rte().eref.unwrap().colnames.iter().map(|n| n.as_string().unwrap().sval).collect();
    assert_eq!(eref_names, ["a", "", "b"]);
    assert_eq!(nsitem.p_nscolumns[1].p_varno, 0);

    pstate.p_namespace.push(nsitem);
    let nsitem = pstate.p_namespace[0];
    let (vars, colnames) = expandNSItemVars(mcx, &pstate, nsitem, 0, 7).unwrap();
    assert_eq!(vars.len(), 2);
    let names: Vec<_> = colnames.iter().map(|n| n.as_string().unwrap().sval).collect();
    assert_eq!(names, ["a", "b"]);
    assert_eq!(vars.nth(1).as_var().unwrap().varattno, 3);
}

#[test]
fn expand_rte_relation_arm_matches_ns_item_expansion() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let nsitem = add(mcx, &mut pstate, "d", None);
    let (colnames, colvars) = expandRTE(
        mcx,
        nsitem.rte(),
        1,
        0,
        types_nodes::VarReturningType::VAR_RETURNING_DEFAULT,
        -1,
        false,
    )
    .unwrap();
    let names: Vec<_> = colnames.iter().map(|n| n.as_string().unwrap().sval).collect();
    assert_eq!(names, ["a", "b"]);
    assert_eq!(colvars.len(), 2);
    assert_eq!(colvars.nth(1).as_var().unwrap().varattno, 3);
}

#[test]
fn add_ns_item_to_query_sets_flags_and_joinlist() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let nsitem = add(mcx, &mut pstate, "t", None);
    addNSItemToQuery(mcx, &mut pstate, nsitem, true, true, false).unwrap();

    assert_eq!(pstate.p_joinlist.len(), 1);
    assert_eq!(pstate.p_joinlist.nth(0).as_range_tbl_ref().unwrap().rtindex, 1);
    let item = pstate.p_namespace[0];
    assert!(item.p_rel_visible);
    assert!(!item.p_cols_visible);
}

#[test]
fn system_columns_resolve_with_catalog_types() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let nsitem = add(mcx, &mut pstate, "t", None);
    pstate.p_namespace.push(nsitem);

    let expect = [
        ("ctid", -1i16, types_core::catalog::TIDOID),
        ("xmin", -2, types_core::catalog::XIDOID),
        ("cmin", -3, types_core::catalog::CIDOID),
        ("xmax", -4, types_core::catalog::XIDOID),
        ("cmax", -5, types_core::catalog::CIDOID),
        ("tableoid", -6, types_core::catalog::OIDOID),
    ];
    for (name, attnum, typid) in expect {
        let node = colNameToVar(mcx, &pstate, name, false, 7).unwrap().unwrap();
        let var = node.as_var().unwrap();
        assert_eq!((var.varno, var.varattno), (1, attnum));
        assert_eq!((var.vartype, var.vartypmod, var.varcollid), (typid, -1, InvalidOid));
        assert_eq!((var.varnosyn, var.varattnosyn), (1, attnum));
    }

    let perminfo = pstate.p_rteperminfos.nth(0).as_rte_permission_info().unwrap();
    assert!(perminfo.selectedCols.is_member(-1 - FirstLowInvalidHeapAttributeNumber));

    // A user column alias shadows a system column name.
    let mut colnames = NodeList::nil();
    for n in ["ctid", "y2"] {
        colnames.lappend(mcx, Node::mk_string(mcx, n).unwrap()).unwrap();
    }
    let alias = Node::mk_mut(mcx, Alias { aliasname: Some("c"), colnames }).unwrap().seal_ref();
    let mut p2 = make_parsestate(mcx, None);
    let n2 = add(mcx, &mut p2, "t", Some(alias));
    p2.p_namespace.push(n2);
    let node = colNameToVar(mcx, &p2, "ctid", false, 7).unwrap().unwrap();
    assert_eq!(node.as_var().unwrap().varattno, 1);
}

#[test]
fn system_column_without_pg_attribute_row_is_no_match() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let nsitem = add(mcx, &mut pstate, "u", None);
    pstate.p_namespace.push(nsitem);
    assert!(colNameToVar(mcx, &pstate, "ctid", false, 7).unwrap().is_none());
}

#[test]
fn system_column_in_check_constraint_is_42p10() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_expr_kind = parser_small1::ParseExprKind::EXPR_KIND_CHECK_CONSTRAINT;

    let nsitem = add(mcx, &mut pstate, "t", None);
    pstate.p_namespace.push(nsitem);

    let err = colNameToVar(mcx, &pstate, "xmin", false, 7).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_COLUMN_REFERENCE);
    assert_eq!(err.message, "system column \"xmin\" reference in check constraint is invalid");
    let ok = colNameToVar(mcx, &pstate, "tableoid", false, 7).unwrap().unwrap();
    assert_eq!(ok.as_var().unwrap().varattno, -6);
}

#[test]
fn attname_attnum_sys_col_ok_returns_negative_attnum() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let rel = make(mcx, T_OID, "t", &T_COLS);
    assert_eq!(attnameAttNum(&rel, "x", true), 1);
    assert_eq!(attnameAttNum(&rel, "ctid", true), -1);
    assert_eq!(attnameAttNum(&rel, "ctid", false), 0);
    assert_eq!(attnameAttNum(&rel, "nope", true), 0);
}

#[test]
fn error_missing_column_single_fuzzy_hint() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let _ = add(mcx, &mut pstate, "d", None);

    let err = errorMissingColumn(mcx, &pstate, None, "aa", 7);
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_COLUMN);
    assert_eq!(err.message, "column \"aa\" does not exist");
    assert_eq!(err.hint(), Some("Perhaps you meant to reference the column \"d.a\"."));
}

#[test]
fn error_missing_column_two_fuzzy_hints() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let _ = add(mcx, &mut pstate, "d", None);

    let err = errorMissingColumn(mcx, &pstate, None, "ab", 7);
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_COLUMN);
    assert_eq!(
        err.hint(),
        Some(
            "Perhaps you meant to reference the column \"d.a\" or the column \"d.b\"."
        )
    );
}

#[test]
fn error_missing_column_three_equidistant_matches_gives_bald_error() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let _ = add(mcx, &mut pstate, "t", None);
    let _ = add(mcx, &mut pstate, "u", None);

    let err = errorMissingColumn(mcx, &pstate, None, "xy", 7);
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_COLUMN);
    assert_eq!(err.message, "column \"xy\" does not exist");
    assert_eq!(err.hint(), None);
}

#[test]
fn error_missing_column_no_candidates_is_bald_42703() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let _ = add(mcx, &mut pstate, "t", None);

    let err = errorMissingColumn(mcx, &pstate, None, "nosuchcol", 7);
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_COLUMN);
    assert_eq!(err.message, "column \"nosuchcol\" does not exist");
    assert_eq!(err.hint(), None);
    assert_eq!(err.detail(), None);
}

#[test]
fn error_missing_column_qualified_message_and_rte_penalty() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let _ = add(mcx, &mut pstate, "t", None);

    let err = errorMissingColumn(mcx, &pstate, Some("t"), "xx", 7);
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_COLUMN);
    assert_eq!(err.message, "column t.xx does not exist");
    assert_eq!(err.hint(), Some("Perhaps you meant to reference the column \"t.x\"."));
}

const F_COMPOSITE: Oid = 9001;
const F_SCALAR: Oid = 9002;
const COMPOSITE_TYPE: Oid = 9010;

fn install_func() {
    static INIT: Once = Once::new();
    install();
    INIT.call_once(|| {
        syscache_seams::lookup_pg_proc_shape::set(|funcid| {
            let shape = |prorettype| syscache_seams::PgProcShape {
                prolang: 12,
                prosecdef: false,
                proconfig_isnull: true,
                pronamespace: 11,
                prorettype,
                provariadic: InvalidOid,
                prosupport: InvalidOid,
                pronargs: 0,
                prokind: b'f' as i8,
                provolatile: b's' as i8,
                proparallel: b'r' as i8,
                proretset: true,
                proisstrict: false,
                proleakproof: false,
            };
            Ok(match funcid {
                F_COMPOSITE => Some(shape(COMPOSITE_TYPE)),
                F_SCALAR => Some(shape(INT4OID)),
                _ => None,
            })
        });
        syscache_seams::pg_proc_result_arrays::set(|_mcx, _funcid| {
            Ok(Some(syscache_seams::PgProcResultArraysShape {
                proallargtypes: None,
                proargmodes: None,
                proargnames: None,
            }))
        });
        syscache_seams::pg_type_typtype::set(|typid| {
            Ok(match typid {
                INT4OID | TEXTOID => Some(b'b' as i8),
                COMPOSITE_TYPE => Some(b'c' as i8),
                _ => None,
            })
        });
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok((typid == INT4OID).then(|| types_tuple::PgTypeShape {
                typlen: 4,
                typbyval: true,
                typalign: types_tuple::TYPALIGN_INT,
                typstorage: types_tuple::TYPSTORAGE_PLAIN,
                typcollation: InvalidOid,
            }))
        });
        typcache_seams::lookup_rowtype_tupdesc_copy::set(|mcx, type_id, typmod| {
            assert_eq!(type_id, COMPOSITE_TYPE);
            assert_eq!(typmod, -1);
            let mut a1 = FormData_pg_attribute {
                atttypid: INT4OID,
                attlen: 4,
                attnum: 1,
                atttypmod: -1,
                attbyval: true,
                attalign: b'i' as i8,
                attstorage: b'p' as i8,
                ..Default::default()
            };
            a1.attname.namestrcpy("c1");
            let mut a2 = FormData_pg_attribute {
                atttypid: TEXTOID,
                attlen: -1,
                attnum: 2,
                atttypmod: -1,
                attbyval: false,
                attalign: b'i' as i8,
                attstorage: b'x' as i8,
                attcollation: 100,
                ..Default::default()
            };
            a2.attname.namestrcpy("c2");
            let mut desc = tupdesc::CreateTupleDesc(mcx, &[a1, a2])?;
            desc.tdtypeid = COMPOSITE_TYPE;
            desc.tdtypmod = -1;
            Ok(desc)
        });
    });
}

fn func_expr<'mcx>(mcx: Mcx<'mcx>, funcid: Oid, funcresulttype: Oid) -> Node<'mcx> {
    Node::mk(
        mcx,
        types_nodes::FuncExpr {
            funcid,
            funcresulttype,
            funcretset: true,
            ..Default::default()
        },
    )
    .unwrap()
}

fn add_function_rte<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    name: &'mcx str,
    fe: Node<'mcx>,
) -> PgResult<&'mcx mut ParseNamespaceItem<'mcx>> {
    let mut fexprs = NodeList::nil();
    fexprs.lappend(mcx, fe)?;
    let nil: &NodeList<'mcx> = mcx::leak_in(mcx::alloc_in(mcx, NodeList::nil())?);
    let rf: &types_nodes::RangeFunction<'mcx> =
        mcx::leak_in(mcx::alloc_in(mcx, types_nodes::RangeFunction::default())?);
    addRangeTableEntryForFunction(mcx, pstate, &[name], fexprs, &[nil], rf, false, true)
}

#[test]
fn composite_function_rte_expands_rowtype_columns() {
    install_func();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let fe = func_expr(mcx, F_COMPOSITE, COMPOSITE_TYPE);
    let nsitem =
        add_function_rte(mcx, &mut pstate, "f", fe).unwrap();

    let rte = nsitem.rte();
    assert_eq!(rte.rtekind, RTEKind::RTE_FUNCTION);
    let rtfunc = rte.functions.nth(0).as_range_tbl_function().unwrap();
    assert_eq!(rtfunc.funccolcount, 2);
    let names: Vec<_> =
        rte.eref.unwrap().colnames.iter().map(|n| n.as_string().unwrap().sval).collect();
    assert_eq!(names, ["c1", "c2"]);
    assert_eq!(nsitem.p_nscolumns[0].p_vartype, INT4OID);
    assert_eq!(nsitem.p_nscolumns[1].p_vartype, TEXTOID);

    let (colnames, colvars) = expandRTE(
        mcx,
        rte,
        1,
        0,
        types_nodes::VarReturningType::VAR_RETURNING_DEFAULT,
        -1,
        false,
    )
    .unwrap();
    let names: Vec<_> = colnames.iter().map(|n| n.as_string().unwrap().sval).collect();
    assert_eq!(names, ["c1", "c2"]);
    assert_eq!(colvars.len(), 2);
    let v1 = colvars.nth(0).as_var().unwrap();
    assert_eq!((v1.varno, v1.varattno, v1.vartype), (1, 1, INT4OID));
    let v2 = colvars.nth(1).as_var().unwrap();
    assert_eq!((v2.varno, v2.varattno, v2.vartype, v2.varcollid), (1, 2, TEXTOID, 100));
}

#[test]
fn scalar_function_rte_expands_single_var() {
    install_func();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let fe = func_expr(mcx, F_SCALAR, INT4OID);
    let nsitem =
        add_function_rte(mcx, &mut pstate, "f", fe).unwrap();

    let (colnames, colvars) = expandRTE(
        mcx,
        nsitem.rte(),
        3,
        0,
        types_nodes::VarReturningType::VAR_RETURNING_DEFAULT,
        -1,
        false,
    )
    .unwrap();
    assert_eq!(colnames.nth(0).as_string().unwrap().sval, "f");
    assert_eq!(colvars.len(), 1);
    let v = colvars.nth(0).as_var().unwrap();
    assert_eq!((v.varno, v.varattno, v.vartype, v.vartypmod), (3, 1, INT4OID, -1));
}
