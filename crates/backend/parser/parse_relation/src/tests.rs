use std::rc::Rc;
use std::sync::Once;

use mcx::{Mcx, MemoryContext, PgVec};
use parser_small1::{make_parsestate, ParseState};
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
    let data = RelationData {
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
        rd_amcache: Default::default(),
        rd_supportinfo: Default::default(),
        rd_indexlist: Default::default(),
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
    let rte = nsitem.p_rte;
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
    let eref = nsitem.p_rte.eref.unwrap();
    assert_eq!(eref.aliasname, Some("c"));
    let names: Vec<_> = eref.colnames.iter().map(|n| n.as_string().unwrap().sval).collect();
    assert_eq!(names, ["a", "y"]);
    assert_eq!(nsitem.p_rte.alias.unwrap().aliasname, Some("c"));
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

    let err = errorMissingColumn(&pstate, None, "x", 7);
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
        nsitem.p_rte.eref.unwrap().colnames.iter().map(|n| n.as_string().unwrap().sval).collect();
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
        nsitem.p_rte,
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
#[should_panic(expected = "specialAttNum")]
fn system_column_lane_is_loud() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let nsitem = add(mcx, &mut pstate, "t", None);
    pstate.p_namespace.push(nsitem);
    let _ = colNameToVar(mcx, &pstate, "ctid", false, -1);
}

#[test]
#[should_panic(expected = "varstr_levenshtein")]
fn fuzzy_hint_lane_is_loud() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let _ = add(mcx, &mut pstate, "t", None);
    let _ = errorMissingColumn(&pstate, None, "nosuchcol", -1);
}
