// publicationcmds.c: CREATE/ALTER PUBLICATION, publication owner changes, and
// the doDeletion removal entry points. InvalidatePublicationRels stays in
// pg_publication (its catalog callers cannot depend on this crate); it is
// re-exported here to keep the C surface addressable.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::catalog::{FirstNormalObjectId, ATTRIBUTE_GENERATED_STORED, ATTRIBUTE_GENERATED_VIRTUAL};
use types_core::fmgr::{F_NAMEEQ, F_OIDEQ, NAMEDATALEN};
use types_core::primitive::RegProcedure;
use types_core::{
    AttrNumber, InvalidOid, Oid, DATABASE_RELATION_ID, NAMESPACE_RELATION_ID, RELATION_RELATION_ID,
};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT,
    ERRCODE_UNDEFINED_SCHEMA, WARNING,
};
use types_nodes::bitmapset::Bitmapset;
use types_nodes::parsenodes::{
    AlterPublicationAction, AlterPublicationStmt, CreatePublicationStmt, DefElem, DropBehavior,
    ObjectType, PublicationObjSpec, PublicationObjSpecType, PublicationTable,
};
use types_nodes::primnodes::{DistinctExpr, NullIfExpr, OpExpr, RowCompareExpr, ScalarArrayOpExpr, Var};
use types_nodes::{Node, NodeList, NodeTag};
use types_rel::pg_class::{REPLICA_IDENTITY_FULL, RELKIND_PARTITIONED_TABLE};
use types_rel::{
    AccessExclusiveLock, AccessShareLock, NoLock, Relation, RowExclusiveLock,
    ShareUpdateExclusiveLock,
};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::{HeapTupleData, TupleDescData};

use cache_syscache::cacheinfo::{NAMESPACEOID, PUBLICATIONNAMESPACEMAP, PUBLICATIONRELMAP};
use cache_syscache::{
    GetSysCacheOid, ReleaseSysCache, SearchSysCache2, SearchSysCacheExists, SysCacheGetAttr,
    SysCacheKey,
};
use nodes_core::{check_functions_in_node, expr_collation, expr_location, expr_type,
    expression_tree_walker, NodeWalker};
use parse_collate::assign_expr_collations;
use parse_relation::{addNSItemToQuery, addRangeTableEntryForRelation};
use parser_small1::{make_parsestate, ParseExprKind};
use pg_depend::ObjectAddress;
use pg_publication::{
    check_and_fetch_column_list, is_schema_publication, pub_collist_to_bitmapset,
    pub_collist_validate, publication_add_relation, publication_add_schema,
    Anum_pg_publication_namespace_oid, Anum_pg_publication_oid, Anum_pg_publication_puballtables,
    Anum_pg_publication_pubdelete, Anum_pg_publication_pubgencols, Anum_pg_publication_pubinsert,
    Anum_pg_publication_pubname, Anum_pg_publication_pubowner, Anum_pg_publication_pubtruncate,
    Anum_pg_publication_pubupdate, Anum_pg_publication_pubviaroot, Anum_pg_publication_rel_oid,
    Anum_pg_publication_rel_prattrs, Anum_pg_publication_rel_prqual,
    Anum_pg_publication_rel_prrelid, GetAllSchemaPublicationRelations,
    GetPubPartitionOptionRelations, GetPublication, GetPublicationRelations, GetPublicationSchemas,
    GetSchemaPublicationRelations, GetTopMostAncestorInPublication, Natts_pg_publication,
    PublicationActions, PublicationNameIndexId, PublicationNamespaceObjectIndexId,
    PublicationNamespaceRelationId, PublicationObjectIndexId, PublicationPartOpt,
    PublicationRelInfo, PublicationRelObjectIndexId, PublicationRelRelationId,
    PublicationRelationId, PUBLISH_GENCOLS_NONE, PUBLISH_GENCOLS_STORED,
};

pub use pg_publication::InvalidatePublicationRels;

const PROVOLATILE_IMMUTABLE: i8 = b'i' as i8;
const InvalidAttrNumber: AttrNumber = 0;

fn eq_key(attno: AttrNumber, func: RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

fn name_arg<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<PgVec<'mcx, u8>> {
    let n = NAMEDATALEN as usize;
    assert!(name.len() < n, "identifier truncation unported: {name:?}");
    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, n)?;
    mcx::vec_append_bytes(&mut buf, name.as_bytes())?;
    mcx::vec_append_bytes(&mut buf, &[0u8; 64][..n - name.len()])?;
    Ok(buf)
}

fn getattr(td: &TupleDescData<'_>, tup: &HeapTupleData<'_>, attno: i32) -> (Datum, bool) {
    let mut isnull = false;
    // SAFETY: tup is a catalog row read under its relation's descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attno, td, &mut isnull) };
    (d, isnull)
}

fn text_from_datum(mcx: Mcx<'_>, d: Datum) -> PgResult<String> {
    let p = d.as_usize() as *const u8;
    // SAFETY: a live varlena readable through its full VARSIZE_ANY.
    let raw = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    let image = detoast_image(mcx, raw)?;
    let payload = if image[0] & 0x01 == 0x01 {
        &image[1..(image[0] >> 1) as usize]
    } else {
        &image[4..(u32::from_ne_bytes(image[..4].try_into().unwrap()) >> 2) as usize]
    };
    Ok(core::str::from_utf8(payload).expect("stored node tree is UTF-8").to_string())
}

fn detoast_image<'mcx>(mcx: Mcx<'mcx>, raw: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    detoast::detoast_attr(mcx, raw)
}

fn errpos(src: Option<&str>, location: types_core::ParseLoc) -> i32 {
    parser_small1::parser_errposition_source(
        src.map(str::as_bytes),
        location,
        mbutils::GetDatabaseEncoding(),
    )
}

#[track_caller]
#[cold]
fn simple_err(sqlstate: types_error::SqlState, msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(sqlstate))
}

#[track_caller]
#[cold]
fn conflicting_options(src: Option<&str>, def: &DefElem<'_>) -> Box<PgError> {
    Box::new(
        PgError::error("conflicting or redundant options")
            .with_sqlstate(ERRCODE_SYNTAX_ERROR)
            .with_cursor_position(errpos(src, def.location)),
    )
}

pub struct ParsedPubOptions {
    pub publish_given: bool,
    pub pubactions: PublicationActions,
    pub publish_via_partition_root_given: bool,
    pub publish_via_partition_root: bool,
    pub publish_generated_columns_given: bool,
    pub publish_generated_columns: u8,
}

fn defGetGeneratedColsOption(mcx: Mcx<'_>, def: &DefElem<'_>) -> PgResult<u8> {
    let mut sval = "";
    if def.arg.is_some() {
        sval = commands_define::defGetString(mcx, def)?;
        if sval.eq_ignore_ascii_case("none") {
            return Ok(PUBLISH_GENCOLS_NONE);
        }
        if sval.eq_ignore_ascii_case("stored") {
            return Ok(PUBLISH_GENCOLS_STORED);
        }
    }
    Err(Box::new(
        PgError::error(format!(
            "invalid value for publication parameter \"{}\": \"{sval}\"",
            def.defname.unwrap_or("")
        ))
        .with_sqlstate(ERRCODE_SYNTAX_ERROR)
        .with_detail("Valid values are \"none\" and \"stored\"."),
    ))
}

pub fn parse_publication_options<'mcx>(
    mcx: Mcx<'mcx>,
    options: &NodeList<'mcx>,
    source: Option<&str>,
) -> PgResult<ParsedPubOptions> {
    let mut out = ParsedPubOptions {
        publish_given: false,
        pubactions: PublicationActions {
            pubinsert: true,
            pubupdate: true,
            pubdelete: true,
            pubtruncate: true,
        },
        publish_via_partition_root_given: false,
        publish_via_partition_root: false,
        publish_generated_columns_given: false,
        publish_generated_columns: PUBLISH_GENCOLS_NONE,
    };

    for cell in options.iter() {
        let defel = cell.as_def_elem().expect("publication option is a DefElem");
        match defel.defname.unwrap_or("") {
            "publish" => {
                if out.publish_given {
                    return Err(conflicting_options(source, defel));
                }
                out.pubactions = PublicationActions {
                    pubinsert: false,
                    pubupdate: false,
                    pubdelete: false,
                    pubtruncate: false,
                };
                out.publish_given = true;

                let publish = commands_define::defGetString(mcx, defel)?;
                let Some(publish_list) = varlena::split_identifier_string(
                    mcx,
                    publish,
                    b',',
                    mbutils::GetDatabaseEncoding(),
                )?
                else {
                    return Err(simple_err(
                        ERRCODE_SYNTAX_ERROR,
                        "invalid list syntax in parameter \"publish\"".into(),
                    ));
                };
                for publish_opt in &publish_list {
                    match publish_opt.as_str() {
                        "insert" => out.pubactions.pubinsert = true,
                        "update" => out.pubactions.pubupdate = true,
                        "delete" => out.pubactions.pubdelete = true,
                        "truncate" => out.pubactions.pubtruncate = true,
                        other => {
                            return Err(simple_err(
                                ERRCODE_SYNTAX_ERROR,
                                format!(
                                    "unrecognized value for publication option \"publish\": \"{other}\""
                                ),
                            ))
                        }
                    }
                }
            }
            "publish_via_partition_root" => {
                if out.publish_via_partition_root_given {
                    return Err(conflicting_options(source, defel));
                }
                out.publish_via_partition_root_given = true;
                out.publish_via_partition_root = commands_define::defGetBoolean(defel)?;
            }
            "publish_generated_columns" => {
                if out.publish_generated_columns_given {
                    return Err(conflicting_options(source, defel));
                }
                out.publish_generated_columns_given = true;
                out.publish_generated_columns = defGetGeneratedColsOption(mcx, defel)?;
            }
            other => {
                return Err(simple_err(
                    ERRCODE_SYNTAX_ERROR,
                    format!("unrecognized publication parameter: \"{other}\""),
                ))
            }
        }
    }
    Ok(out)
}

pub fn ObjectsInPublicationToOids<'mcx>(
    mcx: Mcx<'mcx>,
    pubobjspec_list: &NodeList<'mcx>,
) -> PgResult<(Vec<&'mcx PublicationTable<'mcx>>, Vec<Oid>)> {
    let mut rels: Vec<&'mcx PublicationTable<'mcx>> = Vec::new();
    let mut schemas: Vec<Oid> = Vec::new();
    for cell in pubobjspec_list.iter() {
        let pubobj = cell
            .as_variant::<PublicationObjSpec>()
            .expect("publication object is a PublicationObjSpec");
        match pubobj.pubobjtype {
            PublicationObjSpecType::PUBLICATIONOBJ_TABLE => {
                rels.push(pubobj.pubtable.expect("PUBLICATIONOBJ_TABLE has a pubtable"));
            }
            PublicationObjSpecType::PUBLICATIONOBJ_TABLES_IN_SCHEMA => {
                let schemaid = catalog_namespace::get_namespace_oid(
                    pubobj.name.expect("TABLES IN SCHEMA has a name"),
                    false,
                )?;
                if !schemas.contains(&schemaid) {
                    schemas.push(schemaid);
                }
            }
            PublicationObjSpecType::PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA => {
                let search_path = catalog_namespace::fetch_search_path(mcx, false)?;
                let Some(&schemaid) = search_path.first() else {
                    return Err(simple_err(
                        ERRCODE_UNDEFINED_SCHEMA,
                        "no schema has been selected for CURRENT_SCHEMA".into(),
                    ));
                };
                if !schemas.contains(&schemaid) {
                    schemas.push(schemaid);
                }
            }
            other => panic!("invalid publication object type {other:?}"),
        }
    }
    Ok((rels, schemas))
}

pub struct PubRelOpen<'mcx> {
    pub relation: Relation<'mcx>,
    pub whereClause: Option<Node<'mcx>>,
    pub columns: NodeList<'mcx>,
}

fn to_rel_vocab_rv<'mcx>(prv: &types_nodes::primnodes::RangeVar<'mcx>) -> rel_vocab::RangeVar<'mcx> {
    rel_vocab::RangeVar {
        catalogname: prv.catalogname,
        schemaname: prv.schemaname,
        relname: prv.relname.expect("RangeVar.relname"),
        inh: prv.inh,
        relpersistence: prv.relpersistence,
        location: prv.location,
    }
}

fn conflicting_rf(relname: &str) -> Box<PgError> {
    simple_err(
        ERRCODE_DUPLICATE_OBJECT,
        format!("conflicting or redundant WHERE clauses for table \"{relname}\""),
    )
}

fn conflicting_collist(relname: &str) -> Box<PgError> {
    simple_err(
        ERRCODE_DUPLICATE_OBJECT,
        format!("conflicting or redundant column lists for table \"{relname}\""),
    )
}

fn OpenTableList<'mcx>(
    mcx: Mcx<'mcx>,
    tables: &[&'mcx PublicationTable<'mcx>],
) -> PgResult<Vec<PubRelOpen<'mcx>>> {
    let mut relids: Vec<Oid> = Vec::new();
    let mut rels: Vec<PubRelOpen<'mcx>> = Vec::new();
    let mut relids_with_rf: Vec<Oid> = Vec::new();
    let mut relids_with_collist: Vec<Oid> = Vec::new();

    for t in tables {
        let prv = t.relation.expect("PublicationTable.relation");
        let recurse = prv.inh;
        let rv = to_rel_vocab_rv(prv);
        let rel = table::table_openrv(mcx, &rv, ShareUpdateExclusiveLock)?;
        let myrelid = rel.rd_id;

        if relids.contains(&myrelid) {
            if t.whereClause.is_some() || relids_with_rf.contains(&myrelid) {
                return Err(conflicting_rf(rel.name()));
            }
            if !t.columns.is_nil() || relids_with_collist.contains(&myrelid) {
                return Err(conflicting_collist(rel.name()));
            }
            rel.close(ShareUpdateExclusiveLock)?;
            continue;
        }

        let relkind = rel.rd_rel.relkind as u8;
        let relname = rel.name().to_string();
        rels.push(PubRelOpen {
            relation: rel,
            whereClause: t.whereClause,
            columns: NodeList::from_slice(mcx, t.columns.as_slice())?,
        });
        relids.push(myrelid);
        if t.whereClause.is_some() {
            relids_with_rf.push(myrelid);
        }
        if !t.columns.is_nil() {
            relids_with_collist.push(myrelid);
        }

        if recurse && relkind != RELKIND_PARTITIONED_TABLE {
            let children = pg_inherits::find_all_inheritors(mcx, myrelid, ShareUpdateExclusiveLock)?;
            for &childrelid in children.iter() {
                if relids.contains(&childrelid) {
                    if childrelid != myrelid
                        && (t.whereClause.is_some() || relids_with_rf.contains(&childrelid))
                    {
                        return Err(conflicting_rf(&relname));
                    }
                    if childrelid != myrelid
                        && (!t.columns.is_nil() || relids_with_collist.contains(&childrelid))
                    {
                        return Err(conflicting_collist(&relname));
                    }
                    continue;
                }
                let child = table::table_open(mcx, childrelid, NoLock)?;
                rels.push(PubRelOpen {
                    relation: child,
                    whereClause: t.whereClause,
                    columns: NodeList::from_slice(mcx, t.columns.as_slice())?,
                });
                relids.push(childrelid);
                if t.whereClause.is_some() {
                    relids_with_rf.push(childrelid);
                }
                if !t.columns.is_nil() {
                    relids_with_collist.push(childrelid);
                }
            }
        }
    }
    Ok(rels)
}

fn CloseTableList(rels: Vec<PubRelOpen<'_>>) -> PgResult<()> {
    for r in rels {
        r.relation.close(NoLock)?;
    }
    Ok(())
}

fn LockSchemaList(schemalist: &[Oid]) -> PgResult<()> {
    for &schemaid in schemalist {
        lmgr::LockDatabaseObject(NAMESPACE_RELATION_ID, schemaid, 0, AccessShareLock)?;
        if !SearchSysCacheExists(
            NAMESPACEOID,
            SysCacheKey::Value(Datum::from_oid(schemaid)),
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
        )? {
            return Err(simple_err(
                ERRCODE_UNDEFINED_SCHEMA,
                format!("schema with OID {schemaid} does not exist"),
            ));
        }
    }
    Ok(())
}

fn expr_input_collation(node: Node<'_>) -> Oid {
    match node.node_tag() {
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().inputcollid,
        NodeTag::T_OpExpr => node.as_variant::<OpExpr>().unwrap().inputcollid,
        NodeTag::T_DistinctExpr => node.as_variant::<DistinctExpr>().unwrap().inputcollid,
        NodeTag::T_ScalarArrayOpExpr => node.as_variant::<ScalarArrayOpExpr>().unwrap().inputcollid,
        NodeTag::T_MinMaxExpr => {
            node.as_variant::<types_nodes::primnodes::MinMaxExpr>().unwrap().inputcollid
        }
        _ => InvalidOid,
    }
}

struct RowFilterWalker<'s> {
    source: &'s str,
}

impl<'s> RowFilterWalker<'s> {
    #[track_caller]
    #[cold]
    fn fail(&self, detail: &str, node: Node<'_>) -> Box<PgError> {
        Box::new(
            PgError::error("invalid publication WHERE expression")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                .with_detail(detail)
                .with_cursor_position(errpos(Some(self.source), expr_location(node))),
        )
    }
}

impl<'s, 'mcx> NodeWalker<'mcx> for RowFilterWalker<'s> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        let mut errdetail_msg: Option<&'static str> = None;
        match node.node_tag() {
            NodeTag::T_Var => {
                if node.as_variant::<Var>().unwrap().varattno < InvalidAttrNumber {
                    errdetail_msg = Some("System columns are not allowed.");
                }
            }
            NodeTag::T_OpExpr => {
                if node.as_variant::<OpExpr>().unwrap().opno >= FirstNormalObjectId {
                    errdetail_msg = Some("User-defined operators are not allowed.");
                }
            }
            NodeTag::T_DistinctExpr => {
                if node.as_variant::<DistinctExpr>().unwrap().opno >= FirstNormalObjectId {
                    errdetail_msg = Some("User-defined operators are not allowed.");
                }
            }
            NodeTag::T_NullIfExpr => {
                if node.as_variant::<NullIfExpr>().unwrap().opno >= FirstNormalObjectId {
                    errdetail_msg = Some("User-defined operators are not allowed.");
                }
            }
            NodeTag::T_RowCompareExpr => {
                let rc = node.as_variant::<RowCompareExpr>().unwrap();
                if rc.opnos.iter().any(|opno| opno >= FirstNormalObjectId) {
                    errdetail_msg = Some("User-defined operators are not allowed.");
                }
            }
            NodeTag::T_ScalarArrayOpExpr => {
                if node.as_variant::<ScalarArrayOpExpr>().unwrap().opno >= FirstNormalObjectId {
                    errdetail_msg = Some("User-defined operators are not allowed.");
                }
            }
            NodeTag::T_Const
            | NodeTag::T_FuncExpr
            | NodeTag::T_BoolExpr
            | NodeTag::T_RelabelType
            | NodeTag::T_CollateExpr
            | NodeTag::T_CaseExpr
            | NodeTag::T_CaseTestExpr
            | NodeTag::T_ArrayExpr
            | NodeTag::T_RowExpr
            | NodeTag::T_CoalesceExpr
            | NodeTag::T_MinMaxExpr
            | NodeTag::T_XmlExpr
            | NodeTag::T_NullTest
            | NodeTag::T_BooleanTest
            | NodeTag::T_List => {}
            _ => {
                errdetail_msg = Some(
                    "Only columns, constants, built-in operators, built-in data types, \
                     built-in collations, and immutable built-in functions are allowed.",
                );
            }
        }

        if errdetail_msg.is_none() && node.node_tag() != NodeTag::T_List {
            if expr_type(node) >= FirstNormalObjectId {
                errdetail_msg = Some("User-defined types are not allowed.");
            } else if check_functions_in_node(node, &mut |func_id: Oid| {
                Ok(lsyscache::func_volatile(func_id)? != PROVOLATILE_IMMUTABLE
                    || func_id >= FirstNormalObjectId)
            })? {
                errdetail_msg =
                    Some("User-defined or built-in mutable functions are not allowed.");
            } else if expr_collation(node) >= FirstNormalObjectId
                || expr_input_collation(node) >= FirstNormalObjectId
            {
                errdetail_msg = Some("User-defined collations are not allowed.");
            }
        }

        if let Some(detail) = errdetail_msg {
            return Err(self.fail(detail, node));
        }

        expression_tree_walker(node, self)
    }
}

fn check_simple_rowfilter_expr(node: Node<'_>, source: &str) -> PgResult<bool> {
    let mut walker = RowFilterWalker { source };
    walker.visit(node)
}

// expand_generated_columns_in_expr (rewriteHandler.c:4493) at rt_index 1: Vars
// naming a virtual generated column of rel become the generation expression,
// whose Vars are already at varno 1.
fn expand_generated_columns_in_expr<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    rel: &types_rel::Relation<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    const VIRTUAL_GEN: i8 = types_core::catalog::ATTRIBUTE_GENERATED_VIRTUAL as i8;
    if !rel.rd_att.constr.as_deref().is_some_and(|c| c.has_generated_virtual) {
        return Ok(None);
    }
    if let Some(v) = node.as_var() {
        if v.varlevelsup != 0 || v.varno != 1 {
            return Ok(None);
        }
        if v.varattno == 0 {
            // ReplaceVarsFromTargetList whole-row arm (rewriteManip.c:1801):
            // RowExpr over per-field Vars, dropped columns as NULL int4
            // consts, virtual columns as their generation expressions.
            let mut args = types_nodes::list::NodeList::nil();
            for i in 0..rel.rd_att.natts as usize {
                let att = rel.rd_att.attr(i);
                let field = if att.attisdropped {
                    Node::mk_const(
                        mcx,
                        types_core::catalog::INT4OID,
                        -1,
                        0,
                        4,
                        Datum::null(),
                        true,
                        true,
                    )?
                } else if att.attgenerated == VIRTUAL_GEN {
                    rewrite_handler::build_generation_expression(mcx, rel, i + 1)?
                } else {
                    Node::mk_var(
                        mcx,
                        1,
                        (i + 1) as i16,
                        att.atttypid,
                        att.atttypmod,
                        att.attcollation,
                        0,
                    )?
                };
                args.lappend(mcx, field)?;
            }
            return Ok(Some(Node::mk(
                mcx,
                types_nodes::RowExpr {
                    args,
                    row_typeid: v.vartype,
                    row_format: types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
                    colnames: types_nodes::list::NodeList::nil(),
                    location: v.location,
                },
            )?));
        }
        if rel.rd_att.attr(v.varattno as usize - 1).attgenerated != VIRTUAL_GEN {
            return Ok(None);
        }
        return Ok(Some(rewrite_handler::build_generation_expression(
            mcx,
            rel,
            v.varattno as usize,
        )?));
    }
    nodes_core::expression_tree_mutator(mcx, node, &mut |n| {
        expand_generated_columns_in_expr(mcx, n, rel)
    })
}

fn TransformPubWhereClauses<'mcx>(
    mcx: Mcx<'mcx>,
    rels: &mut [PubRelOpen<'mcx>],
    query_string: &str,
    pubviaroot: bool,
) -> PgResult<()> {
    for pri in rels.iter_mut() {
        let Some(raw) = pri.whereClause else { continue };

        if !pubviaroot && pri.relation.rd_rel.relkind as u8 == RELKIND_PARTITIONED_TABLE {
            return Err(Box::new(
                PgError::error(format!(
                    "cannot use publication WHERE clause for relation \"{}\"",
                    pri.relation.name()
                ))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_detail(
                    "WHERE clause cannot be used for a partitioned table when \
                     publish_via_partition_root is false.",
                ),
            ));
        }

        let mut pstate = make_parsestate(mcx, None);
        {
            let mut v: PgVec<'mcx, u8> = PgVec::new_in(mcx);
            mcx::vec_append_bytes(&mut v, query_string.as_bytes())?;
            pstate.p_sourcetext = Some(v.leak());
        }
        let nsitem = addRangeTableEntryForRelation(
            mcx,
            &mut pstate,
            &pri.relation,
            AccessShareLock,
            None,
            false,
            false,
        )?;
        addNSItemToQuery(mcx, &mut pstate, nsitem, false, true, true)?;

        let whereclause = parse_clause::transformWhereClause(
            mcx,
            &mut pstate,
            Some(raw),
            ParseExprKind::EXPR_KIND_WHERE,
            "PUBLICATION WHERE",
        )?
        .expect("publication WHERE transform yields an expression");

        assign_expr_collations(mcx, &pstate, whereclause)?;

        let whereclause = expand_generated_columns_in_expr(mcx, whereclause, &pri.relation)?
            .unwrap_or(whereclause);

        check_simple_rowfilter_expr(whereclause, query_string)?;

        pri.whereClause = Some(whereclause);
    }
    Ok(())
}

fn CheckPubRelationColumnList(
    mcx: Mcx<'_>,
    pubname: &str,
    rels: &[PubRelOpen<'_>],
    publish_schema: bool,
    pubviaroot: bool,
) -> PgResult<()> {
    for pri in rels {
        if pri.columns.is_nil() {
            continue;
        }
        let nspname = lsyscache::get_namespace_name(mcx, pri.relation.rd_rel.relnamespace)?
            .map(|n| n.as_str().to_string())
            .unwrap_or_default();
        if publish_schema {
            return Err(Box::new(
                PgError::error(format!(
                    "cannot use column list for relation \"{nspname}.{}\" in publication \"{pubname}\"",
                    pri.relation.name()
                ))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_detail(
                    "Column lists cannot be specified in publications containing \
                     FOR TABLES IN SCHEMA elements.",
                ),
            ));
        }
        if !pubviaroot && pri.relation.rd_rel.relkind as u8 == RELKIND_PARTITIONED_TABLE {
            return Err(Box::new(
                PgError::error(format!(
                    "cannot use column list for relation \"{nspname}.{}\" in publication \"{pubname}\"",
                    pri.relation.name()
                ))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_detail(
                    "Column lists cannot be specified for partitioned tables when \
                     publish_via_partition_root is false.",
                ),
            ));
        }
    }
    Ok(())
}

fn get_relkind_objtype(relkind: u8) -> ObjectType {
    use types_rel::pg_class::{
        RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_SEQUENCE, RELKIND_VIEW,
    };
    match relkind {
        RELKIND_SEQUENCE => ObjectType::OBJECT_SEQUENCE,
        RELKIND_VIEW => ObjectType::OBJECT_VIEW,
        RELKIND_MATVIEW => ObjectType::OBJECT_MATVIEW,
        RELKIND_FOREIGN_TABLE => ObjectType::OBJECT_FOREIGN_TABLE,
        _ => ObjectType::OBJECT_TABLE,
    }
}

fn PublicationAddTables<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    rels: &[PubRelOpen<'mcx>],
    if_not_exists: bool,
) -> PgResult<()> {
    for pub_rel in rels {
        let rel = &pub_rel.relation;
        if !aclchk::object_ownercheck(RELATION_RELATION_ID, rel.rd_id, miscinit::GetUserId())? {
            aclchk::aclcheck_error(
                aclchk::ACLCHECK_NOT_OWNER,
                get_relkind_objtype(rel.rd_rel.relkind as u8),
                rel.name(),
            )?;
        }
        let pri = PublicationRelInfo {
            relation: rel,
            whereClause: pub_rel.whereClause,
            columns: &pub_rel.columns,
        };
        publication_add_relation(mcx, pubid, &pri, if_not_exists)?;
    }
    Ok(())
}

fn PublicationDropTables<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    rels: &[PubRelOpen<'mcx>],
    missing_ok: bool,
) -> PgResult<()> {
    for pubrel in rels {
        let rel = &pubrel.relation;
        let relid = rel.rd_id;

        if !pubrel.columns.is_nil() {
            return Err(simple_err(
                ERRCODE_SYNTAX_ERROR,
                "column list must not be specified in ALTER PUBLICATION ... DROP".into(),
            ));
        }

        let prid = GetSysCacheOid(
            PUBLICATIONRELMAP,
            Anum_pg_publication_rel_oid,
            SysCacheKey::Value(Datum::from_oid(relid)),
            SysCacheKey::Value(Datum::from_oid(pubid)),
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
        )?;
        if prid == InvalidOid {
            if missing_ok {
                continue;
            }
            return Err(simple_err(
                ERRCODE_UNDEFINED_OBJECT,
                format!("relation \"{}\" is not part of the publication", rel.name()),
            ));
        }

        if pubrel.whereClause.is_some() {
            return Err(simple_err(
                ERRCODE_SYNTAX_ERROR,
                "cannot use a WHERE clause when removing a table from a publication".into(),
            ));
        }

        let obj = ObjectAddress::set(PublicationRelRelationId, prid);
        catalog_dependency::performDeletion(mcx, &obj, DropBehavior::DROP_CASCADE, 0)?;
    }
    Ok(())
}

fn PublicationAddSchemas(mcx: Mcx<'_>, pubid: Oid, schemas: &[Oid], if_not_exists: bool) -> PgResult<()> {
    for &schemaid in schemas {
        publication_add_schema(mcx, pubid, schemaid, if_not_exists)?;
    }
    Ok(())
}

fn PublicationDropSchemas(mcx: Mcx<'_>, pubid: Oid, schemas: &[Oid], missing_ok: bool) -> PgResult<()> {
    for &schemaid in schemas {
        let psid = GetSysCacheOid(
            PUBLICATIONNAMESPACEMAP,
            Anum_pg_publication_namespace_oid,
            SysCacheKey::Value(Datum::from_oid(schemaid)),
            SysCacheKey::Value(Datum::from_oid(pubid)),
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
        )?;
        if psid == InvalidOid {
            if missing_ok {
                continue;
            }
            let nspname = lsyscache::get_namespace_name(mcx, schemaid)?
                .map(|n| n.as_str().to_string())
                .unwrap_or_default();
            return Err(simple_err(
                ERRCODE_UNDEFINED_OBJECT,
                format!("tables from schema \"{nspname}\" are not part of the publication"),
            ));
        }
        let obj = ObjectAddress::set(PublicationNamespaceRelationId, psid);
        catalog_dependency::performDeletion(mcx, &obj, DropBehavior::DROP_CASCADE, 0)?;
    }
    Ok(())
}

pub fn CreatePublication<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreatePublicationStmt<'mcx>,
    query_string: &str,
) -> PgResult<()> {
    let pubname = stmt.pubname.expect("CreatePublicationStmt.pubname");

    let aclresult = aclchk::object_aclcheck(
        DATABASE_RELATION_ID,
        init_small::globals::MyDatabaseId(),
        miscinit::GetUserId(),
        adt_acl::ACL_CREATE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        let dbname = dbcommands::get_database_name(init_small::globals::MyDatabaseId())?
            .unwrap_or_default();
        aclchk::aclcheck_error(aclresult, ObjectType::OBJECT_DATABASE, &dbname)?;
    }

    if stmt.for_all_tables && !superuser::superuser()? {
        return Err(simple_err(
            ERRCODE_INSUFFICIENT_PRIVILEGE,
            "must be superuser to create FOR ALL TABLES publication".into(),
        ));
    }

    let rel = table::table_open(mcx, PublicationRelationId, RowExclusiveLock)?;

    if lsyscache::get_publication_oid(pubname, true)? != InvalidOid {
        return Err(simple_err(
            ERRCODE_DUPLICATE_OBJECT,
            format!("publication \"{pubname}\" already exists"),
        ));
    }

    let opts = parse_publication_options(mcx, &stmt.options, Some(query_string))?;

    let mut values = [Datum::null(); Natts_pg_publication];
    let nulls = [false; Natts_pg_publication];
    let set = |values: &mut [Datum], anum: i32, v: Datum| values[(anum - 1) as usize] = v;

    let pname = name_arg(mcx, pubname)?;
    set(&mut values, Anum_pg_publication_pubname, Datum::from_usize(pname.as_ptr() as usize));
    set(&mut values, Anum_pg_publication_pubowner, Datum::from_oid(miscinit::GetUserId()));

    let puboid = catalog::GetNewOidWithIndex(
        mcx,
        &rel,
        PublicationObjectIndexId,
        Anum_pg_publication_oid as AttrNumber,
    )?;
    set(&mut values, Anum_pg_publication_oid, Datum::from_oid(puboid));
    set(&mut values, Anum_pg_publication_puballtables, Datum::from_bool(stmt.for_all_tables));
    set(&mut values, Anum_pg_publication_pubinsert, Datum::from_bool(opts.pubactions.pubinsert));
    set(&mut values, Anum_pg_publication_pubupdate, Datum::from_bool(opts.pubactions.pubupdate));
    set(&mut values, Anum_pg_publication_pubdelete, Datum::from_bool(opts.pubactions.pubdelete));
    set(&mut values, Anum_pg_publication_pubtruncate, Datum::from_bool(opts.pubactions.pubtruncate));
    set(&mut values, Anum_pg_publication_pubviaroot, Datum::from_bool(opts.publish_via_partition_root));
    set(&mut values, Anum_pg_publication_pubgencols, Datum::from_char(opts.publish_generated_columns as i8));

    let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;

    pg_shdepend::recordDependencyOnOwner(mcx, PublicationRelationId, puboid, miscinit::GetUserId())?;

    xact::CommandCounterIncrement()?;

    if stmt.for_all_tables {
        inval::invalidate::CacheInvalidateRelcacheAll()?;
    } else {
        let (relations, schemaidlist) = ObjectsInPublicationToOids(mcx, &stmt.pubobjects)?;

        if !schemaidlist.is_empty() && !superuser::superuser()? {
            return Err(simple_err(
                ERRCODE_INSUFFICIENT_PRIVILEGE,
                "must be superuser to create FOR TABLES IN SCHEMA publication".into(),
            ));
        }

        if !relations.is_empty() {
            let mut rels = OpenTableList(mcx, &relations)?;
            TransformPubWhereClauses(mcx, &mut rels, query_string, opts.publish_via_partition_root)?;
            CheckPubRelationColumnList(
                mcx,
                pubname,
                &rels,
                !schemaidlist.is_empty(),
                opts.publish_via_partition_root,
            )?;
            PublicationAddTables(mcx, puboid, &rels, true)?;
            CloseTableList(rels)?;
        }

        if !schemaidlist.is_empty() {
            LockSchemaList(&schemaidlist)?;
            PublicationAddSchemas(mcx, puboid, &schemaidlist, true)?;
        }
    }

    rel.close(RowExclusiveLock)?;

    if transam_xlog::wal_level() != transam_xlog::WAL_LEVEL_LOGICAL {
        elog::ereport(WARNING)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("\"wal_level\" is insufficient to publish logical changes")
            .errhint("Set \"wal_level\" to \"logical\" before creating subscriptions.")
            .finish(types_error::ErrorLocation::new(
                "publicationcmds.c",
                0,
                "CreatePublication",
            ))?;
    }

    Ok(())
}

struct PubTupleFields {
    oid: Oid,
    puballtables: bool,
    pubviaroot: bool,
}

fn pub_fields(td: &TupleDescData<'_>, tup: &HeapTupleData<'_>) -> PubTupleFields {
    PubTupleFields {
        oid: getattr(td, tup, Anum_pg_publication_oid).0.as_oid(),
        puballtables: getattr(td, tup, Anum_pg_publication_puballtables).0.as_bool(),
        pubviaroot: getattr(td, tup, Anum_pg_publication_pubviaroot).0.as_bool(),
    }
}

fn AlterPublicationOptions<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterPublicationStmt<'mcx>,
    rel: &Relation<'mcx>,
    query_string: &str,
) -> PgResult<()> {
    let pubname = stmt.pubname.expect("AlterPublicationStmt.pubname");
    let opts = parse_publication_options(mcx, &stmt.options, Some(query_string))?;

    let pname = name_arg(mcx, pubname)?;
    let keys = [eq_key(
        Anum_pg_publication_pubname as AttrNumber,
        F_NAMEEQ,
        Datum::from_usize(pname.as_ptr() as usize),
    )];
    let mut scan =
        genam::systable_beginscan(mcx, rel, PublicationNameIndexId, true, None, &keys)?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(simple_err(
            ERRCODE_UNDEFINED_OBJECT,
            format!("publication \"{pubname}\" does not exist"),
        ));
    };
    let td = rel.descr();
    let fields = pub_fields(td, tup);

    let mut root_relids: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let mut have_root_relids = false;
    if !fields.puballtables
        && opts.publish_via_partition_root_given
        && !opts.publish_via_partition_root
    {
        lmgr::LockDatabaseObject(PublicationRelationId, fields.oid, 0, AccessShareLock)?;
        root_relids = GetPublicationRelations(mcx, fields.oid, PublicationPartOpt::Root)?;
        have_root_relids = true;

        for &relid in root_relids.iter() {
            let Some(rftuple) = SearchSysCache2(
                PUBLICATIONRELMAP,
                SysCacheKey::Value(Datum::from_oid(relid)),
                SysCacheKey::Value(Datum::from_oid(fields.oid)),
            )?
            else {
                continue;
            };
            let has_rowfilter =
                !SysCacheGetAttr(PUBLICATIONRELMAP, &rftuple, Anum_pg_publication_rel_prqual)?.1;
            let has_collist =
                !SysCacheGetAttr(PUBLICATIONRELMAP, &rftuple, Anum_pg_publication_rel_prattrs)?.1;
            ReleaseSysCache(rftuple);
            if !has_rowfilter && !has_collist {
                continue;
            }
            if lsyscache::get_rel_relkind(relid)? as u8 != RELKIND_PARTITIONED_TABLE {
                continue;
            }
            let Some(relname) = lsyscache::relation::get_rel_name(mcx, relid)? else {
                continue;
            };
            let detail = if has_rowfilter {
                format!(
                    "The publication contains a WHERE clause for partitioned table \"{}\", \
                     which is not allowed when \"publish_via_partition_root\" is false.",
                    relname.as_str()
                )
            } else {
                format!(
                    "The publication contains a column list for partitioned table \"{}\", \
                     which is not allowed when \"publish_via_partition_root\" is false.",
                    relname.as_str()
                )
            };
            return Err(Box::new(
                PgError::error(format!(
                    "cannot set parameter \"publish_via_partition_root\" to false for \
                     publication \"{pubname}\""
                ))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_detail(detail),
            ));
        }
    }

    let natts = td.natts as usize;
    let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    let mut set = |repl_values: &mut PgVec<'_, Datum>, repl: &mut PgVec<'_, bool>, anum: i32, v: Datum| {
        repl_values[(anum - 1) as usize] = v;
        repl[(anum - 1) as usize] = true;
    };

    if opts.publish_given {
        set(&mut repl_values, &mut repl, Anum_pg_publication_pubinsert, Datum::from_bool(opts.pubactions.pubinsert));
        set(&mut repl_values, &mut repl, Anum_pg_publication_pubupdate, Datum::from_bool(opts.pubactions.pubupdate));
        set(&mut repl_values, &mut repl, Anum_pg_publication_pubdelete, Datum::from_bool(opts.pubactions.pubdelete));
        set(&mut repl_values, &mut repl, Anum_pg_publication_pubtruncate, Datum::from_bool(opts.pubactions.pubtruncate));
    }
    if opts.publish_via_partition_root_given {
        set(&mut repl_values, &mut repl, Anum_pg_publication_pubviaroot, Datum::from_bool(opts.publish_via_partition_root));
    }
    if opts.publish_generated_columns_given {
        set(&mut repl_values, &mut repl, Anum_pg_publication_pubgencols, Datum::from_char(opts.publish_generated_columns as i8));
    }

    let mut new_tuple = heaptuple::heap_modify_tuple(mcx, tup, td, &repl_values, &repl_isnull, &repl)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, rel, &otid, &mut new_tuple)?;

    xact::CommandCounterIncrement()?;

    if fields.puballtables {
        inval::invalidate::CacheInvalidateRelcacheAll()?;
    } else {
        let mut relids: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
        if !have_root_relids {
            relids = GetPublicationRelations(mcx, fields.oid, PublicationPartOpt::All)?;
        } else {
            for &relid in root_relids.iter() {
                GetPubPartitionOptionRelations(mcx, &mut relids, PublicationPartOpt::All, relid)?;
            }
        }
        let schemarelids =
            GetAllSchemaPublicationRelations(mcx, fields.oid, PublicationPartOpt::All)?;
        for &r in schemarelids.iter() {
            if !relids.contains(&r) {
                relids.push(r);
            }
        }
        InvalidatePublicationRels(&relids)?;
    }

    Ok(())
}

pub fn InvalidatePubRelSyncCache(mcx: Mcx<'_>, pubid: Oid, puballtables: bool) -> PgResult<()> {
    if puballtables {
        inval::invalidate::CacheInvalidateRelSyncAll()?;
    } else {
        let mut relids = GetPublicationRelations(mcx, pubid, PublicationPartOpt::All)?;
        let schemarelids = GetAllSchemaPublicationRelations(mcx, pubid, PublicationPartOpt::All)?;
        for &r in schemarelids.iter() {
            if !relids.contains(&r) {
                relids.push(r);
            }
        }
        for &relid in relids.iter() {
            inval::invalidate::CacheInvalidateRelSync(relid)?;
        }
    }
    Ok(())
}

fn AlterPublicationTables<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterPublicationStmt<'mcx>,
    fields: &PubTupleFields,
    tables: &[&'mcx PublicationTable<'mcx>],
    query_string: &str,
    mut publish_schema: bool,
) -> PgResult<()> {
    let pubid = fields.oid;

    if tables.is_empty() && stmt.action != AlterPublicationAction::AP_SetObjects {
        return Ok(());
    }

    let mut rels = OpenTableList(mcx, tables)?;

    match stmt.action {
        AlterPublicationAction::AP_AddObjects => {
            TransformPubWhereClauses(mcx, &mut rels, query_string, fields.pubviaroot)?;
            publish_schema |= is_schema_publication(mcx, pubid)?;
            CheckPubRelationColumnList(
                mcx,
                stmt.pubname.unwrap_or(""),
                &rels,
                publish_schema,
                fields.pubviaroot,
            )?;
            PublicationAddTables(mcx, pubid, &rels, false)?;
        }
        AlterPublicationAction::AP_DropObjects => {
            PublicationDropTables(mcx, pubid, &rels, false)?;
        }
        AlterPublicationAction::AP_SetObjects => {
            let oldrelids = GetPublicationRelations(mcx, pubid, PublicationPartOpt::Root)?;
            let mut delrels: Vec<PubRelOpen<'mcx>> = Vec::new();

            TransformPubWhereClauses(mcx, &mut rels, query_string, fields.pubviaroot)?;
            CheckPubRelationColumnList(
                mcx,
                stmt.pubname.unwrap_or(""),
                &rels,
                publish_schema,
                fields.pubviaroot,
            )?;

            for &oldrelid in oldrelids.iter() {
                let mut oldrelwhereclause: Option<Node<'mcx>> = None;
                let mut oldcolumns = Bitmapset::empty();
                if let Some(rftuple) = SearchSysCache2(
                    PUBLICATIONRELMAP,
                    SysCacheKey::Value(Datum::from_oid(oldrelid)),
                    SysCacheKey::Value(Datum::from_oid(pubid)),
                )? {
                    let (d, isnull) =
                        SysCacheGetAttr(PUBLICATIONRELMAP, &rftuple, Anum_pg_publication_rel_prqual)?;
                    if !isnull {
                        let s = text_from_datum(mcx, d)?;
                        oldrelwhereclause = Some(readfuncs::stringToNode(mcx, &s)?);
                    }
                    let (d, isnull) =
                        SysCacheGetAttr(PUBLICATIONRELMAP, &rftuple, Anum_pg_publication_rel_prattrs)?;
                    if !isnull {
                        pub_collist_to_bitmapset(mcx, &mut oldcolumns, d)?;
                    }
                    ReleaseSysCache(rftuple);
                }

                let mut found = false;
                for newpubrel in rels.iter() {
                    let newrelid = newpubrel.relation.rd_id;
                    let newcolumns =
                        pub_collist_validate(mcx, &newpubrel.relation, &newpubrel.columns)?;
                    if newrelid == oldrelid
                        && types_nodes::equal::equal_opt(oldrelwhereclause, newpubrel.whereClause)
                        && oldcolumns.equal(&newcolumns)
                    {
                        found = true;
                        break;
                    }
                }

                if !found {
                    delrels.push(PubRelOpen {
                        relation: table::table_open(mcx, oldrelid, ShareUpdateExclusiveLock)?,
                        whereClause: None,
                        columns: NodeList::nil(),
                    });
                }
            }

            PublicationDropTables(mcx, pubid, &delrels, true)?;
            PublicationAddTables(mcx, pubid, &rels, true)?;
            CloseTableList(delrels)?;
        }
    }

    CloseTableList(rels)
}

fn AlterPublicationSchemas<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterPublicationStmt<'mcx>,
    fields: &PubTupleFields,
    schemaidlist: &[Oid],
) -> PgResult<()> {
    if schemaidlist.is_empty() && stmt.action != AlterPublicationAction::AP_SetObjects {
        return Ok(());
    }

    LockSchemaList(schemaidlist)?;
    match stmt.action {
        AlterPublicationAction::AP_AddObjects => {
            let reloids = GetPublicationRelations(mcx, fields.oid, PublicationPartOpt::Root)?;
            for &reloid in reloids.iter() {
                let Some(coltuple) = SearchSysCache2(
                    PUBLICATIONRELMAP,
                    SysCacheKey::Value(Datum::from_oid(reloid)),
                    SysCacheKey::Value(Datum::from_oid(fields.oid)),
                )?
                else {
                    continue;
                };
                let has_collist =
                    !SysCacheGetAttr(PUBLICATIONRELMAP, &coltuple, Anum_pg_publication_rel_prattrs)?.1;
                ReleaseSysCache(coltuple);
                if has_collist {
                    return Err(Box::new(
                        PgError::error(format!(
                            "cannot add schema to publication \"{}\"",
                            stmt.pubname.unwrap_or("")
                        ))
                        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                        .with_detail(
                            "Schemas cannot be added if any tables that specify a column list \
                             are already part of the publication.",
                        ),
                    ));
                }
            }
            PublicationAddSchemas(mcx, fields.oid, schemaidlist, false)?;
        }
        AlterPublicationAction::AP_DropObjects => {
            PublicationDropSchemas(mcx, fields.oid, schemaidlist, false)?;
        }
        AlterPublicationAction::AP_SetObjects => {
            let oldschemaids = GetPublicationSchemas(mcx, fields.oid)?;
            let delschemas: Vec<Oid> = oldschemaids
                .iter()
                .copied()
                .filter(|s| !schemaidlist.contains(s))
                .collect();
            LockSchemaList(&delschemas)?;
            PublicationDropSchemas(mcx, fields.oid, &delschemas, true)?;
            PublicationAddSchemas(mcx, fields.oid, schemaidlist, true)?;
        }
    }
    Ok(())
}

fn CheckAlterPublication(
    stmt: &AlterPublicationStmt<'_>,
    puballtables: bool,
    has_tables: bool,
    has_schemas: bool,
) -> PgResult<()> {
    if (stmt.action == AlterPublicationAction::AP_AddObjects
        || stmt.action == AlterPublicationAction::AP_SetObjects)
        && has_schemas
        && !superuser::superuser()?
    {
        return Err(simple_err(
            ERRCODE_INSUFFICIENT_PRIVILEGE,
            "must be superuser to add or set schemas".into(),
        ));
    }

    if has_schemas && puballtables {
        return Err(Box::new(
            PgError::error(format!(
                "publication \"{}\" is defined as FOR ALL TABLES",
                stmt.pubname.unwrap_or("")
            ))
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_detail("Schemas cannot be added to or dropped from FOR ALL TABLES publications."),
        ));
    }

    if has_tables && puballtables {
        return Err(Box::new(
            PgError::error(format!(
                "publication \"{}\" is defined as FOR ALL TABLES",
                stmt.pubname.unwrap_or("")
            ))
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_detail("Tables cannot be added to or dropped from FOR ALL TABLES publications."),
        ));
    }
    Ok(())
}

fn fetch_pub_fields_by_oid<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    pubid: Oid,
) -> PgResult<Option<PubTupleFields>> {
    let keys = [eq_key(
        Anum_pg_publication_oid as AttrNumber,
        F_OIDEQ,
        Datum::from_oid(pubid),
    )];
    let mut scan =
        genam::systable_beginscan(mcx, rel, PublicationObjectIndexId, true, None, &keys)?;
    let fields = genam::systable_getnext(mcx, &mut scan)?.map(|tup| pub_fields(rel.descr(), tup));
    genam::systable_endscan(mcx, scan)?;
    Ok(fields)
}

pub fn AlterPublication<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterPublicationStmt<'mcx>,
    query_string: &str,
) -> PgResult<()> {
    let pubname = stmt.pubname.expect("AlterPublicationStmt.pubname");
    let rel = table::table_open(mcx, PublicationRelationId, RowExclusiveLock)?;

    let pubid = lsyscache::get_publication_oid(pubname, true)?;
    if pubid == InvalidOid {
        return Err(simple_err(
            ERRCODE_UNDEFINED_OBJECT,
            format!("publication \"{pubname}\" does not exist"),
        ));
    }

    if !aclchk::object_ownercheck(PublicationRelationId, pubid, miscinit::GetUserId())? {
        aclchk::aclcheck_error(aclchk::ACLCHECK_NOT_OWNER, ObjectType::OBJECT_PUBLICATION, pubname)?;
    }

    if !stmt.options.is_nil() {
        AlterPublicationOptions(mcx, stmt, &rel, query_string)?;
    } else {
        let (relations, schemaidlist) = ObjectsInPublicationToOids(mcx, &stmt.pubobjects)?;

        let fields = fetch_pub_fields_by_oid(mcx, &rel, pubid)?
            .expect("publication row visible under RowExclusiveLock");
        CheckAlterPublication(stmt, fields.puballtables, !relations.is_empty(), !schemaidlist.is_empty())?;

        lmgr::LockDatabaseObject(PublicationRelationId, pubid, 0, AccessExclusiveLock)?;

        let Some(fields) = fetch_pub_fields_by_oid(mcx, &rel, pubid)? else {
            return Err(simple_err(
                ERRCODE_UNDEFINED_OBJECT,
                format!("publication \"{pubname}\" does not exist"),
            ));
        };

        AlterPublicationTables(mcx, stmt, &fields, &relations, query_string, !schemaidlist.is_empty())?;
        AlterPublicationSchemas(mcx, stmt, &fields, &schemaidlist)?;
    }

    rel.close(RowExclusiveLock)
}

pub fn RemovePublicationById<'mcx>(mcx: Mcx<'mcx>, pubid: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, PublicationRelationId, RowExclusiveLock)?;
    let keys = [eq_key(Anum_pg_publication_oid as AttrNumber, F_OIDEQ, Datum::from_oid(pubid))];
    let mut scan =
        genam::systable_beginscan(mcx, &rel, PublicationObjectIndexId, true, None, &keys)?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for publication {pubid}"
        ))));
    };
    let puballtables = getattr(rel.descr(), tup, Anum_pg_publication_puballtables).0.as_bool();
    let tid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;

    if puballtables {
        inval::invalidate::CacheInvalidateRelcacheAll()?;
    }

    catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    rel.close(RowExclusiveLock)
}

pub fn RemovePublicationRelById<'mcx>(mcx: Mcx<'mcx>, proid: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, PublicationRelRelationId, RowExclusiveLock)?;
    let keys = [eq_key(Anum_pg_publication_rel_oid as AttrNumber, F_OIDEQ, Datum::from_oid(proid))];
    let mut scan =
        genam::systable_beginscan(mcx, &rel, PublicationRelObjectIndexId, true, None, &keys)?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for publication table {proid}"
        ))));
    };
    let prrelid = getattr(rel.descr(), tup, Anum_pg_publication_rel_prrelid).0.as_oid();
    let tid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;

    let mut relids: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    GetPubPartitionOptionRelations(mcx, &mut relids, PublicationPartOpt::All, prrelid)?;
    InvalidatePublicationRels(&relids)?;

    catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    rel.close(RowExclusiveLock)
}

pub fn RemovePublicationSchemaById<'mcx>(mcx: Mcx<'mcx>, psoid: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, PublicationNamespaceRelationId, RowExclusiveLock)?;
    let keys = [eq_key(
        Anum_pg_publication_namespace_oid as AttrNumber,
        F_OIDEQ,
        Datum::from_oid(psoid),
    )];
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        PublicationNamespaceObjectIndexId,
        true,
        None,
        &keys,
    )?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for publication schema {psoid}"
        ))));
    };
    let pnnspid = getattr(
        rel.descr(),
        tup,
        pg_publication::Anum_pg_publication_namespace_pnnspid,
    )
    .0
    .as_oid();
    let tid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;

    let schemaRels = GetSchemaPublicationRelations(mcx, pnnspid, PublicationPartOpt::All)?;
    InvalidatePublicationRels(&schemaRels)?;

    catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    rel.close(RowExclusiveLock)
}

fn AlterPublicationOwner_internal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tup: &HeapTupleData<'_>,
    newOwnerId: Oid,
) -> PgResult<Option<(heaptuple::HeapTuple<'mcx>, types_tuple::ItemPointerData)>> {
    let td = rel.descr();
    let oid = getattr(td, tup, Anum_pg_publication_oid).0.as_oid();
    let pubowner = getattr(td, tup, Anum_pg_publication_pubowner).0.as_oid();
    let puballtables = getattr(td, tup, Anum_pg_publication_puballtables).0.as_bool();
    let pubname_d = getattr(td, tup, Anum_pg_publication_pubname).0;
    // SAFETY: a name attr datum addresses NAMEDATALEN in-tuple bytes.
    let name_data =
        unsafe { core::ptr::read_unaligned(pubname_d.as_usize() as *const types_tuple::NameData) };
    let pubname =
        core::str::from_utf8(name_data.name_str()).expect("pubname is UTF-8").to_string();

    if pubowner == newOwnerId {
        return Ok(None);
    }

    if !superuser::superuser()? {
        if !aclchk::object_ownercheck(PublicationRelationId, oid, miscinit::GetUserId())? {
            aclchk::aclcheck_error(
                aclchk::ACLCHECK_NOT_OWNER,
                ObjectType::OBJECT_PUBLICATION,
                &pubname,
            )?;
        }

        if !adt_acl::member_can_set_role(miscinit::GetUserId(), newOwnerId)? {
            let rolename = miscinit::GetUserNameFromId(mcx, newOwnerId, false)?
                .expect("noerr=false yields a name");
            return Err(simple_err(
                ERRCODE_INSUFFICIENT_PRIVILEGE,
                format!("must be able to SET ROLE \"{}\"", rolename.as_str()),
            ));
        }

        let aclresult = aclchk::object_aclcheck(
            DATABASE_RELATION_ID,
            init_small::globals::MyDatabaseId(),
            newOwnerId,
            adt_acl::ACL_CREATE,
        )?;
        if aclresult != aclchk::ACLCHECK_OK {
            let dbname = dbcommands::get_database_name(init_small::globals::MyDatabaseId())?
                .unwrap_or_default();
            aclchk::aclcheck_error(aclresult, ObjectType::OBJECT_DATABASE, &dbname)?;
        }

        if puballtables && !superuser::superuser_arg(newOwnerId)? {
            return Err(Box::new(
                PgError::error(format!(
                    "permission denied to change owner of publication \"{pubname}\""
                ))
                .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .with_hint("The owner of a FOR ALL TABLES publication must be a superuser."),
            ));
        }

        if !superuser::superuser_arg(newOwnerId)? && is_schema_publication(mcx, oid)? {
            return Err(Box::new(
                PgError::error(format!(
                    "permission denied to change owner of publication \"{pubname}\""
                ))
                .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .with_hint("The owner of a FOR TABLES IN SCHEMA publication must be a superuser."),
            ));
        }
    }

    let natts = td.natts as usize;
    let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    repl_values[(Anum_pg_publication_pubowner - 1) as usize] = Datum::from_oid(newOwnerId);
    repl[(Anum_pg_publication_pubowner - 1) as usize] = true;
    let new_tuple = heaptuple::heap_modify_tuple(mcx, tup, td, &repl_values, &repl_isnull, &repl)?;
    Ok(Some((new_tuple, tup.t_self)))
}

fn alter_owner_scan<'mcx>(
    mcx: Mcx<'mcx>,
    key: ScanKeyData,
    index: Oid,
    missing: impl FnOnce() -> Box<PgError>,
    newOwnerId: Oid,
) -> PgResult<Oid> {
    let rel = table::table_open(mcx, PublicationRelationId, RowExclusiveLock)?;
    let keys = [key];
    let mut scan = genam::systable_beginscan(mcx, &rel, index, true, None, &keys)?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(missing());
    };
    let pubid = getattr(rel.descr(), tup, Anum_pg_publication_oid).0.as_oid();
    let update = AlterPublicationOwner_internal(mcx, &rel, tup, newOwnerId)?;
    genam::systable_endscan(mcx, scan)?;
    if let Some((mut new_tuple, otid)) = update {
        catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut new_tuple)?;
        pg_shdepend::changeDependencyOnOwner(mcx, PublicationRelationId, pubid, newOwnerId)?;
    }
    rel.close(RowExclusiveLock)?;
    Ok(pubid)
}

pub fn AlterPublicationOwner<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    newOwnerId: Oid,
) -> PgResult<ObjectAddress> {
    let pname = name_arg(mcx, name)?;
    let key = eq_key(
        Anum_pg_publication_pubname as AttrNumber,
        F_NAMEEQ,
        Datum::from_usize(pname.as_ptr() as usize),
    );
    let pubid = alter_owner_scan(
        mcx,
        key,
        PublicationNameIndexId,
        || {
            simple_err(
                ERRCODE_UNDEFINED_OBJECT,
                format!("publication \"{name}\" does not exist"),
            )
        },
        newOwnerId,
    )?;
    Ok(ObjectAddress::set(PublicationRelationId, pubid))
}

pub fn AlterPublicationOwner_oid<'mcx>(mcx: Mcx<'mcx>, pubid: Oid, newOwnerId: Oid) -> PgResult<()> {
    let key = eq_key(Anum_pg_publication_oid as AttrNumber, F_OIDEQ, Datum::from_oid(pubid));
    alter_owner_scan(
        mcx,
        key,
        PublicationObjectIndexId,
        || {
            simple_err(
                ERRCODE_UNDEFINED_OBJECT,
                format!("publication with OID {pubid} does not exist"),
            )
        },
        newOwnerId,
    )?;
    Ok(())
}

struct RfColumnWalker<'a, 'mcx> {
    mcx: Mcx<'mcx>,
    identity: &'a [i16],
    pubviaroot: bool,
    relid: Oid,
    parentid: Oid,
}

impl<'a, 'mcx> NodeWalker<'mcx> for RfColumnWalker<'a, 'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if node.node_tag() == NodeTag::T_Var {
            let var = node.as_variant::<Var>().unwrap();
            let mut attnum = var.varattno;
            if self.pubviaroot {
                let colname = lsyscache::get_attname(self.mcx, self.parentid, attnum, false)?
                    .expect("missing_ok=false yields a name");
                attnum = lsyscache::get_attnum(self.relid, colname.as_str())?;
            }
            if !self.identity.contains(&attnum) {
                return Ok(true);
            }
        }
        expression_tree_walker(node, self)
    }
}

pub fn pub_rf_contains_invalid_column<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    relation: &Relation<'mcx>,
    ancestors: &[Oid],
    pubviaroot: bool,
) -> PgResult<bool> {
    let relid = relation.rd_id;
    let mut publish_as_relid = relid;

    if relation.rd_rel.relreplident == REPLICA_IDENTITY_FULL {
        return Ok(false);
    }

    if pubviaroot && relation.rd_rel.relispartition {
        publish_as_relid = GetTopMostAncestorInPublication(mcx, pubid, ancestors, None)?;
        if publish_as_relid == InvalidOid {
            publish_as_relid = relid;
        }
    }

    let Some(rftuple) = SearchSysCache2(
        PUBLICATIONRELMAP,
        SysCacheKey::Value(Datum::from_oid(publish_as_relid)),
        SysCacheKey::Value(Datum::from_oid(pubid)),
    )?
    else {
        return Ok(false);
    };

    let (rfdatum, rfisnull) =
        SysCacheGetAttr(PUBLICATIONRELMAP, &rftuple, Anum_pg_publication_rel_prqual)?;
    let mut result = false;
    if !rfisnull {
        let bitmaps = relcache::indexattr::RelationGetIndexAttrBitmap(relid)?;
        let s = text_from_datum(mcx, rfdatum)?;
        let rfnode = readfuncs::stringToNode(mcx, &s)?;
        let mut walker = RfColumnWalker {
            mcx,
            identity: &bitmaps.identity,
            pubviaroot,
            relid,
            parentid: publish_as_relid,
        };
        result = walker.visit(rfnode)?;
    }
    ReleaseSysCache(rftuple);
    Ok(result)
}

pub fn pub_contains_invalid_column<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    relation: &Relation<'mcx>,
    ancestors: &[Oid],
    pubviaroot: bool,
    pubgencols_type: u8,
    invalid_column_list: &mut bool,
    invalid_gen_col: &mut bool,
) -> PgResult<bool> {
    let relid = relation.rd_id;
    let mut publish_as_relid = relid;
    let desc = relation.descr();

    *invalid_column_list = false;
    *invalid_gen_col = false;

    if pubviaroot && relation.rd_rel.relispartition {
        publish_as_relid = GetTopMostAncestorInPublication(mcx, pubid, ancestors, None)?;
        if publish_as_relid == InvalidOid {
            publish_as_relid = relid;
        }
    }

    let publication = GetPublication(mcx, pubid)?;
    let mut columns = Bitmapset::empty();
    let has_column_list =
        check_and_fetch_column_list(mcx, &publication, publish_as_relid, Some(&mut columns))?;

    if relation.rd_rel.relreplident == REPLICA_IDENTITY_FULL {
        *invalid_column_list = has_column_list;
        if let Some(constr) = desc.constr.as_ref() {
            if pubgencols_type != PUBLISH_GENCOLS_STORED && constr.has_generated_stored {
                *invalid_gen_col = true;
            }
            if constr.has_generated_virtual {
                *invalid_gen_col = true;
            }
        }
        if *invalid_gen_col && *invalid_column_list {
            return Ok(true);
        }
    }

    let bitmaps = relcache::indexattr::RelationGetIndexAttrBitmap(relid)?;
    for &id_attnum in bitmaps.identity.iter() {
        let mut attnum = id_attnum;
        let att = desc.attr((attnum - 1) as usize);

        if !has_column_list {
            if att.attgenerated as u8 == ATTRIBUTE_GENERATED_STORED
                && pubgencols_type != PUBLISH_GENCOLS_STORED
            {
                *invalid_gen_col = true;
                break;
            }
            if att.attgenerated as u8 == ATTRIBUTE_GENERATED_VIRTUAL {
                *invalid_gen_col = true;
                break;
            }
            continue;
        }

        if pubviaroot {
            let colname = lsyscache::get_attname(mcx, relid, attnum, false)?
                .expect("missing_ok=false yields a name");
            attnum = lsyscache::get_attnum(publish_as_relid, colname.as_str())?;
        }

        *invalid_column_list |= !columns.is_member(attnum as i32);
        if *invalid_column_list && *invalid_gen_col {
            break;
        }
    }

    Ok(*invalid_column_list || *invalid_gen_col)
}

pub fn init_seams() {
    publicationcmds_seams::remove_publication_by_id::set(RemovePublicationById);
    publicationcmds_seams::remove_publication_rel_by_id::set(RemovePublicationRelById);
    publicationcmds_seams::remove_publication_schema_by_id::set(RemovePublicationSchemaById);
    pg_shdepend::alter_publication_owner_oid::set(AlterPublicationOwner_oid);
}
