// LIKE arm: transformTableLikeClause + expandTableLikeClause +
// generateClonedIndexStmt. LOUD: identity/generated/compression copy,
// non-default opclass/collation, INCLUDE, extended statistics.
use mcx::{Mcx, PgVec};
use types_core::{AttrNumber, InvalidOid, Oid, NAMEDATALEN, RELATION_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::parsenodes::{
    AlterTableCmd, AlterTableStmt, AlterTableType, CommentStmt, ObjectType,
};
use types_nodes::primnodes::RangeVar;
use types_nodes::rawnodes::{
    ColumnDef, Constraint, ConstrType, IndexElem, IndexStmt, SortByDir, SortByNulls,
    TableLikeClause, TypeName, CREATE_TABLE_LIKE_COMMENTS, CREATE_TABLE_LIKE_COMPRESSION,
    CREATE_TABLE_LIKE_CONSTRAINTS, CREATE_TABLE_LIKE_DEFAULTS, CREATE_TABLE_LIKE_GENERATED,
    CREATE_TABLE_LIKE_IDENTITY, CREATE_TABLE_LIKE_INDEXES, CREATE_TABLE_LIKE_STATISTICS,
    CREATE_TABLE_LIKE_STORAGE,
};
use types_nodes::{Node, NodeList};
use types_rel::{AccessShareLock, NoLock, Relation};

use crate::unported;

const RELKIND_RELATION: u8 = b'r';
const RELKIND_VIEW: u8 = b'v';
const RELKIND_MATVIEW: u8 = b'm';
const RELKIND_COMPOSITE_TYPE: u8 = b'c';
const RELKIND_FOREIGN_TABLE: u8 = b'f';
const RELKIND_PARTITIONED_TABLE: u8 = b'p';
const BTREE_AM_OID: Oid = 403;
const ACL_SELECT: u64 = 1 << 1;
const INDOPTION_DESC: i16 = 1 << 0;
const INDOPTION_NULLS_FIRST: i16 = 1 << 1;
const CONSTRAINT_RELATION_ID: Oid = 2606;
const StatisticExtRelationId: Oid = 3381;
const StatisticExtRelidIndexId: Oid = 3379;
const IndexRelidIndexId: Oid = 2679;
const Anum_pg_index_indclass: i32 = 18;

const EXPAND_OPTIONS: u32 = CREATE_TABLE_LIKE_DEFAULTS
    | CREATE_TABLE_LIKE_GENERATED
    | CREATE_TABLE_LIKE_CONSTRAINTS
    | CREATE_TABLE_LIKE_INDEXES
    | CREATE_TABLE_LIKE_STATISTICS;

pub(crate) struct LikeCxt<'a, 'mcx> {
    pub relation: &'mcx RangeVar<'mcx>,
    pub columns: &'a mut NodeList<'mcx>,
    pub nnconstraints: &'a mut NodeList<'mcx>,
    pub likeclauses: &'a mut NodeList<'mcx>,
    pub alist: &'a mut NodeList<'mcx>,
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let mut v: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, s.len())?;
    mcx::vec_append_bytes(&mut v, s.as_bytes())?;
    Ok(core::str::from_utf8(v.leak()).expect("was UTF-8"))
}

fn rel_vocab_rv<'a>(rv: &'a RangeVar<'a>) -> rel_vocab::RangeVar<'a> {
    rel_vocab::RangeVar {
        catalogname: rv.catalogname,
        schemaname: rv.schemaname,
        relname: rv.relname.expect("RangeVar.relname"),
        inh: rv.inh,
        relpersistence: rv.relpersistence,
        location: rv.location,
    }
}

#[cold]
#[inline(never)]
fn errdetail_relkind_not_supported(relkind: u8) -> &'static str {
    match relkind {
        b'S' => "This operation is not supported for sequences.",
        b'i' | b'I' => "This operation is not supported for indexes.",
        b't' => "This operation is not supported for TOAST tables.",
        _ => "This operation is not supported for this kind of relation.",
    }
}

pub(crate) fn transformTableLikeClause<'mcx>(
    mcx: Mcx<'mcx>,
    cxt: &mut LikeCxt<'_, 'mcx>,
    tlc_node: Node<'mcx>,
    query_string: &str,
) -> PgResult<()> {
    let tlc = tlc_node.as_variant::<TableLikeClause>().expect("TableLikeClause");
    let options = tlc.options;
    let src_rv = tlc.relation.expect("TableLikeClause.relation");
    let location = src_rv.location;

    let attach_errpos = |mut e: Box<PgError>| -> Box<PgError> {
        if e.cursor_position().is_none() {
            let pos = parser_small1::parser_errposition_source(
                Some(query_string.as_bytes()),
                location,
                mbutils::GetDatabaseEncoding(),
            );
            if pos > 0 {
                e = Box::new((*e).with_cursor_position(pos));
            }
        }
        e
    };

    let rv = rel_vocab_rv(src_rv);
    let relid = catalog_namespace::RangeVarGetRelid(&rv, AccessShareLock, false)
        .map_err(attach_errpos)?;
    let relation = relation::relation_open(mcx, relid, NoLock)?;

    let relkind = relation.rd_rel.relkind;
    match relkind {
        RELKIND_RELATION | RELKIND_VIEW | RELKIND_MATVIEW => {}
        RELKIND_COMPOSITE_TYPE | RELKIND_FOREIGN_TABLE | RELKIND_PARTITIONED_TABLE => {
            unported("LIKE from composite/foreign/partitioned relations")
        }
        _ => {
            return Err(attach_errpos(Box::new(
                PgError::new(
                    ERROR,
                    format!("relation \"{}\" is invalid in LIKE clause", relation.name()),
                )
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
                .with_detail(errdetail_relkind_not_supported(relkind)),
            )))
        }
    }

    let aclresult = aclchk::pg_class_aclcheck(relid, miscinit::GetUserId(), ACL_SELECT)?;
    if aclresult != 0 {
        // get_relkind_objtype (objectaddress.c) for the reachable kinds.
        let objtype = match relkind {
            RELKIND_VIEW => ObjectType::OBJECT_VIEW,
            RELKIND_MATVIEW => ObjectType::OBJECT_MATVIEW,
            _ => ObjectType::OBJECT_TABLE,
        };
        aclchk::aclcheck_error(aclresult, objtype, relation.name())?;
    }

    let tuple_desc = &relation.rd_att;
    for i in 0..tuple_desc.natts as usize {
        let attribute = tuple_desc.attr(i);
        if attribute.attisdropped {
            continue;
        }
        let attname =
            str_in(mcx, core::str::from_utf8(attribute.attname.name_str()).expect("attname"))?;
        if attname.len() >= NAMEDATALEN as usize {
            unported("overlength column name truncation");
        }
        let tn = TypeName {
            typeOid: attribute.atttypid,
            typemod: attribute.atttypmod,
            location: -1,
            ..TypeName::default()
        };
        let mut def = ColumnDef {
            colname: Some(attname),
            typeName: Some(Node::mk(mcx, tn)?),
            is_local: true,
            collOid: attribute.attcollation,
            location: -1,
            ..ColumnDef::default()
        };
        if attribute.atthasdef
            && attribute.attgenerated != 0
            && (options & CREATE_TABLE_LIKE_GENERATED) != 0
        {
            unported("LIKE INCLUDING GENERATED (generated columns)");
        }
        if attribute.attidentity != 0 && (options & CREATE_TABLE_LIKE_IDENTITY) != 0 {
            unported("LIKE INCLUDING IDENTITY (identity sequences)");
        }
        if (options & CREATE_TABLE_LIKE_STORAGE) != 0 {
            def.storage = attribute.attstorage as u8;
        }
        if (options & CREATE_TABLE_LIKE_COMPRESSION) != 0 && attribute.attcompression != 0 {
            unported("LIKE INCLUDING COMPRESSION (GetCompressionMethodName)");
        }
        if (options & CREATE_TABLE_LIKE_COMMENTS) != 0 {
            if let Some(comment) =
                commands_comment::GetComment(mcx, relid, RELATION_RELATION_ID, i as i32 + 1)?
            {
                let stmt = make_comment_stmt(
                    mcx,
                    ObjectType::OBJECT_COLUMN,
                    cxt.relation,
                    attname,
                    comment.as_str(),
                )?;
                cxt.alist.lappend(mcx, stmt)?;
            }
        }
        cxt.columns.lappend(mcx, Node::mk(mcx, def)?)?;
    }

    let has_not_null =
        relation.rd_att.constr.as_deref().map(|c| c.has_not_null).unwrap_or(false);
    if has_not_null {
        let lst = pg_constraint::RelationGetNotNullConstraints(mcx, &relation, true)?;
        if (options & CREATE_TABLE_LIKE_COMMENTS) != 0 {
            for nnode in lst.iter() {
                let nn = nnode.as_variant::<Constraint>().expect("Constraint");
                let conname = nn.conname.expect("copied not-null conname");
                let con_oid = pg_constraint::get_relation_constraint_oid(mcx, relid, conname)?;
                if let Some(comment) =
                    commands_comment::GetComment(mcx, con_oid, CONSTRAINT_RELATION_ID, 0)?
                {
                    let stmt = make_comment_stmt(
                        mcx,
                        ObjectType::OBJECT_TABCONSTRAINT,
                        cxt.relation,
                        conname,
                        comment.as_str(),
                    )?;
                    cxt.alist.lappend(mcx, stmt)?;
                }
            }
        }
        cxt.nnconstraints.concat(mcx, &lst)?;
    }

    if options & EXPAND_OPTIONS != 0 {
        // SAFETY: parse tree is analyze-owned; no derived refs live.
        unsafe {
            tlc_node
                .with_mut::<TableLikeClause, _>(|t| t.relationOid = relid)
                .expect("TableLikeClause");
        }
        cxt.likeclauses.lappend(mcx, tlc_node)?;
    }

    // Keep the AccessShareLock until xact commit (C table_close NoLock).
    relation.close(NoLock)?;
    Ok(())
}

fn make_comment_stmt<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    relation: &RangeVar<'_>,
    lastname: &str,
    comment: &str,
) -> PgResult<Node<'mcx>> {
    let mut object = NodeList::nil();
    if let Some(schema) = relation.schemaname {
        object.lappend(mcx, Node::mk_string(mcx, str_in(mcx, schema)?)?)?;
    }
    object.lappend(
        mcx,
        Node::mk_string(mcx, str_in(mcx, relation.relname.expect("relname"))?)?,
    )?;
    object.lappend(mcx, Node::mk_string(mcx, str_in(mcx, lastname)?)?)?;
    let stmt = CommentStmt {
        objtype,
        object: Some(Node::mk_list(mcx, object)?),
        comment: Some(str_in(mcx, comment)?),
    };
    Node::mk(mcx, stmt)
}

pub fn expandTableLikeClause<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &'mcx RangeVar<'mcx>,
    tlc: &TableLikeClause<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    assert!(
        tlc.relationOid != InvalidOid,
        "expandTableLikeClause called on untransformed LIKE clause"
    );
    let options = tlc.options;
    let relation = relation::relation_open(mcx, tlc.relationOid, NoLock)?;
    let tuple_desc = &relation.rd_att;

    let child_relid = catalog_namespace::RangeVarGetRelid(&rel_vocab_rv(heap_rel), NoLock, false)?;
    let childrel = relation::relation_open(mcx, child_relid, NoLock)?;

    // build_attrmap_by_name(child, parent): attmap[parent_attno-1] = child attno.
    let mut attmap: PgVec<'mcx, AttrNumber> =
        mcx::vec_with_capacity_in(mcx, tuple_desc.natts as usize)?;
    for i in 0..tuple_desc.natts as usize {
        let pa = tuple_desc.attr(i);
        if pa.attisdropped {
            attmap.push(0);
            continue;
        }
        let mut child_attno = 0;
        for j in 0..childrel.rd_att.natts as usize {
            let ca = childrel.rd_att.attr(j);
            if !ca.attisdropped && ca.attname.name_str() == pa.attname.name_str() {
                assert!(
                    ca.atttypid == pa.atttypid && ca.atttypmod == pa.atttypmod,
                    "attribute \"{}\" of relation \"{}\" does not match parent's type",
                    relation.name(),
                    childrel.name()
                );
                child_attno = ca.attnum;
                break;
            }
        }
        assert!(child_attno != 0, "LIKE column vanished from child relation");
        attmap.push(child_attno);
    }

    let mut result = NodeList::nil();
    let mut atsubcmds = NodeList::nil();

    let constr = tuple_desc.constr.as_deref();
    if (options & (CREATE_TABLE_LIKE_DEFAULTS | CREATE_TABLE_LIKE_GENERATED)) != 0
        && constr.is_some()
    {
        for i in 0..tuple_desc.natts as usize {
            let attribute = tuple_desc.attr(i);
            if attribute.attisdropped || !attribute.atthasdef {
                continue;
            }
            let wanted = if attribute.attgenerated != 0 {
                CREATE_TABLE_LIKE_GENERATED
            } else {
                CREATE_TABLE_LIKE_DEFAULTS
            };
            if options & wanted == 0 {
                continue;
            }
            if attribute.attgenerated != 0 {
                unported("LIKE INCLUDING GENERATED (generation expressions)");
            }
            let defbin = tupdesc::TupleDescGetDefaultBin(tuple_desc, (i + 1) as AttrNumber)
                .unwrap_or_else(|| {
                    panic!(
                        "default expression not found for attribute {} of relation \"{}\"",
                        i + 1,
                        relation.name()
                    )
                });
            let this_default = readfuncs::stringToNode(mcx, defbin.as_str())?;
            let (mapped, found_whole_row) =
                rewrite_manip::map_variable_attnos(mcx, this_default, 1, 0, &attmap)?;
            if found_whole_row {
                return Err(whole_row_error(
                    format!(
                        "Generation expression for column \"{}\" contains a whole-row reference to table \"{}\".",
                        core::str::from_utf8(attribute.attname.name_str()).expect("attname"),
                        relation.name()
                    ),
                ));
            }
            let atsubcmd = AlterTableCmd {
                subtype: AlterTableType::AT_CookedColumnDefault,
                num: attmap[i],
                def: Some(mapped),
                ..AlterTableCmd::default()
            };
            atsubcmds.lappend(mcx, Node::mk(mcx, atsubcmd)?)?;
        }
    }

    if (options & CREATE_TABLE_LIKE_CONSTRAINTS) != 0 {
        if let Some(constr) = constr {
            for cc in constr.check[..constr.num_check as usize].iter() {
                let ccname = cc.ccname.as_ref().expect("check constraint name").as_str();
                let ccbin = cc.ccbin.as_ref().expect("check constraint bin").as_str();
                let ccbin_node = readfuncs::stringToNode(mcx, ccbin)?;
                let (mapped, found_whole_row) =
                    rewrite_manip::map_variable_attnos(mcx, ccbin_node, 1, 0, &attmap)?;
                if found_whole_row {
                    return Err(whole_row_error(format!(
                        "Constraint \"{}\" contains a whole-row reference to table \"{}\".",
                        ccname,
                        relation.name()
                    )));
                }
                let n = Constraint {
                    contype: ConstrType::CONSTR_CHECK,
                    conname: Some(str_in(mcx, ccname)?),
                    location: -1,
                    is_enforced: cc.ccenforced,
                    initially_valid: cc.ccenforced,
                    is_no_inherit: cc.ccnoinherit,
                    raw_expr: None,
                    cooked_expr: Some(str_in(mcx, outfuncs::nodeToString(mcx, mapped)?.as_str())?),
                    skip_validation: true,
                    ..Constraint::default()
                };
                let atsubcmd = AlterTableCmd {
                    subtype: AlterTableType::AT_AddConstraint,
                    def: Some(Node::mk(mcx, n)?),
                    ..AlterTableCmd::default()
                };
                atsubcmds.lappend(mcx, Node::mk(mcx, atsubcmd)?)?;

                if (options & CREATE_TABLE_LIKE_COMMENTS) != 0 {
                    let con_oid = pg_constraint::get_relation_constraint_oid(
                        mcx,
                        relation.rd_id,
                        ccname,
                    )?;
                    if let Some(comment) =
                        commands_comment::GetComment(mcx, con_oid, CONSTRAINT_RELATION_ID, 0)?
                    {
                        let stmt = make_comment_stmt(
                            mcx,
                            ObjectType::OBJECT_TABCONSTRAINT,
                            heap_rel,
                            ccname,
                            comment.as_str(),
                        )?;
                        result.lappend(mcx, stmt)?;
                    }
                }
            }
        }
    }

    if !atsubcmds.is_nil() {
        let atcmd = AlterTableStmt {
            relation: Some(heap_rel),
            cmds: atsubcmds,
            objtype: ObjectType::OBJECT_TABLE,
            missing_ok: false,
        };
        result.lcons(mcx, Node::mk(mcx, atcmd)?)?;
    }

    if (options & CREATE_TABLE_LIKE_INDEXES) != 0
        && relation.rd_rel.relhasindex
        && childrel.rd_rel.relkind != RELKIND_FOREIGN_TABLE
    {
        let parent_indexes = relcache::RelationGetIndexList(mcx, relation.rd_id)?;
        for &parent_index_oid in parent_indexes.iter() {
            let parent_index = indexam::index_open(mcx, parent_index_oid, AccessShareLock)?;
            let mut index_stmt =
                generateClonedIndexStmt(mcx, Some(heap_rel), &parent_index, &attmap)?.0;
            if (options & CREATE_TABLE_LIKE_COMMENTS) != 0 {
                if let Some(comment) =
                    commands_comment::GetComment(mcx, parent_index_oid, RELATION_RELATION_ID, 0)?
                {
                    index_stmt.idxcomment = Some(str_in(mcx, comment.as_str())?);
                }
            }
            result.lappend(mcx, Node::mk(mcx, index_stmt)?)?;
            indexam::index_close(parent_index, AccessShareLock)?;
        }
    }

    if (options & CREATE_TABLE_LIKE_STATISTICS) != 0
        && has_extended_statistics(mcx, relation.rd_id)?
    {
        unported("LIKE INCLUDING STATISTICS (generateClonedExtStatsStmt)");
    }

    childrel.close(NoLock)?;
    relation.close(NoLock)?;
    Ok(result)
}

#[cold]
#[inline(never)]
fn whole_row_error(detail: String) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, "cannot convert whole-row table reference".to_string())
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail(detail),
    )
}

pub fn generateClonedIndexStmt<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: Option<&'mcx RangeVar<'mcx>>,
    source_idx: &Relation<'mcx>,
    attmap: &[AttrNumber],
) -> PgResult<(IndexStmt<'mcx>, Oid)> {
    let idxrec = source_idx.rd_index.as_ref().expect("index relation without rd_index");
    let indrelid = idxrec.indrelid;
    let mut constraint_oid = InvalidOid;

    // get_am_name over the closed AM set (AMOID syscache unported).
    let amname = match source_idx.rd_rel.relam {
        BTREE_AM_OID => "btree",
        405 => "hash",
        2742 => "gin",
        783 => "gist",
        4000 => "spgist",
        3580 => "brin",
        other => unported(&format!("generateClonedIndexStmt: index AM {other}")),
    };
    if source_idx.rd_rel.reltablespace != InvalidOid {
        unported("generateClonedIndexStmt: TABLESPACE");
    }
    if source_idx.rd_options.is_some() {
        unported("generateClonedIndexStmt: index reloptions (untransformRelOptions)");
    }
    if idxrec.indisexclusion {
        unported("generateClonedIndexStmt: exclusion constraints");
    }
    if idxrec.indnatts != idxrec.indnkeyatts {
        unported("generateClonedIndexStmt: INCLUDE columns");
    }

    let mut stmt = IndexStmt {
        relation: heap_rel,
        accessMethod: Some(amname),
        unique: idxrec.indisunique,
        nulls_not_distinct: idxrec.indnullsnotdistinct,
        primary: idxrec.indisprimary,
        transformed: true,
        ..IndexStmt::default()
    };

    if stmt.primary || stmt.unique {
        let constraint_id = pg_depend::get_index_constraint(mcx, source_idx.rd_id)?;
        if constraint_id != InvalidOid {
            stmt.isconstraint = true;
            let (condeferrable, condeferred) =
                pg_constraint::get_constraint_deferrability(mcx, constraint_id)?;
            stmt.deferrable = condeferrable;
            stmt.initdeferred = condeferred;
            constraint_oid = constraint_id;
        }
    }

    let indclass = read_indclass(mcx, source_idx.rd_id, idxrec.indnkeyatts as usize)?;
    let indexprs = match idxrec.indexprs_src.as_ref() {
        Some(src) => Some(
            readfuncs::stringToNode(mcx, src.as_str())?
                .as_list()
                .expect("indexprs is a List"),
        ),
        None => None,
    };
    let mut indexpr_item = indexprs.into_iter().flat_map(|l| l.iter());
    let mut params = NodeList::nil();
    for keyno in 0..idxrec.indnkeyatts as usize {
        let attnum = idxrec.indkey[keyno];
        let opt = source_idx.rd_indoption[keyno];
        let (elem_name, elem_expr, keycoltype) = if attnum != 0 {
            let attname = lsyscache::get_attname(mcx, indrelid, attnum, false)?
                .expect("index key column");
            (
                Some(str_in(mcx, attname.as_str())?),
                None,
                lsyscache::get_atttype(indrelid, attnum)?,
            )
        } else {
            let indexkey =
                indexpr_item.next().expect("too few entries in indexprs list");
            let (mapped, found_whole_row) =
                rewrite_manip::map_variable_attnos(mcx, indexkey, 1, 0, attmap)?;
            if found_whole_row {
                return Err(whole_row_error(format!(
                    "Index \"{}\" contains a whole-row table reference.",
                    source_idx.name()
                )));
            }
            (None, Some(mapped), nodes_core::expr_type(mapped))
        };

        let indcollation = source_idx.rd_indcollation[keyno];
        let typcollation = syscache_seams::lookup_pg_type_shape::call(keycoltype)?
            .expect("pg_type row vanished")
            .typcollation;
        if indcollation != InvalidOid && indcollation != typcollation {
            unported("generateClonedIndexStmt: non-default collations");
        }
        if indclass[keyno]
            != indexcmds_seams::get_default_opclass::call(keycoltype, source_idx.rd_rel.relam)?
        {
            unported("generateClonedIndexStmt: non-default operator classes");
        }

        let mut ordering = SortByDir::SORTBY_DEFAULT;
        let mut nulls_ordering = SortByNulls::SORTBY_NULLS_DEFAULT;
        if opt & INDOPTION_DESC != 0 {
            ordering = SortByDir::SORTBY_DESC;
            if opt & INDOPTION_NULLS_FIRST == 0 {
                nulls_ordering = SortByNulls::SORTBY_NULLS_LAST;
            }
        } else if opt & INDOPTION_NULLS_FIRST != 0 {
            nulls_ordering = SortByNulls::SORTBY_NULLS_FIRST;
        }

        let iparam = IndexElem {
            name: elem_name,
            expr: elem_expr,
            indexcolname: Some(str_in(
                mcx,
                core::str::from_utf8(source_idx.rd_att.attr(keyno).attname.name_str())
                    .expect("index column name"),
            )?),
            ordering,
            nulls_ordering,
            ..IndexElem::default()
        };
        params.lappend(mcx, Node::mk(mcx, iparam)?)?;
    }
    stmt.indexParams = params;

    if let Some(src) = idxrec.indpred_src.as_ref() {
        let pred = readfuncs::stringToNode(mcx, src.as_str())?;
        let (mapped, found_whole_row) =
            rewrite_manip::map_variable_attnos(mcx, pred, 1, 0, attmap)?;
        if found_whole_row {
            return Err(whole_row_error(format!(
                "Index \"{}\" contains a whole-row table reference.",
                source_idx.name()
            )));
        }
        stmt.whereClause = Some(mapped);
    }
    Ok((stmt, constraint_oid))
}

fn read_indclass<'mcx>(mcx: Mcx<'mcx>, index_id: Oid, nkeys: usize) -> PgResult<PgVec<'mcx, Oid>> {
    use datum::Datum;
    use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
    const INDEX_RELATION_ID: Oid = 2610;
    let mut key = ScanKeyData::empty();
    key.sk_attno = 1;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(index_id);
    let rel = table::table_open(mcx, INDEX_RELATION_ID, AccessShareLock)?;
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        IndexRelidIndexId,
        true,
        None,
        core::slice::from_ref(&key),
    )?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for index {index_id}"));
    let mut isnull = false;
    // SAFETY: NOT NULL plain-storage oidvector under pg_index's descriptor.
    let d = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_index_indclass, rel.descr(), &mut isnull)
    };
    debug_assert!(!isnull);
    // SAFETY: live oidvector image; dim1 bounds the value array.
    let vals = unsafe {
        let p = d.as_usize() as *const types_array::oidvector;
        core::slice::from_raw_parts(p.add(1) as *const Oid, (*p).dim1 as usize)
    };
    let mut out: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, nkeys)?;
    for &v in &vals[..nkeys] {
        out.push(v);
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(out)
}

fn has_extended_statistics<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<bool> {
    use datum::Datum;
    use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
    let mut key = ScanKeyData::empty();
    key.sk_attno = 2;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(relid);
    let rel = table::table_open(mcx, StatisticExtRelationId, AccessShareLock)?;
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        StatisticExtRelidIndexId,
        true,
        None,
        core::slice::from_ref(&key),
    )?;
    let found = genam::systable_getnext(mcx, &mut scan)?.is_some();
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(found)
}
