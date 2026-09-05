// LIKE arm: transformTableLikeClause + expandTableLikeClause +
// generateClonedIndexStmt + generateClonedExtStatsStmt. LOUD: compression
// copy, non-default opclass/collation.
use mcx::{Mcx, PgVec};
use types_core::{AttrNumber, InvalidOid, Oid, RELATION_RELATION_ID};
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

const RELKIND_RELATION: u8 = b'r';
const RELKIND_VIEW: u8 = b'v';
const RELKIND_MATVIEW: u8 = b'm';
const RELKIND_COMPOSITE_TYPE: u8 = b'c';
const RELKIND_FOREIGN_TABLE: u8 = b'f';
const RELKIND_PARTITIONED_TABLE: u8 = b'p';
const ACL_SELECT: u64 = 1 << 1;
const INDOPTION_DESC: i16 = 1 << 0;
const INDOPTION_NULLS_FIRST: i16 = 1 << 1;
const CONSTRAINT_RELATION_ID: Oid = 2606;
#[allow(non_upper_case_globals)] // C-parity name
const StatisticExtRelationId: Oid = 3381;
#[allow(non_upper_case_globals)] // C-parity name
const StatisticExtOidIndexId: Oid = 3380;
#[allow(non_upper_case_globals)] // C-parity name
const IndexRelidIndexId: Oid = 2679;
#[allow(non_upper_case_globals)] // C-parity name
const Anum_pg_index_indclass: i32 = 18;

const EXPAND_OPTIONS: u32 = CREATE_TABLE_LIKE_DEFAULTS
    | CREATE_TABLE_LIKE_GENERATED
    | CREATE_TABLE_LIKE_CONSTRAINTS
    | CREATE_TABLE_LIKE_INDEXES
    | CREATE_TABLE_LIKE_STATISTICS;

// The added-on commands (COMMENT, identity OWNED BY) go to the CreateStmtCxt's
// alist, interleaved with the other elements' in element order (C has the one
// cxt->alist; transformCreateStmt snapshots it after the element loop).
pub(crate) struct LikeCxt<'a, 'mcx> {
    pub relation: &'mcx RangeVar<'mcx>,
    pub columns: &'a mut NodeList<'mcx>,
    pub nnconstraints: &'a mut NodeList<'mcx>,
    pub likeclauses: &'a mut NodeList<'mcx>,
    pub is_foreign: bool,
}

// get_collation/get_opclass tail (parse_utilcmd.c:2192, 2225): list_make2 of
// the (always emitted) namespace name and the object name.
fn qualified_name_list<'mcx>(
    mcx: Mcx<'mcx>,
    nspoid: ::types_core::Oid,
    name: &::types_tuple::NameData,
) -> PgResult<NodeList<'mcx>> {
    // C's get_namespace_name returns NULL here without erroring; pgrust has no
    // NULL-name lane, so a vanished pg_namespace row raises elog's catchable
    // XX000 instead of aborting the backend.
    let Some(nsp) = lsyscache::get_namespace_name(mcx, nspoid)? else {
        return Err(crate::cache_lookup_failed("namespace", nspoid));
    };
    let name = core::str::from_utf8(name.name_str()).expect("name");
    let mut list = NodeList::make1(mcx, Node::mk_string(mcx, str_in(mcx, nsp.as_str())?)?)?;
    list.lappend(mcx, Node::mk_string(mcx, str_in(mcx, name)?)?)?;
    Ok(list)
}

pub(crate) fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let mut v: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, s.len())?;
    mcx::vec_append_bytes(&mut v, s.as_bytes())?;
    Ok(core::str::from_utf8(v.leak()).expect("was UTF-8"))
}

// sequence_options (sequence.c:1707): an existing sequence's parameters as
// CREATE SEQUENCE options; 64-bit values become Float per gram.y.
fn sequence_options<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<NodeList<'mcx>> {
    use types_nodes::parsenodes::{DefElem, DefElemAction};
    // sequence.c:1715 elog(ERROR, "cache lookup failed for sequence %u") --
    // catchable XX000, not a backend abort.
    let Some(form) = syscache_seams::lookup_pg_sequence_form::call(relid)? else {
        return Err(crate::cache_lookup_failed("sequence", relid));
    };
    let mut options = NodeList::nil();
    let int_opt = |v: i64| -> PgResult<Node<'mcx>> {
        Node::mk_float(mcx, str_in(mcx, &v.to_string())?)
    };
    let opts: [(&'static str, Node<'mcx>); 6] = [
        ("cache", int_opt(form.seqcache)?),
        ("cycle", Node::mk_boolean(mcx, form.seqcycle)?),
        ("increment", int_opt(form.seqincrement)?),
        ("maxvalue", int_opt(form.seqmax)?),
        ("minvalue", int_opt(form.seqmin)?),
        ("start", int_opt(form.seqstart)?),
    ];
    for (name, arg) in opts {
        let defel = DefElem {
            defnamespace: None,
            defname: Some(name),
            arg: Some(arg),
            defaction: DefElemAction::DEFELEM_UNSPEC,
            location: -1,
        };
        options.lappend(mcx, Node::mk(mcx, defel)?)?;
    }
    Ok(options)
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

// errdetail_relkind_not_supported (catalog/pg_class.c:31-58) over the relkinds
// transformTableLikeClause can refuse (parse_utilcmd.c:1141-1151): the
// accepted kinds (r/v/m/c/f/p) never reach it.
#[cold]
#[inline(never)]
fn errdetail_relkind_not_supported(relkind: u8) -> &'static str {
    match relkind {
        b'S' => "This operation is not supported for sequences.",
        b'i' => "This operation is not supported for indexes.",
        b'I' => "This operation is not supported for partitioned indexes.",
        b't' => "This operation is not supported for TOAST tables.",
        _ => "This operation is not supported for this kind of relation.",
    }
}

pub(crate) fn transformTableLikeClause<'mcx>(
    mcx: Mcx<'mcx>,
    cxt: &mut LikeCxt<'_, 'mcx>,
    create_cxt: &mut crate::CreateStmtCxt<'mcx>,
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
        RELKIND_RELATION | RELKIND_VIEW | RELKIND_MATVIEW | RELKIND_COMPOSITE_TYPE
        | RELKIND_FOREIGN_TABLE | RELKIND_PARTITIONED_TABLE => {}
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

    if relkind == RELKIND_COMPOSITE_TYPE {
        const ACL_USAGE: u64 = 1 << 8;
        let aclresult = aclchk::object_aclcheck(
            types_core::TYPE_RELATION_ID,
            relation.rd_rel.reltype,
            miscinit::GetUserId(),
            ACL_USAGE,
        )?;
        if aclresult != 0 {
            aclchk::aclcheck_error(aclresult, ObjectType::OBJECT_TYPE, relation.name())?;
        }
    } else {
        let aclresult = aclchk::pg_class_aclcheck(relid, miscinit::GetUserId(), ACL_SELECT)?;
        if aclresult != 0 {
            // get_relkind_objtype (objectaddress.c) for the reachable kinds.
            let objtype = match relkind {
                RELKIND_VIEW => ObjectType::OBJECT_VIEW,
                RELKIND_MATVIEW => ObjectType::OBJECT_MATVIEW,
                RELKIND_FOREIGN_TABLE => ObjectType::OBJECT_FOREIGN_TABLE,
                _ => ObjectType::OBJECT_TABLE,
            };
            aclchk::aclcheck_error(aclresult, objtype, relation.name())?;
        }
    }

    let tuple_desc = &relation.rd_att;
    for i in 0..tuple_desc.natts as usize {
        let attribute = tuple_desc.attr(i);
        if attribute.attisdropped {
            continue;
        }
        // C copies NameStr(attribute->attname) verbatim: pg_attribute names
        // are already NAMEDATALEN-truncated at creation, so no length check.
        let attname =
            str_in(mcx, core::str::from_utf8(attribute.attname.name_str()).expect("attname"))?;
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
            def.generated = attribute.attgenerated as u8;
        }
        // Identity/storage/compression never copy onto foreign tables
        // (C parse_utilcmd.c:1219,1239,1247 !cxt->isforeign).
        if (options & CREATE_TABLE_LIKE_STORAGE) != 0 && !cxt.is_foreign {
            def.storage = attribute.attstorage as u8;
        }
        if (options & CREATE_TABLE_LIKE_COMPRESSION) != 0
            && attribute.attcompression != 0
            && !cxt.is_foreign
        {
            def.compression = Some(compression_method_name(attribute.attcompression as u8)?);
        }
        let def_node = Node::mk(mcx, def)?;
        // Copy identity if requested (parse_utilcmd.c:1214-1235): recreate
        // the owned sequence from the source column's sequence parameters.
        if attribute.attidentity != 0
            && (options & CREATE_TABLE_LIKE_IDENTITY) != 0
            && !cxt.is_foreign
        {
            let seq_relid =
                pg_depend::getIdentitySequence(mcx, relid, attribute.attnum as i32, false)?;
            let seq_options = sequence_options(mcx, seq_relid)?;
            crate::generateSerialExtraStmts(
                mcx,
                cxt.relation,
                def_node,
                InvalidOid,
                seq_options,
                true,
                None,
                false,
                create_cxt,
                Some(query_string.as_bytes()),
            )?;
            // SAFETY: parse tree is analyze-owned; no derived refs live.
            unsafe {
                def_node
                    .with_mut::<ColumnDef, _>(|c| c.identity = attribute.attidentity as u8)
                    .expect("ColumnDef");
            }
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
                create_cxt.alist.lappend(mcx, stmt)?;
            }
        }
        cxt.columns.lappend(mcx, def_node)?;
    }

    let has_not_null =
        relation.rd_att.constr.as_deref().map(|c| c.has_not_null).unwrap_or(false);
    if has_not_null {
        let lst = pg_constraint::RelationGetNotNullConstraints(mcx, &relation, true)?;
        if (options & CREATE_TABLE_LIKE_COMMENTS) != 0 {
            for nnode in lst.iter() {
                let nn = nnode.as_variant::<Constraint>().expect("Constraint");
                let conname = nn.conname.expect("copied not-null conname");
                let con_oid = pg_constraint::get_relation_constraint_oid(mcx, relid, conname, false)?;
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
                    create_cxt.alist.lappend(mcx, stmt)?;
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
            // parse_utilcmd.c:1419-1422: an atthasdef column without its
            // pg_attrdef row is elog(ERROR) — catchable XX000, not a panic.
            let Some(defbin) = tupdesc::TupleDescGetDefaultBin(tuple_desc, (i + 1) as AttrNumber)
            else {
                return Err(Box::new(PgError::error(format!(
                    "default expression not found for attribute {} of relation \"{}\"",
                    i + 1,
                    relation.name()
                ))));
            };
            let this_default = readfuncs::stringToNode(mcx, defbin.as_str())?;
            let (mapped, found_whole_row) =
                rewrite_manip::map_variable_attnos(mcx, this_default, 1, 0, &attmap, types_core::InvalidOid)?;
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
                    rewrite_manip::map_variable_attnos(mcx, ccbin_node, 1, 0, &attmap, types_core::InvalidOid)?;
                if found_whole_row {
                    return Err(whole_row_error(format!(
                        "Constraint \"{}\" contains a whole-row reference to table \"{}\".",
                        ccname,
                        relation.name()
                    )));
                }
                // upstream 2780538433fc (18.5): Check for USAGE privilege on types used by stored expressions.
                // Copying a CHECK constraint adds new references. Since the
                // constraint arrives pre-cooked, it bypasses the checks in
                // AddRelationNewConstraints(), so check for USAGE on types
                // here (C re-reads ccbin; only the type references matter, so
                // the source tree serves).
                pg_depend::CheckUsageOnTypesInSingleRelExpr(
                    mcx,
                    ccbin_node,
                    relation.rd_id,
                    miscinit::GetUserId(),
                )?;
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
                        false,
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

    if (options & CREATE_TABLE_LIKE_STATISTICS) != 0 {
        let parent_extstats = relcache::statextlist::RelationGetStatExtList(mcx, relation.rd_id)?;
        for &parent_stat_oid in parent_extstats.iter() {
            let mut stats_stmt =
                generateClonedExtStatsStmt(mcx, heap_rel, childrel.rd_id, parent_stat_oid, &attmap)?;
            if (options & CREATE_TABLE_LIKE_COMMENTS) != 0 {
                if let Some(comment) =
                    commands_comment::GetComment(mcx, parent_stat_oid, StatisticExtRelationId, 0)?
                {
                    stats_stmt.stxcomment = Some(str_in(mcx, comment.as_str())?);
                }
            }
            result.lappend(mcx, Node::mk(mcx, stats_stmt)?)?;
        }
    }

    childrel.close(NoLock)?;
    relation.close(NoLock)?;
    Ok(result)
}

#[track_caller]
#[cold]
#[inline(never)]
fn whole_row_error(detail: String) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, "cannot convert whole-row table reference".to_string())
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail(detail),
    )
}

// GetCompressionMethodName (toast_compression.c:305-317): any other byte is a
// corrupted attcompression, elog(ERROR, "invalid compression method %c") —
// a catchable XX000, never a panic.
fn compression_method_name(c: u8) -> PgResult<&'static str> {
    match c {
        b'p' => Ok("pglz"),
        b'l' => Ok("lz4"),
        _ => Err(Box::new(PgError::error(format!(
            "invalid compression method {}",
            c as char
        )))),
    }
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

    // get_am_name via the AMOID syscache (C parse_utilcmd.c reads
    // amrec->amname), so extension AMs with dynamic oids (bloom, hnsw)
    // clone like the builtins.
    let amname: &'mcx str = {
        let relam = source_idx.rd_rel.relam;
        // parse_utilcmd.c:1744 elog(ERROR, "cache lookup failed for access
        // method %u") -- catchable XX000.
        let Some(name) = syscache_seams::pg_am_amname::call(relam)? else {
            return Err(crate::cache_lookup_failed("access method", relam));
        };
        str_in(mcx, &name)?
    };
    let table_space = if source_idx.rd_rel.reltablespace != InvalidOid {
        let spc = source_idx.rd_rel.reltablespace;
        // C's get_tablespace_name returns NULL silently (parse_utilcmd.c:1762);
        // pgrust has no NULL-tablespace lane, so raise the catchable XX000
        // rather than aborting.
        let Some(name) = tablespace_seams::get_tablespace_name::call(mcx, spc)? else {
            return Err(crate::cache_lookup_failed("tablespace", spc));
        };
        Some(str_in(
            mcx,
            std::str::from_utf8(name.name_str()).expect("tablespace name is utf8"),
        )?)
    } else {
        None
    };
    // Temporal (WITHOUT OVERLAPS) unique/PK indexes are indisexclusion.
    let iswithoutoverlaps =
        (idxrec.indisprimary || idxrec.indisunique) && idxrec.indisexclusion;

    let mut stmt = IndexStmt {
        relation: heap_rel,
        accessMethod: Some(amname),
        tableSpace: table_space,
        unique: idxrec.indisunique,
        nulls_not_distinct: idxrec.indnullsnotdistinct,
        primary: idxrec.indisprimary,
        iswithoutoverlaps,
        transformed: true,
        ..IndexStmt::default()
    };

    if stmt.primary || stmt.unique || idxrec.indisexclusion {
        let constraint_id = pg_depend::get_index_constraint(mcx, source_idx.rd_id)?;
        if constraint_id != InvalidOid {
            stmt.isconstraint = true;
            let (condeferrable, condeferred) =
                pg_constraint::get_constraint_deferrability(mcx, constraint_id)?;
            stmt.deferrable = condeferrable;
            stmt.initdeferred = condeferred;
            constraint_oid = constraint_id;

            // C rebuilds excludeOpNames from conexclop for every
            // indisexclusion index. DIVERGENCE (kept): WITHOUT OVERLAPS
            // clones stay NIL — DefineIndex re-derives the same operators
            // via GetOperatorFromCompareType.
            if idxrec.indisexclusion && !iswithoutoverlaps {
                let ops = relcache_build_seams::scan_exclusion_ops::call(
                    mcx,
                    indrelid,
                    source_idx.rd_id,
                    source_idx.name(),
                    idxrec.indnkeyatts,
                )?;
                let mut names = NodeList::nil();
                for &operid in ops.iter() {
                    // parse_utilcmd.c:1844 elog(ERROR, "cache lookup failed
                    // for operator %u") -- catchable XX000.
                    let Some((oprname, oprnamespace)) =
                        syscache_seams::pg_operator_oprnamensp::call(operid)?
                    else {
                        return Err(crate::cache_lookup_failed("operator", operid));
                    };
                    let namelist = qualified_name_list(mcx, oprnamespace, &oprname)?;
                    names.lappend(mcx, Node::mk_list(mcx, namelist)?)?;
                }
                stmt.excludeOpNames = names;
            }
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
                rewrite_manip::map_variable_attnos(mcx, indexkey, 1, 0, attmap, types_core::InvalidOid)?;
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
        // get_collation (parse_utilcmd.c:2173): NIL when default for the
        // datatype, else always schema-qualified.
        let collation = if indcollation != InvalidOid && indcollation != typcollation {
            // parse_utilcmd.c:2202 elog(ERROR, "cache lookup failed for
            // collation %u") -- catchable XX000.
            let Some(row) =
                syscache_seams::lookup_pg_collation_locale_row::call(mcx, indcollation)?
            else {
                return Err(crate::cache_lookup_failed("collation", indcollation));
            };
            qualified_name_list(mcx, row.collnamespace, &row.collname)?
        } else {
            NodeList::nil()
        };
        // get_opclass (parse_utilcmd.c:2207): NIL when default for the
        // datatype, else always schema-qualified.
        // parse_utilcmd.c:2229 elog(ERROR, "cache lookup failed for opclass
        // %u") -- catchable XX000.
        let Some((opcname, opcnamespace, opcmethod)) =
            syscache_seams::pg_opclass_name_namespace_method::call(indclass[keyno])?
        else {
            return Err(crate::cache_lookup_failed("opclass", indclass[keyno]));
        };
        let opclass = if indclass[keyno]
            != indexcmds_seams::get_default_opclass::call(keycoltype, opcmethod)?
        {
            qualified_name_list(mcx, opcnamespace, &opcname)?
        } else {
            NodeList::nil()
        };

        // C: per-column opclass options (untransformRelOptions of the index
        // column's attoptions).
        let opclassopts = untransform_rel_options(
            mcx,
            lsyscache::get_attoptions(mcx, source_idx.rd_id, keyno as i16 + 1)?,
        )?;

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
            collation,
            opclass,
            opclassopts,
            ordering,
            nulls_ordering,
            ..IndexElem::default()
        };
        params.lappend(mcx, Node::mk(mcx, iparam)?)?;
    }
    stmt.indexParams = params;

    // Included columns (parse_utilcmd.c:1966-1996).
    let mut including_params = NodeList::nil();
    for keyno in idxrec.indnkeyatts as usize..idxrec.indnatts as usize {
        let attnum = idxrec.indkey[keyno];
        if attnum == 0 {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    "expressions are not supported in included columns".to_string(),
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        let attname = lsyscache::get_attname(mcx, indrelid, attnum, false)?
            .expect("included index column");
        let iparam = IndexElem {
            name: Some(str_in(mcx, attname.as_str())?),
            indexcolname: Some(str_in(
                mcx,
                core::str::from_utf8(source_idx.rd_att.attr(keyno).attname.name_str())
                    .expect("index column name"),
            )?),
            ..IndexElem::default()
        };
        including_params.lappend(mcx, Node::mk(mcx, iparam)?)?;
    }
    stmt.indexIncludingParams = including_params;

    // C: copy reloptions if any (the source index's pg_class.reloptions,
    // untransformRelOptions'd back to WITH-clause DefElems).
    stmt.options = index_reloptions_defelems(mcx, source_idx.rd_id)?;

    if let Some(src) = idxrec.indpred_src.as_ref() {
        let pred = readfuncs::stringToNode(mcx, src.as_str())?;
        let (mapped, found_whole_row) =
            rewrite_manip::map_variable_attnos(mcx, pred, 1, 0, attmap, types_core::InvalidOid)?;
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

// untransformRelOptions (reloptions.c) over a text[] datum, as DefElems:
// each "name=value" element becomes a DefElem with a String arg; an element
// without '=' gets a NULL arg. A null datum is NIL.
fn untransform_rel_options<'mcx>(
    mcx: Mcx<'mcx>,
    d: datum::Datum,
) -> PgResult<NodeList<'mcx>> {
    use types_nodes::parsenodes::{DefElem, DefElemAction};

    let mut result = NodeList::nil();
    if d == datum::Datum::null() {
        return Ok(result);
    }
    // DatumGetArrayTypeP: a 4-byte-header image; tuple-stored catalog arrays
    // may come back short-headered or toasted/compressed.
    let img = varlena_image(d);
    let b0 = img[0];
    let image: &[u8] = if b0 == 0x01 || (b0 & 0x03) == 0x02 {
        detoast::detoast_attr(mcx, img)?.leak()
    } else if b0 & 0x01 != 0 {
        let payload = &img[1..];
        let mut v = mcx::vec_with_capacity_in(mcx, payload.len() + 4)?;
        mcx::vec_append_bytes(&mut v, &(((payload.len() + 4) as u32) << 2).to_ne_bytes())?;
        mcx::vec_append_bytes(&mut v, payload)?;
        v.leak()
    } else {
        img
    };
    // deconstruct_array_builtin(array, TEXTOID, &optiondatums, NULL, &noptions)
    // (reloptions.c:1365): any dimensionality; a NULL element is
    // deconstruct_array's 22004 (nullsp == NULL).
    let (elems, nulls) =
        datum::array_build::deconstruct_array_image_nulls(mcx, image, -1, false, b'i')?;
    for (i, &e) in elems.iter().enumerate() {
        if nulls.as_ref().is_some_and(|n| n[i]) {
            return Err(Box::new(
                PgError::error("null array element not allowed in this context")
                    .with_sqlstate(types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED),
            ));
        }
        let s = text_str(mcx, e)?;
        // C splits at the first '='; no '=' leaves a NULL-arg DefElem.
        let (name, value) = match s.split_once('=') {
            Some((n, v)) => (n, Some(v)),
            None => (s, None),
        };
        let arg = match value {
            Some(v) => Some(Node::mk_string(mcx, str_in(mcx, v)?)?),
            None => None,
        };
        let defel = DefElem {
            defnamespace: None,
            defname: Some(str_in(mcx, name)?),
            arg,
            defaction: DefElemAction::DEFELEM_UNSPEC,
            location: -1,
        };
        result.lappend(mcx, Node::mk(mcx, defel)?)?;
    }
    Ok(result)
}

// The source index's pg_class.reloptions, untransformed; C reads it from the
// RELOID syscache in generateClonedIndexStmt.
fn index_reloptions_defelems<'mcx>(mcx: Mcx<'mcx>, index_id: Oid) -> PgResult<NodeList<'mcx>> {
    use datum::Datum;
    use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
    #[allow(non_upper_case_globals)] // C-parity name
    const ClassOidIndexId: Oid = 2662;
    #[allow(non_upper_case_globals)] // C-parity name
    const Anum_pg_class_reloptions: i32 = 33;
    let mut key = ScanKeyData::empty();
    key.sk_attno = 1;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(index_id);
    let rel = table::table_open(mcx, RELATION_RELATION_ID, AccessShareLock)?;
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        ClassOidIndexId,
        true,
        None,
        core::slice::from_ref(&key),
    )?;
    // parse_utilcmd.c:1733 elog(ERROR, "cache lookup failed for relation %u")
    // -- catchable XX000; abort-time release reclaims the scan and the lock.
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(crate::cache_lookup_failed("relation", index_id));
    };
    let mut isnull = false;
    // SAFETY: nullable text[] under pg_class's descriptor; decoded (strings
    // copied into mcx) before the scan ends.
    let d = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_class_reloptions, rel.descr(), &mut isnull)
    };
    let options = if isnull { NodeList::nil() } else { untransform_rel_options(mcx, d)? };
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(options)
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
    // The INDEXRELID row C already holds; a miss is a catchable XX000.
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(crate::cache_lookup_failed("index", index_id));
    };
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

// Inline/detoasted varlena payload past the 4-byte header.
fn varlena_image(d: datum::Datum) -> &'static [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: varlena header declares the image length.
    unsafe {
        let b0 = *p;
        let len = if b0 == 0x01 {
            2 + types_tuple::varatt::vartag_size(*p.add(1))
        } else if b0 & 0x01 != 0 {
            ((b0 as usize) >> 1) & 0x7F
        } else {
            (u32::from_ne_bytes(*(p as *const [u8; 4])) >> 2) as usize
        };
        core::slice::from_raw_parts(p, len)
    }
}

fn text_str<'mcx>(mcx: Mcx<'mcx>, d: datum::Datum) -> PgResult<&'mcx str> {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null text datum into a live catalog tuple.
    let b0 = unsafe { *p };
    let src: &[u8] = if b0 == 0x01 || (b0 & 0x03) == 0x02 {
        &detoast::detoast_attr(mcx, varlena_image(d))?.leak()[4..]
    } else if b0 & 0x01 != 0 {
        let len = ((b0 as usize) >> 1) & 0x7F;
        // SAFETY: short varlena header declares len bytes including itself.
        unsafe { core::slice::from_raw_parts(p.add(1), len - 1) }
    } else {
        &varlena_image(d)[4..]
    };
    let mut copied: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, src.len())?;
    mcx::vec_append_bytes(&mut copied, src)?;
    Ok(core::str::from_utf8(copied.leak()).expect("stxexprs is UTF-8"))
}

// 1-D no-null array payload: 20-byte array header, then elements. Expands
// short/compressed/external images first (C DatumGetArrayTypeP).
fn array_elems<'mcx>(
    mcx: Mcx<'mcx>,
    d: datum::Datum,
    elemtype: Oid,
    what: &str,
) -> PgResult<(usize, &'mcx [u8])> {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena datum into a live catalog tuple.
    let b0 = unsafe { *p };
    let body: &'mcx [u8] = if b0 == 0x01 || (b0 & 0x03) == 0x02 {
        &detoast::detoast_attr(mcx, varlena_image(d))?.leak()[4..]
    } else {
        let src: &[u8] = if b0 & 0x01 != 0 {
            let len = ((b0 as usize) >> 1) & 0x7F;
            // SAFETY: short varlena header declares len bytes incl. itself.
            unsafe { core::slice::from_raw_parts(p.add(1), len - 1) }
        } else {
            &varlena_image(d)[4..]
        };
        let mut copied: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, src.len())?;
        mcx::vec_append_bytes(&mut copied, src)?;
        copied.leak()
    };
    let read = |off: usize| i32::from_ne_bytes(body[off..off + 4].try_into().unwrap());
    if read(0) != 1 || read(4) != 0 || read(8) != elemtype as i32 {
        panic!("{what} has unexpected array shape");
    }
    Ok((read(12) as usize, &body[20..]))
}

// generateClonedExtStatsStmt (parse_utilcmd.c:2046): clone one extended
// statistics object of the LIKE source onto the new table.
fn generateClonedExtStatsStmt<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &'mcx RangeVar<'mcx>,
    heap_relid: Oid,
    source_statsid: Oid,
    attmap: &[AttrNumber],
) -> PgResult<types_nodes::rawnodes::CreateStatsStmt<'mcx>> {
    use datum::Datum;
    use types_nodes::rawnodes::{CreateStatsStmt, StatsElem};
    use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
    #[allow(non_upper_case_globals)] // C-parity name
    const Anum_pg_statistic_ext_stxkeys: i32 = 6;
    #[allow(non_upper_case_globals)] // C-parity name
    const Anum_pg_statistic_ext_stxkind: i32 = 8;
    #[allow(non_upper_case_globals)] // C-parity name
    const Anum_pg_statistic_ext_stxexprs: i32 = 9;
    const CHAROID: Oid = 18;
    const INT2OID: Oid = 21;

    let mut key = ScanKeyData::empty();
    key.sk_attno = 1;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(source_statsid);
    let rel = table::table_open(mcx, StatisticExtRelationId, AccessShareLock)?;
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        StatisticExtOidIndexId,
        true,
        None,
        core::slice::from_ref(&key),
    )?;
    // parse_utilcmd.c:2081 elog(ERROR, "cache lookup failed for statistics
    // object %u") -- catchable XX000.
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(crate::cache_lookup_failed("statistics object", source_statsid));
    };
    let desc = rel.descr();

    let mut isnull = false;
    // SAFETY: NOT NULL stxkind under pg_statistic_ext's descriptor.
    let kind_d =
        unsafe { types_tuple::heap_getattr(tup, Anum_pg_statistic_ext_stxkind, desc, &mut isnull) };
    debug_assert!(!isnull);
    let (nkinds, kinddata) = array_elems(mcx, kind_d, CHAROID, "stxkind")?;
    let mut stat_types = NodeList::nil();
    for &kind in &kinddata[..nkinds] {
        let name = match kind {
            b'd' => "ndistinct",
            b'f' => "dependencies",
            b'm' => "mcv",
            // Expression stats are not exposed to users.
            b'e' => continue,
            // parse_utilcmd.c:2105: elog(ERROR) — catchable XX000.
            other => {
                return Err(Box::new(PgError::error(format!(
                    "unrecognized statistics kind {}",
                    other as char
                ))))
            }
        };
        stat_types.lappend(mcx, Node::mk_string(mcx, name)?)?;
    }

    // SAFETY: NOT NULL int2vector stxkeys under pg_statistic_ext's descriptor.
    let keys_d =
        unsafe { types_tuple::heap_getattr(tup, Anum_pg_statistic_ext_stxkeys, desc, &mut isnull) };
    debug_assert!(!isnull);
    let (nkeys, keydata) = array_elems(mcx, keys_d, INT2OID, "stxkeys")?;
    let mut def_names = NodeList::nil();
    for i in 0..nkeys {
        let attnum = i16::from_ne_bytes(keydata[i * 2..i * 2 + 2].try_into().unwrap());
        // upstream 149c875fc20b (18.4): Fix attnum remapping in generateClonedExtStatsStmt()
        // stxkeys hold the PARENT's attnums; remap through attmap before the
        // child lookup (a dropped parent column renumbers the child), as the
        // expression path below already does.
        let attname =
            lsyscache::get_attname(mcx, heap_relid, attmap[attnum as usize - 1], false)?
                .expect("statistics key column");
        let selem = StatsElem { name: Some(str_in(mcx, attname.as_str())?), expr: None };
        def_names.lappend(mcx, Node::mk(mcx, selem)?)?;
    }

    // Expressions append after simple column references; the relative order
    // is irrelevant for the CREATE command (C comment at 2108-2116).
    // SAFETY: nullable text stxexprs under pg_statistic_ext's descriptor.
    let exprs_d = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_statistic_ext_stxexprs, desc, &mut isnull)
    };
    if !isnull {
        let exprs_str = text_str(mcx, exprs_d)?;
        let exprs =
            readfuncs::stringToNode(mcx, exprs_str)?.as_list().expect("stxexprs is a List");
        for expr in exprs.iter() {
            // C ignores found_whole_row here.
            let (mapped, _) = rewrite_manip::map_variable_attnos(
                mcx,
                expr,
                1,
                0,
                attmap,
                types_core::InvalidOid,
            )?;
            let selem = StatsElem { name: None, expr: Some(mapped) };
            def_names.lappend(mcx, Node::mk(mcx, selem)?)?;
        }
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;

    let heap_rv = Node::mk(
        mcx,
        RangeVar {
            catalogname: heap_rel.catalogname,
            schemaname: heap_rel.schemaname,
            relname: heap_rel.relname,
            inh: heap_rel.inh,
            relpersistence: heap_rel.relpersistence,
            alias: heap_rel.alias,
            location: heap_rel.location,
        },
    )?;
    Ok(CreateStatsStmt {
        defnames: NodeList::nil(),
        stat_types,
        exprs: def_names,
        relations: NodeList::make1(mcx, heap_rv)?,
        stxcomment: None,
        transformed: true,
        if_not_exists: false,
        // upstream a1fa24127d6a (18.6): Preserve the owner of extended statistics rebuilt by ALTER TABLE.
        owner: InvalidOid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_nodes::parsenodes::DefElem;

    fn ctx() -> &'static mcx::MemoryContext {
        Box::leak(Box::new(mcx::MemoryContext::new("like-test")))
    }

    // A 4B-headered 1-D text[] image, elements 4B-headered and INT-aligned,
    // as deconstruct_array_builtin expects stored reloptions to look.
    fn text_array_image(elems: &[&str]) -> Vec<u8> {
        text_array_image_shaped(&[elems.len() as i32], &elems.iter().map(|e| Some(*e)).collect::<Vec<_>>())
    }

    // The same image with explicit dims (any dimensionality) and, when an
    // element is None, a null bitmap (dataoffset = MAXALIGN(header + bitmap)).
    fn text_array_image_shaped(dims: &[i32], elems: &[Option<&str>]) -> Vec<u8> {
        let has_nulls = elems.iter().any(|e| e.is_none());
        let mut img: Vec<u8> = Vec::new();
        img.extend_from_slice(&0u32.to_ne_bytes()); // vl_len, patched below
        img.extend_from_slice(&(dims.len() as i32).to_ne_bytes()); // ndim
        img.extend_from_slice(&0i32.to_ne_bytes()); // dataoffset, patched below
        img.extend_from_slice(&25i32.to_ne_bytes()); // elemtype = TEXTOID
        for d in dims {
            img.extend_from_slice(&d.to_ne_bytes());
        }
        for _ in dims {
            img.extend_from_slice(&1i32.to_ne_bytes()); // lbound
        }
        if has_nulls {
            let hdr = img.len();
            img.resize(hdr + (elems.len() + 7) / 8, 0);
            for (i, e) in elems.iter().enumerate() {
                if e.is_some() {
                    img[hdr + i / 8] |= 1 << (i % 8);
                }
            }
            while img.len() % 8 != 0 {
                img.push(0);
            }
            let dataoffset = img.len() as i32;
            img[8..12].copy_from_slice(&dataoffset.to_ne_bytes());
        }
        for e in elems.iter().flatten() {
            while img.len() % 4 != 0 {
                img.push(0);
            }
            img.extend_from_slice(&(((e.len() + 4) as u32) << 2).to_ne_bytes());
            img.extend_from_slice(e.as_bytes());
        }
        let len = img.len() as u32;
        img[..4].copy_from_slice(&(len << 2).to_ne_bytes());
        img
    }

    // reloptions.c:1365 deconstruct_array_builtin(array, TEXTOID, ..., NULL,
    // ...): a 2-D catalog text[] is walked linearly; a NULL element is
    // deconstruct_array's 22004 — neither shape may panic the backend.
    #[test]
    fn untransform_rel_options_accepts_multidim_and_refuses_nulls() {
        let mcx = ctx().mcx();
        let img = text_array_image_shaped(&[2, 2], &[Some("fillfactor=70"), Some("a=1"), Some("b"), Some("c=d")]);
        let list =
            untransform_rel_options(mcx, datum::Datum::from_usize(img.as_ptr() as usize))
                .unwrap();
        let names: Vec<&str> = list
            .iter()
            .map(|n| n.as_variant::<DefElem>().expect("DefElem").defname.unwrap())
            .collect();
        assert_eq!(names, ["fillfactor", "a", "b", "c"]);

        let img = text_array_image_shaped(&[2], &[Some("fillfactor=70"), None]);
        let err = untransform_rel_options(mcx, datum::Datum::from_usize(img.as_ptr() as usize))
            .unwrap_err();
        assert_eq!(err.message(), "null array element not allowed in this context");
        assert_eq!(err.sqlstate(), types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED);
    }

    #[test]
    fn untransform_rel_options_matches_c() {
        let mcx = ctx().mcx();
        // C untransformRelOptions: "name=value" splits at the first '=',
        // a '='-less element becomes a NULL-arg DefElem.
        let img = text_array_image(&["fillfactor=70", "no_value_opt", "eq=a=b"]);
        let list =
            untransform_rel_options(mcx, datum::Datum::from_usize(img.as_ptr() as usize))
                .unwrap();
        let opts: Vec<(&str, Option<&str>)> = list
            .iter()
            .map(|n| {
                let d = n.as_variant::<DefElem>().expect("DefElem");
                (d.defname.unwrap(), d.arg.map(|a| a.as_string().expect("String arg").sval))
            })
            .collect();
        assert_eq!(
            opts,
            [
                ("fillfactor", Some("70")),
                ("no_value_opt", None),
                ("eq", Some("a=b")),
            ]
        );
        // A null datum (no reloptions) is NIL.
        assert!(untransform_rel_options(mcx, datum::Datum::null()).unwrap().is_nil());
    }
}
