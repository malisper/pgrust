//! LIKE / OF-type helpers (`parse_utilcmd.c`).
//!
//! [`transformOfType`] is GROUNDED: it resolves the composite type via
//! `typenameType`, checks it with `check_of_type`, reads its rowtype `TupleDesc`
//! through the typcache (`lookup_rowtype_tupdesc`), and rebuilds the inherited
//! `ColumnDef`s in-crate. [`transformTableLikeClause`] is likewise grounded: it
//! opens the source relation by name, copies each non-dropped column into a new
//! `ColumnDef` (marking GENERATED / IDENTITY / STORAGE / COMPRESSION per the
//! INCLUDING options), reproduces the NOT NULL constraints, queues column /
//! constraint comments, and remembers the relation OID so `expandTableLikeClause`
//! (run after `DefineRelation`) can finish the column-number-dependent legs
//! (defaults, CHECK constraints, indexes, statistics).

use alloc::format;
use alloc::string::{String, ToString};

use ::mcx::{Mcx, PgString, PgVec};

use ::utils_error::ereport;
use ::types_core::Oid;
use ::types_error::{PgResult, ERRCODE_WRONG_OBJECT_TYPE, ERROR};
use ::types_storage::lock::{AccessShareLock, NoLock};

use ::types_acl::{ACLCHECK_OK, ACL_SELECT, ACL_USAGE};
use ::nodes::ddlnodes::CommentStmt;
use ::nodes::nodes::Node;
use ::nodes::parsenodes::{OBJECT_COLUMN, OBJECT_TABCONSTRAINT, OBJECT_TYPE};
use ::nodes::rawnodes::{ColumnDef, TypeName};
use ::types_tuple::pg_type::FormData_pg_type;

use ::common_relation::{relation_open, relation_openrv};
use ::toast_compression::get_compression_method_name;
use ::table::table_close;
use ::aclchk::{aclcheck_error, object_aclcheck, pg_class_aclcheck};
use ::objectaddress::resolve::get_relkind_objtype;
use ::pg_class::errdetail_relkind_not_supported;
use ::pg_constraint::{
    get_relation_constraint_oid, NotNullConstraint, RelationGetNotNullConstraints,
};
use ::pg_depend::getIdentitySequence;
use ::comment::GetComment;
use ::commands_sequence::sequence_options;
use ::parse_type::{raw_typename_to_parse, typenameType};
use ::small1::{
    cancel_parser_errposition_callback, parser_errposition, setup_parser_errposition_callback,
};
use ::adt_format_type::format_type_be;
use ::cache_typcache::lookup_rowtype_tupdesc;
use ::miscinit::GetUserId;

use crate::core::{make_string, CreateStmtContext, NodePtr};
use crate::serial::generateSerialExtraStmts;

/// `TYPTYPE_COMPOSITE` (`pg_type.h`).
const TYPTYPE_COMPOSITE: i8 = b'c' as i8;
/// `RELKIND_RELATION` (`pg_class.h`).
const RELKIND_RELATION: u8 = b'r';
/// `RELKIND_VIEW` (`pg_class.h`).
const RELKIND_VIEW: u8 = b'v';
/// `RELKIND_MATVIEW` (`pg_class.h`).
const RELKIND_MATVIEW: u8 = b'm';
/// `RELKIND_COMPOSITE_TYPE` (`pg_class.h`).
const RELKIND_COMPOSITE_TYPE: u8 = b'c';
/// `RELKIND_FOREIGN_TABLE` (`pg_class.h`).
const RELKIND_FOREIGN_TABLE: u8 = b'f';
/// `RELKIND_PARTITIONED_TABLE` (`pg_class.h`).
const RELKIND_PARTITIONED_TABLE: u8 = b'p';

/// `TypeRelationId` (`pg_type.h`, OID 1247).
const TYPE_RELATION_ID: Oid = 1247;
/// `RelationRelationId` (`pg_class.h`, OID 1259).
const RELATION_RELATION_ID: Oid = 1259;
/// `ConstraintRelationId` (`pg_constraint.h`, OID 2606).
const CONSTRAINT_RELATION_ID: Oid = 2606;

/// `InvalidCompressionMethod` (`'\0'`) — `CompressionMethodIsValid` is
/// `m != InvalidCompressionMethod`.
const INVALID_COMPRESSION_METHOD: i8 = 0;

// TableLikeOption bits (`nodes/parsenodes.h`) — OR-folded `bits32 options`.
const CREATE_TABLE_LIKE_COMMENTS: u32 = 1 << 0;
const CREATE_TABLE_LIKE_COMPRESSION: u32 = 1 << 1;
const CREATE_TABLE_LIKE_CONSTRAINTS: u32 = 1 << 2;
const CREATE_TABLE_LIKE_DEFAULTS: u32 = 1 << 3;
const CREATE_TABLE_LIKE_GENERATED: u32 = 1 << 4;
const CREATE_TABLE_LIKE_IDENTITY: u32 = 1 << 5;
const CREATE_TABLE_LIKE_INDEXES: u32 = 1 << 6;
const CREATE_TABLE_LIKE_STATISTICS: u32 = 1 << 7;
const CREATE_TABLE_LIKE_STORAGE: u32 = 1 << 8;

/// Bridge a node `rawnodes::RangeVar` to the value-typed
/// `::types_tuple::access::RangeVar` that `relation_openrv` consumes.
pub(crate) fn access_range_var(
    rv: &::nodes::rawnodes::RangeVar<'_>,
) -> ::types_tuple::access::RangeVar {
    ::types_tuple::access::RangeVar {
        catalogname: rv.catalogname.as_ref().map(|s| s.as_str().into()),
        schemaname: rv.schemaname.as_ref().map(|s| s.as_str().into()),
        relname: rv
            .relname
            .as_ref()
            .map_or_else(alloc::string::String::new, |s| s.as_str().into()),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
    }
}

/// `transformTableLikeClause` (parse_utilcmd.c) — change the `LIKE <srctable>`
/// portion of a CREATE TABLE statement into column definitions that recreate the
/// user-defined column portions of `<srctable>`. Options that can't be fully
/// processed at parse time (DEFAULTS / GENERATED / CONSTRAINTS / INDEXES /
/// STATISTICS) cause the `TableLikeClause` to be appended to `cxt->likeclauses`,
/// so `expandTableLikeClause` runs after the new table is created.
pub fn transformTableLikeClause<'mcx>(
    cxt: &mut CreateStmtContext<'mcx>,
    table_like_clause: NodePtr<'mcx>,
) -> PgResult<()> {
    let mcx = cxt.mcx;

    let Some(tlc) = table_like_clause.as_ref().as_tablelikeclause() else {
        unreachable!(
            "transformTableLikeClause: not a TableLikeClause node: {}",
            table_like_clause.node_tag()
        );
    };
    let options = tlc.options;

    // The LIKE source `RangeVar` node.
    let src_rv_node = tlc
        .relation
        .as_deref()
        .ok_or_else(|| {
            ::types_error::PgError::error("transformTableLikeClause: NULL LIKE relation")
        })?;
    let src_rv = src_rv_node
        .as_rangevar()
        .ok_or_else(|| {
            ::types_error::PgError::error("transformTableLikeClause: LIKE relation not a RangeVar")
        })?;
    let src_location = src_rv.location;
    let access_rv = access_range_var(src_rv);

    // setup_parser_errposition_callback(&pcbstate, cxt->pstate, location).
    // The ambient error-context callback chain is retired (docs/query-lifecycle-raii.md);
    // the location is attached at the propagation site instead, exactly as
    // pcb_error_callback does: tag the error with parser_errposition(pstate, location)
    // as the cursor position, but only when the error has none of its own
    // (C: `if (edata->cursorpos == 0)`).
    setup_parser_errposition_callback(&cxt.pstate, src_location);
    let attach_errpos = |mut e: ::types_error::PgError| -> ::types_error::PgError {
        if e.cursor_position().is_none() {
            let pos = parser_errposition(&cxt.pstate, src_location);
            if pos > 0 {
                e = e.with_cursor_position(pos);
            }
        }
        e
    };

    // Open the relation referenced by the LIKE clause.
    let relation = relation_openrv(mcx, &access_rv, AccessShareLock).map_err(attach_errpos)?;

    let relkind = relation.rd_rel.relkind;
    if relkind != RELKIND_RELATION
        && relkind != RELKIND_VIEW
        && relkind != RELKIND_MATVIEW
        && relkind != RELKIND_COMPOSITE_TYPE
        && relkind != RELKIND_FOREIGN_TABLE
        && relkind != RELKIND_PARTITIONED_TABLE
    {
        let name = relation.name().to_string();
        return Err(attach_errpos(
            ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!("relation \"{name}\" is invalid in LIKE clause"))
                .errdetail(errdetail_relkind_not_supported(relkind)?)
                .into_error(),
        ));
    }

    cancel_parser_errposition_callback();

    // Check for privileges.
    if relkind == RELKIND_COMPOSITE_TYPE {
        let aclresult =
            object_aclcheck(mcx, TYPE_RELATION_ID, relation.rd_rel.reltype, GetUserId(), ACL_USAGE)?;
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, OBJECT_TYPE, Some(relation.name().to_string()))?;
        }
    } else {
        let aclresult = pg_class_aclcheck(mcx, relation.rd_id, GetUserId(), ACL_SELECT)?;
        if aclresult != ACLCHECK_OK {
            aclcheck_error(
                aclresult,
                get_relkind_objtype(relkind),
                Some(relation.name().to_string()),
            )?;
        }
    }

    let relid = relation.rd_id;
    let tuple_desc = &relation.rd_att;
    let natts = tuple_desc.natts as usize;

    // Insert the copied attributes into cxt now, so they keep the relative
    // position where the LIKE clause is (SQL99).
    for parent_attno in 0..natts {
        let attribute = tuple_desc.attr(parent_attno);

        // Ignore dropped columns in the parent.
        if attribute.attisdropped {
            continue;
        }

        let attname = core::str::from_utf8(attribute.attname.name_str()).unwrap_or("");
        let mut def = make_column_def(
            mcx,
            attname,
            attribute.atttypid,
            attribute.atttypmod,
            attribute.attcollation,
        )?;

        // Although the default/generation expression is not transferred now,
        // mark the column GENERATED if appropriate.
        if attribute.atthasdef
            && attribute.attgenerated != 0
            && (options & CREATE_TABLE_LIKE_GENERATED) != 0
        {
            def.generated = attribute.attgenerated;
        }

        // Copy identity if requested.
        if attribute.attidentity != 0
            && (options & CREATE_TABLE_LIKE_IDENTITY) != 0
            && !cxt.isforeign
        {
            // find sequence owned by old column; extract its parameters; build a
            // new CREATE SEQUENCE command.
            let seq_relid = getIdentitySequence(mcx, &relation, attribute.attnum, false)?;
            let seq_options = sequence_options(mcx, seq_relid)?;
            generateSerialExtraStmts(
                cxt,
                &mut def,
                ::types_core::primitive::InvalidOid,
                seq_options,
                true,
                false,
            )?;
            def.identity = attribute.attidentity;
        }

        // Copy storage if requested.
        if (options & CREATE_TABLE_LIKE_STORAGE) != 0 && !cxt.isforeign {
            def.storage = attribute.attstorage;
        } else {
            def.storage = 0;
        }

        // Copy compression if requested.
        if (options & CREATE_TABLE_LIKE_COMPRESSION) != 0
            && attribute.attcompression != INVALID_COMPRESSION_METHOD
            && !cxt.isforeign
        {
            def.compression = Some(PgString::from_str_in(
                get_compression_method_name(attribute.attcompression as u8)?,
                mcx,
            )?);
        } else {
            def.compression = None;
        }

        // Copy comment if requested.
        if (options & CREATE_TABLE_LIKE_COMMENTS) != 0 {
            if let Some(comment) = GetComment(mcx, attribute.attrelid, RELATION_RELATION_ID, attribute.attnum as i32)? {
                let colname = def.colname.as_ref().map_or("", PgString::as_str).to_string();
                let stmt = make_column_comment_stmt(cxt, &colname, &comment)?;
                cxt.alist.push(stmt);
            }
        }

        cxt.columns
            .push(::mcx::alloc_in(mcx, Node::mk_column_def(mcx, def)?)?);
    }

    // Reproduce not-null constraints, if any, regardless of options.
    let has_not_null = relation
        .rd_att
        .constr
        .as_ref()
        .map(|c| c.has_not_null)
        .unwrap_or(false);
    if has_not_null {
        // RelationGetNotNullConstraints(relid, false, true) — raw Constraint nodes.
        let lst = RelationGetNotNullConstraints(mcx, relid, false, true)?;

        // Copy comments on not-null constraints if requested, then fold the
        // constraints into cxt->nnconstraints (consuming the list).
        let copy_comments = (options & CREATE_TABLE_LIKE_COMMENTS) != 0;
        for nn in lst {
            let NotNullConstraint::Raw(constr) = nn else {
                unreachable!("RelationGetNotNullConstraints(cooked=false) returned Cooked");
            };
            let conname = constr.conname.as_ref().map(|s| s.as_str().to_string());

            if copy_comments {
                if let Some(ref cn) = conname {
                    let con_oid = get_relation_constraint_oid(mcx, relid, cn, false)?;
                    if let Some(comment) =
                        GetComment(mcx, con_oid, CONSTRAINT_RELATION_ID, 0)?
                    {
                        let stmt = make_tabconstraint_comment_stmt(cxt, cn, &comment)?;
                        cxt.alist.push(stmt);
                    }
                }
            }

            cxt.nnconstraints
                .push(::mcx::alloc_in(mcx, Node::mk_constraint(mcx, constr)?)?);
        }
    }

    // We cannot yet deal with defaults, CHECK constraints, indexes, or
    // statistics (we don't know the final column numbers). If any of those
    // options are specified, remember the relation OID and add the LIKE clause to
    // cxt->likeclauses so expandTableLikeClause runs after DefineRelation.
    if options
        & (CREATE_TABLE_LIKE_DEFAULTS
            | CREATE_TABLE_LIKE_GENERATED
            | CREATE_TABLE_LIKE_CONSTRAINTS
            | CREATE_TABLE_LIKE_INDEXES
            | CREATE_TABLE_LIKE_STATISTICS)
        != 0
    {
        let mut tlc_node = ::mcx::alloc_in(mcx, table_like_clause.clone_in(mcx)?)?;
        if let Some(t) = tlc_node.as_tablelikeclause_mut() {
            t.relationOid = relid;
        }
        cxt.likeclauses.push(tlc_node);
    }

    // Close the parent rel, keeping the AccessShareLock until xact commit so
    // nobody deletes/ALTERs it before expandTableLikeClause runs.
    table_close(relation, NoLock)?;

    Ok(())
}

/// Build a `CommentStmt` on a column: `OBJECT_COLUMN`, object =
/// `list_make3(schemaname, relname, colname)` of String nodes.
fn make_column_comment_stmt<'mcx>(
    cxt: &CreateStmtContext<'mcx>,
    colname: &str,
    comment: &str,
) -> PgResult<NodePtr<'mcx>> {
    let mcx = cxt.mcx;
    let (schemaname, relname) = like_target_names(cxt);
    let mut object: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    object.push(make_string(mcx, &schemaname)?);
    object.push(make_string(mcx, &relname)?);
    object.push(make_string(mcx, colname)?);
    let stmt = CommentStmt {
        objtype: OBJECT_COLUMN,
        object: Some(::mcx::alloc_in(mcx, Node::mk_list(mcx, object)?)?),
        comment: Some(PgString::from_str_in(comment, mcx)?),
    };
    ::mcx::alloc_in(mcx, Node::mk_comment_stmt(mcx, stmt)?)
}

/// Build a `CommentStmt` on a table constraint: `OBJECT_TABCONSTRAINT`, object =
/// `list_make3(schemaname, relname, conname)`.
fn make_tabconstraint_comment_stmt<'mcx>(
    cxt: &CreateStmtContext<'mcx>,
    conname: &str,
    comment: &str,
) -> PgResult<NodePtr<'mcx>> {
    let mcx = cxt.mcx;
    let (schemaname, relname) = like_target_names(cxt);
    let mut object: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    object.push(make_string(mcx, &schemaname)?);
    object.push(make_string(mcx, &relname)?);
    object.push(make_string(mcx, conname)?);
    let stmt = CommentStmt {
        objtype: OBJECT_TABCONSTRAINT,
        object: Some(::mcx::alloc_in(mcx, Node::mk_list(mcx, object)?)?),
        comment: Some(PgString::from_str_in(comment, mcx)?),
    };
    ::mcx::alloc_in(mcx, Node::mk_comment_stmt(mcx, stmt)?)
}

/// The new table's `(schemaname, relname)` (`cxt->relation`), mirroring the C
/// `makeString(cxt->relation->schemaname)` / `->relname`.
fn like_target_names(cxt: &CreateStmtContext<'_>) -> (alloc::string::String, alloc::string::String) {
    match cxt.relation.as_deref().and_then(|n| n.as_rangevar()) {
        Some(rv) => (
            rv.schemaname.as_ref().map_or_else(String::new, |s| s.as_str().to_string()),
            rv.relname.as_ref().map_or_else(String::new, |s| s.as_str().to_string()),
        ),
        None => (String::new(), String::new()),
    }
}

/// `transformOfType` — expand an `OF typename` clause into inherited column
/// definitions, reading the composite type's rowtype `TupleDesc` through the
/// typcache.
pub fn transformOfType<'mcx>(
    cxt: &mut CreateStmtContext<'mcx>,
    of_typename: NodePtr<'mcx>,
) -> PgResult<()> {
    let mcx = cxt.mcx;

    let Some(of_tn) = of_typename.as_ref().as_typename() else {
        unreachable!("transformOfType: not a TypeName node: {}", of_typename.node_tag());
    };

    // `typenameType` resolves the value-typed `parsenodes::TypeName`;
    // bridge the owned-tree `rawnodes::TypeName` across.
    let of_tn_parse = raw_typename_to_parse(of_tn)?;
    let (typ, _typmod) = typenameType(mcx, Some(&cxt.pstate), &of_tn_parse)?;
    check_of_type(mcx, &typ)?;
    let of_type_id = typ.oid;

    // Cache the resolved type OID on the (caller-owned) node for later. The C
    // mutates `ofTypename->typeOid`; mirror it on the node we hold.
    let mut of_typename = of_typename;
    if let Some(tn) = of_typename.as_typename_mut() {
        tn.typeOid = of_type_id;
    }

    let tupdesc = lookup_rowtype_tupdesc(mcx, of_type_id, -1)?;
    let natts = tupdesc.natts as usize;
    for i in 0..natts {
        let attr = tupdesc.attr(i);
        if attr.attisdropped {
            continue;
        }
        let attname = core::str::from_utf8(attr.attname.name_str()).unwrap_or("");
        let mut n =
            make_column_def(mcx, attname, attr.atttypid, attr.atttypmod, attr.attcollation)?;
        n.is_from_type = true;
        cxt.columns.push(::mcx::alloc_in(mcx, Node::mk_column_def(mcx, n)?)?);
    }
    // `ReleaseTupleDesc(tupdesc)` — the owned copy drops here.
    drop(tupdesc);

    Ok(())
}

/// `check_of_type(typetuple)` (`commands/tablecmds.c`): verify that the type a
/// table is `OF` is a stand-alone composite type. Hosted here (no `tablecmds`
/// owner reachable without a cycle); the body is self-contained
/// (`relation_open` + relkind check + `format_type_be` for the error).
fn check_of_type<'mcx>(mcx: Mcx<'mcx>, typ: &FormData_pg_type) -> PgResult<()> {
    if typ.typtype == TYPTYPE_COMPOSITE {
        debug_assert!(typ.typrelid != 0);
        let type_relation = relation_open(mcx, typ.typrelid, AccessShareLock)?;
        let type_ok = type_relation.rd_rel.relkind == RELKIND_COMPOSITE_TYPE;

        // Close the parent rel, but keep the AccessShareLock until xact commit.
        table_close(type_relation, NoLock)?;

        if !type_ok {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(alloc::format!(
                    "type {} is the row type of another table",
                    format_type_be(mcx, typ.oid)?.as_str()
                ))
                .errdetail(
                    "A typed table must use a stand-alone composite type created with CREATE TYPE.",
                )
                .into_error());
        }
        Ok(())
    } else {
        Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(alloc::format!(
                "type {} is not a composite type",
                format_type_be(mcx, typ.oid)?.as_str()
            ))
            .into_error())
    }
}

/// `makeColumnDef(colname, typeOid, typmod, collOid)` (`nodes/makefuncs.c`):
/// build a `ColumnDef` from a resolved type. Hosted here (no `makefuncs` owner
/// crate exists; trivial node constructor).
fn make_column_def<'mcx>(
    mcx: Mcx<'mcx>,
    colname: &str,
    type_oid: Oid,
    typmod: i32,
    coll_oid: Oid,
) -> PgResult<ColumnDef<'mcx>> {
    Ok(ColumnDef {
        colname: Some(PgString::from_str_in(colname, mcx)?),
        typeName: Some(::mcx::alloc_in(mcx, make_type_name_from_oid(mcx, type_oid, typmod))?),
        compression: None,
        inhcount: 0,
        is_local: true,
        is_not_null: false,
        is_from_type: false,
        storage: 0,
        storage_name: None,
        raw_default: None,
        cooked_default: None,
        identity: 0,
        identitySequence: None,
        generated: 0,
        collClause: None,
        collOid: coll_oid,
        constraints: PgVec::new_in(mcx),
        fdwoptions: PgVec::new_in(mcx),
        location: -1,
    })
}

/// `makeTypeNameFromOid(typeOid, typmod)` (`nodes/makefuncs.c`).
fn make_type_name_from_oid<'mcx>(mcx: Mcx<'mcx>, type_oid: Oid, typmod: i32) -> TypeName<'mcx> {
    TypeName {
        names: PgVec::new_in(mcx),
        typeOid: type_oid,
        setof: false,
        pct_type: false,
        typmods: PgVec::new_in(mcx),
        typemod: typmod,
        arrayBounds: PgVec::new_in(mcx),
        location: -1,
    }
}
