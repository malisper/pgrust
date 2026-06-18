//! LIKE / OF-type helpers (`parse_utilcmd.c`).
//!
//! [`transformOfType`] is GROUNDED: it resolves the composite type via
//! `typenameType`, checks it with `check_of_type`, reads its rowtype `TupleDesc`
//! through the typcache (`lookup_rowtype_tupdesc`), and rebuilds the inherited
//! `ColumnDef`s in-crate. [`transformTableLikeClause`] still routes through the
//! outward seam: it reads a source relation's `TupleDesc`, defaults,
//! constraints, identity, storage, compression and comments — that machinery is
//! not yet reachable from this crate.

use mcx::{Mcx, PgBox, PgString, PgVec};

use backend_utils_error::ereport;
use types_core::Oid;
use types_error::{PgResult, ERRCODE_WRONG_OBJECT_TYPE, ERROR};
use types_storage::lock::{AccessShareLock, NoLock};

use types_nodes::nodes::Node;
use types_nodes::rawnodes::{ColumnDef, TypeName};
use types_tuple::pg_type::FormData_pg_type;

use backend_access_common_relation::relation_open;
use backend_access_table_table::table_close;
use backend_parser_parse_type::{raw_typename_to_parse, typenameType};
use backend_utils_adt_format_type::format_type_be;
use backend_utils_cache_typcache::lookup_rowtype_tupdesc;

use backend_parser_parse_utilcmd_outward_seams as sx;

use crate::core::{CreateStmtContext, NodePtr};

/// `TYPTYPE_COMPOSITE` (`pg_type.h`).
const TYPTYPE_COMPOSITE: i8 = b'c' as i8;
/// `RELKIND_COMPOSITE_TYPE` (`pg_class.h`).
const RELKIND_COMPOSITE_TYPE: u8 = b'c';

/// `transformTableLikeClause` — expand `LIKE <srctable>` into recreated column
/// definitions, routing the relcache reads through the seam and folding the
/// generated columns / check constraints / alist statements / deferred
/// LIKE-postprocessing back into the context.
pub fn transformTableLikeClause<'mcx>(
    cxt: &mut CreateStmtContext<'mcx>,
    table_like_clause: NodePtr<'mcx>,
) -> PgResult<()> {
    let mcx = cxt.mcx;

    if !matches!(table_like_clause.as_ref(), Node::TableLikeClause(_)) {
        unreachable!(
            "transformTableLikeClause: not a TableLikeClause node: {}",
            table_like_clause.node_tag()
        );
    }

    let relation = match cxt.relation.as_deref() {
        Some(n) => mcx::alloc_in(mcx, n.clone_in(mcx)?)?,
        None => {
            return Err(types_error::PgError::error(
                "transformTableLikeClause: requires cxt.relation",
            ))
        }
    };

    let (columns, ckconstraints, alist, like_postproc) = sx::transformTableLikeClause::call(
        mcx,
        &cxt.pstate,
        relation,
        table_like_clause,
        cxt.isforeign,
    )?;

    cxt.columns.extend(columns);
    cxt.ckconstraints.extend(ckconstraints);
    cxt.alist.extend(alist);
    cxt.likeclauses.extend(like_postproc);
    Ok(())
}

/// `transformOfType` — expand an `OF typename` clause into inherited column
/// definitions, reading the composite type's rowtype `TupleDesc` through the
/// typcache.
pub fn transformOfType<'mcx>(
    cxt: &mut CreateStmtContext<'mcx>,
    of_typename: NodePtr<'mcx>,
) -> PgResult<()> {
    let mcx = cxt.mcx;

    let Node::TypeName(of_tn) = of_typename.as_ref() else {
        unreachable!("transformOfType: not a TypeName node: {}", of_typename.node_tag());
    };

    // `typenameType` resolves the value-typed `types_parsenodes::TypeName`;
    // bridge the owned-tree `rawnodes::TypeName` across.
    let of_tn_parse = raw_typename_to_parse(of_tn)?;
    let (typ, _typmod) = typenameType(mcx, Some(&cxt.pstate), &of_tn_parse)?;
    check_of_type(mcx, &typ)?;
    let of_type_id = typ.oid;

    // Cache the resolved type OID on the (caller-owned) node for later. The C
    // mutates `ofTypename->typeOid`; mirror it on the node we hold.
    let mut of_typename = of_typename;
    if let Node::TypeName(tn) = of_typename.as_mut() {
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
        cxt.columns.push(mcx::alloc_in(mcx, Node::ColumnDef(n))?);
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
        typeName: Some(mcx::alloc_in(mcx, make_type_name_from_oid(mcx, type_oid, typmod))?),
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
