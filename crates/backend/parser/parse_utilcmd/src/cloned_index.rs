//! `generateClonedIndexStmt` (`parser/parse_utilcmd.c`) — build an `IndexStmt`
//! describing an index equivalent to an already-existing `source_idx`, with
//! expression/predicate Var attnos remapped through `attmap`.
//!
//! Used to clone a parent partitioned index onto a child partition (DefineIndex
//! partitioned recursion / ATTACH PARTITION) and by CREATE TABLE ... LIKE
//! INCLUDING INDEXES.

use alloc::format;
use alloc::string::{String, ToString};

use mcx::{Mcx, PgString, PgVec};

use ::types_core::primitive::{AttrNumber, InvalidOid, OidIsValid};
use ::types_core::Oid;
use ::types_error::PgResult;
use types_error::{ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
use ::nodes::ddlnodes::{DefElem, IndexElem, IndexStmt, DEFELEM_UNSPEC};
use ::nodes::nodes::Node;
use ::nodes::rawnodes::{RangeVar, SORTBY_DEFAULT, SORTBY_DESC, SORTBY_NULLS_DEFAULT, SORTBY_NULLS_FIRST, SORTBY_NULLS_LAST};
use ::nodes::value::StringNode;
use ::rel::Relation;
use ::types_tuple::attmap::AttrMap;

use ::nodes_core::nodefuncs::expr_type;
use ::utils_error::ereport;

use ::index_amapi::GetIndexAmRoutineByAmId;
use ::common_reloptions::untransformRelOptions;

use read_seams as read_seams;
use lsyscache_seams as lsyscache;
use syscache_seams as syscache;
use pg_depend_seams as pg_depend;

use crate::core::{make_string, NodePtr};

/// `INDOPTION_DESC` (`access/amapi.h`).
const INDOPTION_DESC: i16 = 0x0001;
/// `INDOPTION_NULLS_FIRST` (`access/amapi.h`).
const INDOPTION_NULLS_FIRST: i16 = 0x0002;

/// `AttributeNumberIsValid(attnum)` — `attnum != 0`.
#[inline]
fn attribute_number_is_valid(attnum: AttrNumber) -> bool {
    attnum != 0
}

/// `generateClonedIndexStmt(heapRel, source_idx, attmap, &constraintOid)`
/// (parse_utilcmd.c). Returns the cloned `IndexStmt` plus the OID of the
/// constraint associated with the index (`*constraintOid`, `InvalidOid` if
/// none).
pub fn generateClonedIndexStmt<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: Option<&RangeVar<'mcx>>,
    source_idx: &Relation<'mcx>,
    attmap: &AttrMap<'mcx>,
) -> PgResult<(IndexStmt<'mcx>, Oid)> {
    let source_relid = source_idx.rd_id;
    let mut constraint_oid = InvalidOid;

    // Fetch the pg_index image (the C reads fields off rd_indextuple / GETSTRUCT;
    // search_pg_index_info gives the fixed scalars plus the indkey / indcollation
    // / indclass / indoption vectors). indexprs / indpred ride as text.
    let idxinfo = syscache::search_pg_index_info::call(mcx, source_relid)?
        .ok_or_else(|| elog_error(format!("cache lookup failed for index {source_relid}")))?;
    let indrelid = idxinfo.indrelid;

    // Form_pg_class fields the C reads off ht_idxrel: relam, reltablespace,
    // reloptions. relam / reltablespace are available on the relcache rd_rel
    // (exactly the GETSTRUCT view); reloptions is fetched from the syscache tuple
    // below (the C comment: the relcache copy omits optional fields).
    let relam = source_idx.rd_rel.relam;
    let reltablespace = source_idx.rd_rel.reltablespace;

    // pg_am tuple → amname.
    let access_method = lsyscache::get_am_name::call(mcx, relam)?
        .ok_or_else(|| elog_error(format!("cache lookup failed for access method {relam}")))?;

    let table_space = if OidIsValid(reltablespace) {
        tablespace::get_tablespace_name(mcx, reltablespace)?
    } else {
        None
    };

    // iswithoutoverlaps mirrors the C derivation.
    let iswithoutoverlaps =
        (idxinfo.indisprimary || idxinfo.indisunique) && idxinfo.indisexclusion;

    // Begin building the IndexStmt skeleton.
    let mut index = IndexStmt {
        idxname: None,
        relation: match heap_rel {
            Some(rv) => Some(::mcx::alloc_in(mcx, Node::mk_range_var(mcx, rv.clone_in(mcx)?)?)?),
            None => None,
        },
        accessMethod: Some(access_method),
        tableSpace: table_space,
        indexParams: PgVec::new_in(mcx),
        indexIncludingParams: PgVec::new_in(mcx),
        options: PgVec::new_in(mcx),
        whereClause: None,
        excludeOpNames: PgVec::new_in(mcx),
        idxcomment: None,
        indexOid: InvalidOid,
        oldNumber: 0,
        oldCreateSubid: 0,
        oldFirstRelfilelocatorSubid: 0,
        unique: idxinfo.indisunique,
        nulls_not_distinct: idxinfo.indnullsnotdistinct,
        primary: idxinfo.indisprimary,
        isconstraint: false,
        iswithoutoverlaps,
        deferrable: false,
        initdeferred: false,
        transformed: true,
        concurrent: false,
        if_not_exists: false,
        reset_default_tblspc: false,
    };

    // If the index is marked PRIMARY or has an exclusion condition, it's
    // certainly from a constraint; else, if it's not marked UNIQUE, it certainly
    // isn't. If it is or might be from a constraint, fetch the pg_constraint row.
    if index.primary || index.unique || idxinfo.indisexclusion {
        let constraint_id = pg_depend::get_index_constraint::call(source_relid)?;
        if OidIsValid(constraint_id) {
            constraint_oid = constraint_id;

            let (condeferrable, condeferred, _contype, conexclop) =
                syscache::pg_constraint_clone_info::call(mcx, constraint_id)?.ok_or_else(|| {
                    elog_error(format!("cache lookup failed for constraint {constraint_id}"))
                })?;

            index.isconstraint = true;
            index.deferrable = condeferrable;
            index.initdeferred = condeferred;

            // If it's an exclusion constraint, we need the operator names.
            if idxinfo.indisexclusion {
                let elems = conexclop.ok_or_else(|| {
                    elog_error(format!(
                        "unexpected null conexclop for constraint {constraint_id}"
                    ))
                })?;

                for &operid in elems.iter() {
                    let opform = syscache::pg_operator_form::call(mcx, operid)?.ok_or_else(|| {
                        elog_error(format!("cache lookup failed for operator {operid}"))
                    })?;
                    let oprname = opform.oprname;
                    // For simplicity we always schema-qualify the op name.
                    let nspname = lsyscache::get_namespace_name::call(mcx, opform.oprnamespace)?
                        .map(|s| s.as_str().to_string())
                        .unwrap_or_default();
                    let namelist: PgVec<'mcx, NodePtr<'mcx>> = {
                        let mut v = PgVec::new_in(mcx);
                        v.push(make_string(mcx, &nspname)?);
                        v.push(make_string(mcx, &oprname)?);
                        v
                    };
                    let namelist_node =
                        ::mcx::alloc_in(mcx, Node::mk_list(mcx, namelist)?)?;
                    index.excludeOpNames.push(namelist_node);
                }
            }
        }
    }

    // Get the index expressions, if any.
    let indexprs: PgVec<'mcx, NodePtr<'mcx>> =
        match syscache::pg_index_exprs_text::call(source_relid)? {
            Some(exprs_string) => {
                let node = read_seams::string_to_node::call(mcx, &exprs_string)?;
                let mut v = PgVec::new_in(mcx);
                match (*node).as_list() {
                    Some(items) => {
                        for it in items.iter() {
                            v.push(::mcx::alloc_in(mcx, it.clone_in(mcx)?)?);
                        }
                    }
                    // A single-element list can be represented as a bare node.
                    None => v.push(::mcx::alloc_in(mcx, (*node).clone_in(mcx)?)?),
                }
                v
            }
            None => PgVec::new_in(mcx),
        };

    // Build the list of IndexElem (key columns).
    let mut indexpr_iter = indexprs.into_iter();
    for keyno in 0..idxinfo.indnkeyatts as usize {
        let attnum = idxinfo.indkey[keyno];
        // TupleDescAttr(RelationGetDescr(source_idx), keyno)->attname.
        let attr = source_idx.rd_att.attr(keyno);
        let indexcolname = String::from_utf8_lossy(attr.attname.name_str()).into_owned();
        let opt = idxinfo.indoption[keyno];

        let (name, expr, keycoltype): (Option<PgString<'mcx>>, Option<NodePtr<'mcx>>, Oid);

        if attribute_number_is_valid(attnum) {
            // Simple index column.
            let attname = lsyscache::get_attname::call(mcx, indrelid, attnum, false)?
                .ok_or_else(|| elog_error(format!("cache lookup failed for attribute {attnum}")))?;
            let coltype = lsyscache::get_atttype::call(indrelid, attnum)?;
            name = Some(attname);
            expr = None;
            keycoltype = coltype;
        } else {
            // Expressional index.
            let mut indexkey = indexpr_iter
                .next()
                .ok_or_else(|| elog_error("too few entries in indexprs list"))?;

            // Adjust Vars to match new table's column numbering.
            let mut found_whole_row = false;
            rewrite_core::replace::map_variable_attnos(
                &mut indexkey,
                1,
                0,
                &attmap.attnums,
                InvalidOid,
                &mut found_whole_row,
                mcx,
            )?;

            // As in expandTableLikeClause, reject whole-row variables.
            if found_whole_row {
                return Err(whole_row_error(source_idx));
            }

            let coltype = expr_type(indexkey.as_expr())?;
            name = None;
            expr = Some(indexkey);
            keycoltype = coltype;
        }

        // Add the collation name, if non-default.
        let collation = get_collation(mcx, idxinfo.indcollation[keyno], keycoltype)?;
        // Add the operator class name, if non-default.
        let opclass = get_opclass(mcx, idxinfo.indclass[keyno], keycoltype)?;
        // Per-column opclass options.
        let opclassopts = get_opclassopts(mcx, source_relid, (keyno + 1) as i16)?;

        let mut ordering = SORTBY_DEFAULT;
        let mut nulls_ordering = SORTBY_NULLS_DEFAULT;

        // Adjust options if the AM supports sort ordering.
        if GetIndexAmRoutineByAmId(relam)?.amcanorder {
            if opt & INDOPTION_DESC != 0 {
                ordering = SORTBY_DESC;
                if opt & INDOPTION_NULLS_FIRST == 0 {
                    nulls_ordering = SORTBY_NULLS_LAST;
                }
            } else if opt & INDOPTION_NULLS_FIRST != 0 {
                nulls_ordering = SORTBY_NULLS_FIRST;
            }
        }

        let iparam = IndexElem {
            name,
            expr,
            indexcolname: Some(PgString::from_str_in(&indexcolname, mcx)?),
            collation,
            opclass,
            opclassopts,
            ordering,
            nulls_ordering,
        };
        index
            .indexParams
            .push(::mcx::alloc_in(mcx, Node::mk_index_elem(mcx, iparam)?)?);
    }

    // Handle included columns separately.
    for keyno in idxinfo.indnkeyatts as usize..idxinfo.indnatts as usize {
        let attnum = idxinfo.indkey[keyno];
        let attr = source_idx.rd_att.attr(keyno);
        let indexcolname = String::from_utf8_lossy(attr.attname.name_str()).into_owned();

        let name = if attribute_number_is_valid(attnum) {
            let attname = lsyscache::get_attname::call(mcx, indrelid, attnum, false)?
                .ok_or_else(|| elog_error(format!("cache lookup failed for attribute {attnum}")))?;
            Some(attname)
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("expressions are not supported in included columns")
                .into_error());
        };

        let iparam = IndexElem {
            name,
            expr: None,
            indexcolname: Some(PgString::from_str_in(&indexcolname, mcx)?),
            collation: PgVec::new_in(mcx),
            opclass: PgVec::new_in(mcx),
            opclassopts: PgVec::new_in(mcx),
            ordering: SORTBY_DEFAULT,
            nulls_ordering: SORTBY_NULLS_DEFAULT,
        };
        index
            .indexIncludingParams
            .push(::mcx::alloc_in(mcx, Node::mk_index_elem(mcx, iparam)?)?);
    }

    // Copy reloptions if any (pg_class.reloptions of the index).
    let reloptions = syscache::fetch_class_reloptions::call(mcx, source_relid)?;
    if !reloptions.is_null {
        for (defname, arg) in untransformRelOptions(mcx, Some(reloptions.bytes.as_slice()))? {
            index.options.push(make_def_elem(mcx, &defname, arg.as_deref())?);
        }
    }

    // If it's a partial index, decompile and append the predicate.
    if let Some(pred_str) = syscache::pg_index_pred_text::call(source_relid)? {
        let mut pred_tree = read_seams::string_to_node::call(mcx, &pred_str)?;
        let mut found_whole_row = false;
        rewrite_core::replace::map_variable_attnos(
            &mut pred_tree,
            1,
            0,
            &attmap.attnums,
            InvalidOid,
            &mut found_whole_row,
            mcx,
        )?;
        if found_whole_row {
            return Err(whole_row_error(source_idx));
        }
        index.whereClause = Some(pred_tree);
    }

    Ok((index, constraint_oid))
}

/// `get_collation(collation, actual_datatype)` (parse_utilcmd.c): the
/// schema-qualified collation name as a `List` of `String` nodes, or an empty
/// list (NIL) when the collation is invalid or matches the type's default.
fn get_collation<'mcx>(
    mcx: Mcx<'mcx>,
    collation: Oid,
    actual_datatype: Oid,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let mut result = PgVec::new_in(mcx);
    if !OidIsValid(collation) {
        return Ok(result); // easy case
    }
    if collation == lsyscache::get_typcollation::call(actual_datatype)? {
        return Ok(result); // just let it default
    }

    let (nspname, collname) = syscache::collation_qualified_name::call(mcx, collation)?
        .ok_or_else(|| elog_error(format!("cache lookup failed for collation {collation}")))?;
    // For simplicity, we always schema-qualify the name.
    result.push(make_string(mcx, &String::from_utf8_lossy(&nspname))?);
    result.push(make_string(mcx, &String::from_utf8_lossy(&collname))?);
    Ok(result)
}

/// `get_opclass(opclass, actual_datatype)` (parse_utilcmd.c): the
/// schema-qualified opclass name as a `List` of `String` nodes, or an empty list
/// (NIL) when it is the default opclass for the type+AM.
fn get_opclass<'mcx>(
    mcx: Mcx<'mcx>,
    opclass: Oid,
    actual_datatype: Oid,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let mut result = PgVec::new_in(mcx);

    let (opcnamespace, opcmethod, opcname) =
        syscache::opclass_namespace_method_name::call(mcx, opclass)?
            .ok_or_else(|| elog_error(format!("cache lookup failed for opclass {opclass}")))?;

    if lsyscache::get_default_opclass::call(actual_datatype, opcmethod)? != opclass {
        // For simplicity, we always schema-qualify the name.
        let nspname = lsyscache::get_namespace_name::call(mcx, opcnamespace)?
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        result.push(make_string(mcx, &nspname)?);
        result.push(make_string(mcx, opcname.as_str())?);
    }
    Ok(result)
}

/// `untransformRelOptions(get_attoptions(source_relid, keyno + 1))` — the
/// per-column opclass options as a `List` of `DefElem` nodes (NIL when none).
fn get_opclassopts<'mcx>(
    mcx: Mcx<'mcx>,
    source_relid: Oid,
    attnum: i16,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let mut result = PgVec::new_in(mcx);
    let attoptions = ::lsyscache::attribute::get_attoptions(mcx, source_relid, attnum)?;
    let bytes: &[u8] = match &attoptions {
        Some(types_tuple::heaptuple::Datum::ByRef(b)) => &b[..],
        _ => return Ok(result),
    };
    for (defname, arg) in untransformRelOptions(mcx, Some(bytes))? {
        result.push(make_def_elem(mcx, &defname, arg.as_deref())?);
    }
    Ok(result)
}

/// `makeDefElem(name, makeString(arg), -1)` — a `DefElem` node carrying the
/// `(name, arg)` reloption pair; `arg` is `None` for a flag option.
fn make_def_elem<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    arg: Option<&str>,
) -> PgResult<NodePtr<'mcx>> {
    let arg_node = match arg {
        Some(s) => Some(::mcx::alloc_in(
            mcx,
            Node::mk_string(
                mcx,
                StringNode {
                    sval: PgString::from_str_in(s, mcx)?,
                },
            )?,
        )?),
        None => None,
    };
    ::mcx::alloc_in(
        mcx,
        Node::mk_def_elem(
            mcx,
            DefElem {
                defnamespace: None,
                defname: Some(PgString::from_str_in(name, mcx)?),
                arg: arg_node,
                defaction: DEFELEM_UNSPEC,
                location: -1,
            },
        )?,
    )
}

/// `elog(ERROR, msg)` shorthand.
fn elog_error(msg: impl Into<String>) -> ::types_error::PgError {
    ereport(ERROR).errmsg_internal(msg).into_error()
}

/// The `cannot convert whole-row table reference` ereport raised when a cloned
/// index expression/predicate contains a whole-row Var.
fn whole_row_error(source_idx: &Relation<'_>) -> ::types_error::PgError {
    ereport(ERROR)
        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg("cannot convert whole-row table reference")
        .errdetail(format!(
            "Index \"{}\" contains a whole-row table reference.",
            source_idx.rd_rel.relname.as_str()
        ))
        .into_error()
}
