//! The partition-bound transform leg of `parser/parse_utilcmd.c`:
//! `transformPartitionBound` / `transformPartitionRangeBounds` /
//! `validateInfiniteBounds` / `transformPartitionBoundValue`.
//!
//! These convert a raw `PartitionBoundSpec` (`FOR VALUES ...`) into the
//! validated, canonical form: per-strategy argument-count and type-coercion
//! checks, with each bound value transformed through
//! `transformExpr`/`coerce_to_target_type` and reduced to a `Const`. The bodies
//! live here (not in `backend-parser-parse-utilcmd`) because they need the
//! parent's relcache `PartitionKey`, the expression/coercion/planner-evaluation
//! engine, and `format_type_be`/`get_attname` — all direct dependencies of this
//! crate but deliberately unreachable from the low-level parse-utilcmd crate,
//! which routes `transformPartitionBound` through its outward seam (installed
//! here in `init_seams`).
//!
//! Faithful 1:1 port of PostgreSQL 18.3 `parse_utilcmd.c`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::{alloc_in, Mcx, PgBox};
use types_core::primitive::{AttrNumber, Oid};
use types_error::PgResult;
use types_error::{
    ERRCODE_DATATYPE_MISMATCH, ERRCODE_INVALID_OBJECT_DEFINITION, ERROR,
};
use types_nodes::ddlnodes::{
    CoercionContext, PartitionBoundSpec, PartitionRangeDatum,
};
use types_nodes::nodes::{Node, NodePtr};
use types_nodes::parsestmt::{ParseExprKind, ParseState};
use types_nodes::partition::PartitionRangeDatumKind;
use types_nodes::primnodes::{CoercionForm, Const, Expr};
use types_partition::{
    PartitionKeyData, PARTITION_STRATEGY_HASH, PARTITION_STRATEGY_LIST, PARTITION_STRATEGY_RANGE,
};

use backend_utils_error::ereport;

use crate::helpers::here;

/// `get_partition_col_collation(key, i)` — `key->partcollation[i]`.
fn get_partition_col_collation(key: &PartitionKeyData<'_>, i: usize) -> Oid {
    key.partcollation[i]
}
/// `get_partition_col_typid(key, i)` — `key->parttypid[i]`.
fn get_partition_col_typid(key: &PartitionKeyData<'_>, i: usize) -> Oid {
    key.parttypid[i]
}
/// `get_partition_col_typmod(key, i)` — `key->parttypmod[i]`.
fn get_partition_col_typmod(key: &PartitionKeyData<'_>, i: usize) -> i32 {
    key.parttypmod[i]
}

/// Resolve the partition column's name for error messages, mirroring the C
/// `key->partattrs[idx] != 0 ? get_attname(...) : deparse_expression(...)`
/// dispatch. The expression branch (`partattrs[idx] == 0`) is deparsed via the
/// same `ruleutils` path the partspec code uses; here it is needed only for the
/// error string, so when the deparse substrate declines we fall back to a
/// generic column label rather than failing the DDL.
fn partition_colname<'mcx>(
    mcx: Mcx<'mcx>,
    parent_relid: Oid,
    key: &PartitionKeyData<'_>,
    idx: usize,
) -> PgResult<String> {
    let attno: AttrNumber = key.partattrs[idx];
    if attno != 0 {
        // get_attname(RelationGetRelid(parent), key->partattrs[idx], false)
        match backend_utils_cache_lsyscache::attribute::get_attname(mcx, parent_relid, attno, false)?
        {
            Some(s) => Ok(s.as_str().to_string()),
            None => Ok(format!("column {attno}")),
        }
    } else {
        // Expression partition column. The error-only label deparses the i-th
        // partition expression; lacking a stand-alone deparse entry here, use
        // the same generic label C would print the expression for.
        Ok(String::from("partition expression"))
    }
}

/// `transformPartitionBound(pstate, parent, spec)` (parse_utilcmd.c) — transform
/// a partition bound specification against the parent's partition key.
///
/// The parent is opened by OID (the C takes the already-open `Relation`); the
/// caller holds a lock on it. Returns the validated, canonicalized
/// `PartitionBoundSpec` (a fresh copy — C never scribbles on the input).
pub fn transformPartitionBound<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    parent_relid: Oid,
    spec_node: NodePtr<'mcx>,
) -> PgResult<NodePtr<'mcx>> {
    let spec: &PartitionBoundSpec = (*spec_node)
        .as_partitionboundspec()
        .ok_or_else(|| {
            ereport(ERROR)
                .errmsg_internal("transformPartitionBound: not a PartitionBoundSpec node")
                .into_error()
        })?;

    // PartitionKey key = RelationGetPartitionKey(parent);
    let parent = backend_access_common_relation::relation_open(
        mcx,
        parent_relid,
        types_storage::lock::NoLock,
    )?;
    let key = backend_utils_cache_partcache::RelationGetPartitionKey(mcx, &parent)?
        .ok_or_else(|| {
            ereport(ERROR)
                .errmsg_internal("transformPartitionBound: parent has no partition key")
                .into_error()
        })?;
    let strategy = key.strategy;
    let partnatts = key.partnatts as usize;

    // result_spec = copyObject(spec); /* Avoid scribbling on input */
    let mut result_spec: PartitionBoundSpec<'mcx> = spec.clone_in(mcx)?;

    if spec.is_default {
        // Hash partitioning does not support a default partition.
        if strategy == PARTITION_STRATEGY_HASH {
            parent.close(types_storage::lock::NoLock)?;
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("a hash-partitioned table may not have a default partition")
                .finish(here("transformPartitionBound"))
                .map(|()| unreachable!());
        }

        // Assign the parent's strategy to the default partition bound spec.
        result_spec.strategy = strategy;
        parent.close(types_storage::lock::NoLock)?;
        return Ok(alloc_in(mcx, Node::PartitionBoundSpec(result_spec))?);
    }

    if strategy == PARTITION_STRATEGY_HASH {
        {
            if spec.strategy != PARTITION_STRATEGY_HASH {
                parent.close(types_storage::lock::NoLock)?;
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("invalid bound specification for a hash partition")
                    .finish(here("transformPartitionBound"))
                    .map(|()| unreachable!());
            }

            if spec.modulus <= 0 {
                parent.close(types_storage::lock::NoLock)?;
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("modulus for hash partition must be an integer value greater than zero")
                    .finish(here("transformPartitionBound"))
                    .map(|()| unreachable!());
            }

            debug_assert!(spec.remainder >= 0);

            if spec.remainder >= spec.modulus {
                parent.close(types_storage::lock::NoLock)?;
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("remainder for hash partition must be less than modulus")
                    .finish(here("transformPartitionBound"))
                    .map(|()| unreachable!());
            }
        }
    } else if strategy == PARTITION_STRATEGY_LIST {
        {
            if spec.strategy != PARTITION_STRATEGY_LIST {
                parent.close(types_storage::lock::NoLock)?;
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("invalid bound specification for a list partition")
                    .finish(here("transformPartitionBound"))
                    .map(|()| unreachable!());
            }

            // Get the only column's name + type data (single-column list key).
            let colname = partition_colname(mcx, parent_relid, &key, 0)?;
            let coltype = get_partition_col_typid(&key, 0);
            let coltypmod = get_partition_col_typmod(&key, 0);
            let partcollation = get_partition_col_collation(&key, 0);

            let mut new_listdatums: mcx::PgVec<'mcx, NodePtr<'mcx>> =
                mcx::vec_with_capacity_in(mcx, spec.listdatums.len())?;

            for cell in spec.listdatums.iter() {
                let value = transformPartitionBoundValue(
                    mcx,
                    pstate,
                    cell,
                    &colname,
                    coltype,
                    coltypmod,
                    partcollation,
                )?;

                // Don't add to the result if the value is a duplicate.
                let value_node = Node::mk_expr(mcx, Expr::Const(value));
                let mut duplicate = false;
                for existing in new_listdatums.iter() {
                    if backend_nodes_equalfuncs_seams::equal_node::call(existing, &value_node) {
                        duplicate = true;
                        break;
                    }
                }
                if duplicate {
                    continue;
                }

                new_listdatums.push(alloc_in(mcx, value_node)?);
            }

            result_spec.listdatums = new_listdatums;
        }
    } else if strategy == PARTITION_STRATEGY_RANGE {
        {
            if spec.strategy != PARTITION_STRATEGY_RANGE {
                parent.close(types_storage::lock::NoLock)?;
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("invalid bound specification for a range partition")
                    .finish(here("transformPartitionBound"))
                    .map(|()| unreachable!());
            }

            if spec.lowerdatums.len() != partnatts {
                parent.close(types_storage::lock::NoLock)?;
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("FROM must specify exactly one value per partitioning column")
                    .finish(here("transformPartitionBound"))
                    .map(|()| unreachable!());
            }
            if spec.upperdatums.len() != partnatts {
                parent.close(types_storage::lock::NoLock)?;
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("TO must specify exactly one value per partitioning column")
                    .finish(here("transformPartitionBound"))
                    .map(|()| unreachable!());
            }

            // Convert raw parse nodes into PartitionRangeDatum nodes.
            result_spec.lowerdatums =
                transformPartitionRangeBounds(mcx, pstate, &spec.lowerdatums, parent_relid, &key)?;
            result_spec.upperdatums =
                transformPartitionRangeBounds(mcx, pstate, &spec.upperdatums, parent_relid, &key)?;
        }
    } else {
        parent.close(types_storage::lock::NoLock)?;
        return ereport(ERROR)
            .errmsg_internal(format!("unexpected partition strategy: {}", strategy as i32))
            .finish(here("transformPartitionBound"))
            .map(|()| unreachable!());
    }

    parent.close(types_storage::lock::NoLock)?;
    Ok(alloc_in(mcx, Node::PartitionBoundSpec(result_spec))?)
}

/// `transformPartitionRangeBounds(pstate, blist, parent)` (parse_utilcmd.c) —
/// convert the raw range-bound expressions into `PartitionRangeDatum` nodes,
/// handling the `minvalue`/`maxvalue` `ColumnRef` sentinels.
fn transformPartitionRangeBounds<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    blist: &[NodePtr<'mcx>],
    parent_relid: Oid,
    key: &PartitionKeyData<'_>,
) -> PgResult<mcx::PgVec<'mcx, NodePtr<'mcx>>> {
    let mut result: mcx::PgVec<'mcx, NodePtr<'mcx>> = mcx::vec_with_capacity_in(mcx, blist.len())?;

    for (i, lc) in blist.iter().enumerate() {
        let expr_node: &Node = lc;
        let mut prd: Option<PartitionRangeDatum<'mcx>> = None;

        // Infinite range bounds -- "minvalue"/"maxvalue" -- arrive as ColumnRefs.
        if let Some(cref) = expr_node.as_columnref() {
            let mut cname: Option<String> = None;
            if cref.fields.len() == 1 {
                if let Some(s) = cref.fields[0].as_string() {
                    cname = Some(s.sval.as_str().to_string());
                }
            }

            match cname.as_deref() {
                None => {
                    // Not single-field-name form; let transformExpr report.
                }
                Some("minvalue") => {
                    prd = Some(PartitionRangeDatum {
                        kind: PartitionRangeDatumKind::MinValue,
                        value: None,
                        location: -1,
                    });
                }
                Some("maxvalue") => {
                    prd = Some(PartitionRangeDatum {
                        kind: PartitionRangeDatumKind::MaxValue,
                        value: None,
                        location: -1,
                    });
                }
                Some(_) => {}
            }
        }

        if prd.is_none() {
            // Get the column's name + type data.
            let colname = partition_colname(mcx, parent_relid, key, i)?;
            let coltype = get_partition_col_typid(key, i);
            let coltypmod = get_partition_col_typmod(key, i);
            let partcollation = get_partition_col_collation(key, i);

            let value = transformPartitionBoundValue(
                mcx,
                pstate,
                lc,
                &colname,
                coltype,
                coltypmod,
                partcollation,
            )?;
            if value.constisnull {
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("cannot specify NULL in range bound")
                    .finish(here("transformPartitionRangeBounds"))
                    .map(|()| unreachable!());
            }
            prd = Some(PartitionRangeDatum {
                kind: PartitionRangeDatumKind::Value,
                value: Some(alloc_in(mcx, Node::mk_expr(mcx, Expr::Const(value)))?),
                location: -1,
            });
        }

        let mut prd = prd.expect("prd is set on all paths");
        // prd->location = exprLocation(expr);
        prd.location = backend_nodes_core::nodefuncs::expr_location(expr_node.as_expr())?;

        result.push(alloc_in(mcx, Node::PartitionRangeDatum(prd))?);
    }

    // Once we see MINVALUE or MAXVALUE for one column, the rest must match.
    validateInfiniteBounds(&result)?;

    Ok(result)
}

/// `validateInfiniteBounds(pstate, blist)` (parse_utilcmd.c) — a MAXVALUE or
/// MINVALUE bound must be followed only by more of the same.
fn validateInfiniteBounds(blist: &[NodePtr<'_>]) -> PgResult<()> {
    let mut kind = PartitionRangeDatumKind::Value;

    for node in blist.iter() {
        let prd = node.as_partitionrangedatum().ok_or_else(|| {
            ereport(ERROR)
                .errmsg_internal("validateInfiniteBounds: not a PartitionRangeDatum")
                .into_error()
        })?;

        if kind == prd.kind {
            continue;
        }

        match kind {
            PartitionRangeDatumKind::Value => {
                kind = prd.kind;
            }
            PartitionRangeDatumKind::MaxValue => {
                return ereport(ERROR)
                    .errcode(ERRCODE_DATATYPE_MISMATCH)
                    .errmsg("every bound following MAXVALUE must also be MAXVALUE")
                    .finish(here("validateInfiniteBounds"))
                    .map(|()| unreachable!());
            }
            PartitionRangeDatumKind::MinValue => {
                return ereport(ERROR)
                    .errcode(ERRCODE_DATATYPE_MISMATCH)
                    .errmsg("every bound following MINVALUE must also be MINVALUE")
                    .finish(here("validateInfiniteBounds"))
                    .map(|()| unreachable!());
            }
        }
    }

    Ok(())
}

/// `transformPartitionBoundValue(pstate, val, colName, colType, colTypmod,
/// partCollation)` (parse_utilcmd.c) — transform one bound entry into a `Const`.
#[allow(clippy::too_many_arguments)]
fn transformPartitionBoundValue<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    val: &Node<'mcx>,
    col_name: &str,
    col_type: Oid,
    col_typmod: i32,
    part_collation: Oid,
) -> PgResult<Const> {
    let val_location = backend_nodes_core::nodefuncs::expr_location(val.as_expr())?;

    // value = transformExpr(pstate, val, EXPR_KIND_PARTITION_BOUND);
    let transformed = backend_parser_parse_expr::transformExpr(
        pstate,
        Some(val.clone_in(mcx)?),
        ParseExprKind::EXPR_KIND_PARTITION_BOUND,
    )?;
    let value = transformed.ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal("transformPartitionBoundValue: NULL bound expression")
            .into_error()
    })?;

    // Coerce to the correct type.
    let exprtype = backend_nodes_core::nodefuncs::expr_type(Some(&value))?;
    let coerced = backend_parser_coerce::coerce_to_target_type(
        mcx,
        Some(pstate),
        value,
        exprtype,
        col_type,
        col_typmod,
        CoercionContext::COERCION_ASSIGNMENT,
        CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )?;

    let mut value = match coerced {
        Some(v) => v,
        None => {
            let type_name =
                backend_utils_adt_format_type::format_type_be(mcx, col_type)?;
            return ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "specified value cannot be cast to type {} for column \"{}\"",
                    type_name.as_str(),
                    col_name
                ))
                .finish(here("transformPartitionBoundValue"))
                .map(|()| unreachable!());
        }
    };

    // Evaluate the expression, if needed, assigning the partition key's data
    // type and collation to the resulting Const node.
    if !matches!(value, Expr::Const(_)) {
        backend_parser_parse_collate::assign_expr_collations(Some(pstate), &mut value)?;
        let planned = backend_optimizer_plan_planner::expression_planner(mcx, value)?;
        let evaluated = backend_optimizer_util_clauses::evaluate_expr(
            mcx,
            planned,
            col_type,
            col_typmod,
            part_collation,
        )?;
        match evaluated {
            Expr::Const(mut c) => {
                c.location = val_location;
                Ok(c)
            }
            _ => ereport(ERROR)
                .errmsg_internal("could not evaluate partition bound expression")
                .finish(here("transformPartitionBoundValue"))
                .map(|()| unreachable!()),
        }
    } else {
        // Already a Const: just insert the right collation + location.
        match value {
            Expr::Const(mut c) => {
                c.constcollid = part_collation;
                c.location = val_location;
                Ok(c)
            }
            _ => unreachable!(),
        }
    }
}

/// The `transformPartitionBound` outward-seam wrapper installed for
/// `backend-parser-parse-utilcmd`'s ATTACH PARTITION leg.
pub fn transform_partition_bound_seam<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    parent_relid: Oid,
    spec: PgBox<'mcx, Node<'mcx>>,
) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    transformPartitionBound(mcx, pstate, parent_relid, spec)
}
