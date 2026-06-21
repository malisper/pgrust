//! `indexcmds.c` — [`ComputeIndexAttrs`]: the per-index-column opclass /
//! collation / expression resolution loop, the exclusion-operator and
//! WITHOUT-OVERLAPS handling, the included-column restrictions, the per-column
//! `indoption` (DESC / NULLS FIRST/LAST) derivation, and per-column
//! `attoptions`.
//!
//! Branch order, casts, error codes / messages / SQLSTATE match the C source.
//! The owned [`IndexInfo`] makes the C `indexInfo->ii_*` reads/writes plain
//! field accesses.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use mcx::{Mcx, PgVec};

use types_amapi::{CompareType, COMPARE_EQ, COMPARE_OVERLAP};
use types_core::primitive::Oid;
use types_core::{InvalidOid, OidIsValid};
use types_error::PgResult;
use types_nodes::ddlnodes::IndexElem;
use types_nodes::execnodes::IndexInfo;
use types_nodes::nodes::{ntag, NodePtr};
use types_nodes::primnodes::Expr;
use types_scan::scankey::{InvalidStrategy, StrategyNumber};

use backend_utils_error::ereport;
use types_error::ERROR;

use backend_catalog_namespace::get_collation_oid;
use backend_nodes_core::nodefuncs::{expr_collation, expr_type};
use backend_parser_parse_oper::compatible_oper_opid;
use backend_optimizer_util_clauses::contain_mutable_functions_after_planning;

use backend_utils_init_miscinit::{GetUserIdAndSecContext, SetUserIdAndSecContext};
use backend_utils_misc_guc::{at_eoxact_guc, NewGUCNestLevel};
use backend_utils_misc_guc_seams::restrict_search_path;

use backend_utils_adt_format_type_seams as formattype_seam;
use backend_utils_adt_regproc_seams as regproc_seam;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_syscache_seams as syscache;

use types_catalog::pg_index::{INDOPTION_DESC, INDOPTION_NULLS_FIRST};
use types_nodes::rawnodes::{SORTBY_DEFAULT, SORTBY_DESC, SORTBY_NULLS_DEFAULT, SORTBY_NULLS_FIRST};

use crate::{name_list, name_list_strings, opclass::GetOperatorFromCompareType, opclass::ResolveOpClass};

/// `ComputeIndexAttrs(...)`.
///
/// Computes per-index-column information, including indexed column numbers or
/// index expressions, opclasses, and their options. All output vectors must be
/// allocated for all columns, including INCLUDE'd ones.
///
/// `att_list` is the list of [`IndexElem`]s. `exclusion_op_names` is the list of
/// operator-name lists (one per key column) for an exclusion constraint, or
/// `None` (each element is a `Node::List` of `String` value nodes). When the
/// caller switched to the table owner, `ddl_userid` is the role for ACL checks;
/// otherwise it is `InvalidOid` and the other `ddl_*` arguments are undefined.
#[allow(clippy::too_many_arguments)]
pub fn ComputeIndexAttrs<'mcx>(
    mcx: Mcx<'mcx>,
    index_info: &mut IndexInfo<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    type_oids: &mut [Oid],
    collation_oids: &mut [Oid],
    opclass_oids: &mut [Oid],
    opclass_options: &mut [types_tuple::Datum<'mcx>],
    col_options: &mut [i16],
    att_list: &[&IndexElem<'mcx>],
    exclusion_op_names: Option<&PgVec<'mcx, NodePtr<'mcx>>>,
    rel_id: Oid,
    access_method_name: &str,
    access_method_id: Oid,
    amcanorder: bool,
    isconstraint: bool,
    iswithoutoverlaps: bool,
    ddl_userid: Oid,
    ddl_sec_context: i32,
    ddl_save_nestlevel: &mut i32,
) -> PgResult<()> {
    let _ = rel;
    let nkeycols = index_info.ii_NumIndexKeyAttrs;
    let mut save_userid = InvalidOid;
    let mut save_sec_context = 0i32;

    // Allocate space for exclusion operator info, if needed.
    //
    // `nextExclOp` walks the exclusion-op-name list in lockstep with the
    // attribute loop; modelled as an index into the list.
    let mut next_excl_idx: Option<usize> = if let Some(excl) = exclusion_op_names {
        debug_assert!(excl.len() as i32 == nkeycols);
        alloc_exclusion(mcx, index_info, nkeycols);
        Some(0)
    } else {
        None
    };

    // If this is a WITHOUT OVERLAPS constraint, we need space for exclusion
    // ops, but we don't need to parse anything, so we can let nextExclOp be
    // NULL. Note that for partitions/inheriting/LIKE, exclusionOpNames will be
    // set, so we already allocated above.
    if iswithoutoverlaps {
        if exclusion_op_names.is_none() {
            alloc_exclusion(mcx, index_info, nkeycols);
        }
        next_excl_idx = None;
    }

    if OidIsValid(ddl_userid) {
        let (u, c) = GetUserIdAndSecContext();
        save_userid = u;
        save_sec_context = c;
    }

    // process attributeList
    let mut attn: i32 = 0;
    for attribute in att_list.iter() {
        let atttype: Oid;
        let mut attcollation: Oid;

        // Process the column-or-expression to be indexed.
        if let Some(name) = attribute.name.as_ref() {
            // Simple index attribute.
            debug_assert!(attribute.expr.is_none());
            let name = name.as_str();
            match syscache::search_attname_attnum::call(rel_id, name)? {
                Some((attnum, _attisdropped)) => {
                    let attform = syscache::pg_attribute_form::call(rel_id, attnum)?
                        .expect("ComputeIndexAttrs: attname found but attribute form missing");
                    index_info.ii_IndexAttrNumbers[attn as usize] = attform.attnum;
                    atttype = attform.atttypid;
                    attcollation = attform.attcollation;
                }
                None => {
                    // difference in error message spellings is historical
                    if isconstraint {
                        return Err(ereport(ERROR)
                            .errcode(types_error::ERRCODE_UNDEFINED_COLUMN)
                            .errmsg(format!("column \"{name}\" named in key does not exist"))
                            .into_error());
                    } else {
                        return Err(ereport(ERROR)
                            .errcode(types_error::ERRCODE_UNDEFINED_COLUMN)
                            .errmsg(format!("column \"{name}\" does not exist"))
                            .into_error());
                    }
                }
            }
        } else {
            // Index expression.
            let expr_node = attribute
                .expr
                .as_ref()
                .map(|n| n.as_ref())
                .expect("ComputeIndexAttrs: expression IndexElem has neither name nor expr");

            if attn >= nkeycols {
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("expressions are not supported in included columns")
                    .into_error());
            }
            let expr_as_expr = expr_node
                .as_expr()
                .expect("ComputeIndexAttrs: index expression is not an Expr");
            atttype = expr_type(Some(expr_as_expr))?;
            attcollation = expr_collation(Some(expr_as_expr))?;

            // Strip any top-level COLLATE clause. This ensures that we treat
            // "x COLLATE y" and "(x COLLATE y)" alike. The index expression lives
            // in the `Expr` domain here (`CollateExpr.arg` is an `Expr`), so the
            // strip walks `Expr`s rather than the C `Node *`.
            let mut stripped: &Expr = expr_as_expr;
            while let Expr::CollateExpr(ce) = stripped {
                stripped = ce
                    .arg
                    .as_deref()
                    .expect("ComputeIndexAttrs: CollateExpr with NULL arg");
            }

            if let Some(v) = stripped.as_var() {
                if v.varattno != 0 {
                    // User wrote "(column)" or "(column COLLATE something)".
                    // Treat it like a simple attribute anyway.
                    index_info.ii_IndexAttrNumbers[attn as usize] = v.varattno;
                } else {
                    set_expression_column(mcx, index_info, attn, stripped)?;
                }
            } else {
                set_expression_column(mcx, index_info, attn, stripped)?;
            }
        }

        type_oids[attn as usize] = atttype;

        // Included columns have no collation, no opclass and no ordering
        // options.
        if attn >= nkeycols {
            if !attribute.collation.is_empty() {
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("including column does not support a collation")
                    .into_error());
            }
            if !attribute.opclass.is_empty() {
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("including column does not support an operator class")
                    .into_error());
            }
            if attribute.ordering != SORTBY_DEFAULT {
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("including column does not support ASC/DESC options")
                    .into_error());
            }
            if attribute.nulls_ordering != SORTBY_NULLS_DEFAULT {
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("including column does not support NULLS FIRST/LAST options")
                    .into_error());
            }

            opclass_oids[attn as usize] = InvalidOid;
            opclass_options[attn as usize] = types_tuple::Datum::null();
            col_options[attn as usize] = 0;
            collation_oids[attn as usize] = InvalidOid;
            attn += 1;

            continue;
        }

        // Apply collation override if any. Use of ddl_userid is necessary due to
        // ACL checks therein, and it's safe because collations don't contain
        // opaque expressions.
        if !attribute.collation.is_empty() {
            if OidIsValid(ddl_userid) {
                at_eoxact_guc(false, *ddl_save_nestlevel);
                SetUserIdAndSecContext(ddl_userid, ddl_sec_context);
            }
            attcollation = get_collation_oid(mcx, &name_list(&attribute.collation), false)?;
            if OidIsValid(ddl_userid) {
                SetUserIdAndSecContext(save_userid, save_sec_context);
                *ddl_save_nestlevel = NewGUCNestLevel();
                restrict_search_path::call()?;
            }
        }

        // Check we have a collation iff it's a collatable type. The only
        // expected failures here are (1) COLLATE applied to a noncollatable
        // type, or (2) index expression had an unresolved collation. But we
        // might as well code this to be a complete consistency check.
        if lsyscache::type_is_collatable::call(atttype)? {
            if !OidIsValid(attcollation) {
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_INDETERMINATE_COLLATION)
                    .errmsg("could not determine which collation to use for index expression")
                    .errhint("Use the COLLATE clause to set the collation explicitly.")
                    .into_error());
            }
        } else if OidIsValid(attcollation) {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "collations are not supported by type {}",
                    formattype_seam::format_type_be_owned::call(atttype)?
                ))
                .into_error());
        }

        collation_oids[attn as usize] = attcollation;

        // Identify the opclass to use. Use of ddl_userid is necessary due to
        // ACL checks therein. This is safe despite opclasses containing opaque
        // expressions (specifically, functions), because only superusers can
        // define opclasses.
        if OidIsValid(ddl_userid) {
            at_eoxact_guc(false, *ddl_save_nestlevel);
            SetUserIdAndSecContext(ddl_userid, ddl_sec_context);
        }
        opclass_oids[attn as usize] = ResolveOpClass(
            mcx,
            &attribute.opclass,
            atttype,
            access_method_name,
            access_method_id,
        )?;
        if OidIsValid(ddl_userid) {
            SetUserIdAndSecContext(save_userid, save_sec_context);
            *ddl_save_nestlevel = NewGUCNestLevel();
            restrict_search_path::call()?;
        }

        // Identify the exclusion operator, if any.
        if let Some(idx) = next_excl_idx {
            let opname = exclusion_list_entry(&exclusion_op_names.unwrap()[idx]);
            let opid: Oid;

            // Find the operator --- it must accept the column datatype without
            // runtime coercion (but binary compatibility is OK).
            if OidIsValid(ddl_userid) {
                at_eoxact_guc(false, *ddl_save_nestlevel);
                SetUserIdAndSecContext(ddl_userid, ddl_sec_context);
            }
            opid = compatible_oper_opid(&opname, atttype, atttype, false)?;
            if OidIsValid(ddl_userid) {
                SetUserIdAndSecContext(save_userid, save_sec_context);
                *ddl_save_nestlevel = NewGUCNestLevel();
                restrict_search_path::call()?;
            }

            // Only allow commutative operators to be used in exclusion
            // constraints. If X conflicts with Y, but Y does not conflict with
            // X, bad things will happen.
            if lsyscache::get_commutator::call(opid)? != opid {
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg(format!(
                        "operator {} is not commutative",
                        format_operator_str(opid)?
                    ))
                    .errdetail("Only commutative operators can be used in exclusion constraints.")
                    .into_error());
            }

            // Operator must be a member of the right opfamily, too.
            let opfamily = lsyscache::get_opclass_family::call(opclass_oids[attn as usize])?;
            let strat = lsyscache::get_op_opfamily_strategy::call(opid, opfamily)?;
            if strat == 0 {
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg(format!(
                        "operator {} is not a member of operator family \"{}\"",
                        format_operator_str(opid)?,
                        get_opfamily_name_str(opfamily)?
                    ))
                    .errdetail(
                        "The exclusion operator must be related to the index operator class for the constraint.",
                    )
                    .into_error());
            }

            set_exclusion(index_info, attn as usize, opid, lsyscache::get_opcode::call(opid)?, strat as u16);
            next_excl_idx = Some(idx + 1);
        } else if iswithoutoverlaps {
            let cmptype: CompareType = if attn == nkeycols - 1 {
                COMPARE_OVERLAP
            } else {
                COMPARE_EQ
            };
            let mut opid = InvalidOid;
            let mut strat: StrategyNumber = InvalidStrategy;
            GetOperatorFromCompareType(
                opclass_oids[attn as usize],
                InvalidOid,
                cmptype,
                &mut opid,
                &mut strat,
            )?;
            set_exclusion(index_info, attn as usize, opid, lsyscache::get_opcode::call(opid)?, strat);
        }

        // Set up the per-column options (indoption field). For now, this is zero
        // for any un-ordered index, while ordered indexes have DESC and NULLS
        // FIRST/LAST options.
        col_options[attn as usize] = 0;
        if amcanorder {
            // default ordering is ASC
            if attribute.ordering == SORTBY_DESC {
                col_options[attn as usize] |= INDOPTION_DESC as i16;
            }
            // default null ordering is LAST for ASC, FIRST for DESC
            if attribute.nulls_ordering == SORTBY_NULLS_DEFAULT {
                if attribute.ordering == SORTBY_DESC {
                    col_options[attn as usize] |= INDOPTION_NULLS_FIRST as i16;
                }
            } else if attribute.nulls_ordering == SORTBY_NULLS_FIRST {
                col_options[attn as usize] |= INDOPTION_NULLS_FIRST as i16;
            }
        } else {
            // index AM does not support ordering
            if attribute.ordering != SORTBY_DEFAULT {
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!(
                        "access method \"{access_method_name}\" does not support ASC/DESC options"
                    ))
                    .into_error());
            }
            if attribute.nulls_ordering != SORTBY_NULLS_DEFAULT {
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!(
                        "access method \"{access_method_name}\" does not support NULLS FIRST/LAST options"
                    ))
                    .into_error());
            }
        }

        // Set up the per-column opclass options (attoptions field).
        //
        // C: opclassOptions[attn] = transformRelOptions((Datum) 0,
        //    attribute->opclassopts, NULL, NULL, false, false).
        //
        // `transform_attoptions_byref` builds the per-column `text[]` attoptions
        // varlena image (the bytes `pg_attribute.attoptions` stores) and lowers
        // it onto the `Datum::ByRef` lane `index_create` / `AppendAttributeTuples`
        // consume (the Datum-bridge fix). An empty `opclassopts` is the C
        // `(Datum) 0` (SQL NULL attoptions).
        if !attribute.opclassopts.is_empty() {
            debug_assert!(attn < nkeycols);
            let bytes = crate::transform_attoptions_byref(mcx, &attribute.opclassopts)?;
            opclass_options[attn as usize] =
                types_tuple::Datum::from_byref_bytes_in(mcx, &bytes)?;
        } else {
            opclass_options[attn as usize] = types_tuple::Datum::null();
        }

        attn += 1;
    }

    Ok(())
}

/// Append `expr` to `index_info.ii_Expressions` (marking the column as an
/// expression column with attno 0) and run the planner's mutable-function check
/// on it. Factored out of the two `else` arms in `ComputeIndexAttrs`.
fn set_expression_column<'mcx>(
    mcx: Mcx<'mcx>,
    index_info: &mut IndexInfo<'mcx>,
    attn: i32,
    expr: &Expr<'mcx>,
) -> PgResult<()> {
    index_info.ii_IndexAttrNumbers[attn as usize] = 0; // marks expression
    let expr_val: Expr<'mcx> = expr.clone();
    append_expression(mcx, index_info, expr_val.clone());

    // transformExpr() should already have rejected subqueries / aggregates /
    // window functions, based on the EXPR_KIND_ for an index expression.
    //
    // An expression using mutable functions is probably wrong, since if you
    // aren't going to get the same result for the same data every time, it's not
    // clear what the index entries mean at all.
    if contain_mutable_functions_after_planning(mcx, expr_val)? {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("functions in index expression must be marked IMMUTABLE")
            .into_error());
    }
    Ok(())
}

/// `indexInfo->ii_Expressions = lappend(indexInfo->ii_Expressions, expr)`.
fn append_expression<'mcx>(mcx: Mcx<'mcx>, index_info: &mut IndexInfo<'mcx>, expr: Expr<'mcx>) {
    let list = index_info
        .ii_Expressions
        .get_or_insert_with(|| PgVec::new_in(mcx));
    list.push(expr);
}

/// Allocate the exclusion-op output vectors in `IndexInfo`, one entry per key
/// column (`palloc_array(Oid/uint16, nkeycols)` in the C).
fn alloc_exclusion<'mcx>(mcx: Mcx<'mcx>, index_info: &mut IndexInfo<'mcx>, nkeycols: i32) {
    let n = nkeycols.max(0) as usize;
    let mut ops = PgVec::new_in(mcx);
    let mut procs = PgVec::new_in(mcx);
    let mut strats = PgVec::new_in(mcx);
    for _ in 0..n {
        ops.push(InvalidOid);
        procs.push(InvalidOid);
        strats.push(0u16);
    }
    index_info.ii_ExclusionOps = Some(ops);
    index_info.ii_ExclusionProcs = Some(procs);
    index_info.ii_ExclusionStrats = Some(strats);
}

/// Store the resolved exclusion `(op, proc, strat)` for key column `i`.
fn set_exclusion(index_info: &mut IndexInfo<'_>, i: usize, opid: Oid, proc_oid: Oid, strat: u16) {
    if let Some(ops) = index_info.ii_ExclusionOps.as_mut() {
        ops[i] = opid;
    }
    if let Some(procs) = index_info.ii_ExclusionProcs.as_mut() {
        procs[i] = proc_oid;
    }
    if let Some(strats) = index_info.ii_ExclusionStrats.as_mut() {
        strats[i] = strat;
    }
}

/// One entry of the `exclusionOpNames` list is itself a `List` of `String` value
/// nodes (the operator name); render it as `Vec<String>` for
/// `compatible_oper_opid`.
fn exclusion_list_entry(node: &NodePtr<'_>) -> Vec<String> {
    match node.node_tag() {
        ntag::T_List => name_list_strings(node.expect_list()),
        _ => panic!(
            "ComputeIndexAttrs: exclusionOpNames entry is not a List node (got {:?})",
            node.node_tag()
        ),
    }
}

/// `format_operator(opno)` rendered as an owned `String` for error messages.
fn format_operator_str(opno: Oid) -> PgResult<String> {
    let tmp = mcx::MemoryContext::new("indexcmds:format_operator");
    let s = regproc_seam::format_operator::call(tmp.mcx(), opno)?
        .as_str()
        .to_string();
    Ok(s)
}

/// `get_opfamily_name(opfamily, false)` rendered as an owned `String`.
fn get_opfamily_name_str(opfamily: Oid) -> PgResult<String> {
    let tmp = mcx::MemoryContext::new("indexcmds:get_opfamily_name");
    let name = lsyscache::get_opfamily_name::call(tmp.mcx(), opfamily, false)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();
    Ok(name)
}
