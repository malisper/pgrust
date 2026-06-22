//! Polymorphic pseudo-type resolution — `funcapi.c` lines 589–1378.
//!
//! Deduce the actual types behind `anyelement`/`anyarray`/`anyrange`/
//! `anymultirange` (and the `anycompatible*` family) from the function's actual
//! argument types, substitute them into a declared-argument array or a result
//! `TupleDesc`, and classify a type OID into a [`TypeFuncClass`].

use backend_utils_error::ereport;
use types_core::{InvalidOid, Oid, OidIsValid};
use types_error::{
    PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_INTERNAL_ERROR, ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use types_nodes::funcapi::{PolymorphicActuals, TypeFuncClass};
use types_nodes::nodes::Node;
use types_tuple::heaptuple::{
    TupleDesc, ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID, ANYCOMPATIBLENONARRAYOID,
    ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID, ANYMULTIRANGEOID,
    ANYNONARRAYOID, ANYRANGEOID, CSTRINGOID, RECORDOID, VOIDOID,
};

// Seam crates for unported neighbours (routed through each owner).
use backend_utils_adt_format_type_seams as format_type;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_nodes_nodeFuncs_seams as node_funcs;

/// The `call_expr` the funcapi result-type / polymorphic-resolution cluster
/// threads (C's `Node *call_expr`). C uses one `Node *` because its expression
/// nodes ARE plan-tree nodes; this port models `FuncExpr`/`OpExpr` only on the
/// owned `primnodes::Expr` side, so the carrier holds the call expression as an
/// erased field-bearing `Expr` ([`types_fmgr::ExternalFnExpr`]):
///
///   * `get_call_result_type` builds it from the erased `FmgrInfo.fn_expr`
///     (`fcinfo->flinfo->fn_expr`).
///   * `get_expr_result_type` builds it from its `FuncExpr`/`OpExpr` plan-tree
///     `Node` argument (cloning the owned `Expr` into the arena).
///
/// It answers the three inspections the resolver needs — `exprType(call_expr)`,
/// `get_call_expr_argtype(call_expr, i)`, and `exprInputCollation(call_expr)` —
/// so the resolver is written once against this abstraction.
#[derive(Clone)]
pub struct CallExpr {
    external: types_fmgr::ExternalFnExpr,
}

impl CallExpr {
    /// Build a [`CallExpr`] from the erased `FmgrInfo.fn_expr` carrier (the
    /// `get_call_result_type` route). `tag` is unused on this side — the erased
    /// `Expr` carries its own kind.
    pub fn from_erased(erased: types_core::fmgr::FnExprErased) -> Self {
        Self {
            external: types_fmgr::ExternalFnExpr {
                tag: 0,
                node: Some(erased),
            },
        }
    }

    /// Build a [`CallExpr`] from a plan-tree call-expression `Node` (the
    /// `get_expr_result_type` route), erasing its owned `Expr` into the arena so
    /// the `ExternalFnExpr` / `&Expr` seams can read it. A `Node` that is not an
    /// expression yields a tag-only carrier (the C `exprType` fall-through).
    pub fn from_node<'mcx>(mcx: mcx::Mcx<'mcx>, node: &Node<'mcx>) -> PgResult<Self> {
        let tag = node.tag().0;
        let erased = match node.as_expr() {
            Some(e) => Some(types_core::fmgr::FnExprErased::from_node_erased::<
                types_nodes::primnodes::Expr,
                types_nodes::primnodes::Expr,
            >(e.clone_in(mcx)?)),
            None => None,
        };
        Ok(Self {
            external: types_fmgr::ExternalFnExpr { tag, node: erased },
        })
    }

    /// The erased owned `Expr` behind this call expression, if any.
    fn expr(&self) -> Option<&types_nodes::primnodes::Expr<'static>> {
        self.external
            .node
            .as_ref()
            .and_then(|n| n.downcast_ref::<types_nodes::primnodes::Expr>())
    }

    /// `get_call_expr_argtype(call_expr, argnum)` (fmgr.c:1929) — the actual type
    /// OID of argument `argnum`, or `InvalidOid` out of range / unhandled kind.
    pub fn argtype(&self, argnum: i32) -> PgResult<Oid> {
        match self.expr() {
            Some(expr) => node_funcs::get_call_expr_argtype_expr::call(expr, argnum),
            None => Ok(InvalidOid),
        }
    }

    /// `exprType(call_expr)` (nodeFuncs.c) — the result type OID of the call
    /// expression, or `InvalidOid` for an unhandled node kind.
    pub fn result_type(&self) -> Oid {
        node_funcs::expr_type::call(self.external.clone())
    }

    /// `exprInputCollation(call_expr)` (nodeFuncs.c) — the input collation the
    /// call uses, or `InvalidOid`.
    pub fn input_collation(&self) -> Oid {
        match self.expr() {
            Some(expr) => node_funcs::expr_input_collation_expr::call(expr),
            None => InvalidOid,
        }
    }
}

// `get_typtype` returns `pg_type.typtype`; the `TYPTYPE_*` chars (catalog/
// pg_type.h). The lsyscache seam reports them as `u8`.
const TYPTYPE_BASE: u8 = b'b';
const TYPTYPE_COMPOSITE: u8 = b'c';
const TYPTYPE_DOMAIN: u8 = b'd';
const TYPTYPE_ENUM: u8 = b'e';
const TYPTYPE_MULTIRANGE: u8 = b'm';
const TYPTYPE_PSEUDO: u8 = b'p';
const TYPTYPE_RANGE: u8 = b'r';

// `PROARGMODE_*` (catalog/pg_proc.h); the `argmodes[]` bytes
// `resolve_polymorphic_argtypes` switches on.
const PROARGMODE_IN: u8 = b'i';
const PROARGMODE_OUT: u8 = b'o';
const PROARGMODE_TABLE: u8 = b't';

/// C `elog(ERROR, "could not determine polymorphic type")` — `elog` defaults
/// to `ERRCODE_INTERNAL_ERROR` (elog.c).
fn could_not_determine_polymorphic_type() -> types_error::PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INTERNAL_ERROR)
        .errmsg("could not determine polymorphic type")
        .into_error()
}

/// `get_element_type(array_type)` reports `None` for the C `InvalidOid`; the C
/// callers treat that as `InvalidOid`, so flatten it back.
fn get_element_type(array_type: Oid) -> PgResult<Oid> {
    Ok(lsyscache::get_element_type::call(array_type)?.unwrap_or(InvalidOid))
}

/// `get_array_type(input_type)` reports `None` for the C `InvalidOid`.
fn get_array_type(input_type: Oid) -> PgResult<Oid> {
    Ok(lsyscache::get_array_type::call(input_type)?.unwrap_or(InvalidOid))
}

/// `resolve_anyelement_from_others(actuals)` (funcapi.c:589) — derive
/// `anyelement_type` from a known `anyarray`/`anyrange`/`anymultirange` actual
/// (its element type), `ereport`-ing if none is available.
pub fn resolve_anyelement_from_others(actuals: &mut PolymorphicActuals) -> PgResult<()> {
    if OidIsValid(actuals.anyarray_type) {
        // Use the element type corresponding to actual type.
        let array_base_type = lsyscache::get_base_type::call(actuals.anyarray_type)?;
        let array_typelem = get_element_type(array_base_type)?;

        if !OidIsValid(array_typelem) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "argument declared {} is not an array but type {}",
                    "anyarray",
                    format_type::format_type_be_owned::call(array_base_type)?
                ))
                .into_error());
        }
        actuals.anyelement_type = array_typelem;
    } else if OidIsValid(actuals.anyrange_type) {
        // Use the element type corresponding to actual type.
        let range_base_type = lsyscache::get_base_type::call(actuals.anyrange_type)?;
        let range_typelem = lsyscache::get_range_subtype::call(range_base_type)?;

        if !OidIsValid(range_typelem) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "argument declared {} is not a range type but type {}",
                    "anyrange",
                    format_type::format_type_be_owned::call(range_base_type)?
                ))
                .into_error());
        }
        actuals.anyelement_type = range_typelem;
    } else if OidIsValid(actuals.anymultirange_type) {
        // Use the element type based on the multirange type.
        let multirange_base_type = lsyscache::get_base_type::call(actuals.anymultirange_type)?;
        let multirange_typelem = lsyscache::get_multirange_range::call(multirange_base_type)?;
        if !OidIsValid(multirange_typelem) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "argument declared {} is not a multirange type but type {}",
                    "anymultirange",
                    format_type::format_type_be_owned::call(multirange_base_type)?
                ))
                .into_error());
        }

        let range_base_type = lsyscache::get_base_type::call(multirange_typelem)?;
        let range_typelem = lsyscache::get_range_subtype::call(range_base_type)?;

        if !OidIsValid(range_typelem) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "argument declared {} does not contain a range type but type {}",
                    "anymultirange",
                    format_type::format_type_be_owned::call(range_base_type)?
                ))
                .into_error());
        }
        actuals.anyelement_type = range_typelem;
    } else {
        return Err(could_not_determine_polymorphic_type());
    }
    Ok(())
}

/// `resolve_anyarray_from_others(actuals)` (funcapi.c:655) — derive
/// `anyarray_type` from the known `anyelement` actual (its array type).
pub fn resolve_anyarray_from_others(actuals: &mut PolymorphicActuals) -> PgResult<()> {
    // If we don't know ANYELEMENT, resolve that first.
    if !OidIsValid(actuals.anyelement_type) {
        resolve_anyelement_from_others(actuals)?;
    }

    if OidIsValid(actuals.anyelement_type) {
        // Use the array type corresponding to actual type.
        let array_typeid = get_array_type(actuals.anyelement_type)?;

        if !OidIsValid(array_typeid) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "could not find array type for data type {}",
                    format_type::format_type_be_owned::call(actuals.anyelement_type)?
                ))
                .into_error());
        }
        actuals.anyarray_type = array_typeid;
    } else {
        return Err(could_not_determine_polymorphic_type());
    }
    Ok(())
}

/// `resolve_anyrange_from_others(actuals)` (funcapi.c:681) — derive
/// `anyrange_type` from the known `anymultirange` actual (its range type).
///
/// We can't deduce a range type from polymorphic array/base types (multiple
/// range types may share a subtype), but we can from a polymorphic multirange.
pub fn resolve_anyrange_from_others(actuals: &mut PolymorphicActuals) -> PgResult<()> {
    if OidIsValid(actuals.anymultirange_type) {
        // Use the element type based on the multirange type.
        let multirange_base_type = lsyscache::get_base_type::call(actuals.anymultirange_type)?;
        let multirange_typelem = lsyscache::get_multirange_range::call(multirange_base_type)?;

        if !OidIsValid(multirange_typelem) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "argument declared {} is not a multirange type but type {}",
                    "anymultirange",
                    format_type::format_type_be_owned::call(multirange_base_type)?
                ))
                .into_error());
        }
        actuals.anyrange_type = multirange_typelem;
    } else {
        return Err(could_not_determine_polymorphic_type());
    }
    Ok(())
}

/// `resolve_anymultirange_from_others(actuals)` (funcapi.c:710) — derive
/// `anymultirange_type` from the known `anyrange` actual (its multirange type).
///
/// We can't deduce a multirange type from polymorphic array/base types, but we
/// can from a polymorphic range type.
pub fn resolve_anymultirange_from_others(actuals: &mut PolymorphicActuals) -> PgResult<()> {
    if OidIsValid(actuals.anyrange_type) {
        let range_base_type = lsyscache::get_base_type::call(actuals.anyrange_type)?;
        let multirange_typeid = lsyscache::get_range_multirange::call(range_base_type)?;

        if !OidIsValid(multirange_typeid) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "could not find multirange type for data type {}",
                    format_type::format_type_be_owned::call(actuals.anyrange_type)?
                ))
                .into_error());
        }
        actuals.anymultirange_type = multirange_typeid;
    } else {
        return Err(could_not_determine_polymorphic_type());
    }
    Ok(())
}

/// `resolve_polymorphic_tupdesc(tupdesc, declared_args, call_expr)`
/// (funcapi.c:744) — substitute the resolved polymorphic actuals into each
/// polymorphic column of `tupdesc` in place; returns `false` if a substitution
/// could not be determined (C `bool`).
pub fn resolve_polymorphic_tupdesc<'mcx>(
    tupdesc: &mut TupleDesc<'mcx>,
    declared_args: &[Oid],
    call_expr: Option<&CallExpr>,
) -> PgResult<bool> {
    // C: `int natts = tupdesc->natts;` — a non-NULL tupdesc is required.
    let td = tupdesc
        .as_mut()
        .expect("resolve_polymorphic_tupdesc: tupdesc must be non-NULL");
    let natts = td.natts;
    let nargs = declared_args.len() as i32;
    let mut have_polymorphic_result = false;
    let mut have_anyelement_result = false;
    let mut have_anyarray_result = false;
    let mut have_anyrange_result = false;
    let mut have_anymultirange_result = false;
    let mut have_anycompatible_result = false;
    let mut have_anycompatible_array_result = false;
    let mut have_anycompatible_range_result = false;
    let mut have_anycompatible_multirange_result = false;
    let mut poly_actuals = PolymorphicActuals::default();
    let mut anyc_actuals = PolymorphicActuals::default();
    let mut anycollation: Oid = InvalidOid;
    let mut anycompatcollation: Oid = InvalidOid;

    // See if there are any polymorphic outputs; quick out if not.
    for i in 0..natts {
        match td.attr(i as usize).atttypid {
            ANYELEMENTOID | ANYNONARRAYOID | ANYENUMOID => {
                have_polymorphic_result = true;
                have_anyelement_result = true;
            }
            ANYARRAYOID => {
                have_polymorphic_result = true;
                have_anyarray_result = true;
            }
            ANYRANGEOID => {
                have_polymorphic_result = true;
                have_anyrange_result = true;
            }
            ANYMULTIRANGEOID => {
                have_polymorphic_result = true;
                have_anymultirange_result = true;
            }
            ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => {
                have_polymorphic_result = true;
                have_anycompatible_result = true;
            }
            ANYCOMPATIBLEARRAYOID => {
                have_polymorphic_result = true;
                have_anycompatible_array_result = true;
            }
            ANYCOMPATIBLERANGEOID => {
                have_polymorphic_result = true;
                have_anycompatible_range_result = true;
            }
            ANYCOMPATIBLEMULTIRANGEOID => {
                have_polymorphic_result = true;
                have_anycompatible_multirange_result = true;
            }
            _ => {}
        }
    }
    if !have_polymorphic_result {
        return Ok(true);
    }

    /*
     * Otherwise, extract actual datatype(s) from input arguments.  (We assume
     * the parser already validated consistency of the arguments.  Also, for
     * the ANYCOMPATIBLE pseudotype family, we expect that all matching
     * arguments were coerced to the selected common supertype, so that it
     * doesn't matter which one's exposed type we look at.)
     */
    let call_expr = match call_expr {
        Some(e) => e,
        None => return Ok(false), // no hope
    };

    for i in 0..nargs {
        match declared_args[i as usize] {
            ANYELEMENTOID | ANYNONARRAYOID | ANYENUMOID => {
                if !OidIsValid(poly_actuals.anyelement_type) {
                    poly_actuals.anyelement_type =
                        call_expr.argtype(i)?;
                    if !OidIsValid(poly_actuals.anyelement_type) {
                        return Ok(false);
                    }
                }
            }
            ANYARRAYOID => {
                if !OidIsValid(poly_actuals.anyarray_type) {
                    poly_actuals.anyarray_type =
                        call_expr.argtype(i)?;
                    if !OidIsValid(poly_actuals.anyarray_type) {
                        return Ok(false);
                    }
                }
            }
            ANYRANGEOID => {
                if !OidIsValid(poly_actuals.anyrange_type) {
                    poly_actuals.anyrange_type =
                        call_expr.argtype(i)?;
                    if !OidIsValid(poly_actuals.anyrange_type) {
                        return Ok(false);
                    }
                }
            }
            ANYMULTIRANGEOID => {
                if !OidIsValid(poly_actuals.anymultirange_type) {
                    poly_actuals.anymultirange_type =
                        call_expr.argtype(i)?;
                    if !OidIsValid(poly_actuals.anymultirange_type) {
                        return Ok(false);
                    }
                }
            }
            ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => {
                if !OidIsValid(anyc_actuals.anyelement_type) {
                    anyc_actuals.anyelement_type =
                        call_expr.argtype(i)?;
                    if !OidIsValid(anyc_actuals.anyelement_type) {
                        return Ok(false);
                    }
                }
            }
            ANYCOMPATIBLEARRAYOID => {
                if !OidIsValid(anyc_actuals.anyarray_type) {
                    anyc_actuals.anyarray_type =
                        call_expr.argtype(i)?;
                    if !OidIsValid(anyc_actuals.anyarray_type) {
                        return Ok(false);
                    }
                }
            }
            ANYCOMPATIBLERANGEOID => {
                if !OidIsValid(anyc_actuals.anyrange_type) {
                    anyc_actuals.anyrange_type =
                        call_expr.argtype(i)?;
                    if !OidIsValid(anyc_actuals.anyrange_type) {
                        return Ok(false);
                    }
                }
            }
            ANYCOMPATIBLEMULTIRANGEOID => {
                if !OidIsValid(anyc_actuals.anymultirange_type) {
                    anyc_actuals.anymultirange_type =
                        call_expr.argtype(i)?;
                    if !OidIsValid(anyc_actuals.anymultirange_type) {
                        return Ok(false);
                    }
                }
            }
            _ => {}
        }
    }

    // If needed, deduce one polymorphic type from others.
    if have_anyelement_result && !OidIsValid(poly_actuals.anyelement_type) {
        resolve_anyelement_from_others(&mut poly_actuals)?;
    }
    if have_anyarray_result && !OidIsValid(poly_actuals.anyarray_type) {
        resolve_anyarray_from_others(&mut poly_actuals)?;
    }
    if have_anyrange_result && !OidIsValid(poly_actuals.anyrange_type) {
        resolve_anyrange_from_others(&mut poly_actuals)?;
    }
    if have_anymultirange_result && !OidIsValid(poly_actuals.anymultirange_type) {
        resolve_anymultirange_from_others(&mut poly_actuals)?;
    }
    if have_anycompatible_result && !OidIsValid(anyc_actuals.anyelement_type) {
        resolve_anyelement_from_others(&mut anyc_actuals)?;
    }
    if have_anycompatible_array_result && !OidIsValid(anyc_actuals.anyarray_type) {
        resolve_anyarray_from_others(&mut anyc_actuals)?;
    }
    if have_anycompatible_range_result && !OidIsValid(anyc_actuals.anyrange_type) {
        resolve_anyrange_from_others(&mut anyc_actuals)?;
    }
    if have_anycompatible_multirange_result && !OidIsValid(anyc_actuals.anymultirange_type) {
        resolve_anymultirange_from_others(&mut anyc_actuals)?;
    }

    /*
     * Identify the collation to use for polymorphic OUT parameters. (It'll
     * necessarily be the same for both anyelement and anyarray, likewise for
     * anycompatible and anycompatiblearray.)  Note that range types are not
     * collatable, so any possible internal collation of a range type is not
     * considered here.
     */
    if OidIsValid(poly_actuals.anyelement_type) {
        anycollation = lsyscache::get_typcollation::call(poly_actuals.anyelement_type)?;
    } else if OidIsValid(poly_actuals.anyarray_type) {
        anycollation = lsyscache::get_typcollation::call(poly_actuals.anyarray_type)?;
    }

    if OidIsValid(anyc_actuals.anyelement_type) {
        anycompatcollation = lsyscache::get_typcollation::call(anyc_actuals.anyelement_type)?;
    } else if OidIsValid(anyc_actuals.anyarray_type) {
        anycompatcollation = lsyscache::get_typcollation::call(anyc_actuals.anyarray_type)?;
    }

    if OidIsValid(anycollation) || OidIsValid(anycompatcollation) {
        /*
         * The types are collatable, so consider whether to use a nondefault
         * collation.  We do so if we can identify the input collation used
         * for the function.
         */
        let inputcollation = call_expr.input_collation();

        if OidIsValid(inputcollation) {
            if OidIsValid(anycollation) {
                anycollation = inputcollation;
            }
            if OidIsValid(anycompatcollation) {
                anycompatcollation = inputcollation;
            }
        }
    }

    // And finally replace the tuple column types as needed.
    for i in 0..natts {
        let atttypid = td.attr(i as usize).atttypid;
        // C: NameStr(att->attname) — the descriptor's column name.
        let attname_bytes = td.attr(i as usize).attname.name_str().to_vec();
        let attname = core::str::from_utf8(&attname_bytes).ok();
        let attno = (i + 1) as types_core::AttrNumber;

        match atttypid {
            ANYELEMENTOID | ANYNONARRAYOID | ANYENUMOID => {
                backend_access_common_tupdesc::TupleDescInitEntry(
                    td,
                    attno,
                    attname,
                    poly_actuals.anyelement_type,
                    -1,
                    0,
                )?;
                backend_access_common_tupdesc::TupleDescInitEntryCollation(td, attno, anycollation)?;
            }
            ANYARRAYOID => {
                backend_access_common_tupdesc::TupleDescInitEntry(
                    td,
                    attno,
                    attname,
                    poly_actuals.anyarray_type,
                    -1,
                    0,
                )?;
                backend_access_common_tupdesc::TupleDescInitEntryCollation(td, attno, anycollation)?;
            }
            ANYRANGEOID => {
                backend_access_common_tupdesc::TupleDescInitEntry(
                    td,
                    attno,
                    attname,
                    poly_actuals.anyrange_type,
                    -1,
                    0,
                )?;
                // no collation should be attached to a range type
            }
            ANYMULTIRANGEOID => {
                backend_access_common_tupdesc::TupleDescInitEntry(
                    td,
                    attno,
                    attname,
                    poly_actuals.anymultirange_type,
                    -1,
                    0,
                )?;
                // no collation should be attached to a multirange type
            }
            ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => {
                backend_access_common_tupdesc::TupleDescInitEntry(
                    td,
                    attno,
                    attname,
                    anyc_actuals.anyelement_type,
                    -1,
                    0,
                )?;
                backend_access_common_tupdesc::TupleDescInitEntryCollation(
                    td,
                    attno,
                    anycompatcollation,
                )?;
            }
            ANYCOMPATIBLEARRAYOID => {
                backend_access_common_tupdesc::TupleDescInitEntry(
                    td,
                    attno,
                    attname,
                    anyc_actuals.anyarray_type,
                    -1,
                    0,
                )?;
                backend_access_common_tupdesc::TupleDescInitEntryCollation(
                    td,
                    attno,
                    anycompatcollation,
                )?;
            }
            ANYCOMPATIBLERANGEOID => {
                backend_access_common_tupdesc::TupleDescInitEntry(
                    td,
                    attno,
                    attname,
                    anyc_actuals.anyrange_type,
                    -1,
                    0,
                )?;
                // no collation should be attached to a range type
            }
            ANYCOMPATIBLEMULTIRANGEOID => {
                backend_access_common_tupdesc::TupleDescInitEntry(
                    td,
                    attno,
                    attname,
                    anyc_actuals.anymultirange_type,
                    -1,
                    0,
                )?;
                // no collation should be attached to a multirange type
            }
            _ => {}
        }
    }

    Ok(true)
}

/// `resolve_polymorphic_argtypes(numargs, argtypes, argmodes, call_expr)`
/// (funcapi.c:1064) — two-pass substitution of the polymorphic entries of the
/// `argtypes` array (per `argmodes`) from the call's actual argument types;
/// returns `false` if resolution failed (C `bool`).
pub fn resolve_polymorphic_argtypes(
    argtypes: &mut [Oid],
    argmodes: Option<&[u8]>,
    call_expr: Option<&CallExpr>,
) -> PgResult<bool> {
    let numargs = argtypes.len();
    let mut have_polymorphic_result = false;
    let mut have_anyelement_result = false;
    let mut have_anyarray_result = false;
    let mut have_anyrange_result = false;
    let mut have_anymultirange_result = false;
    let mut have_anycompatible_result = false;
    let mut have_anycompatible_array_result = false;
    let mut have_anycompatible_range_result = false;
    let mut have_anycompatible_multirange_result = false;
    let mut poly_actuals = PolymorphicActuals::default();
    let mut anyc_actuals = PolymorphicActuals::default();
    let mut inargno: i32 = 0;

    /*
     * First pass: resolve polymorphic inputs, check for outputs.  As in
     * resolve_polymorphic_tupdesc, we rely on the parser to have enforced
     * type consistency and coerced ANYCOMPATIBLE args to a common supertype.
     */
    for i in 0..numargs {
        let argmode = match argmodes {
            Some(m) => m[i],
            None => PROARGMODE_IN,
        };
        let is_out = argmode == PROARGMODE_OUT || argmode == PROARGMODE_TABLE;

        match argtypes[i] {
            ANYELEMENTOID | ANYNONARRAYOID | ANYENUMOID => {
                if is_out {
                    have_polymorphic_result = true;
                    have_anyelement_result = true;
                } else {
                    if !OidIsValid(poly_actuals.anyelement_type) {
                        poly_actuals.anyelement_type = call_expr_argtype(call_expr, inargno)?;
                        if !OidIsValid(poly_actuals.anyelement_type) {
                            return Ok(false);
                        }
                    }
                    argtypes[i] = poly_actuals.anyelement_type;
                }
            }
            ANYARRAYOID => {
                if is_out {
                    have_polymorphic_result = true;
                    have_anyarray_result = true;
                } else {
                    if !OidIsValid(poly_actuals.anyarray_type) {
                        poly_actuals.anyarray_type = call_expr_argtype(call_expr, inargno)?;
                        if !OidIsValid(poly_actuals.anyarray_type) {
                            return Ok(false);
                        }
                    }
                    argtypes[i] = poly_actuals.anyarray_type;
                }
            }
            ANYRANGEOID => {
                if is_out {
                    have_polymorphic_result = true;
                    have_anyrange_result = true;
                } else {
                    if !OidIsValid(poly_actuals.anyrange_type) {
                        poly_actuals.anyrange_type = call_expr_argtype(call_expr, inargno)?;
                        if !OidIsValid(poly_actuals.anyrange_type) {
                            return Ok(false);
                        }
                    }
                    argtypes[i] = poly_actuals.anyrange_type;
                }
            }
            ANYMULTIRANGEOID => {
                if is_out {
                    have_polymorphic_result = true;
                    have_anymultirange_result = true;
                } else {
                    if !OidIsValid(poly_actuals.anymultirange_type) {
                        poly_actuals.anymultirange_type = call_expr_argtype(call_expr, inargno)?;
                        if !OidIsValid(poly_actuals.anymultirange_type) {
                            return Ok(false);
                        }
                    }
                    argtypes[i] = poly_actuals.anymultirange_type;
                }
            }
            ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => {
                if is_out {
                    have_polymorphic_result = true;
                    have_anycompatible_result = true;
                } else {
                    if !OidIsValid(anyc_actuals.anyelement_type) {
                        anyc_actuals.anyelement_type = call_expr_argtype(call_expr, inargno)?;
                        if !OidIsValid(anyc_actuals.anyelement_type) {
                            return Ok(false);
                        }
                    }
                    argtypes[i] = anyc_actuals.anyelement_type;
                }
            }
            ANYCOMPATIBLEARRAYOID => {
                if is_out {
                    have_polymorphic_result = true;
                    have_anycompatible_array_result = true;
                } else {
                    if !OidIsValid(anyc_actuals.anyarray_type) {
                        anyc_actuals.anyarray_type = call_expr_argtype(call_expr, inargno)?;
                        if !OidIsValid(anyc_actuals.anyarray_type) {
                            return Ok(false);
                        }
                    }
                    argtypes[i] = anyc_actuals.anyarray_type;
                }
            }
            ANYCOMPATIBLERANGEOID => {
                if is_out {
                    have_polymorphic_result = true;
                    have_anycompatible_range_result = true;
                } else {
                    if !OidIsValid(anyc_actuals.anyrange_type) {
                        anyc_actuals.anyrange_type = call_expr_argtype(call_expr, inargno)?;
                        if !OidIsValid(anyc_actuals.anyrange_type) {
                            return Ok(false);
                        }
                    }
                    argtypes[i] = anyc_actuals.anyrange_type;
                }
            }
            ANYCOMPATIBLEMULTIRANGEOID => {
                if is_out {
                    have_polymorphic_result = true;
                    have_anycompatible_multirange_result = true;
                } else {
                    if !OidIsValid(anyc_actuals.anymultirange_type) {
                        anyc_actuals.anymultirange_type = call_expr_argtype(call_expr, inargno)?;
                        if !OidIsValid(anyc_actuals.anymultirange_type) {
                            return Ok(false);
                        }
                    }
                    argtypes[i] = anyc_actuals.anymultirange_type;
                }
            }
            _ => {}
        }
        if argmode != PROARGMODE_OUT && argmode != PROARGMODE_TABLE {
            inargno += 1;
        }
    }

    // Done?
    if !have_polymorphic_result {
        return Ok(true);
    }

    // If needed, deduce one polymorphic type from others.
    if have_anyelement_result && !OidIsValid(poly_actuals.anyelement_type) {
        resolve_anyelement_from_others(&mut poly_actuals)?;
    }
    if have_anyarray_result && !OidIsValid(poly_actuals.anyarray_type) {
        resolve_anyarray_from_others(&mut poly_actuals)?;
    }
    if have_anyrange_result && !OidIsValid(poly_actuals.anyrange_type) {
        resolve_anyrange_from_others(&mut poly_actuals)?;
    }
    if have_anymultirange_result && !OidIsValid(poly_actuals.anymultirange_type) {
        resolve_anymultirange_from_others(&mut poly_actuals)?;
    }
    if have_anycompatible_result && !OidIsValid(anyc_actuals.anyelement_type) {
        resolve_anyelement_from_others(&mut anyc_actuals)?;
    }
    if have_anycompatible_array_result && !OidIsValid(anyc_actuals.anyarray_type) {
        resolve_anyarray_from_others(&mut anyc_actuals)?;
    }
    if have_anycompatible_range_result && !OidIsValid(anyc_actuals.anyrange_type) {
        resolve_anyrange_from_others(&mut anyc_actuals)?;
    }
    if have_anycompatible_multirange_result && !OidIsValid(anyc_actuals.anymultirange_type) {
        resolve_anymultirange_from_others(&mut anyc_actuals)?;
    }

    // And finally replace the output column types as needed.
    for i in 0..numargs {
        match argtypes[i] {
            ANYELEMENTOID | ANYNONARRAYOID | ANYENUMOID => {
                argtypes[i] = poly_actuals.anyelement_type;
            }
            ANYARRAYOID => argtypes[i] = poly_actuals.anyarray_type,
            ANYRANGEOID => argtypes[i] = poly_actuals.anyrange_type,
            ANYMULTIRANGEOID => argtypes[i] = poly_actuals.anymultirange_type,
            ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => {
                argtypes[i] = anyc_actuals.anyelement_type;
            }
            ANYCOMPATIBLEARRAYOID => argtypes[i] = anyc_actuals.anyarray_type,
            ANYCOMPATIBLERANGEOID => argtypes[i] = anyc_actuals.anyrange_type,
            ANYCOMPATIBLEMULTIRANGEOID => argtypes[i] = anyc_actuals.anymultirange_type,
            _ => {}
        }
    }

    Ok(true)
}

/// `get_type_func_class(typid, base_typeid)` (funcapi.c:1328) — classify a type
/// OID into a [`TypeFuncClass`] (the `get_typtype` switch); for a domain also
/// yields the resolved base type OID.
pub fn get_type_func_class(typid: Oid) -> PgResult<(TypeFuncClass, Oid)> {
    // C: *base_typeid = typid;
    let mut base_typeid = typid;
    let mut typid = typid;

    match lsyscache::get_typtype::call(typid)? {
        TYPTYPE_COMPOSITE => Ok((TypeFuncClass::Composite, base_typeid)),
        TYPTYPE_BASE | TYPTYPE_ENUM | TYPTYPE_RANGE | TYPTYPE_MULTIRANGE => {
            Ok((TypeFuncClass::Scalar, base_typeid))
        }
        TYPTYPE_DOMAIN => {
            // *base_typeid = typid = getBaseType(typid);
            base_typeid = lsyscache::get_base_type::call(typid)?;
            typid = base_typeid;
            if lsyscache::get_typtype::call(typid)? == TYPTYPE_COMPOSITE {
                Ok((TypeFuncClass::CompositeDomain, base_typeid))
            } else {
                // domain base type can't be a pseudotype
                Ok((TypeFuncClass::Scalar, base_typeid))
            }
        }
        TYPTYPE_PSEUDO => {
            if typid == RECORDOID {
                Ok((TypeFuncClass::Record, base_typeid))
            } else if typid == VOIDOID || typid == CSTRINGOID {
                /*
                 * We treat VOID and CSTRING as legitimate scalar datatypes,
                 * mostly for the convenience of the JDBC driver.
                 */
                Ok((TypeFuncClass::Scalar, base_typeid))
            } else {
                Ok((TypeFuncClass::Other, base_typeid))
            }
        }
        // shouldn't get here, probably
        _ => Ok((TypeFuncClass::Other, base_typeid)),
    }
}

/// `get_call_expr_argtype(call_expr, argnum)` (fmgr.c:1929) — the actual type
/// OID of argument `argnum` of the call expression, or `InvalidOid`. C
/// `if (expr == NULL) return InvalidOid;` guard, then the `IsA` dispatch (carried
/// by [`CallExpr::argtype`]). `Err` carries the `get_base_element_type` /
/// `exprType` cache-lookup `ereport` the `ScalarArrayOpExpr` element-type hack
/// can raise.
fn call_expr_argtype(call_expr: Option<&CallExpr>, argnum: i32) -> PgResult<Oid> {
    match call_expr {
        None => Ok(InvalidOid),
        Some(expr) => expr.argtype(argnum),
    }
}
