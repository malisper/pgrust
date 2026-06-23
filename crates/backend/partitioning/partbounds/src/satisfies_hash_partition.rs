//! `satisfies_hash_partition(PG_FUNCTION_ARGS)` (partbounds.c) — the
//! SQL-callable function used in hash-partition CHECK constraints.
//!
//! The first three arguments are the parent table OID, modulus, and remainder.
//! The remaining arguments are the values of the partitioning columns (or, when
//! called with `VARIADIC`, a single array argument carrying them); these are
//! hashed and combined with `hash_combine64`. Returns true iff
//! `rowHash % modulus == remainder`.
//!
//! NB: it is important this never returns NULL, as the constraint machinery
//! would treat a NULL result as a "pass" (see C comment). We register the
//! builtin as **non-strict** so the NULL-argument cases are handled here
//! (returning `false`) rather than short-circuited by the strict wrapper.

use mcx::{Mcx, MemoryContext};
use ::types_core::primitive::Oid;
use types_error::{PgError, PgResult};
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData};
use ::nodes::partition::{PartitionKeyData, PartitionStrategy};
use ::types_storage::lock::AccessShareLock;
use types_tuple::heaptuple::Datum;

use crate::{hash_combine64, HASH_PARTITION_SEED};

use common_relation_seams as relation_seams;
use coerce_seams as coerce_seams;
use arrayfuncs_seams as arrayfuncs_seams;
use format_type_seams as format_type_seams;
use lsyscache_seams as lsyscache_seams;
use partcache_seams as partcache_seams;
use fmgr_core as fmgr_core;
use fmgr_seams as fmgr;

/// `errcode(ERRCODE_INVALID_PARAMETER_VALUE)` + `errmsg(msg)`.
fn invalid_parameter(msg: impl Into<String>) -> PgError {
    PgError::error(msg.into()).with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE)
}

/// `format_type_be(oid)` for an error message (best-effort: a lookup failure
/// degrades to the bare OID, never derailing the diagnostic itself).
fn format_type_be(mcx: Mcx<'_>, oid: Oid) -> String {
    match format_type_seams::format_type_be::call(mcx, oid) {
        Ok(s) => s.as_str().to_string(),
        Err(_) => oid.to_string(),
    }
}

/// `DatumGetUInt64(FunctionCall2Coll(&partsupfunc, collation, value, seed))`.
fn call_hash(fn_oid: Oid, collation: Oid, value: Datum, seed: Datum) -> PgResult<u64> {
    let ctx = MemoryContext::new("satisfies_hash_partition");
    let result =
        fmgr::function_call2_coll_datum::call(ctx.mcx(), fn_oid, collation, value, seed)?;
    Ok(result.as_u64())
}

/// Materialize the canonical [`Datum`] for non-variadic key argument `argno`
/// (a by-value type rides its by-value word; a by-reference type rides its
/// on-disk bytes on the by-ref side channel), copied into `mcx` so it outlives
/// the borrow of `fcinfo`.
fn arg_key_datum<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    argno: usize,
    typbyval: bool,
) -> PgResult<Datum<'mcx>> {
    if typbyval {
        let word = fcinfo
            .arg(argno)
            .map(|d| d.value.as_usize())
            .unwrap_or(0);
        Ok(Datum::from_usize(word))
    } else {
        let bytes = fcinfo
            .ref_arg(argno)
            .and_then(|p| {
                p.as_varlena()
                    .or_else(|| p.as_composite())
                    .or_else(|| p.as_cstring().map(|c| c.as_bytes()))
            })
            .ok_or_else(|| {
                PgError::error(format!(
                    "satisfies_hash_partition: by-ref key argument {argno} missing from by-ref lane"
                ))
            })?;
        Datum::from_byref_bytes_in(mcx, bytes)
    }
}

/// The body of `satisfies_hash_partition`. `mcx` is a transient context for the
/// whole call (partition-key build + per-value hashing).
fn body<'mcx>(mcx: Mcx<'mcx>, fcinfo: &FunctionCallInfoBaseData) -> PgResult<Datum<'mcx>> {
    let seed = Datum::from_u64(HASH_PARTITION_SEED);

    // Return false if the parent OID, modulus, or remainder is NULL.
    let a0 = fcinfo.arg(0);
    let a1 = fcinfo.arg(1);
    let a2 = fcinfo.arg(2);
    if a0.is_none_or(|d| d.isnull) || a1.is_none_or(|d| d.isnull) || a2.is_none_or(|d| d.isnull) {
        return Ok(Datum::from_bool(false));
    }
    let parent_id: Oid = a0.unwrap().value.as_oid();
    let modulus: i32 = a1.unwrap().value.as_i32();
    let remainder: i32 = a2.unwrap().value.as_i32();

    // Sanity check modulus and remainder.
    if modulus <= 0 {
        return Err(invalid_parameter(
            "modulus for hash partition must be an integer value greater than zero",
        ));
    }
    if remainder < 0 {
        return Err(invalid_parameter(
            "remainder for hash partition must be an integer value greater than or equal to zero",
        ));
    }
    if remainder >= modulus {
        return Err(invalid_parameter(
            "remainder for hash partition must be less than modulus",
        ));
    }

    // Open parent relation and fetch partition key info.
    let parent = relation_seams::relation_open::call(mcx, parent_id, AccessShareLock)?;
    let rel_name = parent.rd_rel.relname.as_str().to_string();
    let key_box = partcache_seams::relation_get_partition_key::call(mcx, parent)?;

    // Reject parent table that is not hash-partitioned.
    let key: &PartitionKeyData = match key_box.as_deref() {
        Some(k) if k.strategy == PartitionStrategy::Hash => k,
        _ => {
            return Err(invalid_parameter(format!(
                "\"{rel_name}\" is not a hash partitioned table"
            )));
        }
    };

    let partnatts = key.partnatts as usize;
    let variadic = fmgr_core::get_fn_expr_variadic(fcinfo.flinfo.as_deref());

    let mut row_hash: u64 = 0;

    if !variadic {
        let nargs = fcinfo.nargs().saturating_sub(3);

        // Complain if wrong number of column values.
        if partnatts != nargs {
            return Err(invalid_parameter(format!(
                "number of partitioning columns ({partnatts}) does not match number of partition keys provided ({nargs})"
            )));
        }

        // Check argument types and hash each non-NULL value.
        for j in 0..partnatts {
            let argno = j + 3;
            let argtype = fmgr_core::get_fn_expr_argtype(fcinfo.flinfo.as_deref(), argno as i32);
            let parttypid = key.parttypid[j];
            if argtype != parttypid && !coerce_seams::is_binary_coercible::call(argtype, parttypid)? {
                return Err(invalid_parameter(format!(
                    "column {} of the partition key has type {}, but supplied value is of type {}",
                    j + 1,
                    format_type_be(mcx, parttypid),
                    format_type_be(mcx, argtype),
                )));
            }

            // keys start from fourth argument of function.
            if fcinfo.arg(argno).is_none_or(|d| d.isnull) {
                continue;
            }
            let value = arg_key_datum(mcx, fcinfo, argno, key.parttypbyval[j])?;
            let hash = call_hash(
                key.partsupfunc[j].fn_oid,
                key.partcollation[j],
                value,
                seed.clone(),
            )?;
            row_hash = hash_combine64(row_hash, hash);
        }
    } else {
        // VARIADIC: a single array argument carries all the column values.
        if fcinfo.arg(3).is_none_or(|d| d.isnull) {
            // C's PG_GETARG_ARRAYTYPE_P would fail on NULL; the strict-ish
            // contract means a NULL array can't reach here in practice (the
            // planner-built constraint never passes NULL), but mirror the
            // "no values" outcome defensively.
            return Ok(Datum::from_bool((row_hash % modulus as u64) == remainder as u64));
        }
        // C: my_extra->variadic_type = ARR_ELEMTYPE(variadic_array). The array
        // arrives on the by-ref lane as its on-disk varlena image.
        let array_bytes = fcinfo
            .ref_arg(3)
            .and_then(|p| p.as_varlena())
            .ok_or_else(|| {
                PgError::error(
                    "satisfies_hash_partition: VARIADIC array missing from by-ref lane",
                )
            })?;
        let variadic_type = arrayfuncs_seams::array_get_elemtype_bytes::call(mcx, array_bytes)?;
        let tlba = lsyscache_seams::get_typlenbyvalalign::call(variadic_type)?;
        eprintln!(
            "DBG shp: array_bytes.len={} variadic_type={} partnatts={} parttypid={:?}",
            array_bytes.len(),
            variadic_type,
            partnatts,
            (0..partnatts).map(|j| key.parttypid[j]).collect::<Vec<_>>()
        );

        // Check argument types: every key column must match the array element
        // type exactly (the variadic path uses partsupfunc[0]/partcollation[0]).
        for j in 0..partnatts {
            if key.parttypid[j] != variadic_type {
                return Err(invalid_parameter(format!(
                    "column {} of the partition key has type \"{}\", but supplied value is of type \"{}\"",
                    j + 1,
                    format_type_be(mcx, key.parttypid[j]),
                    format_type_be(mcx, variadic_type),
                )));
            }
        }

        // deconstruct_array(variadic_array, ...). `deconstruct_array_values_bytes`
        // detoasts the on-disk image and returns each element as a value-carrying
        // canonical Datum.
        let elems = arrayfuncs_seams::deconstruct_array_values_bytes::call(
            mcx,
            array_bytes,
            variadic_type,
            tlba.typlen,
            tlba.typbyval,
            tlba.typalign as core::ffi::c_char,
        )?;

        // Complain if wrong number of column values.
        if elems.len() != partnatts {
            return Err(invalid_parameter(format!(
                "number of partitioning columns ({}) does not match number of partition keys provided ({})",
                partnatts,
                elems.len()
            )));
        }

        for (datum, isnull) in elems.iter() {
            if *isnull {
                continue;
            }
            let hash = call_hash(
                key.partsupfunc[0].fn_oid,
                key.partcollation[0],
                datum.clone(),
                seed.clone(),
            )?;
            row_hash = hash_combine64(row_hash, hash);
        }
    }

    Ok(Datum::from_bool((row_hash % modulus as u64) == remainder as u64))
}

/// The native fmgr entry point (`PgFnNative`). Returns the bare-word
/// `datum::Datum` the fmgr boundary expects; the by-value boolean result
/// carries no borrow of the per-call context.
fn fc_satisfies_hash_partition(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<datum::Datum> {
    let ctx = MemoryContext::new("satisfies_hash_partition_call");
    let word = body(ctx.mcx(), fcinfo)?.as_usize();
    Ok(datum::Datum::from_usize(word))
}

/// Register the OID-5028 builtin into the native fmgr overlay. Non-strict so
/// the NULL-handling logic in `body` runs (matching the C function, which
/// inspects `PG_ARGISNULL` itself and never returns NULL).
pub fn register() {
    fmgr_core::register_builtins_native([(
        BuiltinFunction {
            foid: 5028,
            name: "satisfies_hash_partition".to_string(),
            // C's fmgr_builtins[] records nargs = 4 (3 fixed + the trailing
            // VARIADIC "any" slot); the function is non-strict (it inspects
            // PG_ARGISNULL itself and must never return NULL).
            nargs: 4,
            strict: false,
            retset: false,
            func: None,
        },
        fc_satisfies_hash_partition as ::fmgr::PgFnNative,
    )]);
}
