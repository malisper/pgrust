//! Query / qual text generation helpers (`ri_triggers.c` "Local functions"):
//! `quoteOneName`, `quoteRelationName`, `ri_GenerateQual`,
//! `ri_GenerateQualCollation`.
//!
//! All identifier/query text is carried as raw bytes (`&[u8]` / `Vec<u8>`), not
//! `String`, so a server-encoded name with bytes >= 0x80 survives verbatim
//! exactly as C's `char *` path does. Buffers grow with `try_reserve`; an OOM
//! becomes the context's `mcx.oom(..)` error (every `palloc` on this path can
//! `ereport(ERROR, ERRCODE_OUT_OF_MEMORY)`).

extern crate alloc;
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::PgResult;

use crate::RelSide;

/// Fallibly append `bytes` to `buf` (reserve-then-extend); OOM → `mcx.oom`.
pub(crate) fn try_extend(mcx: Mcx<'_>, buf: &mut Vec<u8>, bytes: &[u8]) -> PgResult<()> {
    buf.try_reserve(bytes.len()).map_err(|_| mcx.oom(bytes.len()))?;
    buf.extend_from_slice(bytes);
    Ok(())
}

/// Fallibly push one byte.
pub(crate) fn try_push(mcx: Mcx<'_>, buf: &mut Vec<u8>, byte: u8) -> PgResult<()> {
    buf.try_reserve(1).map_err(|_| mcx.oom(1))?;
    buf.push(byte);
    Ok(())
}

/// `quoteOneName` --- append `"name"` (doubling embedded `"`) directly to `buf`.
pub(crate) fn append_quoted_name(mcx: Mcx<'_>, buf: &mut Vec<u8>, name: &[u8]) -> PgResult<()> {
    try_push(mcx, buf, b'"')?;
    for &ch in name {
        if ch == b'"' {
            try_push(mcx, buf, b'"')?;
        }
        try_push(mcx, buf, ch)?;
    }
    try_push(mcx, buf, b'"')
}

/// `quoteOneName` --- the standalone quoted form (operator-clause operand).
pub(crate) fn quote_one_name(mcx: Mcx<'_>, name: &[u8]) -> PgResult<Vec<u8>> {
    let mut buffer = Vec::new();
    append_quoted_name(mcx, &mut buffer, name)?;
    Ok(buffer)
}

/// `quoteRelationName` --- append `"nsp"."rel"` directly to `buf`.
pub(crate) fn append_quoted_relation(
    mcx: Mcx<'_>,
    buf: &mut Vec<u8>,
    rel: RelSide<'_, '_>,
) -> PgResult<()> {
    let nsp = relation_namespace_name(mcx, rel.namespace())?;
    let relname = rel.name_bytes(mcx)?;
    append_quoted_name(mcx, buf, &nsp)?;
    try_push(mcx, buf, b'.')?;
    append_quoted_name(mcx, buf, &relname)
}

/// `get_namespace_name(nspid)` as raw bytes, via the lsyscache seam. A missing
/// namespace is C's NULL — the caller would have caught the relation first, so
/// we surface an empty name (the C `get_namespace_name` callers here are always
/// for a live relation's namespace).
pub(crate) fn relation_namespace_name(mcx: Mcx<'_>, nspid: Oid) -> PgResult<Vec<u8>> {
    match backend_utils_cache_lsyscache_seams::get_namespace_name::call(mcx, nspid)? {
        Some(s) => Ok(s.as_str().as_bytes().to_vec()),
        None => Ok(Vec::new()),
    }
}

/// `ri_GenerateQual` --- append " sep " then `generate_operator_clause(...)`.
pub(crate) fn ri_generate_qual(
    mcx: Mcx<'_>,
    buf: &mut Vec<u8>,
    sep: &str,
    leftop: &[u8],
    leftoptype: Oid,
    opoid: Oid,
    rightop: &[u8],
    rightoptype: Oid,
) -> PgResult<()> {
    try_extend(mcx, buf, b" ")?;
    try_extend(mcx, buf, sep.as_bytes())?;
    try_extend(mcx, buf, b" ")?;
    let clause = backend_utils_adt_ruleutils_seams::generate_operator_clause::call(
        mcx,
        leftop,
        leftoptype,
        opoid,
        rightop,
        rightoptype,
    )?;
    try_extend(mcx, buf, &clause)
}

/// `ri_GenerateQualCollation` --- append a ` COLLATE "nsp"."collname"` spec.
/// Nothing to do for a noncollatable type (`!OidIsValid(collation)`).
pub(crate) fn ri_generate_qual_collation(
    mcx: Mcx<'_>,
    buf: &mut Vec<u8>,
    collation: Oid,
) -> PgResult<()> {
    if collation == InvalidOid {
        return Ok(());
    }
    let (nspname, collname) =
        match backend_utils_cache_syscache_seams::collation_qualified_name::call(mcx, collation)? {
            Some((n, c)) => (n.to_vec(), c.to_vec()),
            None => return Ok(()),
        };
    try_extend(mcx, buf, b" COLLATE ")?;
    append_quoted_name(mcx, buf, &nspname)?;
    try_push(mcx, buf, b'.')?;
    append_quoted_name(mcx, buf, &collname)
}
