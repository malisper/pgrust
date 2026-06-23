//! Typed by-OID dispatch for the GIN `anyarray_ops` opclass support procedures
//! (`ginarrayproc.c`).
//!
//! The GIN access method resolves each opclass support function into a
//! `GinState` / `GinScanKey` `FmgrInfo` slot (`index_getprocinfo` →
//! `fmgr_info`), and the build / scan machinery invokes them through the
//! `ginutil-seams` (`gin_extract_value` / `gin_extract_query`) and
//! `core-probe-seams` (`gin_consistent_call_bool` / `gin_consistent_call_tri`)
//! seams. Those support procs are `prolang => internal` functions whose
//! out-parameters (`*nentries`, `**nullFlags`, `*searchMode`, `*recheck`) cannot
//! cross the by-word fmgr `Datum` lane.
//!
//! The faithful re-model — exactly the proven typanalyze (`array_typanalyze`),
//! BRIN (`F_BRIN_MINMAX_*`) and GiST (`F_GIST_*`) opclass-dispatch idiom — is a
//! TYPED dispatch keyed on the resolved support-proc OID (`FmgrInfo::fn_oid` /
//! `GinScanKey::*_fmgr_oid`, the `index_getprocinfo` row). This module installs
//! the four GIN dispatch seams over the `anyarray_ops` OIDs and routes each to
//! the ported [`crate::ginarrayproc`] body, passing the real Rust references the
//! C out-parameters stand for. The element keys travel as the canonical unified
//! `::types_tuple::Datum` value (via `deconstruct_array_v`), so by-reference
//! element types (`text[]`, …) carry real `ByRef` bytes, not a dangling word.
//!
//! For `fmgr_info` to resolve the `internal`-language prosrc names at all (else
//! `CREATE INDEX ... USING gin` errors `internal function "ginarrayextract" is
//! not in internal lookup table`), the `fmgr_builtins[]` rows are registered in
//! [`crate::fmgr_builtins`]; their `fn_addr` is structurally never reached (the
//! AM dispatches by `fn_oid`), mirroring the GiST opclass.
//!
//! The `anyarray_ops`, `tsvector_ops`, and `jsonb_ops` / `jsonb_path_ops`
//! support procs are all wired here; any other opclass support-proc OID bottoms
//! out loudly (a user-defined opclass would need a `Datum::Internal` fmgr arm).

use mcx::{Mcx, PgVec};
use ::types_core::primitive::Oid;
use types_error::{PgError, PgResult};
use types_tuple::heaptuple::Datum;

use core_probe_seams::{gin_consistent_call_bool, gin_consistent_call_tri};
use ginutil_seams::{
    gin_compare_partial, gin_extract_query, gin_extract_value, GinExtractQueryResult,
};
use arrayfuncs_seams as arrayfuncs_seams;
use lsyscache_seams as lsyscache_seams;

use gin::{GinTernaryValue, GIN_CAT_NULL_KEY};

use crate::ginarrayproc::{
    self, GinContainedStrategy, GinContainsStrategy, GinEqualStrategy, GinOverlapStrategy,
};
use crate::ginlogic::GinScanKey;

// GIN search modes (gin.h) — the `*searchMode` values `ginqueryarrayextract`
// selects per strategy.
const GIN_SEARCH_MODE_DEFAULT: i32 = 0;
const GIN_SEARCH_MODE_INCLUDE_EMPTY: i32 = 1;
const GIN_SEARCH_MODE_ALL: i32 = 2;

// pg_proc OIDs (fmgroids.h / pg_proc.dat) of the `anyarray_ops` GIN support
// procedures. These are the values `index_getprocinfo` records in the resolved
// `FmgrInfo::fn_oid` for the array opclass.
/// `ginarrayextract(anyarray, internal, internal)` — `extractValue`.
pub const F_GINARRAYEXTRACT: u32 = 2743;
/// `ginarrayextract(anyarray, internal)` — legacy two-arg `extractValue`.
pub const F_GINARRAYEXTRACT_2ARGS: u32 = 3076;
/// `ginqueryarrayextract(...)` — `extractQuery`.
pub const F_GINQUERYARRAYEXTRACT: u32 = 2774;
/// `ginarrayconsistent(...)` — boolean `consistent`.
pub const F_GINARRAYCONSISTENT: u32 = 2744;
/// `ginarraytriconsistent(...)` — ternary `triConsistent`.
pub const F_GINARRAYTRICONSISTENT: u32 = 3920;

// pg_proc OIDs (fmgroids.h / pg_proc.dat) of the `tsvector_ops` GIN support
// procedures (`tsginidx.c`). The default `gin/tsvector_ops` opclass
// (pg_amproc.dat) resolves amprocnum 2/3/4/6 to OIDs 3656/3657/3658/3921; the
// older / back-compat declarations (3077/3087/3088, 3791/3792) share the same
// bodies and are accepted too (a reloaded pre-9.1 contrib/tsearch2 opclass, or
// an opclass that pins the `_oldsig` rows).
/// `gin_extract_tsvector(tsvector, internal, internal)` — `extractValue`.
pub const F_GIN_EXTRACT_TSVECTOR: u32 = 3656;
/// `gin_extract_tsvector(tsvector, internal)` — legacy two-arg `extractValue`.
pub const F_GIN_EXTRACT_TSVECTOR_2ARGS: u32 = 3077;
/// `gin_extract_tsquery(tsvector, internal, int2, ...)` — `extractQuery`.
pub const F_GIN_EXTRACT_TSQUERY: u32 = 3657;
/// `gin_extract_tsquery(tsquery, internal, int2, ...)` legacy 5-arg.
pub const F_GIN_EXTRACT_TSQUERY_5ARGS: u32 = 3087;
/// `gin_extract_tsquery(tsquery, internal, int2, ...)` old-signature stub.
pub const F_GIN_EXTRACT_TSQUERY_OLDSIG: u32 = 3791;
/// `gin_tsquery_consistent(internal, int2, tsvector, int4, ...)` — `consistent`.
pub const F_GIN_TSQUERY_CONSISTENT: u32 = 3658;
/// `gin_tsquery_consistent(internal, int2, tsquery, int4, ...)` legacy 6-arg.
pub const F_GIN_TSQUERY_CONSISTENT_6ARGS: u32 = 3088;
/// `gin_tsquery_consistent(internal, int2, tsquery, int4, ...)` old-signature.
pub const F_GIN_TSQUERY_CONSISTENT_OLDSIG: u32 = 3792;
/// `gin_tsquery_triconsistent(...)` — ternary `triConsistent`.
pub const F_GIN_TSQUERY_TRICONSISTENT: u32 = 3921;
/// `gin_cmp_prefix(text, text, int2, internal)` — tsvector_ops `comparePartial`.
pub const F_GIN_CMP_PREFIX: u32 = 2700;

// pg_proc.dat OIDs of the `jsonb_ops` / `jsonb_path_ops` GIN support procedures
// (`jsonb_gin.c`). These are the values `index_getprocinfo` records in the
// resolved support-proc `FmgrInfo::fn_oid` for the jsonb opclasses. The compare
// proc (3480, amprocnum 1) is dispatched through the by-OID fmgr path
// (`gin_compare_entries` → `function_call2_coll_datum`), not here.
/// `gin_extract_jsonb(jsonb, internal, internal)` — jsonb_ops `extractValue`.
pub const F_GIN_EXTRACT_JSONB: u32 = 3482;
/// `gin_extract_jsonb_query(...)` — jsonb_ops `extractQuery`.
pub const F_GIN_EXTRACT_JSONB_QUERY: u32 = 3483;
/// `gin_consistent_jsonb(...)` — jsonb_ops boolean `consistent`.
pub const F_GIN_CONSISTENT_JSONB: u32 = 3484;
/// `gin_triconsistent_jsonb(...)` — jsonb_ops ternary `triConsistent`.
pub const F_GIN_TRICONSISTENT_JSONB: u32 = 3488;
/// `gin_extract_jsonb_path(jsonb, internal, internal)` — jsonb_path_ops
/// `extractValue`.
pub const F_GIN_EXTRACT_JSONB_PATH: u32 = 3485;
/// `gin_extract_jsonb_query_path(...)` — jsonb_path_ops `extractQuery`.
pub const F_GIN_EXTRACT_JSONB_QUERY_PATH: u32 = 3486;
/// `gin_consistent_jsonb_path(...)` — jsonb_path_ops boolean `consistent`.
pub const F_GIN_CONSISTENT_JSONB_PATH: u32 = 3487;
/// `gin_triconsistent_jsonb_path(...)` — jsonb_path_ops ternary `triConsistent`.
pub const F_GIN_TRICONSISTENT_JSONB_PATH: u32 = 3489;

// jsonb GIN strategy numbers (jsonb.h) — selecting the query argument variant.
const JSONB_CONTAINS_STRATEGY: u16 = 7;
const JSONB_EXISTS_STRATEGY: u16 = 9;
const JSONB_EXISTS_ANY_STRATEGY: u16 = 10;
const JSONB_EXISTS_ALL_STRATEGY: u16 = 11;
const JSONB_JSONPATH_EXISTS_STRATEGY: u16 = 15;
const JSONB_JSONPATH_PREDICATE_STRATEGY: u16 = 16;

/// `VARHDRSZ` — the 4-byte varlena length header. The canonical by-ref `Datum`
/// image of a jsonb / text / text[] value includes this header; the jsonb_gin
/// value cores want the payload after it (the jsonb `JsonbContainer` root, the
/// `text` key string), while the jsonpath cores want the *whole* varlena (their
/// `jsonpath_is_lax` / `jspInit` read the header word at bytes 4..8).
const VARHDRSZ: usize = 4;

/// `VARDATA_ANY(ptr)` for an inline (non-compressed, non-external) varlena image:
/// skip ONE header byte for a short (1-byte, low-bit-set) header, else `VARHDRSZ`.
/// GIN index keys / args (text, jsonb) reach this dispatch as already-detoasted
/// by-ref images, but detoast does NOT convert a short inline header to long — so
/// a small stored value arrives short-headed once `SHORT_VARLENA_PACKING` is on,
/// where a fixed 4-byte strip would drop three payload bytes. No-op while off.
#[inline]
fn vardata_any(image: &[u8]) -> &[u8] {
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= VARHDRSZ => &image[VARHDRSZ..],
        _ => &[],
    }
}

/// Un-pack a (possibly short-headed) inline varlena `image` to the canonical
/// 4-byte-header form, allocating the rewritten image in `mcx` when needed.
///
/// The `tsvector`/`tsquery` opclass bodies (`gin_extract_tsvector`,
/// `gin_extract_tsquery`, `gin_tsquery_consistent`) read the datum's `size` field
/// at the fixed offset 4 and walk the `WordEntry`/`QueryItem` arrays at
/// `DATAHDRSIZE`/`HDRSIZETQ`-relative offsets — i.e. they require the image to
/// carry a 4-byte header. C's `PG_GETARG_TSVECTOR`/`PG_GETARG_TSQUERY` are
/// `PG_DETOAST_DATUM`, which un-packs a short header to the 4-byte form; the GIN
/// values reach this dispatch already-detoasted (no compressed/external image),
/// but detoast leaves a short header packed, so a small stored value arrives
/// short-headed once `SHORT_VARLENA_PACKING` is on. Mirror the un-pack here.
/// No-op (returns the input slice) while the flag is off — every stored value is
/// already 4B.
#[inline]
fn unpack_short_to_4b<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> PgResult<::mcx::PgVec<'mcx, u8>> {
    // VARATT_IS_1B (short header) and not VARATT_IS_1B_E (0x01, external).
    let short = matches!(image.first(), Some(&h) if h != 0x01 && (h & 0x01) == 0x01);
    if short {
        // VARSIZE_1B = (va_header >> 1) & 0x7F covers the 1-byte header + payload.
        let total_1b = ((image[0] >> 1) & 0x7F) as usize;
        let data_size = total_1b.saturating_sub(1);
        let new_size = data_size + VARHDRSZ;
        let mut out: ::mcx::PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, new_size)?;
        // SET_VARSIZE(out, new_size): a plain 4-byte header (low 2 bits 00).
        let word = (new_size as u32) << 2;
        for &b in &word.to_ne_bytes() {
            out.push(b);
        }
        for &b in &image[1..1 + data_size] {
            out.push(b);
        }
        Ok(out)
    } else {
        let mut out: ::mcx::PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, image.len())?;
        for &b in image {
            out.push(b);
        }
        Ok(out)
    }
}

/// Encode `gin_extract_tsquery`'s `map_item_operand` (the `int *` map the C code
/// stores in every `extra_data[]` slot) as a native-endian `i32` byte blob, the
/// per-key opclass-private `extra_data` the GIN scan carries from `extractQuery`
/// through to `consistent` (`extra_data[0]`). The encoding is symmetric with
/// [`decode_map_item_operand`].
fn encode_map_item_operand(map: &[i32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(map.len() * 4);
    for &v in map {
        bytes.extend_from_slice(&v.to_ne_bytes());
    }
    bytes
}

/// Decode the `extra_data[0]` byte blob produced by [`encode_map_item_operand`]
/// back into the `map_item_operand` `i32` array `gin_tsquery_consistent` reads.
fn decode_map_item_operand(bytes: &[u8]) -> Vec<i32> {
    bytes
        .chunks_exact(4)
        .map(|c| i32::from_ne_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

/// Deconstruct an array `Datum` into its canonical element values + null flags,
/// the shared front half of `ginarrayextract` / `ginqueryarrayextract`
/// (`get_typlenbyvalalign(ARR_ELEMTYPE(array))` + `deconstruct_array`). The
/// element type is read from the array header (`ARR_ELEMTYPE`); the elements
/// travel as the canonical `::types_tuple::Datum` so by-reference keys keep their
/// bytes.
fn deconstruct_query_or_value<'mcx>(
    mcx: Mcx<'mcx>,
    array: Datum<'mcx>,
) -> PgResult<(PgVec<'mcx, Datum<'mcx>>, PgVec<'mcx, bool>)> {
    // ARR_ELEMTYPE(DatumGetArrayTypeP(array)) — the element type OID, read from
    // the detoasted array bytes.
    let arr_elemtype: Oid =
        arrayfuncs_seams::array_get_elemtype_bytes::call(mcx, array.as_ref_bytes())?;

    // get_typlenbyvalalign(ARR_ELEMTYPE(array), &elmlen, &elmbyval, &elmalign);
    let tlba = lsyscache_seams::get_typlenbyvalalign::call(arr_elemtype)?;

    // deconstruct_array(array, ARR_ELEMTYPE(array), elmlen, elmbyval, elmalign,
    //                   &elems, &nulls, &nelems);
    let pairs = arrayfuncs_seams::deconstruct_array_v::call(
        mcx,
        array,
        arr_elemtype,
        tlba.typlen,
        tlba.typbyval,
        tlba.typalign as core::ffi::c_char,
    )?;

    let mut elems: PgVec<'mcx, Datum<'mcx>> = PgVec::new_in(mcx);
    let mut nulls: PgVec<'mcx, bool> = PgVec::new_in(mcx);
    for (d, isnull) in pairs {
        elems.push(d);
        nulls.push(isnull);
    }
    Ok((elems, nulls))
}

/// `gin_extract_value` dispatch (`extractValueFn`, `FunctionCall3Coll`): route
/// `flinfo.fn_oid` to the `anyarray_ops` `ginarrayextract` body. Returns the
/// extracted element keys + per-key null flags (the seam's `*nkeys` /
/// `**nullFlags` out-params), or `None` (no keys) for the placeholder path.
fn dispatch_extract_value<'mcx>(
    mcx: Mcx<'mcx>,
    flinfo: &::types_core::fmgr::FmgrInfo,
    _collation: Oid,
    value: Datum<'mcx>,
) -> PgResult<Option<(PgVec<'mcx, Datum<'mcx>>, PgVec<'mcx, bool>)>> {
    match flinfo.fn_oid {
        F_GINARRAYEXTRACT | F_GINARRAYEXTRACT_2ARGS => {
            // ginarrayextract / ginarrayextract_2args: identical extraction (the
            // legacy 2-arg wrapper just forwards). The fmgr wrapper supplies the
            // already-copied array; the canonical `value` carries the bytes.
            let (elems, nulls) = deconstruct_query_or_value(mcx, value)?;
            Ok(Some((elems, nulls)))
        }
        F_GIN_EXTRACT_TSVECTOR | F_GIN_EXTRACT_TSVECTOR_2ARGS => {
            // gin_extract_tsvector(vector): one `text` key per lexeme. The
            // detoasted tsvector bytes are the canonical by-ref value image; the
            // ported body walks them and returns the entry `text` varlenas. C
            // leaves `nullFlags` NULL (no lexeme key is null), so `null_flags`
            // stays empty (the seam's C-`NULL` sentinel).
            let vector = unpack_short_to_4b(mcx, value.as_ref_bytes())?;
            let entries =
                tsginidx::gin_extract_tsvector(mcx, &vector)?;
            let mut elems: PgVec<'mcx, Datum<'mcx>> = PgVec::new_in(mcx);
            for txt in entries {
                elems.push(Datum::from_byref_bytes_in(mcx, &txt)?);
            }
            Ok(Some((elems, PgVec::new_in(mcx))))
        }
        F_GIN_EXTRACT_JSONB => {
            // gin_extract_jsonb(jsonb): one `text` GIN key per JSON key / scalar.
            // The canonical by-ref image is the full jsonb varlena; the core
            // wants the `JsonbContainer` root after the varlena header.
            let jb = value.as_ref_bytes();
            let keys = jsonb_gin_seams::gin_extract_jsonb::call(
                mcx,
                vardata_any(jb),
            )?;
            let elems = text_keys_to_datums(mcx, keys)?;
            Ok(Some((elems, PgVec::new_in(mcx))))
        }
        F_GIN_EXTRACT_JSONB_PATH => {
            // gin_extract_jsonb_path(jsonb): one bare `uint32` hash key per JSON
            // value (the jsonb_path_ops opclass). The keys travel as their 4
            // native-endian hash bytes (a by-value `uint32` GIN key).
            let jb = value.as_ref_bytes();
            let keys = jsonb_gin_seams::gin_extract_jsonb_path::call(
                vardata_any(jb),
            )?;
            let elems = hash_keys_to_datums(mcx, keys)?;
            Ok(Some((elems, PgVec::new_in(mcx))))
        }
        other => Err(unported(other, "extractValue")),
    }
}

/// Wrap a vector of `jsonb_ops` GIN key payloads (the `make_text_key` varlenas)
/// back into canonical by-ref `text` `Datum`s. The bytes are the on-disk GIN
/// key image the access method indexes / compares (`gin_compare_jsonb`); the
/// jsonb_ops opclass `opckeytype` is `text`, a by-ref type.
fn text_keys_to_datums<'mcx>(
    mcx: Mcx<'mcx>,
    keys: Vec<Vec<u8>>,
) -> PgResult<PgVec<'mcx, Datum<'mcx>>> {
    let mut elems: PgVec<'mcx, Datum<'mcx>> = PgVec::new_in(mcx);
    for k in keys {
        elems.push(Datum::from_byref_bytes_in(mcx, &k)?);
    }
    Ok(elems)
}

/// Wrap a vector of `jsonb_path_ops` GIN key payloads (each the 4 native-endian
/// bytes of a `uint32` value hash, as `uint32_get_datum` produced) into the
/// by-value `int4` `Datum`s the `jsonb_path_ops` opclass uses (its `opckeytype`
/// is `int4`, a pass-by-value type; the keys are ordered / compared with the
/// default `btint4cmp`).
fn hash_keys_to_datums<'mcx>(
    mcx: Mcx<'mcx>,
    keys: Vec<Vec<u8>>,
) -> PgResult<PgVec<'mcx, Datum<'mcx>>> {
    let mut elems: PgVec<'mcx, Datum<'mcx>> = PgVec::new_in(mcx);
    for k in keys {
        // C: `UInt32GetDatum(hash)` — a by-value 4-byte word.
        let h = u32::from_ne_bytes([k[0], k[1], k[2], k[3]]);
        elems.push(Datum::from_u32(h));
    }
    Ok(elems)
}

/// `gin_extract_query` dispatch (`extractQueryFn`, `FunctionCall7Coll`): route
/// `flinfo.fn_oid` to the `anyarray_ops` `ginqueryarrayextract` body. Returns
/// the extracted query keys + null flags + search mode (the seam's out-params);
/// `partial_matches` / `extra_data` stay empty (the C `NULL` — array_ops sets
/// neither).
fn dispatch_extract_query<'mcx>(
    mcx: Mcx<'mcx>,
    flinfo: &::types_core::fmgr::FmgrInfo,
    _collation: Oid,
    query: Datum<'mcx>,
    strategy: u16,
) -> PgResult<GinExtractQueryResult<'mcx>> {
    match flinfo.fn_oid {
        F_GINQUERYARRAYEXTRACT => {
            let (elems, nulls) = deconstruct_query_or_value(mcx, query)?;
            let nelems = elems.len() as i32;

            // switch (strategy) { ... *searchMode = ...; } — mirrors
            // ginqueryarrayextract (ginarrayproc.c:107) exactly.
            let search_mode: i32 = match strategy {
                GinOverlapStrategy => GIN_SEARCH_MODE_DEFAULT,
                GinContainsStrategy => {
                    if nelems > 0 {
                        GIN_SEARCH_MODE_DEFAULT
                    } else {
                        // everything contains the empty set
                        GIN_SEARCH_MODE_ALL
                    }
                }
                GinContainedStrategy => {
                    // empty set is contained in everything
                    GIN_SEARCH_MODE_INCLUDE_EMPTY
                }
                GinEqualStrategy => {
                    if nelems > 0 {
                        GIN_SEARCH_MODE_DEFAULT
                    } else {
                        GIN_SEARCH_MODE_INCLUDE_EMPTY
                    }
                }
                other => {
                    return Err(PgError::error(format!(
                        "ginqueryarrayextract: unknown strategy number: {other}"
                    )));
                }
            };

            Ok(GinExtractQueryResult {
                query_values: elems,
                null_flags: nulls,
                // array_ops returns no partial-match / extra-data arrays.
                partial_matches: PgVec::new_in(mcx),
                extra_data: PgVec::new_in(mcx),
                search_mode,
            })
        }
        F_GIN_EXTRACT_TSQUERY | F_GIN_EXTRACT_TSQUERY_5ARGS | F_GIN_EXTRACT_TSQUERY_OLDSIG => {
            // gin_extract_tsquery(query): one operand `text` key per QI_VAL, plus
            // the per-key partial-match flags and the item->operand map. The map
            // (`map_item_operand`, the C `extra_data[0]`) is stored — encoded as
            // a native-endian i32 blob — in EVERY entry's `extra_data` slot,
            // exactly as C sets `(*extra_data)[j] = (Pointer) map_item_operand`.
            let _ = strategy; // tsvector_ops ignores the strategy in extractQuery
            let qimage = unpack_short_to_4b(mcx, query.as_ref_bytes())?;
            let ext = tsginidx::gin_extract_tsquery(mcx, &qimage)?;

            let mut query_values: PgVec<'mcx, Datum<'mcx>> = PgVec::new_in(mcx);
            for txt in &ext.entries {
                query_values.push(Datum::from_byref_bytes_in(mcx, txt)?);
            }

            let mut partial_matches: PgVec<'mcx, bool> = PgVec::new_in(mcx);
            for &p in &ext.partialmatch {
                partial_matches.push(p);
            }

            // The same map for every entry (C aliases one `map_item_operand`).
            let map_blob = encode_map_item_operand(&ext.map_item_operand);
            let mut extra_data: PgVec<'mcx, Option<::mcx::PgVec<'mcx, u8>>> = PgVec::new_in(mcx);
            for _ in 0..ext.entries.len() {
                let mut slot: ::mcx::PgVec<'mcx, u8> = PgVec::new_in(mcx);
                for &b in &map_blob {
                    slot.push(b);
                }
                extra_data.push(Some(slot));
            }

            // `*searchMode` is untouched by C when query->size == 0 (then
            // nentries == 0 and the value is unused); default it.
            let search_mode = ext.search_mode.unwrap_or(GIN_SEARCH_MODE_DEFAULT);

            Ok(GinExtractQueryResult {
                query_values,
                // gin_extract_tsquery leaves nullFlags NULL (no key is null).
                null_flags: PgVec::new_in(mcx),
                partial_matches,
                extra_data,
                search_mode,
            })
        }
        F_GIN_EXTRACT_JSONB_QUERY => dispatch_jsonb_extract_query(mcx, query, strategy, false),
        F_GIN_EXTRACT_JSONB_QUERY_PATH => dispatch_jsonb_extract_query(mcx, query, strategy, true),
        other => Err(unported(other, "extractQuery")),
    }
}

/// Shared `extractQuery` dispatch for the `jsonb_ops` (`path_ops == false`) and
/// `jsonb_path_ops` (`path_ops == true`) opclasses. Builds the strategy-tagged
/// [`GinJsonbQuery`] from the (already-detoasted) query `Datum`, calls the
/// matching value core through the seam, and marshals the returned entries +
/// `*searchMode` + `(*extra_data)[0]` node back into [`GinExtractQueryResult`].
fn dispatch_jsonb_extract_query<'mcx>(
    mcx: Mcx<'mcx>,
    query: Datum<'mcx>,
    strategy: u16,
    path_ops: bool,
) -> PgResult<GinExtractQueryResult<'mcx>> {
    use ::types_jsonb::jsonb_gin::GinJsonbQuery;

    // Call the matching value core through the seam, given a `GinJsonbQuery`.
    let call = |mcx: Mcx<'mcx>, jq: GinJsonbQuery| -> PgResult<_> {
        if path_ops {
            jsonb_gin_seams::gin_extract_jsonb_query_path::call(mcx, jq, strategy)
        } else {
            jsonb_gin_seams::gin_extract_jsonb_query::call(mcx, jq, strategy)
        }
    };

    // The canonical by-ref image of the query argument.
    let qbytes = query.as_ref_bytes();

    let ext = match strategy {
        JSONB_CONTAINS_STRATEGY => {
            // Query is a jsonb; the core wants the container root after VARHDRSZ.
            call(mcx, GinJsonbQuery::Contains(vardata_any(qbytes)))?
        }
        JSONB_EXISTS_STRATEGY => {
            // Query is a `text` key; the core wants the raw string payload.
            call(mcx, GinJsonbQuery::Exists(vardata_any(qbytes)))?
        }
        JSONB_EXISTS_ANY_STRATEGY | JSONB_EXISTS_ALL_STRATEGY => {
            // Query is a `text[]`; each element's text payload is a candidate key
            // (SQL NULL elements become `None`, ignored by the core). The element
            // Datums (by-ref `text`) own the payload bytes; build a borrowing view
            // and call the core within this scope so the borrows stay valid.
            let (elems, nulls) = deconstruct_query_or_value(mcx, query)?;
            let payloads: Vec<Option<&[u8]>> = elems
                .iter()
                .zip(nulls.iter())
                .map(|(d, &isnull)| {
                    if isnull {
                        None
                    } else {
                        // VARDATA_ANY of the text element (strip the varlena header).
                        Some(vardata_any(d.as_ref_bytes()))
                    }
                })
                .collect();
            call(mcx, GinJsonbQuery::ExistsArray(&payloads))?
        }
        JSONB_JSONPATH_EXISTS_STRATEGY | JSONB_JSONPATH_PREDICATE_STRATEGY => {
            // Query is the on-disk jsonpath varlena; the core reads its header word
            // (bytes 4..8), so it wants the WHOLE varlena image.
            call(mcx, GinJsonbQuery::Jsonpath(qbytes))?
        }
        other => {
            return Err(PgError::error(format!(
                "unrecognized jsonb GIN strategy number: {other}"
            )));
        }
    };

    marshal_jsonb_query_extraction(mcx, ext, path_ops)
}

/// Marshal a [`GinQueryExtraction`] (the de-pointered `gin_extract_jsonb_query
/// [_path]` outputs) into the GIN-core [`GinExtractQueryResult`]. The query keys
/// are wrapped the same way as the value keys (by-ref `text` for `jsonb_ops`,
/// by-value `int4` hashes for `jsonb_path_ops`). The jsonpath strategies carry a
/// single opclass-private root node, which C stashes in `(*extra_data)[0]`; here
/// it is serialized into the first key's `extra_data` slot (the GIN scan carries
/// `extra_data[0]` through to the consistent procs). `nullFlags` /
/// `partial_matches` are empty (the jsonb opclasses set neither).
fn marshal_jsonb_query_extraction<'mcx>(
    mcx: Mcx<'mcx>,
    ext: ::types_jsonb::jsonb_gin::GinQueryExtraction,
    path_ops: bool,
) -> PgResult<GinExtractQueryResult<'mcx>> {
    let nentries = ext.entries.len();

    let query_values = if path_ops {
        hash_keys_to_datums(mcx, ext.entries)?
    } else {
        text_keys_to_datums(mcx, ext.entries)?
    };

    // extra_data[]: one slot per query key (C `(*extra_data)` length == nentries).
    // The jsonpath root node lives only in slot 0; all other slots are NULL. The
    // node is serialized to bytes for the opaque `Pointer` channel.
    let mut extra_data: PgVec<'mcx, Option<PgVec<'mcx, u8>>> = PgVec::new_in(mcx);
    if let Some(node) = ext.node {
        let blob = node.encode_extra_data();
        for i in 0..nentries {
            if i == 0 {
                let mut slot: PgVec<'mcx, u8> = PgVec::new_in(mcx);
                for &b in &blob {
                    slot.push(b);
                }
                extra_data.push(Some(slot));
            } else {
                extra_data.push(None);
            }
        }
    }

    let search_mode = if ext.search_mode_all {
        GIN_SEARCH_MODE_ALL
    } else {
        GIN_SEARCH_MODE_DEFAULT
    };

    Ok(GinExtractQueryResult {
        query_values,
        null_flags: PgVec::new_in(mcx),
        partial_matches: PgVec::new_in(mcx),
        extra_data,
        search_mode,
    })
}

/// `gin_consistent_call_bool` dispatch (`consistentFn`, `FunctionCall8Coll`):
/// route `key.consistent_fmgr_oid` to the `anyarray_ops` `ginarrayconsistent`
/// body. The C `check` (arg 0) is `key->entryRes` read as `bool[]`, `nkeys` is
/// `key->nuserentries`, `nullFlags` (arg 7) is `key->queryCategories` read as
/// `bool[]` (`GIN_CAT_NULL_KEY == 1` → null), and `recheck` (arg 5) is written
/// back into `key->recheckCurItem`.
fn dispatch_consistent_bool(key: &mut GinScanKey) -> bool {
    match key.consistent_fmgr_oid {
        F_GINARRAYCONSISTENT => {
            let nkeys = key.nuserentries as usize;
            // check[i] = (entryRes[i] != GIN_FALSE) — GinTernaryValue as bool.
            let check: Vec<bool> =
                key.entryRes[..nkeys].iter().map(|&v| v != 0).collect();
            // nullFlags[i] = (queryCategories[i] == GIN_CAT_NULL_KEY).
            let null_flags: Vec<bool> = key.queryCategories[..nkeys]
                .iter()
                .map(|&c| c == GIN_CAT_NULL_KEY)
                .collect();
            let mut recheck = key.recheckCurItem;
            let res = match ginarrayproc::ginarrayconsistent(
                &check,
                key.strategy,
                nkeys as i32,
                &null_flags,
                &mut recheck,
            ) {
                Ok(r) => r,
                Err(e) => std::panic::panic_any(e),
            };
            key.recheckCurItem = recheck;
            res
        }
        F_GIN_TSQUERY_CONSISTENT | F_GIN_TSQUERY_CONSISTENT_6ARGS
        | F_GIN_TSQUERY_CONSISTENT_OLDSIG => {
            let nkeys = key.nuserentries as usize;
            // check[i] = (entryRes[i] != GIN_FALSE) — gin_tsquery_consistent
            // reinterprets the GIN `bool` check array as the entry presence.
            let check: Vec<bool> = key.entryRes[..nkeys].iter().map(|&v| v != 0).collect();
            // extra_data[0] is the map_item_operand blob extractQuery stored.
            let map = key
                .extra_data
                .first()
                .and_then(|o| o.as_ref())
                .map(|b| decode_map_item_operand(b))
                .unwrap_or_default();
            // Transient per-call scratch context (C's GIN scan tempCtx): the body
            // allocates getquery()/check_tri here; the result is a scalar.
            let scratch = ::mcx::MemoryContext::new("gin_tsquery_consistent");
            let qimage = match unpack_short_to_4b(scratch.mcx(), key.query.as_ref_bytes()) {
                Ok(q) => q,
                Err(e) => std::panic::panic_any(e),
            };
            let (res, recheck) = match tsginidx::gin_tsquery_consistent(
                scratch.mcx(),
                &check,
                &qimage,
                &map,
            ) {
                Ok(r) => r,
                Err(e) => std::panic::panic_any(e),
            };
            key.recheckCurItem = recheck;
            res
        }
        F_GIN_CONSISTENT_JSONB | F_GIN_CONSISTENT_JSONB_PATH => {
            let path_ops = key.consistent_fmgr_oid == F_GIN_CONSISTENT_JSONB_PATH;
            let nkeys = key.nuserentries as usize;
            // check[i] = (entryRes[i] != GIN_FALSE) — the GIN bool check array.
            let check: Vec<bool> = key.entryRes[..nkeys].iter().map(|&v| v != 0).collect();
            // extra_data[0] is the serialized jsonpath root node (None for the
            // containment / existence strategies, which carry no node).
            let node = decode_jsonb_extra_node(key);
            let (res, recheck) = match jsonb_consistent_bool(
                path_ops,
                &check,
                key.strategy,
                nkeys as i32,
                node.as_ref(),
            ) {
                Ok(r) => r,
                Err(e) => std::panic::panic_any(e),
            };
            key.recheckCurItem = recheck;
            res
        }
        other => std::panic::panic_any(unported(other, "consistent")),
    }
}

/// Decode the `JsonPathGinNode` the jsonb `extractQuery` serialized into
/// `extra_data[0]` (the jsonpath strategies only). Returns `None` when no node
/// was stored.
fn decode_jsonb_extra_node(
    key: &GinScanKey,
) -> Option<::types_jsonb::jsonb_gin::JsonPathGinNode> {
    key.extra_data
        .first()
        .and_then(|o| o.as_ref())
        .and_then(|b| ::types_jsonb::jsonb_gin::JsonPathGinNode::decode_extra_data(b))
}

/// Route to the boolean `gin_consistent_jsonb` / `gin_consistent_jsonb_path`
/// value core through the seam.
fn jsonb_consistent_bool(
    path_ops: bool,
    check: &[bool],
    strategy: u16,
    nkeys: i32,
    node: Option<&::types_jsonb::jsonb_gin::JsonPathGinNode>,
) -> PgResult<(bool, bool)> {
    if path_ops {
        jsonb_gin_seams::gin_consistent_jsonb_path::call(
            check, strategy, nkeys, node,
        )
    } else {
        jsonb_gin_seams::gin_consistent_jsonb::call(
            check, strategy, nkeys, node,
        )
    }
}

/// `gin_consistent_call_tri` dispatch (`triConsistentFn`, `FunctionCall7Coll`):
/// route `key.tri_consistent_fmgr_oid` to the `anyarray_ops`
/// `ginarraytriconsistent` body. Same arg mapping as the boolean dispatch, but
/// `check` carries the ternary `GIN_TRUE`/`GIN_FALSE`/`GIN_MAYBE` values and
/// there is no `recheck` out-param (the ternary interface encodes recheck as
/// `GIN_MAYBE`).
fn dispatch_consistent_tri(key: &mut GinScanKey) -> GinTernaryValue {
    match key.tri_consistent_fmgr_oid {
        F_GINARRAYTRICONSISTENT => {
            let nkeys = key.nuserentries as usize;
            let check: Vec<GinTernaryValue> = key.entryRes[..nkeys].to_vec();
            let null_flags: Vec<bool> = key.queryCategories[..nkeys]
                .iter()
                .map(|&c| c == GIN_CAT_NULL_KEY)
                .collect();
            match ginarrayproc::ginarraytriconsistent(
                &check,
                key.strategy,
                nkeys as i32,
                &null_flags,
            ) {
                Ok(r) => r,
                Err(e) => std::panic::panic_any(e),
            }
        }
        F_GIN_TSQUERY_TRICONSISTENT => {
            let nkeys = key.nuserentries as usize;
            // check carries the ternary GIN values directly.
            let check: Vec<GinTernaryValue> = key.entryRes[..nkeys].to_vec();
            let map = key
                .extra_data
                .first()
                .and_then(|o| o.as_ref())
                .map(|b| decode_map_item_operand(b))
                .unwrap_or_default();
            let scratch = ::mcx::MemoryContext::new("gin_tsquery_triconsistent");
            let qimage = match unpack_short_to_4b(scratch.mcx(), key.query.as_ref_bytes()) {
                Ok(q) => q,
                Err(e) => std::panic::panic_any(e),
            };
            match tsginidx::gin_tsquery_triconsistent(
                scratch.mcx(),
                &check,
                &qimage,
                &map,
            ) {
                Ok(r) => r,
                Err(e) => std::panic::panic_any(e),
            }
        }
        F_GIN_TRICONSISTENT_JSONB | F_GIN_TRICONSISTENT_JSONB_PATH => {
            let path_ops = key.tri_consistent_fmgr_oid == F_GIN_TRICONSISTENT_JSONB_PATH;
            let nkeys = key.nuserentries as usize;
            // check carries the ternary GIN values directly.
            let check: Vec<GinTernaryValue> = key.entryRes[..nkeys].to_vec();
            let node = decode_jsonb_extra_node(key);
            let res = if path_ops {
                jsonb_gin_seams::gin_triconsistent_jsonb_path::call(
                    &check,
                    key.strategy,
                    nkeys as i32,
                    node.as_ref(),
                )
            } else {
                jsonb_gin_seams::gin_triconsistent_jsonb::call(
                    &check,
                    key.strategy,
                    nkeys as i32,
                    node.as_ref(),
                )
            };
            match res {
                Ok(r) => r,
                Err(e) => std::panic::panic_any(e),
            }
        }
        other => std::panic::panic_any(unported(other, "triConsistent")),
    }
}

/// `gin_compare_partial` dispatch (`comparePartialFn`, `FunctionCall4Coll`,
/// ginget.c `collectMatchBitmap` / `matchPartialInPendingList`): route
/// `flinfo.fn_oid` to the `tsvector_ops` `gin_cmp_prefix` body. The query key and
/// the stored index key are both `text` GIN keys (the tsvector_ops `opckeytype`
/// is `text`); their canonical by-ref `Datum` image is the full varlena, and the
/// `gin_cmp_prefix` core consumes the header-stripped `VARDATA_ANY` payload (just
/// like `gin_cmp_tslexeme`). The `strategy` / `extra_data` args are `#ifdef
/// NOT_USED` in the C body and ignored. Other opclasses have no `comparePartial`
/// proc (anyarray_ops / jsonb_ops set `canPartialMatch=false`), so any other OID
/// bottoms out loudly.
fn dispatch_compare_partial<'mcx>(
    _flinfo: &::types_core::fmgr::FmgrInfo,
    _collation: Oid,
    query_key: Datum<'mcx>,
    idatum: Datum<'mcx>,
    _strategy: u16,
    _extra_data: Option<&[u8]>,
) -> PgResult<i32> {
    match _flinfo.fn_oid {
        F_GIN_CMP_PREFIX => {
            // gin_cmp_prefix(partial_key, key): VARDATA_ANY of both `text` keys.
            let a = vardata_any(query_key.as_ref_bytes());
            let b = vardata_any(idatum.as_ref_bytes());
            Ok(tsginidx::gin_cmp_prefix(a, b))
        }
        other => Err(unported(other, "comparePartial")),
    }
}

/// The loud bottom-out for a GIN opclass support-proc OID this dispatch does not
/// handle (a tsvector_ops / jsonb_ops support proc whose body is not yet ported,
/// or a user-defined opclass that would need a `Datum::Internal` fmgr arm).
fn unported(foid: u32, role: &str) -> PgError {
    PgError::error(format!(
        "GIN opclass {role} support function (OID {foid}) has no owned dispatch \
         (anyarray_ops, tsvector_ops, and jsonb_ops / jsonb_path_ops procedures \
         are wired through the typed by-OID GIN dispatch; this OID is not one of \
         them — a user-defined opclass would need a Datum::Internal fmgr arm)"
    ))
}

/// Install the four GIN opclass-dispatch seams over the `anyarray_ops`
/// support-proc OIDs. Called from this crate's `init_seams()`.
pub fn install() {
    gin_extract_value::set(dispatch_extract_value);
    gin_extract_query::set(dispatch_extract_query);
    gin_consistent_call_bool::set(dispatch_consistent_bool);
    gin_consistent_call_tri::set(dispatch_consistent_tri);
    gin_compare_partial::set(dispatch_compare_partial);
}
