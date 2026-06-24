//! `contrib/hstore/hstore_subs.c` — subscripting support functions for hstore.
//!
//! Ports the hstore `SubscriptRoutines` execution callbacks installed by
//! `hstore_exec_setup`: the primitive-backed FETCH / ASSIGN bodies
//! (`hstore_subscript_fetch`, `hstore_subscript_assign`). hstore subscripting is
//! far simpler than array/jsonb (hstore_subs.c's own header comment): the result
//! of subscripting an hstore is just a `text` string (the value for the key), so
//! there is no slice support, no multi-subscript support, and — because the
//! result is never a SQL container — no nested-assignment scenario, hence no
//! `fetch_old` and no `check_subscripts`. The fetch/assign callbacks do
//! everything.
//!
//! ## What lives where (mirror of jsonbsubs / arraysubs)
//!
//! As with the array/jsonb subscripting bodies, the raw `void (*)(ExprState *,
//! ExprEvalStep *, ExprContext *)` callback shape can not thread the owned
//! `Mcx`/EState/result cell, so:
//!
//!   * `hstore_subscript_transform` (the parse-analysis method) lives in the
//!     `subscripting_transform` parser seam (it depends on `transformExpr` /
//!     `coerce_to_target_type`, parser-layer entry points above this unit).
//!   * `hstore_exec_setup` (method-discriminant selection) is executor-side
//!     compilation logic and lives in the executor (`execExpr`).
//!   * the two primitive-backed bodies ported here are reached through the
//!     `hstoresubs_seams` per-callback seams.
//!
//! ## `hstore_subscript_handler` registration
//!
//! `hstore_subscript_handler(PG_FUNCTION_ARGS)` returns a pointer to a `static
//! const SubscriptRoutines` via `PG_RETURN_POINTER`. Its result type is
//! `internal`, which is unmarshalable at the fmgr boundary, so the builtin is
//! not registered through the normal `PGFunction` lane (mirror of
//! `array_subscript_handler` / `jsonb_subscript_handler`). The routine is a pure
//! constant-returning function; its `SubscriptRoutines` value is resolved
//! directly in `lsyscache::getSubscriptingRoutines` (the
//! `SubscriptHandler::Hstore` arm, reached when the type's `typsubscript` OID
//! resolves to the `hstore_subscript_handler` proname), exactly as PostgreSQL
//! would obtain it through `OidFunctionCall0(typsubscript)`.

use ::mcx::Mcx;
use ::types_error::{PgError, PgResult, ERRCODE_NULL_VALUE_NOT_ALLOWED};
use types_tuple::heaptuple::Datum as DatumV;

use crate::repr::{build_hstore, find_key, HstoreView, Pair};
use crate::{check_key_len, check_val_len, varlena_payload};

/// `PG_DETOAST_DATUM_PACKED(d)` — detoast a by-reference varlena Datum. The
/// subscripting executor reads `*op->resvalue` straight off the slot, which the
/// central fmgr per-arg detoast never touched, so a compressed-inline or
/// out-of-line external value arrives here still toasted.
fn detoast<'mcx>(mcx: Mcx<'mcx>, d: &DatumV<'mcx>) -> PgResult<::mcx::PgVec<'mcx, u8>> {
    detoast_seams::pg_detoast_datum_packed::call(mcx, d.as_ref_bytes())
}

/// `hstore_subscript_fetch` (hstore_subs.c): evaluate a SubscriptingRef fetch
/// for hstore. Mirrors `hstore_fetchval`.
///
/// ```c
/// hs = DatumGetHStoreP(*op->resvalue);
/// key = DatumGetTextPP(sbsrefstate->upperindex[0]);
/// idx = hstoreFindKey(hs, NULL, VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key));
/// if (idx < 0 || HSTORE_VALISNULL(entries, idx)) { *op->resnull = true; return; }
/// out = cstring_to_text_with_len(HSTORE_VAL(...), HSTORE_VALLEN(...));
/// *op->resvalue = PointerGetDatum(out);
/// ```
///
/// The source container is known not NULL (fetch_strict) and the subscript is
/// known not NULL (a NULL subscript short-circuits to NULL before this is
/// reached). Returns `(text_value, isnull)`.
pub fn hstore_subscript_fetch<'mcx>(
    mcx: Mcx<'mcx>,
    container: DatumV<'mcx>,
    key: DatumV<'mcx>,
) -> PgResult<(DatumV<'mcx>, bool)> {
    let hs_bytes = detoast(mcx, &container)?;
    let key_bytes = detoast(mcx, &key)?;
    let hs = HstoreView::from_vardata(varlena_payload(hs_bytes.as_slice()));
    let key_payload = varlena_payload(key_bytes.as_slice());

    match find_key(&hs, None, key_payload) {
        Some(idx) if !hs.val_isnull(idx) => {
            // cstring_to_text_with_len(HSTORE_VAL, HSTORE_VALLEN): a header-ful
            // text varlena over the value payload.
            let out = make_text(mcx, hs.val(idx))?;
            Ok((DatumV::ByRef(out), false))
        }
        // idx < 0 || HSTORE_VALISNULL(entries, idx): *op->resnull = true.
        _ => Ok((DatumV::null(), true)),
    }
}

/// `hstore_subscript_assign` (hstore_subs.c): evaluate a SubscriptingRef
/// assignment for hstore, returning the new whole hstore value (never NULL).
///
/// ```c
/// key = DatumGetTextPP(sbsrefstate->upperindex[0]);
/// p.key = VARDATA_ANY(key); p.keylen = hstoreCheckKeyLen(VARSIZE_ANY_EXHDR(key));
/// if (sbsrefstate->replacenull) { p.vallen = 0; p.isnull = true; }
/// else { val = DatumGetTextPP(replacevalue); p.val = VARDATA_ANY(val);
///        p.vallen = hstoreCheckValLen(VARSIZE_ANY_EXHDR(val)); p.isnull = false; }
/// if (*op->resnull) out = hstorePairs(&p, 1, ...);      // NULL input
/// else { /* merge the new key into the hstore, based on hstore_concat */ }
/// *op->resvalue = PointerGetDatum(out); *op->resnull = false;
/// ```
pub fn hstore_subscript_assign<'mcx>(
    mcx: Mcx<'mcx>,
    container: DatumV<'mcx>,
    container_null: bool,
    key: DatumV<'mcx>,
    replacevalue: DatumV<'mcx>,
    replacenull: bool,
) -> PgResult<(DatumV<'mcx>, bool)> {
    let key_bytes = detoast(mcx, &key)?;
    let key_payload = varlena_payload(key_bytes.as_slice());
    // p.keylen = hstoreCheckKeyLen(VARSIZE_ANY_EXHDR(key));
    check_key_len(key_payload.len())?;

    // Build the Pairs entry for subscript + replacement value.
    let new_pair = if replacenull {
        // p.vallen = 0; p.isnull = true;
        Pair {
            key: key_payload.to_vec(),
            val: None,
            needfree: false,
        }
    } else {
        // val = DatumGetTextPP(replacevalue);
        let val_bytes = detoast(mcx, &replacevalue)?;
        let val_payload = varlena_payload(val_bytes.as_slice());
        // p.vallen = hstoreCheckValLen(VARSIZE_ANY_EXHDR(val));
        check_val_len(val_payload.len())?;
        Pair {
            key: key_payload.to_vec(),
            val: Some(val_payload.to_vec()),
            needfree: false,
        }
    };

    let out: Vec<u8> = if container_null {
        // Just build a one-element hstore (cf. hstore_from_text).
        build_hstore(&[new_pair])
    } else {
        // Otherwise, merge the new key into the hstore. Based on hstore_concat:
        // the existing pairs are sorted+unique; insert/replace the new key,
        // keeping the result sorted by (keylen, key). A duplicate of the new
        // key is overwritten by the new pair (the C merge advances s1idx past it
        // on difference == 0).
        let hs_bytes = detoast(mcx, &container)?;
        let hs = HstoreView::from_vardata(varlena_payload(hs_bytes.as_slice()));
        let s1count = hs.count();

        let mut merged: Vec<Pair> = Vec::with_capacity(s1count + 1);
        let mut s1idx = 0usize;
        let mut s2done = false;
        // C loop: for (s1idx=s2idx=0; s1idx<s1count || s2idx<1; ++outcount)
        while s1idx < s1count || !s2done {
            // difference: >=0 take the new pair (and skip s1 entry on ==0);
            // <0 copy the existing s1 entry.
            let difference = if s1idx >= s1count {
                1
            } else if s2done {
                -1
            } else {
                let s1key = hs.key(s1idx);
                let s1keylen = s1key.len();
                let s2keylen = new_pair.key.len();
                if s1keylen == s2keylen {
                    match s1key.cmp(new_pair.key.as_slice()) {
                        core::cmp::Ordering::Less => -1,
                        core::cmp::Ordering::Equal => 0,
                        core::cmp::Ordering::Greater => 1,
                    }
                } else if s1keylen > s2keylen {
                    1
                } else {
                    -1
                }
            };

            if difference >= 0 {
                // HS_ADDITEM(ed, ..., p): emit the new pair.
                merged.push(new_pair.clone());
                s2done = true;
                if difference == 0 {
                    // duplicate key: drop the existing entry.
                    s1idx += 1;
                }
            } else {
                // HS_COPYITEM: copy the existing s1 entry verbatim.
                merged.push(Pair {
                    key: hs.key(s1idx).to_vec(),
                    val: if hs.val_isnull(s1idx) {
                        None
                    } else {
                        Some(hs.val(s1idx).to_vec())
                    },
                    needfree: false,
                });
                s1idx += 1;
            }
        }

        build_hstore(&merged)
    };

    // *op->resvalue = PointerGetDatum(out); *op->resnull = false;
    let out = copy_to_mcx(mcx, &out)?;
    Ok((DatumV::ByRef(out), false))
}

/// Copy a plain byte image into an `mcx`-allocated `PgVec` (the `DatumV::ByRef`
/// payload lives in the per-query expression-eval context).
fn copy_to_mcx<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<::mcx::PgVec<'mcx, u8>> {
    let mut v = ::mcx::vec_with_capacity_in(mcx, bytes.len())?;
    for &b in bytes {
        v.push(b);
    }
    Ok(v)
}

/// `cstring_to_text_with_len(payload, len)` — a header-ful `text` varlena image
/// over `payload`.
fn make_text<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<::mcx::PgVec<'mcx, u8>> {
    let total = payload.len() + ::datum::varlena::VARHDRSZ;
    let mut img = ::mcx::vec_with_capacity_in(mcx, total)?;
    for b in ::datum::varlena::set_varsize_4b(total) {
        img.push(b);
    }
    for &b in payload {
        img.push(b);
    }
    Ok(img)
}

/// Helper used by the null-subscript-in-assignment error path mirror (kept for
/// documentation symmetry; the NULL-subscript check is performed in the
/// interpreter before this owner seam is reached, exactly as in jsonbsubs).
#[allow(dead_code)]
fn hstore_subscript_null_assignment_error() -> PgError {
    PgError::error("hstore subscript in assignment must not be null")
        .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED)
}

/// Install this unit's per-callback execution seams. Invoked from the hstore
/// library's `init_seams()` wiring.
pub fn init_seams() {
    hstoresubs_seams::hstore_subscript_fetch::set(hstore_subscript_fetch);
    hstoresubs_seams::hstore_subscript_assign::set(hstore_subscript_assign);
}
