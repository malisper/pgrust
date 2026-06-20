//! `unnest(tsvector)` / `tsvector_unnest` (OID 3322) registered as an
//! executor-frame materialize-mode set-returning function.
//!
//! `tsvector_op.c`'s `tsvector_unnest` is a materialize-mode SRF: it calls
//! `InitMaterializedSRF(fcinfo, 0)` to build the `(lexeme text, positions
//! int2[], weights "char"[])` tuplestore on `rsinfo->setResult`, then
//! `tuplestore_putvalues`-es one row per `WordEntry` of the input tsvector and
//! returns `(Datum) 0`. The lexeme/position/weight decode core (the
//! `WordEntry`/`WordEntryPos` walk, lexeme slicing, and the per-position
//! `WEP_GETPOS`/`WEP_GETWEIGHT` extraction) is ported in
//! [`backend_utils_adt_tsvector_core::op::tsvector_unnest`], which hands back a
//! `Vec<UnnestRow>` of decoded rows.
//!
//! Here that core is driven over the executor frame: `InitMaterializedSRF` with
//! `MAT_SRF_USE_EXPECTED_DESC` takes the executor's expected `(text, int2[],
//! "char"[])` descriptor (no catalog `get_call_result_type` needed — the
//! executor already resolved the row type), the row series is appended via
//! `materialized_srf_putvalues`, and the entry point returns SQL NULL. The
//! executor (`ExecMakeTableFunctionResult` Materialize branch) then drains the
//! tuplestore. Registered from [`register_tsvector_unnest`] (called by
//! `init_seams`) — the executor-frame `fmgr_builtins[]` analogue, bypassing the
//! by-OID builtin registry whose tag-only `resultinfo` cannot carry the live
//! `ReturnSetInfo` (the WONTFIX dual-home).
//!
//! `SELECT * FROM unnest('a:1 b:2'::tsvector)` reaches this via nodeFunctionscan
//! → [`crate::ExecMakeTableFunctionResult`] → the executor-frame SRF table.

use mcx::Mcx;
use types_core::Oid;
use types_nodes::fmgr::{FmgrArgRef, FunctionCallInfoBaseData};
use types_nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_utils_fmgr_funcapi::srf_support::{InitMaterializedSRF, materialized_srf_putvalues};

use crate::register_srf;

/// `tsvector_unnest(tsvector)` (OID 3322) — the SRF behind `unnest(tsvector)`.
const TSVECTOR_UNNEST: Oid = 3322;

/// Register `tsvector_unnest` in the executor-frame SRF table.
pub(crate) fn register_tsvector_unnest() {
    register_srf(TSVECTOR_UNNEST, tsvector_unnest);
}

/// Copy a raw varlena image (`text`/`int2[]`/`"char"[]` on-disk bytes) into an
/// `mcx`-owned by-reference Datum (C: the pointer the builder returned into the
/// per-query context). The image is already header-ful (the array/text
/// constructors emit a complete varlena), so it round-trips header-for-header
/// through the tuplestore / printtup output lane.
fn byref_image<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> Datum<'mcx> {
    let mut buf = mcx::PgVec::new_in(mcx);
    buf.try_reserve(image.len())
        .unwrap_or_else(|_| std::panic::panic_any(mcx.oom(image.len())));
    buf.extend_from_slice(image);
    Datum::ByRef(buf)
}

/// `tsvector_unnest(PG_FUNCTION_ARGS)` (tsvector_op.c:631) over the executor
/// frame.
fn tsvector_unnest<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> Datum<'mcx> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("tsvector_unnest: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: tsin = PG_GETARG_TSVECTOR(0) — the detoasted, header-ful on-disk
    // tsvector varlena image on the by-reference side channel.
    let image: Vec<u8> = match fcinfo.ref_arg(0) {
        Some(FmgrArgRef::Varlena(b)) => b.as_slice().to_vec(),
        _ => panic!("tsvector_unnest: tsvector argument missing from the by-ref lane"),
    };

    // Decode the whole row series up front (C does this lazily per call; the
    // owned materialize protocol fills the tuplestore once).
    let rows = backend_utils_adt_tsvector_core::op::tsvector_unnest(&image)
        .unwrap_or_else(|e| std::panic::panic_any(e));

    // C: InitMaterializedSRF(fcinfo, 0). The owned model passes
    // MAT_SRF_USE_EXPECTED_DESC so the materialize descriptor is the executor's
    // already-resolved `(text, int2[], "char"[])` row type, skipping the catalog
    // get_call_result_type (which would need the function's pg_proc rowtype).
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)
        .unwrap_or_else(|e| std::panic::panic_any(e));

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("tsvector_unnest: InitMaterializedSRF establishes fcinfo->resultinfo");

    for row in rows {
        // values[0] = PointerGetDatum(cstring_to_text_with_len(lexeme, len)).
        let lexeme = backend_utils_adt_varlena_seams::cstring_to_text_v::call(
            mcx,
            core::str::from_utf8(&row.lexeme)
                .expect("tsvector_unnest: lexeme is valid UTF-8 text"),
        )
        .unwrap_or_else(|e| std::panic::panic_any(e));

        // values[1]/values[2] = positions int2[] / weights "char"[]; both NULL
        // when the WordEntry carries no positions (C: nulls[1] = nulls[2] = true).
        let (positions, weights, isnull): (Datum<'mcx>, Datum<'mcx>, [bool; 3]) =
            match &row.posweights {
                Some((p, w)) => {
                    // construct_int2_array(positions) — int2[] on-disk image.
                    let pimg = backend_utils_adt_array_more_seams::construct_int2_array::call(p)
                        .unwrap_or_else(|e| std::panic::panic_any(e));
                    // construct_text_array(one-byte weight chars) — "char"[] is a
                    // text[]-shaped array of single-character labels (C builds a
                    // text[] of "D"/"C"/"B"/"A").
                    let mut wcols: Vec<Vec<u8>> = Vec::with_capacity(w.len());
                    for c in w {
                        wcols.push(vec![*c]);
                    }
                    let wimg =
                        backend_utils_adt_array_more_seams::construct_text_array::call(&wcols)
                            .unwrap_or_else(|e| std::panic::panic_any(e));
                    (
                        byref_image(mcx, &pimg),
                        byref_image(mcx, &wimg),
                        [false, false, false],
                    )
                }
                None => (Datum::null(), Datum::null(), [false, true, true]),
            };

        let values = [lexeme, positions, weights];
        materialized_srf_putvalues(rsinfo, &values, &isnull)
            .unwrap_or_else(|e| std::panic::panic_any(e));
    }

    // C: return (Datum) 0; — the whole set is in the materialize tuplestore.
    fcinfo.isnull = true;
    Datum::null()
}
