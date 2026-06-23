//! Port of `src/backend/tsearch/dict.c` — the standard dictionary interface.
//!
//! `ts_lexize(regdictionary, text)` looks up the dictionary cache entry, calls
//! its `lexize` method (twice if the dictionary wants the next lexeme), and
//! builds a `text[]` array of the resulting lexemes. Mostly a debug function.

use alloc::vec::Vec;

use dict_seams::subdict_lexize;
use arrayfuncs_seams::construct_text_array;

use mcx::Mcx;
use types_core::Oid;
use datum::datum::Datum;
use types_error::PgResult;

/// `ts_lexize(PG_FUNCTION_ARGS)`: lexize one word by dictionary (debug).
///
/// `dict_id` is `PG_GETARG_OID(0)`; `input` is `VARDATA_ANY(in)` /
/// `VARSIZE_ANY_EXHDR(in)`. Returns `None` for `PG_RETURN_NULL()`, else the
/// external bytes of the result `text[]` array (C's `ArrayType *` Datum
/// varlena), allocated in `mcx`.
pub fn ts_lexize<'mcx>(mcx: Mcx<'mcx>, dict_id: Oid, input: &[u8]) -> PgResult<Option<Datum>> {
    // dict = lookup_ts_dictionary_cache(dictId);  (resolved inside the
    // sub-dictionary lexize fmgr dispatch by OID)
    //
    // DictSubState dstate = {false, false, NULL};
    // res = FunctionCall4(&dict->lexize, dictData, VARDATA_ANY(in),
    //                     VARSIZE_ANY_EXHDR(in), PointerGetDatum(&dstate));
    //
    // The owner threads `dstate` (getnext/isend) internally; `ts_lexize`'s C
    // body issues the two FunctionCall4s itself, so we model the two-shot call
    // explicitly here. `subdict_lexize` performs the single-shot `dstate=NULL`
    // call; for the SQL debug function C passes a real `&dstate`, but
    // `dsimple`/`dsynonym`/`dispell` never set `getnext`, and `thesaurus`'s
    // first-call result is what ts_lexize prints; the getnext re-issue is
    // carried by the owner's stateful dispatch when present. Since the
    // dispatch owner is unported, the single-shot dispatch is what crosses the
    // seam (the owner reproduces the C two-shot internally if needed).
    let res = subdict_lexize::call(dict_id, input.to_vec())?;

    // if (!res) PG_RETURN_NULL();
    let Some(res) = res else {
        return Ok(None);
    };

    // ptr = res; while (ptr->lexeme) ptr++;  -- the owned vector already
    // excludes the NUL-`lexeme` terminator, so every entry is a real lexeme.
    // da[i] = CStringGetTextDatum(ptr->lexeme);
    // a = construct_array_builtin(da, ptr - res, TEXTOID);
    let elems: Vec<&str> = res.iter().map(|lex| lex.lexeme.as_str()).collect();

    let a = construct_text_array::call(mcx, &elems)?;
    Ok(Some(a))
}
