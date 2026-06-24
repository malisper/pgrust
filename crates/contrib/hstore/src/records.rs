//! Record / jsonb conversion functions (hstore_io.c):
//! `hstore_from_record`, `hstore_populate_record`, `hstore_to_jsonb[_loose]`.

use ::fmgr::boundary::RefPayload;
use ::fmgr::FunctionCallInfoBaseData;
use ::types_error::{PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERROR};
use ::types_tuple::heaptuple::{
    Datum as TDatum, FormedTuple, HeapTupleHeaderGetTypMod, HeapTupleHeaderGetTypeId,
};
use ::utils_error::ereport;

use crate::repr::{build_hstore, find_key, unique_pairs, HstoreView, Pair};
use crate::{arg_bytes, arg_isnull, arg_hstore, raise, Datum};

/// Read a composite (record) by-ref arg's image, accepting either the
/// `Composite` or the generic `Varlena` lane (`as_composite` then `as_varlena`).
fn composite_arg<'a>(fcinfo: &'a FunctionCallInfoBaseData, n: usize) -> Option<&'a [u8]> {
    fcinfo
        .ref_arg(n)
        .and_then(|p| p.as_composite().or_else(|| p.as_varlena()))
}

const RECORDOID: ::types_core::Oid = 2249;

/// `get_fn_expr_argtype(fcinfo->flinfo, n)` at the contrib `::fmgr` boundary.
fn fn_arg_type(fcinfo: &FunctionCallInfoBaseData, n: i32) -> ::types_core::Oid {
    fmgr_core::get_fn_expr_argtype(fcinfo.flinfo.as_deref(), n)
}

/// `type_is_rowtype(typid)` (lsyscache.c): RECORD, or a composite/row type.
fn type_is_rowtype(typid: ::types_core::Oid) -> PgResult<bool> {
    if typid == RECORDOID {
        return Ok(true);
    }
    // get_typtype == 'c' (composite).
    Ok(lsyscache_seams::get_typtype::call(typid)? == b'c')
}

// ===========================================================================
// hstore_from_record(record) -> hstore  (hstore_io.c:833)
// ===========================================================================

pub fn fc_hstore_from_record(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match from_record_impl(fcinfo) {
        Ok(Some(img)) => {
            fcinfo.isnull = false;
            fcinfo.set_ref_result(RefPayload::Varlena(img));
            Datum::from_usize(0)
        }
        Ok(None) => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
        Err(e) => raise(e),
    }
}

fn from_record_impl(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Option<Vec<u8>>> {
    let scratch = ::mcx::MemoryContext::new("hstore_from_record");
    let mcx = scratch.mcx();

    let (tup_type, tup_typmod, rec): (::types_core::Oid, i32, Option<FormedTuple>) =
        if arg_isnull(fcinfo, 0) {
            let argtype = fn_arg_type(fcinfo, 0);
            (argtype, -1, None)
        } else {
            let image = composite_arg(fcinfo, 0)
                .ok_or_else(|| PgError::error("hstore_from_record: missing record arg"))?
                .to_vec();
            let ft = FormedTuple::from_datum_image(mcx, &image)?;
            let t_data = ft.tuple.t_data.as_ref().expect("record has t_data");
            let tt = HeapTupleHeaderGetTypeId(t_data);
            let tm = HeapTupleHeaderGetTypMod(t_data);
            (tt, tm, Some(ft))
        };

    let tupdesc = typcache_seams::lookup_rowtype_tupdesc_domain::call(mcx, tup_type, tup_typmod, false)?
        .expect("lookup_rowtype_tupdesc_domain(noError=false) is Some or errors");
    let ncolumns = tupdesc.natts as usize;

    // Deform the input tuple (if any).
    let deformed: Option<Vec<(TDatum, bool)>> = match &rec {
        Some(ft) => {
            let cols = heaptuple::heap_deform_tuple(mcx, &ft.tuple, &tupdesc, &ft.data)?;
            Some(cols.iter().cloned().collect())
        }
        None => None,
    };

    let mut pairs: Vec<Pair> = Vec::with_capacity(ncolumns);
    for i in 0..ncolumns {
        let att = tupdesc.attr(i);
        if att.attisdropped {
            continue;
        }
        let key = att.attname.name_str().to_vec();
        let (value, isnull) = match &deformed {
            Some(cols) => (cols[i].0.clone(), cols[i].1),
            None => (TDatum::ByVal(0), true),
        };
        if isnull {
            pairs.push(Pair {
                key,
                val: None,
                needfree: false,
            });
            continue;
        }
        // Convert the column value to text via its output function.
        let column_type = att.atttypid;
        let (typoutput, _typisvarlena) = lsyscache_seams::get_type_output_info::call(column_type)?;
        let s = fmgr_seams::oid_output_function_call_datum::call(mcx, typoutput, value)?;
        pairs.push(Pair {
            key,
            val: Some(s.as_bytes().to_vec()),
            needfree: false,
        });
    }

    let (pairs, _) = unique_pairs(pairs);
    Ok(Some(build_hstore(&pairs)))
}

// ===========================================================================
// hstore_populate_record(anyelement, hstore) -> anyelement  (hstore_io.c:992)
// ===========================================================================

pub fn fc_hstore_populate_record(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match populate_record_impl(fcinfo) {
        Ok(PopResult::Null) => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
        Ok(PopResult::Composite(image)) => {
            fcinfo.isnull = false;
            fcinfo.set_ref_result(RefPayload::Composite(image));
            Datum::from_usize(0)
        }
        Err(e) => raise(e),
    }
}

enum PopResult {
    Null,
    Composite(Vec<u8>),
}

fn populate_record_impl(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<PopResult> {
    let scratch = ::mcx::MemoryContext::new("hstore_populate_record");
    let mcx = scratch.mcx();

    let argtype = fn_arg_type(fcinfo, 0);
    if !type_is_rowtype(argtype)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg("first argument must be a rowtype")
            .into_error());
    }

    let rec_null = arg_isnull(fcinfo, 0);
    let hs_null = arg_isnull(fcinfo, 1);

    let (tup_type, tup_typmod, rec): (::types_core::Oid, i32, Option<FormedTuple>) = if rec_null {
        if hs_null {
            return Ok(PopResult::Null);
        }
        (argtype, -1, None)
    } else {
        let image = composite_arg(fcinfo, 0)
            .ok_or_else(|| PgError::error("hstore_populate_record: missing record arg"))?
            .to_vec();
        if hs_null {
            // PG_RETURN_POINTER(rec) — return the input record unchanged.
            return Ok(PopResult::Composite(image));
        }
        let ft = FormedTuple::from_datum_image(mcx, &image)?;
        let t_data = ft.tuple.t_data.as_ref().expect("record has t_data");
        let tt = HeapTupleHeaderGetTypeId(t_data);
        let tm = HeapTupleHeaderGetTypMod(t_data);
        (tt, tm, Some(ft))
    };

    let hs_body = crate::varlena_payload(arg_bytes(fcinfo, 1));
    let hs = HstoreView::from_vardata(hs_body);

    // If the input hstore is empty and we have a non-null record, return it.
    if hs.count() == 0 {
        if let Some(ft) = &rec {
            return Ok(PopResult::Composite(
                composite_arg(fcinfo, 0)
                    .map(|b| b.to_vec())
                    .unwrap_or_else(|| ft.data.to_vec()),
            ));
        }
    }

    let tupdesc = typcache_seams::lookup_rowtype_tupdesc_domain::call(mcx, tup_type, tup_typmod, false)?
        .expect("lookup_rowtype_tupdesc_domain(noError=false) is Some or errors");
    let ncolumns = tupdesc.natts as usize;

    // Deform the existing record (or all-null).
    let mut values: Vec<TDatum> = Vec::with_capacity(ncolumns);
    let mut nulls: Vec<bool> = Vec::with_capacity(ncolumns);
    match &rec {
        Some(ft) => {
            let cols = heaptuple::heap_deform_tuple(mcx, &ft.tuple, &tupdesc, &ft.data)?;
            for (d, n) in cols.iter() {
                values.push(d.clone());
                nulls.push(*n);
            }
        }
        None => {
            for _ in 0..ncolumns {
                values.push(TDatum::ByVal(0));
                nulls.push(true);
            }
        }
    }

    let entries = &hs;
    for i in 0..ncolumns {
        let att = tupdesc.attr(i);
        if att.attisdropped {
            nulls[i] = true;
            continue;
        }
        let column_type = att.atttypid;
        let attname = att.attname.name_str();

        let idx = find_key(entries, None, attname);

        // If key not found and we were passed a non-null record, keep existing.
        if idx.is_none() && rec.is_some() {
            continue;
        }

        let (typinput, typioparam) = lsyscache_seams::get_type_input_info::call(column_type)?;
        let atttypmod = att.atttypmod;

        match idx {
            Some(j) if !entries.val_isnull(j) => {
                let vbytes = entries.val(j);
                let s = core::str::from_utf8(vbytes)
                    .map_err(|_| PgError::error("hstore value is not valid UTF-8"))?;
                let v = fmgr_seams::input_function_call::call(
                    mcx, typinput, Some(s), typioparam, atttypmod, None,
                )?;
                values[i] = v;
                nulls[i] = false;
            }
            _ => {
                // key absent or value NULL: run input function on NULL (domain
                // checks), result is SQL NULL.
                let v = fmgr_seams::input_function_call::call(
                    mcx, typinput, None, typioparam, atttypmod, None,
                )?;
                values[i] = v;
                nulls[i] = true;
            }
        }
    }

    let rettuple = heaptuple::heap_form_tuple(mcx, &tupdesc, &values, &nulls)
        .map_err(|e| PgError::error(alloc::format!("heap_form_tuple failed: {e:?}")))?;
    let datum = heaptuple::HeapTupleGetDatum(mcx, &rettuple, &tupdesc)?;
    // datum is Datum::ByRef(image) — the self-contained composite image.
    let image = match datum {
        TDatum::ByRef(b) => b.as_slice().to_vec(),
        _ => return Err(PgError::error("heap_form_tuple did not produce a composite image")),
    };
    let _ = RECORDOID;
    Ok(PopResult::Composite(image))
}

// ===========================================================================
// hstore_to_jsonb[_loose](hstore) -> jsonb  (hstore_io.c:1434)
// ===========================================================================

pub fn fc_hstore_to_jsonb(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    to_jsonb(fcinfo, false)
}

pub fn fc_hstore_to_jsonb_loose(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    to_jsonb(fcinfo, true)
}

fn to_jsonb(fcinfo: &mut FunctionCallInfoBaseData, loose: bool) -> Datum {
    match to_jsonb_impl(fcinfo, loose) {
        Ok(image) => {
            fcinfo.isnull = false;
            fcinfo.set_ref_result(RefPayload::Varlena(image));
            Datum::from_usize(0)
        }
        Err(e) => raise(e),
    }
}

fn to_jsonb_impl(fcinfo: &mut FunctionCallInfoBaseData, loose: bool) -> PgResult<Vec<u8>> {
    use ::types_jsonb::jsonb::{jbvType, JsonbIteratorToken};
    use ::types_jsonb::jsonb_util::{JsonbValue, JsonbValueData};
    let hs = arg_hstore(fcinfo, 0);
    let count = hs.count();
    let scratch = ::mcx::MemoryContext::new("hstore_to_jsonb");
    let mcx = scratch.mcx();

    let mut state: Option<alloc::boxed::Box<::types_jsonb::jsonb_util::JsonbParseState>> = None;
    jsonb_util::pushJsonbValue(&mut state, JsonbIteratorToken::WJB_BEGIN_OBJECT, None)?;

    for i in 0..count {
        // key.
        let key = JsonbValue {
            typ: jbvType::jbvString,
            val: JsonbValueData::String(hs.key(i).to_vec()),
        };
        jsonb_util::pushJsonbValue(&mut state, JsonbIteratorToken::WJB_KEY, Some(&key))?;

        let val = if hs.val_isnull(i) {
            JsonbValue::null()
        } else {
            let v = hs.val(i);
            if loose && v.len() == 1 && v[0] == b't' {
                JsonbValue {
                    typ: jbvType::jbvBool,
                    val: JsonbValueData::Bool(true),
                }
            } else if loose && v.len() == 1 && v[0] == b'f' {
                JsonbValue {
                    typ: jbvType::jbvBool,
                    val: JsonbValueData::Bool(false),
                }
            } else if loose && jsonapi::is_valid_json_number(v) {
                // numeric_in(v) -> jbvNumeric.
                let s = core::str::from_utf8(v).unwrap_or("");
                let numeric = small1_seams::numeric_in::call(mcx, s)?;
                JsonbValue {
                    typ: jbvType::jbvNumeric,
                    val: JsonbValueData::Numeric(numeric.as_slice().to_vec()),
                }
            } else {
                JsonbValue {
                    typ: jbvType::jbvString,
                    val: JsonbValueData::String(v.to_vec()),
                }
            }
        };
        jsonb_util::pushJsonbValue(&mut state, JsonbIteratorToken::WJB_VALUE, Some(&val))?;
    }

    let res = jsonb_util::pushJsonbValue(&mut state, JsonbIteratorToken::WJB_END_OBJECT, None)?
        .expect("WJB_END_OBJECT yields a container");
    let image = jsonb_util::JsonbValueToJsonb(mcx, &res)?;
    Ok(image.as_slice().to_vec())
}

// ===========================================================================
// GiST / GIN opclass support functions + subscript handler.
//
// The GiST/GIN bodies live in `crate::hstore_gist` / `crate::hstore_gin`; these
// `PGFunction` shims marshal the `gist::extproc` / `gin::extproc` internal
// protocol structs (the generic catalog-driven dispatch) in/out and call the
// bodies. (The subscript handler remains the documented gap.)
// ===========================================================================

use ::gin::extproc::{
    GinConsistentInOut, GinExtractQueryOut, GinExtractValueOut, GIN_EXTPROC_INTERNAL_SLOT,
};
use ::gist::extproc::{
    GistConsistentInOut, GistEntryInOut, GistPenaltyInOut, GistPicksplitInOut, GistSameInOut,
    GistUnionInOut, GIST_EXTPROC_INTERNAL_SLOT,
};

use crate::{hstore_gin, hstore_gist};

fn unported(sym: &str) -> ! {
    raise(PgError::error(alloc::format!(
        "hstore: {sym} (contrib/hstore index opclass / subscripting) is not yet wired in pgrust"
    )));
}

// ghstore_in / ghstore_out always ereport(ERROR) in C (the ghstore pseudo-type
// has no text I/O); keep them as the loud-error shims.
pub fn fc_ghstore_in(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    raise(PgError::error("cannot accept a value of type ghstore"));
}
pub fn fc_ghstore_out(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    raise(PgError::error("cannot display a value of type ghstore"));
}

pub fn fc_hstore_subscript_handler(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unported("hstore_subscript_handler");
}

// --- GiST ------------------------------------------------------------------

/// Pull the boxed GiST protocol struct of type `T` out of the internal lane,
/// run `body` on it (mutating in place), and put it back so the dispatch reads
/// the outputs.
fn with_gist_proto<T: ::core::any::Any>(
    fcinfo: &mut FunctionCallInfoBaseData,
    body: impl FnOnce(&mut T) -> PgResult<()>,
) -> Datum {
    let mut state = match fcinfo.take_internal_arg(GIST_EXTPROC_INTERNAL_SLOT) {
        Some(boxed) => match boxed.downcast::<T>() {
            Ok(s) => s,
            Err(_) => raise(PgError::error(
                "hstore GiST support: internal protocol state has the wrong type",
            )),
        },
        None => raise(PgError::error(
            "hstore GiST support function invoked without its internal protocol \
             state — pgrust's generic GiST opclass dispatch was bypassed",
        )),
    };
    if let Err(e) = body(&mut state) {
        fcinfo.set_internal_arg(GIST_EXTPROC_INTERNAL_SLOT, state);
        raise(e);
    }
    fcinfo.set_internal_arg(GIST_EXTPROC_INTERNAL_SLOT, state);
    fcinfo.isnull = false;
    Datum::from_usize(0)
}

/// `ghstore_compress(entry)`.
pub fn fc_ghstore_compress(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    with_gist_proto::<GistEntryInOut>(fcinfo, |io| {
        match hstore_gist::ghstore_compress(io.entry.leafkey, &io.entry.key, io.entry.key_is_null)? {
            Some(new_key) => {
                io.passthrough = false;
                io.retval_key = new_key;
                io.retval_leafkey = false;
            }
            None => io.passthrough = true,
        }
        Ok(())
    })
}

/// `ghstore_decompress(entry)` — identity on the owned by-ref lane.
pub fn fc_ghstore_decompress(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    with_gist_proto::<GistEntryInOut>(fcinfo, |io| {
        match hstore_gist::ghstore_decompress() {
            Some(new_key) => {
                io.passthrough = false;
                io.retval_key = new_key;
                io.retval_leafkey = io.entry.leafkey;
            }
            None => io.passthrough = true,
        }
        Ok(())
    })
}

/// `ghstore_penalty(origentry, newentry, &penalty)`.
pub fn fc_ghstore_penalty(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    with_gist_proto::<GistPenaltyInOut>(fcinfo, |io| {
        io.penalty = hstore_gist::ghstore_penalty(&io.orig_key, &io.new_key)?;
        Ok(())
    })
}

/// `ghstore_picksplit(entryvec, &v)`.
pub fn fc_ghstore_picksplit(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    with_gist_proto::<GistPicksplitInOut>(fcinfo, |io| {
        let entries: Vec<(Vec<u8>, bool)> = io
            .entries
            .iter()
            .map(|e| (e.key.clone(), e.key_is_null))
            .collect();
        let (left, right, ldatum, rdatum) = hstore_gist::ghstore_picksplit(&entries)?;
        io.spl_left = left;
        io.spl_right = right;
        io.spl_ldatum = ldatum;
        io.spl_rdatum = rdatum;
        Ok(())
    })
}

/// `ghstore_union(entryvec, &size)`.
pub fn fc_ghstore_union(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    with_gist_proto::<GistUnionInOut>(fcinfo, |io| {
        let entries: Vec<(Vec<u8>, bool)> = io
            .entries
            .iter()
            .map(|e| (e.key.clone(), e.key_is_null))
            .collect();
        io.result = hstore_gist::ghstore_union(&entries)?;
        Ok(())
    })
}

/// `ghstore_same(a, b, &result)`.
pub fn fc_ghstore_same(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    with_gist_proto::<GistSameInOut>(fcinfo, |io| {
        io.equal = hstore_gist::ghstore_same(&io.a, &io.b)?;
        Ok(())
    })
}

/// `ghstore_consistent(entry, query, strategy, subtype, recheck)`. The query
/// (an hstore / text / text[]) rides the by-ref lane at slot 1.
pub fn fc_ghstore_consistent(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let query = arg_bytes(fcinfo, 1).to_vec();
    with_gist_proto::<GistConsistentInOut>(fcinfo, |io| {
        let (matched, recheck) = hstore_gist::ghstore_consistent(
            &io.entry.key,
            io.entry.key_is_null,
            &query,
            io.strategy,
        )?;
        io.matched = matched;
        io.recheck = recheck;
        Ok(())
    })
}

/// `ghstore_options(relopts)` — register the `siglen` opclass option.
pub fn fc_ghstore_options(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut relopts = match fcinfo.take_ref_arg(0) {
        Some(RefPayload::Internal(b)) => {
            match b.downcast::<::types_reloptions::local_relopts>() {
                Ok(r) => r,
                Err(_) => raise(PgError::error(
                    "ghstore_options: arg 0 internal is not a local_relopts",
                )),
            }
        }
        _ => raise(PgError::error(
            "ghstore_options invoked without its local_relopts internal arg — \
             pgrust's index_options_function_call path was bypassed",
        )),
    };
    hstore_gist::ghstore_options(&mut relopts);
    fcinfo.set_ref_arg(0, RefPayload::Internal(relopts));
    fcinfo.isnull = false;
    Datum::from_usize(0)
}

// --- GIN -------------------------------------------------------------------

/// Pull the boxed GIN protocol struct of type `T` out of the internal lane, run
/// `body` on it (mutating in place), and put it back. `value`/`query` is the
/// by-ref arg-0 payload (the full header-ful image; the bodies strip headers).
fn with_gin_proto<T: ::core::any::Any>(
    fcinfo: &mut FunctionCallInfoBaseData,
    body: impl FnOnce(&mut T, &[u8]) -> PgResult<()>,
) -> Datum {
    let value = arg_bytes(fcinfo, 0).to_vec();
    let mut state = match fcinfo.take_internal_arg(GIN_EXTPROC_INTERNAL_SLOT) {
        Some(boxed) => match boxed.downcast::<T>() {
            Ok(s) => s,
            Err(_) => raise(PgError::error(
                "hstore GIN support: internal protocol state has the wrong type",
            )),
        },
        None => raise(PgError::error(
            "hstore GIN support function invoked without its internal protocol \
             state — pgrust's generic GIN opclass dispatch was bypassed",
        )),
    };
    if let Err(e) = body(&mut state, &value) {
        fcinfo.set_internal_arg(GIN_EXTPROC_INTERNAL_SLOT, state);
        raise(e);
    }
    fcinfo.set_internal_arg(GIN_EXTPROC_INTERNAL_SLOT, state);
    fcinfo.isnull = false;
    Datum::from_usize(0)
}

/// `gin_extract_hstore(hstore, internal) RETURNS internal`.
pub fn fc_gin_extract_hstore(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    with_gin_proto::<GinExtractValueOut>(fcinfo, |out, value| {
        out.keys = hstore_gin::gin_extract_hstore(value);
        Ok(())
    })
}

/// `gin_extract_hstore_query(hstore, internal, int2, internal, internal)
/// RETURNS internal`.
pub fn fc_gin_extract_hstore_query(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    with_gin_proto::<GinExtractQueryOut>(fcinfo, |out, query| {
        let (keys, search_mode) = hstore_gin::gin_extract_hstore_query(query, out.strategy)?;
        out.keys = keys;
        if let Some(mode) = search_mode {
            out.search_mode = mode;
        }
        Ok(())
    })
}

/// `gin_consistent_hstore(internal, int2, hstore, int4, internal, internal)
/// RETURNS bool`.
pub fn fc_gin_consistent_hstore(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    with_gin_proto::<GinConsistentInOut>(fcinfo, |io, _query| {
        let (matched, recheck) =
            hstore_gin::gin_consistent_hstore(&io.check, io.strategy, io.nkeys)?;
        io.matched = matched;
        io.recheck = recheck;
        Ok(())
    })
}
