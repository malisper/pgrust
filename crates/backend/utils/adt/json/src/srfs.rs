//! jsonfuncs.c json-half SRFs: json_object_keys, json_each[_text],
//! json_array_elements[_text]. C materializes rows up front (tuplestore /
//! first-call collection); the owned row vectors here are the same cost
//! shape, driven through the funcapi ValuePerCall frame.

extern crate alloc;

use alloc::vec::Vec;

use crate::funcs::{invalid_param, parse_sem_or_ereport};
use crate::jsonapi::{JsonLex, JsonLexDe, JsonSem, JsonSemToken, JsonToken};
use datum::Datum;
use mcx::Mcx;
use types_core::catalog::{JSONOID, RECORDOID, TEXTOID};
use types_error::PgResult;
use types_fmgr::{byref_result, varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

// Owned cross-call rows (per-call memory resets between SRF calls).
pub(crate) enum SrfRows {
    Texts(Vec<Option<Vec<u8>>>),
    Tuples(Vec<Vec<u8>>),
}

pub(crate) fn srf_drive(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    name: &'static str,
    collect: impl FnOnce(&Fcinfo) -> PgResult<SrfRows>,
) -> PgResult<Datum> {
    let flinfo = flinfo.unwrap_or_else(|| panic!("{name}: NULL flinfo"));
    if !flinfo.has_fn_extra() {
        let rows = collect(fcinfo)?;
        let fctx = funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(rows));
    }
    let fctx = funcapi::per_MultiFuncCall(flinfo);
    let idx = fctx.call_cntr as usize;
    let rows = fctx
        .user_fctx
        .as_ref()
        .expect("SRF rows set at first call")
        .downcast_ref::<SrfRows>()
        .expect("user_fctx is SrfRows");
    let mcx = fcinfo.result_mcx();
    let out: Option<Option<Datum>> = match rows {
        SrfRows::Texts(v) => v.get(idx).map(|r| match r {
            None => None,
            Some(bytes) => Some(
                varlena::cstring_to_text(mcx, bytes)
                    .map(varlena_result)
                    .expect("text result"),
            ),
        }),
        SrfRows::Tuples(v) => v
            .get(idx)
            .map(|img| Some(byref_result(mcx, img).expect("tuple result"))),
    };
    match out {
        Some(Some(d)) => Ok(funcapi::srf_return_next(flinfo, fcinfo, d)),
        Some(None) => Ok(funcapi::srf_return_next_null(flinfo, fcinfo)),
        None => Ok(funcapi::srf_return_done(flinfo, fcinfo)),
    }
}

struct OkeysState<'mcx> {
    keys: Vec<Option<Vec<u8>>>,
    _marker: core::marker::PhantomData<&'mcx ()>,
}

impl<'mcx> JsonSem<'mcx> for OkeysState<'mcx> {
    fn object_field_start(
        &mut self,
        lex: &JsonLex<'_>,
        fname: &'mcx [u8],
        _isnull: bool,
    ) -> PgResult<bool> {
        if lex.lex_level == 1 {
            self.keys.push(Some(fname.to_vec()));
        }
        Ok(true)
    }

    fn array_start(&mut self, lex: &JsonLex<'_>) -> PgResult<bool> {
        if lex.lex_level == 0 {
            return Err(invalid_param(
                "cannot call json_object_keys on an array".into(),
            ));
        }
        Ok(true)
    }

    fn scalar(&mut self, lex: &JsonLex<'_>, _token: JsonSemToken<'mcx>) -> PgResult<bool> {
        if lex.lex_level == 0 {
            return Err(invalid_param(
                "cannot call json_object_keys on a scalar".into(),
            ));
        }
        Ok(true)
    }
}

/// C: json_object_keys — the first-call key collection.
pub(crate) fn object_keys_rows(mcx: Mcx<'_>, json: &[u8]) -> PgResult<SrfRows> {
    let mut lex = JsonLexDe::new(mcx, json, mbutils::GetDatabaseEncoding());
    let mut state = OkeysState {
        keys: Vec::new(),
        _marker: core::marker::PhantomData,
    };
    parse_sem_or_ereport(&mut lex, &mut state)?;
    Ok(SrfRows::Texts(state.keys))
}

struct EachState<'a, 'mcx> {
    input: &'a [u8],
    normalize: bool,
    next_scalar: bool,
    normalized_scalar: Option<&'mcx [u8]>,
    result_start: usize,
    pairs: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

impl<'mcx> JsonSem<'mcx> for EachState<'_, 'mcx> {
    fn object_field_start(
        &mut self,
        lex: &JsonLex<'_>,
        _fname: &'mcx [u8],
        _isnull: bool,
    ) -> PgResult<bool> {
        if lex.lex_level == 1 {
            if self.normalize && lex.token_type == JsonToken::String {
                self.next_scalar = true;
            } else {
                self.result_start = lex.token_start.expect("value token started");
            }
        }
        Ok(true)
    }

    fn object_field_end(
        &mut self,
        lex: &JsonLex<'_>,
        fname: &'mcx [u8],
        isnull: bool,
    ) -> PgResult<bool> {
        if lex.lex_level != 1 {
            return Ok(true);
        }
        let val = if isnull && self.normalize {
            None
        } else if self.next_scalar {
            let s = self.normalized_scalar.expect("scalar recorded");
            self.next_scalar = false;
            Some(s.to_vec())
        } else {
            Some(self.input[self.result_start..lex.prev_token_terminator].to_vec())
        };
        self.pairs.push((fname.to_vec(), val));
        Ok(true)
    }

    fn array_start(&mut self, lex: &JsonLex<'_>) -> PgResult<bool> {
        if lex.lex_level == 0 {
            return Err(invalid_param(
                "cannot deconstruct an array as an object".into(),
            ));
        }
        Ok(true)
    }

    fn scalar(&mut self, lex: &JsonLex<'_>, token: JsonSemToken<'mcx>) -> PgResult<bool> {
        if lex.lex_level == 0 {
            return Err(invalid_param("cannot deconstruct a scalar".into()));
        }
        if self.next_scalar {
            let JsonSemToken::String(s) = token else {
                panic!("next_scalar set on a non-string token")
            };
            self.normalized_scalar = Some(s);
        }
        Ok(true)
    }
}

/// C: each_worker — rows materialized as composite images with a freshly
/// built 2-column rowtype.
pub(crate) fn each_rows(mcx: Mcx<'_>, json: &[u8], as_text: bool) -> PgResult<SrfRows> {
    let mut lex = JsonLexDe::new(mcx, json, mbutils::GetDatabaseEncoding());
    let mut state = EachState {
        input: json,
        normalize: as_text,
        next_scalar: false,
        normalized_scalar: None,
        result_start: 0,
        pairs: Vec::new(),
    };
    parse_sem_or_ereport(&mut lex, &mut state)?;

    let mut desc = tupdesc::CreateTemplateTupleDesc(mcx, 2)?;
    tupdesc::TupleDescInitEntry(&mut desc, 1, Some("key"), TEXTOID, -1, 0)?;
    tupdesc::TupleDescInitEntry(
        &mut desc,
        2,
        Some("value"),
        if as_text { TEXTOID } else { JSONOID },
        -1,
        0,
    )?;
    desc.tdtypeid = RECORDOID;
    desc.tdtypmod = -1;
    // C: BlessTupleDesc (jsonfuncs.c each_worker, InitMaterializedSRF(fcinfo,
    // MAT_SRF_BLESS)) — the rows are anonymous record datums; record_out
    // needs the registered typmod stamped into each tuple header.
    ::typcache_seams::assign_record_type_typmod::call(&mut desc)?;

    let mut rows: Vec<Vec<u8>> = Vec::with_capacity(state.pairs.len());
    for (key, val) in &state.pairs {
        let key_datum = varlena_result(varlena::cstring_to_text(mcx, key)?);
        let (val_datum, val_null) = match val {
            None => (Datum::null(), true),
            Some(bytes) => (varlena_result(varlena::cstring_to_text(mcx, bytes)?), false),
        };
        let tuple =
            heaptuple::heap_form_tuple(mcx, &desc, &[key_datum, val_datum], &[false, val_null])?;
        rows.push(tuple.image().to_vec());
    }
    Ok(SrfRows::Tuples(rows))
}

struct ElementsState<'a, 'mcx> {
    input: &'a [u8],
    function_name: &'static str,
    normalize: bool,
    next_scalar: bool,
    normalized_scalar: Option<&'mcx [u8]>,
    result_start: usize,
    rows: Vec<Option<Vec<u8>>>,
}

impl<'mcx> JsonSem<'mcx> for ElementsState<'_, 'mcx> {
    fn array_element_start(&mut self, lex: &JsonLex<'_>, _isnull: bool) -> PgResult<bool> {
        if lex.lex_level == 1 {
            if self.normalize && lex.token_type == JsonToken::String {
                self.next_scalar = true;
            } else {
                self.result_start = lex.token_start.expect("element token started");
            }
        }
        Ok(true)
    }

    fn array_element_end(&mut self, lex: &JsonLex<'_>, isnull: bool) -> PgResult<bool> {
        if lex.lex_level != 1 {
            return Ok(true);
        }
        if isnull && self.normalize {
            self.rows.push(None);
        } else if self.next_scalar {
            let s = self.normalized_scalar.expect("scalar recorded");
            self.next_scalar = false;
            self.rows.push(Some(s.to_vec()));
        } else {
            self.rows
                .push(Some(self.input[self.result_start..lex.prev_token_terminator].to_vec()));
        }
        Ok(true)
    }

    fn object_start(&mut self, lex: &JsonLex<'_>) -> PgResult<bool> {
        if lex.lex_level == 0 {
            return Err(invalid_param(alloc::format!(
                "cannot call {} on a non-array",
                self.function_name
            )));
        }
        Ok(true)
    }

    fn scalar(&mut self, lex: &JsonLex<'_>, token: JsonSemToken<'mcx>) -> PgResult<bool> {
        if lex.lex_level == 0 {
            return Err(invalid_param(alloc::format!(
                "cannot call {} on a scalar",
                self.function_name
            )));
        }
        if self.next_scalar {
            let JsonSemToken::String(s) = token else {
                panic!("next_scalar set on a non-string token")
            };
            self.normalized_scalar = Some(s);
        }
        Ok(true)
    }
}

/// C: elements_worker's parse (lexer escapes only when as_text, as C).
pub(crate) fn elements_rows(
    mcx: Mcx<'_>,
    json: &[u8],
    function_name: &'static str,
    as_text: bool,
) -> PgResult<SrfRows> {
    let mut lex = JsonLexDe::with_escapes(mcx, json, mbutils::GetDatabaseEncoding(), as_text);
    let mut state = ElementsState {
        input: json,
        function_name,
        normalize: as_text,
        next_scalar: false,
        normalized_scalar: None,
        result_start: 0,
        rows: Vec::new(),
    };
    parse_sem_or_ereport(&mut lex, &mut state)?;
    Ok(SrfRows::Texts(state.rows))
}
