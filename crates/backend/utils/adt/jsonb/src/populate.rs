//! jsonfuncs.c populate slice: json_populate_type (EEOP_JSONEXPR_COERCION),
//! the json[b]_populate_record(set)/to_record(set) family, and the shared
//! populate_record_field machinery over both the json (text) and jsonb legs.

extern crate alloc;

use core::ffi::CStr;

use adt_json::jsonapi::{JsonLex, JsonLexDe, JsonSem, JsonSemToken, JsonToken};
use datum::Datum;
use mcx::{alloc_in, Mcx, MemoryContext, PgBox, PgHashMap, PgVec};
use stack_depth::check_stack_depth;
use stringinfo::StringInfo;
use types_core::catalog::{JSONBOID, JSONOID};
use types_core::{InvalidOid, Oid, RECORDOID};
use types_error::{
    ereturn, PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_TEXT_REPRESENTATION,
};
use types_fmgr::{
    input_function_call_safe, ErrorSaveNode, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    SetFunctionReturnMode, SFRM_Materialize, SFRM_Materialize_Random,
};
use types_tuple::{HeapTupleData, HeapTupleHeaderData, ItemPointerData, TupleDescData};

use crate::builtins::image_result as image_datum;
use crate::container::{
    container_is_array, container_is_object, container_is_scalar, container_size, get_key_value,
    JsonbItem,
};
use crate::iter::{JsonbIterator, WjbToken};

const NAMEDATALEN: usize = 64;

struct ScalarIoData {
    typiofunc: FmgrInfo,
    typioparam: Oid,
}

/// C CompositeIOData. `tupdesc`/`record_io` fill lazily (update_cached_tupdesc
/// / populate_record) because RECORD base typmods are only known per tuple.
pub struct CompositeIoData<'mcx> {
    cache_mcx: Mcx<'mcx>,
    base_typid: Oid,
    base_typmod: i32,
    tupdesc: Option<TupleDescData<'mcx>>,
    record_io: Option<RecordIoData<'mcx>>,
}

/// C RecordIOData; column caches prepare lazily on (typid, typmod) mismatch.
struct RecordIoData<'mcx> {
    record_type: Oid,
    record_typmod: i32,
    // std Vec justified: droppy element (FmgrInfo) — lives on the
    // fn_extra-lifetime cache, (re)built once per rowtype per query.
    columns: alloc::vec::Vec<Option<ColumnIoData<'mcx>>>,
}

enum ColumnKind<'mcx> {
    Scalar,
    Array {
        element: PgBox<'mcx, ColumnIoData<'mcx>>,
    },
    Composite {
        io: CompositeIoData<'mcx>,
    },
    CompositeDomain {
        io: CompositeIoData<'mcx>,
    },
    Domain {
        base: PgBox<'mcx, ColumnIoData<'mcx>>,
    },
}

/// C ColumnIOData: resolve-once type metadata for one (typid, typmod).
pub struct ColumnIoData<'mcx> {
    typid: Oid,
    typmod: i32,
    // C prepare_column_cache resolves scalar_io at every level
    // (need_scalar=true from populate_record_field): the json-string-to-
    // non-scalar input-function hack reads it for arrays/composites too.
    io: ScalarIoData,
    kind: ColumnKind<'mcx>,
}

impl<'mcx> ColumnIoData<'mcx> {
    // C prepare_column_cache (jsonfuncs.c).
    pub fn new(cache_mcx: Mcx<'mcx>, typid: Oid, typmod: i32) -> PgResult<ColumnIoData<'mcx>> {
        check_stack_depth()?;
        let typtype = lsyscache::get_typtype(typid)?;
        let element_type = lsyscache::get_element_type(typid)?;
        let kind = if typtype == lsyscache::TYPTYPE_DOMAIN {
            let mut base_typmod = typmod;
            let base_typid = lsyscache::getBaseTypeAndTypmod(typid, &mut base_typmod)?;
            if lsyscache::get_typtype(base_typid)? == lsyscache::TYPTYPE_COMPOSITE {
                ColumnKind::CompositeDomain {
                    io: CompositeIoData {
                        cache_mcx,
                        base_typid,
                        base_typmod,
                        tupdesc: None,
                        record_io: None,
                    },
                }
            } else {
                ColumnKind::Domain {
                    base: alloc_in(
                        cache_mcx,
                        ColumnIoData::new(cache_mcx, base_typid, base_typmod)?,
                    )?,
                }
            }
        } else if typtype == lsyscache::TYPTYPE_COMPOSITE || typid == RECORDOID {
            ColumnKind::Composite {
                io: CompositeIoData {
                    cache_mcx,
                    base_typid: typid,
                    base_typmod: typmod,
                    tupdesc: None,
                    record_io: None,
                },
            }
        } else if element_type != types_core::InvalidOid {
            // C: array element typmod is the attribute's typmod.
            ColumnKind::Array {
                element: alloc_in(
                    cache_mcx,
                    ColumnIoData::new(cache_mcx, element_type, typmod)?,
                )?,
            }
        } else {
            ColumnKind::Scalar
        };
        let (typinput, typioparam) = lsyscache::getTypeInputInfo(typid)?;
        let typiofunc = fmgr_seams::fmgr_info::call(typinput)?;
        Ok(ColumnIoData {
            typid,
            typmod,
            io: ScalarIoData { typiofunc, typioparam },
            kind,
        })
    }

    fn composite_io_mut(&mut self) -> Option<&mut CompositeIoData<'mcx>> {
        match &mut self.kind {
            ColumnKind::Composite { io } | ColumnKind::CompositeDomain { io } => Some(io),
            _ => None,
        }
    }

    fn composite_io_ref(&self) -> Option<&CompositeIoData<'mcx>> {
        match &self.kind {
            ColumnKind::Composite { io } | ColumnKind::CompositeDomain { io } => Some(io),
            _ => None,
        }
    }
}

/// C JsValue.
pub enum JsValue<'a> {
    Json {
        s: Option<&'a [u8]>,
        ttype: JsonToken,
    },
    Jsonb(Option<JsonbItem<'a>>),
}

// C JsValueIsNull.
fn js_value_is_null(jsv: &JsValue<'_>) -> bool {
    match jsv {
        JsValue::Json { s, ttype } => s.is_none() || *ttype == JsonToken::Null,
        JsValue::Jsonb(v) => matches!(v, None | Some(JsonbItem::Null)),
    }
}

// C JsValueIsString.
fn js_value_is_string(jsv: &JsValue<'_>) -> bool {
    match jsv {
        JsValue::Json { s, ttype } => s.is_some() && *ttype == JsonToken::String,
        JsValue::Jsonb(v) => matches!(v, Some(JsonbItem::String(_))),
    }
}

fn soft_occurred(escontext: &Option<&mut ErrorSaveNode>) -> bool {
    escontext.as_ref().is_some_and(|n| n.ctx.error_occurred())
}

/// C json_populate_type (jsonfuncs.c). `cache_mcx` is C's `mcxt` (per-query,
/// owns the ColumnIOData tree); `mcx` is CurrentMemoryContext (results and
/// per-call scratch).
///
/// # Safety
/// When `!*isnull`, `json_val` is a live non-null json/jsonb varlena datum
/// readable for the duration of the call.
#[allow(clippy::too_many_arguments)]
pub unsafe fn json_populate_type<'mcx>(
    json_val: Datum,
    json_type: Oid,
    typid: Oid,
    typmod: i32,
    cache: &mut Option<ColumnIoData<'mcx>>,
    cache_mcx: Mcx<'mcx>,
    mcx: Mcx<'_>,
    isnull: &mut bool,
    omit_quotes: bool,
    escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<Datum> {
    let is_json = json_type == JSONOID;
    let mut payload_holder = None;
    let mut text_holder = None;
    let mut unquoted_holder: Option<PgVec<'_, u8>> = None;
    let jsv: JsValue<'_> = if *isnull {
        if is_json {
            JsValue::Json { s: None, ttype: JsonToken::Invalid }
        } else {
            JsValue::Jsonb(None)
        }
    } else if is_json {
        // SAFETY: caller contract — live non-null text varlena.
        let payload = text_holder.insert(unsafe { text_payload_from_datum(mcx, json_val)? });
        JsValue::Json { s: Some(&payload[..]), ttype: JsonToken::Invalid }
    } else {
        // SAFETY: caller contract — live non-null jsonb varlena.
        let payload = payload_holder
            .insert(unsafe { crate::builtins::jsonb_payload_from_datum(mcx, json_val)? });
        if omit_quotes {
            let s = unquoted_holder.insert(jsonb_unquote(mcx, payload.as_bytes())?);
            JsValue::Jsonb(Some(JsonbItem::String(&s[..])))
        } else {
            JsValue::Jsonb(Some(JsonbItem::Binary(payload.as_bytes())))
        }
    };
    if cache.is_none() {
        *cache = Some(ColumnIoData::new(cache_mcx, typid, typmod)?);
    }
    let col = cache.as_mut().expect("cache just filled");
    // C re-prepares on (typid, typmod) change; every coercion step here has a
    // fixed target, so a mismatch is a caller bug.
    debug_assert!(col.typid == typid && col.typmod == typmod);
    populate_record_field(col, None, mcx, None, &jsv, isnull, escontext, omit_quotes)
}

// C DatumGetTextPP: detoasted text payload (no header).
unsafe fn text_payload_from_datum<'mcx>(
    mcx: Mcx<'mcx>,
    d: Datum,
) -> PgResult<PgVec<'mcx, u8>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: caller contract — live varlena readable through VARSIZE_ANY.
    let raw = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    let flat = detoast_seams::detoast_attr::call(mcx, raw)?;
    let bytes: &[u8] = &flat;
    let payload = if bytes[0] & 0x01 == 0x01 {
        &bytes[1..]
    } else {
        &bytes[4..]
    };
    let mut v = mcx::vec_with_capacity_in(mcx, payload.len())?;
    mcx::vec_append_bytes(&mut v, payload)?;
    Ok(v)
}

// C populate_record_field. `defaultval` is a composite datum (composite
// targets only).
#[allow(clippy::too_many_arguments)]
fn populate_record_field(
    col: &mut ColumnIoData<'_>,
    colname: Option<&str>,
    mcx: Mcx<'_>,
    defaultval: Option<Datum>,
    jsv: &JsValue<'_>,
    isnull: &mut bool,
    escontext: Option<&mut ErrorSaveNode>,
    omit_scalar_quotes: bool,
) -> PgResult<Datum> {
    check_stack_depth()?;
    debug_assert!(col.typid != types_core::InvalidOid);
    *isnull = js_value_is_null(jsv);

    // C: a json string converts to a non-scalar type through its input fn.
    let as_scalar = js_value_is_string(jsv)
        && matches!(
            col.kind,
            ColumnKind::Array { .. }
                | ColumnKind::Composite { .. }
                | ColumnKind::CompositeDomain { .. }
        );

    // C: domain checks must run for NULLs; everything else exits now.
    if *isnull
        && !matches!(
            col.kind,
            ColumnKind::Domain { .. } | ColumnKind::CompositeDomain { .. }
        )
    {
        return Ok(Datum::null());
    }

    if as_scalar {
        return populate_scalar(
            &mut col.io,
            col.typid,
            col.typmod,
            mcx,
            jsv,
            isnull,
            escontext,
            omit_scalar_quotes,
        );
    }
    match &mut col.kind {
        ColumnKind::Scalar => populate_scalar(
            &mut col.io,
            col.typid,
            col.typmod,
            mcx,
            jsv,
            isnull,
            escontext,
            omit_scalar_quotes,
        ),
        ColumnKind::Array { element } => populate_array(
            element,
            colname,
            mcx,
            jsv,
            isnull,
            escontext,
        ),
        ColumnKind::Composite { io } | ColumnKind::CompositeDomain { io } => {
            let dfl = match defaultval {
                // SAFETY: a non-null composite datum from the caller.
                Some(d) => Some(unsafe { detoast_composite(mcx, d)? }),
                None => None,
            };
            let r = populate_composite(
                io,
                col.typid,
                colname,
                mcx,
                dfl.as_ref().map(|v| &v[..]),
                jsv,
                isnull,
                escontext,
            );
            // The Defaulted shortcut returns a datum INTO dfl's buffer; leak
            // it into mcx (C: the detoasted copy lives in the row context).
            if let Some(v) = dfl {
                core::mem::forget(v);
            }
            r
        }
        ColumnKind::Domain { base } => populate_domain(
            base,
            col.typid,
            colname,
            mcx,
            jsv,
            isnull,
            escontext,
            omit_scalar_quotes,
        ),
    }
}

// C DatumGetHeapTupleHeader (PG_DETOAST_DATUM on a composite datum).
unsafe fn detoast_composite<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<PgVec<'mcx, u8>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: caller contract — live composite varlena datum.
    let raw = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    detoast_seams::detoast_attr::call(mcx, raw)
}

// C populate_scalar.
#[allow(clippy::too_many_arguments)]
fn populate_scalar(
    io: &mut ScalarIoData,
    typid: Oid,
    typmod: i32,
    mcx: Mcx<'_>,
    jsv: &JsValue<'_>,
    isnull: &mut bool,
    escontext: Option<&mut ErrorSaveNode>,
    omit_quotes: bool,
) -> PgResult<Datum> {
    let mut buf = StringInfo::new_in(mcx)?;
    match jsv {
        JsValue::Json { s, ttype } => {
            let s = s.expect("non-null json jsv");
            // C: converting to json/jsonb re-escapes the de-escaped string.
            if (typid == JSONOID || typid == JSONBOID) && *ttype == JsonToken::String {
                adt_json::escape_json(&mut buf, s)?;
            } else {
                buf.append_bytes(s)?;
            }
        }
        JsValue::Jsonb(v) => {
            let jbv = v.as_ref().expect("non-null jsonb jsv");
            // C branch order: a quote-stripped string wins over the JSONBOID
            // direct JsonbValueToJsonb return.
            if typid == JSONBOID && !(omit_quotes && matches!(jbv, JsonbItem::String(_))) {
                return Ok(image_datum(crate::build::item_to_jsonb_image(mcx, *jbv)?));
            }
            match jbv {
                JsonbItem::String(s) if omit_quotes => buf.append_bytes(s)?,
                // C: scalar jsonb to json preserves top-level string quotes
                // (JsonbValueToJsonb + JsonbToCString collapses to a render).
                JsonbItem::String(s) if typid == JSONOID => adt_json::escape_json(&mut buf, s)?,
                JsonbItem::String(s) => buf.append_bytes(s)?,
                JsonbItem::Bool(b) => buf.append_bytes(if *b { b"true" } else { b"false" })?,
                JsonbItem::Numeric(image) => {
                    let mut scratch = alloc::vec::Vec::new();
                    adt_numeric::numeric_out_into(
                        adt_numeric::Num::from_payload(&image[4..]),
                        &mut scratch,
                    );
                    buf.append_bytes(&scratch)?
                }
                JsonbItem::Binary(c) => {
                    crate::io::jsonb_to_cstring_into(mcx, &mut buf, c, c.len() + 4)?
                }
                other => panic!("unrecognized jsonb type: {}", other.type_ord()),
            }
        }
    }
    buf.append_bytes(b"\0")?;
    let cstr = CStr::from_bytes_with_nul(buf.as_bytes()).expect("json text has no interior NUL");
    let mut res = Datum::null();
    if !input_function_call_safe(
        &mut io.typiofunc,
        Some(cstr),
        io.typioparam,
        typmod,
        mcx,
        escontext,
        &mut res,
    )? {
        res = Datum::null();
        *isnull = true;
    }
    Ok(res)
}

// C populate_domain; constraint evaluation rides the compiled-check engine
// behind typcache_seams::domain_check_input (C domain_check_safe).
#[allow(clippy::too_many_arguments)]
fn populate_domain(
    base: &mut ColumnIoData<'_>,
    typid: Oid,
    colname: Option<&str>,
    mcx: Mcx<'_>,
    jsv: &JsValue<'_>,
    isnull: &mut bool,
    mut escontext: Option<&mut ErrorSaveNode>,
    omit_quotes: bool,
) -> PgResult<Datum> {
    let mut res = Datum::null();
    if !*isnull {
        res = populate_record_field(
            base,
            colname,
            mcx,
            None,
            jsv,
            isnull,
            escontext.as_deref_mut(),
            omit_quotes,
        )?;
        debug_assert!(!*isnull || soft_occurred(&escontext));
    }
    typcache_seams::domain_check_input::call(
        res,
        *isnull,
        typid,
        escontext.as_deref_mut().map(|n| &mut n.ctx),
    )?;
    if soft_occurred(&escontext) {
        *isnull = true;
        return Ok(Datum::null());
    }
    Ok(res)
}

// C update_cached_tupdesc (copy sans constraints into the cache context).
fn update_cached_tupdesc(io: &mut CompositeIoData<'_>) -> PgResult<()> {
    let refresh = match &io.tupdesc {
        None => true,
        Some(d) => d.tdtypeid != io.base_typid || d.tdtypmod != io.base_typmod,
    };
    if refresh {
        io.tupdesc = Some(typcache_seams::lookup_rowtype_tupdesc_copy::call(
            io.cache_mcx,
            io.base_typid,
            io.base_typmod,
        )?);
    }
    Ok(())
}

/// C JsonHashEntry. `val` borrows either the raw json input (sub-object
/// slices) or the lexer arena (de-escaped scalar tokens).
struct JsonHashEntry<'v> {
    ttype: JsonToken,
    val: Option<&'v [u8]>,
}

type JsonHash<'v> = PgHashMap<'v, &'v [u8], JsonHashEntry<'v>>;

// C JHashState sem actions (get_json_object_as_hash).
struct JHashSem<'v, 'h> {
    function_name: &'static str,
    hash: &'h mut JsonHash<'v>,
    input: &'v [u8],
    saved_scalar: Option<&'v [u8]>,
    saved_token_type: JsonToken,
    save_json_start: Option<usize>,
}

#[track_caller]
#[cold]
fn cannot_call_on(function_name: &str, what: &str) -> Box<PgError> {
    Box::new(
        PgError::error(alloc::format!("cannot call {function_name} on {what}"))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

fn scalar_token_bytes<'v>(token: &JsonSemToken<'v>) -> &'v [u8] {
    match token {
        JsonSemToken::String(s) => s,
        JsonSemToken::Number(n) => n,
        JsonSemToken::True => b"true",
        JsonSemToken::False => b"false",
        JsonSemToken::Null => b"null",
    }
}

impl<'v> JsonSem<'v> for JHashSem<'v, '_> {
    fn object_field_start(
        &mut self,
        lex: &JsonLex<'_>,
        _fname: &'v [u8],
        _isnull: bool,
    ) -> PgResult<bool> {
        if lex.lex_level > 1 {
            return Ok(true);
        }
        self.saved_token_type = lex.token_type;
        self.save_json_start = if matches!(lex.token_type, JsonToken::ArrayStart | JsonToken::ObjectStart)
        {
            lex.token_start
        } else {
            None
        };
        Ok(true)
    }

    fn object_field_end(
        &mut self,
        lex: &JsonLex<'_>,
        fname: &'v [u8],
        _isnull: bool,
    ) -> PgResult<bool> {
        if lex.lex_level > 1 {
            return Ok(true);
        }
        // C: names >= NAMEDATALEN can't match a record field.
        if fname.len() >= NAMEDATALEN {
            return Ok(true);
        }
        let val = match self.save_json_start {
            Some(start) => Some(&self.input[start..lex.prev_token_terminator]),
            None => self.saved_scalar,
        };
        // C hash_search HASH_ENTER: a later duplicate overrides the earlier.
        self.hash.insert(fname, JsonHashEntry { ttype: self.saved_token_type, val });
        Ok(true)
    }

    fn array_start(&mut self, lex: &JsonLex<'_>) -> PgResult<bool> {
        if lex.lex_level == 0 {
            return Err(cannot_call_on(self.function_name, "an array"));
        }
        Ok(true)
    }

    fn scalar(&mut self, lex: &JsonLex<'_>, token: JsonSemToken<'v>) -> PgResult<bool> {
        if lex.lex_level == 0 {
            return Err(cannot_call_on(self.function_name, "a scalar"));
        }
        if lex.lex_level == 1 {
            self.saved_scalar = Some(scalar_token_bytes(&token));
        }
        Ok(true)
    }
}

// C get_json_object_as_hash: None = parse failed softly.
fn get_json_object_as_hash<'v>(
    mcx: Mcx<'v>,
    json: &'v [u8],
    function_name: &'static str,
    escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<Option<JsonHash<'v>>> {
    let mut hash: JsonHash<'v> = PgHashMap::with_capacity_in(16, mcx);
    let mut lex = JsonLexDe::new(mcx, json, mbutils::GetDatabaseEncoding());
    let mut sem = JHashSem {
        function_name,
        hash: &mut hash,
        input: json,
        saved_scalar: None,
        saved_token_type: JsonToken::Invalid,
        save_json_start: None,
    };
    let r = adt_json::jsonapi::parse_sem(&mut lex, &mut sem)?;
    if r != adt_json::jsonapi::JsonError::Success {
        adt_json::errsave_parse_error(r, &lex.lex, escontext.map(|n| &mut n.ctx))?;
        return Ok(None);
    }
    Ok(Some(hash))
}

/// C JsObject.
enum JsObject<'v> {
    Json(JsonHash<'v>),
    Jsonb(Option<&'v [u8]>),
}

// C JsObjectIsEmpty.
fn js_object_is_empty(obj: &JsObject<'_>) -> bool {
    match obj {
        JsObject::Json(h) => h.is_empty(),
        JsObject::Jsonb(c) => c.map_or(true, |c| container_size(c) == 0),
    }
}

// C JsValueToJsObject: None when the error was reported softly.
fn js_value_to_js_object<'v>(
    jsv: &JsValue<'v>,
    mcx: Mcx<'v>,
    escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<Option<JsObject<'v>>> {
    match jsv {
        JsValue::Json { s, .. } => {
            let s = s.expect("non-null json jsv");
            Ok(get_json_object_as_hash(mcx, s, "populate_composite", escontext)?
                .map(JsObject::Json))
        }
        JsValue::Jsonb(v) => match v {
            Some(JsonbItem::Binary(c)) if container_is_object(c) => {
                Ok(Some(JsObject::Jsonb(Some(c))))
            }
            other => {
                let is_scalar = match other {
                    Some(JsonbItem::Binary(c)) => container_is_scalar(c),
                    Some(item) => item.is_scalar(),
                    None => false,
                };
                let what = if is_scalar { "a scalar" } else { "an array" };
                ereturn(
                    escontext.map(|n| &mut n.ctx),
                    None,
                    PgError::error(alloc::format!("cannot call populate_composite on {what}"))
                        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
                )
            }
        },
    }
}

// C JsObjectGetField.
fn js_object_get_field<'v>(obj: &JsObject<'v>, field: &[u8]) -> (bool, JsValue<'v>) {
    match obj {
        JsObject::Json(h) => match h.get(field) {
            Some(e) => (
                true,
                JsValue::Json {
                    s: if e.ttype == JsonToken::Null { None } else { e.val },
                    ttype: e.ttype,
                },
            ),
            None => (false, JsValue::Json { s: None, ttype: JsonToken::Null }),
        },
        JsObject::Jsonb(cont) => {
            let v = cont.and_then(|c| get_key_value(c, field));
            let found = v.is_some();
            (found, JsValue::Jsonb(v))
        }
    }
}

enum PopulatedRecord {
    Filled,
    Defaulted,
}

// C populate_record: fills `values`/`nulls`; Defaulted = the empty-object
// defaultval shortcut fired (values/nulls untouched).
#[allow(clippy::too_many_arguments)]
fn populate_record<'c>(
    tupdesc: &TupleDescData<'_>,
    record_io: &mut Option<RecordIoData<'c>>,
    cache_mcx: Mcx<'c>,
    defaultval: Option<&[u8]>,
    mcx: Mcx<'_>,
    obj: &JsObject<'_>,
    mut escontext: Option<&mut ErrorSaveNode>,
    values: &mut [Datum],
    nulls: &mut [bool],
) -> PgResult<PopulatedRecord> {
    let ncolumns = tupdesc.natts as usize;

    // C: an empty input can only skip the work when a non-null record came
    // in, else domain nulls would go unchecked.
    if defaultval.is_some() && js_object_is_empty(obj) {
        return Ok(PopulatedRecord::Defaulted);
    }

    let refresh = match record_io.as_ref() {
        None => true,
        Some(r) => r.columns.len() != ncolumns,
    };
    if refresh {
        let mut columns: alloc::vec::Vec<Option<ColumnIoData<'c>>> =
            alloc::vec::Vec::with_capacity(ncolumns);
        columns.resize_with(ncolumns, || None);
        *record_io = Some(RecordIoData {
            record_type: InvalidOid,
            record_typmod: 0,
            columns,
        });
    }
    let record = record_io.as_mut().expect("record_io just filled");
    if record.record_type != tupdesc.tdtypeid || record.record_typmod != tupdesc.tdtypmod {
        for c in record.columns.iter_mut() {
            *c = None;
        }
        record.record_type = tupdesc.tdtypeid;
        record.record_typmod = tupdesc.tdtypmod;
    }

    if let Some(dfl) = defaultval {
        // SAFETY: dfl is a detoasted composite image (header prefix in bounds).
        let hdr = unsafe { &*(dfl.as_ptr() as *const HeapTupleHeaderData) };
        // SAFETY: MAXALIGN'd detoasted image of datum_length() bytes.
        let tuple = unsafe {
            HeapTupleData::from_raw_parts(
                dfl.as_ptr(),
                hdr.datum_length(),
                ItemPointerData::invalid(),
                InvalidOid,
            )
        };
        types_tuple::heap_deform_tuple(&tuple, tupdesc, values, nulls);
    } else {
        for i in 0..ncolumns {
            values[i] = Datum::null();
            nulls[i] = true;
        }
    }

    for i in 0..ncolumns {
        let att = &tupdesc.attrs[i];
        if att.attisdropped {
            nulls[i] = true;
            continue;
        }
        let colname = att.attname.name_str();
        let (found, field) = js_object_get_field(obj, colname);

        // C: a missing key still runs the input function over a null default
        // (domain types); with a non-null record the existing value stands.
        if defaultval.is_some() && !found {
            continue;
        }

        let need = match record.columns[i].as_ref() {
            None => true,
            Some(c) => c.typid != att.atttypid || c.typmod != att.atttypmod,
        };
        if need {
            record.columns[i] = Some(ColumnIoData::new(cache_mcx, att.atttypid, att.atttypmod)?);
        }
        let col = record.columns[i].as_mut().expect("column cache just filled");
        let dfl_datum = if nulls[i] { None } else { Some(values[i]) };
        // SAFETY: attnames are valid server-encoding text.
        let colname_str = unsafe { core::str::from_utf8_unchecked(colname) };
        values[i] = populate_record_field(
            col,
            Some(colname_str),
            mcx,
            dfl_datum,
            &field,
            &mut nulls[i],
            escontext.as_deref_mut(),
            false,
        )?;
    }
    Ok(PopulatedRecord::Filled)
}

// C populate_composite.
#[allow(clippy::too_many_arguments)]
fn populate_composite(
    io: &mut CompositeIoData<'_>,
    typid: Oid,
    _colname: Option<&str>,
    mcx: Mcx<'_>,
    defaultval: Option<&[u8]>,
    jsv: &JsValue<'_>,
    isnull: &mut bool,
    mut escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<Datum> {
    update_cached_tupdesc(io)?;

    let mut result = Datum::null();
    if !*isnull {
        let Some(obj) = js_value_to_js_object(jsv, mcx, escontext.as_deref_mut())? else {
            *isnull = true;
            return Ok(Datum::null());
        };
        let io2 = &mut *io;
        let tupdesc = io2.tupdesc.as_ref().expect("tupdesc just updated");
        let ncolumns = tupdesc.natts as usize;
        let mut values: PgVec<'_, Datum> = mcx::vec_from_elem_in(mcx, Datum::null(), ncolumns);
        let mut nulls: PgVec<'_, bool> = mcx::vec_from_elem_in(mcx, true, ncolumns);
        let r = populate_record(
            tupdesc,
            &mut io2.record_io,
            io2.cache_mcx,
            defaultval,
            mcx,
            &obj,
            escontext.as_deref_mut(),
            &mut values,
            &mut nulls,
        )?;
        if soft_occurred(&escontext) {
            *isnull = true;
            return Ok(Datum::null());
        }
        result = match r {
            PopulatedRecord::Defaulted => {
                Datum::from_usize(defaultval.expect("defaulted shortcut").as_ptr() as usize)
            }
            PopulatedRecord::Filled => {
                let tuple = heaptuple::heap_form_tuple(mcx, tupdesc, &values, &nulls)?;
                // C HeapTupleHeaderGetDatum flattens external fields here.
                if tuple.has_external() {
                    detoast_seams::toast_flatten_tuple_to_datum::call(
                        mcx,
                        tuple.as_tuple(),
                        tupdesc,
                    )?
                } else {
                    let d = Datum::from_usize(tuple.image().as_ptr() as usize);
                    core::mem::forget(tuple);
                    d
                }
            }
        };
    }

    // C: domain over composite — check constraints (RECORD input skips).
    if typid != io.base_typid && typid != RECORDOID {
        typcache_seams::domain_check_input::call(
            result,
            *isnull,
            typid,
            escontext.as_deref_mut().map(|n| &mut n.ctx),
        )?;
        if soft_occurred(&escontext) {
            *isnull = true;
            return Ok(Datum::null());
        }
    }
    Ok(result)
}

// C PopulateArrayContext: `astate` lives in C's ctx.acxt = CurrentMemoryContext.
struct PopulateArrayContext<'e, 'c, 'r> {
    element: &'e mut ColumnIoData<'c>,
    astate: Option<::datum::array_build::ArrayBuildState<'r>>,
    colname: Option<&'e str>,
    mcx: Mcx<'r>,
    ndims: i32,
    dims: PgVec<'r, i32>,
    sizes: PgVec<'r, i32>,
}

#[cold]
fn expected_json_array_error(ctx: &PopulateArrayContext<'_, '_, '_>, ndim: i32) -> PgError {
    let mut e = PgError::error("expected JSON array")
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION);
    if ndim <= 0 {
        if let Some(colname) = ctx.colname {
            e = e.with_hint(alloc::format!("See the value of key \"{colname}\"."));
        }
    } else {
        debug_assert!(ctx.ndims > 0 && ndim < ctx.ndims);
        let mut indices = alloc::string::String::new();
        for i in 0..ndim as usize {
            indices.push_str(&alloc::format!("[{}]", ctx.sizes[i]));
        }
        e = match ctx.colname {
            Some(colname) => e.with_hint(alloc::format!(
                "See the array element {indices} of key \"{colname}\"."
            )),
            None => e.with_hint(alloc::format!("See the array element {indices}.")),
        };
    }
    e
}

// C populate_array_report_expected_array: errsave (soft when escontext armed).
fn populate_array_report_expected_array(
    ctx: &PopulateArrayContext<'_, '_, '_>,
    ndim: i32,
    escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<()> {
    ereturn(
        escontext.map(|n| &mut n.ctx),
        (),
        expected_json_array_error(ctx, ndim),
    )
}

// C populate_array_assign_ndims.
fn populate_array_assign_ndims(
    ctx: &mut PopulateArrayContext<'_, '_, '_>,
    ndims: i32,
    escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<bool> {
    debug_assert!(ctx.ndims <= 0);
    if ndims <= 0 {
        populate_array_report_expected_array(ctx, ndims, escontext)?;
        return Ok(false);
    }
    ctx.ndims = ndims;
    for _ in 0..ndims {
        ctx.dims.push(-1);
        ctx.sizes.push(0);
    }
    Ok(true)
}

// C populate_array_check_dimension.
fn populate_array_check_dimension(
    ctx: &mut PopulateArrayContext<'_, '_, '_>,
    ndim: i32,
    escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<bool> {
    let ndim = ndim as usize;
    let dim = ctx.sizes[ndim];
    if ctx.dims[ndim] == -1 {
        ctx.dims[ndim] = dim;
    } else if ctx.dims[ndim] != dim {
        ereturn(
            escontext.map(|n| &mut n.ctx),
            (),
            PgError::error("malformed JSON array")
                .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                .with_detail(
                    "Multidimensional arrays must have sub-arrays with matching dimensions.",
                ),
        )?;
        return Ok(false);
    }
    ctx.sizes[ndim] = 0;
    if ndim > 0 {
        ctx.sizes[ndim - 1] += 1;
    }
    Ok(true)
}

// C populate_array_element.
fn populate_array_element(
    ctx: &mut PopulateArrayContext<'_, '_, '_>,
    ndim: i32,
    jsv: &JsValue<'_>,
    mut escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<bool> {
    let mut element_isnull = false;
    let element = populate_record_field(
        ctx.element,
        None,
        ctx.mcx,
        None,
        jsv,
        &mut element_isnull,
        escontext.as_deref_mut(),
        false,
    )?;
    if soft_occurred(&escontext) {
        return Ok(false);
    }
    let element_type = ctx.element.typid;
    ctx.astate = Some(arrayfuncs::accum_array_result(
        ctx.mcx,
        ctx.astate.take(),
        element,
        element_isnull,
        element_type,
    )?);
    debug_assert!(ndim > 0);
    ctx.sizes[ndim as usize - 1] += 1;
    Ok(true)
}

// C PopulateArrayState sem actions (populate_array_json).
struct PopulateArraySem<'s, 'e, 'c, 'r, 'v> {
    ctx: &'s mut PopulateArrayContext<'e, 'c, 'r>,
    escontext: Option<&'s mut ErrorSaveNode>,
    input: &'v [u8],
    element_start: Option<usize>,
    element_type: JsonToken,
    element_scalar: Option<&'v [u8]>,
}

impl<'v> JsonSem<'v> for PopulateArraySem<'_, '_, '_, '_, 'v> {
    fn object_start(&mut self, lex: &JsonLex<'_>) -> PgResult<bool> {
        let ndim = lex.lex_level;
        if self.ctx.ndims <= 0 {
            populate_array_assign_ndims(self.ctx, ndim, self.escontext.as_deref_mut())
        } else if ndim < self.ctx.ndims {
            populate_array_report_expected_array(self.ctx, ndim, self.escontext.as_deref_mut())?;
            Ok(false)
        } else {
            Ok(true)
        }
    }

    fn array_end(&mut self, lex: &JsonLex<'_>) -> PgResult<bool> {
        let ndim = lex.lex_level;
        if self.ctx.ndims <= 0
            && !populate_array_assign_ndims(self.ctx, ndim + 1, self.escontext.as_deref_mut())?
        {
            return Ok(false);
        }
        if ndim < self.ctx.ndims {
            return populate_array_check_dimension(self.ctx, ndim, self.escontext.as_deref_mut());
        }
        Ok(true)
    }

    fn array_element_start(&mut self, lex: &JsonLex<'_>, _isnull: bool) -> PgResult<bool> {
        let ndim = lex.lex_level;
        if self.ctx.ndims <= 0 || ndim == self.ctx.ndims {
            self.element_start = lex.token_start;
            self.element_type = lex.token_type;
            self.element_scalar = None;
        }
        Ok(true)
    }

    fn array_element_end(&mut self, lex: &JsonLex<'_>, isnull: bool) -> PgResult<bool> {
        let ndim = lex.lex_level;
        debug_assert!(self.ctx.ndims > 0);
        if ndim != self.ctx.ndims {
            return Ok(true);
        }
        let jsv = if isnull {
            debug_assert_eq!(self.element_type, JsonToken::Null);
            JsValue::Json { s: None, ttype: JsonToken::Null }
        } else if let Some(scalar) = self.element_scalar {
            JsValue::Json { s: Some(scalar), ttype: self.element_type }
        } else {
            let start = self.element_start.expect("element start recorded");
            JsValue::Json {
                s: Some(&self.input[start..lex.prev_token_terminator]),
                ttype: self.element_type,
            }
        };
        populate_array_element(self.ctx, ndim, &jsv, self.escontext.as_deref_mut())
    }

    fn scalar(&mut self, lex: &JsonLex<'_>, token: JsonSemToken<'v>) -> PgResult<bool> {
        let ndim = lex.lex_level;
        if self.ctx.ndims <= 0 {
            if !populate_array_assign_ndims(self.ctx, ndim, self.escontext.as_deref_mut())? {
                return Ok(false);
            }
        } else if ndim < self.ctx.ndims {
            populate_array_report_expected_array(self.ctx, ndim, self.escontext.as_deref_mut())?;
            return Ok(false);
        }
        if ndim == self.ctx.ndims {
            self.element_scalar = Some(scalar_token_bytes(&token));
        }
        Ok(true)
    }
}

// C populate_array_json: false = error reported softly.
fn populate_array_json(
    ctx: &mut PopulateArrayContext<'_, '_, '_>,
    json: &[u8],
    mut escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<bool> {
    let mcx: Mcx<'_> = ctx.mcx;
    let mut lex = JsonLexDe::new(mcx, json, mbutils::GetDatabaseEncoding());
    let mut sem = PopulateArraySem {
        ctx,
        escontext: escontext.as_deref_mut(),
        input: json,
        element_start: None,
        element_type: JsonToken::Invalid,
        element_scalar: None,
    };
    let r = adt_json::jsonapi::parse_sem(&mut lex, &mut sem)?;
    match r {
        adt_json::jsonapi::JsonError::Success => {
            debug_assert!(sem.ctx.ndims > 0);
        }
        adt_json::jsonapi::JsonError::SemActionFailed => {}
        err => {
            adt_json::errsave_parse_error(
                err,
                &lex.lex,
                escontext.as_deref_mut().map(|n| &mut n.ctx),
            )?;
        }
    }
    Ok(!soft_occurred(&escontext))
}

// C populate_array_dim_jsonb.
fn populate_array_dim_jsonb(
    ctx: &mut PopulateArrayContext<'_, '_, '_>,
    jbv: JsonbItem<'_>,
    ndim: i32,
    mut escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<bool> {
    check_stack_depth()?;

    // C: even scalars can end up here thanks to ExecEvalJsonCoercion().
    let jbc = match jbv {
        JsonbItem::Binary(c) if container_is_array(c) && !container_is_scalar(c) => c,
        _ => {
            populate_array_report_expected_array(ctx, ndim - 1, escontext)?;
            return Ok(false);
        }
    };

    let mut it = JsonbIterator::init(ctx.mcx, jbc)?;
    let (tok, _) = it.next(true);
    debug_assert_eq!(tok, WjbToken::BeginArray);

    let (mut tok, mut val) = it.next(true);

    if ctx.ndims <= 0
        && (tok == WjbToken::EndArray
            || (tok == WjbToken::Elem
                && !matches!(val, JsonbItem::Binary(c) if container_is_array(c))))
    {
        if !populate_array_assign_ndims(ctx, ndim, escontext.as_deref_mut())? {
            return Ok(false);
        }
    }

    while tok == WjbToken::Elem {
        if ctx.ndims > 0 && ndim >= ctx.ndims {
            if !populate_array_element(
                ctx,
                ndim,
                &JsValue::Jsonb(Some(val)),
                escontext.as_deref_mut(),
            )? {
                return Ok(false);
            }
        } else {
            if !populate_array_dim_jsonb(ctx, val, ndim + 1, escontext.as_deref_mut())? {
                return Ok(false);
            }
            debug_assert!(ctx.ndims > 0);
            if !populate_array_check_dimension(ctx, ndim, escontext.as_deref_mut())? {
                return Ok(false);
            }
        }
        (tok, val) = it.next(true);
    }

    debug_assert_eq!(tok, WjbToken::EndArray);
    let (tok, _) = it.next(true);
    debug_assert_eq!(tok, WjbToken::Done);
    Ok(true)
}

// C populate_array.
fn populate_array(
    element: &mut ColumnIoData<'_>,
    colname: Option<&str>,
    mcx: Mcx<'_>,
    jsv: &JsValue<'_>,
    isnull: &mut bool,
    mut escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<Datum> {
    let element_type = element.typid;
    let mut ctx = PopulateArrayContext {
        element,
        astate: Some(arrayfuncs::init_array_result(mcx, element_type, true)?),
        colname,
        mcx,
        ndims: 0,
        dims: mcx::vec_with_capacity_in(mcx, 1)?,
        sizes: mcx::vec_with_capacity_in(mcx, 1)?,
    };

    match jsv {
        JsValue::Json { s, .. } => {
            let s = s.expect("non-null json jsv");
            if !populate_array_json(&mut ctx, s, escontext.as_deref_mut())? {
                *isnull = true;
                return Ok(Datum::null());
            }
        }
        JsValue::Jsonb(v) => {
            let jbv = v.as_ref().expect("non-null jsonb jsv");
            if !populate_array_dim_jsonb(&mut ctx, *jbv, 1, escontext.as_deref_mut())? {
                *isnull = true;
                return Ok(Datum::null());
            }
            ctx.dims[0] = ctx.sizes[0];
        }
    }

    debug_assert!(ctx.ndims > 0);
    let mut lbs: PgVec<'_, i32> = mcx::vec_with_capacity_in(mcx, ctx.ndims as usize)?;
    for _ in 0..ctx.ndims {
        lbs.push(1);
    }
    let image = arrayfuncs::make_md_array_result(
        mcx,
        ctx.astate.as_ref().expect("astate initialized"),
        ctx.ndims,
        &ctx.dims,
        &lbs,
    )?;
    *isnull = false;
    Ok(image_datum(image))
}

// C JsonbUnquote (jsonb.c): quote-stripped text of a scalar, else the
// serialized container. No trailing NUL.
fn jsonb_unquote<'r>(mcx: Mcx<'r>, payload: &[u8]) -> PgResult<PgVec<'r, u8>> {
    match crate::io::extract_scalar(payload) {
        Some(JsonbItem::String(s)) => {
            let mut v = mcx::vec_with_capacity_in(mcx, s.len())?;
            mcx::vec_append_bytes(&mut v, s)?;
            Ok(v)
        }
        Some(JsonbItem::Bool(b)) => {
            let s: &[u8] = if b { b"true" } else { b"false" };
            let mut v = mcx::vec_with_capacity_in(mcx, s.len())?;
            mcx::vec_append_bytes(&mut v, s)?;
            Ok(v)
        }
        Some(JsonbItem::Numeric(image)) => {
            let mut scratch = alloc::vec::Vec::new();
            adt_numeric::numeric_out_into(adt_numeric::Num::from_payload(&image[4..]), &mut scratch);
            let mut v = mcx::vec_with_capacity_in(mcx, scratch.len())?;
            mcx::vec_append_bytes(&mut v, &scratch)?;
            Ok(v)
        }
        Some(JsonbItem::Null) => {
            let mut v = mcx::vec_with_capacity_in(mcx, 4)?;
            mcx::vec_append_bytes(&mut v, b"null")?;
            Ok(v)
        }
        Some(other) => panic!("unrecognized jsonb value type {}", other.type_ord()),
        None => {
            let mut out = StringInfo::new_in(mcx)?;
            crate::io::jsonb_to_cstring_into(mcx, &mut out, payload, payload.len() + 4)?;
            Ok(out.into_vec())
        }
    }
}

/// C PopulateRecordCache, riding fn_extra (its lifetime replaces fn_mcxt).
/// `own` plays C's fn_mcxt: the ColumnIOData tree allocates from it.
pub struct PopulateRecordCache {
    argtype: Oid,
    // SAFETY invariant: every 'static allocation inside `c` lives in `own`'s
    // arena; `c` is declared first so it drops before `own`, and `own` is
    // boxed so its arena address survives moves of this struct.
    c: Option<ColumnIoData<'static>>,
    // std Box justified: rides FmgrInfo.fn_extra (FnExtra slot), written once
    // per resolved FmgrInfo.
    own: alloc::boxed::Box<MemoryContext>,
}

impl PopulateRecordCache {
    fn new() -> Self {
        PopulateRecordCache {
            argtype: InvalidOid,
            c: None,
            own: alloc::boxed::Box::new(MemoryContext::new("PopulateRecordCache")),
        }
    }

    fn mcx(&self) -> Mcx<'static> {
        // SAFETY: see the struct invariant — allocations never outlive `own`.
        unsafe { core::mem::transmute::<Mcx<'_>, Mcx<'static>>(self.own.mcx()) }
    }

    fn composite_io_mut(&mut self) -> &mut CompositeIoData<'static> {
        self.c
            .as_mut()
            .and_then(|c| c.composite_io_mut())
            .expect("record cache holds a composite ColumnIoData")
    }
}

#[track_caller]
#[cold]
fn not_a_row_type(funcname: &str) -> Box<PgError> {
    Box::new(
        PgError::error(alloc::format!(
            "first argument of {funcname} must be a row type"
        ))
        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH),
    )
}

#[track_caller]
#[cold]
fn cannot_determine_row_type(funcname: &str) -> Box<PgError> {
    Box::new(
        PgError::error(alloc::format!(
            "could not determine row type for result of {funcname}"
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .with_hint(
            "Provide a non-null record argument, or call the function in the FROM clause \
             using a column definition list.",
        ),
    )
}

// C get_record_type_from_argument.
fn get_record_type_from_argument(
    flinfo: &FmgrInfo,
    funcname: &str,
    cache: &mut PopulateRecordCache,
) -> PgResult<()> {
    let argtype = funcapi::get_fn_expr_argtype(Some(flinfo), 0);
    cache.argtype = argtype;
    let c = ColumnIoData::new(cache.mcx(), argtype, -1)?;
    if !matches!(
        c.kind,
        ColumnKind::Composite { .. } | ColumnKind::CompositeDomain { .. }
    ) {
        return Err(not_a_row_type(funcname));
    }
    cache.c = Some(c);
    Ok(())
}

// C get_record_type_from_query.
fn get_record_type_from_query(
    flinfo: &FmgrInfo,
    fcinfo: &mut Fcinfo,
    funcname: &str,
    cache: &mut PopulateRecordCache,
) -> PgResult<()> {
    // SAFETY: the armed result mcx outlives this call frame (never re-armed).
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    // SAFETY: expectedDesc contract — the executor armed it with the scan
    // tupdesc, live for the duration of this call.
    let expected = fcinfo
        .rsinfo_mut()
        .and_then(|rsi| rsi.expectedDesc)
        .map(|p| unsafe { p.cast::<TupleDescData<'_>>().as_ref() });
    let resolved = funcapi::get_call_result_type(mcx, flinfo, expected)?;
    if resolved.class != funcapi::TypeFuncClass::Composite {
        return Err(cannot_determine_row_type(funcname));
    }
    let src = resolved.result_tuple_desc.expect("composite result carries a tupdesc");
    cache.argtype = src.tdtypeid;
    if cache.c.is_none() {
        cache.c = Some(ColumnIoData::new(cache.mcx(), RECORDOID, -1)?);
    }
    let cache_mcx = cache.mcx();
    let io = cache.composite_io_mut();
    io.tupdesc = Some(tupdesc::CreateTupleDescCopy(cache_mcx, &src)?);
    io.base_typid = src.tdtypeid;
    io.base_typmod = src.tdtypmod;
    Ok(())
}

// Take the cache out of the fn_extra slot as an opaque handle (C keeps it in
// fn_mcxt across errors; callers must restore it). The memo type is statically
// bound to the resolved function per the FnExtra wiring contract.
fn take_cache(flinfo: &mut FmgrInfo) -> Option<types_fmgr::FnExtra> {
    flinfo.fn_extra.take()
}

// C PG_GETARG_HEAPTUPLEHEADER: detoasted composite arg image.
fn arg_record<'mcx>(fcinfo: &Fcinfo, i: usize, mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u8>> {
    // SAFETY: catalog arg i is a non-null composite datum (checked null).
    unsafe { detoast_composite(mcx, fcinfo.arg(i)) }
}

fn record_header(rec: &[u8]) -> &HeapTupleHeaderData {
    // SAFETY: a detoasted composite image; header prefix is in bounds.
    unsafe { &*(rec.as_ptr() as *const HeapTupleHeaderData) }
}

// C populate_record_worker.
fn populate_record_worker(
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
    funcname: &'static str,
    is_json: bool,
    have_record_arg: bool,
    mut escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<Datum> {
    let json_arg_num = if have_record_arg { 1 } else { 0 };
    // SAFETY: the armed result mcx outlives this call frame (never re-armed).
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    let mut cache_extra = match take_cache(flinfo) {
        Some(c) => c,
        None => {
            let mut cache = PopulateRecordCache::new();
            let r = if have_record_arg {
                get_record_type_from_argument(flinfo, funcname, &mut cache)
            } else {
                get_record_type_from_query(flinfo, fcinfo, funcname, &mut cache)
            };
            if let Err(e) = r {
                return Err(e);
            }
            types_fmgr::FnExtra::new(cache)
        }
    };
    let mut cache = cache_extra.downcast_mut::<PopulateRecordCache>();

    let mut rec_holder: Option<PgVec<'_, u8>> = None;
    // C keeps the cache in fn_mcxt across errors; restore before `?`.
    let result = (|| -> PgResult<Option<Datum>> {
        if have_record_arg && !fcinfo.argisnull(0) {
            let rec = rec_holder.insert(arg_record(fcinfo, 0, mcx)?);
            // C: a declared RECORD arg carries its concrete type in the tuple.
            if cache.argtype == RECORDOID {
                let hdr = record_header(rec);
                let io = cache.composite_io_mut();
                io.base_typid = hdr.type_id();
                io.base_typmod = hdr.typmod();
            }
        } else if have_record_arg && cache.argtype == RECORDOID {
            get_record_type_from_query(flinfo, fcinfo, funcname, &mut cache)?;
            debug_assert!(cache.argtype == RECORDOID);
        }

        if fcinfo.argisnull(json_arg_num) {
            return Ok(rec_holder
                .as_ref()
                .map(|rec| Datum::from_usize(rec.as_ptr() as usize)));
        }

        let mut text_holder = None;
        let mut payload_holder = None;
        let jsv = if is_json {
            // SAFETY: arg checked non-null; live text varlena.
            let t = text_holder
                .insert(unsafe { text_payload_from_datum(mcx, fcinfo.arg(json_arg_num))? });
            JsValue::Json { s: Some(&t[..]), ttype: JsonToken::Invalid }
        } else {
            let p = payload_holder.insert(crate::builtins::arg_jsonb(fcinfo, json_arg_num, mcx)?);
            JsValue::Jsonb(Some(JsonbItem::Binary(p.as_bytes())))
        };

        let mut isnull = false;
        let argtype = cache.argtype;
        let io = cache.composite_io_mut();
        let rettuple = populate_composite(
            io,
            argtype,
            None,
            mcx,
            rec_holder.as_ref().map(|v| &v[..]),
            &jsv,
            &mut isnull,
            escontext.as_deref_mut(),
        )?;
        debug_assert!(!isnull || soft_occurred(&escontext));
        Ok(Some(rettuple))
    })();
    flinfo.fn_extra = Some(cache_extra);
    match result? {
        Some(d) => {
            // The result may point into the detoasted record arg; leak it
            // into mcx (C: the copy lives in the calling context).
            if let Some(rec) = rec_holder {
                core::mem::forget(rec);
            }
            Ok(d)
        }
        None => Ok(fcinfo.return_null()),
    }
}

// C populate_recordset_record.
#[allow(clippy::too_many_arguments)]
fn populate_recordset_record(
    io: &mut CompositeIoData<'static>,
    argtype: Oid,
    is_composite_domain: bool,
    rec: Option<&[u8]>,
    store: &mut tuplestore::Tuplestore,
    mcx: Mcx<'_>,
    obj: &JsObject<'_>,
) -> PgResult<()> {
    update_cached_tupdesc(io)?;
    let io2 = &mut *io;
    let tupdesc = io2.tupdesc.as_ref().expect("tupdesc just updated");
    let ncolumns = tupdesc.natts as usize;
    let mut values: PgVec<'_, Datum> = mcx::vec_from_elem_in(mcx, Datum::null(), ncolumns);
    let mut nulls: PgVec<'_, bool> = mcx::vec_from_elem_in(mcx, true, ncolumns);
    let r = populate_record(
        tupdesc,
        &mut io2.record_io,
        io2.cache_mcx,
        rec,
        mcx,
        obj,
        None,
        &mut values,
        &mut nulls,
    )?;
    if let PopulatedRecord::Defaulted = r {
        let dfl = rec.expect("defaulted shortcut");
        let hdr = record_header(dfl);
        // SAFETY: MAXALIGN'd detoasted image of datum_length() bytes.
        let tuple = unsafe {
            HeapTupleData::from_raw_parts(
                dfl.as_ptr(),
                hdr.datum_length(),
                ItemPointerData::invalid(),
                InvalidOid,
            )
        };
        types_tuple::heap_deform_tuple(&tuple, tupdesc, &mut values, &mut nulls);
    }
    if is_composite_domain {
        let tuple = heaptuple::heap_form_tuple(mcx, tupdesc, &values, &nulls)?;
        // C HeapTupleHeaderGetDatum flattens external fields here.
        let d = if tuple.has_external() {
            detoast_seams::toast_flatten_tuple_to_datum::call(mcx, tuple.as_tuple(), tupdesc)?
        } else {
            Datum::from_usize(tuple.image().as_ptr() as usize)
        };
        typcache_seams::domain_check_input::call(d, false, argtype, None)?;
    }
    store.putvalues(tupdesc, &values, &nulls)?;
    Ok(())
}

// C PopulateRecordsetState sem actions (json leg).
struct RecordsetSem<'a, 'v> {
    function_name: &'static str,
    io: &'a mut CompositeIoData<'static>,
    argtype: Oid,
    is_composite_domain: bool,
    rec: Option<&'a [u8]>,
    store: &'a mut tuplestore::Tuplestore,
    mcx: Mcx<'v>,
    input: &'v [u8],
    saved_scalar: Option<&'v [u8]>,
    saved_token_type: JsonToken,
    save_json_start: Option<usize>,
    json_hash: Option<JsonHash<'v>>,
}

impl<'v> JsonSem<'v> for RecordsetSem<'_, 'v> {
    fn object_start(&mut self, lex: &JsonLex<'_>) -> PgResult<bool> {
        let lex_level = lex.lex_level;
        if lex_level == 0 {
            return Err(cannot_call_on(self.function_name, "an object"));
        }
        if lex_level == 1 {
            self.json_hash = Some(PgHashMap::with_capacity_in(16, self.mcx));
        }
        Ok(true)
    }

    fn object_end(&mut self, lex: &JsonLex<'_>) -> PgResult<bool> {
        if lex.lex_level > 1 {
            return Ok(true);
        }
        let hash = self.json_hash.take().expect("level-1 object opened a hash");
        let obj = JsObject::Json(hash);
        populate_recordset_record(
            self.io,
            self.argtype,
            self.is_composite_domain,
            self.rec,
            self.store,
            self.mcx,
            &obj,
        )?;
        Ok(true)
    }

    fn array_element_start(&mut self, lex: &JsonLex<'_>, _isnull: bool) -> PgResult<bool> {
        if lex.lex_level == 1 && lex.token_type != JsonToken::ObjectStart {
            return Err(Box::new(
                PgError::error(alloc::format!(
                    "argument of {} must be an array of objects",
                    self.function_name
                ))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
            ));
        }
        Ok(true)
    }

    fn scalar(&mut self, lex: &JsonLex<'_>, token: JsonSemToken<'v>) -> PgResult<bool> {
        if lex.lex_level == 0 {
            return Err(cannot_call_on(self.function_name, "a scalar"));
        }
        if lex.lex_level == 2 {
            self.saved_scalar = Some(scalar_token_bytes(&token));
        }
        Ok(true)
    }

    fn object_field_start(
        &mut self,
        lex: &JsonLex<'_>,
        _fname: &'v [u8],
        _isnull: bool,
    ) -> PgResult<bool> {
        if lex.lex_level > 2 {
            return Ok(true);
        }
        self.saved_token_type = lex.token_type;
        self.save_json_start = if matches!(lex.token_type, JsonToken::ArrayStart | JsonToken::ObjectStart)
        {
            lex.token_start
        } else {
            None
        };
        Ok(true)
    }

    fn object_field_end(
        &mut self,
        lex: &JsonLex<'_>,
        fname: &'v [u8],
        _isnull: bool,
    ) -> PgResult<bool> {
        if lex.lex_level > 2 {
            return Ok(true);
        }
        if fname.len() >= NAMEDATALEN {
            return Ok(true);
        }
        let val = match self.save_json_start {
            Some(start) => Some(&self.input[start..lex.prev_token_terminator]),
            None => self.saved_scalar,
        };
        if let Some(hash) = self.json_hash.as_mut() {
            hash.insert(fname, JsonHashEntry { ttype: self.saved_token_type, val });
        }
        Ok(true)
    }
}

#[track_caller]
#[cold]
fn srf_context_error() -> Box<PgError> {
    Box::new(
        PgError::error("set-valued function called in context that cannot accept a set")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

#[track_caller]
#[cold]
fn materialize_required() -> Box<PgError> {
    Box::new(
        PgError::error("materialize mode required, but it is not allowed in this context")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

// C populate_recordset_worker.
fn populate_recordset_worker(
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
    funcname: &'static str,
    is_json: bool,
    have_record_arg: bool,
) -> PgResult<Datum> {
    let json_arg_num = if have_record_arg { 1 } else { 0 };
    // SAFETY: the armed result mcx outlives this call frame (never re-armed).
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    let allowed_modes = match fcinfo.rsinfo_mut() {
        Some(rsi) => rsi.allowedModes,
        None => return Err(srf_context_error()),
    };
    if allowed_modes & SFRM_Materialize == 0 {
        return Err(materialize_required());
    }
    // C sets returnMode before any early return: a NULL-json exit must read
    // as Materialize-with-no-store (empty set), not value-per-call.
    fcinfo.rsinfo_mut().expect("checked above").returnMode = SetFunctionReturnMode::Materialize;

    let mut cache_extra = match take_cache(flinfo) {
        Some(c) => c,
        None => {
            let mut cache = PopulateRecordCache::new();
            let r = if have_record_arg {
                get_record_type_from_argument(flinfo, funcname, &mut cache)
            } else {
                get_record_type_from_query(flinfo, fcinfo, funcname, &mut cache)
            };
            if let Err(e) = r {
                return Err(e);
            }
            types_fmgr::FnExtra::new(cache)
        }
    };
    let mut cache = cache_extra.downcast_mut::<PopulateRecordCache>();

    let mut rec_holder: Option<PgVec<'_, u8>> = None;
    if have_record_arg && !fcinfo.argisnull(0) {
        let rec = rec_holder.insert(arg_record(fcinfo, 0, mcx)?);
        if cache.argtype == RECORDOID {
            let hdr = record_header(rec);
            let io = cache.composite_io_mut();
            io.base_typid = hdr.type_id();
            io.base_typmod = hdr.typmod();
        }
    } else if have_record_arg && cache.argtype == RECORDOID {
        if let Err(e) = get_record_type_from_query(flinfo, fcinfo, funcname, &mut cache) {
            return Err(e);
        }
    }

    // C: null json sends back an empty set.
    if fcinfo.argisnull(json_arg_num) {
        flinfo.fn_extra = Some(cache_extra);
        return Ok(fcinfo.return_null());
    }

    let argtype = cache.argtype;
    let is_composite_domain = matches!(
        cache.c.as_ref().map(|c| &c.kind),
        Some(ColumnKind::CompositeDomain { .. })
    );
    let io = cache.composite_io_mut();
    // C: forcibly update the cached tupdesc even if the JSON has no rows.
    update_cached_tupdesc(io)?;

    let mut store = tuplestore::Tuplestore::begin_heap(
        allowed_modes & SFRM_Materialize_Random != 0,
        false,
        init_small::globals::work_mem(),
    );

    let mut run = || -> PgResult<()> {
        if is_json {
            // SAFETY: arg checked non-null; live text varlena.
            let t = unsafe { text_payload_from_datum(mcx, fcinfo.arg(json_arg_num))? };
            let json: &[u8] = &t;
            let mut lex = JsonLexDe::new(mcx, json, mbutils::GetDatabaseEncoding());
            let mut sem = RecordsetSem {
                function_name: funcname,
                io,
                argtype,
                is_composite_domain,
                rec: rec_holder.as_ref().map(|v| &v[..]),
                store: &mut store,
                mcx,
                input: json,
                saved_scalar: None,
                saved_token_type: JsonToken::Invalid,
                save_json_start: None,
                json_hash: None,
            };
            let r = adt_json::jsonapi::parse_sem(&mut lex, &mut sem)?;
            if r != adt_json::jsonapi::JsonError::Success {
                adt_json::errsave_parse_error(r, &lex.lex, None)?;
            }
        } else {
            let p = crate::builtins::arg_jsonb(fcinfo, json_arg_num, mcx)?;
            let payload = p.as_bytes();
            if container_is_scalar(payload) || !container_is_array(payload) {
                return Err(cannot_call_on(funcname, "a non-array"));
            }
            let mut it = JsonbIterator::init(mcx, payload)?;
            loop {
                let (tok, v) = it.next(true);
                match tok {
                    WjbToken::Done => break,
                    WjbToken::Elem => {
                        let JsonbItem::Binary(c) = v else {
                            return Err(Box::new(
                                PgError::error(alloc::format!(
                                    "argument of {funcname} must be an array of objects"
                                ))
                                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
                            ));
                        };
                        if !container_is_object(c) {
                            return Err(Box::new(
                                PgError::error(alloc::format!(
                                    "argument of {funcname} must be an array of objects"
                                ))
                                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
                            ));
                        }
                        let obj = JsObject::Jsonb(Some(c));
                        populate_recordset_record(
                            io,
                            argtype,
                            is_composite_domain,
                            rec_holder.as_ref().map(|v| &v[..]),
                            &mut store,
                            mcx,
                            &obj,
                        )?;
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    };
    let r = run();
    flinfo.fn_extra = Some(cache_extra);
    r?;

    let rsi = fcinfo.rsinfo_mut().expect("checked above");
    rsi.returnMode = SetFunctionReturnMode::Materialize;
    rsi.setResult = Some(alloc::boxed::Box::new(store));
    // C: rsi->setDesc = CreateTupleDescCopy(cache tupdesc) — the executor's
    // tupledesc_match runs against it. The cache outlives the call (fn_extra).
    let cache_ref = flinfo
        .fn_extra_ref::<PopulateRecordCache>()
        .expect("cache just restored");
    if let Some(io) = cache_ref.c.as_ref().and_then(|c| c.composite_io_ref()) {
        if let Some(td) = io.tupdesc.as_ref() {
            rsi.setDesc = Some(
                core::ptr::NonNull::from(td).cast::<core::ffi::c_void>(),
            );
        }
    }
    Ok(fcinfo.return_null())
}

fn require_flinfo<'a>(
    flinfo: Option<&'a mut FmgrInfo>,
    name: &str,
) -> &'a mut FmgrInfo {
    flinfo.unwrap_or_else(|| panic!("{name}: NULL flinfo"))
}

pub fn fc_jsonb_populate_record(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = require_flinfo(flinfo, "jsonb_populate_record");
    populate_record_worker(flinfo, fcinfo, "jsonb_populate_record", false, true, None)
}

pub fn fc_jsonb_populate_record_valid(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = require_flinfo(flinfo, "jsonb_populate_record_valid");
    let mut escontext = ErrorSaveNode::new(false);
    populate_record_worker(
        flinfo,
        fcinfo,
        "jsonb_populate_record",
        false,
        true,
        Some(&mut escontext),
    )?;
    Ok(Datum::from_bool(!escontext.ctx.error_occurred()))
}

pub fn fc_jsonb_to_record(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = require_flinfo(flinfo, "jsonb_to_record");
    populate_record_worker(flinfo, fcinfo, "jsonb_to_record", false, false, None)
}

pub fn fc_json_populate_record(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = require_flinfo(flinfo, "json_populate_record");
    populate_record_worker(flinfo, fcinfo, "json_populate_record", true, true, None)
}

pub fn fc_json_to_record(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = require_flinfo(flinfo, "json_to_record");
    populate_record_worker(flinfo, fcinfo, "json_to_record", true, false, None)
}

pub fn fc_jsonb_populate_recordset(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = require_flinfo(flinfo, "jsonb_populate_recordset");
    populate_recordset_worker(flinfo, fcinfo, "jsonb_populate_recordset", false, true)
}

pub fn fc_jsonb_to_recordset(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = require_flinfo(flinfo, "jsonb_to_recordset");
    populate_recordset_worker(flinfo, fcinfo, "jsonb_to_recordset", false, false)
}

pub fn fc_json_populate_recordset(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = require_flinfo(flinfo, "json_populate_recordset");
    populate_recordset_worker(flinfo, fcinfo, "json_populate_recordset", true, true)
}

pub fn fc_json_to_recordset(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = require_flinfo(flinfo, "json_to_recordset");
    populate_recordset_worker(flinfo, fcinfo, "json_to_recordset", true, false)
}
