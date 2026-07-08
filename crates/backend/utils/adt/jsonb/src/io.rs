extern crate alloc;

use crate::build::*;
use crate::container::*;
use crate::iter::{JsonbIterator, WjbToken};
use adt_json::jsonapi::{parse_sem, JsonError, JsonLex, JsonLexDe, JsonSem, JsonSemToken};
use adt_numeric::Num;
use mcx::{Mcx, PgVec};
use stringinfo::StringInfo;
use types_error::{ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_PROGRAM_LIMIT_EXCEEDED};

// C: checkStringLen. Ok(false) = soft-failed into escontext.
fn check_string_len(
    len: usize,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    if len > JENTRY_OFFLENMASK as usize {
        let err = PgError::error("string too long to represent as jsonb string")
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .with_detail(alloc::format!(
                "Due to an implementation restriction, jsonb strings cannot exceed {} bytes.",
                JENTRY_OFFLENMASK
            ));
        ereturn(escontext, false, err)
    } else {
        Ok(true)
    }
}

// C: JsonbInState + the jsonb_in_* semantic actions.
struct JsonbInSink<'s, 'mcx, 'e> {
    mcx: Mcx<'mcx>,
    state: &'s mut JsonbBuildState<'mcx>,
    res: &'s mut Option<JsonbValue<'mcx>>,
    unique_keys: bool,
    escontext: Option<&'e mut SoftErrorContext>,
}

impl<'mcx> JsonbInSink<'_, 'mcx, '_> {
    // C: jsonb_in_scalar tail — lone scalars get the raw-scalar array wrap.
    fn push_scalar(&mut self, v: JsonbValue<'mcx>) -> PgResult<()> {
        if self.state.depth() == 0 {
            self.state.begin_array(true)?;
            self.state.push_elem(v)?;
            *self.res = self.state.end_array()?;
        } else {
            self.push_container_child(v)?;
        }
        Ok(())
    }

    fn push_container_child(&mut self, v: JsonbValue<'mcx>) -> PgResult<()> {
        if self.state.in_array() {
            self.state.push_elem(v)?;
        } else {
            self.state.push_value(v);
        }
        Ok(())
    }
}

impl<'mcx> JsonSem<'mcx> for JsonbInSink<'_, 'mcx, '_> {
    fn object_start(&mut self, _lex: &JsonLex<'_>) -> PgResult<bool> {
        self.state.begin_object(self.unique_keys)?;
        Ok(true)
    }

    fn object_end(&mut self, _lex: &JsonLex<'_>) -> PgResult<bool> {
        if let Some(done) = self.state.end_object()? {
            *self.res = Some(done);
        }
        Ok(true)
    }

    fn array_start(&mut self, _lex: &JsonLex<'_>) -> PgResult<bool> {
        self.state.begin_array(false)?;
        Ok(true)
    }

    fn array_end(&mut self, _lex: &JsonLex<'_>) -> PgResult<bool> {
        if let Some(done) = self.state.end_array()? {
            *self.res = Some(done);
        }
        Ok(true)
    }

    fn object_field_start(
        &mut self,
        _lex: &JsonLex<'_>,
        fname: &'mcx [u8],
        _isnull: bool,
    ) -> PgResult<bool> {
        if !check_string_len(fname.len(), self.escontext.as_deref_mut())? {
            return Ok(false);
        }
        self.state.push_key(fname)?;
        Ok(true)
    }

    fn scalar(&mut self, _lex: &JsonLex<'_>, token: JsonSemToken<'mcx>) -> PgResult<bool> {
        let v = match token {
            JsonSemToken::String(s) => {
                if !check_string_len(s.len(), self.escontext.as_deref_mut())? {
                    return Ok(false);
                }
                JsonbValue::String(s)
            }
            JsonSemToken::Number(tok) => {
                let s = core::str::from_utf8(tok).expect("lexer-validated number is ASCII");
                match adt_numeric::numeric_in(s, -1, self.escontext.as_deref_mut())? {
                    Some(img) => JsonbValue::Numeric(mcx::slice_in(self.mcx, img.as_bytes())?.leak()),
                    None => return Ok(false),
                }
            }
            JsonSemToken::True => JsonbValue::Bool(true),
            JsonSemToken::False => JsonbValue::Bool(false),
            JsonSemToken::Null => JsonbValue::Null,
        };
        self.push_scalar(v)?;
        Ok(true)
    }
}

// C: checkStringLen with a NULL escontext (hard-error path).
pub(crate) fn check_string_len_hard(len: usize) -> PgResult<()> {
    check_string_len(len, None).map(|_| ())
}

/// C: datum_to_jsonb_internal's JSONTYPE_JSON arm — parse json text straight
/// into an open push state via the jsonb_in_* semantic actions.
pub(crate) fn parse_json_into<'mcx>(
    mcx: Mcx<'mcx>,
    ps: &mut crate::mutate::JsonbPush<'mcx>,
    json: &[u8],
) -> PgResult<()> {
    let (state, res) = ps.parts();
    let mut lex = JsonLexDe::new(mcx, json, mbutils::GetDatabaseEncoding());
    let mut sink = JsonbInSink {
        mcx,
        state,
        res,
        unique_keys: false,
        escontext: None,
    };
    let result = parse_sem(&mut lex, &mut sink)?;
    match result {
        JsonError::Success => Ok(()),
        JsonError::SemActionFailed => {
            panic!("JSON semantic action function did not provide error information")
        }
        err => {
            adt_json::errsave_parse_error(err, &lex.lex, None)?;
            unreachable!("hard errsave without escontext returns Err")
        }
    }
}

/// C: jsonb_from_cstring. `None` = a soft-error context absorbed the failure.
pub fn jsonb_from_cstring<'mcx>(
    mcx: Mcx<'mcx>,
    json: &[u8],
    unique_keys: bool,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let mut lex = JsonLexDe::new(mcx, json, mbutils::GetDatabaseEncoding());
    let mut state = JsonbBuildState::new(mcx)?;
    let mut res = None;
    let mut sink = JsonbInSink {
        mcx,
        state: &mut state,
        res: &mut res,
        unique_keys,
        escontext: escontext.as_deref_mut(),
    };
    let result = parse_sem(&mut lex, &mut sink)?;
    match result {
        JsonError::Success => {}
        JsonError::SemActionFailed => {
            // C: json_errsave_error — the action must have recorded a soft error.
            match &escontext {
                Some(esc) if esc.error_occurred() => return Ok(None),
                _ => panic!("JSON semantic action function did not provide error information"),
            }
        }
        err => {
            adt_json::errsave_parse_error(err, &lex.lex, escontext)?;
            return Ok(None);
        }
    }
    drop(sink);
    let val = res.expect("parse succeeded without a result");
    Ok(Some(convert_to_jsonb(mcx, &val)?))
}

/// C: jsonb_in.
pub fn jsonb_in<'mcx>(
    mcx: Mcx<'mcx>,
    input: &[u8],
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    jsonb_from_cstring(mcx, input, false, escontext)
}

/// C: jsonb_recv (version 1 = text payload).
pub fn jsonb_recv<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut StringInfo<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    let version = pqformat::pq_getmsgint(buf, 1)?;
    if version != 1 {
        // C elog(ERROR): XX000, client-reachable via binary input.
        return Err(Box::new(PgError::error(format!(
            "unsupported jsonb version number {version}"
        ))));
    }
    let rawbytes = buf.len().saturating_sub(buf.cursor);
    let str = pqformat::pq_getmsgtext(mcx, buf, rawbytes)?;
    match jsonb_from_cstring(mcx, &str, false, None)? {
        Some(image) => Ok(image),
        None => unreachable!("hard errsave without escontext returns Err"),
    }
}

// C: jsonb_put_escaped_value.
fn put_escaped_value(
    out: &mut StringInfo<'_>,
    v: &JsonbItem<'_>,
    numscratch: &mut alloc::vec::Vec<u8>,
) -> PgResult<()> {
    match v {
        JsonbItem::Null => out.append_bytes(b"null"),
        JsonbItem::String(s) => adt_json::escape_json(out, s),
        JsonbItem::Numeric(image) => {
            // numeric_out_into wants a std Vec; retained scratch owned by the caller.
            numscratch.clear();
            adt_numeric::numeric_out_into(Num::from_payload(&image[4..]), numscratch);
            out.append_bytes(numscratch)
        }
        JsonbItem::Bool(true) => out.append_bytes(b"true"),
        JsonbItem::Bool(false) => out.append_bytes(b"false"),
        _ => panic!("unknown jsonb scalar type"),
    }
}

/// C: JsonbToCString — indent=false. Appends to `out` without a trailing NUL.
pub fn jsonb_to_cstring_into<'mcx>(
    mcx: Mcx<'mcx>,
    out: &mut StringInfo<'_>,
    container: &[u8],
    estimated_len: usize,
) -> PgResult<()> {
    jsonb_to_cstring_worker(mcx, out, container, estimated_len, false)
}

/// C: JsonbToCStringIndent (jsonb_pretty).
pub fn jsonb_to_cstring_indent_into<'mcx>(
    mcx: Mcx<'mcx>,
    out: &mut StringInfo<'_>,
    container: &[u8],
    estimated_len: usize,
) -> PgResult<()> {
    jsonb_to_cstring_worker(mcx, out, container, estimated_len, true)
}

fn add_indent(out: &mut StringInfo<'_>, indent: bool, level: i32) -> PgResult<()> {
    if indent {
        out.append_byte(b'\n')?;
        for _ in 0..level * 4 {
            out.append_byte(b' ')?;
        }
    }
    Ok(())
}

// C: JsonbToCStringWorker.
fn jsonb_to_cstring_worker<'mcx>(
    mcx: Mcx<'mcx>,
    out: &mut StringInfo<'_>,
    container: &[u8],
    estimated_len: usize,
    indent: bool,
) -> PgResult<()> {
    let mut first = true;
    let mut redo: Option<(WjbToken, JsonbItem<'_>)> = None;
    let mut raw_scalar = false;
    let mut level = 0i32;
    let mut numscratch = alloc::vec::Vec::new();
    // C: ispaces — no space after commas when indenting.
    let comma: &[u8] = if indent { b"," } else { b", " };
    let mut use_indent = false;
    let mut last_was_key = false;

    out.enlarge(estimated_len.max(64))?;
    let mut it = JsonbIterator::init(mcx, container)?;

    loop {
        let was_key = last_was_key;
        last_was_key = false;
        let (tok, v) = match redo.take() {
            Some(tv) => tv,
            None => it.next(false),
        };
        match tok {
            WjbToken::Done => break,
            WjbToken::BeginArray => {
                if !first {
                    out.append_bytes(comma)?;
                }
                let JsonbItem::Array { raw_scalar: rs, .. } = v else {
                    unreachable!()
                };
                if !rs {
                    add_indent(out, use_indent && !was_key, level)?;
                    out.append_byte(b'[')?;
                } else {
                    raw_scalar = true;
                }
                first = true;
                level += 1;
            }
            WjbToken::BeginObject => {
                if !first {
                    out.append_bytes(comma)?;
                }
                add_indent(out, use_indent && !was_key, level)?;
                out.append_byte(b'{')?;
                first = true;
                level += 1;
            }
            WjbToken::Key => {
                if !first {
                    out.append_bytes(comma)?;
                }
                first = true;
                add_indent(out, use_indent, level)?;
                put_escaped_value(out, &v, &mut numscratch)?;
                out.append_bytes(b": ")?;
                let (vtok, vv) = it.next(false);
                if vtok == WjbToken::Value {
                    first = false;
                    put_escaped_value(out, &vv, &mut numscratch)?;
                } else {
                    debug_assert!(matches!(vtok, WjbToken::BeginObject | WjbToken::BeginArray));
                    redo = Some((vtok, vv));
                    last_was_key = true;
                }
            }
            WjbToken::Elem => {
                if !first {
                    out.append_bytes(comma)?;
                }
                first = false;
                if !raw_scalar {
                    add_indent(out, use_indent, level)?;
                }
                put_escaped_value(out, &v, &mut numscratch)?;
            }
            WjbToken::EndArray => {
                level -= 1;
                if !raw_scalar {
                    add_indent(out, use_indent, level)?;
                    out.append_byte(b']')?;
                }
                first = false;
            }
            WjbToken::EndObject => {
                level -= 1;
                add_indent(out, use_indent, level)?;
                out.append_byte(b'}')?;
                first = false;
            }
            WjbToken::Value => panic!("unknown jsonb iterator token type"),
        }
        use_indent = indent;
    }
    debug_assert_eq!(level, 0);
    Ok(())
}

/// C: jsonb_out — JsonbToCString over the root container, NUL-terminated.
pub fn jsonb_out<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let mut out = StringInfo::new_in(mcx)?;
    jsonb_to_cstring_into(mcx, &mut out, payload, payload.len() + 4)?;
    let mut v = out.into_vec();
    v.push(0);
    Ok(v)
}

/// C: jsonb_send — version byte + the text rendering.
pub fn jsonb_send<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<datum::Bytea<'mcx>> {
    let mut jtext = StringInfo::new_in(mcx)?;
    jsonb_to_cstring_into(mcx, &mut jtext, payload, payload.len() + 4)?;
    let mut sbuf = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendint8(&mut sbuf, 1)?;
    pqformat::pq_sendtext(&mut sbuf, jtext.as_bytes())?;
    Ok(pqformat::pq_endtypsend(sbuf))
}

/// C: JsonbExtractScalar's success lane — the root must be a raw-scalar
/// pseudo array; returns the sole element.
pub fn extract_scalar<'a>(c: &'a [u8]) -> Option<JsonbItem<'a>> {
    if !container_is_array(c) || !container_is_scalar(c) {
        return None;
    }
    get_ith_value(c, 0)
}

/// C: JsonbContainerTypeName (jsonb_typeof).
pub fn container_type_name(c: &[u8]) -> &'static str {
    match extract_scalar(c) {
        Some(JsonbItem::Null) => "null",
        Some(JsonbItem::String(_)) => "string",
        Some(JsonbItem::Numeric(_)) => "number",
        Some(JsonbItem::Bool(_)) => "boolean",
        Some(_) => panic!("unexpected jsonb scalar type"),
        None if container_is_array(c) => "array",
        None if container_is_object(c) => "object",
        None => panic!("invalid jsonb container type: 0x{:08x}", container_header(c)),
    }
}
