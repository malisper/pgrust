use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_error::{PgError, PgResult, ERRCODE_SYNTAX_ERROR};
use types_nodes::parsenodes::DefElem;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DefValue<'mcx> {
    Int(i32),
    Float(&'mcx str),
    Bool(bool),
    Str(&'mcx str),
}

#[derive(Clone, Copy, Debug)]
pub struct DefItem<'mcx> {
    pub name: &'mcx str,
    pub value: Option<DefValue<'mcx>>,
}

pub fn alloc_str<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let mut v: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, s.len())?;
    mcx::vec_append_bytes(&mut v, s.as_bytes())?;
    // SAFETY: exact copy of a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(v.leak()) })
}

#[track_caller]
#[cold]
fn requires_parameter(name: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("{name} requires a parameter"))
            .with_sqlstate(ERRCODE_SYNTAX_ERROR),
    )
}

#[track_caller]
#[cold]
fn unrecognized_arg(name: &str) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "tsearchcmds def_item_from_defelem: unsupported DefElem arg shape for \"{name}\""
    )))
}

// defGetString's node fan-out (define.c), reduced to the arg shapes the
// CREATE/ALTER TEXT SEARCH grammar can produce.
pub fn def_item_from_defelem<'mcx>(
    mcx: Mcx<'mcx>,
    defel: &DefElem<'mcx>,
) -> PgResult<DefItem<'mcx>> {
    let name = defel.defname.unwrap_or("");
    let value = match defel.arg {
        None => None,
        Some(arg) => Some(if let Some(i) = arg.as_integer() {
            DefValue::Int(i.ival)
        } else if let Some(f) = arg.as_float() {
            DefValue::Float(f.fval)
        } else if let Some(b) = arg.as_boolean() {
            DefValue::Bool(b.boolval)
        } else if let Some(s) = arg.as_string() {
            DefValue::Str(s.sval)
        } else if let Some(t) = arg.as_type_name() {
            DefValue::Str(join_names(mcx, &t.names)?)
        } else if let Some(l) = arg.as_list() {
            DefValue::Str(join_names(mcx, l)?)
        } else {
            return Err(unrecognized_arg(name));
        }),
    };
    Ok(DefItem { name, value })
}

fn join_names<'mcx>(
    mcx: Mcx<'mcx>,
    names: &types_nodes::NodeList<'mcx>,
) -> PgResult<&'mcx str> {
    let mut out: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    for (i, n) in names.iter().enumerate() {
        if i > 0 {
            out.push(b'.');
        }
        let s = n.as_string().expect("qualified name holds Strings").sval;
        mcx::vec_append_bytes(&mut out, s.as_bytes())?;
    }
    // SAFETY: concatenation of &strs and ASCII dots.
    Ok(unsafe { core::str::from_utf8_unchecked(out.leak()) })
}

pub fn def_value_string<'mcx>(mcx: Mcx<'mcx>, item: &DefItem<'mcx>) -> PgResult<&'mcx str> {
    match item.value {
        None => Err(requires_parameter(item.name)),
        Some(DefValue::Int(i)) => alloc_str(mcx, &i.to_string()),
        Some(DefValue::Float(f)) => Ok(f),
        Some(DefValue::Bool(b)) => Ok(if b { "true" } else { "false" }),
        Some(DefValue::Str(s)) => Ok(s),
    }
}

// serialize_deflist (tsearchcmds.c): pg_dump-reloadable option text; Integer/
// Float unquoted, all else single-quoted with '' doubling and E'' when a
// backslash appears.
pub fn serialize_deflist<'mcx>(
    mcx: Mcx<'mcx>,
    items: &[DefItem<'mcx>],
) -> PgResult<PgVec<'mcx, u8>> {
    let mut buf: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    for (i, item) in items.iter().enumerate() {
        let val = def_value_string(mcx, item)?;
        mcx::vec_append_bytes(&mut buf, format_type::quote_identifier(item.name).as_bytes())?;
        mcx::vec_append_bytes(&mut buf, b" = ")?;
        match item.value {
            Some(DefValue::Int(_)) | Some(DefValue::Float(_)) => {
                mcx::vec_append_bytes(&mut buf, val.as_bytes())?;
            }
            _ => {
                if val.contains('\\') {
                    buf.push(b'E');
                }
                buf.push(b'\'');
                for &b in val.as_bytes() {
                    if b == b'\'' || b == b'\\' {
                        buf.push(b);
                    }
                    buf.push(b);
                }
                buf.push(b'\'');
            }
        }
        if i + 1 < items.len() {
            mcx::vec_append_bytes(&mut buf, b", ")?;
        }
    }
    Ok(buf)
}

#[track_caller]
#[cold]
fn invalid_list(input: &[u8]) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "invalid parameter list format: \"{}\"",
            String::from_utf8_lossy(input)
        ))
        .with_sqlstate(ERRCODE_SYNTAX_ERROR),
    )
}

// buildDefItem (tsearchcmds.c): unquoted values re-parse as int, float, then
// boolean literals; quoted values stay strings.
fn build_def_item<'mcx>(
    mcx: Mcx<'mcx>,
    name: &[u8],
    val: &[u8],
    was_quoted: bool,
) -> PgResult<DefItem<'mcx>> {
    let name = alloc_str(mcx, core::str::from_utf8(name).map_err(|_| invalid_list(name))?)?;
    let sval = core::str::from_utf8(val).map_err(|_| invalid_list(val))?;
    if !was_quoted && !sval.is_empty() {
        if let Ok(v) = sval.parse::<i32>() {
            return Ok(DefItem { name, value: Some(DefValue::Int(v)) });
        }
        if sval.parse::<f64>().is_ok() {
            return Ok(DefItem { name, value: Some(DefValue::Float(alloc_str(mcx, sval)?)) });
        }
        if sval == "true" {
            return Ok(DefItem { name, value: Some(DefValue::Bool(true)) });
        }
        if sval == "false" {
            return Ok(DefItem { name, value: Some(DefValue::Bool(false)) });
        }
    }
    Ok(DefItem { name, value: Some(DefValue::Str(alloc_str(mcx, sval)?)) })
}

// deserialize_deflist (tsearchcmds.c): the eight-state scanner, accepting
// unquoted and double-quoted values serialize_deflist never emits.
pub fn deserialize_deflist<'mcx>(
    mcx: Mcx<'mcx>,
    input: &[u8],
) -> PgResult<PgVec<'mcx, DefItem<'mcx>>> {
    #[derive(PartialEq)]
    enum S {
        WaitKey,
        InKey,
        InQKey,
        WaitEq,
        WaitValue,
        InSqValue,
        InDqValue,
        InWValue,
    }
    let mut result: PgVec<'mcx, DefItem<'mcx>> = PgVec::new_in(mcx);
    let mut key: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    let mut val: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    let mut state = S::WaitKey;
    let mut i = 0usize;
    while i < input.len() {
        let c = input[i];
        match state {
            S::WaitKey => {
                if c.is_ascii_whitespace() || c == b',' {
                } else if c == b'"' {
                    key.clear();
                    state = S::InQKey;
                } else {
                    key.clear();
                    key.push(c);
                    state = S::InKey;
                }
            }
            S::InKey => {
                if c.is_ascii_whitespace() {
                    state = S::WaitEq;
                } else if c == b'=' {
                    state = S::WaitValue;
                } else {
                    key.push(c);
                }
            }
            S::InQKey => {
                if c == b'"' {
                    if input.get(i + 1) == Some(&b'"') {
                        key.push(c);
                        i += 1;
                    } else {
                        state = S::WaitEq;
                    }
                } else {
                    key.push(c);
                }
            }
            S::WaitEq => {
                if c == b'=' {
                    state = S::WaitValue;
                } else if !c.is_ascii_whitespace() {
                    return Err(invalid_list(input));
                }
            }
            S::WaitValue => {
                if c == b'\'' {
                    val.clear();
                    state = S::InSqValue;
                } else if c == b'E' && input.get(i + 1) == Some(&b'\'') {
                    i += 1;
                    val.clear();
                    state = S::InSqValue;
                } else if c == b'"' {
                    val.clear();
                    state = S::InDqValue;
                } else if !c.is_ascii_whitespace() {
                    val.clear();
                    val.push(c);
                    state = S::InWValue;
                }
            }
            S::InSqValue => {
                if c == b'\'' {
                    if input.get(i + 1) == Some(&b'\'') {
                        val.push(c);
                        i += 1;
                    } else {
                        result.push(build_def_item(mcx, &key, &val, true)?);
                        state = S::WaitKey;
                    }
                } else if c == b'\\' {
                    if input.get(i + 1) == Some(&b'\\') {
                        val.push(c);
                        i += 1;
                    } else {
                        val.push(c);
                    }
                } else {
                    val.push(c);
                }
            }
            S::InDqValue => {
                if c == b'"' {
                    if input.get(i + 1) == Some(&b'"') {
                        val.push(c);
                        i += 1;
                    } else {
                        result.push(build_def_item(mcx, &key, &val, true)?);
                        state = S::WaitKey;
                    }
                } else {
                    val.push(c);
                }
            }
            S::InWValue => {
                if c == b',' || c.is_ascii_whitespace() {
                    result.push(build_def_item(mcx, &key, &val, false)?);
                    state = S::WaitKey;
                } else {
                    val.push(c);
                }
            }
        }
        i += 1;
    }
    if state == S::InWValue {
        result.push(build_def_item(mcx, &key, &val, false)?);
    } else if state != S::WaitKey {
        return Err(invalid_list(input));
    }
    Ok(result)
}
