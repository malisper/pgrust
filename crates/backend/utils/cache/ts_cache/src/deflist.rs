use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_error::{PgError, PgResult, ERRCODE_SYNTAX_ERROR};

// One deserialize_deflist DefElem: `value` is the defGetString rendering;
// `int_value` is Some iff buildDefItem made a T_Integer node.
pub struct DefListItem<'mcx> {
    pub name: PgVec<'mcx, u8>,
    pub value: PgVec<'mcx, u8>,
    pub int_value: Option<i64>,
}

#[derive(PartialEq)]
enum DsState {
    WaitKey,
    InKey,
    InQKey,
    WaitEq,
    WaitValue,
    InSqValue,
    InDqValue,
    InWValue,
}

#[track_caller]
#[cold]
fn bad_format(input: &[u8]) -> Box<PgError> {
    PgError::error(format!(
        "invalid parameter list format: \"{}\"",
        String::from_utf8_lossy(input)
    ))
    .with_sqlstate(ERRCODE_SYNTAX_ERROR)
    .into()
}

// buildDefItem (tsearchcmds.c): unquoted values try integer (i32, exact, with
// the defGetString "%d" re-rendering), then float and true/false — both of
// which keep the literal text, so they fold into the plain-string arm here.
fn build_item<'mcx>(
    mcx: Mcx<'mcx>,
    name: &[u8],
    value: &[u8],
    was_quoted: bool,
) -> PgResult<DefListItem<'mcx>> {
    let mut name_v = vec_with_capacity_in(mcx, name.len())?;
    name_v.extend_from_slice(name);
    let int_value = if !was_quoted && !value.is_empty() {
        core::str::from_utf8(value).ok().and_then(|s| s.parse::<i32>().ok())
    } else {
        None
    };
    let (rendered, int_value) = match int_value {
        Some(v) => {
            let s = v.to_string();
            let mut val_v = vec_with_capacity_in(mcx, s.len())?;
            val_v.extend_from_slice(s.as_bytes());
            (val_v, Some(v as i64))
        }
        None => {
            let mut val_v = vec_with_capacity_in(mcx, value.len())?;
            val_v.extend_from_slice(value);
            (val_v, None)
        }
    };
    Ok(DefListItem { name: name_v, value: rendered, int_value })
}

// deserialize_deflist (tsearchcmds.c): parse a stored dictinitoption text
// back into (name, value) items; accepts single-quoted, E'', double-quoted,
// and unquoted whitespace/comma-delimited values.
pub fn deserialize_deflist<'mcx>(
    mcx: Mcx<'mcx>,
    input: &[u8],
) -> PgResult<PgVec<'mcx, DefListItem<'mcx>>> {
    let mut result: PgVec<'mcx, DefListItem<'mcx>> = PgVec::new_in(mcx);
    let mut key: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, 16)?;
    let mut val: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, input.len())?;
    let mut state = DsState::WaitKey;
    let mut i = 0usize;
    let len = input.len();
    while i < len {
        let c = input[i];
        match state {
            DsState::WaitKey => {
                if c.is_ascii_whitespace() || c == b',' {
                } else if c == b'"' {
                    key.clear();
                    state = DsState::InQKey;
                } else {
                    key.clear();
                    key.push(c);
                    state = DsState::InKey;
                }
            }
            DsState::InKey => {
                if c.is_ascii_whitespace() {
                    state = DsState::WaitEq;
                } else if c == b'=' {
                    state = DsState::WaitValue;
                } else {
                    key.push(c);
                }
            }
            DsState::InQKey => {
                if c == b'"' {
                    if input.get(i + 1) == Some(&b'"') {
                        key.push(c);
                        i += 1;
                    } else {
                        state = DsState::WaitEq;
                    }
                } else {
                    key.push(c);
                }
            }
            DsState::WaitEq => {
                if c == b'=' {
                    state = DsState::WaitValue;
                } else if !c.is_ascii_whitespace() {
                    return Err(bad_format(input));
                }
            }
            DsState::WaitValue => {
                if c == b'\'' {
                    val.clear();
                    state = DsState::InSqValue;
                } else if c == b'E' && input.get(i + 1) == Some(&b'\'') {
                    i += 1;
                    val.clear();
                    state = DsState::InSqValue;
                } else if c == b'"' {
                    val.clear();
                    state = DsState::InDqValue;
                } else if !c.is_ascii_whitespace() {
                    val.clear();
                    val.push(c);
                    state = DsState::InWValue;
                }
            }
            DsState::InSqValue => {
                if c == b'\'' {
                    if input.get(i + 1) == Some(&b'\'') {
                        val.push(c);
                        i += 1;
                    } else {
                        result.push(build_item(mcx, &key, &val, true)?);
                        state = DsState::WaitKey;
                    }
                } else if c == b'\\' && input.get(i + 1) == Some(&b'\\') {
                    val.push(c);
                    i += 1;
                } else {
                    val.push(c);
                }
            }
            DsState::InDqValue => {
                if c == b'"' {
                    if input.get(i + 1) == Some(&b'"') {
                        val.push(c);
                        i += 1;
                    } else {
                        result.push(build_item(mcx, &key, &val, true)?);
                        state = DsState::WaitKey;
                    }
                } else {
                    val.push(c);
                }
            }
            DsState::InWValue => {
                if c == b',' || c.is_ascii_whitespace() {
                    result.push(build_item(mcx, &key, &val, false)?);
                    state = DsState::WaitKey;
                } else {
                    val.push(c);
                }
            }
        }
        i += 1;
    }
    if state == DsState::InWValue {
        result.push(build_item(mcx, &key, &val, false)?);
    } else if state != DsState::WaitKey {
        return Err(bad_format(input));
    }
    Ok(result)
}
