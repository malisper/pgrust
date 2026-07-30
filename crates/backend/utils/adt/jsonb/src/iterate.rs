//! jsonfuncs.c: parse_jsonb_index_flags + iterate_json(b)_values +
//! transform_json(b)_string_values.

extern crate alloc;

use alloc::boxed::Box;
use alloc::format;

use adt_json::jsonapi::{JsonLex, JsonLexDe, JsonSem, JsonSemToken};
use adt_numeric::Num;
use mcx::{Mcx, PgVec};
use stringinfo::StringInfo;
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};

use crate::build::convert_to_jsonb;
use crate::container::JsonbItem;
use crate::iter::{JsonbIterator, WjbToken};
use crate::mutate::JsonbPush;

pub const JTI_KEY: u32 = 0x01;
pub const JTI_STRING: u32 = 0x02;
pub const JTI_NUMERIC: u32 = 0x04;
pub const JTI_BOOL: u32 = 0x08;
pub const JTI_ALL: u32 = JTI_KEY | JTI_STRING | JTI_NUMERIC | JTI_BOOL;

const FLAG_HINT: &str =
    "Possible values are: \"string\", \"numeric\", \"boolean\", \"key\", and \"all\".";

#[track_caller]
#[cold]
#[inline(never)]
fn flag_error(msg: impl Into<alloc::string::String>, hint: bool) -> Box<PgError> {
    let mut e = PgError::error(msg.into()).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE);
    if hint {
        e = e.with_hint(FLAG_HINT);
    }
    Box::new(e)
}

pub fn parse_jsonb_index_flags(mcx: Mcx<'_>, jb: &[u8]) -> PgResult<u32> {
    let mut it = JsonbIterator::init(mcx, jb)?;
    let mut flags = 0u32;

    let (mut ty, _) = it.next(false);
    if ty != WjbToken::BeginArray {
        return Err(flag_error(
            "wrong flag type, only arrays and scalars are allowed",
            false,
        ));
    }

    loop {
        let (t, v) = it.next(false);
        ty = t;
        if ty != WjbToken::Elem {
            break;
        }
        let JsonbItem::String(s) = v else {
            return Err(flag_error("flag array element is not a string", true));
        };
        if s.eq_ignore_ascii_case(b"all") {
            flags |= JTI_ALL;
        } else if s.eq_ignore_ascii_case(b"key") {
            flags |= JTI_KEY;
        } else if s.eq_ignore_ascii_case(b"string") {
            flags |= JTI_STRING;
        } else if s.eq_ignore_ascii_case(b"numeric") {
            flags |= JTI_NUMERIC;
        } else if s.eq_ignore_ascii_case(b"boolean") {
            flags |= JTI_BOOL;
        } else {
            return Err(flag_error(
                format!(
                    "wrong flag in flag array: \"{}\"",
                    alloc::string::String::from_utf8_lossy(s)
                ),
                true,
            ));
        }
    }

    if ty != WjbToken::EndArray {
        return Err(Box::new(PgError::error(
            "unexpected end of flag array".to_string(),
        )));
    }
    let (ty, _) = it.next(false);
    if ty != WjbToken::Done {
        return Err(Box::new(PgError::error(
            "unexpected end of flag array".to_string(),
        )));
    }
    Ok(flags)
}

pub fn iterate_jsonb_values(
    mcx: Mcx<'_>,
    jb: &[u8],
    flags: u32,
    action: &mut dyn FnMut(&[u8]) -> PgResult<()>,
) -> PgResult<()> {
    let mut it = JsonbIterator::init(mcx, jb)?;
    let mut numscratch: alloc::vec::Vec<u8> = alloc::vec::Vec::new();
    loop {
        let (t, v) = it.next(false);
        match t {
            WjbToken::Done => break,
            WjbToken::Key => {
                if flags & JTI_KEY != 0 {
                    let JsonbItem::String(s) = v else {
                        panic!("unexpected jsonb type as object key");
                    };
                    action(s)?;
                }
            }
            WjbToken::Value | WjbToken::Elem => match v {
                JsonbItem::String(s) => {
                    if flags & JTI_STRING != 0 {
                        action(s)?;
                    }
                }
                JsonbItem::Numeric(image) => {
                    if flags & JTI_NUMERIC != 0 {
                        numscratch.clear();
                        adt_numeric::numeric_out_into(
                            Num::from_payload(&image[4..]),
                            &mut numscratch,
                        );
                        action(&numscratch)?;
                    }
                }
                JsonbItem::Bool(b) => {
                    if flags & JTI_BOOL != 0 {
                        action(if b { b"true" } else { b"false" })?;
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }
    Ok(())
}

struct IterateValuesSem<'x> {
    flags: u32,
    action: &'x mut dyn FnMut(&[u8]) -> PgResult<()>,
}

impl<'mcx> JsonSem<'mcx> for IterateValuesSem<'_> {
    fn scalar(&mut self, _lex: &JsonLex<'_>, token: JsonSemToken<'mcx>) -> PgResult<bool> {
        match token {
            JsonSemToken::String(s) => {
                if self.flags & JTI_STRING != 0 {
                    (self.action)(s)?;
                }
            }
            JsonSemToken::Number(n) => {
                if self.flags & JTI_NUMERIC != 0 {
                    (self.action)(n)?;
                }
            }
            JsonSemToken::True => {
                if self.flags & JTI_BOOL != 0 {
                    (self.action)(b"true")?;
                }
            }
            JsonSemToken::False => {
                if self.flags & JTI_BOOL != 0 {
                    (self.action)(b"false")?;
                }
            }
            JsonSemToken::Null => {}
        }
        Ok(true)
    }

    fn object_field_start(
        &mut self,
        _lex: &JsonLex<'_>,
        fname: &'mcx [u8],
        _isnull: bool,
    ) -> PgResult<bool> {
        if self.flags & JTI_KEY != 0 {
            (self.action)(fname)?;
        }
        Ok(true)
    }
}

pub fn iterate_json_values(
    mcx: Mcx<'_>,
    json: &[u8],
    flags: u32,
    action: &mut dyn FnMut(&[u8]) -> PgResult<()>,
) -> PgResult<()> {
    let mut lex = JsonLexDe::new(mcx, json, mbutils::GetDatabaseEncoding());
    let mut sem = IterateValuesSem { flags, action };
    let r = adt_json::jsonapi::parse_sem(&mut lex, &mut sem)?;
    if r != adt_json::jsonapi::JsonError::Success {
        adt_json::errsave_parse_error(r, &lex.lex, None)?;
        unreachable!("hard errsave without escontext returns Err");
    }
    Ok(())
}

pub fn transform_jsonb_string_values<'mcx>(
    mcx: Mcx<'mcx>,
    jb: &'mcx [u8],
    action: &mut dyn FnMut(&[u8]) -> PgResult<&'mcx [u8]>,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut it = JsonbIterator::init(mcx, jb)?;
    let mut ps = JsonbPush::new(mcx)?;
    loop {
        let (t, v) = it.next(false);
        if t == WjbToken::Done {
            break;
        }
        match (t, v) {
            (WjbToken::Value | WjbToken::Elem, JsonbItem::String(s)) => {
                let out = action(s)?;
                ps.push(t, JsonbItem::String(out))?;
            }
            _ => ps.push(t, v)?,
        }
    }
    convert_to_jsonb(mcx, &ps.finish())
}

struct TransformSem<'x, 'mcx> {
    strval: StringInfo<'mcx>,
    action: &'x mut dyn FnMut(&[u8]) -> PgResult<PgVec<'mcx, u8>>,
}

impl<'x, 'mcx> TransformSem<'x, 'mcx> {
    fn sep_unless_last(&mut self, open: u8) -> PgResult<()> {
        if *self.strval.as_bytes().last().expect("inside a container") != open {
            self.strval.append_byte(b',')?;
        }
        Ok(())
    }
}

impl<'mcx> JsonSem<'mcx> for TransformSem<'_, '_> {
    fn object_start(&mut self, _lex: &JsonLex<'_>) -> PgResult<bool> {
        self.strval.append_byte(b'{')?;
        Ok(true)
    }
    fn object_end(&mut self, _lex: &JsonLex<'_>) -> PgResult<bool> {
        self.strval.append_byte(b'}')?;
        Ok(true)
    }
    fn array_start(&mut self, _lex: &JsonLex<'_>) -> PgResult<bool> {
        self.strval.append_byte(b'[')?;
        Ok(true)
    }
    fn array_end(&mut self, _lex: &JsonLex<'_>) -> PgResult<bool> {
        self.strval.append_byte(b']')?;
        Ok(true)
    }
    fn object_field_start(
        &mut self,
        _lex: &JsonLex<'_>,
        fname: &'mcx [u8],
        _isnull: bool,
    ) -> PgResult<bool> {
        self.sep_unless_last(b'{')?;
        adt_json::escape_json(&mut self.strval, fname)?;
        self.strval.append_byte(b':')?;
        Ok(true)
    }
    fn array_element_start(&mut self, _lex: &JsonLex<'_>, _isnull: bool) -> PgResult<bool> {
        self.sep_unless_last(b'[')?;
        Ok(true)
    }
    fn scalar(&mut self, _lex: &JsonLex<'_>, token: JsonSemToken<'mcx>) -> PgResult<bool> {
        match token {
            JsonSemToken::String(s) => {
                let out = (self.action)(s)?;
                adt_json::escape_json(&mut self.strval, &out)?;
            }
            JsonSemToken::Number(n) => self.strval.append_bytes(n)?,
            JsonSemToken::True => self.strval.append_bytes(b"true")?,
            JsonSemToken::False => self.strval.append_bytes(b"false")?,
            JsonSemToken::Null => self.strval.append_bytes(b"null")?,
        }
        Ok(true)
    }
}

pub fn transform_json_string_values<'mcx>(
    mcx: Mcx<'mcx>,
    json: &[u8],
    action: &mut dyn FnMut(&[u8]) -> PgResult<PgVec<'mcx, u8>>,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut lex = JsonLexDe::new(mcx, json, mbutils::GetDatabaseEncoding());
    let mut sem = TransformSem { strval: StringInfo::with_capacity_in(mcx, json.len())?, action };
    let r = adt_json::jsonapi::parse_sem(&mut lex, &mut sem)?;
    if r != adt_json::jsonapi::JsonError::Success {
        adt_json::errsave_parse_error(r, &lex.lex, None)?;
        unreachable!("hard errsave without escontext returns Err");
    }
    Ok(sem.strval.into_vec())
}
