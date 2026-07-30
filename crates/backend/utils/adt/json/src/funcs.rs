//! jsonfuncs.c json-half scalar workers: json_array_length, json_strip_nulls,
//! json_typeof.

extern crate alloc;

use crate::escape_json;
use crate::jsonapi::{self, JsonError, JsonLex, JsonLexDe, JsonSem, JsonSemToken, JsonToken};
use mcx::Mcx;
use stringinfo::StringInfo;
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};

#[cold]
#[inline(never)]
pub(crate) fn invalid_param(msg: alloc::string::String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE))
}

// C: pg_parse_json_or_ereport over a sem-action lane.
pub(crate) fn parse_sem_or_ereport<'mcx>(
    lex: &mut JsonLexDe<'_, 'mcx>,
    sem: &mut impl JsonSem<'mcx>,
) -> PgResult<()> {
    let r = jsonapi::parse_sem(lex, sem)?;
    if r != JsonError::Success {
        crate::errsave_parse_error(r, &lex.lex, None)?;
        unreachable!("hard errsave without escontext returns Err");
    }
    Ok(())
}

struct AlenState {
    count: i32,
}

impl<'mcx> JsonSem<'mcx> for AlenState {
    fn object_start(&mut self, lex: &JsonLex<'_>) -> PgResult<bool> {
        if lex.lex_level == 0 {
            return Err(invalid_param(
                "cannot get array length of a non-array".into(),
            ));
        }
        Ok(true)
    }

    fn scalar(&mut self, lex: &JsonLex<'_>, _token: JsonSemToken<'mcx>) -> PgResult<bool> {
        if lex.lex_level == 0 {
            return Err(invalid_param("cannot get array length of a scalar".into()));
        }
        Ok(true)
    }

    fn array_element_start(&mut self, lex: &JsonLex<'_>, _isnull: bool) -> PgResult<bool> {
        if lex.lex_level == 1 {
            self.count += 1;
        }
        Ok(true)
    }
}

/// C: json_array_length.
pub fn json_array_length(mcx: Mcx<'_>, json: &[u8]) -> PgResult<i32> {
    let mut lex = JsonLexDe::with_escapes(mcx, json, mbutils::GetDatabaseEncoding(), false);
    let mut state = AlenState { count: 0 };
    parse_sem_or_ereport(&mut lex, &mut state)?;
    Ok(state.count)
}

struct StripState<'s, 'mcx> {
    out: &'s mut StringInfo<'mcx>,
    skip_next_null: bool,
    strip_in_arrays: bool,
}

impl<'mcx> JsonSem<'mcx> for StripState<'_, '_> {
    fn object_start(&mut self, _lex: &JsonLex<'_>) -> PgResult<bool> {
        self.out.append_byte(b'{')?;
        Ok(true)
    }

    fn object_end(&mut self, _lex: &JsonLex<'_>) -> PgResult<bool> {
        self.out.append_byte(b'}')?;
        Ok(true)
    }

    fn array_start(&mut self, _lex: &JsonLex<'_>) -> PgResult<bool> {
        self.out.append_byte(b'[')?;
        Ok(true)
    }

    fn array_end(&mut self, _lex: &JsonLex<'_>) -> PgResult<bool> {
        self.out.append_byte(b']')?;
        Ok(true)
    }

    fn object_field_start(
        &mut self,
        _lex: &JsonLex<'_>,
        fname: &'mcx [u8],
        isnull: bool,
    ) -> PgResult<bool> {
        if isnull {
            // The next thing must be a scalar or isnull couldn't be true;
            // the flag is reset in the scalar action.
            self.skip_next_null = true;
            return Ok(true);
        }
        if self.out.as_bytes().last() != Some(&b'{') {
            self.out.append_byte(b',')?;
        }
        escape_json(self.out, fname)?;
        self.out.append_byte(b':')?;
        Ok(true)
    }

    fn array_element_start(&mut self, _lex: &JsonLex<'_>, isnull: bool) -> PgResult<bool> {
        if isnull && self.strip_in_arrays {
            self.skip_next_null = true;
            return Ok(true);
        }
        if !self.out.is_empty() && self.out.as_bytes().last() != Some(&b'[') {
            self.out.append_byte(b',')?;
        }
        Ok(true)
    }

    fn scalar(&mut self, _lex: &JsonLex<'_>, token: JsonSemToken<'mcx>) -> PgResult<bool> {
        if self.skip_next_null {
            debug_assert!(matches!(token, JsonSemToken::Null));
            self.skip_next_null = false;
            return Ok(true);
        }
        match token {
            JsonSemToken::String(s) => escape_json(self.out, s)?,
            JsonSemToken::Number(raw) => self.out.append_bytes(raw)?,
            JsonSemToken::True => self.out.append_bytes(b"true")?,
            JsonSemToken::False => self.out.append_bytes(b"false")?,
            JsonSemToken::Null => self.out.append_bytes(b"null")?,
        }
        Ok(true)
    }
}

/// C: json_strip_nulls.
pub fn json_strip_nulls<'mcx>(
    mcx: Mcx<'mcx>,
    json: &[u8],
    strip_in_arrays: bool,
) -> PgResult<datum::Varlena<'mcx>> {
    let mut out = StringInfo::new_in(mcx)?;
    let mut lex = JsonLexDe::new(mcx, json, mbutils::GetDatabaseEncoding());
    let mut state = StripState {
        out: &mut out,
        skip_next_null: false,
        strip_in_arrays,
    };
    parse_sem_or_ereport(&mut lex, &mut state)?;
    varlena::cstring_to_text(mcx, out.as_bytes())
}

// C JsonUniqueParsingState: (object_id, de-escaped key) hash with a stack of
// per-nesting object ids.
struct UniqueState<'mcx> {
    check: mcx::PgFxHashMap<'mcx, (i32, &'mcx [u8]), ()>,
    stack: alloc::vec::Vec<i32>,
    id_counter: i32,
    unique: bool,
}

impl<'mcx> JsonSem<'mcx> for UniqueState<'mcx> {
    fn object_start(&mut self, _lex: &JsonLex<'_>) -> PgResult<bool> {
        if self.unique {
            self.stack.push(self.id_counter);
            self.id_counter += 1;
        }
        Ok(true)
    }

    fn object_end(&mut self, _lex: &JsonLex<'_>) -> PgResult<bool> {
        if self.unique {
            self.stack.pop();
        }
        Ok(true)
    }

    fn object_field_start(
        &mut self,
        _lex: &JsonLex<'_>,
        fname: &'mcx [u8],
        _isnull: bool,
    ) -> PgResult<bool> {
        if self.unique {
            let object_id = *self.stack.last().expect("field inside an object");
            if self.check.insert((object_id, fname), ()).is_some() {
                self.unique = false;
                self.stack.clear();
            }
        }
        Ok(true)
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn duplicate_json_object_key() -> Box<PgError> {
    Box::new(
        PgError::error("duplicate JSON object key value")
            .with_sqlstate(types_error::ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE),
    )
}

/// C: json_validate (json.c:1811).
pub fn json_validate<'mcx>(
    mcx: Mcx<'mcx>,
    json: &'mcx [u8],
    check_unique_keys: bool,
    throw_error: bool,
) -> PgResult<bool> {
    if check_unique_keys {
        let mut lex = JsonLexDe::new(mcx, json, mbutils::GetDatabaseEncoding());
        let mut state = UniqueState {
            check: mcx::PgFxHashMap::with_hasher_in(Default::default(), mcx),
            stack: alloc::vec::Vec::new(),
            id_counter: 0,
            unique: true,
        };
        let r = jsonapi::parse_sem(&mut lex, &mut state)?;
        if r != JsonError::Success {
            if throw_error {
                crate::errsave_parse_error(r, &lex.lex, None)?;
                unreachable!("hard errsave without escontext returns Err");
            }
            return Ok(false);
        }
        if !state.unique {
            if throw_error {
                return Err(duplicate_json_object_key());
            }
            return Ok(false);
        }
        return Ok(true);
    }
    let mut lex = JsonLex::new(json, mbutils::GetDatabaseEncoding());
    let r = crate::jsonapi::parse(&mut lex)?;
    if r != JsonError::Success {
        if throw_error {
            crate::errsave_parse_error(r, &lex, None)?;
            unreachable!("hard errsave without escontext returns Err");
        }
        return Ok(false);
    }
    Ok(true)
}

/// C: json_typeof — a single json_lex over the validated text.
pub fn json_typeof(json: &[u8]) -> PgResult<&'static str> {
    let mut lex = JsonLex::new(json, mbutils::GetDatabaseEncoding());
    let r = lex.lex();
    if r != JsonError::Success {
        crate::errsave_parse_error(r, &lex, None)?;
        unreachable!("hard errsave without escontext returns Err");
    }
    Ok(match lex.token_type {
        JsonToken::ObjectStart => "object",
        JsonToken::ArrayStart => "array",
        JsonToken::String => "string",
        JsonToken::Number => "number",
        JsonToken::True | JsonToken::False => "boolean",
        JsonToken::Null => "null",
        other => panic!("unexpected json token: {other:?}"),
    })
}
