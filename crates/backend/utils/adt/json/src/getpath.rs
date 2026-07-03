//! jsonfuncs.c json-half getter machinery: get_worker + the -> ->> #> #>>
//! operators and json_extract_path[_text]. Lex-based over the stored text;
//! non-normalized results are verbatim input slices (whitespace preserved).

use crate::funcs::parse_sem_or_ereport;
use crate::jsonapi::{
    json_count_array_elements, JsonLex, JsonLexDe, JsonSem, JsonSemToken, JsonToken,
};
use mcx::{Mcx, PgVec};
use types_error::PgResult;

enum TResult<'mcx> {
    Range(usize, usize),
    Str(&'mcx [u8]),
}

struct GetState<'a, 'mcx> {
    npath: usize,
    names: Option<&'a [&'a [u8]]>,
    indexes: Option<&'a mut [i32]>,
    normalize: bool,
    next_scalar: bool,
    pathok: PgVec<'mcx, bool>,
    array_cur_index: PgVec<'mcx, i32>,
    result_start: Option<usize>,
    tresult: Option<TResult<'mcx>>,
}

impl GetState<'_, '_> {
    fn name_matches(&self, lex_level: usize, fname: &[u8]) -> bool {
        lex_level <= self.npath
            && self.pathok[lex_level - 1]
            && self
                .names
                .map(|n| n[lex_level - 1] == fname)
                .unwrap_or(false)
    }

    fn index_matches(&self, lex_level: usize) -> bool {
        lex_level <= self.npath
            && self.pathok[lex_level - 1]
            && self
                .indexes
                .as_ref()
                .map(|ix| self.array_cur_index[lex_level - 1] == ix[lex_level - 1])
                .unwrap_or(false)
    }

    fn start_capture(&mut self, lex: &JsonLex<'_>) {
        // C: this match overrides any previous matching value.
        self.tresult = None;
        self.result_start = None;
        if self.normalize && lex.token_type == JsonToken::String {
            self.next_scalar = true;
        } else {
            self.result_start = lex.token_start;
        }
    }

    fn end_capture(&mut self, lex: &JsonLex<'_>, isnull: bool) {
        if let Some(start) = self.result_start {
            if isnull && self.normalize {
                self.tresult = None;
            } else {
                self.tresult = Some(TResult::Range(start, lex.prev_token_terminator));
            }
            self.result_start = None;
        }
    }
}

impl<'mcx> JsonSem<'mcx> for GetState<'_, 'mcx> {
    fn object_start(&mut self, lex: &JsonLex<'_>) -> PgResult<bool> {
        if lex.lex_level == 0 && self.npath == 0 {
            self.result_start = lex.token_start;
        }
        Ok(true)
    }

    fn object_end(&mut self, lex: &JsonLex<'_>) -> PgResult<bool> {
        if lex.lex_level == 0 && self.npath == 0 {
            let start = self.result_start.expect("object start captured");
            self.tresult = Some(TResult::Range(start, lex.prev_token_terminator));
        }
        Ok(true)
    }

    fn array_start(&mut self, lex: &JsonLex<'_>) -> PgResult<bool> {
        let lex_level = lex.lex_level as usize;
        if let Some(indexes) = self.indexes.as_deref_mut() {
            if lex_level < self.npath {
                self.array_cur_index[lex_level] = -1;
                if indexes[lex_level] < 0 && indexes[lex_level] != i32::MIN {
                    match json_count_array_elements(lex)? {
                        Ok(nelements) => {
                            if -indexes[lex_level] <= nelements {
                                indexes[lex_level] += nelements;
                            }
                        }
                        Err(e) => {
                            crate::errsave_parse_error(e, lex, None)?;
                            unreachable!("hard errsave without escontext returns Err");
                        }
                    }
                }
                return Ok(true);
            }
        }
        if lex.lex_level == 0 && self.npath == 0 {
            self.result_start = lex.token_start;
        }
        Ok(true)
    }

    fn array_end(&mut self, lex: &JsonLex<'_>) -> PgResult<bool> {
        if lex.lex_level == 0 && self.npath == 0 {
            let start = self.result_start.expect("array start captured");
            self.tresult = Some(TResult::Range(start, lex.prev_token_terminator));
        }
        Ok(true)
    }

    fn object_field_start(
        &mut self,
        lex: &JsonLex<'_>,
        fname: &'mcx [u8],
        _isnull: bool,
    ) -> PgResult<bool> {
        if self.names.is_none() {
            return Ok(true);
        }
        let lex_level = lex.lex_level as usize;
        if self.name_matches(lex_level, fname) {
            if lex_level < self.npath {
                self.pathok[lex_level] = true;
            } else {
                self.start_capture(lex);
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
        if self.names.is_none() {
            return Ok(true);
        }
        let lex_level = lex.lex_level as usize;
        if self.name_matches(lex_level, fname) {
            if lex_level < self.npath {
                self.pathok[lex_level] = false;
            } else {
                self.end_capture(lex, isnull);
            }
        }
        Ok(true)
    }

    fn array_element_start(&mut self, lex: &JsonLex<'_>, _isnull: bool) -> PgResult<bool> {
        if self.indexes.is_none() {
            return Ok(true);
        }
        let lex_level = lex.lex_level as usize;
        if lex_level <= self.npath {
            self.array_cur_index[lex_level - 1] += 1;
        }
        if self.index_matches(lex_level) {
            if lex_level < self.npath {
                self.pathok[lex_level] = true;
            } else {
                self.start_capture(lex);
            }
        }
        Ok(true)
    }

    fn array_element_end(&mut self, lex: &JsonLex<'_>, isnull: bool) -> PgResult<bool> {
        if self.indexes.is_none() {
            return Ok(true);
        }
        let lex_level = lex.lex_level as usize;
        if self.index_matches(lex_level) {
            if lex_level < self.npath {
                self.pathok[lex_level] = false;
            } else {
                self.end_capture(lex, isnull);
            }
        }
        Ok(true)
    }

    fn scalar(&mut self, lex: &JsonLex<'_>, token: JsonSemToken<'mcx>) -> PgResult<bool> {
        if lex.lex_level == 0 && self.npath == 0 {
            // C keys off the callback's tokentype: the lexer has already
            // advanced past the scalar here.
            if self.normalize && matches!(token, JsonSemToken::String(_)) {
                self.next_scalar = true;
            } else if self.normalize && matches!(token, JsonSemToken::Null) {
                self.tresult = None;
            } else {
                // C: whitespace after the scalar is suppressed, whitespace
                // before it is kept (result starts at the input's beginning).
                self.tresult = Some(TResult::Range(0, lex.prev_token_terminator));
            }
        }
        if self.next_scalar {
            let JsonSemToken::String(s) = token else {
                panic!("next_scalar set on a non-string token")
            };
            self.tresult = Some(TResult::Str(s));
            self.next_scalar = false;
        }
        Ok(true)
    }
}

/// C: get_worker. Ok(None) = SQL NULL.
pub fn get_worker<'mcx>(
    mcx: Mcx<'mcx>,
    json: &[u8],
    names: Option<&[&[u8]]>,
    indexes: Option<&mut [i32]>,
    npath: usize,
    normalize_results: bool,
) -> PgResult<Option<datum::Varlena<'mcx>>> {
    let mut pathok: PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, npath.max(1))?;
    pathok.resize(npath, false);
    if npath > 0 {
        pathok[0] = true;
    }
    let mut array_cur_index: PgVec<'mcx, i32> = mcx::vec_with_capacity_in(mcx, npath.max(1))?;
    array_cur_index.resize(npath, 0);

    let mut state = GetState {
        npath,
        names,
        indexes,
        normalize: normalize_results,
        next_scalar: false,
        pathok,
        array_cur_index,
        result_start: None,
        tresult: None,
    };
    let mut lex = JsonLexDe::new(mcx, json, mbutils::GetDatabaseEncoding());
    parse_sem_or_ereport(&mut lex, &mut state)?;

    match state.tresult {
        None => Ok(None),
        Some(TResult::Range(start, end)) => {
            Ok(Some(varlena::cstring_to_text(mcx, &json[start..end])?))
        }
        Some(TResult::Str(s)) => Ok(Some(varlena::cstring_to_text(mcx, s)?)),
    }
}

// C: get_path_all's strtoint conversion — leading whitespace and sign
// accepted, trailing junk or out-of-int-range yields the INT_MIN sentinel.
pub fn path_index(s: &[u8]) -> i32 {
    if s.is_empty() {
        return i32::MIN;
    }
    let t = s.trim_ascii_start();
    let Ok(text) = core::str::from_utf8(t) else {
        return i32::MIN;
    };
    match text.parse::<i64>() {
        Ok(v) if v >= i32::MIN as i64 && v <= i32::MAX as i64 => v as i32,
        _ => i32::MIN,
    }
}
