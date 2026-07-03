use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::ts_locale::dict_api::{def_get_boolean, DictInitData, LexizeResult};
use ::ts_locale::{get_tsearch_config_filename, lowerstr, tsearch_readlines, TsLexeme, TSL_PREFIX};
use ::types_error::{PgError, PgResult, ERRCODE_CONFIG_FILE_ERROR};

use crate::simple::invalid_param;

pub(crate) struct Syn {
    pub(crate) input: PgVec<'static, u8>,
    pub(crate) output: PgVec<'static, u8>,
    pub(crate) flags: u16,
}

pub struct DictSyn {
    pub(crate) syn: PgVec<'static, Syn>,
    pub(crate) case_sensitive: bool,
}

// findwrd: next whitespace-delimited word; a single trailing '*' byte flags
// TSL_PREFIX (second-word calls only) and is excluded from the word.
fn findwrd(line: &[u8], start: usize, flags: Option<&mut u16>) -> Option<(usize, usize)> {
    let mut i = start;
    while i < line.len() && line[i].is_ascii_whitespace() {
        i += ::mbutils::pg_mblen(&line[i..]) as usize;
    }
    if i >= line.len() {
        return None;
    }
    let begin = i;
    let mut lastchar = i;
    while i < line.len() && !line[i].is_ascii_whitespace() {
        lastchar = i;
        i += ::mbutils::pg_mblen(&line[i..]) as usize;
    }
    let mut end = i;
    if let Some(flags) = flags {
        if i - lastchar == 1 && line[lastchar] == b'*' {
            *flags = TSL_PREFIX;
            end = lastchar;
        } else {
            *flags = 0;
        }
    }
    Some((begin, end))
}

pub fn dsynonym_init(init: &DictInitData<'static>) -> PgResult<DictSyn> {
    let mcx = init.mcx;
    let mut filename: Option<&[u8]> = None;
    let mut case_sensitive = false;
    for (i, (name, value)) in init.dict_options.iter().enumerate() {
        if name.as_slice() == b"synonyms" {
            filename = Some(value.as_slice());
        } else if name.as_slice() == b"casesensitive" {
            case_sensitive = def_get_boolean(name, value, init.int_options[i])?;
        } else {
            return Err(invalid_param(format!(
                "unrecognized synonym parameter: \"{}\"",
                String::from_utf8_lossy(name)
            )));
        }
    }
    let Some(filename) = filename else {
        return Err(invalid_param("missing Synonyms parameter".into()));
    };
    let path = get_tsearch_config_filename(mcx, filename, "syn")?;
    let Some(lines) = tsearch_readlines(mcx, &path)? else {
        return Err(PgError::error(format!(
            "could not open synonym file \"{}\": No such file or directory",
            String::from_utf8_lossy(&path)
        ))
        .with_sqlstate(ERRCODE_CONFIG_FILE_ERROR)
        .into());
    };
    let syn = load_synonyms(mcx, &lines, case_sensitive)?;
    Ok(DictSyn { syn, case_sensitive })
}

pub(crate) fn load_synonyms(
    mcx: ::mcx::Mcx<'static>,
    lines: &[PgVec<'static, u8>],
    case_sensitive: bool,
) -> PgResult<PgVec<'static, Syn>> {
    let mut syn: PgVec<'static, Syn> = PgVec::new_in(mcx);
    for line in lines.iter() {
        let Some((bi, ei)) = findwrd(line, 0, None) else {
            continue;
        };
        if ei >= line.len() {
            // A line with only one word. Ignore silently.
            continue;
        }
        let mut flags = 0u16;
        let Some((bo, eo)) = findwrd(line, ei + 1, Some(&mut flags)) else {
            continue;
        };
        let (input, output) = if case_sensitive {
            let mut i_v = vec_with_capacity_in(mcx, ei - bi)?;
            i_v.extend_from_slice(&line[bi..ei]);
            let mut o_v = vec_with_capacity_in(mcx, eo - bo)?;
            o_v.extend_from_slice(&line[bo..eo]);
            (i_v, o_v)
        } else {
            (lowerstr(mcx, &line[bi..ei])?, lowerstr(mcx, &line[bo..eo])?)
        };
        syn.push(Syn { input, output, flags });
    }
    syn.sort_unstable_by(|a, b| a.input.as_slice().cmp(b.input.as_slice()));
    Ok(syn)
}

pub fn dsynonym_lexize<'mcx>(
    mcx: Mcx<'mcx>,
    d: &DictSyn,
    token: &[u8],
) -> PgResult<Option<LexizeResult<'mcx>>> {
    if token.is_empty() || d.syn.is_empty() {
        return Ok(None);
    }
    let key: PgVec<'mcx, u8> = if d.case_sensitive {
        let mut k = vec_with_capacity_in(mcx, token.len())?;
        k.extend_from_slice(token);
        k
    } else {
        lowerstr(mcx, token)?
    };
    let Ok(idx) = d.syn.binary_search_by(|s| s.input.as_slice().cmp(&key)) else {
        return Ok(None);
    };
    let found = &d.syn[idx];
    let mut lexeme = vec_with_capacity_in(mcx, found.output.len())?;
    lexeme.extend_from_slice(&found.output);
    let mut out = PgVec::new_in(mcx);
    out.push(TsLexeme { nvariant: 0, flags: found.flags, lexeme });
    Ok(Some(LexizeResult(out)))
}
