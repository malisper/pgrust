//! Port of `src/backend/tsearch/dict_synonym.c` — the `synonym` dictionary
//! template: replace a word by its synonym.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use backend_commands_define_seams::{def_get_boolean, def_get_string, DefElemArg};
use backend_tsearch_ts_locale_seams::readfile;
use backend_tsearch_ts_utils_seams::get_tsearch_config_filename;
use backend_utils_adt_formatting_seams::str_tolower;
use backend_utils_mb_mbutils_seams::pg_mblen_range;

use mcx::{Mcx, PgString, PgVec};
use types_error::PgResult;
use types_tsearch::{DictSyn, Syn, TSLexeme, TSL_PREFIX};

use crate::dict_simple::slice_from_in;
use crate::{config_file_error, invalid_param, is_space, DEFAULT_COLLATION_OID};

/// `findwrd(in, &end, flags)` (dict_synonym.c): find the next
/// whitespace-delimited word in `line` starting at byte `pos`. Returns
/// `Some((start, end, flags))` where `start..end` is the word's byte range and
/// `flags` is `TSL_PREFIX` if it ended with a bare `*` (only when `want_flags`),
/// else `None` (empty line, C `*end == NULL`).
fn findwrd(line: &[u8], mut pos: usize, want_flags: bool) -> PgResult<Option<(usize, usize, u16)>> {
    // Skip leading spaces: while (*in && isspace(*in)) in += pg_mblen_cstr(in);
    while pos < line.len() && line[pos] != 0 && is_space(line[pos]) {
        pos += pg_mblen_range::call(&line[pos..])? as usize;
    }

    // Return NULL on empty lines: if (*in == '\0') { *end = NULL; return NULL; }
    if pos >= line.len() || line[pos] == 0 {
        return Ok(None);
    }

    let start = pos;
    let mut lastchar = pos;

    // Find end of word: while (*in && !isspace(*in)) { lastchar = in; in += pg_mblen_cstr(in); }
    while pos < line.len() && line[pos] != 0 && !is_space(line[pos]) {
        lastchar = pos;
        pos += pg_mblen_range::call(&line[pos..])? as usize;
    }

    // if (in - lastchar == 1 && t_iseq(lastchar, '*') && flags)
    if pos - lastchar == 1 && line[lastchar] == b'*' && want_flags {
        Ok(Some((start, lastchar, TSL_PREFIX)))
    } else {
        Ok(Some((start, pos, 0)))
    }
}

/// `compareSyn(a, b)` (dict_synonym.c): `strcmp(a->in, b->in)`.
fn compare_syn(a: &Syn<'_>, b: &Syn<'_>) -> core::cmp::Ordering {
    a.r#in.as_bytes().cmp(b.r#in.as_bytes())
}

/// `dsynonym_init(PG_FUNCTION_ARGS)`: read the synonyms file, parse
/// `casesensitive`, build the sorted synonym table. Built [`DictSyn`] in `mcx`.
pub fn dsynonym_init<'mcx>(
    mcx: Mcx<'mcx>,
    dictoptions: &[(String, Option<DefElemArg>)],
) -> PgResult<DictSyn<'mcx>> {
    let mut filename: Option<PgString<'mcx>> = None;
    let mut case_sensitive = false;

    for (defname, arg) in dictoptions {
        if defname == "synonyms" {
            filename = Some(def_get_string::call(mcx, defname.clone(), arg.clone())?);
        } else if defname == "casesensitive" {
            case_sensitive = def_get_boolean::call(defname.clone(), arg.clone())?;
        } else {
            return Err(invalid_param(format!(
                "unrecognized synonym parameter: \"{defname}\""
            )));
        }
    }

    let Some(filename) = filename else {
        return Err(invalid_param("missing Synonyms parameter"));
    };

    // filename = get_tsearch_config_filename(filename, "syn");
    let path = get_tsearch_config_filename::call(mcx, filename.as_bytes(), b"syn")?;

    // if (!tsearch_readline_begin(&trst, filename)) ereport(...could not open...);
    let content = readfile::call(&path).map_err(|_| {
        config_file_error(format!(
            "could not open synonym file \"{}\": %m",
            String::from_utf8_lossy(&path)
        ))
    })?;

    // d = palloc0(sizeof(DictSyn));
    let mut syn: PgVec<'mcx, Syn<'mcx>> = PgVec::new_in(mcx);

    // while ((line = tsearch_readline(&trst)) != NULL) { ... }
    for line in content.split(|&b| b == b'\n') {
        // skipline: empty / single-word lines are silently ignored.
        let Some((starti_b, starti_e, _)) = findwrd(line, 0, false)? else {
            continue; // empty line
        };
        if starti_e >= line.len() || line[starti_e] == 0 {
            // A line with only one word. Ignore silently.
            continue;
        }
        // *end = '\0';  (starti now ends at starti_e)

        // starto = findwrd(end + 1, &end, &flags);
        let Some((starto_b, starto_e, flags)) = findwrd(line, starti_e + 1, true)? else {
            // A line with only one word (+whitespace). Ignore silently.
            continue;
        };

        let in_word = &line[starti_b..starti_e];
        let out_word = &line[starto_b..starto_e];

        let (in_s, out_s) = if case_sensitive {
            // d->syn[cur].in = pstrdup(starti); ... = pstrdup(starto);
            (pstr_in(mcx, in_word)?, pstr_in(mcx, out_word)?)
        } else {
            // = str_tolower(starti, strlen(starti), DEFAULT_COLLATION_OID);
            let il = str_tolower::call(mcx, in_word, DEFAULT_COLLATION_OID)?;
            let ol = str_tolower::call(mcx, out_word, DEFAULT_COLLATION_OID)?;
            (bytes_to_pgstring(mcx, &il)?, bytes_to_pgstring(mcx, &ol)?)
        };

        let outlen = out_s.len() as i32;
        syn.try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<Syn>()))?;
        syn.push(Syn {
            r#in: in_s,
            out: out_s,
            outlen,
            flags,
        });
    }

    // qsort(d->syn, d->len, sizeof(Syn), compareSyn);
    syn.sort_by(compare_syn);

    Ok(DictSyn {
        syn,
        case_sensitive,
    })
}

/// `dsynonym_lexize(PG_FUNCTION_ARGS)`: bsearch the synonym table for the input.
///
/// `None` for the C `PG_RETURN_POINTER(NULL)` cases; `Some(vec)` is the single
/// substituted lexeme. Allocated in `mcx`.
pub fn dsynonym_lexize<'mcx>(
    mcx: Mcx<'mcx>,
    d: &DictSyn<'_>,
    input: &[u8],
    len: i32,
) -> PgResult<Option<PgVec<'mcx, TSLexeme<'mcx>>>> {
    // if (len <= 0 || d->len <= 0) PG_RETURN_POINTER(NULL);
    if len <= 0 || d.syn.is_empty() {
        return Ok(None);
    }

    // key.in = case_sensitive ? pnstrdup(in, len) : str_tolower(in, len, ...);
    let in_bytes = slice_from_in(input, len);
    let key_in: Vec<u8> = if d.case_sensitive {
        in_bytes.to_vec()
    } else {
        str_tolower::call(mcx, in_bytes, DEFAULT_COLLATION_OID)?.to_vec()
    };

    // found = bsearch(&key, d->syn, d->len, sizeof(Syn), compareSyn);
    let found = d
        .syn
        .binary_search_by(|probe| probe.r#in.as_bytes().cmp(&key_in[..]))
        .ok();

    let Some(idx) = found else {
        // if (!found) PG_RETURN_POINTER(NULL);
        return Ok(None);
    };
    let found = &d.syn[idx];

    // res = palloc0(sizeof(TSLexeme) * 2);
    // res[0].lexeme = pnstrdup(found->out, found->outlen);
    // res[0].flags = found->flags;
    let mut res: PgVec<'mcx, TSLexeme<'mcx>> = PgVec::new_in(mcx);
    let out_slice = &found.out.as_bytes()[..(found.outlen as usize).min(found.out.len())];
    let lexeme = bytes_to_pgstring(mcx, out_slice)?;
    res.try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<TSLexeme>()))?;
    res.push(TSLexeme {
        nvariant: 0,
        flags: found.flags,
        lexeme,
    });

    Ok(Some(res))
}

/// `pstrdup`-equivalent for a word byte slice into a `mcx` [`PgString`].
fn pstr_in<'mcx>(mcx: Mcx<'mcx>, b: &[u8]) -> PgResult<PgString<'mcx>> {
    bytes_to_pgstring(mcx, b)
}

fn bytes_to_pgstring<'mcx>(mcx: Mcx<'mcx>, b: &[u8]) -> PgResult<PgString<'mcx>> {
    let s = core::str::from_utf8(b)
        .map_err(|_| crate::elog_error("dict_synonym: non-UTF-8 word"))?;
    PgString::from_str_in(s, mcx)
}
