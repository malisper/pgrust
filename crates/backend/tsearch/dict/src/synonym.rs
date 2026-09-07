use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::ts_locale::dict_api::{def_get_boolean, DictInitData, LexizeResult};
use ::ts_locale::{
    could_not_open_error, get_tsearch_config_filename, lowerstr, tsearch_readline_begin, TsLexeme,
    TSL_PREFIX,
};
use ::types_error::PgResult;

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
    while i < line.len() && pg_string::isspace_c_locale(line[i]) {
        i += ::mbutils::pg_mblen(&line[i..]) as usize;
    }
    if i >= line.len() {
        return None;
    }
    let begin = i;
    let mut lastchar = i;
    while i < line.len() && !pg_string::isspace_c_locale(line[i]) {
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
    // dict_synonym.c:133 — `could not open synonym file "%s": %m`.
    let mut rd = match tsearch_readline_begin(mcx, &path) {
        Ok(rd) => rd,
        Err(errno) => return Err(could_not_open_error("synonym", &path, errno).into()),
    };
    // dict_synonym.c:138-199: the readline callback is on error_context_stack
    // for the whole loop, so an error while a line is in flight carries it.
    let mut syn: PgVec<'static, Syn> = PgVec::new_in(mcx);
    loop {
        let next = rd.readline();
        let Some(line) = rd.with_context(next)? else {
            break;
        };
        let parsed = synonym_line(mcx, &line, case_sensitive, &mut syn);
        rd.with_context(parsed)?;
    }
    let syn = sort_syn(mcx, syn)?;
    Ok(DictSyn { syn, case_sensitive })
}

// dict_synonym.c:32 compareSyn: strcmp on the input word only.
fn compare_syn(a: &Syn, b: &Syn) -> i32 {
    match a.input.as_slice().cmp(b.input.as_slice()) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

// dict_synonym.c:202 qsort(d->syn, d->len, sizeof(Syn), compareSyn): qsort
// is pg_qsort (port.h:479), whose equal-key output order is observable here
// — a synonym file may list one input word several times and bsearch below
// returns whichever of them the sort left at its probe. Sorting an index
// permutation with the same comparator over the referenced rows performs
// exactly the comparisons and swaps C performs on the Syn array itself, so
// the permutation is C's.
fn sort_syn(mcx: Mcx<'static>, syn: PgVec<'static, Syn>) -> PgResult<PgVec<'static, Syn>> {
    let mut order: PgVec<'static, usize> = vec_with_capacity_in(mcx, syn.len())?;
    order.extend(0..syn.len());
    ::pg_qsort::pg_qsort(&mut order, |&a, &b| compare_syn(&syn[a], &syn[b]));
    let mut slots: PgVec<'static, Option<Syn>> = PgVec::with_capacity_in(syn.len(), mcx);
    slots.extend(syn.into_iter().map(Some));
    let mut sorted: PgVec<'static, Syn> = PgVec::with_capacity_in(slots.len(), mcx);
    for i in order.iter() {
        // pg_qsort leaves a permutation of 0..n: every slot is taken once.
        sorted.push(slots[*i].take().expect("pg_qsort output is a permutation"));
    }
    Ok(sorted)
}

// bsearch(3) (glibc stdlib/bsearch.c, the shape C 18.6 links): probe
// (l + u) / 2 and return the FIRST equal element the probe sequence meets —
// not the lowest or highest equal index. Rust's binary_search_by chooses
// its probes and its equal-run landing point differently.
fn c_bsearch(syn: &[Syn], key: &[u8]) -> Option<usize> {
    let (mut l, mut u) = (0usize, syn.len());
    while l < u {
        let idx = (l + u) / 2;
        match key.cmp(syn[idx].input.as_slice()) {
            core::cmp::Ordering::Less => u = idx,
            core::cmp::Ordering::Greater => l = idx + 1,
            core::cmp::Ordering::Equal => return Some(idx),
        }
    }
    None
}

// One dict_synonym.c:138-197 loop iteration.
fn synonym_line(
    mcx: ::mcx::Mcx<'static>,
    line: &[u8],
    case_sensitive: bool,
    syn: &mut PgVec<'static, Syn>,
) -> PgResult<()> {
    let Some((bi, ei)) = findwrd(line, 0, None) else {
        return Ok(());
    };
    if ei >= line.len() {
        // A line with only one word. Ignore silently.
        return Ok(());
    }
    let mut flags = 0u16;
    let Some((bo, eo)) = findwrd(line, ei + 1, Some(&mut flags)) else {
        return Ok(());
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
    Ok(())
}

#[cfg(test)]
pub(crate) fn load_synonyms(
    mcx: ::mcx::Mcx<'static>,
    lines: &[PgVec<'static, u8>],
    case_sensitive: bool,
) -> PgResult<PgVec<'static, Syn>> {
    let mut syn: PgVec<'static, Syn> = PgVec::new_in(mcx);
    for line in lines.iter() {
        synonym_line(mcx, line, case_sensitive, &mut syn)?;
    }
    sort_syn(mcx, syn)
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
    // dict_synonym.c:230 bsearch(&key, d->syn, d->len, sizeof(Syn), compareSyn).
    let Some(idx) = c_bsearch(&d.syn, &key) else {
        return Ok(None);
    };
    let found = &d.syn[idx];
    let mut lexeme = vec_with_capacity_in(mcx, found.output.len())?;
    lexeme.extend_from_slice(&found.output);
    let mut out = PgVec::new_in(mcx);
    out.push(TsLexeme { nvariant: 0, flags: found.flags, lexeme });
    Ok(Some(LexizeResult(out)))
}
