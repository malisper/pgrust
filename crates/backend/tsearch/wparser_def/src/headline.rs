//! wparser_def.c headline selection: prsd_headline + hlCover +
//! mark_hl_words / mark_hl_fragments.

use ::adt_tsvector_core::execute::{
    ts_execute, ts_execute_locations, ExecPhraseData, Ternary, TS_EXEC_EMPTY,
};
use ::adt_tsvector_core::layout::wep_getpos;
use ::adt_tsvector_core::query::{Operand, TsQueryRef};
use ::mcx::Mcx;
use ::ts_cache::DefListItem;
use ::ts_parse::headline::{HeadlineParsedText, HeadlineWordEntry};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};

use crate::parser::{
    ASCIIHWORD, DECIMAL_T, HWORD, NUMHWORD, PROTOCOL, SCIENTIFIC, SIGNEDINT, SPACE, TAG_T,
    UNSIGNEDINT, URL_T, VERSIONNUMBER, XMLENTITY,
};

fn ts_idignore(x: i32) -> bool {
    x == TAG_T || x == PROTOCOL || x == SPACE || x == XMLENTITY
}
fn hlidreplace(x: i32) -> bool {
    x == TAG_T
}
fn hlidskip(x: i32) -> bool {
    x == URL_T || x == NUMHWORD || x == ASCIIHWORD || x == HWORD
}
fn xmlhlidskip(x: i32) -> bool {
    x == URL_T || x == NUMHWORD || x == ASCIIHWORD || x == HWORD
}
fn nonwordtoken(x: i32) -> bool {
    x == SPACE || hlidreplace(x) || hlidskip(x)
}
fn noendtoken(x: i32) -> bool {
    nonwordtoken(x)
        || x == SCIENTIFIC
        || x == VERSIONNUMBER
        || x == DECIMAL_T
        || x == SIGNEDINT
        || x == UNSIGNEDINT
        || ts_idignore(x)
}

fn interesting_word(w: &HeadlineWordEntry<'_>) -> bool {
    w.item.is_some() && !w.repeated
}
fn badendpoint(w: &HeadlineWordEntry<'_>, shortword: i32) -> bool {
    (noendtoken(w.typ) || w.word.len() as i32 <= shortword) && !interesting_word(w)
}

// checkcondition_HL (wparser_def.c) over a word subrange; item identity is
// the QueryItem index.
fn checkcondition_hl<'mcx>(
    mcx: Mcx<'mcx>,
    words: &[HeadlineWordEntry<'_>],
    item_idx: usize,
    _val: &Operand,
    data: Option<&mut ExecPhraseData<'mcx>>,
) -> Ternary {
    let _ = mcx;
    match data {
        None => {
            if words.iter().any(|w| w.item == Some(item_idx)) {
                Ternary::Yes
            } else {
                Ternary::No
            }
        }
        Some(data) => {
            for w in words {
                if w.item == Some(item_idx) {
                    if data.pos.is_empty() || *data.pos.last().unwrap() < w.pos {
                        data.pos.push(w.pos);
                    }
                }
            }
            if data.npos() > 0 {
                Ternary::Yes
            } else {
                Ternary::No
            }
        }
    }
}

// hlCover (wparser_def.c): earliest-after-*nextpos minimal cover.
fn hl_cover<'mcx>(
    mcx: Mcx<'mcx>,
    prs: &HeadlineParsedText<'mcx>,
    query: TsQueryRef<'_>,
    locations: &[ExecPhraseData<'mcx>],
    nextpos: &mut i32,
) -> PgResult<Option<(usize, usize)>> {
    let mut pos = *nextpos;

    loop {
        // For each AND'ed term/phrase, its first occurrence at/after pos.
        let mut pose = -1i32;
        for pdata in locations {
            let mut first = -1i32;
            for i in 0..pdata.npos() {
                // Phrase matches use the ending lexeme.
                let endp = wep_getpos(pdata.pos[i]) as i32;
                if endp >= pos {
                    first = endp;
                    break;
                }
            }
            if first < 0 {
                return Ok(None);
            }
            if first > pose {
                pose = first;
            }
        }
        if pose < 0 {
            return Ok(None);
        }

        // ... and its last occurrence at/before pose.
        let mut posb = i32::MAX - 1;
        for pdata in locations {
            let mut last = -1i32;
            for i in (0..pdata.npos()).rev() {
                // Phrase matches use the starting lexeme.
                let startp = wep_getpos(pdata.pos[i]) as i32 - pdata.width;
                if startp <= pose {
                    last = startp;
                    break;
                }
            }
            if last < posb {
                posb = last;
            }
        }
        // A phrase match may cross pos; the location data is imprecise for
        // phrase-OR-plain queries, so try the match starting at pos anyway.
        posb = posb.max(pos);

        if posb <= pose {
            // Convert lexeme positions to word indexes.
            let mut idxb = -1i64;
            let mut idxe = -1i64;
            for (i, w) in prs.words.iter().enumerate() {
                if w.item.is_none() {
                    continue;
                }
                if idxb < 0 && w.pos as i32 >= posb {
                    idxb = i as i64;
                }
                if (w.pos as i32) <= pose {
                    idxe = i as i64;
                } else {
                    break;
                }
            }
            if idxb >= 0 && idxe >= idxb {
                // Recheck that the range satisfies the query.
                let range = &prs.words[idxb as usize..=idxe as usize];
                let mut chk = |item_idx: usize,
                               val: &Operand,
                               data: Option<&mut ExecPhraseData<'mcx>>|
                 -> PgResult<Ternary> {
                    Ok(checkcondition_hl(mcx, range, item_idx, val, data))
                };
                if ts_execute(mcx, query, TS_EXEC_EMPTY, &mut chk)? {
                    *nextpos = posb + 1;
                    return Ok(Some((idxb as usize, idxe as usize)));
                }
            }
        }

        pos = posb + 1;
    }
}

// mark_fragment (wparser_def.c).
fn mark_fragment(prs: &mut HeadlineParsedText<'_>, highlightall: bool, startpos: usize, endpos: usize) {
    for i in startpos..=endpos {
        let w = &mut prs.words[i];
        if w.item.is_some() {
            w.selected = true;
        }
        if !highlightall {
            if hlidreplace(w.typ) {
                w.replace = true;
            } else if hlidskip(w.typ) {
                w.skip = true;
            }
        } else if xmlhlidskip(w.typ) {
            w.skip = true;
        }
        w.in_ = !w.repeated;
    }
}

// get_next_fragment (wparser_def.c).
fn get_next_fragment(
    prs: &HeadlineParsedText<'_>,
    startpos: &mut i32,
    endpos: &mut i32,
    curlen: &mut i32,
    poslen: &mut i32,
    max_words: i32,
) {
    let mut i = *startpos;
    while i <= *endpos {
        *startpos = i;
        if interesting_word(&prs.words[i as usize]) {
            break;
        }
        i += 1;
    }
    *curlen = 0;
    *poslen = 0;
    i = *startpos;
    while i <= *endpos && *curlen < max_words {
        if !nonwordtoken(prs.words[i as usize].typ) {
            *curlen += 1;
        }
        if interesting_word(&prs.words[i as usize]) {
            *poslen += 1;
        }
        i += 1;
    }
    if *endpos > i {
        *endpos = i;
        let mut j = *endpos;
        while j >= *startpos {
            *endpos = j;
            if interesting_word(&prs.words[j as usize]) {
                break;
            }
            if !nonwordtoken(prs.words[j as usize].typ) {
                *curlen -= 1;
            }
            j -= 1;
        }
    }
}

struct CoverPos {
    startpos: i32,
    endpos: i32,
    poslen: i32,
    curlen: i32,
    chosen: bool,
    excluded: bool,
}

// mark_hl_fragments (wparser_def.c): MaxFragments > 0 selector.
#[allow(clippy::too_many_arguments)]
fn mark_hl_fragments<'mcx>(
    mcx: Mcx<'mcx>,
    prs: &mut HeadlineParsedText<'mcx>,
    query: TsQueryRef<'_>,
    locations: &[ExecPhraseData<'mcx>],
    highlightall: bool,
    shortword: i32,
    min_words: i32,
    max_words: i32,
    max_fragments: i32,
) -> PgResult<()> {
    let mut covers: Vec<CoverPos> = Vec::with_capacity(32);
    let mut nextpos = 0i32;
    let (mut curlen, mut poslen) = (0i32, 0i32);

    while let Some((p, q)) = hl_cover(mcx, prs, query, locations, &mut nextpos)? {
        let mut startpos = p as i32;
        let mut endpos = q as i32;
        while startpos <= endpos {
            get_next_fragment(prs, &mut startpos, &mut endpos, &mut curlen, &mut poslen, max_words);
            covers.push(CoverPos {
                startpos,
                endpos,
                curlen,
                poslen,
                chosen: false,
                excluded: false,
            });
            startpos = endpos + 1;
            endpos = q as i32;
        }
    }

    let mut num_f = 0i32;
    for _ in 0..max_fragments {
        let mut maxitems = 0i32;
        let mut minwords = i32::MAX;
        let mut min_i: i64 = -1;
        for (i, c) in covers.iter().enumerate() {
            if !c.chosen
                && !c.excluded
                && (maxitems < c.poslen || (maxitems == c.poslen && minwords > c.curlen))
            {
                maxitems = c.poslen;
                minwords = c.curlen;
                min_i = i as i64;
            }
        }
        if min_i < 0 {
            break;
        }
        let min_i = min_i as usize;
        covers[min_i].chosen = true;
        let mut startpos = covers[min_i].startpos;
        let mut endpos = covers[min_i].endpos;
        let mut curlen = covers[min_i].curlen;
        if curlen < max_words {
            let maxstretch = (max_words - curlen) / 2;
            // Stretch the start: stop at document start, maxstretch, or an
            // already-marked fragment.
            let mut stretch = 0i32;
            let mut posmarker = startpos;
            let mut i = startpos - 1;
            while i >= 0 && stretch < maxstretch && !prs.words[i as usize].in_ {
                if !nonwordtoken(prs.words[i as usize].typ) {
                    curlen += 1;
                    stretch += 1;
                }
                posmarker = i;
                i -= 1;
            }
            // Cut back till a good endpoint.
            let mut i = posmarker;
            while i < startpos && badendpoint(&prs.words[i as usize], shortword) {
                if !nonwordtoken(prs.words[i as usize].typ) {
                    curlen -= 1;
                }
                i += 1;
            }
            startpos = i;
            // Stretch the end as much as possible.
            posmarker = endpos;
            let mut i = endpos + 1;
            while (i as usize) < prs.words.len() && curlen < max_words && !prs.words[i as usize].in_
            {
                if !nonwordtoken(prs.words[i as usize].typ) {
                    curlen += 1;
                }
                posmarker = i;
                i += 1;
            }
            // Cut back till a good endpoint.
            let mut i = posmarker;
            while i > endpos && badendpoint(&prs.words[i as usize], shortword) {
                if !nonwordtoken(prs.words[i as usize].typ) {
                    curlen -= 1;
                }
                i -= 1;
            }
            endpos = i;
        }
        covers[min_i].startpos = startpos;
        covers[min_i].endpos = endpos;
        covers[min_i].curlen = curlen;
        mark_fragment(prs, highlightall, startpos as usize, endpos as usize);
        num_f += 1;
        for (i, c) in covers.iter_mut().enumerate() {
            if i != min_i
                && ((c.startpos >= startpos && c.startpos <= endpos)
                    || (c.endpos >= startpos && c.endpos <= endpos)
                    || (c.startpos < startpos && c.endpos > endpos))
            {
                c.excluded = true;
            }
        }
    }

    if num_f <= 0 {
        let mut curlen = 0i32;
        let mut endpos: i64 = -1;
        let mut i = 0usize;
        while i < prs.words.len() && curlen < min_words {
            if !nonwordtoken(prs.words[i].typ) {
                curlen += 1;
            }
            endpos = i as i64;
            i += 1;
        }
        if endpos >= 0 {
            mark_fragment(prs, highlightall, 0, endpos as usize);
        }
    }
    Ok(())
}

// mark_hl_words (wparser_def.c): MaxFragments == 0 selector.
fn mark_hl_words<'mcx>(
    mcx: Mcx<'mcx>,
    prs: &mut HeadlineParsedText<'mcx>,
    query: TsQueryRef<'_>,
    locations: &[ExecPhraseData<'mcx>],
    highlightall: bool,
    shortword: i32,
    min_words: i32,
    max_words: i32,
) -> PgResult<()> {
    let mut bestb: i64 = -1;
    let mut beste: i64 = -1;
    let mut bestlen: i32 = -1;
    let mut bestcover = false;

    if !highlightall {
        let mut nextpos = 0i32;
        while let Some((p, q)) = hl_cover(mcx, prs, query, locations, &mut nextpos)? {
            let p = p as i64;
            let q = q as i64;
            let mut curlen = 0i32;
            let mut poslen = 0i32;
            let mut posb = p;
            let mut pose = p;
            let mut i = p;
            while i <= q && curlen < max_words {
                if !nonwordtoken(prs.words[i as usize].typ) {
                    curlen += 1;
                }
                if interesting_word(&prs.words[i as usize]) {
                    poslen += 1;
                }
                pose = i;
                i += 1;
            }

            if curlen < max_words {
                // Room to lengthen: search forward for a good stopping point,
                // reconsidering the word at q first.
                i -= 1;
                while (i as usize) < prs.words.len() && curlen < max_words {
                    if i > q {
                        if !nonwordtoken(prs.words[i as usize].typ) {
                            curlen += 1;
                        }
                        if interesting_word(&prs.words[i as usize]) {
                            poslen += 1;
                        }
                    }
                    pose = i;
                    if badendpoint(&prs.words[i as usize], shortword) {
                        i += 1;
                        continue;
                    }
                    if curlen >= min_words {
                        break;
                    }
                    i += 1;
                }
                if curlen < min_words {
                    // Still short at end of text: extend to the left.
                    let mut j = p - 1;
                    while j >= 0 {
                        if !nonwordtoken(prs.words[j as usize].typ) {
                            curlen += 1;
                        }
                        if interesting_word(&prs.words[j as usize]) {
                            poslen += 1;
                        }
                        if curlen >= max_words {
                            break;
                        }
                        if badendpoint(&prs.words[j as usize], shortword) {
                            j -= 1;
                            continue;
                        }
                        if curlen >= min_words {
                            break;
                        }
                        j -= 1;
                    }
                    posb = if j >= 0 { j } else { 0 };
                }
            } else {
                // Consider shortening to avoid a bad endpoint.
                if i > q {
                    i = q;
                }
                while curlen > min_words {
                    if !badendpoint(&prs.words[i as usize], shortword) {
                        break;
                    }
                    if !nonwordtoken(prs.words[i as usize].typ) {
                        curlen -= 1;
                    }
                    if interesting_word(&prs.words[i as usize]) {
                        poslen -= 1;
                    }
                    pose = i - 1;
                    i -= 1;
                }
            }

            let poscover = posb <= p && pose >= q;
            let best_bad = beste >= 0 && badendpoint(&prs.words[beste as usize], shortword);
            if (poscover && !bestcover)
                || (poscover == bestcover && poslen > bestlen)
                || (poscover == bestcover
                    && poslen == bestlen
                    && !badendpoint(&prs.words[pose as usize], shortword)
                    && best_bad)
            {
                bestb = posb;
                beste = pose;
                bestlen = poslen;
                bestcover = poscover;
            }
        }

        if bestlen < 0 {
            let mut curlen = 0i32;
            let mut pose: i64 = -1;
            let mut i = 0usize;
            while i < prs.words.len() && curlen < min_words {
                if !nonwordtoken(prs.words[i].typ) {
                    curlen += 1;
                }
                pose = i as i64;
                i += 1;
            }
            bestb = 0;
            beste = pose;
        }
    } else {
        bestb = 0;
        beste = prs.words.len() as i64 - 1;
    }

    if bestb >= 0 && beste >= bestb {
        mark_fragment(prs, highlightall, bestb as usize, beste as usize);
    }
    Ok(())
}

fn opt_err(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE))
}

// prsd_headline (wparser_def.c) body over deserialize_deflist items.
pub fn prsd_headline_impl<'mcx>(
    mcx: Mcx<'mcx>,
    prs: &mut HeadlineParsedText<'mcx>,
    options: &[DefListItem<'mcx>],
    query: TsQueryRef<'_>,
) -> PgResult<()> {
    let mut min_words = 15i32;
    let mut max_words = 35i32;
    let mut shortword = 3i32;
    let mut max_fragments = 0i32;
    let mut highlightall = false;

    let int_val = |item: &DefListItem<'_>| -> PgResult<i32> {
        let s = core::str::from_utf8(&item.value).unwrap_or("");
        s.trim().parse::<i32>().map_err(|_| {
            Box::new(
                PgError::error(format!("invalid input syntax for type integer: \"{s}\""))
                    .with_sqlstate(::types_error::ERRCODE_INVALID_TEXT_REPRESENTATION),
            )
        })
    };

    for item in options {
        let name = core::str::from_utf8(&item.name).unwrap_or("");
        let val = core::str::from_utf8(&item.value).unwrap_or("");
        if name.eq_ignore_ascii_case("MaxWords") {
            max_words = int_val(item)?;
        } else if name.eq_ignore_ascii_case("MinWords") {
            min_words = int_val(item)?;
        } else if name.eq_ignore_ascii_case("ShortWord") {
            shortword = int_val(item)?;
        } else if name.eq_ignore_ascii_case("MaxFragments") {
            max_fragments = int_val(item)?;
        } else if name.eq_ignore_ascii_case("StartSel") {
            prs.startsel = Some(bytes_in(mcx, val.as_bytes())?);
        } else if name.eq_ignore_ascii_case("StopSel") {
            prs.stopsel = Some(bytes_in(mcx, val.as_bytes())?);
        } else if name.eq_ignore_ascii_case("FragmentDelimiter") {
            prs.fragdelim = Some(bytes_in(mcx, val.as_bytes())?);
        } else if name.eq_ignore_ascii_case("HighlightAll") {
            highlightall = val.eq_ignore_ascii_case("1")
                || val.eq_ignore_ascii_case("on")
                || val.eq_ignore_ascii_case("true")
                || val.eq_ignore_ascii_case("t")
                || val.eq_ignore_ascii_case("y")
                || val.eq_ignore_ascii_case("yes");
        } else {
            return Err(opt_err(format!("unrecognized headline parameter: \"{name}\"")));
        }
    }

    if !highlightall {
        if min_words >= max_words {
            return Err(opt_err("MinWords must be less than MaxWords".into()));
        }
        if min_words <= 0 {
            return Err(opt_err("MinWords must be positive".into()));
        }
        if shortword < 0 {
            return Err(opt_err("ShortWord must be >= 0".into()));
        }
        if max_fragments < 0 {
            return Err(opt_err("MaxFragments must be >= 0".into()));
        }
    }

    let locations = if query.size() > 0 {
        let words = &prs.words;
        // SAFETY: locations computation only reads words; the later mutating
        // selectors run after this borrow ends (the raw slice pins the read
        // view over the closure, C's hlCheck shape).
        let words_view: &[HeadlineWordEntry<'mcx>] =
            unsafe { core::slice::from_raw_parts(words.as_ptr(), words.len()) };
        let mut chk = |item_idx: usize,
                       val: &Operand,
                       data: Option<&mut ExecPhraseData<'mcx>>|
         -> PgResult<Ternary> {
            Ok(checkcondition_hl(mcx, words_view, item_idx, val, data))
        };
        ts_execute_locations(mcx, query, &mut chk)?
    } else {
        Vec::new()
    };

    if max_fragments == 0 {
        mark_hl_words(mcx, prs, query, &locations, highlightall, shortword, min_words, max_words)?;
    } else {
        mark_hl_fragments(
            mcx,
            prs,
            query,
            &locations,
            highlightall,
            shortword,
            min_words,
            max_words,
            max_fragments,
        )?;
    }

    if prs.startsel.is_none() {
        prs.startsel = Some(bytes_in(mcx, b"<b>")?);
    }
    if prs.stopsel.is_none() {
        prs.stopsel = Some(bytes_in(mcx, b"</b>")?);
    }
    if prs.fragdelim.is_none() {
        prs.fragdelim = Some(bytes_in(mcx, b" ... ")?);
    }
    Ok(())
}

fn bytes_in<'mcx>(mcx: Mcx<'mcx>, b: &[u8]) -> PgResult<::mcx::PgVec<'mcx, u8>> {
    let mut v: ::mcx::PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, b.len())?;
    v.extend_from_slice(b);
    Ok(v)
}
