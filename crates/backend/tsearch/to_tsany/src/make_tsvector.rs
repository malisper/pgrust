//! `make_tsvector` + `uniqueWORD` + `compareWORD` (to_tsany.c:57..240).
//!
//! Builds a `tsvector` on-disk image from a [`ParsedText`] (the output of
//! `parsetext`). The image layout is identical to `tsvectorin`'s — the same
//! `CALCDATASIZE` / `WordEntry` / position-vector format — so this reuses the
//! `backend-utils-adt-tsvector-core::access` helpers.
//!
//! Repo divergence from C: the repo's [`ParsedWord`] holds a *flat* `pos: u16`
//! and has no `pos.apos`/`alen` union (C's `ParsedWord.pos` is a
//! `union { uint16 pos; uint16 *apos; }` that `uniqueWORD` converts in place to
//! an `apos` array). Here `unique_word` returns the deduplicated word list plus
//! a parallel `Vec<Vec<u16>>` of per-surviving-word position arrays (each
//! mirroring C's `apos`, where `apos[0]` is the count), which `make_tsvector`
//! then emits. The arithmetic (the `LIMITPOS`, `MAXNUMPOS`, `MAXENTRYPOS`
//! dedup/clamp rules, the doubling growth) is byte-for-byte the same.

use alloc::vec::Vec;
use core::cmp::Ordering;

use parse::{ParsedText, ParsedWord};
use ::tsvector_core::access::{
    set_tsv_size, set_varsize, shortalign, strptr_off, SIZEOF_NPOS, SIZEOF_WEP, SIZEOF_WORDENTRY,
};
use ::tsearch::tsearch::DATAHDRSIZE;
use ::utils_error::ereport;
use types_error::{PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED};
use ::tsearch::tsearch::{WordEntry, WordEntryPos, LIMITPOS, MAXENTRYPOS, MAXNUMPOS, MAXSTRPOS};

/// `compareWORD(a, b)` (to_tsany.c:57): order by `tsCompareString` over the
/// lexeme bytes, then by `pos.pos`.
fn compare_word(a: &ParsedWord, b: &ParsedWord) -> Ordering {
    // tsCompareString(a->word, a->len, b->word, b->len, false): a prefix-aware
    // byte comparison. With `prefix == false` it is a plain length-then-bytes
    // comparison of the two lexemes (memcmp over min(len), shorter sorts first).
    let res = ts_compare_string(&a.word, a.len as usize, &b.word, b.len as usize);
    match res {
        Ordering::Equal => {
            if a.pos == b.pos {
                Ordering::Equal
            } else if a.pos > b.pos {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        other => other,
    }
}

/// `tsCompareString(a, lena, b, lenb, prefix=false)` (ts_utils.c): compare the
/// raw lexeme bytes. `memcmp` over the shared length, then the lengths.
fn ts_compare_string(a: &[u8], lena: usize, b: &[u8], lenb: usize) -> Ordering {
    let n = lena.min(lenb);
    match a[..n].cmp(&b[..n]) {
        Ordering::Equal => lena.cmp(&lenb),
        other => other,
    }
}

/// `uniqueWORD(a, l)` (to_tsany.c:77): sort the words and merge duplicates,
/// summarizing each surviving word's position list.
///
/// Returns the deduplicated word list (in result order) and, for each surviving
/// word, its `apos` array: index 0 is the count, indices `1..=count` are the
/// positions (exactly C's `apos`).
fn unique_word(mut words: Vec<ParsedWord>) -> (Vec<ParsedWord>, Vec<Vec<u16>>) {
    let l = words.len();

    if l == 1 {
        // tmppos = LIMITPOS(a->pos.pos); apos = {1, tmppos}
        let tmppos = LIMITPOS(words[0].pos as i32) as u16;
        let apos = alloc::vec![1u16, tmppos];
        words[0].alen = 2;
        return (words, alloc::vec![apos]);
    }

    // Sort words with their positions.
    words.sort_by(compare_word);

    let mut out_words: Vec<ParsedWord> = Vec::new();
    let mut out_apos: Vec<Vec<u16>> = Vec::new();

    // Initialize first word and its first position.
    let tmppos = LIMITPOS(words[0].pos as i32) as u16;
    {
        let mut w = clone_word_head(&words[0]);
        w.alen = 2;
        out_words.push(w);
        out_apos.push(alloc::vec![1u16, tmppos]);
    }

    // Summarize position information for each word (C: while (ptr - a < l)).
    for ptr in words.iter().skip(1) {
        let res_idx = out_words.len() - 1;
        let res = &out_words[res_idx];
        let same = ptr.len == res.len && ptr.word[..res.len as usize] == res.word[..res.len as usize];
        if !same {
            // Got a new word, so put it in result.
            let tmppos = LIMITPOS(ptr.pos as i32) as u16;
            let mut w = clone_word_head(ptr);
            w.alen = 2;
            out_words.push(w);
            out_apos.push(alloc::vec![1u16, tmppos]);
        } else {
            // The word already exists; adjust position info, honoring the
            // MAXNUMPOS / MAXENTRYPOS / uniqueness checks (C:138-150).
            let apos = &mut out_apos[res_idx];
            let count = apos[0] as usize;
            let limit = LIMITPOS(ptr.pos as i32) as u16;
            if (apos[0] as i32) < MAXNUMPOS - 1
                && apos[count] != MAXENTRYPOS - 1
                && apos[count] != limit
            {
                // C grows the apos array by doubling against `alen`; the Vec
                // grows automatically, so only the `apos[0]`-uniqueness guard
                // matters here.
                if apos[0] == 0 || apos[count] != limit {
                    apos.push(limit);
                    apos[0] += 1;
                }
            }
        }
    }

    (out_words, out_apos)
}

/// Copy a `ParsedWord`'s identity fields (the surviving-word head C keeps:
/// `len`, `word`, `nvariant`, `flags`); position state is rebuilt in `apos`.
fn clone_word_head(w: &ParsedWord) -> ParsedWord {
    ParsedWord {
        flags: w.flags,
        len: w.len,
        nvariant: w.nvariant,
        alen: w.alen,
        pos: w.pos,
        word: w.word.clone(),
    }
}

/// `make_tsvector(prs)` (to_tsany.c:165): build the `tsvector` image. Frees
/// `prs->words` (here: leaves `prs` consumed; caller drops it).
pub fn make_tsvector(prs: &mut ParsedText) -> PgResult<Vec<u8>> {
    // Merge duplicate words.
    let words = core::mem::take(&mut prs.words);
    let (words, apos): (Vec<ParsedWord>, Vec<Vec<u16>>) = if !words.is_empty() {
        unique_word(words)
    } else {
        (Vec::new(), Vec::new())
    };
    let curwords = words.len();
    prs.curwords = curwords as i32;

    // Determine space needed (lenstr).
    let mut lenstr: usize = 0;
    for (i, w) in words.iter().enumerate() {
        lenstr += w.len as usize;
        let alen = apos.get(i).map(|a| a.len()).unwrap_or(0);
        // C: if (prs->words[i].alen) — a word always has alen==2+ after
        // uniqueWORD, so positions are always present.
        if alen != 0 {
            lenstr = shortalign(lenstr);
            let npos = apos[i][0] as usize;
            lenstr += SIZEOF_NPOS + npos * SIZEOF_WEP;
        }
    }

    if lenstr > MAXSTRPOS as usize {
        return Err(ereport(::types_error::ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(alloc::format!(
                "string is too long for tsvector ({} bytes, max {} bytes)",
                lenstr,
                MAXSTRPOS
            ))
            .into_error());
    }

    // totallen = CALCDATASIZE(curwords, lenstr).
    let totallen = DATAHDRSIZE + curwords * SIZEOF_WORDENTRY + lenstr;
    let mut out: Vec<u8> = alloc::vec![0u8; totallen];
    set_varsize(&mut out, totallen);
    set_tsv_size(&mut out, curwords as i32);

    let strbuf_off = strptr_off(curwords as i32);
    let mut stroff: usize = 0;
    for (i, w) in words.iter().enumerate() {
        let elen = w.len as usize;
        let mut entry = WordEntry::default();
        entry.set_len(elen as u32);
        entry.set_pos(stroff as u32);
        // memcpy(str + stroff, word, len).
        let dst = strbuf_off + stroff;
        out[dst..dst + elen].copy_from_slice(&w.word[..elen]);
        stroff += elen;

        let count = apos.get(i).map(|a| a[0] as usize).unwrap_or(0);
        if count > 0 {
            if count > 0xFFFF {
                return Err(::types_error::PgError::error("positions array too long"));
            }
            entry.set_haspos(1);
            stroff = shortalign(stroff);
            // *(uint16 *)(str + stroff) = (uint16) k;
            let npos = count as u16;
            out[strbuf_off + stroff..strbuf_off + stroff + SIZEOF_NPOS]
                .copy_from_slice(&npos.to_ne_bytes());
            // wptr = POSDATAPTR; WEP_SETWEIGHT(0); WEP_SETPOS(apos[j+1]).
            for j in 0..count {
                let mut wep: WordEntryPos = 0;
                ::tsearch::tsearch::WEP_SETWEIGHT(&mut wep, 0);
                ::tsearch::tsearch::WEP_SETPOS(&mut wep, apos[i][j + 1]);
                let off = strbuf_off + stroff + SIZEOF_NPOS + j * SIZEOF_WEP;
                out[off..off + SIZEOF_WEP].copy_from_slice(&wep.to_ne_bytes());
            }
            stroff += SIZEOF_NPOS + count * SIZEOF_WEP;
        } else {
            entry.set_haspos(0);
        }

        let eoff = DATAHDRSIZE + i * SIZEOF_WORDENTRY;
        out[eoff..eoff + SIZEOF_WORDENTRY].copy_from_slice(&entry.word.to_ne_bytes());
    }

    debug_assert_eq!(strbuf_off + stroff, totallen);
    Ok(out)
}
