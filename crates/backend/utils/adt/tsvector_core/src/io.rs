//! Port of `src/backend/utils/adt/tsvector.c` — the I/O functions for the
//! `tsvector` type.
//!
//! A `tsvector` value is its flat varlena image (`TSVectorData`): a 4-byte
//! varlena header, an `int32 size`, then `size` `WordEntry` records, then the
//! lexeme/position storage. `tsvectorin`/`tsvectorrecv` produce those bytes
//! (`Vec<u8>`); `tsvectorout`/`tsvectorsend` consume them. `tsvectorin` drives
//! the in-crate parser directly (same crate, with positions) rather than
//! through the seam, which drops positions.

use alloc::format;
use alloc::string::ToString;
use alloc::vec::Vec;

use mcx::Mcx;
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR,
};
use stringinfo::StringInfo;
use tsearch::tsearch::{
    WordEntry, WordEntryPos, DATAHDRSIZE, MAXENTRYPOS, MAXNUMPOS, MAXSTRLEN, MAXSTRPOS, WEP_GETPOS,
    WEP_GETWEIGHT,
};

use pqformat as pq;
use ts_small::util::oom;
use utils_error::ereport;
use mbutils_seams as mb;

use crate::access::{
    arrptr, posdatalen, posdataptr, set_arrptr, set_varsize, shortalign, strptr_off, tsv_size,
    SIZEOF_NPOS, SIZEOF_WEP, SIZEOF_WORDENTRY,
};
use crate::parser;

extern crate alloc;

/// `MaxAllocSize` (memutils.h).
const MAX_ALLOC_SIZE: usize = mcx::MAX_ALLOC_SIZE;

/// `WordEntryIN` (tsvector.c) — the internal build record while parsing /
/// constructing a `tsvector`: a [`WordEntry`] (must be first, see
/// [`compareentry`]) plus an owned position array and its length.
#[derive(Clone, Default)]
struct WordEntryIN {
    /// must be first, see `compareentry`
    entry: WordEntry,
    /// positions for this lexeme
    pos: Vec<WordEntryPos>,
    /// number of elements in `pos`
    poslen: i32,
}

/// `pg_cmp_s32` (common/int.h) — `(a > b) - (a < b)`.
#[inline]
fn pg_cmp_s32(a: i32, b: i32) -> i32 {
    (a > b) as i32 - (a < b) as i32
}

/// `compareWordEntryPos` (tsvector.c:36) — qsort comparator on the position
/// (`WEP_GETPOS`) of two [`WordEntryPos`] values.
fn compareWordEntryPos(a: &WordEntryPos, b: &WordEntryPos) -> i32 {
    pg_cmp_s32(WEP_GETPOS(*a) as i32, WEP_GETPOS(*b) as i32)
}

/// `uniquePos` (tsvector.c:52) — sort a lexeme's positions and remove
/// duplicates. If two entries share a pos but differ in weight, the higher
/// weight is retained (so `qunique` is not usable). Returns the new length.
fn uniquePos(a: &mut [WordEntryPos], l: i32) -> i32 {
    if l <= 1 {
        return l;
    }
    let l = l as usize;
    a[..l].sort_by(|x, y| match compareWordEntryPos(x, y) {
        n if n < 0 => core::cmp::Ordering::Less,
        0 => core::cmp::Ordering::Equal,
        _ => core::cmp::Ordering::Greater,
    });

    let mut res: usize = 0;
    let mut ptr: usize = 1;
    while ptr < l {
        if WEP_GETPOS(a[ptr]) != WEP_GETPOS(a[res]) {
            res += 1;
            a[res] = a[ptr];
            if res >= (MAXNUMPOS as usize - 1) || WEP_GETPOS(a[res]) == MAXENTRYPOS - 1 {
                break;
            }
        } else if WEP_GETWEIGHT(a[ptr]) > WEP_GETWEIGHT(a[res]) {
            let w = WEP_GETWEIGHT(a[ptr]);
            tsearch::tsearch::WEP_SETWEIGHT(&mut a[res], w);
        }
        ptr += 1;
    }
    (res + 1) as i32
}

/// `compareentry` (tsvector.c:87) — `qsort_arg` comparator on two [`WordEntry`]
/// by their lexeme strings, looked up in `buf` at `entry.pos`.
fn compareentry(a: &WordEntry, b: &WordEntry, buf: &[u8]) -> i32 {
    let apos = a.pos() as usize;
    let alen = a.len() as usize;
    let bpos = b.pos() as usize;
    let blen = b.len() as usize;
    crate::op::tsCompareString(&buf[apos..apos + alen], &buf[bpos..bpos + blen], false)
}

/// `uniqueentry` (tsvector.c:103) — sort an array of [`WordEntryIN`], remove
/// duplicate lexemes (merging positions), reporting the bytes needed for the
/// string + position storage in `outbuflen`. Returns the new length.
fn uniqueentry(
    a: &mut [WordEntryIN],
    l: i32,
    buf: &[u8],
    outbuflen: &mut i32,
) -> PgResult<i32> {
    debug_assert!(l >= 1);
    let l = l as usize;

    if l > 1 {
        a[..l].sort_by(|x, y| match compareentry(&x.entry, &y.entry, buf) {
            n if n < 0 => core::cmp::Ordering::Less,
            0 => core::cmp::Ordering::Equal,
            _ => core::cmp::Ordering::Greater,
        });
    }

    let mut buflen: usize = 0;
    let mut res: usize = 0;
    let mut ptr: usize = 1;
    while ptr < l {
        let same = a[ptr].entry.len() == a[res].entry.len() && {
            let rp = a[res].entry.pos() as usize;
            let pp = a[ptr].entry.pos() as usize;
            let n = a[res].entry.len() as usize;
            buf[pp..pp + n] == buf[rp..rp + n]
        };

        if !same {
            buflen += a[res].entry.len() as usize;
            if a[res].entry.haspos() != 0 {
                let newpos = uniquePos(&mut a[res].pos, a[res].poslen);
                a[res].poslen = newpos;
                buflen = shortalign(buflen);
                buflen += a[res].poslen as usize * SIZEOF_WEP + SIZEOF_NPOS;
            }
            res += 1;
            if res != ptr {
                a[res] = core::mem::take(&mut a[ptr]);
            }
        } else if a[ptr].entry.haspos() != 0 {
            if a[res].entry.haspos() != 0 {
                let newlen = a[ptr].poslen + a[res].poslen;
                let mut moved = core::mem::take(&mut a[ptr].pos);
                a[res].pos.try_reserve(moved.len()).map_err(|_| oom())?;
                a[res].pos.append(&mut moved);
                a[res].poslen = newlen;
            } else {
                a[res].entry.set_haspos(1);
                a[res].pos = core::mem::take(&mut a[ptr].pos);
                a[res].poslen = a[ptr].poslen;
            }
        }
        ptr += 1;
    }

    buflen += a[res].entry.len() as usize;
    if a[res].entry.haspos() != 0 {
        let newpos = uniquePos(&mut a[res].pos, a[res].poslen);
        a[res].poslen = newpos;
        buflen = shortalign(buflen);
        buflen += a[res].poslen as usize * SIZEOF_WEP + SIZEOF_NPOS;
    }

    *outbuflen = buflen as i32;
    Ok((res + 1) as i32)
}

/// A fresh zero-filled owned buffer of `len` bytes (palloc0 analog).
fn try_zeroed(len: usize) -> PgResult<Vec<u8>> {
    let mut v: Vec<u8> = Vec::new();
    v.try_reserve(len).map_err(|_| oom())?;
    v.resize(len, 0u8);
    Ok(v)
}

/// `elog(ERROR, ...)` for the internal errors of the I/O functions.
fn elog_internal(msg: &str) -> PgError {
    PgError::error(msg.to_string())
}

/// `tsvectorin` (tsvector.c:175) — parse a text `tsvector` literal into the
/// on-disk `TSVectorData` bytes. `buf` is the input C-string content (without
/// the trailing NUL).
pub fn tsvectorin<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &[u8],
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Vec<u8>>> {
    let _ = mcx;
    // C: state = init_tsvector_parser(buf, 0, escontext);  A lexer/syntax error
    // ereturns through `escontext`; with a soft sink installed `tsvectorin`
    // returns NULL (`Ok(None)`), otherwise it throws (`Err`).
    let state = parser::init_tsvector_parser_seam(buf, 0)?;

    let r = tsvectorin_build(state, buf, escontext.as_deref_mut());
    parser::close_tsvector_parser_seam(state);
    r
}

fn tsvectorin_build(
    state: tsearch::tsearch::TsVectorParseStateHandle,
    buf: &[u8],
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Vec<u8>>> {
    let _ = buf;
    // arr is the WordEntryIN build array; tmpbuf accumulates lexeme bytes.
    let mut arr: Vec<WordEntryIN> = Vec::new();
    let mut tmpbuf: Vec<u8> = Vec::new();
    let mut len: usize = 0;

    // C: while (gettoken_tsvector(state, &token, &toklen, &pos, &poslen, NULL,
    //          escontext))
    loop {
        let tok = match parser::gettoken_tsvector_full(state, true, escontext.as_deref_mut())? {
            Some(t) => t,
            None => break,
        };

        let token = tok.strval;
        let pos = tok.pos;
        let poslen = pos.len() as i32;
        let toklen = token.len();

        // C: ereturn(escontext, (Datum) 0, ...) — soft with a sink installed,
        // hard otherwise.
        if toklen >= MAXSTRLEN as usize {
            let err = ereport(ERROR)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg(format!(
                    "word is too long ({} bytes, max {} bytes)",
                    toklen as i64,
                    (MAXSTRLEN - 1) as i64
                ))
                .into_error();
            return ereturn(escontext.as_deref_mut(), None, err);
        }

        if tmpbuf.len() > MAXSTRPOS as usize {
            let err = ereport(ERROR)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg(format!(
                    "string is too long for tsvector ({} bytes, max {} bytes)",
                    tmpbuf.len() as i64,
                    MAXSTRPOS as i64
                ))
                .into_error();
            return ereturn(escontext.as_deref_mut(), None, err);
        }

        if len >= arr.len() {
            arr.try_reserve(1).map_err(|_| oom())?;
            arr.push(WordEntryIN::default());
        }

        let cur = tmpbuf.len();
        arr[len].entry.set_len(toklen as u32);
        arr[len].entry.set_pos(cur as u32);
        tmpbuf.try_reserve(toklen).map_err(|_| oom())?;
        tmpbuf.extend_from_slice(&token);

        if poslen != 0 {
            arr[len].entry.set_haspos(1);
            arr[len].pos = pos;
            arr[len].poslen = poslen;
        } else {
            arr[len].entry.set_haspos(0);
            arr[len].pos = Vec::new();
            arr[len].poslen = 0;
        }
        len += 1;
    }

    // C: close_tsvector_parser(state); if (SOFT_ERROR_OCCURRED(escontext))
    //    PG_RETURN_NULL();  A soft lexer/syntax error recorded during
    // tokenizing returns NULL here (the caller closes the parser).
    if let Some(ctx) = escontext.as_deref_mut() {
        if ctx.error_occurred() {
            return Ok(None);
        }
    }

    let mut buflen: i32 = 0;
    if len > 0 {
        len = uniqueentry(&mut arr, len as i32, &tmpbuf, &mut buflen)? as usize;
    } else {
        buflen = 0;
    }

    if buflen > MAXSTRPOS as i32 {
        let err = ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!(
                "string is too long for tsvector ({} bytes, max {} bytes)",
                buflen, MAXSTRPOS
            ))
            .into_error();
        return ereturn(escontext.as_deref_mut(), None, err);
    }

    // totallen = CALCDATASIZE(len, buflen);
    let totallen = DATAHDRSIZE + len * SIZEOF_WORDENTRY + buflen as usize;
    let mut out = try_zeroed(totallen)?;
    set_varsize(&mut out, totallen);
    crate::access::set_tsv_size(&mut out, len as i32);

    let strbuf_off = strptr_off(len as i32);
    let mut stroff: usize = 0;
    for i in 0..len {
        let epos = arr[i].entry.pos() as usize;
        let elen = arr[i].entry.len() as usize;
        let dst = strbuf_off + stroff;
        out[dst..dst + elen].copy_from_slice(&tmpbuf[epos..epos + elen]);
        arr[i].entry.set_pos(stroff as u32);
        stroff += elen;
        if arr[i].entry.haspos() != 0 {
            // unreachable because of MAXNUMPOS restrictions
            if arr[i].poslen > 0xFFFF {
                return Err(elog_internal("positions array too long"));
            }
            stroff = shortalign(stroff);
            let npos = arr[i].poslen as u16;
            out[strbuf_off + stroff..strbuf_off + stroff + 2].copy_from_slice(&npos.to_ne_bytes());
            stroff += SIZEOF_NPOS;
            for k in 0..arr[i].poslen as usize {
                let p = arr[i].pos[k];
                let off = strbuf_off + stroff + k * SIZEOF_WEP;
                out[off..off + 2].copy_from_slice(&p.to_ne_bytes());
            }
            stroff += arr[i].poslen as usize * SIZEOF_WEP;
        }
        let off = DATAHDRSIZE + i * SIZEOF_WORDENTRY;
        out[off..off + 4].copy_from_slice(&arr[i].entry.word.to_ne_bytes());
    }

    debug_assert_eq!(strbuf_off + stroff, totallen);
    Ok(Some(out))
}

/// `tsvectorout` (tsvector.c:314) — render the on-disk `TSVectorData` bytes to
/// their canonical text representation (no trailing NUL).
pub fn tsvectorout<'mcx>(mcx: Mcx<'mcx>, vec: &[u8]) -> PgResult<Vec<u8>> {
    let _ = mcx;
    let size = tsv_size(vec);
    let str_off = strptr_off(size);

    // lenbuf computation mirroring C's `palloc(lenbuf)`.
    let mut lenbuf: usize = if size > 0 {
        (size as usize) * 2 + (size as usize - 1) + 2
    } else {
        2usize.saturating_sub(1)
    };
    for i in 0..size {
        let ent = arrptr(vec, i as usize);
        lenbuf += ent.len() as usize * 2;
        if ent.haspos() != 0 {
            lenbuf += 1 + 7 * posdatalen(vec, size, ent) as usize;
        }
    }

    let mut outbuf: Vec<u8> = Vec::new();
    outbuf.try_reserve(lenbuf).map_err(|_| oom())?;

    for i in 0..size {
        let ent = arrptr(vec, i as usize);
        let curin_off = str_off + ent.pos() as usize;
        let curend = curin_off + ent.len() as usize;
        let mut curin = curin_off;

        if i != 0 {
            outbuf.push(b' ');
        }
        outbuf.push(b'\'');
        while curin < curend {
            let mut mlen = mb::pg_mblen_range::call(&vec[curin..curend])?;
            if vec[curin] == b'\'' {
                outbuf.push(b'\'');
            } else if vec[curin] == b'\\' {
                outbuf.push(b'\\');
            }
            while mlen > 0 {
                outbuf.push(vec[curin]);
                curin += 1;
                mlen -= 1;
            }
        }
        outbuf.push(b'\'');

        let pp = posdatalen(vec, size, ent);
        if pp != 0 {
            outbuf.push(b':');
            let mut p = pp;
            let mut widx = 0usize;
            while p != 0 {
                let wptr = posdataptr(vec, size, ent, widx);
                outbuf.extend_from_slice(WEP_GETPOS(wptr).to_string().as_bytes());
                match WEP_GETWEIGHT(wptr) {
                    3 => outbuf.push(b'A'),
                    2 => outbuf.push(b'B'),
                    1 => outbuf.push(b'C'),
                    _ => {}
                }
                if p > 1 {
                    outbuf.push(b',');
                }
                p -= 1;
                widx += 1;
            }
        }
    }

    Ok(outbuf)
}

/// `tsvectorsend` (tsvector.c:408) — serialize the on-disk bytes to the binary
/// wire format. Returns the `bytea` body bytes (the varlena wrapper of
/// `pq_endtypsend` is intentionally dropped, matching the sibling
/// `tsquerysend`).
pub fn tsvectorsend<'mcx>(mcx: Mcx<'mcx>, vec: &[u8]) -> PgResult<Vec<u8>> {
    let size = tsv_size(vec);
    let str_off = strptr_off(size);

    let mut buf = pq::pq_begintypsend(mcx)?;
    pq::pq_sendint32(&mut buf, size as u32)?;
    for i in 0..size {
        let ent = arrptr(vec, i as usize);
        // The strings are not null-terminated, so send the NUL separately.
        let lex_off = str_off + ent.pos() as usize;
        let lex_len = ent.len() as usize;
        pq::pq_sendtext(&mut buf, &vec[lex_off..lex_off + lex_len])?;
        pq::pq_sendbyte(&mut buf, b'\0')?;

        let npos = posdatalen(vec, size, ent);
        pq::pq_sendint16(&mut buf, npos)?;
        for j in 0..npos as usize {
            let wep = posdataptr(vec, size, ent, j);
            pq::pq_sendint16(&mut buf, wep)?;
        }
    }

    let bytea = pq::pq_endtypsend(buf);
    Ok(bytea.as_bytes().to_vec())
}

/// `tsvectorrecv` (tsvector.c:447) — deserialize the binary wire format into
/// the on-disk `TSVectorData` bytes, validating lexeme ordering / lengths.
pub fn tsvectorrecv<'mcx>(mcx: Mcx<'mcx>, buf: &mut StringInfo<'mcx>) -> PgResult<Vec<u8>> {
    let _ = mcx;
    let nentries = pq::pq_getmsgint(buf, core::mem::size_of::<i32>() as i32)? as i32;
    if nentries < 0 || (nentries as usize) > (MAX_ALLOC_SIZE / SIZEOF_WORDENTRY) {
        return Err(elog_internal("invalid size of tsvector"));
    }
    let nentries = nentries as usize;

    let hdrlen = DATAHDRSIZE + SIZEOF_WORDENTRY * nentries;
    let mut vlen = hdrlen * 2; // times two to make room for lexemes
    let mut vec = try_zeroed(vlen)?;
    crate::access::set_tsv_size(&mut vec, nentries as i32);

    let strptr_off_v = hdrlen;
    let mut datalen: usize = 0;
    let mut need_sort = false;

    for i in 0..nentries {
        let lexeme = read_msgstring(buf)?;
        let npos = pq::pq_getmsgint(buf, core::mem::size_of::<u16>() as i32)? as u16;

        let lex_len = lexeme.len();
        if lex_len > MAXSTRLEN as usize {
            return Err(elog_internal("invalid tsvector: lexeme too long"));
        }
        if datalen > MAXSTRPOS as usize {
            return Err(elog_internal(
                "invalid tsvector: maximum total lexeme length exceeded",
            ));
        }
        if npos as i32 > MAXNUMPOS {
            return Err(elog_internal("unexpected number of tsvector positions"));
        }

        // Make sure the buffer is large enough first.
        while hdrlen + shortalign(datalen + lex_len) + SIZEOF_NPOS + npos as usize * SIZEOF_WEP
            >= vlen
        {
            vlen *= 2;
            vec.try_reserve(vlen.saturating_sub(vec.len())).map_err(|_| oom())?;
            vec.resize(vlen, 0u8);
        }

        let mut ent = WordEntry::default();
        ent.set_haspos(if npos > 0 { 1 } else { 0 });
        ent.set_len(lex_len as u32);
        ent.set_pos(datalen as u32);
        set_arrptr(&mut vec, i, ent);

        vec[strptr_off_v + datalen..strptr_off_v + datalen + lex_len].copy_from_slice(&lexeme);
        datalen += lex_len;

        if i > 0 {
            let prev = arrptr(&vec, i - 1);
            let cur = arrptr(&vec, i);
            if compareentry(&cur, &prev, &vec[strptr_off_v..]) <= 0 {
                need_sort = true;
            }
        }

        if npos > 0 {
            // Pad to 2-byte alignment if necessary.
            if datalen != shortalign(datalen) {
                vec[strptr_off_v + datalen] = b'\0';
                datalen = shortalign(datalen);
            }
            vec[strptr_off_v + datalen..strptr_off_v + datalen + 2]
                .copy_from_slice(&npos.to_ne_bytes());

            let posbase = strptr_off_v + datalen + SIZEOF_NPOS;
            let mut prev_pos: u16 = 0;
            for j in 0..npos as usize {
                let raw = pq::pq_getmsgint(buf, core::mem::size_of::<u16>() as i32)? as WordEntryPos;
                let woff = posbase + j * SIZEOF_WEP;
                vec[woff..woff + 2].copy_from_slice(&raw.to_ne_bytes());
                if j > 0 && WEP_GETPOS(raw) <= WEP_GETPOS(prev_pos) {
                    return Err(elog_internal("position information is misordered"));
                }
                prev_pos = raw;
            }
            datalen += SIZEOF_NPOS + npos as usize * SIZEOF_WEP;
        }
    }

    let total = hdrlen + datalen;
    vec.truncate(total);
    set_varsize(&mut vec, total);

    if need_sort {
        sort_entries(&mut vec, nentries, strptr_off_v);
    }

    Ok(vec)
}

/// `pq_getmsgstring(buf)` — a NUL-terminated C string; returns its content
/// (without the NUL). Mirrors the libpq reader semantics inline (the pqformat
/// variant returns a context-charged copy; the bytes are needed only briefly).
fn read_msgstring(buf: &mut StringInfo<'_>) -> PgResult<Vec<u8>> {
    let data = buf.as_bytes();
    let start = buf.cursor;
    let mut cur = start;
    while cur < data.len() && data[cur] != 0 {
        cur += 1;
    }
    if cur >= data.len() {
        return Err(elog_internal("invalid string in message"));
    }
    let s = data[start..cur].to_vec();
    buf.cursor = cur + 1; // skip NUL
    Ok(s)
}

/// `qsort_arg(ARRPTR(vec), size, sizeof(WordEntry), compareentry, STRPTR(vec))`
/// — sort the entries array in place by lexeme order. The lexemes are not
/// moved; only the `WordEntry` records are reordered.
fn sort_entries(vec: &mut [u8], size: usize, strptr_off_v: usize) {
    let mut entries: Vec<u32> = (0..size).map(|i| arrptr(vec, i).word).collect();
    let strbuf = vec[strptr_off_v..].to_vec();
    entries.sort_by(|a, b| {
        let ea = WordEntry { word: *a };
        let eb = WordEntry { word: *b };
        match compareentry(&ea, &eb, &strbuf) {
            n if n < 0 => core::cmp::Ordering::Less,
            0 => core::cmp::Ordering::Equal,
            _ => core::cmp::Ordering::Greater,
        }
    });
    for (i, w) in entries.into_iter().enumerate() {
        set_arrptr(vec, i, WordEntry { word: w });
    }
}
