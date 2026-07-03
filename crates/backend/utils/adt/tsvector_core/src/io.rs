use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};

use crate::layout::*;
use crate::parser::{Next, TsvParser};

// uniquePos: sort, dedup by position keeping the max weight.
pub fn unique_pos(a: &mut PgVec<'_, WordEntryPos>) {
    if a.len() <= 1 {
        return;
    }
    a.sort_by_key(|&p| wep_getpos(p));
    let mut res = 0usize;
    for ptr in 1..a.len() {
        if wep_getpos(a[ptr]) != wep_getpos(a[res]) {
            res += 1;
            a[res] = a[ptr];
            if res >= MAXNUMPOS - 1 || wep_getpos(a[res]) as u32 == MAXENTRYPOS - 1 {
                break;
            }
        } else if wep_getweight(a[ptr]) > wep_getweight(a[res]) {
            let w = wep_getweight(a[ptr]);
            wep_setweight(&mut a[res], w);
        }
    }
    a.truncate(res + 1);
}

struct EntryIn<'mcx> {
    word: PgVec<'mcx, u8>,
    pos: PgVec<'mcx, WordEntryPos>,
}

#[cold]
fn too_long_word(esc: Option<&mut SoftErrorContext>, len: usize) -> PgResult<()> {
    ereturn(
        esc,
        (),
        PgError::error(format!(
            "word is too long ({len} bytes, max {} bytes)",
            MAXSTRLEN - 1
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
    )
}

#[cold]
fn string_too_long(esc: Option<&mut SoftErrorContext>, len: usize) -> PgResult<()> {
    ereturn(
        esc,
        (),
        PgError::error(format!(
            "string is too long for tsvector ({len} bytes, max {MAXSTRPOS} bytes)"
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
    )
}

// tsvectorin body; Ok(None) = soft error recorded in `esc`.
pub fn tsvector_in_core<'mcx>(
    mcx: Mcx<'mcx>,
    input: &[u8],
    mut esc: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let mut prs = TsvParser::new(mcx, input, 0, esc.take());
    let mut arr: PgVec<EntryIn> = PgVec::new_in(mcx);
    let mut strlen_total = 0usize;

    loop {
        match prs.next_token()? {
            Next::End => break,
            Next::Err => {
                return Ok(None);
            }
            Next::Tok => {
                if prs.word.len() >= MAXSTRLEN {
                    let n = prs.word.len();
                    too_long_word(prs.esc.take(), n)?;
                    return Ok(None);
                }
                if strlen_total > MAXSTRPOS {
                    string_too_long(prs.esc.take(), strlen_total)?;
                    return Ok(None);
                }
                strlen_total += prs.word.len();
                let mut word = vec_with_capacity_in(mcx, prs.word.len())?;
                word.extend_from_slice(&prs.word);
                let mut pos = vec_with_capacity_in(mcx, prs.pos.len())?;
                pos.extend_from_slice(&prs.pos);
                arr.push(EntryIn { word, pos });
            }
        }
    }
    esc = prs.esc.take();
    if let Some(c) = esc.as_deref() {
        if c.error_occurred() {
            return Ok(None);
        }
    }

    let mut buflen = 0usize;
    if !arr.is_empty() {
        arr.sort_by(|a, b| match ts_compare_string(&a.word, &b.word, false) {
            n if n < 0 => core::cmp::Ordering::Less,
            0 => core::cmp::Ordering::Equal,
            _ => core::cmp::Ordering::Greater,
        });
        // uniqueentry: merge duplicate lexemes' position lists.
        let mut res = 0usize;
        for ptr in 1..arr.len() {
            if arr[ptr].word != arr[res].word {
                res += 1;
                if res != ptr {
                    arr[res] = core::mem::replace(
                        &mut arr[ptr],
                        EntryIn { word: PgVec::new_in(mcx), pos: PgVec::new_in(mcx) },
                    );
                }
            } else if !arr[ptr].pos.is_empty() {
                let moved = core::mem::replace(&mut arr[ptr].pos, PgVec::new_in(mcx));
                arr[res].pos.extend_from_slice(&moved);
            }
        }
        arr.truncate(res + 1);
        for e in arr.iter_mut() {
            unique_pos(&mut e.pos);
            buflen += e.word.len();
            if !e.pos.is_empty() {
                buflen = shortalign(buflen);
                buflen += e.pos.len() * 2 + 2;
            }
        }
    }

    if buflen > MAXSTRPOS {
        string_too_long(esc, buflen)?;
        return Ok(None);
    }

    let mut b = TsVecBuilder::with_capacity(mcx, arr.len(), buflen)?;
    for e in &arr {
        b.push(&e.word, &e.pos)?;
    }
    Ok(Some(b.finish(mcx)?))
}

fn push_u16_dec(out: &mut PgVec<'_, u8>, v: u16) {
    let mut buf = [0u8; 5];
    let mut i = buf.len();
    let mut v = v as u32;
    loop {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    out.extend_from_slice(&buf[i..]);
}

pub fn tsvector_out_core<'mcx>(mcx: Mcx<'mcx>, v: TsVec<'_>) -> PgResult<PgVec<'mcx, u8>> {
    let size = v.size();
    let mut out: PgVec<u8> = vec_with_capacity_in(mcx, v.payload.len() * 2 + 8)?;
    for i in 0..size {
        let e = v.entry(i);
        if i != 0 {
            out.push(b' ');
        }
        out.push(b'\'');
        let lex = v.lexeme(e);
        let mut k = 0usize;
        while k < lex.len() {
            let cl = (::mbutils::pg_mblen(&lex[k..]) as usize).min(lex.len() - k);
            if lex[k] == b'\'' {
                out.push(b'\'');
            } else if lex[k] == b'\\' {
                out.push(b'\\');
            }
            out.extend_from_slice(&lex[k..k + cl]);
            k += cl;
        }
        out.push(b'\'');
        let poss = v.positions(e);
        if !poss.is_empty() {
            out.push(b':');
            for (j, &p) in poss.iter().enumerate() {
                if j != 0 {
                    out.push(b',');
                }
                push_u16_dec(&mut out, wep_getpos(p));
                match wep_getweight(p) {
                    3 => out.push(b'A'),
                    2 => out.push(b'B'),
                    1 => out.push(b'C'),
                    _ => {}
                }
            }
        }
    }
    out.push(0);
    Ok(out)
}

pub fn tsvector_send_core<'mcx>(
    mcx: Mcx<'mcx>,
    v: TsVec<'_>,
) -> PgResult<::datum::Bytea<'mcx>> {
    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendint32(&mut buf, v.size() as u32)?;
    for i in 0..v.size() {
        let e = v.entry(i);
        ::pqformat::pq_sendtext(&mut buf, v.lexeme(e))?;
        ::pqformat::pq_sendbyte(&mut buf, 0)?;
        let poss = v.positions(e);
        ::pqformat::pq_sendint16(&mut buf, poss.len() as u16)?;
        for &p in poss {
            ::pqformat::pq_sendint16(&mut buf, p)?;
        }
    }
    Ok(::pqformat::pq_endtypsend(buf))
}

pub fn tsvector_recv_core<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut ::stringinfo::StringInfo<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    let nentries = ::pqformat::pq_getmsgint(buf, 4)? as i32;
    if nentries < 0 || nentries as usize > 0x3fff_ffff / 4 {
        return Err(PgError::error("invalid size of tsvector").into());
    }
    let nentries = nentries as usize;
    let mut b = TsVecBuilder::with_capacity(mcx, nentries, nentries * 8)?;
    let mut needs_sort = false;
    let mut prev: Option<PgVec<'mcx, u8>> = None;
    let mut poss: PgVec<WordEntryPos> = PgVec::new_in(mcx);
    for _ in 0..nentries {
        let lexeme = ::pqformat::pq_getmsgstring(mcx, buf)?;
        let lex = lexeme.as_bytes();
        let mut word = vec_with_capacity_in(mcx, lex.len())?;
        word.extend_from_slice(lex);
        let npos = ::pqformat::pq_getmsgint(buf, 2)? as u16;

        if word.len() > MAXSTRLEN {
            return Err(PgError::error("invalid tsvector: lexeme too long").into());
        }
        if b.strlen() > MAXSTRPOS {
            return Err(
                PgError::error("invalid tsvector: maximum total lexeme length exceeded").into(),
            );
        }
        if npos as usize > MAXNUMPOS {
            return Err(PgError::error("unexpected number of tsvector positions").into());
        }
        poss.clear();
        for j in 0..npos {
            let p = ::pqformat::pq_getmsgint(buf, 2)? as WordEntryPos;
            if j > 0 && wep_getpos(p) <= wep_getpos(poss[j as usize - 1]) {
                return Err(PgError::error("position information is misordered").into());
            }
            poss.push(p);
        }
        if let Some(pw) = &prev {
            if ts_compare_string(&word, pw, false) <= 0 {
                needs_sort = true;
            }
        }
        b.push(&word, &poss)?;
        prev = Some(word);
    }

    if !needs_sort {
        return b.finish(mcx);
    }
    // Rare wire case: rebuild via sort on a decoded view.
    let img = b.finish(mcx)?;
    let v = TsVec { payload: &img[4..] };
    let mut idx: PgVec<usize> = vec_with_capacity_in(mcx, v.size())?;
    idx.extend(0..v.size());
    idx.sort_by(|&a, &bb| {
        match ts_compare_string(v.lexeme(v.entry(a)), v.lexeme(v.entry(bb)), false) {
            n if n < 0 => core::cmp::Ordering::Less,
            0 => core::cmp::Ordering::Equal,
            _ => core::cmp::Ordering::Greater,
        }
    });
    let mut b2 = TsVecBuilder::with_capacity(mcx, v.size(), img.len())?;
    for &i in &idx {
        let e = v.entry(i);
        b2.push_raw(v.lexeme(e), v.posblock(e))?;
    }
    b2.finish(mcx)
}
