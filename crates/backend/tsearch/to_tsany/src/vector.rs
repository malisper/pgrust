use ::adt_tsvector_core::layout::{shortalign, TsVecBuilder, MAXNUMPOS, MAXSTRPOS};
use ::mcx::{Mcx, PgVec};
use ::ts_parse::{limitpos, ParsedText, ParsedWord, MAXENTRYPOS};
use ::types_error::{PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED};

fn compare_word(a: &ParsedWord<'_>, b: &ParsedWord<'_>) -> core::cmp::Ordering {
    a.word
        .as_slice()
        .cmp(b.word.as_slice())
        .then(a.pos.cmp(&b.pos))
}

// uniqueWORD: sort + merge duplicate lexemes, folding positions into apos.
pub fn unique_words<'mcx>(mcx: Mcx<'mcx>, prs: &mut ParsedText<'mcx>) -> PgResult<()> {
    if prs.words.is_empty() {
        return Ok(());
    }
    let mut words = core::mem::replace(&mut prs.words, PgVec::new_in(mcx));
    words.sort_unstable_by(compare_word);

    let mut out: PgVec<'mcx, ParsedWord<'mcx>> = PgVec::new_in(mcx);
    out.try_reserve_exact(words.len())
        .map_err(|_| mcx.oom(words.len()))?;
    for mut w in words {
        let pos = limitpos(w.pos as u32);
        match out.last_mut() {
            Some(res) if res.word.as_slice() == w.word.as_slice() => {
                let last = *res.apos.last().expect("merged word has positions");
                if res.apos.len() < MAXNUMPOS - 1
                    && last != (MAXENTRYPOS - 1) as u16
                    && last != pos
                {
                    res.apos.push(pos);
                }
            }
            _ => {
                w.apos = PgVec::new_in(mcx);
                w.apos.push(pos);
                out.push(w);
            }
        }
    }
    prs.words = out;
    Ok(())
}

// make_tsvector: dedup + flat image build (4-byte zero header for stamping).
pub fn make_tsvector<'mcx>(mcx: Mcx<'mcx>, prs: &mut ParsedText<'mcx>) -> PgResult<PgVec<'mcx, u8>> {
    unique_words(mcx, prs)?;

    let mut lenstr = 0usize;
    for w in prs.words.iter() {
        lenstr += w.word.len();
        if !w.apos.is_empty() {
            lenstr = shortalign(lenstr);
            lenstr += 2 + w.apos.len() * 2;
        }
    }
    if lenstr > MAXSTRPOS {
        return Err(PgError::error(format!(
            "string is too long for tsvector ({lenstr} bytes, max {MAXSTRPOS} bytes)"
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .into());
    }

    let mut b = TsVecBuilder::with_capacity(mcx, prs.words.len(), lenstr)?;
    for w in prs.words.iter() {
        b.push(&w.word, &w.apos)?;
    }
    b.finish(mcx)
}
