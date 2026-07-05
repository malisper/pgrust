//! ts_parse.c headline framework: hlparsetext + generateHeadline.

use ::adt_tsvector_core::layout::ts_compare_string;
use ::adt_tsvector_core::query::{Item, TsQueryRef};
use ::mcx::{Mcx, PgVec};
use ::ts_locale::{TsLexeme, TSL_ADDPOS};
use ::types_error::PgResult;

use crate::{limitpos, LexizeData, ParsePlex, TsParseEnv, MAXSTRLEN};

// HeadlineWordEntry (ts_public.h); `item` is the matched QueryItem index
// (C stores a QueryOperand pointer).
pub struct HeadlineWordEntry<'mcx> {
    pub word: PgVec<'mcx, u8>,
    pub typ: i32,
    pub pos: u16,
    pub item: Option<usize>,
    pub selected: bool,
    pub in_: bool,
    pub replace: bool,
    pub repeated: bool,
    pub skip: bool,
}

pub struct HeadlineParsedText<'mcx> {
    pub words: PgVec<'mcx, HeadlineWordEntry<'mcx>>,
    pub vectorpos: i32,
    pub startsel: Option<PgVec<'mcx, u8>>,
    pub stopsel: Option<PgVec<'mcx, u8>>,
    pub fragdelim: Option<PgVec<'mcx, u8>>,
}

impl<'mcx> HeadlineParsedText<'mcx> {
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        HeadlineParsedText {
            words: PgVec::new_in(mcx),
            vectorpos: 0,
            startsel: None,
            stopsel: None,
            fragdelim: None,
        }
    }
}

// hladdword (ts_parse.c).
fn hladdword<'mcx>(
    mcx: Mcx<'mcx>,
    prs: &mut HeadlineParsedText<'mcx>,
    buf: &[u8],
    typ: i32,
) -> PgResult<()> {
    let mut word: PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, buf.len())?;
    word.extend_from_slice(buf);
    prs.words.push(HeadlineWordEntry {
        word,
        typ,
        pos: 0,
        item: None,
        selected: false,
        in_: false,
        replace: false,
        repeated: false,
        skip: false,
    });
    Ok(())
}

// hlfinditem (ts_parse.c): attach pos + matching query items to the
// last-added word; extra matches replicate it with repeated = 1.
fn hlfinditem<'mcx>(
    mcx: Mcx<'mcx>,
    prs: &mut HeadlineParsedText<'mcx>,
    query: TsQueryRef<'_>,
    pos: i32,
    buf: &[u8],
) -> PgResult<()> {
    let widx = prs.words.len() - 1;
    prs.words[widx].pos = limitpos(pos.max(0) as u32);
    for i in 0..query.size() {
        let Item::Val(op) = query.item(i) else { continue };
        if ts_compare_string(query.operand_str(&op), buf, op.prefix) == 0 {
            if prs.words[widx].item.is_some() {
                let w = &prs.words[widx];
                let mut word: PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, w.word.len())?;
                word.extend_from_slice(&w.word);
                let dup = HeadlineWordEntry {
                    word,
                    typ: w.typ,
                    pos: w.pos,
                    item: Some(i),
                    selected: w.selected,
                    in_: w.in_,
                    replace: w.replace,
                    repeated: true,
                    skip: w.skip,
                };
                prs.words.push(dup);
            } else {
                prs.words[widx].item = Some(i);
            }
        }
    }
    Ok(())
}

// addHLParsedLex (ts_parse.c): consumed raw tokens become words; norms
// attach match data to the last-added word.
fn add_hl_parsed_lex<'mcx>(
    mcx: Mcx<'mcx>,
    prs: &mut HeadlineParsedText<'mcx>,
    query: TsQueryRef<'_>,
    buf: &[u8],
    lexs: &[ParsePlex],
    norms: Option<&[TsLexeme<'mcx>]>,
) -> PgResult<()> {
    for lex in lexs {
        if lex.typ > 0 {
            hladdword(
                mcx,
                prs,
                &buf[lex.off as usize..(lex.off + lex.len) as usize],
                lex.typ,
            )?;
        }
        if let Some(norms) = norms {
            let mut savedpos = prs.vectorpos;
            for n in norms {
                if n.flags & TSL_ADDPOS != 0 {
                    savedpos += 1;
                }
                hlfinditem(mcx, prs, query, savedpos, &n.lexeme)?;
            }
        }
    }
    if let Some(norms) = norms {
        for n in norms {
            if n.flags & TSL_ADDPOS != 0 {
                prs.vectorpos += 1;
            }
        }
    }
    Ok(())
}

// hlparsetext (ts_parse.c).
pub fn hlparsetext<'mcx, E: TsParseEnv<'mcx>>(
    mcx: Mcx<'mcx>,
    env: &mut E,
    prs: &mut HeadlineParsedText<'mcx>,
    query: TsQueryRef<'_>,
    buf: &[u8],
) -> PgResult<()> {
    env.prs_start(buf)?;
    let mut ldata = LexizeData::new(mcx);

    loop {
        let (typ, off, len) = env.prs_next()?;

        if typ > 0 && len as usize >= MAXSTRLEN {
            crate::parse::elog_notice_word_too_long()?;
            continue;
        }

        ldata.add_lemm_pub(typ, off, len);

        loop {
            let prev_head = ldata.head();
            let norms = crate::parse::lexize_exec(&mut ldata, env, buf)?;
            let consumed = ldata.consumed_since(prev_head);
            match norms {
                Some(norms) => {
                    prs.vectorpos += 1;
                    add_hl_parsed_lex(mcx, prs, query, buf, &consumed, Some(&norms))?;
                }
                None => {
                    add_hl_parsed_lex(mcx, prs, query, buf, &consumed, None)?;
                    break;
                }
            }
        }

        if typ <= 0 {
            break;
        }
    }

    env.prs_end()
}

// generateHeadline (ts_parse.c).
pub fn generate_headline<'mcx>(
    mcx: Mcx<'mcx>,
    prs: &HeadlineParsedText<'mcx>,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut out: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    let mut infrag = false;
    let mut numfragments = 0usize;

    for wrd in prs.words.iter() {
        if wrd.in_ && !wrd.repeated {
            if !infrag {
                infrag = true;
                numfragments += 1;
                if numfragments > 1 {
                    ::mcx::vec_append_bytes(&mut out, prs.fragdelim.as_deref().expect("fragdelim filled"))?;
                }
            }
            if wrd.replace {
                out.push(b' ');
            } else if !wrd.skip {
                if wrd.selected {
                    ::mcx::vec_append_bytes(&mut out, prs.startsel.as_deref().expect("startsel filled"))?;
                }
                ::mcx::vec_append_bytes(&mut out, &wrd.word)?;
                if wrd.selected {
                    ::mcx::vec_append_bytes(&mut out, prs.stopsel.as_deref().expect("stopsel filled"))?;
                }
            }
        } else if !wrd.repeated {
            infrag = false;
        }
    }

    Ok(out)
}
