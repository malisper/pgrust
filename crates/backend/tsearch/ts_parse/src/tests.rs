use ::mcx::{MemoryContext, Mcx, PgVec};
use ::ts_locale::{DictSubState, TsLexeme, TSL_FILTER};
use ::types_core::Oid;
use ::types_error::PgResult;

use crate::parse::{parsetext, ParsedText, TsParseEnv};

const D_STEM: Oid = 1001;
const D_STOP: Oid = 1002;
const D_THES: Oid = 1003;
const D_FILTER: Oid = 1004;

struct MockEnv<'mcx> {
    mcx: Mcx<'mcx>,
    tokens: Vec<(i32, &'static str)>,
    next: usize,
    buf: Vec<u8>,
    dicts: Vec<Oid>,
    thes_seen: Vec<String>,
}

impl<'mcx> MockEnv<'mcx> {
    fn new(mcx: Mcx<'mcx>, tokens: Vec<(i32, &'static str)>, dicts: Vec<Oid>) -> Self {
        MockEnv { mcx, tokens, next: 0, buf: Vec::new(), dicts, thes_seen: Vec::new() }
    }

    fn lex<'a>(&self, s: &str) -> PgVec<'mcx, TsLexeme<'mcx>> {
        let mut v = PgVec::new_in(self.mcx);
        let mut w = PgVec::new_in(self.mcx);
        w.extend_from_slice(s.as_bytes());
        v.push(TsLexeme { nvariant: 0, flags: 0, lexeme: w });
        v
    }
}

impl<'mcx> TsParseEnv<'mcx> for MockEnv<'mcx> {
    fn prs_start(&mut self, buf: &[u8]) -> PgResult<()> {
        self.buf = buf.to_vec();
        let mut off = 0usize;
        let toks = core::mem::take(&mut self.tokens);
        self.tokens = toks
            .into_iter()
            .map(|(t, w)| {
                let pos = String::from_utf8(self.buf.clone())
                    .unwrap()
                    .get(off..)
                    .and_then(|rest| rest.find(w).map(|p| p + off))
                    .unwrap_or(0);
                off = pos + w.len();
                (t, w)
            })
            .collect();
        self.next = 0;
        Ok(())
    }

    fn prs_next(&mut self) -> PgResult<(i32, u32, u32)> {
        if self.next >= self.tokens.len() {
            return Ok((0, 0, 0));
        }
        let (t, w) = self.tokens[self.next];
        self.next += 1;
        let off = std::str::from_utf8(&self.buf).unwrap().find(w).unwrap();
        Ok((t, off as u32, w.len() as u32))
    }

    fn prs_end(&mut self) -> PgResult<()> {
        Ok(())
    }

    fn map_len(&mut self, toktype: i32) -> PgResult<usize> {
        Ok(if toktype == 1 { self.dicts.len() } else { 0 })
    }

    fn map_dict(&mut self, _toktype: i32, i: usize) -> PgResult<Oid> {
        Ok(self.dicts[i])
    }

    fn lexize(
        &mut self,
        dict: Oid,
        token: &[u8],
        state: &mut DictSubState,
    ) -> PgResult<Option<PgVec<'mcx, TsLexeme<'mcx>>>> {
        let t = std::str::from_utf8(token).unwrap().to_ascii_lowercase();
        match dict {
            D_STEM => Ok(Some(self.lex(t.trim_end_matches('s')))),
            D_STOP => {
                if t == "the" {
                    Ok(Some(PgVec::new_in(self.mcx)))
                } else {
                    Ok(None)
                }
            }
            D_FILTER => {
                if t == "colour" {
                    let mut v = self.lex("color");
                    v[0].flags = TSL_FILTER;
                    Ok(Some(v))
                } else {
                    Ok(None)
                }
            }
            D_THES => {
                // two-word phrase "new york" -> "nyc"; getnext protocol.
                if state.isend && !self.thes_seen.is_empty() {
                    let phrase = self.thes_seen.join(" ");
                    self.thes_seen.clear();
                    if phrase == "new york" {
                        return Ok(Some(self.lex("nyc")));
                    }
                    return Ok(None);
                }
                if t == "new" {
                    self.thes_seen.push(t);
                    state.getnext = true;
                    return Ok(None);
                }
                if t == "york" && self.thes_seen == ["new"] {
                    self.thes_seen.push(t);
                    state.getnext = true;
                    return Ok(Some(self.lex("nyc")));
                }
                if !self.thes_seen.is_empty() {
                    self.thes_seen.clear();
                    return Ok(None);
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }
}

fn run<'mcx>(
    mcx: Mcx<'mcx>,
    text: &str,
    tokens: Vec<(i32, &'static str)>,
    dicts: Vec<Oid>,
) -> ParsedText<'mcx> {
    let mut env = MockEnv::new(mcx, tokens, dicts);
    let mut prs = ParsedText::with_capacity(mcx, 4).unwrap();
    parsetext(mcx, &mut env, &mut prs, text.as_bytes()).unwrap();
    prs
}

fn words(prs: &ParsedText<'_>) -> Vec<(String, u16)> {
    prs.words
        .iter()
        .map(|w| (String::from_utf8(w.word.to_vec()).unwrap(), w.pos))
        .collect()
}

#[test]
fn basic_stemming_and_positions() {
    let ctx = MemoryContext::new("ts-parse-test");
    let prs = run(
        ctx.mcx(),
        "cats dogs",
        vec![(1, "cats"), (1, "dogs")],
        vec![D_STOP, D_STEM],
    );
    assert_eq!(words(&prs), vec![("cat".into(), 1), ("dog".into(), 2)]);
}

#[test]
fn stopword_advances_position() {
    let ctx = MemoryContext::new("ts-parse-test");
    let prs = run(
        ctx.mcx(),
        "the cats",
        vec![(1, "the"), (1, "cats")],
        vec![D_STOP, D_STEM],
    );
    assert_eq!(words(&prs), vec![("cat".into(), 2)]);
}

#[test]
fn unmapped_token_type_skipped_without_position() {
    let ctx = MemoryContext::new("ts-parse-test");
    let prs = run(
        ctx.mcx(),
        "x cats",
        vec![(2, "x"), (1, "cats")],
        vec![D_STEM],
    );
    assert_eq!(words(&prs), vec![("cat".into(), 1)]);
}

#[test]
fn filter_dict_rewrites_token_for_later_dicts() {
    let ctx = MemoryContext::new("ts-parse-test");
    let prs = run(
        ctx.mcx(),
        "colour",
        vec![(1, "colour")],
        vec![D_FILTER, D_STEM],
    );
    assert_eq!(words(&prs), vec![("color".into(), 1)]);
}

#[test]
fn thesaurus_multiword_match() {
    let ctx = MemoryContext::new("ts-parse-test");
    let prs = run(
        ctx.mcx(),
        "new york cats",
        vec![(1, "new"), (1, "york"), (1, "cats")],
        vec![D_THES, D_STEM],
    );
    assert_eq!(words(&prs), vec![("nyc".into(), 1), ("cat".into(), 2)]);
}

#[test]
fn thesaurus_partial_match_falls_back() {
    let ctx = MemoryContext::new("ts-parse-test");
    let prs = run(
        ctx.mcx(),
        "new cats",
        vec![(1, "new"), (1, "cats")],
        vec![D_THES, D_STEM],
    );
    assert_eq!(words(&prs), vec![("new".into(), 1), ("cat".into(), 2)]);
}
