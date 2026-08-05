use ::adt_tsvector_core::layout::TsVec;
use ::mcx::{MemoryContext, Mcx, PgVec};
use ::ts_locale::{DictSubState, TsLexeme};
use ::ts_parse::{ParsedText, ParsedWord, TsParseEnv};
use ::types_core::Oid;
use ::types_error::PgResult;

use crate::query::{pushval_morph, QPush};
use crate::vector::make_tsvector;
use crate::{OP_AND, OP_OR, OP_PHRASE};

fn word<'mcx>(mcx: Mcx<'mcx>, w: &str, pos: u16, nvariant: u16) -> ParsedWord<'mcx> {
    let mut v = PgVec::new_in(mcx);
    v.extend_from_slice(w.as_bytes());
    ParsedWord { word: v, nvariant, flags: 0, pos, apos: PgVec::new_in(mcx) }
}

#[test]
fn make_tsvector_sorts_dedups_and_merges_positions() {
    let ctx = MemoryContext::new("to-tsany-test");
    let mcx = ctx.mcx();
    let mut prs = ParsedText::with_capacity(mcx, 4).unwrap();
    prs.words.push(word(mcx, "dog", 2, 0));
    prs.words.push(word(mcx, "cat", 1, 0));
    prs.words.push(word(mcx, "dog", 3, 0));
    prs.words.push(word(mcx, "dog", 3, 0));

    let img = make_tsvector(mcx, &mut prs).unwrap();
    let v = TsVec { payload: &img[4..] };
    assert_eq!(v.size(), 2);
    let e0 = v.entry(0);
    let e1 = v.entry(1);
    assert_eq!(v.lexeme(e0), b"cat");
    assert_eq!(v.positions(e0), &[1]);
    assert_eq!(v.lexeme(e1), b"dog");
    assert_eq!(v.positions(e1), &[2, 3]);
}

#[test]
fn make_tsvector_empty_input() {
    let ctx = MemoryContext::new("to-tsany-test");
    let mcx = ctx.mcx();
    let mut prs = ParsedText::with_capacity(mcx, 2).unwrap();
    let img = make_tsvector(mcx, &mut prs).unwrap();
    let v = TsVec { payload: &img[4..] };
    assert_eq!(v.size(), 0);
    assert_eq!(img.len(), 8);
}

struct MorphEnv<'mcx> {
    mcx: Mcx<'mcx>,
    toks: Vec<(u32, u32)>,
    next: usize,
}

const D_MOCK: Oid = 900;

impl<'mcx> MorphEnv<'mcx> {
    fn new(mcx: Mcx<'mcx>) -> Self {
        MorphEnv { mcx, toks: Vec::new(), next: 0 }
    }
}

impl<'mcx> TsParseEnv<'mcx> for MorphEnv<'mcx> {
    fn prs_start(&mut self, buf: &[u8]) -> PgResult<()> {
        self.toks.clear();
        self.next = 0;
        let mut start = None;
        for (i, &b) in buf.iter().chain([b' '].iter()).enumerate() {
            match (start, b == b' ') {
                (None, false) => start = Some(i),
                (Some(s), true) => {
                    self.toks.push((s as u32, (i - s) as u32));
                    start = None;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn prs_next(&mut self) -> PgResult<(i32, u32, u32)> {
        if self.next >= self.toks.len() {
            return Ok((0, 0, 0));
        }
        let (off, len) = self.toks[self.next];
        self.next += 1;
        Ok((1, off, len))
    }

    fn prs_end(&mut self) -> PgResult<()> {
        Ok(())
    }

    fn map_len(&mut self, toktype: i32) -> PgResult<usize> {
        Ok(if toktype == 1 { 1 } else { 0 })
    }

    fn map_dict(&mut self, _t: i32, _i: usize) -> PgResult<Oid> {
        Ok(D_MOCK)
    }

    fn lexize(
        &mut self,
        _dict: Oid,
        token: &[u8],
        _state: &mut DictSubState,
    ) -> PgResult<Option<PgVec<'mcx, TsLexeme<'mcx>>>> {
        let t = std::str::from_utf8(token).unwrap();
        let mut out = PgVec::new_in(self.mcx);
        if t == "the" {
            return Ok(Some(out));
        }
        let variants: Vec<(&str, u16)> = if t == "dual" {
            vec![("dual", 1), ("duo", 2)]
        } else {
            vec![(t.trim_end_matches('s'), 0)]
        };
        for (w, nv) in variants {
            let mut b = PgVec::new_in(self.mcx);
            b.extend_from_slice(w.as_bytes());
            out.push(TsLexeme { nvariant: nv, flags: 0, lexeme: b });
        }
        Ok(Some(out))
    }
}

fn render(pushes: &PgVec<'_, QPush<'_>>) -> Vec<String> {
    pushes
        .iter()
        .map(|p| match p {
            QPush::Value { word, prefix, .. } => format!(
                "V:{}{}",
                String::from_utf8(word.to_vec()).unwrap(),
                if *prefix { ":*" } else { "" }
            ),
            QPush::Op { oper, distance } => match *oper {
                OP_AND => "AND".into(),
                OP_OR => "OR".into(),
                OP_PHRASE => format!("PHRASE<{distance}>"),
                _ => "?".into(),
            },
            QPush::Stop => "STOP".into(),
        })
        .collect()
}

fn morph<'mcx>(mcx: Mcx<'mcx>, s: &str, qoperator: i8) -> PgVec<'mcx, QPush<'mcx>> {
    let mut env = MorphEnv::new(mcx);
    let mut out = PgVec::new_in(mcx);
    pushval_morph(mcx, &mut env, s.as_bytes(), 0, false, qoperator, &mut out).unwrap();
    out
}

#[test]
fn morph_words_join_with_qoperator() {
    let ctx = MemoryContext::new("to-tsany-test");
    let pushes = morph(ctx.mcx(), "fat cats", OP_PHRASE);
    assert_eq!(render(&pushes), ["V:fat", "V:cat", "PHRASE<1>"]);
}

#[test]
fn morph_stopword_gap_gets_placeholder() {
    let ctx = MemoryContext::new("to-tsany-test");
    let pushes = morph(ctx.mcx(), "fat the cats", OP_PHRASE);
    assert_eq!(
        render(&pushes),
        ["V:fat", "STOP", "PHRASE<1>", "V:cat", "PHRASE<1>"]
    );
}

#[test]
fn morph_all_stopwords_pushes_single_stop() {
    let ctx = MemoryContext::new("to-tsany-test");
    let pushes = morph(ctx.mcx(), "the the", OP_PHRASE);
    assert_eq!(render(&pushes), ["STOP"]);
}

#[test]
fn morph_variants_or_together() {
    let ctx = MemoryContext::new("to-tsany-test");
    let pushes = morph(ctx.mcx(), "dual", OP_PHRASE);
    assert_eq!(render(&pushes), ["V:dual", "V:duo", "OR"]);
}

mod json_workers {
    use super::MorphEnv;
    use crate::json::{json_to_tsvector_worker, jsonb_to_tsvector_worker};
    use ::adt_jsonb::iterate::{JTI_ALL, JTI_STRING};
    use ::adt_tsvector_core::layout::TsVec;
    use ::mcx::{Mcx, MemoryContext};

    fn setup() {
        let _ = mbutils::SetDatabaseEncoding(wchar::PG_UTF8);
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(mbutils::init_seams);
    }

    fn jsonb_payload<'m>(mcx: Mcx<'m>, doc: &[u8]) -> Vec<u8> {
        ::adt_jsonb::io::jsonb_in(mcx, doc, None)
            .unwrap()
            .expect("hard path returns Some")[..]
            .to_vec()
    }

    fn words(img: &[u8]) -> Vec<(String, Vec<u16>)> {
        let v = TsVec { payload: &img[4..] };
        (0..v.size())
            .map(|i| {
                let e = v.entry(i);
                (
                    String::from_utf8(v.lexeme(e).to_vec()).unwrap(),
                    v.positions(e).to_vec(),
                )
            })
            .collect()
    }

    const DOC: &[u8] = br#"{"a": "fat cats", "b": {"c": "dogs"}, "n": 7}"#;

    #[test]
    fn jsonb_worker_breaks_positions_between_elements() {
        setup();
        let ctx = MemoryContext::new("to-tsany-test");
        let mcx = ctx.mcx();
        let jb = jsonb_payload(mcx, DOC);
        let mut env = MorphEnv::new(mcx);
        let img = jsonb_to_tsvector_worker(mcx, &mut env, &jb[4..], JTI_STRING).unwrap();
        assert_eq!(
            words(&img),
            vec![
                ("cat".to_string(), vec![2]),
                ("dog".to_string(), vec![4]),
                ("fat".to_string(), vec![1]),
            ]
        );
    }

    #[test]
    fn json_worker_matches_jsonb_worker() {
        setup();
        let ctx = MemoryContext::new("to-tsany-test");
        let mcx = ctx.mcx();
        let jb = jsonb_payload(mcx, DOC);
        let mut env1 = MorphEnv::new(mcx);
        let a = jsonb_to_tsvector_worker(mcx, &mut env1, &jb[4..], JTI_ALL).unwrap();
        let mut env2 = MorphEnv::new(mcx);
        let b = json_to_tsvector_worker(mcx, &mut env2, DOC, JTI_ALL).unwrap();
        assert_eq!(&a[..], &b[..]);
    }
}
