use ::mcx::{Mcx, PgVec};
use ::ts_locale::TSL_PREFIX;
use ::ts_parse::{parsetext, ParsedText, TsParseEnv};
use ::types_error::PgResult;

use crate::{OP_AND, OP_OR};

// One parser-stack action of pushval_morph, replayed into the tsquery
// parser state (order matches C: operands before their operator).
pub enum QPush<'mcx> {
    Value {
        word: PgVec<'mcx, u8>,
        weight: i16,
        prefix: bool,
    },
    Op {
        oper: i8,
        distance: i16,
    },
    Stop,
}

pub fn pushval_morph<'mcx, E: TsParseEnv<'mcx>>(
    mcx: Mcx<'mcx>,
    env: &mut E,
    strval: &[u8],
    weight: i16,
    prefix: bool,
    qoperator: i8,
    out: &mut PgVec<'mcx, QPush<'mcx>>,
) -> PgResult<()> {
    let mut prs = ParsedText::with_capacity(mcx, 4)?;
    parsetext(mcx, env, &mut prs, strval)?;

    if prs.words.is_empty() {
        out.push(QPush::Stop);
        return Ok(());
    }

    let words = core::mem::replace(&mut prs.words, PgVec::new_in(mcx));
    let n = words.len();
    let mut iter = words.into_iter().peekable();
    let mut count = 0usize;
    let mut pos: u16 = 0;
    let mut cntpos = 0u32;

    while count < n {
        let next_pos = iter.peek().expect("count < n").pos;
        if pos > 0 && pos + 1 < next_pos {
            while pos + 1 < next_pos {
                out.push(QPush::Stop);
                if cntpos > 0 {
                    out.push(QPush::Op { oper: qoperator, distance: 1 });
                }
                cntpos += 1;
                pos += 1;
            }
        }

        pos = next_pos;
        let mut cntvar = 0u32;
        while count < n && iter.peek().is_some_and(|w| w.pos == pos) {
            let variant = iter.peek().expect("peeked").nvariant;
            let mut cnt = 0u32;
            while count < n
                && iter
                    .peek()
                    .is_some_and(|w| w.pos == pos && w.nvariant == variant)
            {
                let w = iter.next().expect("peeked");
                out.push(QPush::Value {
                    prefix: (w.flags & TSL_PREFIX != 0) || prefix,
                    weight,
                    word: w.word,
                });
                if cnt > 0 {
                    out.push(QPush::Op { oper: OP_AND, distance: 0 });
                }
                cnt += 1;
                count += 1;
            }
            if cntvar > 0 {
                out.push(QPush::Op { oper: OP_OR, distance: 0 });
            }
            cntvar += 1;
        }

        if cntpos > 0 {
            out.push(QPush::Op { oper: qoperator, distance: 1 });
        }
        cntpos += 1;
    }

    Ok(())
}
