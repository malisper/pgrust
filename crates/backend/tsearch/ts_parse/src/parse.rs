use ::mcx::{Mcx, PgVec};
use ::ts_locale::{DictSubState, TsLexeme, TSL_ADDPOS, TSL_PREFIX};
use ::types_core::primitive::InvalidOid;
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED};

pub const MAXSTRLEN: usize = (1 << 11) - 1;
pub const MAXENTRYPOS: u32 = 1 << 14;

#[inline]
pub fn limitpos(x: u32) -> u16 {
    if x >= MAXENTRYPOS {
        (MAXENTRYPOS - 1) as u16
    } else {
        x as u16
    }
}

pub struct ParsedWord<'mcx> {
    pub word: PgVec<'mcx, u8>,
    pub nvariant: u16,
    pub flags: u16,
    pub pos: u16,
    pub apos: PgVec<'mcx, u16>,
}

pub struct ParsedText<'mcx> {
    pub words: PgVec<'mcx, ParsedWord<'mcx>>,
    pub pos: u32,
}

impl<'mcx> ParsedText<'mcx> {
    pub fn with_capacity(mcx: Mcx<'mcx>, n: usize) -> PgResult<Self> {
        Ok(ParsedText {
            words: { let mut v = ::mcx::PgVec::new_in(mcx); v.try_reserve_exact(n).map_err(|_| mcx.oom(n))?; v },
            pos: 0,
        })
    }
}

// The ts_cache/wparser surface parsetext consumes; the concrete impl (to_tsany)
// wires the config map + fmgr-resolved parser/dictionary carriers. Token
// coordinates are byte offsets into the parsetext input (the default parser
// yields pointers into it; a token outside the input is a contract violation).
pub trait TsParseEnv<'mcx> {
    fn prs_start(&mut self, buf: &[u8]) -> PgResult<()>;
    fn prs_next(&mut self) -> PgResult<(i32, u32, u32)>;
    fn prs_end(&mut self) -> PgResult<()>;
    fn map_len(&mut self, toktype: i32) -> PgResult<usize>;
    fn map_dict(&mut self, toktype: i32, i: usize) -> PgResult<Oid>;
    fn lexize(
        &mut self,
        dict: Oid,
        token: &[u8],
        state: &mut DictSubState,
    ) -> PgResult<Option<PgVec<'mcx, TsLexeme<'mcx>>>>;
}

// ParsedLex coordinates (byte offsets into the parsetext input).
#[derive(Clone, Copy)]
pub struct ParsePlex {
    pub typ: i32,
    pub off: u32,
    pub len: u32,
}
use ParsePlex as PLex;

pub struct LexizeData<'mcx> {
    cur_dict: Oid,
    pos_dict: usize,
    dict_state: DictSubState,
    queue: PgVec<'mcx, PLex>,
    head: usize,
    cur_sub: usize,
    last_res: usize,
    tmp_res: Option<PgVec<'mcx, TsLexeme<'mcx>>>,
}

impl<'mcx> LexizeData<'mcx> {
    pub(crate) fn new(mcx: Mcx<'mcx>) -> Self {
        LexizeData {
            cur_dict: InvalidOid,
            pos_dict: 0,
            dict_state: DictSubState {
                isend: false,
                getnext: false,
                private_state: core::ptr::null_mut(),
            },
            queue: PgVec::new_in(mcx),
            head: 0,
            cur_sub: 0,
            last_res: 0,
            tmp_res: None,
        }
    }

    fn add_lemm(&mut self, typ: i32, off: u32, len: u32) {
        self.queue.push(PLex { typ, off, len });
        self.cur_sub = self.queue.len() - 1;
    }

    fn remove_head(&mut self) {
        self.head += 1;
        self.pos_dict = 0;
    }

    // moveToWaste(ld, stop): heads through `stop` inclusive leave towork.
    fn move_to_waste(&mut self, stop: usize) {
        self.head = stop + 1;
        self.pos_dict = 0;
        self.cur_sub = stop + 1;
    }

    // Headline consumers read which queue entries a lexize_exec call retired.
    pub(crate) fn head(&self) -> usize {
        self.head
    }

    pub(crate) fn consumed_since(&self, prev_head: usize) -> Vec<PLex> {
        self.queue[prev_head..self.head].to_vec()
    }

    pub(crate) fn add_lemm_pub(&mut self, typ: i32, off: u32, len: u32) {
        self.add_lemm(typ, off, len)
    }

    fn take_tmp(&mut self, lex_at: usize, res: Option<PgVec<'mcx, TsLexeme<'mcx>>>) {
        if let Some(res) = res {
            self.tmp_res = Some(res);
            self.last_res = lex_at;
        }
    }
}

pub(crate) fn lexize_exec<'mcx, E: TsParseEnv<'mcx>>(
    ld: &mut LexizeData<'mcx>,
    env: &mut E,
    buf: &[u8],
) -> PgResult<Option<PgVec<'mcx, TsLexeme<'mcx>>>> {
    'restart: loop {
        if ld.cur_dict == InvalidOid {
            while ld.head < ld.queue.len() {
                let cur_val = ld.queue[ld.head];
                let mut filtered: Option<PgVec<'mcx, u8>> = None;

                if cur_val.typ == 0 || env.map_len(cur_val.typ)? == 0 {
                    ld.remove_head();
                    continue;
                }

                let map_len = env.map_len(cur_val.typ)?;
                let mut i = ld.pos_dict;
                while i < map_len {
                    let dict = env.map_dict(cur_val.typ, i)?;

                    ld.dict_state.isend = false;
                    ld.dict_state.getnext = false;
                    ld.dict_state.private_state = core::ptr::null_mut();
                    let token: &[u8] = match &filtered {
                        Some(f) => f,
                        None => &buf[cur_val.off as usize..(cur_val.off + cur_val.len) as usize],
                    };
                    let res = env.lexize(dict, token, &mut ld.dict_state)?;

                    if ld.dict_state.getnext {
                        ld.cur_dict = dict;
                        ld.pos_dict = i + 1;
                        ld.cur_sub = ld.head + 1;
                        ld.take_tmp(ld.head, res);
                        continue 'restart;
                    }

                    let Some(res) = res else {
                        i += 1;
                        continue;
                    };

                    if res.first().is_some_and(|l| l.flags & ::ts_locale::TSL_FILTER != 0) {
                        let first = res.into_iter().next().expect("TSL_FILTER lexeme");
                        filtered = Some(first.lexeme);
                        i += 1;
                        continue;
                    }

                    ld.remove_head();
                    return Ok(Some(res));
                }

                ld.remove_head();
            }
        } else {
            let dict = ld.cur_dict;

            while ld.cur_sub < ld.queue.len() {
                let cur_val = ld.queue[ld.cur_sub];

                if cur_val.typ != 0 {
                    let map_len = env.map_len(cur_val.typ)?;
                    if map_len == 0 {
                        ld.cur_sub += 1;
                        continue;
                    }
                    let mut dict_exists = false;
                    for i in 0..map_len {
                        if env.map_dict(cur_val.typ, i)? == dict {
                            dict_exists = true;
                            break;
                        }
                    }
                    if !dict_exists {
                        ld.cur_dict = InvalidOid;
                        continue 'restart;
                    }
                }

                ld.dict_state.isend = cur_val.typ == 0;
                ld.dict_state.getnext = false;
                let res = env.lexize(
                    dict,
                    &buf[cur_val.off as usize..(cur_val.off + cur_val.len) as usize],
                    &mut ld.dict_state,
                )?;

                if ld.dict_state.getnext {
                    let at = ld.cur_sub;
                    ld.cur_sub += 1;
                    ld.take_tmp(at, res);
                    continue;
                }

                if res.is_some() || ld.tmp_res.is_some() {
                    let out = match res {
                        Some(res) => {
                            ld.move_to_waste(ld.cur_sub);
                            res
                        }
                        None => {
                            let out = ld.tmp_res.take().expect("tmpRes checked");
                            ld.move_to_waste(ld.last_res);
                            out
                        }
                    };
                    ld.cur_dict = InvalidOid;
                    ld.pos_dict = 0;
                    ld.last_res = 0;
                    ld.tmp_res = None;
                    return Ok(Some(out));
                }

                ld.cur_dict = InvalidOid;
                continue 'restart;
            }
        }

        return Ok(None);
    }
}

pub fn parsetext<'mcx, E: TsParseEnv<'mcx>>(
    mcx: Mcx<'mcx>,
    env: &mut E,
    prs: &mut ParsedText<'mcx>,
    buf: &[u8],
) -> PgResult<()> {
    env.prs_start(buf)?;
    let mut ldata = LexizeData::new(mcx);

    loop {
        let (typ, off, len) = env.prs_next()?;

        if typ > 0 && len as usize >= MAXSTRLEN {
            elog_notice_word_too_long()?;
            continue;
        }

        ldata.add_lemm(typ, off, len);

        while let Some(norms) = lexize_exec(&mut ldata, env, buf)? {
            prs.pos += 1;
            for lex in norms {
                if lex.flags & TSL_ADDPOS != 0 {
                    prs.pos += 1;
                }
                prs.words.push(ParsedWord {
                    nvariant: lex.nvariant,
                    flags: lex.flags & TSL_PREFIX,
                    pos: limitpos(prs.pos),
                    apos: PgVec::new_in(mcx),
                    word: lex.lexeme,
                });
            }
        }

        if typ <= 0 {
            break;
        }
    }

    env.prs_end()
}

#[cold]
pub(crate) fn elog_notice_word_too_long() -> PgResult<()> {
    ::elog_seams::ereport::call(
        PgError::notice("word is too long to be indexed")
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .with_detail(format!(
                "Words longer than {MAXSTRLEN} characters are ignored."
            )),
    )
}
