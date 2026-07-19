use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::ts_locale::{DictSubState, TsLexeme};
use ::types_core::primitive::InvalidOid;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    function_call1_coll, function_call2_coll_in, function_call3_coll, function_call4_coll_in,
    FmgrInfo,
};

use crate::cache_bind::{self, ConfigMap, ParserFns};

pub struct CacheEnv<'mcx> {
    mcx: Mcx<'mcx>,
    cfg: Oid,
    // C looks the configuration up inside parsetext/hlparsetext (ts_parse.c),
    // NOT at the SQL entry point: to_tsquery(0::regconfig, '') never pushes a
    // value, so the bogus config is never resolved and the call succeeds with
    // an empty result + NOTICE (fnconf batch-1 ts-config-laziness family).
    // Resolved on first parser use (prs_start = C's parsetext entry).
    loaded: Option<(ConfigMap<'mcx>, ParserFns)>,
    prsdata: Datum,
    buf: (*const u8, usize),
    dicts: PgVec<'mcx, (Oid, Datum, FmgrInfo)>,
}

impl<'mcx> CacheEnv<'mcx> {
    pub fn new(mcx: Mcx<'mcx>, cfg: Oid) -> Self {
        CacheEnv {
            mcx,
            cfg,
            loaded: None,
            prsdata: Datum::null(),
            buf: (core::ptr::null(), 0),
            dicts: PgVec::new_in(mcx),
        }
    }

    // C: parsetext's lookup_ts_config_cache(cfgId) + lookup_ts_parser_cache.
    fn ensure(&mut self) -> PgResult<&mut (ConfigMap<'mcx>, ParserFns)> {
        if self.loaded.is_none() {
            let map = cache_bind::config_map(self.mcx, self.cfg)?;
            let prs = cache_bind::parser_fns(map.prs_id)?;
            self.loaded = Some((map, prs));
        }
        Ok(self.loaded.as_mut().expect("just resolved"))
    }

    fn dict_at(&mut self, dict: Oid) -> PgResult<usize> {
        if let Some(i) = self.dicts.iter().position(|d| d.0 == dict) {
            return Ok(i);
        }
        let (data, lexize) = cache_bind::dict_carrier(dict)?;
        self.dicts.push((dict, data, lexize));
        Ok(self.dicts.len() - 1)
    }
}

impl<'mcx> ::ts_parse::TsParseEnv<'mcx> for CacheEnv<'mcx> {
    fn prs_start(&mut self, buf: &[u8]) -> PgResult<()> {
        self.buf = (buf.as_ptr(), buf.len());
        let mcx = self.mcx;
        let prsdata = {
            let (_, prs) = self.ensure()?;
            function_call2_coll_in(
                &mut prs.start,
                InvalidOid,
                mcx,
                Datum::from_usize(buf.as_ptr() as usize),
                Datum::from_i32(buf.len() as i32),
            )?
        };
        self.prsdata = prsdata;
        Ok(())
    }

    fn prs_next(&mut self) -> PgResult<(i32, u32, u32)> {
        let mut lemm: *const u8 = core::ptr::null();
        let mut lenlemm: i32 = 0;
        let prsdata = self.prsdata;
        let typ = {
            let (_, prs) = self.ensure()?;
            function_call3_coll(
                &mut prs.token,
                InvalidOid,
                prsdata,
                Datum::from_usize(core::ptr::from_mut(&mut lemm) as usize),
                Datum::from_usize(core::ptr::from_mut(&mut lenlemm) as usize),
            )?
            .as_i32()
        };
        if typ <= 0 {
            return Ok((typ, 0, 0));
        }
        let (base, blen) = self.buf;
        let off = (lemm as usize).wrapping_sub(base as usize);
        if off > blen || off + lenlemm.max(0) as usize > blen {
            panic!("to_tsany: parser token escapes the input buffer (off {off}, len {lenlemm})");
        }
        Ok((typ, off as u32, lenlemm.max(0) as u32))
    }

    fn prs_end(&mut self) -> PgResult<()> {
        let prsdata = self.prsdata;
        let (_, prs) = self.ensure()?;
        function_call1_coll(&mut prs.end, InvalidOid, prsdata).map(|_| ())
    }

    fn map_len(&mut self, toktype: i32) -> PgResult<usize> {
        if toktype <= 0 {
            return Ok(0);
        }
        let (map, _) = self.ensure()?;
        Ok(map.map.get(toktype as usize).map_or(0, |dicts| dicts.len()))
    }

    fn map_dict(&mut self, toktype: i32, i: usize) -> PgResult<Oid> {
        let (map, _) = self.ensure()?;
        Ok(map.map[toktype as usize][i])
    }

    fn lexize(
        &mut self,
        dict: Oid,
        token: &[u8],
        state: &mut DictSubState,
    ) -> PgResult<Option<PgVec<'mcx, TsLexeme<'mcx>>>> {
        let i = self.dict_at(dict)?;
        let mcx = self.mcx;
        let (_, data, fi) = &mut self.dicts[i];
        let d = function_call4_coll_in(
            fi,
            InvalidOid,
            mcx,
            *data,
            Datum::from_usize(token.as_ptr() as usize),
            Datum::from_i32(token.len() as i32),
            Datum::from_usize(core::ptr::from_mut(state) as usize),
        )?;
        if d.as_usize() == 0 {
            return Ok(None);
        }
        let ptr = d.as_usize() as *mut ::ts_locale::dict_api::LexizeResult<'mcx>;
        // SAFETY: dict_api contract — the lexize builtin allocated a
        // LexizeResult in the armed mcx (`self.mcx`); ownership moves out
        // exactly once, the shell stays behind in the arena.
        let res = unsafe { core::ptr::read(ptr) };
        Ok(Some(res.0))
    }
}
