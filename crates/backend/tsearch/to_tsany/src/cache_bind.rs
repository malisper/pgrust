use ::mcx::{Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::FmgrInfo;

pub struct ParserFns {
    pub start: FmgrInfo,
    pub token: FmgrInfo,
    pub end: FmgrInfo,
}

// Index = parser token type; empty list = unmapped type (skip).
pub struct ConfigMap<'mcx> {
    pub prs_id: Oid,
    pub map: PgVec<'mcx, PgVec<'mcx, Oid>>,
}

pub fn config_map<'mcx>(mcx: Mcx<'mcx>, cfg: Oid) -> PgResult<ConfigMap<'mcx>> {
    let entry = ::ts_cache::lookup_ts_config_cache(cfg)?;
    let mut map: PgVec<'mcx, PgVec<'mcx, Oid>> = PgVec::new_in(mcx);
    map.try_reserve_exact(entry.map.len())
        .map_err(|_| mcx.oom(entry.map.len()))?;
    for ld in entry.map.iter() {
        let mut dicts: PgVec<'mcx, Oid> = ::mcx::vec_with_capacity_in(mcx, ld.dict_ids.len())?;
        for &d in ld.dict_ids.iter() {
            dicts.push(d);
        }
        map.push(dicts);
    }
    Ok(ConfigMap { prs_id: entry.prs_id, map })
}

pub fn parser_fns(prs_oid: Oid) -> PgResult<ParserFns> {
    let entry = ::ts_cache::lookup_ts_parser_cache(prs_oid)?;
    Ok(ParserFns {
        start: fmgr_seams::fmgr_info::call(entry.start_oid)?,
        token: fmgr_seams::fmgr_info::call(entry.token_oid)?,
        end: fmgr_seams::fmgr_info::call(entry.end_oid)?,
    })
}

pub fn dict_carrier(dict: Oid) -> PgResult<(::datum::Datum, FmgrInfo)> {
    let entry = ::ts_cache::lookup_ts_dictionary_cache(dict)?;
    Ok((
        ::datum::Datum::from_usize(entry.dict_data),
        fmgr_seams::fmgr_info::call(entry.lexize_oid)?,
    ))
}

pub fn current_config() -> PgResult<Oid> {
    ::ts_cache::getTSCurrentConfig(true)
}
