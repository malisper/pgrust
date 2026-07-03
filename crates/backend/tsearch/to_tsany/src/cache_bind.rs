use ::mcx::{Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::FmgrInfo;

// The ts_cache touchpoints, isolated: bodies bind to the ts_cache crate at
// integration; unbound = loud panic (never a silent stub).

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

pub fn config_map<'mcx>(_mcx: Mcx<'mcx>, _cfg: Oid) -> PgResult<ConfigMap<'mcx>> {
    unimplemented!("to_tsany::cache_bind::config_map -> ts_cache::lookup_ts_config_cache (tsearch-lane integration)")
}

pub fn parser_fns(_prs_oid: Oid) -> PgResult<ParserFns> {
    unimplemented!("to_tsany::cache_bind::parser_fns -> ts_cache::lookup_ts_parser_cache (tsearch-lane integration)")
}

pub fn dict_carrier(_dict: Oid) -> PgResult<(::datum::Datum, FmgrInfo)> {
    unimplemented!("to_tsany::cache_bind::dict_carrier -> ts_cache::lookup_ts_dictionary_cache (tsearch-lane integration)")
}

pub fn current_config() -> PgResult<Oid> {
    unimplemented!("to_tsany::cache_bind::current_config -> ts_cache::getTSCurrentConfig (tsearch-lane integration)")
}
