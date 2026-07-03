use ::mcx::{Mcx, PgVec};

// ts_public.h TSLexeme: the cross-dictionary lexize result element. `lexeme`
// bytes live in the caller-supplied mcx; a dictionary result is a
// PgVec<TsLexeme> (no NULL terminator — len replaces it).
pub struct TsLexeme<'mcx> {
    pub nvariant: u16,
    pub flags: u16,
    pub lexeme: PgVec<'mcx, u8>,
}

pub const TSL_ADDPOS: u16 = 0x01;
pub const TSL_PREFIX: u16 = 0x02;
pub const TSL_FILTER: u16 = 0x04;

// ts_public.h DictSubState, passed by pointer through the lexize call.
pub struct DictSubState {
    pub isend: bool,
    pub getnext: bool,
    pub private_state: *mut core::ffi::c_void,
}

pub struct StopList<'mcx> {
    pub stop: PgVec<'mcx, PgVec<'mcx, u8>>,
}

pub fn t_isalpha(_s: &[u8]) -> bool {
    unimplemented!("ts_locale::t_isalpha (tsearch-lane porter B)")
}

pub fn t_isalnum(_s: &[u8]) -> bool {
    unimplemented!("ts_locale::t_isalnum (tsearch-lane porter B)")
}

pub fn lowerstr<'mcx>(_mcx: Mcx<'mcx>, _s: &[u8]) -> ::types_error::PgResult<PgVec<'mcx, u8>> {
    unimplemented!("ts_locale::lowerstr (tsearch-lane porter B)")
}

pub fn get_tsearch_config_filename<'mcx>(
    _mcx: Mcx<'mcx>,
    _basename: &[u8],
    _extension: &str,
) -> ::types_error::PgResult<PgVec<'mcx, u8>> {
    unimplemented!("ts_locale::get_tsearch_config_filename (tsearch-lane porter B)")
}

pub fn tsearch_readlines<'mcx>(
    _mcx: Mcx<'mcx>,
    _filename: &[u8],
) -> ::types_error::PgResult<Option<PgVec<'mcx, PgVec<'mcx, u8>>>> {
    unimplemented!("ts_locale::tsearch_readlines (tsearch-lane porter B)")
}

pub fn readstoplist<'mcx>(
    _mcx: Mcx<'mcx>,
    _fname: Option<&[u8]>,
    _lower: bool,
) -> ::types_error::PgResult<StopList<'mcx>> {
    unimplemented!("ts_locale::readstoplist (tsearch-lane porter B)")
}

pub fn searchstoplist(_s: &StopList<'_>, _key: &[u8]) -> bool {
    unimplemented!("ts_locale::searchstoplist (tsearch-lane porter B)")
}
