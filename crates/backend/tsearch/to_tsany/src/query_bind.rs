use ::adt_tsquery_core::parse::{parse_tsquery, QueryParser};
use ::datum::{Datum, Varlena};
use ::mcx::{Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::types_fmgr::varlena_result;

use crate::env::CacheEnv;
use crate::query::{pushval_morph, QPush};

pub fn morph_to_tsquery<'mcx>(
    mcx: Mcx<'mcx>,
    cfg: Oid,
    input: &[u8],
    flags: i32,
    qoperator: i8,
) -> PgResult<Datum> {
    // C (to_tsany.c to_tsquery_byid + ts_parse.c): the config is looked up
    // per pushed value inside pushval_morph→parsetext, so a query with no
    // lexemes (e.g. '') never validates the config — lazy CacheEnv mirrors
    // that (fnconf batch-1: to_tsquery(0::regconfig, '') must return empty).
    let mut env = CacheEnv::new(mcx, cfg);

    let mut pushval = |p: &mut QueryParser<'_, '_, 'mcx>,
                       val: &[u8],
                       weight: i16,
                       prefix: bool|
     -> PgResult<()> {
        let mut acts: PgVec<'mcx, QPush<'mcx>> = PgVec::new_in(mcx);
        pushval_morph(mcx, &mut env, val, weight, prefix, qoperator, &mut acts)?;
        for a in acts {
            match a {
                QPush::Value { word, weight, prefix } => p.push_value(&word, weight, prefix)?,
                QPush::Op { oper, distance } => p.push_operator(oper, distance),
                QPush::Stop => p.push_stop(),
            }
        }
        Ok(())
    };

    let parsed = parse_tsquery(mcx, input, flags, None, &mut pushval)?
        .ok_or_else(|| PgError::error("parse_tsquery soft-failed without a soft context"))?;
    Ok(varlena_result(Varlena::from_image(parsed.img)))
}
