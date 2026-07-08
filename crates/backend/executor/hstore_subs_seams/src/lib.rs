//! Seams for hstore subscripting execution (contrib/hstore/hstore_subs.c
//! sbs_fetch/sbs_assign bodies), installed by the hstore crate. `key` and
//! `replace` are live text varlena datums; results allocate in `mcx`.

use datum::{Datum, NullableDatum};
use mcx::Mcx;
use types_error::PgResult;

seam_core::seam!(
    /// hstore_subscript_fetch core: fetchval of `key` in the `source` hstore
    /// datum; NULL result for a missing key or NULL value.
    pub fn hstore_subs_fetch<'m>(mcx: Mcx<'m>, source: Datum, key: Datum) -> PgResult<NullableDatum>
);

seam_core::seam!(
    /// hstore_subscript_assign core: merge (key, replace) into `source`
    /// (NULL source builds a one-pair hstore); `replace` None is a NULL value.
    pub fn hstore_subs_assign<'m>(
        mcx: Mcx<'m>,
        source: NullableDatum,
        key: Datum,
        replace: NullableDatum,
    ) -> PgResult<Datum>
);
