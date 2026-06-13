//! Seam declarations for the `backend-utils-adt-numeric` unit
//! (`utils/adt/numeric.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `DatumGetBool(DirectFunctionCall2(numeric_eq, a, b))` over two on-disk
    /// `numeric` varlenas — value equality (scale-insensitive). `a`/`b` are
    /// the whole on-disk `numeric` images (varlena header included). Reached
    /// from `jsonb_util.c`'s `equalsJsonbScalarValue` `jbvNumeric` arm. Pure
    /// computation; infallible.
    pub fn numeric_eq(a: &[u8], b: &[u8]) -> bool
);

seam_core::seam!(
    /// `DatumGetInt32(DirectFunctionCall2(numeric_cmp, a, b))` over two on-disk
    /// `numeric` varlenas — the 3-way B-tree comparison (`-1`/`0`/`1`, with
    /// full special-value ordering). Reached from `jsonb_util.c`'s
    /// `compareJsonbScalarValue` `jbvNumeric` arm. Pure computation;
    /// infallible.
    pub fn numeric_cmp(a: &[u8], b: &[u8]) -> i32
);

seam_core::seam!(
    /// `numeric_maximum_size(typmod)` (numeric.c): the maximum on-disk size of
    /// a `numeric` value with the given typmod, or -1 if indeterminate. Pure
    /// arithmetic on the typmod-encoded precision/scale; no allocation, no
    /// error path.
    pub fn numeric_maximum_size(typmod: i32) -> i32
);

seam_core::seam!(
    /// `DatumGetFloat8(DirectFunctionCall1(numeric_float8,
    /// DirectFunctionCall2(numeric_sub, v1, v2)))` — the `numrange_subdiff`
    /// body (rangetypes.c:1703): the `numeric` subtype distance `v1 - v2` as a
    /// `float8`. `v1` / `v2` are `numeric` `Datum`s. `Err` carries the
    /// `numeric_sub` / `numeric_float8` `ereport(ERROR)`s (e.g. overflow).
    pub fn numeric_subdiff(
        v1: types_datum::Datum,
        v2: types_datum::Datum,
    ) -> types_error::PgResult<f64>
);

seam_core::seam!(
    /// `addHyperLogLog(&nss->abbr_card, DatumGetUInt32(hash_uint32(tmp)))`
    /// (numeric.c:5173, the `estimating` branch of `numeric_abbrev_convert_var`).
    /// `tmp` is the folded 32-bit abbreviation (`(uint32) result ^ (uint32)
    /// ((uint64) result >> 32)`), already computed in the numeric unit; the
    /// `hash_uint32` mix + HyperLogLog accumulation against the sort's
    /// `hyperLogLogState` (attached to `SortSupport.ssup_extra` by the
    /// genuinely-unported `numeric_sortsupport` setup) live behind this seam.
    /// Pure side-effect on the HLL counter; never ereports. OUTBOUND: this unit
    /// only calls it; the sort-support/HLL owner installs it.
    pub fn numeric_abbrev_add_sample(tmp: u32)
);

seam_core::seam!(
    /// `estimateHyperLogLog(&nss->abbr_card)` (numeric.c:2241, the body of
    /// `numeric_abbrev_abort`). Reads the running cardinality estimate off the
    /// sort's `hyperLogLogState` (attached to `SortSupport.ssup_extra` by the
    /// genuinely-unported `numeric_sortsupport` setup). The abort *decision*
    /// (the 10k/100k thresholds + `estimating` toggle) lives in the numeric
    /// unit; only the HLL counter read is behind this seam. Pure read; never
    /// ereports. OUTBOUND: this unit only calls it; the sort-support/HLL owner
    /// installs it.
    pub fn numeric_abbrev_estimate() -> f64
);
