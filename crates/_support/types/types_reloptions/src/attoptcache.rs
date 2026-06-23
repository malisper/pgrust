//! `AttributeOpts` (`utils/attoptcache.h`).

/// Parsed `pg_attribute.attoptions` (`attribute_reloptions`). A negative
/// value means "not set" (the reloptions defaults are `-1`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct AttributeOpts {
    /// `n_distinct` (`float8`).
    pub n_distinct: f64,
    /// `n_distinct_inherited` (`float8`).
    pub n_distinct_inherited: f64,
}
