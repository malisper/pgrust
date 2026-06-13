//! Seam declarations for the `backend-utils-adt-ruleutils` unit
//! (`utils/adt/ruleutils.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgString};
use types_error::PgResult;

seam_core::seam!(
    /// `quote_qualified_identifier(qualifier, ident)` (ruleutils.c): each
    /// part quoted with `quote_identifier` if needed, joined with a dot,
    /// allocated in `mcx` (C: palloc in the current context). `Err` is OOM.
    pub fn quote_qualified_identifier<'mcx>(
        mcx: Mcx<'mcx>,
        qualifier: Option<&str>,
        ident: &str,
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// `quote_identifier(ident)` (ruleutils.c): double-quote the identifier
    /// if needed for re-parse safety (non-lowercase letters, keywords, ...).
    /// The result is copied into `mcx` (C pallocs the quoted form in the
    /// current context; the unquoted case returns the input pointer — the
    /// owned image copies either way). `Err` carries OOM.
    pub fn quote_identifier<'mcx>(mcx: Mcx<'mcx>, ident: &str) -> PgResult<PgString<'mcx>>
);

// (quote_identifier is already declared above; postinit reuses it.)

seam_core::seam!(
    /// `generate_operator_clause(buf, leftop, leftoptype, opoid, rightop,
    /// rightoptype)` (ruleutils.c): the schema-qualified, casted
    /// `leftop OPERATOR(...) rightop` fragment `ri_GenerateQual` appends.
    /// `leftop`/`rightop` are raw server-encoded identifier/parameter bytes;
    /// the returned fragment is likewise raw bytes (C operates on `char *`
    /// end-to-end), copied into `mcx`. Catalog lookups can `ereport(ERROR)`.
    pub fn generate_operator_clause<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        leftop: &[u8],
        leftoptype: types_core::Oid,
        opoid: types_core::Oid,
        rightop: &[u8],
        rightoptype: types_core::Oid,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `pg_get_partconstrdef_string(RelationGetRelid(pk_rel), "pk")`
    /// (ruleutils.c): the partition's bound constraint as SQL text, copied
    /// into `mcx`; `Ok(None)` for the empty default-partition constraint. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn partition_constraint_def<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        pk_relid: types_core::Oid,
    ) -> types_error::PgResult<Option<mcx::PgString<'mcx>>>
);
