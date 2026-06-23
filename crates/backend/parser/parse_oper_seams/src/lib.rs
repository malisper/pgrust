//! Seam declarations for the `backend-parser-parse-oper` unit
//! (`parser/parse_oper.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;
use opclass::ObjectWithArgs;
use parsenodes::ObjectWithArgs as ParseObjectWithArgs;

seam_core::seam!(
    /// `LookupOperWithArgs(oper, noError)` (parse_oper.c): resolve an
    /// `ObjectWithArgs` describing an operator (name + explicit arg types) to
    /// its OID. With `no_error = false` a missing operator raises (`Err`);
    /// with `no_error = true` it returns `InvalidOid`.
    pub fn lookup_oper_with_args(oper: &ObjectWithArgs, no_error: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `LookupOperWithArgs(oper, missing_ok)` (parse_oper.c), taking the
    /// parser's own [`ParseObjectWithArgs`] (the `castNode(ObjectWithArgs,
    /// object)` `get_object_address`'s `OBJECT_OPERATOR` arm passes). Resolves
    /// the operator name + explicit left/right argument types to its
    /// `pg_operator` OID. With `missing_ok = false` a miss raises (`Err`); else
    /// `InvalidOid`.
    pub fn lookup_oper_with_args_node(
        oper: &ParseObjectWithArgs,
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `LookupOperName(NULL, opername, oprleft, oprright, false, -1)`
    /// (parse_oper.c): resolve a binary operator by qualified name and input
    /// types. A missing operator raises (`Err`).
    pub fn lookup_oper_name(
        opername: &[String],
        oprleft: Oid,
        oprright: Oid,
    ) -> PgResult<Oid>
);
