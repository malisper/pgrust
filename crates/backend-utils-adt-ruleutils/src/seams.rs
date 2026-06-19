//! Install this crate's inward seams (declared in
//! `backend-utils-adt-ruleutils-seams`) and the ruleutils-owned GUC variable
//! accessors.
//!
//! F0a (the deparse name-resolution engine) installs exactly the one seam it
//! can faithfully provide from the engine alone: `select_rtable_names_for_explain`,
//! the EXPLAIN frontend to `set_rtable_names`. The other declared ruleutils
//! seams (the expression deparser, the catalog def-builders, the plan-tree
//! deparse context) belong to later families (F1/F2/F3-cat / F0b) and stay
//! uninstalled (mirror-PG-and-panic) until those land.
//!
//! `ruleutils.c` is also the defining module of the `quote_all_identifiers`
//! GUC (`bool quote_all_identifiers = false;`, registered in `guc_tables.c`
//! pointing at this file's global). The GUC machinery reaches that backend-local
//! variable through the slot accessors installed here, and `quote_identifier`
//! reads the very same store — exactly as C reads the global directly.

// The C global `bool quote_all_identifiers` is a per-backend GUC variable
// (PGC_USERSET). Mirror it with a backend-local `thread_local` `Cell`, exposed
// to the GUC machinery through the accessors installed below; this is the Rust
// home for the C file-scope global the GUC slot's `variable` pointer targets.
extern crate std;
use core::cell::Cell;
use std::thread_local;

thread_local! {
    /// `bool quote_all_identifiers = false;` (ruleutils.c).
    static QUOTE_ALL_IDENTIFIERS: Cell<bool> = const { Cell::new(false) };
}

/// Read `quote_all_identifiers` (`*conf->variable`).
#[inline]
pub fn quote_all_identifiers() -> bool {
    QUOTE_ALL_IDENTIFIERS.with(Cell::get)
}

#[inline]
fn set_quote_all_identifiers(value: bool) {
    QUOTE_ALL_IDENTIFIERS.with(|c| c.set(value));
}

pub fn init_seams() {
    use backend_utils_misc_guc_tables::{vars, GucVarAccessors};

    backend_utils_adt_ruleutils_seams::select_rtable_names_for_explain::set(
        crate::select_rtable_names_for_explain,
    );

    // The identifier-quoting helpers are now ported in this crate (lib.rs):
    // `quote.c`'s `quote_ident` and `format_type`'s qualified-name builder reach
    // them through these ruleutils-owned seams.
    // get_range_partbound_string (ruleutils.c 13676): render one range
    // partition bound to a SQL string. Consumed by partbounds'
    // check_new_partition_bound for the empty-range errdetail.
    backend_partitioning_partbounds_seams::get_range_partbound_string::set(
        crate::expr_deparse::get_range_partbound_string,
    );

    backend_utils_adt_ruleutils_seams::quote_identifier::set(crate::quote_identifier);
    backend_utils_adt_ruleutils_seams::quote_qualified_identifier::set(
        crate::quote_qualified_identifier,
    );

    // EXPLAIN's plan-tree expression deparse (`show_expression`): build the
    // deparse context for the plan tree, point it at the node, and render the
    // expression. Folds `set_deparse_context_plan` + `deparse_expression` (both
    // ruleutils-private) so the `deparse_namespace` never leaves this crate.
    backend_utils_adt_ruleutils_seams::deparse_expr_for_plan::set(
        crate::deparse_expr_for_plan,
    );

    // The catalog half of `generate_relation_name` (the CTE-conflict scan is
    // done in-crate by the deparser). Reads relname/relnamespace + visibility,
    // qualifies, and quotes — all owners (lsyscache/namespace) are installed.
    backend_utils_adt_ruleutils_seams::generate_relation_name::set(
        crate::generate_relation_name_catalog,
    );

    // generate_operator_clause(buf, leftop, leftoptype, opoid, rightop,
    // rightoptype) — the schema-qualified casted operator fragment ri_triggers.c
    // appends when building the FK enforcement query.
    backend_utils_adt_ruleutils_seams::generate_operator_clause::set(
        crate::generate_operator_clause_catalog,
    );

    // guc_funcs.c's GUC_LIST_QUOTE flatten branch (flatten_set_variable_args)
    // reaches `quote_identifier` through its own outward seam crate. C:
    // `char *quote_identifier(const char *)`; the owner is Mcx-bound (the result
    // palloc), so run it in a scratch context and hand back an owned String.
    backend_utils_misc_guc_funcs_seams::quote_identifier::set(|val| {
        let scratch = mcx::MemoryContext::new("guc_funcs quote_identifier seam");
        crate::quote_identifier(scratch.mcx(), &val)
            .map(|s| alloc::string::String::from(s.as_str()))
            .expect("quote_identifier failed")
    });

    // typecmds.c (ALTER DOMAIN constraint storage) deparses the cooked
    // constraint with `deparse_expression(expr, NIL, false, false)`.
    backend_commands_typecmds_seams::deparse_expression::set(|mcx, expr| {
        crate::deparse_expression(mcx, &expr, mcx::PgVec::new_in(mcx), false, false)
    });

    // Register the SQL-callable deparser builtins (C: their `fmgr_builtins[]`
    // rows) so by-OID fmgr dispatch resolves them.
    crate::register_ruleutils_builtins();

    // Install the `quote_all_identifiers` GUC slot's variable accessors so the
    // GUC machinery can read/write the backend-local store above. Guarded so a
    // re-run (or a future second installer) does not panic on double-install.
    if !vars::quote_all_identifiers.installed() {
        vars::quote_all_identifiers.install(GucVarAccessors {
            get: quote_all_identifiers,
            set: set_quote_all_identifiers,
        });
    }
}
