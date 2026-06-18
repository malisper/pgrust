//! Port of the `backend-utils-adt-misc2` catalog unit — the bundle of adt
//! support files: `domains.c`, `expandeddatum.c`, `expandedrecord.c`,
//! `genfile.c`, `hbafuncs.c`, `lockfuncs.c`, `partitionfuncs.c`,
//! `pg_upgrade_support.c`, `regproc.c`, `rowtypes.c`, `tid.c`,
//! `windowfuncs.c` (per CATALOG.tsv / the c2rust run; despite the unit name,
//! it does NOT contain `misc.c`).
//!
//! This unit came back NEEDS_DECOMP and is split into 7 families:
//!
//! * `expandeddatum`  — KEYSTONE. The `ExpandedObjectHeader` ABI every
//!   expanded container embeds; ported in the scaffold phase so the unit (and
//!   external consumer `backend-access-common-heaptuple`) compiles against it.
//!   Installs the two consumed seams `eoh_get_flat_size` / `eoh_flatten_into`.
//! * `expandedrecord` — depends on keystone + `domains`.
//! * `domains`        — domain type I/O + constraint checking.
//! * `rowtypes`       — composite (RECORD) I/O / compare / hash.
//! * `regproc`        — `reg*` alias-type I/O + format_*/name parsing.
//! * `scalars`        — `tid.c` + `windowfuncs.c` scalar/window functions.
//! * `admin`          — `genfile`/`hbafuncs`/`lockfuncs`/`partitionfuncs`/
//!   `pg_upgrade_support` SRF / admin glue.
//!
//! All families are ported with real bodies (no placeholder/unported stubs);
//! the keystone `expandeddatum.c` is complete. DESIGN HINT (the adt-infra
//! SortSupportData / SRF FuncCallContext / pg_prng substrate the window/SRF
//! families build on) is confirmed available on main, so those families seam
//! into the real owners, not stubs. Genuinely-unported owners (hba.c parser,
//! the binary-upgrade catalog state owners) are seam-and-panic, named in the
//! family modules.

#![no_std]
#![allow(clippy::too_many_arguments)]

extern crate alloc;
// The `fmgr_builtins` registration layer (`Datum fn(PG_FUNCTION_ARGS)`) reads /
// writes the fmgr call frame through the `std`-typed `types_fmgr` boundary
// (`String`/`Vec`/`panic_any`), so this crate links `std` even though its
// value-core families are `no_std`/`alloc`.
extern crate std;

pub mod admin;
pub mod domains;
pub mod expandeddatum;
pub mod expandedrecord;
pub mod fmgr_builtins;
pub mod regproc;
pub mod rowtypes;
pub mod scalars;

/// Install this unit's inward seams (the `expandeddatum.c` keystone surface
/// consumed by `backend-access-common-heaptuple`). Wired into
/// `seams-init::init_all()`.
pub fn init_seams() {
    backend_utils_adt_misc2_seams::eoh_get_flat_size::set(expandeddatum::eoh_get_flat_size);
    backend_utils_adt_misc2_seams::eoh_flatten_into::set(expandeddatum::eoh_flatten_into);
    backend_utils_adt_misc2_seams::make_expanded_object_read_only_internal_v::set(
        expandeddatum::make_expanded_object_read_only_internal_v,
    );

    // domains.c: the typcache->domains.c domain-constraint planning seam. The
    // typcache's `load_domaintype_info` plans each CHECK constraint's conbin via
    // `stringToNode` + `expression_planner`; the value-typed body lives here
    // (domains.c is the natural domain-constraint home and can reach the node
    // reader + value-typed planner through their thin seam crates). The other
    // domain-constraint seams (catalog scan, syscache type-level lookup, the
    // "Domain constraints" memory-context lifecycle, and `exec_init_expr`) have
    // distinct real owners (pg_constraint / syscache / mmgr / executor) and are
    // installed by those owners; `exec_init_expr` in particular is still blocked
    // on EState-less `ExecInitExpr` substrate.
    backend_utils_adt_domains_seams::plan_check_expr::set(domains::plan_check_expr);
    // The per-level `SearchSysCache1(TYPEOID)` projection that drives the
    // domain-stack crawl (typtype/typnotnull/typbasetype). Reached through the
    // pg_type syscache seam; no cycle.
    backend_utils_adt_domains_seams::lookup_domain_type_level::set(
        domains::lookup_domain_type_level,
    );

    // regproc.c printable-name / name-parsing helpers (owned seam crate
    // `backend-utils-adt-regproc-seams`). `format_procedure`/`format_operator`
    // match the owner signature exactly; the `reg*in` / name-list seams need a
    // thin shim (scratch context + hard-error/soft folding) below.
    backend_utils_adt_regproc_seams::format_procedure::set(regproc::format_procedure);
    backend_utils_adt_regproc_seams::format_operator::set(regproc::format_operator);
    backend_utils_adt_regproc_seams::format_procedure_extended::set(
        regproc::format_procedure_extended,
    );
    backend_utils_adt_regproc_seams::format_operator_extended::set(
        regproc::format_operator_extended,
    );
    backend_utils_adt_misc2_seams::format_procedure_parts::set(regproc::format_procedure_parts);
    backend_utils_adt_misc2_seams::format_operator_parts::set(regproc::format_operator_parts);
    backend_utils_adt_regproc_seams::regprocedurein::set(seam_regprocedurein);
    backend_utils_adt_regproc_seams::regtypein::set(seam_regtypein);
    backend_utils_adt_regproc_seams::string_to_qualified_name_list::set(
        seam_string_to_qualified_name_list,
    );

    // Register this unit's SQL-callable builtins into the fmgr-core builtin
    // table (C: `fmgr_builtins[]`), so by-OID dispatch resolves them.
    fmgr_builtins::register_misc2_builtins();
}

/// Seam shim: the `regprocedurein(signature)` seam is the hard-error
/// `DirectFunctionCall1` shape (no soft-error context), and folds the owner's
/// `Ok(None)` "unmatched but valid signature" into `InvalidOid`. The owner
/// allocates only transient lookup scratch, so a fresh context suffices.
fn seam_regprocedurein(signature: &str) -> types_error::PgResult<types_core::Oid> {
    let scratch = mcx::MemoryContext::new("regprocedurein seam");
    Ok(regproc::regprocedurein(scratch.mcx(), signature, None)?.unwrap_or(types_core::InvalidOid))
}

/// Seam shim: `regtypein(typename)`, same hard-error / `Ok(None)`->`InvalidOid`
/// folding as [`seam_regprocedurein`].
fn seam_regtypein(typename: &str) -> types_error::PgResult<types_core::Oid> {
    let scratch = mcx::MemoryContext::new("regtypein seam");
    Ok(regproc::regtypein(scratch.mcx(), typename, None)?.unwrap_or(types_core::InvalidOid))
}

/// Seam shim: `stringToQualifiedNameList(string, escontext)`. `soft = true`
/// supplies a soft-error context (C: an `ErrorSaveContext`, mapping bad syntax
/// to `Ok(None)`); `soft = false` passes `None` (hard error). The owner's
/// `Vec<String>` is copied into the caller's `mcx` as `PgVec<PgString>`.
fn seam_string_to_qualified_name_list<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    string: &str,
    soft: bool,
) -> types_error::PgResult<Option<mcx::PgVec<'mcx, mcx::PgString<'mcx>>>> {
    let mut escontext = if soft {
        Some(types_error::SoftErrorContext::new(true))
    } else {
        None
    };
    let parts = regproc::stringToQualifiedNameList(mcx, string, escontext.as_mut())?;
    match parts {
        None => Ok(None),
        Some(names) => {
            let mut out = mcx::vec_with_capacity_in(mcx, names.len())?;
            for n in &names {
                out.push(mcx::PgString::from_str_in(n, mcx)?);
            }
            Ok(Some(out))
        }
    }
}
