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
//! All families are ported with real bodies (no `todo!()`/`unimplemented!()`);
//! the keystone `expandeddatum.c` is complete. DESIGN HINT (the adt-infra
//! SortSupportData / SRF FuncCallContext / pg_prng substrate the window/SRF
//! families build on) is confirmed available on main, so those families seam
//! into the real owners, not stubs. Genuinely-unported owners (hba.c parser,
//! the binary-upgrade catalog state owners) are seam-and-panic, named in the
//! family modules.

#![no_std]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

pub mod admin;
pub mod domains;
pub mod expandeddatum;
pub mod expandedrecord;
pub mod regproc;
pub mod rowtypes;
pub mod scalars;

/// Install this unit's inward seams (the `expandeddatum.c` keystone surface
/// consumed by `backend-access-common-heaptuple`). Wired into
/// `seams-init::init_all()`.
pub fn init_seams() {
    backend_utils_adt_misc2_seams::eoh_get_flat_size::set(expandeddatum::eoh_get_flat_size);
    backend_utils_adt_misc2_seams::eoh_flatten_into::set(expandeddatum::eoh_flatten_into);

    // regproc.c printable-name / name-parsing helpers (owned seam crate
    // `backend-utils-adt-regproc-seams`). `format_procedure`/`format_operator`
    // match the owner signature exactly; the `reg*in` / name-list seams need a
    // thin shim (scratch context + hard-error/soft folding) below.
    backend_utils_adt_regproc_seams::format_procedure::set(regproc::format_procedure);
    backend_utils_adt_regproc_seams::format_operator::set(regproc::format_operator);
    backend_utils_adt_regproc_seams::regprocedurein::set(seam_regprocedurein);
    backend_utils_adt_regproc_seams::regtypein::set(seam_regtypein);
    backend_utils_adt_regproc_seams::string_to_qualified_name_list::set(
        seam_string_to_qualified_name_list,
    );
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
