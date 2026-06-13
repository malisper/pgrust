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
//! Each non-keystone module carries fixed public signatures with `todo!()`
//! bodies; the keystone is ported. DESIGN HINT (the adt-infra
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
}
