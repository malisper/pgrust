//! Seam declarations for the `backend-archive-shell-archive` unit
//! (`archive/shell_archive.c`).
//!
//! `shell_archive_init()` returns the built-in shell-archiving callback table.
//! The archiver (`pgarch.c`) selects it when `archive_library` is empty and
//! `archive_command` is set; otherwise it dynamically loads an external
//! archive library's `_PG_archive_module_init`. The owning unit installs this
//! from its `init_seams()` when it lands; until then a call panics loudly,
//! which is the correct behavior for an archiver started before its module
//! provider exists.

seam_core::seam!(
    /// `shell_archive_init(void)` (`archive/shell_archive.c`): return the
    /// built-in shell-archiving callback table. The table is a `'static`
    /// constant in shell_archive.c, so the seam hands back a shared static
    /// reference. Infallible in C.
    pub fn shell_archive_init() -> &'static types_pgarch::ArchiveModuleCallbacks
);
