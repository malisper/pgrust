//! Seam declarations for the `backend-storage-page` unit (`bufpage.c`): the
//! process-/session-local GUC variables it *defines* (the C `bool` globals
//! declared in `bufpage.c`) and that consumers in other units read across a
//! dependency cycle.
//!
//! The owning unit (`backend-storage-page`) installs these from its
//! `init_seams()`; until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `ignore_checksum_failure` (bufpage.c, declared at line 27, registered as
    /// a `PGC_SUSET` developer-options GUC in guc_tables.c) — when `true`, a
    /// data-page checksum mismatch is downgraded from an error to a warning and
    /// processing continues. It is a plain session-local backend `bool` read
    /// directly from the GUC slot (it is *not* persisted in the control file,
    /// unlike `data_checksums_enabled`). `bufmgr.c`'s
    /// `StartReadBuffersImpl` reads this backend's value to promote it to the
    /// `READ_BUFFERS_IGNORE_CHECKSUM_FAILURES` flag.
    pub fn ignore_checksum_failure() -> bool
);
