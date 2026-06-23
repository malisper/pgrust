//! Install this crate's inward seams (`backend-storage-file-fileset-seams`) to
//! the real `fileset.c` functions. Marshal-and-delegate only.
//!
//! The seam contract passes a [`FileSetHandle`](execparallel::FileSetHandle),
//! which (per its `types-execparallel` doc) is a real `FileSet *` reinterpreted
//! as `usize` — an inherited-opacity handle, never an invented token. The
//! adapters resolve it back to a borrow of the live `FileSet` body and call the
//! ported function. `buffile.c` (the sole consumer) only ever passes a pointer
//! to a `FileSet` it (or `sharedfileset.c`) keeps alive for the duration of the
//! call, exactly as the C `FileSet *` argument requires.

use fileset_seams as seams;
use sharedfileset_seams as shared_seams;
use execparallel::FileSetHandle;
use types_storage::fileset::FileSet;

/// Resolve a [`FileSetHandle`] (a `FileSet *` carried as `usize`) to a shared
/// borrow of the live `FileSet`.
///
/// # Safety contract
/// The handle is a genuine pointer to a `FileSet` the caller keeps valid and
/// properly initialized across the call (the C `FileSet *` contract). The cast
/// is the inverse of the pointer-to-`usize` widening `types-execparallel`
/// documents for this handle.
fn as_ref<'a>(handle: FileSetHandle) -> &'a FileSet {
    // SAFETY: handle.0 is a live, aligned `*const FileSet` per the contract above.
    unsafe { &*(handle.0 as *const FileSet) }
}

/// Install every `backend-storage-file-fileset` seam, plus the
/// `backend-storage-file-sharedfileset-seams` set this unit also owns
/// (`sharedfileset.c` is part of this same `backend-storage-file` unit).
///
/// `FileSetInit`/`FileSetDeleteAll` are consumed by `sharedfileset.c` (the
/// sibling owner in this same unit), which calls this crate's public functions
/// directly rather than through a seam, so they are not part of this inward-seam
/// set.
pub fn init_seams() {
    seams::file_set_create::set(|fileset, name| super::FileSetCreate(as_ref(fileset), name));
    seams::file_set_open::set(|fileset, name, mode| {
        super::FileSetOpen(as_ref(fileset), name, mode)
    });
    seams::file_set_delete::set(|fileset, name, error_on_failure| {
        super::FileSetDelete(as_ref(fileset), name, error_on_failure)
    });

    // sharedfileset.c (same unit): the parallel-hash / parallel-tuplesort
    // shared-temp-file protocol. These marshal-and-delegate to the real ported
    // functions in `super::sharedfileset`.
    shared_seams::SharedFileSetInit::set(super::sharedfileset::SharedFileSetInit);
    shared_seams::SharedFileSetAttach::set(super::sharedfileset::SharedFileSetAttach);
    shared_seams::SharedFileSetDeleteAll::set(super::sharedfileset::SharedFileSetDeleteAll);
}
