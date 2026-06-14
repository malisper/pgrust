//! Install this crate's inward seams (`backend-storage-file-fileset-seams`) to
//! the real `fileset.c` functions. Marshal-and-delegate only.
//!
//! The seam contract passes a [`FileSetHandle`](types_execparallel::FileSetHandle),
//! which (per its `types-execparallel` doc) is a real `FileSet *` reinterpreted
//! as `usize` — an inherited-opacity handle, never an invented token. The
//! adapters resolve it back to a borrow of the live `FileSet` body and call the
//! ported function. `buffile.c` (the sole consumer) only ever passes a pointer
//! to a `FileSet` it (or `sharedfileset.c`) keeps alive for the duration of the
//! call, exactly as the C `FileSet *` argument requires.

use backend_storage_file_fileset_seams as seams;
use types_execparallel::FileSetHandle;
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

/// Install every `backend-storage-file-fileset` seam.
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
}
