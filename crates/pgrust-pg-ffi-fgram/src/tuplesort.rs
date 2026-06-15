//! ABI / on-disk / shared-memory vocabulary for the tuplesort subsystem.
//!
//! Mirrors `src/include/utils/tuplesort.h` and the shared-memory `Sharedsort`
//! header from `src/backend/utils/sort/tuplesort.c`.  Only the exact-layout
//! ABI structs live here; the idiomatic working state (`Tuplesortstate`) is
//! defined privately inside the porting crate.

use core::ffi::{c_int, c_void};

use crate::{slock_t, Datum, SharedFileSet};

/* Bitwise option flags for tuple sorts (tuplesort.h). */

/// No options.
pub const TUPLESORT_NONE: c_int = 0;
/// Specifies whether non-sequential access to the sort result is required.
pub const TUPLESORT_RANDOMACCESS: c_int = 1 << 0;
/// Specifies if the tuplesort is able to support bounded sorts.
pub const TUPLESORT_ALLOWBOUNDED: c_int = 1 << 1;

/// Keep the NUM_TUPLESORTMETHODS constant in sync with the number of bits!
pub const NUM_TUPLESORTMETHODS: c_int = 4;

/// Sort algorithm used, reported by `tuplesort_get_stats`.
///
/// The parallel-sort infrastructure relies on having a zero TuplesortMethod to
/// indicate that a worker never did anything, so `SORT_TYPE_STILL_IN_PROGRESS`
/// is zero.  The other values can be OR'ed together to represent a situation
/// where different workers used different methods, so each one has its own bit.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TuplesortMethod {
    SORT_TYPE_STILL_IN_PROGRESS = 0,
    SORT_TYPE_TOP_N_HEAPSORT = 1 << 0,
    SORT_TYPE_QUICKSORT = 1 << 1,
    SORT_TYPE_EXTERNAL_SORT = 1 << 2,
    SORT_TYPE_EXTERNAL_MERGE = 1 << 3,
}

/// Type of space `spaceUsed` represents.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TuplesortSpaceType {
    SORT_SPACE_TYPE_DISK = 0,
    SORT_SPACE_TYPE_MEMORY = 1,
}

/// Data structure for reporting sort statistics.
///
/// Note that this can't contain any pointers because we sometimes put it in
/// shared memory.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TuplesortInstrumentation {
    /// Sort algorithm used.
    pub sortMethod: TuplesortMethod,
    /// Type of space spaceUsed represents.
    pub spaceType: TuplesortSpaceType,
    /// Space consumption, in kB.
    pub spaceUsed: i64,
}

/// The objects we actually sort are SortTuple structs.
///
/// They contain a pointer to the tuple proper (might be a MinimalTuple or
/// IndexTuple), the tuple's first key column in Datum/nullflag format, and a
/// source/input tape number that tracks which tape each element belongs to
/// during merging.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SortTuple {
    /// The tuple itself.
    pub tuple: *mut c_void,
    /// Value of first key column.
    pub datum1: Datum,
    /// Is first key column NULL?
    pub isnull1: bool,
    /// Source tape number.
    pub srctape: c_int,
}

/// One element of the `Sharedsort.tapes` flexible array: a worker reports the
/// first block number of its frozen result tape back to the leader.
// `TapeShare` is the canonical sort/logtape struct defined in `storage`; re-export
// it here (the tuplesort branch had defined an identical copy — deduped on merge).
pub use crate::storage::TapeShare;

/// Tuplesort parallel coordination state, allocated by each participant in
/// local memory.  Participant caller initializes everything.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SortCoordinateData {
    /// Worker process?  If not, must be leader.
    pub isWorker: bool,
    /// Leader-passed number of participants known launched (workers set -1).
    pub nParticipants: c_int,
    /// Private opaque state (points to shared memory).
    pub sharedsort: *mut Sharedsort,
}

pub type SortCoordinate = *mut SortCoordinateData;

/// Private mutable state of a tuplesort-parallel-operation.  This is allocated
/// in shared memory.  The `tapes` flexible array member follows the header.
#[repr(C)]
pub struct Sharedsort {
    /// Mutex protects all fields prior to tapes.
    pub mutex: slock_t,
    /// Generates ordinal identifier numbers for parallel sort workers.
    pub currentWorker: c_int,
    /// Workers increment workersFinished to indicate having finished.
    pub workersFinished: c_int,
    /// Temporary file space.
    pub fileset: SharedFileSet,
    /// Size of tapes flexible array.
    pub nTapes: c_int,
    /// Tapes array used by workers to report info needed by the leader to
    /// concatenate all worker tapes into one for merging.
    pub tapes: [TapeShare; 0],
}

/// `Tuplesortstate` and `LogicalTape` are opaque to callers outside the sort
/// crate; the ABI surface exposes only an opaque marker pointer type.
#[repr(C)]
pub struct Tuplesortstate {
    _private: [u8; 0],
}

/// Opaque logical tape handle (defined by the logtape subsystem).
#[repr(C)]
pub struct LogicalTape {
    _private: [u8; 0],
}

/// Comparator following the qsort_arg convention.
pub type SortTupleComparator = Option<
    unsafe extern "C" fn(
        a: *const SortTuple,
        b: *const SortTuple,
        state: *mut Tuplesortstate,
    ) -> c_int,
>;

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn sort_tuple_matches_generated_layout() {
        assert_eq!(size_of::<SortTuple>(), 24);
        assert_eq!(align_of::<SortTuple>(), 8);
        assert_eq!(offset_of!(SortTuple, tuple), 0);
        assert_eq!(offset_of!(SortTuple, datum1), 8);
        assert_eq!(offset_of!(SortTuple, isnull1), 16);
        assert_eq!(offset_of!(SortTuple, srctape), 20);
    }

    #[test]
    fn tuplesort_instrumentation_matches_generated_layout() {
        assert_eq!(size_of::<TuplesortInstrumentation>(), 16);
        assert_eq!(align_of::<TuplesortInstrumentation>(), 8);
        assert_eq!(offset_of!(TuplesortInstrumentation, sortMethod), 0);
        assert_eq!(offset_of!(TuplesortInstrumentation, spaceType), 4);
        assert_eq!(offset_of!(TuplesortInstrumentation, spaceUsed), 8);
    }

    #[test]
    fn sort_coordinate_data_matches_generated_layout() {
        assert_eq!(size_of::<SortCoordinateData>(), 16);
        assert_eq!(align_of::<SortCoordinateData>(), 8);
        assert_eq!(offset_of!(SortCoordinateData, isWorker), 0);
        assert_eq!(offset_of!(SortCoordinateData, nParticipants), 4);
        assert_eq!(offset_of!(SortCoordinateData, sharedsort), 8);
    }

    #[test]
    fn tape_share_matches_generated_layout() {
        assert_eq!(size_of::<TapeShare>(), 8);
        assert_eq!(align_of::<TapeShare>(), 8);
        assert_eq!(offset_of!(TapeShare, firstblocknumber), 0);
    }

    #[test]
    fn sharedsort_header_matches_generated_layout() {
        // slock_t mutex@0, currentWorker@4, workersFinished@8,
        // SharedFileSet fileset@12 (size 52) -> ends @64, nTapes@64,
        // flexible TapeShare array aligned to 8 -> @72; header size 72.
        assert_eq!(align_of::<Sharedsort>(), 8);
        assert_eq!(offset_of!(Sharedsort, mutex), 0);
        assert_eq!(offset_of!(Sharedsort, currentWorker), 4);
        assert_eq!(offset_of!(Sharedsort, workersFinished), 8);
        assert_eq!(offset_of!(Sharedsort, fileset), 12);
        assert_eq!(offset_of!(Sharedsort, nTapes), 64);
        assert_eq!(offset_of!(Sharedsort, tapes), 72);
        assert_eq!(size_of::<Sharedsort>(), 72);
    }

    #[test]
    fn method_and_space_enum_values() {
        assert_eq!(TuplesortMethod::SORT_TYPE_STILL_IN_PROGRESS as i32, 0);
        assert_eq!(TuplesortMethod::SORT_TYPE_TOP_N_HEAPSORT as i32, 1);
        assert_eq!(TuplesortMethod::SORT_TYPE_QUICKSORT as i32, 2);
        assert_eq!(TuplesortMethod::SORT_TYPE_EXTERNAL_SORT as i32, 4);
        assert_eq!(TuplesortMethod::SORT_TYPE_EXTERNAL_MERGE as i32, 8);
        assert_eq!(TuplesortSpaceType::SORT_SPACE_TYPE_DISK as i32, 0);
        assert_eq!(TuplesortSpaceType::SORT_SPACE_TYPE_MEMORY as i32, 1);
    }
}
