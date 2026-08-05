use crate::storage::Spinlock;
use ::types_core::{uint32, Oid};

pub const FILESET_MAX_TABLESPACES: usize = 8;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct FileSet {
    pub creator_pid: i32,
    pub number: uint32,
    pub ntablespaces: i32,
    pub tablespaces: [Oid; FILESET_MAX_TABLESPACES],
}

// DSM-embedded (e.g. ParallelHashJoinState) with a live spinlock — never
// Copy/Clone.
#[derive(Debug)]
#[repr(C)]
pub struct SharedFileSet {
    pub fs: FileSet,
    pub mutex: Spinlock,
    pub refcnt: i32,
}

const _: () = assert!(core::mem::size_of::<FileSet>() == 44);
