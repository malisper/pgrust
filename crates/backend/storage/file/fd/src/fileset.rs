// fileset.c + sharedfileset.c, thread-native: the set of named temp files a
// parallel query's participants share. C keys the set to creator_pid + a
// per-backend counter; threads share a pid, so the counter is process-wide.
// Placement: C hashes each segment name across the temp tablespaces; we hash
// the base name once so every segment of a BufFile shares a directory
// (placement is not observable). Deletion: C ties SharedFileSetDeleteAll to
// DSM detach; here the owner (Arc'd parallel node state) drops the FileSet
// when the last participant releases it, which deletes the directories.
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_storage::File;

use crate::temp::{
    GetTempTablespaces, PathNameCreateTemporaryDir, PathNameCreateTemporaryFile,
    PathNameDeleteTemporaryDir, PathNameOpenTemporaryFile, TempTablespacePath,
};

const PG_TEMP_FILE_PREFIX: &str = "pgsql_tmp";

static FILESET_NUMBER: AtomicU64 = AtomicU64::new(0);

pub struct FileSet {
    // C creator_pid; captured ONCE — MyProcPid is per-thread (virtual backend
    // pid) in the thread-native substrate, so lazy resolution would give each
    // participant a different directory.
    creator_pid: i32,
    number: u64,
    tablespaces: Vec<Oid>,
}

impl FileSet {
    /// `FileSetInit`: capture the temp tablespaces to spread files across.
    pub fn init() -> PgResult<FileSet> {
        crate::buffile::PrepareTempTablespaces()?;
        let mut spaces = [Oid::default(); 64];
        let n = GetTempTablespaces(&mut spaces);
        let mut tablespaces: Vec<Oid> = spaces[..n.max(0) as usize].to_vec();
        if tablespaces.is_empty() {
            tablespaces.push(Oid::default());
        }
        Ok(FileSet {
            creator_pid: init_small::globals::MyProcPid(),
            number: FILESET_NUMBER.fetch_add(1, Relaxed),
            tablespaces,
        })
    }

    // `FileSetPath`: <tempdir>/pgsql_tmp<creator_pid>.<set>.fileset
    fn dir_path(&self, tablespace: Oid) -> String {
        format!(
            "{}/{PG_TEMP_FILE_PREFIX}{}.{}.fileset",
            TempTablespacePath(tablespace),
            self.creator_pid,
            self.number
        )
    }

    // `ChooseTablespace`: stable name-keyed spread (C uses hash_bytes; any
    // stable map works).
    fn tablespace_for(&self, name: &str) -> Oid {
        let h = name.bytes().fold(0u64, |a, b| a.wrapping_mul(31).wrapping_add(b as u64));
        self.tablespaces[(h % self.tablespaces.len() as u64) as usize]
    }

    /// Path prefix for a named file; segment i lives at "<prefix>.<i>".
    pub fn name_path(&self, name: &str) -> String {
        format!("{}/{name}", self.dir_path(self.tablespace_for(name)))
    }

    /// `FileSetCreate` for one segment path (from [`FileSet::name_path`]).
    pub fn create_seg(&self, name: &str, path: &str) -> PgResult<File> {
        let file = PathNameCreateTemporaryFile(path, false)?;
        if file.0 > 0 {
            return Ok(file);
        }
        // The directories may not exist yet; create them and retry loudly.
        let tblspc = self.tablespace_for(name);
        PathNameCreateTemporaryDir(&TempTablespacePath(tblspc), &self.dir_path(tblspc))?;
        PathNameCreateTemporaryFile(path, true)
    }

    /// `FileSetOpen`: File(<=0) when the segment doesn't exist.
    pub fn open_seg(&self, path: &str, mode: i32) -> PgResult<File> {
        PathNameOpenTemporaryFile(path, mode)
    }

    /// `FileSetDeleteAll`.
    pub fn delete_all(&self) -> PgResult<()> {
        for &tblspc in &self.tablespaces {
            PathNameDeleteTemporaryDir(&self.dir_path(tblspc))?;
        }
        Ok(())
    }
}

impl Drop for FileSet {
    fn drop(&mut self) {
        let _ = self.delete_all();
    }
}
