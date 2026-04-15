use std::collections::{BTreeMap, HashMap};

use crate::backend::storage::smgr::{
    AnyStorageManager, MdStorageManager, SmgrError, StorageManager,
};
use crate::include::storage::buf_internals::*;

pub trait StorageBackend {
    fn read_page(&mut self, tag: BufferTag) -> Result<Page, String>;
    /// Write a page to stable storage.
    ///
    /// `skip_fsync`: when `true`, the write may be left in the OS page cache
    /// without an explicit fsync. Safe only when WAL is durable for this page
    /// — the WAL can be replayed to recover the page after a crash.
    fn write_page(&mut self, tag: BufferTag, page: &Page, skip_fsync: bool) -> Result<(), String>;
}

#[derive(Debug, Default, Clone)]
pub struct FakeStorage {
    pages: BTreeMap<BufferTag, Page>,
    fail_reads: HashMap<BufferTag, String>,
    fail_writes: HashMap<BufferTag, String>,
}

impl FakeStorage {
    pub fn put_page(&mut self, tag: BufferTag, page: Page) {
        self.pages.insert(tag, page);
    }

    pub fn get_page(&self, tag: BufferTag) -> Option<Page> {
        self.pages.get(&tag).copied()
    }

    pub fn fail_next_read(&mut self, tag: BufferTag, message: impl Into<String>) {
        self.fail_reads.insert(tag, message.into());
    }

    pub fn fail_next_write(&mut self, tag: BufferTag, message: impl Into<String>) {
        self.fail_writes.insert(tag, message.into());
    }
}

impl StorageBackend for FakeStorage {
    fn read_page(&mut self, tag: BufferTag) -> Result<Page, String> {
        if let Some(err) = self.fail_reads.get(&tag) {
            return Err(err.clone());
        }
        Ok(self.pages.get(&tag).copied().unwrap_or([0; PAGE_SIZE]))
    }

    fn write_page(&mut self, tag: BufferTag, page: &Page, _skip_fsync: bool) -> Result<(), String> {
        if let Some(err) = self.fail_writes.remove(&tag) {
            return Err(err);
        }
        self.pages.insert(tag, *page);
        Ok(())
    }
}

/// Adapts `MdStorageManager` to the `StorageBackend` interface expected by
/// `BufferPool`.
pub struct SmgrStorageBackend {
    pub smgr: AnyStorageManager,
}

impl SmgrStorageBackend {
    pub fn new(smgr: MdStorageManager) -> Self {
        Self {
            smgr: AnyStorageManager::Md(smgr),
        }
    }

    pub fn new_mem() -> Self {
        Self {
            smgr: AnyStorageManager::mem(),
        }
    }
}

impl StorageBackend for SmgrStorageBackend {
    fn read_page(&mut self, tag: BufferTag) -> Result<Page, String> {
        let mut buf = [0u8; PAGE_SIZE];
        self.smgr
            .read_block(tag.rel, tag.fork, tag.block, &mut buf)
            .map_err(|e: SmgrError| e.to_string())?;
        Ok(buf)
    }

    fn write_page(&mut self, tag: BufferTag, page: &Page, skip_fsync: bool) -> Result<(), String> {
        self.smgr
            .write_block(tag.rel, tag.fork, tag.block, page, skip_fsync)
            .map_err(|e: SmgrError| e.to_string())
    }
}
