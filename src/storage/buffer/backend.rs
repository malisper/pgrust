use std::collections::{BTreeMap, HashMap};

use super::types::*;
use crate::storage::smgr::{MdStorageManager, SmgrError, StorageManager};

pub trait StorageBackend {
    fn read_page(&mut self, tag: BufferTag) -> Result<Page, String>;
    fn write_page(&mut self, tag: BufferTag, page: &Page) -> Result<(), String>;
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

    fn write_page(&mut self, tag: BufferTag, page: &Page) -> Result<(), String> {
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
    pub smgr: MdStorageManager,
}

impl SmgrStorageBackend {
    pub fn new(smgr: MdStorageManager) -> Self {
        Self { smgr }
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

    fn write_page(&mut self, tag: BufferTag, page: &Page) -> Result<(), String> {
        self.smgr
            .write_block(tag.rel, tag.fork, tag.block, page, false)
            .map_err(|e: SmgrError| e.to_string())
    }
}
