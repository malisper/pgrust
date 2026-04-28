use super::*;

use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Default, Clone)]
pub struct MemStorageManager {
    opened_rels: BTreeSet<RelFileLocator>,
    forks: BTreeMap<(RelFileLocator, ForkNumber), Vec<[u8; BLCKSZ]>>,
}

impl MemStorageManager {
    pub fn new() -> Self {
        Self::default()
    }

    fn fork_pages_mut(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
    ) -> Result<&mut Vec<[u8; BLCKSZ]>, SmgrError> {
        self.forks
            .get_mut(&(rel, fork))
            .ok_or(SmgrError::RelationNotFound { rel, fork })
    }

    fn fork_pages(
        &self,
        rel: RelFileLocator,
        fork: ForkNumber,
    ) -> Result<&Vec<[u8; BLCKSZ]>, SmgrError> {
        self.forks
            .get(&(rel, fork))
            .ok_or(SmgrError::RelationNotFound { rel, fork })
    }
}

impl StorageManager for MemStorageManager {
    fn open(&mut self, rel: RelFileLocator) -> Result<(), SmgrError> {
        self.opened_rels.insert(rel);
        Ok(())
    }

    fn close(&mut self, _rel: RelFileLocator, _fork: ForkNumber) -> Result<(), SmgrError> {
        Ok(())
    }

    fn create(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        _is_redo: bool,
    ) -> Result<(), SmgrError> {
        self.opened_rels.insert(rel);
        self.forks.entry((rel, fork)).or_default();
        Ok(())
    }

    fn exists(&mut self, rel: RelFileLocator, fork: ForkNumber) -> bool {
        self.forks.contains_key(&(rel, fork))
    }

    fn unlink(&mut self, rel: RelFileLocator, fork: Option<ForkNumber>, _is_redo: bool) {
        match fork {
            Some(fork) => {
                self.forks.remove(&(rel, fork));
            }
            None => {
                self.forks.retain(|(entry_rel, _), _| *entry_rel != rel);
                self.opened_rels.remove(&rel);
            }
        }
    }

    fn read_block(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        buf: &mut [u8],
    ) -> Result<(), SmgrError> {
        if buf.len() != BLCKSZ {
            return Err(SmgrError::BadBufferSize { size: buf.len() });
        }
        let pages = self.fork_pages(rel, fork)?;
        let page = pages
            .get(block as usize)
            .ok_or(SmgrError::BlockOutOfRange { rel, fork, block })?;
        buf.copy_from_slice(page);
        Ok(())
    }

    fn write_block(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        data: &[u8],
        _skip_fsync: bool,
    ) -> Result<(), SmgrError> {
        if data.len() != BLCKSZ {
            return Err(SmgrError::BadBufferSize { size: data.len() });
        }
        let pages = self.fork_pages_mut(rel, fork)?;
        let page = pages
            .get_mut(block as usize)
            .ok_or(SmgrError::BlockOutOfRange { rel, fork, block })?;
        page.copy_from_slice(data);
        Ok(())
    }

    fn writeback(
        &mut self,
        _rel: RelFileLocator,
        _fork: ForkNumber,
        _block: BlockNumber,
        _nblocks: u32,
    ) -> Result<(), SmgrError> {
        Ok(())
    }

    fn prefetch(
        &mut self,
        _rel: RelFileLocator,
        _fork: ForkNumber,
        _block: BlockNumber,
        _nblocks: u32,
    ) -> Result<(), SmgrError> {
        Ok(())
    }

    fn max_combine(&self, _rel: RelFileLocator, _fork: ForkNumber, _block: BlockNumber) -> u32 {
        MAX_IO_COMBINE_LIMIT
    }

    #[cfg(unix)]
    fn fd(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        _block: BlockNumber,
    ) -> Result<(i32, u64), SmgrError> {
        Err(SmgrError::RelationNotFound { rel, fork })
    }

    fn extend(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        data: &[u8],
        _skip_fsync: bool,
    ) -> Result<(), SmgrError> {
        if data.len() != BLCKSZ {
            return Err(SmgrError::BadBufferSize { size: data.len() });
        }
        let pages = self.fork_pages_mut(rel, fork)?;
        if block != pages.len() as BlockNumber {
            return Err(SmgrError::BlockOutOfRange { rel, fork, block });
        }
        let mut page = [0u8; BLCKSZ];
        page.copy_from_slice(data);
        pages.push(page);
        Ok(())
    }

    fn zero_extend(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        nblocks: u32,
        _skip_fsync: bool,
    ) -> Result<(), SmgrError> {
        let pages = self.fork_pages_mut(rel, fork)?;
        if block != pages.len() as BlockNumber {
            return Err(SmgrError::BlockOutOfRange { rel, fork, block });
        }
        pages.resize(pages.len() + nblocks as usize, [0u8; BLCKSZ]);
        Ok(())
    }

    fn reserve_block(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        _skip_fsync: bool,
    ) -> Result<(), SmgrError> {
        let pages = self.fork_pages_mut(rel, fork)?;
        if block != pages.len() as BlockNumber {
            return Err(SmgrError::BlockOutOfRange { rel, fork, block });
        }
        pages.push([0u8; BLCKSZ]);
        Ok(())
    }

    fn nblocks(&mut self, rel: RelFileLocator, fork: ForkNumber) -> Result<BlockNumber, SmgrError> {
        Ok(self.fork_pages(rel, fork)?.len() as BlockNumber)
    }

    fn truncate(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        nblocks: BlockNumber,
    ) -> Result<(), SmgrError> {
        self.fork_pages_mut(rel, fork)?.truncate(nblocks as usize);
        Ok(())
    }

    fn immedsync(&mut self, _rel: RelFileLocator, _fork: ForkNumber) -> Result<(), SmgrError> {
        Ok(())
    }
}
