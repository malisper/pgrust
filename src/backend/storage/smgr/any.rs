use super::*;

pub enum AnyStorageManager {
    Md(MdStorageManager),
    Mem(MemStorageManager),
}

impl AnyStorageManager {
    pub fn md(base_dir: impl Into<std::path::PathBuf>) -> Self {
        Self::Md(MdStorageManager::new(base_dir))
    }

    pub fn mem() -> Self {
        Self::Mem(MemStorageManager::new())
    }

    pub fn acquire_external_fd(&mut self) {
        if let Self::Md(smgr) = self {
            smgr.acquire_external_fd();
        }
    }

    pub fn release_external_fd(&mut self) {
        if let Self::Md(smgr) = self {
            smgr.release_external_fd();
        }
    }

    pub fn replace_relation_main_fork_from_shadow(
        &mut self,
        shadow: RelFileLocator,
        target: RelFileLocator,
    ) -> Result<(), SmgrError> {
        match self {
            Self::Md(smgr) => smgr.replace_relation_main_fork_from_shadow(shadow, target),
            Self::Mem(_) => Err(SmgrError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "atomic relfile replacement is not supported by memory storage",
            ))),
        }
    }
}

impl StorageManager for AnyStorageManager {
    fn open(&mut self, rel: RelFileLocator) -> Result<(), SmgrError> {
        match self {
            Self::Md(smgr) => smgr.open(rel),
            Self::Mem(smgr) => smgr.open(rel),
        }
    }

    fn close(&mut self, rel: RelFileLocator, fork: ForkNumber) -> Result<(), SmgrError> {
        match self {
            Self::Md(smgr) => smgr.close(rel, fork),
            Self::Mem(smgr) => smgr.close(rel, fork),
        }
    }

    fn create(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        is_redo: bool,
    ) -> Result<(), SmgrError> {
        match self {
            Self::Md(smgr) => smgr.create(rel, fork, is_redo),
            Self::Mem(smgr) => smgr.create(rel, fork, is_redo),
        }
    }

    fn exists(&mut self, rel: RelFileLocator, fork: ForkNumber) -> bool {
        match self {
            Self::Md(smgr) => smgr.exists(rel, fork),
            Self::Mem(smgr) => smgr.exists(rel, fork),
        }
    }

    fn unlink(&mut self, rel: RelFileLocator, fork: Option<ForkNumber>, is_redo: bool) {
        match self {
            Self::Md(smgr) => smgr.unlink(rel, fork, is_redo),
            Self::Mem(smgr) => smgr.unlink(rel, fork, is_redo),
        }
    }

    fn read_block(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        buf: &mut [u8],
    ) -> Result<(), SmgrError> {
        match self {
            Self::Md(smgr) => smgr.read_block(rel, fork, block, buf),
            Self::Mem(smgr) => smgr.read_block(rel, fork, block, buf),
        }
    }

    fn write_block(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        data: &[u8],
        skip_fsync: bool,
    ) -> Result<(), SmgrError> {
        match self {
            Self::Md(smgr) => smgr.write_block(rel, fork, block, data, skip_fsync),
            Self::Mem(smgr) => smgr.write_block(rel, fork, block, data, skip_fsync),
        }
    }

    fn writeback(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        nblocks: u32,
    ) -> Result<(), SmgrError> {
        match self {
            Self::Md(smgr) => smgr.writeback(rel, fork, block, nblocks),
            Self::Mem(smgr) => smgr.writeback(rel, fork, block, nblocks),
        }
    }

    fn prefetch(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        nblocks: u32,
    ) -> Result<(), SmgrError> {
        match self {
            Self::Md(smgr) => smgr.prefetch(rel, fork, block, nblocks),
            Self::Mem(smgr) => smgr.prefetch(rel, fork, block, nblocks),
        }
    }

    fn max_combine(&self, rel: RelFileLocator, fork: ForkNumber, block: BlockNumber) -> u32 {
        match self {
            Self::Md(smgr) => smgr.max_combine(rel, fork, block),
            Self::Mem(smgr) => smgr.max_combine(rel, fork, block),
        }
    }

    #[cfg(unix)]
    fn fd(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
    ) -> Result<(i32, u64), SmgrError> {
        match self {
            Self::Md(smgr) => smgr.fd(rel, fork, block),
            Self::Mem(smgr) => smgr.fd(rel, fork, block),
        }
    }

    fn extend(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        data: &[u8],
        skip_fsync: bool,
    ) -> Result<(), SmgrError> {
        match self {
            Self::Md(smgr) => smgr.extend(rel, fork, block, data, skip_fsync),
            Self::Mem(smgr) => smgr.extend(rel, fork, block, data, skip_fsync),
        }
    }

    fn zero_extend(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        nblocks: u32,
        skip_fsync: bool,
    ) -> Result<(), SmgrError> {
        match self {
            Self::Md(smgr) => smgr.zero_extend(rel, fork, block, nblocks, skip_fsync),
            Self::Mem(smgr) => smgr.zero_extend(rel, fork, block, nblocks, skip_fsync),
        }
    }

    fn reserve_block(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        skip_fsync: bool,
    ) -> Result<(), SmgrError> {
        match self {
            Self::Md(smgr) => smgr.reserve_block(rel, fork, block, skip_fsync),
            Self::Mem(smgr) => smgr.reserve_block(rel, fork, block, skip_fsync),
        }
    }

    fn nblocks(&mut self, rel: RelFileLocator, fork: ForkNumber) -> Result<BlockNumber, SmgrError> {
        match self {
            Self::Md(smgr) => smgr.nblocks(rel, fork),
            Self::Mem(smgr) => smgr.nblocks(rel, fork),
        }
    }

    fn truncate(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        nblocks: BlockNumber,
    ) -> Result<(), SmgrError> {
        match self {
            Self::Md(smgr) => smgr.truncate(rel, fork, nblocks),
            Self::Mem(smgr) => smgr.truncate(rel, fork, nblocks),
        }
    }

    fn immedsync(&mut self, rel: RelFileLocator, fork: ForkNumber) -> Result<(), SmgrError> {
        match self {
            Self::Md(smgr) => smgr.immedsync(rel, fork),
            Self::Mem(smgr) => smgr.immedsync(rel, fork),
        }
    }
}
