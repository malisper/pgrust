//! Seam declarations for the nbtree owner (`src/backend/access/nbtree/nbtree.c`,
//! `nbtutils.c`). ipci.c sizes (`BTreeShmemSize`) and initializes
//! (`BTreeShmemInit`) the btree vacuum-cycle-id shared state. The owning unit
//! installs these from its `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// `BTreeShmemSize()` (nbtree.c) — shared-memory bytes for the btree
    /// vacuum shared state; summed by ipci.c `CalculateShmemSize`. `Err`
    /// carries the `add_size`/`mul_size` overflow `ereport`. Scaffolded slot.
    pub fn btree_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `BTreeShmemInit()` (nbtree.c) — allocate-or-attach the btree vacuum
    /// shared state. `Err` carries the out-of-shmem `ereport(ERROR)`.
    /// Scaffolded slot.
    pub fn btree_shmem_init() -> types_error::PgResult<()>
);
