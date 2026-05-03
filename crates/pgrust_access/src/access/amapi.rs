#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IndexBuildResult {
    pub heap_tuples: u64,
    pub index_tuples: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IndexBulkDeleteResult {
    pub num_pages: u64,
    pub num_index_tuples: u64,
    pub num_removed_tuples: u64,
    pub num_deleted_pages: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexUniqueCheck {
    No,
    Yes,
    Partial,
}
