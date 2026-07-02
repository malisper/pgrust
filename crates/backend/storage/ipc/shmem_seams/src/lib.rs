use types_error::PgResult;

seam_core::seam!(
    pub fn shmem_init_struct(name: &str, size: usize) -> PgResult<(*mut u8, bool)>
);

seam_core::seam!(
    pub fn shmem_alloc(size: usize) -> PgResult<*mut u8>
);

seam_core::seam!(
    pub fn add_size(s1: usize, s2: usize) -> PgResult<usize>
);

seam_core::seam!(
    pub fn mul_size(s1: usize, s2: usize) -> PgResult<usize>
);

seam_core::seam!(
    pub fn shmem_lock_acquire()
);

seam_core::seam!(
    pub fn shmem_lock_release()
);
