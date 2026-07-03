
seam_core::seam!(
    // pg_prng_uint32(&pg_global_prng_state) (pg_prng.c).
    pub fn global_prng_uint32() -> u32
);
