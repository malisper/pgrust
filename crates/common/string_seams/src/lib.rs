seam_core::seam!(
    // pg_clean_ascii (common/string.c); None only under MCXT_ALLOC_NO_OOM.
    pub fn pg_clean_ascii(s: &str, alloc_flags: i32) -> Option<String>
);
