seam_core::seam!(
    // parse_bool(value, &result) (bool.c); None is the C `return false`.
    pub fn parse_bool(value: &str) -> Option<bool>
);
