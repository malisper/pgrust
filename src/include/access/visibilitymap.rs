pub use crate::backend::access::heap::visibilitymap::{
    VisibilityMapBuffer, VisibilityMapError, visibilitymap_clear, visibilitymap_count,
    visibilitymap_get_status, visibilitymap_pin, visibilitymap_pin_ok, visibilitymap_prepare_truncate,
    visibilitymap_set, visibilitymap_truncation_length,
};

pub use crate::include::access::visibilitymapdefs::{
    BITS_PER_HEAPBLOCK, VISIBILITYMAP_ALL_FROZEN, VISIBILITYMAP_ALL_VISIBLE,
    VISIBILITYMAP_VALID_BITS,
};
