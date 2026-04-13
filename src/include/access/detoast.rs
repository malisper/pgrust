pub use crate::include::varatt::{
    TOAST_POINTER_SIZE, VARTAG_ONDISK, VarattExternal, decode_ondisk_toast_pointer,
    encode_ondisk_toast_pointer, is_ondisk_toast_pointer, varatt_external_get_compression_method,
    varatt_external_get_extsize, varatt_external_is_compressed,
    varatt_external_set_size_and_compression_method,
};
