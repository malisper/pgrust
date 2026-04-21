pub use crate::include::varatt::{
    TOAST_POINTER_SIZE, VARTAG_ONDISK, VarattExternal, compressed_inline_compression_method,
    compressed_inline_extsize, compressed_inline_total_size, decode_compressed_inline_datum,
    decode_ondisk_toast_pointer, encode_compressed_inline_datum, encode_ondisk_toast_pointer,
    is_compressed_inline_datum, is_ondisk_toast_pointer, varatt_external_get_compression_method,
    varatt_external_get_extsize, varatt_external_is_compressed,
    varatt_external_set_size_and_compression_method,
};
