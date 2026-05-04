use crate::access::heaptoast::ToastChunk;
use crate::common::toast_compression::decompress_external_payload;
use crate::varatt::{
    VARHDRSZ, VarattExternal, decode_ondisk_toast_pointer, varatt_external_get_compression_method,
    varatt_external_get_extsize, varatt_external_is_compressed,
};
use crate::{AccessError, AccessResult};

pub fn ondisk_toast_pointer(bytes: &[u8]) -> AccessResult<VarattExternal> {
    decode_ondisk_toast_pointer(bytes).ok_or(AccessError::Corrupt("invalid on-disk toast pointer"))
}

pub fn reconstruct_external_toast_value(
    pointer: VarattExternal,
    mut chunks: Vec<ToastChunk>,
) -> AccessResult<Vec<u8>> {
    chunks.sort_by_key(|chunk| chunk.seq);
    let mut data = Vec::new();
    for chunk in chunks {
        data.extend_from_slice(&chunk.data);
    }

    let expected = varatt_external_get_extsize(pointer) as usize;
    if data.len() != expected {
        let value_id = pointer.va_valueid;
        return Err(AccessError::Scalar(format!(
            "toast value {} reconstructed to {} bytes, expected {}",
            value_id,
            data.len(),
            expected
        )));
    }

    if !varatt_external_is_compressed(pointer) {
        return Ok(data);
    }

    let rawsize = usize::try_from(pointer.va_rawsize)
        .unwrap_or_default()
        .saturating_sub(VARHDRSZ);
    decompress_external_payload(
        &data,
        rawsize,
        varatt_external_get_compression_method(pointer),
    )
    .map_err(Into::into)
}

pub fn detoast_value_bytes_with_chunks(
    bytes: &[u8],
    chunks: Vec<ToastChunk>,
) -> AccessResult<Vec<u8>> {
    reconstruct_external_toast_value(ondisk_toast_pointer(bytes)?, chunks)
}

pub fn detoast_value_bytes_with_fetch(
    bytes: &[u8],
    mut fetch_chunks: impl FnMut(VarattExternal) -> AccessResult<Vec<ToastChunk>>,
) -> AccessResult<Vec<u8>> {
    let pointer = ondisk_toast_pointer(bytes)?;
    let chunks = fetch_chunks(pointer)?;
    reconstruct_external_toast_value(pointer, chunks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::varatt::{
        encode_ondisk_toast_pointer, varatt_external_set_size_and_compression_method,
    };

    #[test]
    fn reconstructs_external_toast_chunks_in_sequence_order() {
        let pointer = VarattExternal {
            va_rawsize: 9,
            va_extinfo: 5,
            va_valueid: 42,
            va_toastrelid: 99,
        };
        let result = reconstruct_external_toast_value(
            pointer,
            vec![
                ToastChunk {
                    id: 42,
                    seq: 1,
                    data: b"lo".to_vec(),
                },
                ToastChunk {
                    id: 42,
                    seq: 0,
                    data: b"hel".to_vec(),
                },
            ],
        )
        .unwrap();
        assert_eq!(result, b"hello");
    }

    #[test]
    fn fetches_chunks_after_decoding_pointer() {
        let pointer = VarattExternal {
            va_rawsize: 7,
            va_extinfo: varatt_external_set_size_and_compression_method(3, 2),
            va_valueid: 7,
            va_toastrelid: 11,
        };
        let encoded = encode_ondisk_toast_pointer(pointer);
        let mut seen = None;
        let result = detoast_value_bytes_with_fetch(&encoded, |decoded| {
            seen = Some(decoded);
            Ok(vec![ToastChunk {
                id: decoded.va_valueid,
                seq: 0,
                data: b"abc".to_vec(),
            }])
        })
        .unwrap();
        assert_eq!(seen, Some(pointer));
        assert_eq!(result, b"abc");
    }
}
