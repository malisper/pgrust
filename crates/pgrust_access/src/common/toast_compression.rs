use crate::access::htup::AttributeCompression;
use crate::access::toast_compression::ToastCompressionId;
use crate::access::toast_internals::toast_compress_set_size_and_compression_method;
use crate::varatt::{
    VARHDRSZ_COMPRESSED, decode_compressed_inline_datum, encode_compressed_inline_datum,
};

use super::pglz;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompressedDatum {
    pub encoded: Vec<u8>,
    pub method: AttributeCompression,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToastCompressionError {
    Lz4NotSupported,
    InvalidCompressionMethod(String),
    InvalidStorageValue { details: &'static str },
}

pub fn ensure_attribute_compression_supported(
    compression: AttributeCompression,
) -> Result<(), ToastCompressionError> {
    match compression {
        AttributeCompression::Lz4 => {
            #[cfg(not(feature = "lz4"))]
            {
                Err(ToastCompressionError::Lz4NotSupported)
            }
            #[cfg(feature = "lz4")]
            {
                Ok(())
            }
        }
        _ => Ok(()),
    }
}

pub fn compression_name(compression: AttributeCompression) -> &'static str {
    match compression {
        AttributeCompression::Default => "default",
        AttributeCompression::Pglz => "pglz",
        AttributeCompression::Lz4 => "lz4",
    }
}

pub fn parse_attribute_compression(
    value: &str,
) -> Result<AttributeCompression, ToastCompressionError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "default" => Ok(AttributeCompression::Default),
        "pglz" => Ok(AttributeCompression::Pglz),
        "lz4" => {
            ensure_attribute_compression_supported(AttributeCompression::Lz4)?;
            Ok(AttributeCompression::Lz4)
        }
        _ => Err(ToastCompressionError::InvalidCompressionMethod(
            value.trim().to_string(),
        )),
    }
}

pub fn resolve_attribute_compression(
    compression: AttributeCompression,
    default_compression: AttributeCompression,
) -> Result<AttributeCompression, ToastCompressionError> {
    let resolved = match compression {
        AttributeCompression::Default => default_compression,
        other => other,
    };
    ensure_attribute_compression_supported(resolved)?;
    Ok(resolved)
}

fn compression_id(method: AttributeCompression) -> ToastCompressionId {
    match method {
        AttributeCompression::Pglz => ToastCompressionId::Pglz,
        AttributeCompression::Lz4 => ToastCompressionId::Lz4,
        AttributeCompression::Default => ToastCompressionId::Invalid,
    }
}

pub fn compress_inline_datum(
    value: &[u8],
    requested: AttributeCompression,
    default_compression: AttributeCompression,
) -> Result<Option<CompressedDatum>, ToastCompressionError> {
    let resolved = resolve_attribute_compression(requested, default_compression)?;
    let compressed = match resolved {
        AttributeCompression::Pglz => pglz::compress(value, None),
        AttributeCompression::Lz4 => {
            #[cfg(feature = "lz4")]
            {
                let mut out = vec![0u8; lz4_flex::block::get_maximum_output_size(value.len())];
                let len = lz4_flex::block::compress_into(value, &mut out)
                    .expect("lz4 max output size must be sufficient");
                out.truncate(len);
                if out.len() > value.len() {
                    None
                } else {
                    Some(out)
                }
            }
            #[cfg(not(feature = "lz4"))]
            {
                return Err(ToastCompressionError::Lz4NotSupported);
            }
        }
        AttributeCompression::Default => unreachable!("resolved above"),
    };

    let Some(compressed) = compressed else {
        return Ok(None);
    };

    if VARHDRSZ_COMPRESSED + compressed.len() >= value.len().saturating_sub(2) {
        return Ok(None);
    }

    let tcinfo = toast_compress_set_size_and_compression_method(
        value.len() as u32,
        compression_id(resolved) as u32,
    );
    Ok(Some(CompressedDatum {
        encoded: encode_compressed_inline_datum(tcinfo, &compressed),
        method: resolved,
    }))
}

fn decompress_payload(
    method: u32,
    payload: &[u8],
    rawsize: usize,
) -> Result<Vec<u8>, ToastCompressionError> {
    match ToastCompressionId::from_u32(method).ok_or({
        ToastCompressionError::InvalidStorageValue {
            details: "invalid compression method",
        }
    })? {
        ToastCompressionId::Pglz => pglz::decompress(payload, rawsize, true).ok_or(
            ToastCompressionError::InvalidStorageValue {
                details: "compressed pglz data is corrupt",
            },
        ),
        ToastCompressionId::Lz4 => {
            #[cfg(feature = "lz4")]
            {
                lz4_flex::block::decompress(payload, rawsize).map_err(|_| {
                    ToastCompressionError::InvalidStorageValue {
                        details: "compressed lz4 data is corrupt",
                    }
                })
            }
            #[cfg(not(feature = "lz4"))]
            {
                Err(ToastCompressionError::Lz4NotSupported)
            }
        }
        ToastCompressionId::Invalid => Err(ToastCompressionError::InvalidStorageValue {
            details: "invalid compression method",
        }),
    }
}

pub fn decompress_inline_datum(bytes: &[u8]) -> Result<Vec<u8>, ToastCompressionError> {
    let (payload, rawsize, method) = decode_compressed_inline_datum(bytes).ok_or(
        ToastCompressionError::InvalidStorageValue {
            details: "invalid compressed inline datum",
        },
    )?;
    decompress_payload(method, payload, rawsize as usize)
}

pub fn decompress_external_payload(
    bytes: &[u8],
    rawsize: usize,
    method: u32,
) -> Result<Vec<u8>, ToastCompressionError> {
    if method == ToastCompressionId::Invalid as u32 {
        return Ok(bytes.to_vec());
    }
    let payload = bytes
        .get(4..)
        .ok_or(ToastCompressionError::InvalidStorageValue {
            details: "compressed external datum too short",
        })?;
    decompress_payload(method, payload, rawsize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pglz_inline_roundtrip() {
        let input = b"abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabc".to_vec();
        let compressed = compress_inline_datum(
            &input,
            AttributeCompression::Pglz,
            AttributeCompression::Pglz,
        )
        .unwrap()
        .expect("value should compress");
        let decompressed = decompress_inline_datum(&compressed.encoded).unwrap();
        assert_eq!(decompressed, input);
    }

    #[cfg(not(feature = "lz4"))]
    #[test]
    fn lz4_requires_feature() {
        let err = parse_attribute_compression("lz4").unwrap_err();
        assert_eq!(err, ToastCompressionError::Lz4NotSupported);
    }
}
