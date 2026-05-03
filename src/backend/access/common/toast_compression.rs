// :HACK: root compatibility shim while portable TOAST compression lives in
// `pgrust_access`. Root wrappers preserve the historical `ExecError` surface.
pub use crate::include::access::toast_compression::*;

use crate::backend::executor::ExecError;
use crate::include::access::htup::AttributeCompression;

pub(crate) use pgrust_access::common::toast_compression::CompressedDatum;
pub use pgrust_access::common::toast_compression::compression_name;
use pgrust_access::common::toast_compression::{self, ToastCompressionError};

impl From<ToastCompressionError> for ExecError {
    fn from(error: ToastCompressionError) -> Self {
        match error {
            ToastCompressionError::Lz4NotSupported => ExecError::DetailedError {
                message: "compression method lz4 not supported".into(),
                detail: Some(
                    "This functionality requires the server to be built with lz4 support.".into(),
                ),
                hint: None,
                sqlstate: "0A000",
            },
            ToastCompressionError::InvalidCompressionMethod(value) => ExecError::DetailedError {
                message: format!("invalid compression method \"{value}\""),
                detail: None,
                hint: None,
                sqlstate: "22023",
            },
            ToastCompressionError::InvalidStorageValue { details } => {
                ExecError::InvalidStorageValue {
                    column: "<toast>".into(),
                    details: details.into(),
                }
            }
        }
    }
}

pub(crate) fn ensure_attribute_compression_supported(
    compression: AttributeCompression,
) -> Result<(), ExecError> {
    toast_compression::ensure_attribute_compression_supported(compression).map_err(Into::into)
}

pub(crate) fn parse_attribute_compression(value: &str) -> Result<AttributeCompression, ExecError> {
    toast_compression::parse_attribute_compression(value).map_err(Into::into)
}

pub(crate) fn resolve_attribute_compression(
    compression: AttributeCompression,
    default_compression: AttributeCompression,
) -> Result<AttributeCompression, ExecError> {
    toast_compression::resolve_attribute_compression(compression, default_compression)
        .map_err(Into::into)
}

pub(crate) fn compress_inline_datum(
    value: &[u8],
    requested: AttributeCompression,
    default_compression: AttributeCompression,
) -> Result<Option<CompressedDatum>, ExecError> {
    toast_compression::compress_inline_datum(value, requested, default_compression)
        .map_err(Into::into)
}

pub(crate) fn decompress_inline_datum(bytes: &[u8]) -> Result<Vec<u8>, ExecError> {
    toast_compression::decompress_inline_datum(bytes).map_err(Into::into)
}

pub(crate) fn decompress_external_payload(
    bytes: &[u8],
    rawsize: usize,
    method: u32,
) -> Result<Vec<u8>, ExecError> {
    toast_compression::decompress_external_payload(bytes, rawsize, method).map_err(Into::into)
}
