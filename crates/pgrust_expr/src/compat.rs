pub mod backend {
    pub mod access {
        pub mod common {
            pub mod detoast {
                use crate::compat::include::nodes::execnodes::ToastFetchContext;
                use crate::expr_backend::executor::ExecError;

                pub fn detoast_value_bytes(
                    _toast: &ToastFetchContext,
                    _bytes: &[u8],
                ) -> Result<Vec<u8>, ExecError> {
                    Err(ExecError::InvalidStorageValue {
                        column: "<toast>".into(),
                        details: "toast fetch requires root executor".into(),
                    })
                }
            }

            pub mod toast_compression {
                use crate::expr_backend::executor::ExecError;

                pub fn decompress_inline_datum(bytes: &[u8]) -> Result<Vec<u8>, ExecError> {
                    #[cfg(feature = "lz4")]
                    {
                        let _ = bytes;
                    }
                    Err(ExecError::InvalidStorageValue {
                        column: "<compressed>".into(),
                        details: "compressed inline varlena decoding requires root executor".into(),
                    })
                }
            }
        }

        pub mod hash {
            pub use crate::expr_backend::access::hash::*;
        }

        pub mod nbtree {
            pub mod nbtcompare {
                use std::cmp::Ordering;

                use pgrust_nodes::datum::Value;

                pub fn compare_bt_values(left: &Value, right: &Value) -> Ordering {
                    crate::expr_backend::executor::expr_ops::compare_order_values(
                        left, right, None, None, false,
                    )
                    .unwrap_or(Ordering::Equal)
                }
            }
        }
    }

    pub mod executor {
        pub use crate::expr_backend::executor::*;
    }

    pub mod catalog {
        pub mod catalog {
            pub use pgrust_catalog_data::desc::column_desc;
        }
    }

    pub mod libpq {
        pub mod pqformat {
            pub use crate::expr_backend::libpq::pqformat::*;
        }
    }

    pub mod parser {
        pub use crate::services::{
            BoundRelation, DomainConstraintLookup, DomainConstraintLookupKind, DomainLookup,
            ExprCatalogLookup as CatalogLookup,
        };
        pub use pgrust_nodes::parsenodes::{
            ParseError, RawTypeName, SqlType, SqlTypeKind, XmlOption, XmlStandalone,
        };
        pub use pgrust_parser::parse_type_name;

        pub fn resolve_raw_type_name(
            raw: &RawTypeName,
            catalog: &dyn CatalogLookup,
        ) -> Result<SqlType, ParseError> {
            match raw {
                RawTypeName::Builtin(ty) => Ok(*ty),
                RawTypeName::Record => Ok(SqlType::new(SqlTypeKind::Record)),
                RawTypeName::Serial(_) => Err(ParseError::FeatureNotSupported(
                    "serial type resolution is not supported in pgrust_expr".into(),
                )),
                RawTypeName::Named { name, array_bounds } => {
                    let Some(row) = catalog.type_by_name(name) else {
                        return Err(ParseError::UnsupportedType(name.clone()));
                    };
                    let mut ty = row.sql_type;
                    if *array_bounds > 0 {
                        ty.is_array = true;
                    }
                    Ok(ty)
                }
            }
        }
    }

    pub mod storage {
        pub mod page {
            pub mod bufpage {
                pub fn max_align(offset: usize) -> usize {
                    (offset + (pgrust_core::storage::MAXALIGN - 1))
                        & !(pgrust_core::storage::MAXALIGN - 1)
                }
            }
        }
    }

    pub mod tsearch {
        pub use crate::expr_backend::tsearch::*;
    }

    pub mod utils {
        pub mod crc32c {
            pub use crate::expr_backend::utils::crc32c::*;
        }

        pub mod misc {
            pub mod guc_datetime {
                pub use crate::expr_backend::utils::misc::guc_datetime::*;
            }

            pub mod guc_xml {
                pub use crate::expr_backend::utils::misc::guc_xml::*;
            }

            pub mod notices {
                pub use crate::expr_backend::utils::misc::notices::*;
            }

            pub mod stack_depth {
                pub use crate::expr_backend::utils::misc::stack_depth::*;

                pub fn stack_depth_limit_error(max_stack_depth_kb: u32) -> crate::error::ExprError {
                    crate::error::ExprError::DetailedError {
                        message: "stack depth limit exceeded".into(),
                        detail: None,
                        hint: Some(pgrust_core::stack_depth::stack_depth_limit_hint(
                            max_stack_depth_kb,
                        )),
                        sqlstate: "54001",
                    }
                }
            }
        }

        pub mod record {
            use pgrust_nodes::datum::RecordDescriptor;
            use pgrust_nodes::parsenodes::SqlType;

            pub fn register_anonymous_record_descriptor(descriptor: &RecordDescriptor) {
                crate::services::register_record_descriptor(descriptor);
            }

            pub fn assign_anonymous_record_descriptor(
                fields: Vec<(String, SqlType)>,
            ) -> RecordDescriptor {
                let descriptor = pgrust_nodes::record::assign_anonymous_record_descriptor(fields);
                crate::services::register_record_descriptor(&descriptor);
                descriptor
            }

            pub fn lookup_anonymous_record_descriptor(typmod: i32) -> RecordDescriptor {
                crate::services::anonymous_record_descriptor(typmod)
            }
        }

        pub mod time {
            pub mod date {
                pub use crate::expr_backend::utils::time::date::*;
            }
            pub mod datetime {
                pub use crate::expr_backend::utils::time::datetime::*;
            }
            pub mod timestamp {
                pub use crate::expr_backend::utils::time::timestamp::*;
            }
            pub mod system_time {
                pub use crate::expr_backend::utils::time::system_time::*;
            }
            pub mod instant {
                pub use crate::expr_backend::utils::time::instant::*;
            }
        }
    }
}

pub mod include {
    pub mod access {
        pub mod detoast {
            pub use crate::varatt::{
                TOAST_POINTER_SIZE, VARTAG_ONDISK, VarattExternal,
                compressed_inline_compression_method, compressed_inline_extsize,
                compressed_inline_total_size, decode_compressed_inline_datum,
                decode_ondisk_toast_pointer, encode_compressed_inline_datum,
                is_compressed_inline_datum, is_ondisk_toast_pointer,
                varatt_external_get_compression_method, varatt_external_get_extsize,
                varatt_external_is_compressed, varatt_external_set_size_and_compression_method,
            };
        }

        pub mod htup {
            pub use pgrust_core::{
                AttributeAlign, AttributeCompression, AttributeDesc, AttributeStorage,
                ItemPointerData,
            };

            use crate::expr_backend::executor::ExecError;

            #[derive(Debug, Clone, PartialEq, Eq)]
            pub enum TupleValue {
                Null,
                Bytes(Vec<u8>),
                EncodedVarlena(Vec<u8>),
            }

            #[derive(Debug, Clone, PartialEq, Eq)]
            pub struct HeapTuple {
                values: Vec<Option<Vec<u8>>>,
            }

            impl HeapTuple {
                pub fn parse(bytes: &[u8]) -> Result<Self, ExecError> {
                    let mut offset = 0usize;
                    let count = read_u32(bytes, &mut offset)? as usize;
                    let mut values = Vec::with_capacity(count);
                    for _ in 0..count {
                        let len = read_i32(bytes, &mut offset)?;
                        if len < 0 {
                            values.push(None);
                            continue;
                        }
                        let len = len as usize;
                        let Some(value) = bytes.get(offset..offset + len) else {
                            return Err(ExecError::InvalidStorageValue {
                                column: "<tuple>".into(),
                                details: "truncated portable tuple payload".into(),
                            });
                        };
                        values.push(Some(value.to_vec()));
                        offset += len;
                    }
                    Ok(Self { values })
                }

                pub fn from_values(
                    _desc: &[AttributeDesc],
                    values: &[TupleValue],
                ) -> Result<Self, ExecError> {
                    Ok(Self {
                        values: values
                            .iter()
                            .map(|value| match value {
                                TupleValue::Null => None,
                                TupleValue::Bytes(bytes) | TupleValue::EncodedVarlena(bytes) => {
                                    Some(bytes.clone())
                                }
                            })
                            .collect(),
                    })
                }

                pub fn serialize(&self) -> Vec<u8> {
                    let mut out = Vec::new();
                    out.extend_from_slice(&(self.values.len() as u32).to_le_bytes());
                    for value in &self.values {
                        match value {
                            Some(bytes) => {
                                out.extend_from_slice(&(bytes.len() as i32).to_le_bytes());
                                out.extend_from_slice(bytes);
                            }
                            None => out.extend_from_slice(&(-1_i32).to_le_bytes()),
                        }
                    }
                    out
                }

                pub fn deform<'a>(
                    &'a self,
                    _desc: &[AttributeDesc],
                ) -> Result<Vec<Option<&'a [u8]>>, ExecError> {
                    Ok(self.values.iter().map(|value| value.as_deref()).collect())
                }
            }

            fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32, ExecError> {
                let Some(raw) = bytes.get(*offset..*offset + 4) else {
                    return Err(ExecError::InvalidStorageValue {
                        column: "<tuple>".into(),
                        details: "truncated portable tuple payload".into(),
                    });
                };
                *offset += 4;
                Ok(u32::from_le_bytes(raw.try_into().unwrap()))
            }

            fn read_i32(bytes: &[u8], offset: &mut usize) -> Result<i32, ExecError> {
                let Some(raw) = bytes.get(*offset..*offset + 4) else {
                    return Err(ExecError::InvalidStorageValue {
                        column: "<tuple>".into(),
                        details: "truncated portable tuple payload".into(),
                    });
                };
                *offset += 4;
                Ok(i32::from_le_bytes(raw.try_into().unwrap()))
            }
        }

        pub mod itemptr {
            pub use pgrust_core::ItemPointerData;
        }

        pub mod tupdesc {
            pub use pgrust_core::{
                AttributeAlign, AttributeCompression, AttributeDesc, AttributeStorage,
            };
        }
    }

    pub mod catalog {
        pub use pgrust_catalog_data::*;
        pub use pgrust_core::{PolicyCommand, RangeCanonicalization};
    }

    pub mod nodes {
        pub mod datum {
            pub use pgrust_nodes::datum::*;
        }
        pub mod datetime {
            pub use pgrust_nodes::datetime::*;
        }
        pub mod execnodes {
            pub use pgrust_nodes::datum::{NumericValue, Value};

            #[derive(Debug, Clone, PartialEq, Eq)]
            pub struct ToastFetchContext;
        }
        pub mod parsenodes {
            pub use pgrust_nodes::parsenodes::*;
        }
        pub mod plannodes {
            pub use pgrust_nodes::plannodes::*;
        }
        pub mod primnodes {
            pub use pgrust_nodes::primnodes::*;
        }
        pub mod tsearch {
            pub use pgrust_nodes::tsearch::*;
        }
    }

    pub mod varatt {
        pub use crate::varatt::*;
    }
}

pub mod pgrust {
    pub mod compact_string {
        pub use pgrust_core::CompactString;
    }

    pub mod session {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum ByteaOutputFormat {
            Hex,
            Escape,
        }
    }
}
