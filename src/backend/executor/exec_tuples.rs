//! Compiled tuple decoder — precomputes decode steps at plan time to eliminate
//! per-tuple type dispatch, alignment computation, and branch overhead.

use super::ExecError;
use super::exec_expr::parse_numeric_text;
use super::expr_geometry::{decode_path_bytes, decode_polygon_bytes};
use super::value_io::missing_column_value;
use super::value_io::{decode_anyarray_bytes, decode_array_bytes};
use crate::backend::executor::{decode_tsquery_bytes, decode_tsvector_bytes};
use crate::include::access::htup::HEAP_NATTS_MASK;
use crate::include::access::htup::{AttributeDesc, HEAP_HASNULL, SIZEOF_HEAP_TUPLE_HEADER};
use crate::include::nodes::datum::{GeoBox, GeoCircle, GeoLine, GeoLseg, GeoPoint};
use crate::include::nodes::execnodes::{RelationDesc, ScalarType, Value};

/// A precomputed decode step for one column, eliminating per-tuple type
/// dispatch and alignment computation.
#[derive(Clone, Debug)]
enum DecodeStep {
    /// Fixed-length column at a known offset from the start of the data area.
    /// Only possible when all preceding columns are also fixed-width and NOT NULL.
    FixedInt32 {
        data_offset: usize,
        is_oid: bool,
    },
    FixedBool {
        data_offset: usize,
    },
    /// Variable-length text column.
    VarlenText {
        align: crate::include::access::htup::AttributeAlign,
    },
    /// Generic fallback for columns that can't be precomputed.
    Generic {
        attlen: i16,
        align: crate::include::access::htup::AttributeAlign,
        ty: ScalarType,
        sql_type: crate::backend::parser::SqlType,
        is_oid: bool,
    },
}

/// A compiled tuple decoder for a specific table schema.  Built once at plan
/// time, then reused for every tuple in the scan.
#[derive(Clone, Debug)]
pub(crate) struct CompiledTupleDecoder {
    steps: Vec<DecodeStep>,
    ncols: usize,
    missing_defaults: Vec<Option<Value>>,
}

impl CompiledTupleDecoder {
    /// Compile a decoder for the given relation descriptor.
    pub fn compile(desc: &RelationDesc, attr_descs: &[AttributeDesc]) -> Self {
        let mut steps = Vec::with_capacity(desc.columns.len());
        let mut fixed_offset: Option<usize> = Some(0);

        for (column, attr) in desc.columns.iter().zip(attr_descs.iter()) {
            if let Some(off) = fixed_offset {
                let aligned = attr.attalign.align_offset(off);
                if attr.attlen > 0 && !attr.nullable {
                    // Fixed-width NOT NULL — we know the exact byte offset.
                    let step = match (&column.ty, attr.attlen) {
                        (ScalarType::Int32, 4) => DecodeStep::FixedInt32 {
                            data_offset: aligned,
                            is_oid: matches!(
                                column.sql_type.kind,
                                crate::backend::parser::SqlTypeKind::Oid
                            ),
                        },
                        (ScalarType::Bool, 1) => DecodeStep::FixedBool {
                            data_offset: aligned,
                        },
                        _ => DecodeStep::Generic {
                            attlen: attr.attlen,
                            align: attr.attalign,
                            ty: column.ty.clone(),
                            sql_type: column.sql_type,
                            is_oid: matches!(
                                column.sql_type.kind,
                                crate::backend::parser::SqlTypeKind::Oid
                            ),
                        },
                    };
                    steps.push(step);
                    fixed_offset = Some(aligned + attr.attlen as usize);
                    continue;
                } else if attr.attlen == -1 {
                    let step = match &column.ty {
                        ScalarType::Text => DecodeStep::VarlenText {
                            align: attr.attalign,
                        },
                        _ => DecodeStep::Generic {
                            attlen: attr.attlen,
                            align: attr.attalign,
                            ty: column.ty.clone(),
                            sql_type: column.sql_type,
                            is_oid: matches!(
                                column.sql_type.kind,
                                crate::backend::parser::SqlTypeKind::Oid
                            ),
                        },
                    };
                    steps.push(step);
                    fixed_offset = None;
                    continue;
                }
            }

            // Fallback: prior column was varlen or nullable.
            steps.push(DecodeStep::Generic {
                attlen: attr.attlen,
                align: attr.attalign,
                ty: column.ty.clone(),
                sql_type: column.sql_type,
                is_oid: matches!(
                    column.sql_type.kind,
                    crate::backend::parser::SqlTypeKind::Oid
                ),
            });
            if attr.attlen <= 0 || attr.nullable {
                fixed_offset = None;
            }
        }

        Self {
            steps,
            ncols: desc.columns.len(),
            missing_defaults: desc
                .columns
                .iter()
                .map(|column| Some(missing_column_value(column)))
                .collect(),
        }
    }

    pub(crate) fn ncols(&self) -> usize {
        self.ncols
    }

    /// Return the fixed byte offset for a column if it's a FixedInt32.
    /// Used by the predicate compiler to read int32 values directly from
    /// raw tuple bytes, bypassing the full decode path.
    pub(crate) fn fixed_int32_offset(&self, col: usize) -> Option<usize> {
        if col >= self.steps.len() {
            return None;
        }
        match &self.steps[col] {
            DecodeStep::FixedInt32 { data_offset, .. } => Some(*data_offset),
            _ => None,
        }
    }

    /// Incrementally decode columns `start_attr..end_attr` into `values`,
    /// resuming from `byte_offset` in the tuple data area.
    ///
    /// Like PostgreSQL's `slot_deform_heap_tuple`: only decodes the columns
    /// that haven't been decoded yet. Fixed-offset columns jump directly to
    /// their precomputed offset; variable-width columns resume from
    /// `byte_offset`.
    pub fn decode_range(
        &self,
        tuple_bytes: &[u8],
        values: &mut Vec<Value>,
        start_attr: usize,
        end_attr: usize,
        byte_offset: &mut usize,
    ) -> Result<(), ExecError> {
        let end_attr = end_attr.min(self.ncols);
        if start_attr >= end_attr {
            return Ok(());
        }

        if tuple_bytes.len() < SIZEOF_HEAP_TUPLE_HEADER {
            return Err(ExecError::Tuple(
                crate::include::access::htup::TupleError::HeaderTooShort,
            ));
        }
        let hoff = tuple_bytes[22] as usize;
        let infomask2 = u16::from_le_bytes([tuple_bytes[18], tuple_bytes[19]]);
        let infomask = u16::from_le_bytes([tuple_bytes[20], tuple_bytes[21]]);
        let physical_natts = usize::from(infomask2 & HEAP_NATTS_MASK);
        let has_null = infomask & HEAP_HASNULL != 0;
        let null_bitmap = if has_null {
            &tuple_bytes[SIZEOF_HEAP_TUPLE_HEADER..]
        } else {
            &[] as &[u8]
        };
        let data = &tuple_bytes[hoff..];

        let mut off = *byte_offset;

        for i in start_attr..end_attr {
            if i >= physical_natts {
                values.push(self.missing_attr_value(i));
                continue;
            }

            let step = &self.steps[i];

            if has_null && crate::include::access::htup::att_isnull(i, null_bitmap) {
                values.push(Value::Null);
                continue;
            }

            match step {
                DecodeStep::FixedInt32 {
                    data_offset,
                    is_oid,
                } => {
                    let o = *data_offset;
                    let raw = i32::from_le_bytes([data[o], data[o + 1], data[o + 2], data[o + 3]]);
                    if *is_oid {
                        values.push(Value::Int64(raw as u32 as i64));
                    } else {
                        values.push(Value::Int32(raw));
                    }
                    off = o + 4;
                }
                DecodeStep::FixedBool { data_offset } => {
                    values.push(Value::Bool(data[*data_offset] != 0));
                    off = *data_offset + 1;
                }
                DecodeStep::VarlenText { align } => {
                    if data[off] & 0x01 != 0 {
                        // Short varlena: 1-byte header, no alignment.
                        let total_len = (data[off] >> 1) as usize;
                        let start = off + 1;
                        let end = off + total_len;
                        values.push(Value::TextRef(
                            data[start..end].as_ptr(),
                            (end - start) as u32,
                        ));
                        off = end;
                    } else {
                        off = align.align_offset(off);
                        let raw = u32::from_le_bytes([
                            data[off],
                            data[off + 1],
                            data[off + 2],
                            data[off + 3],
                        ]);
                        let total_len = (raw >> 2) as usize;
                        let start = off + 4;
                        let end = off + total_len;
                        values.push(Value::TextRef(
                            data[start..end].as_ptr(),
                            (end - start) as u32,
                        ));
                        off = end;
                    }
                }
                DecodeStep::Generic {
                    attlen,
                    align,
                    ty,
                    sql_type,
                    is_oid,
                } => match *attlen {
                    len if len > 0 => {
                        off = align.align_offset(off);
                        let end = off + len as usize;
                        let bytes = &data[off..end];
                        off = end;
                        values.push(match ty {
                            ScalarType::Int16 => {
                                Value::Int16(i16::from_le_bytes([bytes[0], bytes[1]]))
                            }
                            ScalarType::Int32 => {
                                let raw =
                                    i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                                if *is_oid {
                                    Value::Int64(raw as u32 as i64)
                                } else {
                                    Value::Int32(raw)
                                }
                            }
                            ScalarType::Int64 => Value::Int64(i64::from_le_bytes([
                                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
                                bytes[6], bytes[7],
                            ])),
                            ScalarType::Date => {
                                Value::Date(crate::include::nodes::datetime::DateADT(
                                    i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
                                ))
                            }
                            ScalarType::Time => Value::Time(
                                crate::include::nodes::datetime::TimeADT(i64::from_le_bytes([
                                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
                                    bytes[6], bytes[7],
                                ])),
                            ),
                            ScalarType::TimeTz => {
                                Value::TimeTz(crate::include::nodes::datetime::TimeTzADT {
                                    time: crate::include::nodes::datetime::TimeADT(
                                        i64::from_le_bytes([
                                            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4],
                                            bytes[5], bytes[6], bytes[7],
                                        ]),
                                    ),
                                    offset_seconds: i32::from_le_bytes([
                                        bytes[8], bytes[9], bytes[10], bytes[11],
                                    ]),
                                })
                            }
                            ScalarType::Timestamp => {
                                Value::Timestamp(crate::include::nodes::datetime::TimestampADT(
                                    i64::from_le_bytes([
                                        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
                                        bytes[6], bytes[7],
                                    ]),
                                ))
                            }
                            ScalarType::TimestampTz => {
                                Value::TimestampTz(crate::include::nodes::datetime::TimestampTzADT(
                                    i64::from_le_bytes([
                                        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
                                        bytes[6], bytes[7],
                                    ]),
                                ))
                            }
                            ScalarType::BitString => {
                                values.push(Value::Null);
                                continue;
                            }
                            ScalarType::Float32 => Value::Float64(f32::from_le_bytes([
                                bytes[0], bytes[1], bytes[2], bytes[3],
                            ])
                                as f64),
                            ScalarType::Float64 => Value::Float64(f64::from_le_bytes([
                                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
                                bytes[6], bytes[7],
                            ])),
                            ScalarType::Point => Value::Point(GeoPoint {
                                x: f64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                                y: f64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                            }),
                            ScalarType::Line => Value::Line(GeoLine {
                                a: f64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                                b: f64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                                c: f64::from_le_bytes(bytes[16..24].try_into().unwrap()),
                            }),
                            ScalarType::Lseg => Value::Lseg(GeoLseg {
                                p: [
                                    GeoPoint {
                                        x: f64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                                        y: f64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                                    },
                                    GeoPoint {
                                        x: f64::from_le_bytes(bytes[16..24].try_into().unwrap()),
                                        y: f64::from_le_bytes(bytes[24..32].try_into().unwrap()),
                                    },
                                ],
                            }),
                            ScalarType::Box => Value::Box(GeoBox {
                                high: GeoPoint {
                                    x: f64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                                    y: f64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                                },
                                low: GeoPoint {
                                    x: f64::from_le_bytes(bytes[16..24].try_into().unwrap()),
                                    y: f64::from_le_bytes(bytes[24..32].try_into().unwrap()),
                                },
                            }),
                            ScalarType::Circle => Value::Circle(GeoCircle {
                                center: GeoPoint {
                                    x: f64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                                    y: f64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                                },
                                radius: f64::from_le_bytes(bytes[16..24].try_into().unwrap()),
                            }),
                            ScalarType::Bool => Value::Bool(bytes[0] != 0),
                            ScalarType::Numeric => {
                                values.push(Value::Null);
                                continue;
                            }
                            ScalarType::Json => {
                                values.push(Value::Null);
                                continue;
                            }
                            ScalarType::Jsonb => {
                                values.push(Value::Null);
                                continue;
                            }
                            ScalarType::JsonPath => {
                                values.push(Value::Null);
                                continue;
                            }
                            ScalarType::TsVector => {
                                values.push(Value::Null);
                                continue;
                            }
                            ScalarType::TsQuery => {
                                values.push(Value::Null);
                                continue;
                            }
                            ScalarType::Bytea => {
                                values.push(Value::Null);
                                continue;
                            }
                            ScalarType::Text => {
                                values.push(Value::Null);
                                continue;
                            }
                            ScalarType::Path => {
                                values.push(Value::Null);
                                continue;
                            }
                            ScalarType::Polygon => {
                                values.push(Value::Null);
                                continue;
                            }
                            ScalarType::Array(_) => {
                                values.push(Value::Null);
                                continue;
                            }
                        });
                    }
                    -1 => {
                        let (bytes_slice, new_off) = if data[off] & 0x01 != 0 {
                            let total_len = (data[off] >> 1) as usize;
                            (&data[off + 1..off + total_len], off + total_len)
                        } else {
                            off = align.align_offset(off);
                            let raw = u32::from_le_bytes([
                                data[off],
                                data[off + 1],
                                data[off + 2],
                                data[off + 3],
                            ]);
                            let total_len = (raw >> 2) as usize;
                            (&data[off + 4..off + total_len], off + total_len)
                        };
                        off = new_off;
                        match ty {
                            ScalarType::Numeric => {
                                values.push(Value::Numeric(
                                    parse_numeric_text(unsafe {
                                        std::str::from_utf8_unchecked(bytes_slice)
                                    })
                                    .ok_or_else(|| {
                                        ExecError::InvalidStorageValue {
                                            column: "<tuple>".into(),
                                            details: "invalid numeric text".into(),
                                        }
                                    })?,
                                ));
                            }
                            ScalarType::Json => {
                                values.push(Value::Json(
                                    crate::pgrust::compact_string::CompactString::new(unsafe {
                                        std::str::from_utf8_unchecked(bytes_slice)
                                    }),
                                ));
                            }
                            ScalarType::Jsonb => {
                                values.push(Value::Jsonb(bytes_slice.to_vec()));
                            }
                            ScalarType::JsonPath => {
                                values.push(Value::JsonPath(
                                    crate::pgrust::compact_string::CompactString::new(unsafe {
                                        std::str::from_utf8_unchecked(bytes_slice)
                                    }),
                                ));
                            }
                            ScalarType::TsVector => {
                                values.push(Value::TsVector(decode_tsvector_bytes(bytes_slice)?));
                            }
                            ScalarType::TsQuery => {
                                values.push(Value::TsQuery(decode_tsquery_bytes(bytes_slice)?));
                            }
                            ScalarType::BitString => {
                                if bytes_slice.len() < 4 {
                                    return Err(ExecError::InvalidStorageValue {
                                        column: "<tuple>".into(),
                                        details: "bit payload too short".into(),
                                    });
                                }
                                let bit_len =
                                    u32::from_le_bytes(bytes_slice[0..4].try_into().unwrap())
                                        as i32;
                                values.push(Value::Bit(
                                    crate::include::nodes::datum::BitString::new(
                                        bit_len,
                                        bytes_slice[4..].to_vec(),
                                    ),
                                ));
                            }
                            ScalarType::Bytea => {
                                values.push(Value::Bytea(bytes_slice.to_vec()));
                            }
                            ScalarType::Text => {
                                values.push(Value::TextRef(
                                    bytes_slice.as_ptr(),
                                    bytes_slice.len() as u32,
                                ));
                            }
                            ScalarType::Path => {
                                values.push(Value::Path(decode_path_bytes(bytes_slice)?));
                            }
                            ScalarType::Polygon => {
                                values.push(Value::Polygon(decode_polygon_bytes(bytes_slice)?));
                            }
                            ScalarType::Array(elem_ty) => {
                                let _ = elem_ty;
                                values.push(
                                    if sql_type.kind
                                        == crate::backend::parser::SqlTypeKind::AnyArray
                                    {
                                        decode_anyarray_bytes(bytes_slice)?
                                    } else {
                                        decode_array_bytes(sql_type.element_type(), bytes_slice)?
                                    },
                                );
                            }
                            _ => values.push(Value::Null),
                        }
                    }
                    -2 => {
                        let mut end = off;
                        while data[end] != 0 {
                            end += 1;
                        }
                        let bytes = &data[off..end];
                        off = end + 1;
                        match ty {
                            ScalarType::Numeric => {
                                values.push(Value::Numeric(
                                    parse_numeric_text(unsafe {
                                        std::str::from_utf8_unchecked(bytes)
                                    })
                                    .ok_or_else(|| {
                                        ExecError::InvalidStorageValue {
                                            column: "<tuple>".into(),
                                            details: "invalid numeric text".into(),
                                        }
                                    })?,
                                ));
                            }
                            ScalarType::Json => {
                                values.push(Value::Json(
                                    crate::pgrust::compact_string::CompactString::new(unsafe {
                                        std::str::from_utf8_unchecked(bytes)
                                    }),
                                ));
                            }
                            ScalarType::Jsonb => {
                                values.push(Value::Jsonb(bytes.to_vec()));
                            }
                            ScalarType::JsonPath => {
                                values.push(Value::JsonPath(
                                    crate::pgrust::compact_string::CompactString::new(unsafe {
                                        std::str::from_utf8_unchecked(bytes)
                                    }),
                                ));
                            }
                            ScalarType::TsVector => {
                                values.push(Value::TsVector(decode_tsvector_bytes(bytes)?));
                            }
                            ScalarType::TsQuery => {
                                values.push(Value::TsQuery(decode_tsquery_bytes(bytes)?));
                            }
                            ScalarType::BitString => {
                                if bytes.len() < 4 {
                                    return Err(ExecError::InvalidStorageValue {
                                        column: "<tuple>".into(),
                                        details: "bit payload too short".into(),
                                    });
                                }
                                let bit_len =
                                    u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as i32;
                                values.push(Value::Bit(
                                    crate::include::nodes::datum::BitString::new(
                                        bit_len,
                                        bytes[4..].to_vec(),
                                    ),
                                ));
                            }
                            ScalarType::Bytea => {
                                values.push(Value::Bytea(bytes.to_vec()));
                            }
                            ScalarType::Text => {
                                values.push(Value::TextRef(bytes.as_ptr(), bytes.len() as u32));
                            }
                            ScalarType::Path => {
                                values.push(Value::Path(decode_path_bytes(bytes)?));
                            }
                            ScalarType::Polygon => {
                                values.push(Value::Polygon(decode_polygon_bytes(bytes)?));
                            }
                            ScalarType::Array(elem_ty) => {
                                let _ = elem_ty;
                                values.push(
                                    if sql_type.kind
                                        == crate::backend::parser::SqlTypeKind::AnyArray
                                    {
                                        decode_anyarray_bytes(bytes)?
                                    } else {
                                        decode_array_bytes(sql_type.element_type(), bytes)?
                                    },
                                );
                            }
                            _ => values.push(Value::Null),
                        }
                    }
                    _ => values.push(Value::Null),
                },
            }
        }

        *byte_offset = off;
        Ok(())
    }

    fn missing_attr_value(&self, index: usize) -> Value {
        self.missing_defaults
            .get(index)
            .and_then(|value| value.clone())
            .unwrap_or(Value::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::executor::value_io::tuple_from_values;
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::include::nodes::datum::{ArrayDimension, ArrayValue};

    #[test]
    fn compiled_tuple_decoder_roundtrips_anyarray_flat_payload() {
        let desc = RelationDesc {
            columns: vec![column_desc("v", SqlType::new(SqlTypeKind::AnyArray), true)],
        };
        let value = Value::PgArray(
            ArrayValue::from_1d(vec![
                Value::Text("a".into()),
                Value::Null,
                Value::Text("bee".into()),
            ])
            .with_element_type_oid(crate::include::catalog::TEXT_TYPE_OID),
        );

        let tuple = tuple_from_values(&desc, std::slice::from_ref(&value)).unwrap();
        let raw = tuple.serialize();
        let decoder = CompiledTupleDecoder::compile(&desc, &desc.attribute_descs());
        let mut values = Vec::new();
        let mut byte_offset = 0;
        decoder
            .decode_range(&raw, &mut values, 0, 1, &mut byte_offset)
            .unwrap();

        assert_eq!(values, vec![value]);
    }

    #[test]
    fn compiled_tuple_decoder_preserves_zero_length_array_shape() {
        let desc = RelationDesc {
            columns: vec![column_desc(
                "v",
                SqlType::array_of(SqlType::new(SqlTypeKind::Int4)),
                true,
            )],
        };
        let value = Value::PgArray(
            ArrayValue::from_dimensions(
                vec![ArrayDimension {
                    lower_bound: 5,
                    length: 0,
                }],
                Vec::new(),
            )
            .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
        );

        let tuple = tuple_from_values(&desc, std::slice::from_ref(&value)).unwrap();
        let raw = tuple.serialize();
        let decoder = CompiledTupleDecoder::compile(&desc, &desc.attribute_descs());
        let mut values = Vec::new();
        let mut byte_offset = 0;
        decoder
            .decode_range(&raw, &mut values, 0, 1, &mut byte_offset)
            .unwrap();

        assert_eq!(values, vec![value]);
    }
}
