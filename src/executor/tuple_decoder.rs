//! Compiled tuple decoder — precomputes decode steps at plan time to eliminate
//! per-tuple type dispatch, alignment computation, and branch overhead.

use crate::access::heap::tuple::{AttributeDesc, HEAP_HASNULL, SIZEOF_HEAP_TUPLE_HEADER};
use crate::compact_string::CompactString;

use super::nodes::{RelationDesc, ScalarType, Value};
use super::ExecError;

/// A precomputed decode step for one column, eliminating per-tuple type
/// dispatch and alignment computation.
#[derive(Clone, Debug)]
enum DecodeStep {
    /// Fixed-length column at a known offset from the start of the data area.
    /// Only possible when all preceding columns are also fixed-width and NOT NULL.
    FixedInt32 { data_offset: usize },
    FixedBool { data_offset: usize },
    /// Variable-length text column.
    VarlenText { align: crate::access::heap::tuple::AttributeAlign },
    /// Generic fallback for columns that can't be precomputed.
    Generic {
        attlen: i16,
        align: crate::access::heap::tuple::AttributeAlign,
        ty: ScalarType,
    },
}

/// A compiled tuple decoder for a specific table schema.  Built once at plan
/// time, then reused for every tuple in the scan.
#[derive(Clone, Debug)]
pub(crate) struct CompiledTupleDecoder {
    steps: Vec<DecodeStep>,
    ncols: usize,
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
                    let step = match (column.ty, attr.attlen) {
                        (ScalarType::Int32, 4) => DecodeStep::FixedInt32 { data_offset: aligned },
                        (ScalarType::Bool, 1) => DecodeStep::FixedBool { data_offset: aligned },
                        _ => DecodeStep::Generic {
                            attlen: attr.attlen,
                            align: attr.attalign,
                            ty: column.ty,
                        },
                    };
                    steps.push(step);
                    fixed_offset = Some(aligned + attr.attlen as usize);
                    continue;
                } else if attr.attlen == -1 {
                    let step = match column.ty {
                        ScalarType::Text => DecodeStep::VarlenText { align: attr.attalign },
                        _ => DecodeStep::Generic {
                            attlen: attr.attlen,
                            align: attr.attalign,
                            ty: column.ty,
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
                ty: column.ty,
            });
            if attr.attlen <= 0 || attr.nullable {
                fixed_offset = None;
            }
        }

        Self {
            steps,
            ncols: desc.columns.len(),
        }
    }

    /// Decode into an existing Vec, reusing its heap allocation.
    /// The Vec is cleared and refilled. Callers keep the Vec across
    /// iterations to avoid per-row allocation (like PostgreSQL's
    /// TupleTableSlot Datum array).
    #[inline]
    pub fn decode_into(&self, tuple_bytes: &[u8], values: &mut Vec<Value>) -> Result<(), ExecError> {
        values.clear();
        self.decode_inner(tuple_bytes, values)
    }

    #[inline]
    fn decode_inner(&self, tuple_bytes: &[u8], values: &mut Vec<Value>) -> Result<(), ExecError> {
        if tuple_bytes.len() < SIZEOF_HEAP_TUPLE_HEADER {
            return Err(ExecError::Tuple(crate::access::heap::tuple::TupleError::HeaderTooShort));
        }
        let hoff = tuple_bytes[22] as usize;
        let infomask = u16::from_le_bytes([tuple_bytes[20], tuple_bytes[21]]);
        let has_null = infomask & HEAP_HASNULL != 0;
        let null_bitmap = if has_null {
            &tuple_bytes[SIZEOF_HEAP_TUPLE_HEADER..]
        } else {
            &[] as &[u8]
        };
        let data = &tuple_bytes[hoff..];

        values.reserve(self.ncols.saturating_sub(values.capacity()));
        let mut off = 0usize;

        for (i, step) in self.steps.iter().enumerate() {
            if has_null && crate::access::heap::tuple::att_isnull(i, null_bitmap) {
                values.push(Value::Null);
                continue;
            }

            match step {
                DecodeStep::FixedInt32 { data_offset } => {
                    let o = *data_offset;
                    values.push(Value::Int32(i32::from_le_bytes([
                        data[o], data[o + 1], data[o + 2], data[o + 3],
                    ])));
                    off = o + 4;
                }
                DecodeStep::FixedBool { data_offset } => {
                    values.push(Value::Bool(data[*data_offset] != 0));
                    off = *data_offset + 1;
                }
                DecodeStep::VarlenText { align } => {
                    off = align.align_offset(off);
                    let total_len = u32::from_le_bytes([
                        data[off], data[off + 1], data[off + 2], data[off + 3],
                    ]) as usize;
                    let start = off + 4;
                    let end = off + total_len;
                    // SAFETY: text columns are stored as valid UTF-8 by the insert path.
                    values.push(Value::Text(CompactString::new(
                        unsafe { std::str::from_utf8_unchecked(&data[start..end]) }
                    )));
                    off = end;
                }
                DecodeStep::Generic { attlen, align, ty } => {
                    match *attlen {
                        len if len > 0 => {
                            off = align.align_offset(off);
                            let end = off + len as usize;
                            let bytes = &data[off..end];
                            off = end;
                            values.push(match ty {
                                ScalarType::Int32 => Value::Int32(i32::from_le_bytes([
                                    bytes[0], bytes[1], bytes[2], bytes[3],
                                ])),
                                ScalarType::Bool => Value::Bool(bytes[0] != 0),
                                ScalarType::Text => {
                                    values.push(Value::Null);
                                    continue;
                                }
                            });
                        }
                        -1 => {
                            off = align.align_offset(off);
                            let total_len = u32::from_le_bytes([
                                data[off], data[off + 1], data[off + 2], data[off + 3],
                            ]) as usize;
                            let start = off + 4;
                            let end = off + total_len;
                            let bytes = &data[start..end];
                            off = end;
                            match ty {
                                ScalarType::Text => {
                                    values.push(Value::Text(CompactString::new(
                                        unsafe { std::str::from_utf8_unchecked(bytes) }
                                    )));
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
                                ScalarType::Text => {
                                    values.push(Value::Text(CompactString::new(
                                        unsafe { std::str::from_utf8_unchecked(bytes) }
                                    )));
                                }
                                _ => values.push(Value::Null),
                            }
                        }
                        _ => values.push(Value::Null),
                    }
                }
            }
        }

        Ok(())
    }
}
