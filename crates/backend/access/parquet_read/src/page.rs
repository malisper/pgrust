//! Page-header parse and the per-chunk page walker. Kernel dispatch is PER
//! PAGE (mandatory: dictionary-to-PLAIN fallback happens mid-chunk). v1 data
//! pages and dictionary pages decode; v2 pages error cleanly in this
//! increment.

use types_error::PgResult;

use crate::meta::unsupported;
use crate::thrift::{corrupt, Cur, T_I32, T_STRUCT};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Enc {
    Plain,
    PlainDictionary,
    Rle,
    BitPacked,
    DeltaBinaryPacked,
    DeltaLengthByteArray,
    DeltaByteArray,
    RleDictionary,
    ByteStreamSplit,
    Unknown(i32),
}

impl Enc {
    fn from_i32(v: i32) -> Enc {
        match v {
            0 => Enc::Plain,
            2 => Enc::PlainDictionary,
            3 => Enc::Rle,
            4 => Enc::BitPacked,
            5 => Enc::DeltaBinaryPacked,
            6 => Enc::DeltaLengthByteArray,
            7 => Enc::DeltaByteArray,
            8 => Enc::RleDictionary,
            9 => Enc::ByteStreamSplit,
            other => Enc::Unknown(other),
        }
    }

    pub fn name(self) -> String {
        match self {
            Enc::Plain => "PLAIN".into(),
            Enc::PlainDictionary => "PLAIN_DICTIONARY".into(),
            Enc::Rle => "RLE".into(),
            Enc::BitPacked => "BIT_PACKED".into(),
            Enc::DeltaBinaryPacked => "DELTA_BINARY_PACKED".into(),
            Enc::DeltaLengthByteArray => "DELTA_LENGTH_BYTE_ARRAY".into(),
            Enc::DeltaByteArray => "DELTA_BYTE_ARRAY".into(),
            Enc::RleDictionary => "RLE_DICTIONARY".into(),
            Enc::ByteStreamSplit => "BYTE_STREAM_SPLIT".into(),
            Enc::Unknown(v) => format!("encoding #{v}"),
        }
    }
}

pub(crate) enum PageKind {
    /// v1 data page.
    Data { num_values: usize, encoding: Enc, def_encoding: Enc },
    Dict { num_values: usize, encoding: Enc },
    /// Index pages are skipped, not decoded.
    Skip,
}

pub(crate) struct PageHead {
    pub kind: PageKind,
    pub uncompressed_size: usize,
    pub compressed_size: usize,
    /// Offset of the first payload byte inside the chunk buffer.
    pub payload_at: usize,
}

/// Parse one page header at `at` inside `chunk`; the header's thrift bytes
/// are self-delimiting (fields until STOP).
pub(crate) fn parse_page_header(chunk: &[u8], at: usize, column: &str) -> PgResult<PageHead> {
    let Some(rest) = chunk.get(at..) else {
        return Err(corrupt("page header offset beyond chunk"));
    };
    let mut cur = Cur::new(rest);

    let mut page_type: Option<i32> = None;
    let mut uncompressed: Option<i32> = None;
    let mut compressed: Option<i32> = None;
    let mut data_hdr: Option<(usize, Enc, Enc)> = None;
    let mut dict_hdr: Option<(usize, Enc)> = None;
    let mut saw_v2_hdr = false;

    let mut last_id = 0i16;
    while let Some((t, id)) = cur.field(&mut last_id)? {
        match id {
            1 => page_type = Some(cur.i32_value(t)?),
            2 => uncompressed = Some(cur.i32_value(t)?),
            3 => compressed = Some(cur.i32_value(t)?),
            5 => {
                if t != T_STRUCT {
                    return Err(corrupt("data page header is not a struct"));
                }
                let mut num_values: Option<i32> = None;
                let mut enc: Enc = Enc::Plain;
                let mut def_enc: Enc = Enc::Rle;
                let mut lid = 0i16;
                while let Some((ft, fid)) = cur.field(&mut lid)? {
                    match fid {
                        1 => num_values = Some(cur.i32_value(ft)?),
                        2 => enc = Enc::from_i32(cur.i32_value(ft)?),
                        3 => def_enc = Enc::from_i32(cur.i32_value(ft)?),
                        4 => {
                            // Repetition-level encoding: flat columns carry
                            // zero rep-level bytes; value recorded, unused.
                            if ft == T_I32 {
                                let _ = cur.zig_i32()?;
                            } else {
                                cur.skip(ft, 0)?;
                            }
                        }
                        _ => cur.skip(ft, 0)?, // statistics etc.
                    }
                }
                let n = num_values.ok_or_else(|| corrupt("data page without num_values"))?;
                let n = usize::try_from(n)
                    .map_err(|_| corrupt("negative data page num_values"))?;
                data_hdr = Some((n, enc, def_enc));
            }
            7 => {
                if t != T_STRUCT {
                    return Err(corrupt("dictionary page header is not a struct"));
                }
                let mut num_values: Option<i32> = None;
                let mut enc: Enc = Enc::Plain;
                let mut lid = 0i16;
                while let Some((ft, fid)) = cur.field(&mut lid)? {
                    match fid {
                        1 => num_values = Some(cur.i32_value(ft)?),
                        2 => enc = Enc::from_i32(cur.i32_value(ft)?),
                        _ => cur.skip(ft, 0)?,
                    }
                }
                let n =
                    num_values.ok_or_else(|| corrupt("dictionary page without num_values"))?;
                let n = usize::try_from(n)
                    .map_err(|_| corrupt("negative dictionary num_values"))?;
                dict_hdr = Some((n, enc));
            }
            8 => {
                cur.skip(t, 0)?;
                saw_v2_hdr = true;
            }
            _ => cur.skip(t, 0)?,
        }
    }

    let (Some(page_type), Some(uncompressed), Some(compressed)) =
        (page_type, uncompressed, compressed)
    else {
        return Err(corrupt("page header missing type or sizes"));
    };
    if uncompressed < 0 || compressed < 0 {
        return Err(corrupt("negative page sizes"));
    }
    let payload_at = at + cur.pos();
    if payload_at + compressed as usize > chunk.len() {
        return Err(corrupt("page payload overruns chunk"));
    }

    let kind = match page_type {
        0 => {
            let (num_values, encoding, def_encoding) =
                data_hdr.ok_or_else(|| corrupt("data page without its header"))?;
            PageKind::Data { num_values, encoding, def_encoding }
        }
        1 => PageKind::Skip, // index page: skip payload, keep walking
        2 => {
            let (num_values, encoding) =
                dict_hdr.ok_or_else(|| corrupt("dictionary page without its header"))?;
            PageKind::Dict { num_values, encoding }
        }
        3 => {
            let _ = saw_v2_hdr;
            return Err(unsupported(format!(
                "data page v2 (column \"{column}\")"
            )));
        }
        _ => return Err(corrupt("unrecognized page type")),
    };

    Ok(PageHead {
        kind,
        uncompressed_size: uncompressed as usize,
        compressed_size: compressed as usize,
        payload_at,
    })
}
