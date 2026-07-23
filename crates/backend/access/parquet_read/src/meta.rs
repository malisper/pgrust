//! Parquet footer (FileMetaData) skip-parse: schema + per-chunk
//! {file offsets, codec, value counts, dict-page offset}. Statistics,
//! page-index references, encoding stats and key/value metadata are
//! skipped byte-wise, never decoded.

use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};

use crate::thrift::{corrupt, Cur, T_BINARY, T_BYTE, T_LIST, T_STRUCT};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Phys {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
    /// FIXED_LEN_BYTE_ARRAY(type_length).
    Flba(u32),
}

impl Phys {
    pub fn name(self) -> &'static str {
        match self {
            Phys::Boolean => "BOOLEAN",
            Phys::Int32 => "INT32",
            Phys::Int64 => "INT64",
            Phys::Int96 => "INT96",
            Phys::Float => "FLOAT",
            Phys::Double => "DOUBLE",
            Phys::ByteArray => "BYTE_ARRAY",
            Phys::Flba(_) => "FIXED_LEN_BYTE_ARRAY",
        }
    }

    /// Fixed byte width of PLAIN-encoded values, `None` for BYTE_ARRAY.
    pub fn plain_width(self) -> Option<usize> {
        match self {
            Phys::Boolean => None, // bit-packed, handled separately
            Phys::Int32 | Phys::Float => Some(4),
            Phys::Int64 | Phys::Double => Some(8),
            Phys::Int96 => Some(12),
            Phys::ByteArray => None,
            Phys::Flba(n) => Some(n as usize),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TimeUnit {
    Millis,
    Micros,
    Nanos,
}

/// The logical annotation the reader acts on. LogicalType (schema field 10)
/// wins over the legacy ConvertedType (field 6) when both are present.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Logical {
    None,
    String,
    Enum,
    Json,
    Bson,
    Uuid,
    Date,
    Time { unit: TimeUnit },
    Timestamp { unit: TimeUnit, utc: bool },
    Int { bits: u8, signed: bool },
    Decimal { precision: i32, scale: i32 },
    Interval,
    Float16,
    /// Logical UNKNOWN (always-null) or an annotation this reader has no
    /// mapping for; binding decides whether that matters.
    Other,
}

impl Logical {
    pub fn name(self) -> &'static str {
        match self {
            Logical::None => "",
            Logical::String => "STRING",
            Logical::Enum => "ENUM",
            Logical::Json => "JSON",
            Logical::Bson => "BSON",
            Logical::Uuid => "UUID",
            Logical::Date => "DATE",
            Logical::Time { .. } => "TIME",
            Logical::Timestamp { .. } => "TIMESTAMP",
            Logical::Int { .. } => "INT",
            Logical::Decimal { .. } => "DECIMAL",
            Logical::Interval => "INTERVAL",
            Logical::Float16 => "FLOAT16",
            Logical::Other => "UNKNOWN",
        }
    }
}

/// One flat (root-child leaf) column of the file schema.
pub struct ColumnSchema {
    pub name: String,
    pub phys: Phys,
    /// 0 = REQUIRED, 1 = OPTIONAL. Deeper nesting is refused at open.
    pub max_def: u16,
    pub logical: Logical,
}

impl ColumnSchema {
    /// "INT64 (TIMESTAMP)"-style description for schema-mismatch errors.
    pub fn type_desc(&self) -> String {
        if self.logical == Logical::None {
            self.phys.name().to_string()
        } else {
            format!("{} ({})", self.phys.name(), self.logical.name())
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CodecId {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    /// Legacy Hadoop-framed LZ4 (codec 5) — refused with a clean error.
    Lz4Framed,
    Zstd,
    Lz4Raw,
}

impl CodecId {
    fn from_i32(v: i32) -> PgResult<CodecId> {
        Ok(match v {
            0 => CodecId::Uncompressed,
            1 => CodecId::Snappy,
            2 => CodecId::Gzip,
            3 => CodecId::Lzo,
            4 => CodecId::Brotli,
            5 => CodecId::Lz4Framed,
            6 => CodecId::Zstd,
            7 => CodecId::Lz4Raw,
            _ => return Err(corrupt("unrecognized compression codec")),
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            CodecId::Uncompressed => "UNCOMPRESSED",
            CodecId::Snappy => "SNAPPY",
            CodecId::Gzip => "GZIP",
            CodecId::Lzo => "LZO",
            CodecId::Brotli => "BROTLI",
            CodecId::Lz4Framed => "LZ4 (Hadoop-framed)",
            CodecId::Zstd => "ZSTD",
            CodecId::Lz4Raw => "LZ4_RAW",
        }
    }
}

/// Per column chunk: exactly what the page walker needs, nothing else.
pub struct ChunkMeta {
    /// Ordinal into `FileMeta::columns` (resolved via path_in_schema).
    pub column: usize,
    pub codec: CodecId,
    pub num_values: i64,
    pub data_page_offset: i64,
    /// Some writers leave this unset while still writing a dictionary page
    /// at the chunk start; the page walker keys off page headers, not this.
    pub dict_page_offset: Option<i64>,
    pub total_compressed_size: i64,
}

impl ChunkMeta {
    /// First byte of the chunk in the file. A set-but-larger
    /// dictionary_page_offset (a known writer quirk) is ignored.
    pub fn start_offset(&self) -> i64 {
        match self.dict_page_offset {
            Some(d) if d > 0 && d < self.data_page_offset => d,
            _ => self.data_page_offset,
        }
    }
}

pub struct RowGroupMeta {
    pub num_rows: i64,
    pub chunks: Vec<ChunkMeta>,
}

pub struct FileMeta {
    pub num_rows: i64,
    pub columns: Vec<ColumnSchema>,
    pub row_groups: Vec<RowGroupMeta>,
    pub created_by: Option<String>,
}

#[cold]
#[inline(never)]
pub(crate) fn unsupported(what: String) -> Box<PgError> {
    Box::new(
        PgError::error(format!("unsupported parquet feature: {what}"))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

/// Parse FileMetaData from the footer bytes (between the leading magic and
/// the trailing 8-byte length+magic).
pub fn parse_file_meta(buf: &[u8]) -> PgResult<FileMeta> {
    let mut cur = Cur::new(buf);
    let mut version: Option<i32> = None;
    let mut num_rows: Option<i64> = None;
    let mut schema: Option<Vec<ColumnSchema>> = None;
    let mut row_groups: Option<Vec<RowGroupMeta>> = None;
    let mut created_by: Option<String> = None;

    let mut last_id = 0i16;
    while let Some((t, id)) = cur.field(&mut last_id)? {
        match id {
            1 => version = Some(cur.i32_value(t)?),
            2 => {
                if t != T_LIST {
                    return Err(corrupt("schema is not a list"));
                }
                schema = Some(parse_schema(&mut cur)?);
            }
            3 => num_rows = Some(cur.i64_value(t)?),
            4 => {
                if t != T_LIST {
                    return Err(corrupt("row_groups is not a list"));
                }
                let (et, n) = cur.list_header()?;
                if et != T_STRUCT {
                    return Err(corrupt("row_groups element is not a struct"));
                }
                let ncols = schema
                    .as_ref()
                    .ok_or_else(|| corrupt("row_groups precede schema"))?
                    .len();
                let mut rgs = Vec::new();
                rgs.try_reserve(n).map_err(|_| corrupt("row group list too large"))?;
                for _ in 0..n {
                    rgs.push(parse_row_group(&mut cur, schema.as_ref().unwrap(), ncols)?);
                }
                row_groups = Some(rgs);
            }
            6 => {
                created_by =
                    Some(String::from_utf8_lossy(cur.binary_value(t)?).into_owned());
            }
            8 => {
                // EncryptionAlgorithm on a plaintext-footer file: columns are
                // encrypted even though the footer is readable.
                return Err(unsupported("encrypted columns".into()));
            }
            _ => cur.skip(t, 0)?,
        }
    }

    let _ = version;
    let (Some(columns), Some(num_rows)) = (schema, num_rows) else {
        return Err(corrupt("FileMetaData missing schema or num_rows"));
    };
    let row_groups = row_groups.unwrap_or_default();
    if num_rows < 0 {
        return Err(corrupt("negative num_rows"));
    }
    let rg_rows: i64 = row_groups.iter().map(|rg| rg.num_rows).try_fold(0i64, |a, b| {
        a.checked_add(b).ok_or_else(|| corrupt("row counts overflow"))
    })?;
    if rg_rows != num_rows {
        return Err(corrupt("row group row counts do not sum to num_rows"));
    }
    Ok(FileMeta { num_rows, columns, row_groups, created_by })
}

// SchemaElement field ids.
const SE_TYPE: i16 = 1;
const SE_TYPE_LENGTH: i16 = 2;
const SE_REPETITION: i16 = 3;
const SE_NAME: i16 = 4;
const SE_NUM_CHILDREN: i16 = 5;
const SE_CONVERTED: i16 = 6;
const SE_SCALE: i16 = 7;
const SE_PRECISION: i16 = 8;
const SE_LOGICAL: i16 = 10;

struct SchemaElem {
    name: String,
    phys: Option<Phys>,
    repetition: i32,
    num_children: usize,
    logical: Logical,
}

/// Parse the flattened schema-element list into the flat-column form this
/// reader supports: every root child must be a leaf (no groups, no REPEATED
/// fields). Nested schemas are refused with a clean error naming the column.
fn parse_schema(cur: &mut Cur<'_>) -> PgResult<Vec<ColumnSchema>> {
    let (et, n) = cur.list_header()?;
    if et != T_STRUCT {
        return Err(corrupt("schema element is not a struct"));
    }
    if n == 0 {
        return Err(corrupt("empty schema"));
    }
    let root = parse_schema_element(cur)?;
    let ncols = root.num_children;
    if ncols == 0 {
        return Err(corrupt("schema root has no columns"));
    }
    if n != ncols + 1 {
        // More elements than root children implies nesting somewhere; find
        // the offending child below for a named error, or fail structurally.
        if n < ncols + 1 {
            return Err(corrupt("schema list shorter than root child count"));
        }
    }
    let mut cols = Vec::new();
    cols.try_reserve(ncols).map_err(|_| corrupt("schema too large"))?;
    let mut remaining = n - 1;
    for _ in 0..ncols {
        if remaining == 0 {
            return Err(corrupt("schema list shorter than root child count"));
        }
        let el = parse_schema_element(cur)?;
        remaining -= 1;
        if el.num_children > 0 {
            return Err(unsupported(format!(
                "nested column \"{}\" (group schemas are not supported yet)",
                el.name
            )));
        }
        let Some(phys) = el.phys else {
            return Err(corrupt("leaf schema element without a physical type"));
        };
        let max_def = match el.repetition {
            0 => 0u16, // REQUIRED
            1 => 1u16, // OPTIONAL
            2 => {
                return Err(unsupported(format!(
                    "repeated column \"{}\" (nesting is not supported yet)",
                    el.name
                )))
            }
            _ => return Err(corrupt("invalid repetition type")),
        };
        cols.push(ColumnSchema { name: el.name, phys, max_def, logical: el.logical });
    }
    if remaining != 0 {
        return Err(corrupt("schema list longer than root child count"));
    }
    Ok(cols)
}

fn parse_schema_element(cur: &mut Cur<'_>) -> PgResult<SchemaElem> {
    let mut name: Option<String> = None;
    let mut phys_code: Option<i32> = None;
    let mut type_length: Option<i32> = None;
    let mut repetition: i32 = 0;
    let mut num_children: usize = 0;
    let mut converted: Option<i32> = None;
    let mut scale: Option<i32> = None;
    let mut precision: Option<i32> = None;
    let mut logical: Option<Logical> = None;

    let mut last_id = 0i16;
    while let Some((t, id)) = cur.field(&mut last_id)? {
        match id {
            SE_TYPE => phys_code = Some(cur.i32_value(t)?),
            SE_TYPE_LENGTH => type_length = Some(cur.i32_value(t)?),
            SE_REPETITION => repetition = cur.i32_value(t)?,
            SE_NAME => {
                name = Some(
                    String::from_utf8_lossy(cur.binary_value(t)?).into_owned(),
                );
            }
            SE_NUM_CHILDREN => {
                let n = cur.i32_value(t)?;
                num_children =
                    usize::try_from(n).map_err(|_| corrupt("negative num_children"))?;
            }
            SE_CONVERTED => converted = Some(cur.i32_value(t)?),
            SE_SCALE => scale = Some(cur.i32_value(t)?),
            SE_PRECISION => precision = Some(cur.i32_value(t)?),
            SE_LOGICAL => {
                if t != T_STRUCT {
                    return Err(corrupt("logicalType is not a struct"));
                }
                logical = Some(parse_logical_type(cur)?);
            }
            _ => cur.skip(t, 0)?,
        }
    }

    let name = name.ok_or_else(|| corrupt("schema element without a name"))?;
    let phys = match phys_code {
        None => None,
        Some(0) => Some(Phys::Boolean),
        Some(1) => Some(Phys::Int32),
        Some(2) => Some(Phys::Int64),
        Some(3) => Some(Phys::Int96),
        Some(4) => Some(Phys::Float),
        Some(5) => Some(Phys::Double),
        Some(6) => Some(Phys::ByteArray),
        Some(7) => {
            let n = type_length.unwrap_or(-1);
            if n <= 0 {
                return Err(corrupt("FIXED_LEN_BYTE_ARRAY without a positive type_length"));
            }
            Some(Phys::Flba(n as u32))
        }
        Some(_) => return Err(corrupt("unrecognized physical type")),
    };
    // LogicalType wins; ConvertedType is the legacy fallback.
    let logical = match logical {
        Some(l) => l,
        None => converted_to_logical(converted, scale, precision),
    };
    Ok(SchemaElem { name, phys, repetition, num_children, logical })
}

fn converted_to_logical(
    converted: Option<i32>,
    scale: Option<i32>,
    precision: Option<i32>,
) -> Logical {
    match converted {
        None => Logical::None,
        Some(0) => Logical::String,                       // UTF8
        Some(4) => Logical::Enum,                         // ENUM
        Some(5) => Logical::Decimal {
            precision: precision.unwrap_or(0),
            scale: scale.unwrap_or(0),
        },
        Some(6) => Logical::Date,                         // DATE
        Some(7) => Logical::Time { unit: TimeUnit::Millis }, // TIME_MILLIS
        Some(8) => Logical::Time { unit: TimeUnit::Micros }, // TIME_MICROS
        // Legacy converted timestamps carry no UTC flag; instant semantics
        // (adjusted to UTC) is the convention every writer follows.
        Some(9) => Logical::Timestamp { unit: TimeUnit::Millis, utc: true },
        Some(10) => Logical::Timestamp { unit: TimeUnit::Micros, utc: true },
        Some(11) => Logical::Int { bits: 8, signed: false },  // UINT_8
        Some(12) => Logical::Int { bits: 16, signed: false }, // UINT_16
        Some(13) => Logical::Int { bits: 32, signed: false }, // UINT_32
        Some(14) => Logical::Int { bits: 64, signed: false }, // UINT_64
        Some(15) => Logical::Int { bits: 8, signed: true },   // INT_8
        Some(16) => Logical::Int { bits: 16, signed: true },  // INT_16
        Some(17) => Logical::Int { bits: 32, signed: true },  // INT_32
        Some(18) => Logical::Int { bits: 64, signed: true },  // INT_64
        Some(19) => Logical::Json,                            // JSON
        Some(20) => Logical::Bson,                            // BSON
        Some(21) => Logical::Interval,                        // INTERVAL
        Some(_) => Logical::Other,
    }
}

/// LogicalType union: one field set, keyed by field id.
fn parse_logical_type(cur: &mut Cur<'_>) -> PgResult<Logical> {
    let mut out = Logical::Other;
    let mut last_id = 0i16;
    while let Some((t, id)) = cur.field(&mut last_id)? {
        match id {
            1 => {
                cur.skip(t, 0)?;
                out = Logical::String;
            }
            4 => {
                cur.skip(t, 0)?;
                out = Logical::Enum;
            }
            5 => {
                // DecimalType { 1: scale, 2: precision }
                let (mut scale, mut precision) = (0i32, 0i32);
                let mut lid = 0i16;
                while let Some((ft, fid)) = cur.field(&mut lid)? {
                    match fid {
                        1 => scale = cur.i32_value(ft)?,
                        2 => precision = cur.i32_value(ft)?,
                        _ => cur.skip(ft, 0)?,
                    }
                }
                out = Logical::Decimal { precision, scale };
            }
            6 => {
                cur.skip(t, 0)?;
                out = Logical::Date;
            }
            7 => {
                let (_utc, unit) = parse_time_struct(cur)?;
                out = Logical::Time { unit };
            }
            8 => {
                let (utc, unit) = parse_time_struct(cur)?;
                out = Logical::Timestamp { unit, utc };
            }
            10 => {
                // IntType { 1: bitWidth (byte), 2: isSigned (bool) }
                let (mut bits, mut signed) = (0u8, false);
                let mut lid = 0i16;
                while let Some((ft, fid)) = cur.field(&mut lid)? {
                    match fid {
                        1 => {
                            if ft != T_BYTE {
                                return Err(corrupt("IntType bitWidth is not a byte"));
                            }
                            bits = cur.bytes(1)?[0];
                        }
                        2 => signed = cur.bool_value(ft)?,
                        _ => cur.skip(ft, 0)?,
                    }
                }
                out = Logical::Int { bits, signed };
            }
            12 => {
                cur.skip(t, 0)?;
                out = Logical::Json;
            }
            13 => {
                cur.skip(t, 0)?;
                out = Logical::Bson;
            }
            14 => {
                cur.skip(t, 0)?;
                out = Logical::Uuid;
            }
            15 => {
                cur.skip(t, 0)?;
                out = Logical::Float16;
            }
            _ => {
                cur.skip(t, 0)?;
                // MAP(2)/LIST(3) never annotate a leaf; UNKNOWN(11) and
                // anything newer fold to Other.
            }
        }
    }
    Ok(out)
}

/// TimeType/TimestampType { 1: isAdjustedToUTC, 2: unit (TimeUnit union) }.
fn parse_time_struct(cur: &mut Cur<'_>) -> PgResult<(bool, TimeUnit)> {
    let mut utc = false;
    let mut unit = TimeUnit::Millis;
    let mut last_id = 0i16;
    while let Some((t, id)) = cur.field(&mut last_id)? {
        match id {
            1 => utc = cur.bool_value(t)?,
            2 => {
                if t != T_STRUCT {
                    return Err(corrupt("TimeUnit is not a struct"));
                }
                let mut lid = 0i16;
                while let Some((ut, uid)) = cur.field(&mut lid)? {
                    match uid {
                        1 => unit = TimeUnit::Millis,
                        2 => unit = TimeUnit::Micros,
                        3 => unit = TimeUnit::Nanos,
                        _ => {}
                    }
                    cur.skip(ut, 0)?;
                }
            }
            _ => cur.skip(t, 0)?,
        }
    }
    Ok((utc, unit))
}

// RowGroup field ids.
const RG_COLUMNS: i16 = 1;
const RG_NUM_ROWS: i16 = 3;

fn parse_row_group(
    cur: &mut Cur<'_>,
    schema: &[ColumnSchema],
    ncols: usize,
) -> PgResult<RowGroupMeta> {
    let mut num_rows: Option<i64> = None;
    let mut chunks: Option<Vec<ChunkMeta>> = None;

    let mut last_id = 0i16;
    while let Some((t, id)) = cur.field(&mut last_id)? {
        match id {
            RG_COLUMNS => {
                if t != T_LIST {
                    return Err(corrupt("row group columns is not a list"));
                }
                let (et, n) = cur.list_header()?;
                if et != T_STRUCT {
                    return Err(corrupt("column chunk is not a struct"));
                }
                if n != ncols {
                    return Err(corrupt("row group chunk count does not match schema"));
                }
                let mut v = Vec::new();
                v.try_reserve(n).map_err(|_| corrupt("chunk list too large"))?;
                for i in 0..n {
                    v.push(parse_column_chunk(cur, schema, i)?);
                }
                chunks = Some(v);
            }
            RG_NUM_ROWS => num_rows = Some(cur.i64_value(t)?),
            _ => cur.skip(t, 0)?,
        }
    }
    let (Some(chunks), Some(num_rows)) = (chunks, num_rows) else {
        return Err(corrupt("row group missing columns or num_rows"));
    };
    if num_rows < 0 {
        return Err(corrupt("negative row group num_rows"));
    }
    Ok(RowGroupMeta { num_rows, chunks })
}

// ColumnChunk / ColumnMetaData field ids.
const CC_FILE_PATH: i16 = 1;
const CC_META: i16 = 3;
const CC_CRYPTO: i16 = 8;
const CM_ENCODINGS: i16 = 2;
const CM_PATH: i16 = 3;
const CM_CODEC: i16 = 4;
const CM_NUM_VALUES: i16 = 5;
const CM_TOTAL_COMPRESSED: i16 = 7;
const CM_DATA_PAGE_OFFSET: i16 = 9;
const CM_DICT_PAGE_OFFSET: i16 = 11;
const CM_STATISTICS: i16 = 12;

fn parse_column_chunk(
    cur: &mut Cur<'_>,
    schema: &[ColumnSchema],
    ordinal: usize,
) -> PgResult<ChunkMeta> {
    let mut meta: Option<ChunkMeta> = None;
    let mut last_id = 0i16;
    while let Some((t, id)) = cur.field(&mut last_id)? {
        match id {
            CC_FILE_PATH => {
                let p = cur.binary_value(t)?;
                return Err(unsupported(format!(
                    "column chunk stored in external file \"{}\"",
                    String::from_utf8_lossy(p)
                )));
            }
            CC_META => {
                if t != T_STRUCT {
                    return Err(corrupt("column metadata is not a struct"));
                }
                meta = Some(parse_column_meta(cur, schema, ordinal)?);
            }
            CC_CRYPTO => return Err(unsupported("encrypted columns".into())),
            _ => cur.skip(t, 0)?,
        }
    }
    meta.ok_or_else(|| corrupt("column chunk without metadata"))
}

fn parse_column_meta(
    cur: &mut Cur<'_>,
    schema: &[ColumnSchema],
    ordinal: usize,
) -> PgResult<ChunkMeta> {
    let mut column: Option<usize> = None;
    let mut codec: Option<CodecId> = None;
    let mut num_values: Option<i64> = None;
    let mut total_compressed: Option<i64> = None;
    let mut data_page_offset: Option<i64> = None;
    let mut dict_page_offset: Option<i64> = None;

    let mut last_id = 0i16;
    while let Some((t, id)) = cur.field(&mut last_id)? {
        match id {
            CM_ENCODINGS => {
                // Page headers carry the authoritative per-page encoding
                // (dict->PLAIN fallback happens mid-chunk); skip the list.
                cur.skip(t, 0)?;
            }
            CM_PATH => {
                if t != T_LIST {
                    return Err(corrupt("path_in_schema is not a list"));
                }
                let (et, n) = cur.list_header()?;
                if et != T_BINARY {
                    return Err(corrupt("path_in_schema element is not a string"));
                }
                if n != 1 {
                    return Err(unsupported(
                        "nested column path in column metadata".into(),
                    ));
                }
                let name = cur.binary()?;
                // Chunks appear in schema-leaf order in every known writer;
                // verify, and fall back to a name search if a writer ever
                // permutes them.
                if schema.get(ordinal).is_some_and(|c| c.name.as_bytes() == name) {
                    column = Some(ordinal);
                } else {
                    column = schema.iter().position(|c| c.name.as_bytes() == name);
                    if column.is_none() {
                        return Err(corrupt("column chunk path not found in schema"));
                    }
                }
            }
            CM_CODEC => codec = Some(CodecId::from_i32(cur.i32_value(t)?)?),
            CM_NUM_VALUES => num_values = Some(cur.i64_value(t)?),
            CM_TOTAL_COMPRESSED => total_compressed = Some(cur.i64_value(t)?),
            CM_DATA_PAGE_OFFSET => data_page_offset = Some(cur.i64_value(t)?),
            CM_DICT_PAGE_OFFSET => dict_page_offset = Some(cur.i64_value(t)?),
            CM_STATISTICS => cur.skip(t, 0)?, // never decoded
            _ => cur.skip(t, 0)?,
        }
    }

    let (Some(codec), Some(num_values), Some(total_compressed), Some(data_page_offset)) =
        (codec, num_values, total_compressed, data_page_offset)
    else {
        return Err(corrupt("column metadata missing required fields"));
    };
    if num_values < 0 || total_compressed < 0 || data_page_offset < 0 {
        return Err(corrupt("negative column metadata offsets or counts"));
    }
    Ok(ChunkMeta {
        column: column.unwrap_or(ordinal),
        codec,
        num_values,
        data_page_offset,
        dict_page_offset,
        total_compressed_size: total_compressed,
    })
}
