// COPY ... FROM 'file' WITH (FORMAT 'parquet') — the generic row-producing
// path: parquet column batches -> datums -> the existing COPY insert path,
// so it loads into ANY table AM. pg_parquet (the behavioral spec of record
// for this surface) drives the option names (match_by position|name), the
// schema-matching error classes and the type-mapping table; this increment
// implements the flat-column read side and errors cleanly on the rest.
//
// The columnar-store fast path (dictionary remap into part builders, sorted
// runs into the parallel-load merge) is a later increment; parquet_read's
// batch API already separates page decode from batch fill for it.

use datum::Datum;
use mcx::Mcx;
use parquet_read::{BatchData, ColumnBatch, FileReader, Logical, Phys, RowGroupReader, TimeUnit};
use types_core::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_BAD_COPY_FILE_FORMAT, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_DATETIME_VALUE_OUT_OF_RANGE, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_STRING_DATA_RIGHT_TRUNCATION,
    ERRCODE_UNDEFINED_COLUMN,
};
use types_tuple::TupleDescData;

use crate::from::CopyFromState;

// Microseconds from the Unix epoch to the engine timestamp epoch (2000-01-01).
const EPOCH_SHIFT_US: i64 = 946_684_800_000_000;
// The same shift in whole seconds (epoch-coercion input unit).
const EPOCH_SHIFT_SECS: i64 = 946_684_800;
// Days from the Unix epoch to the engine date epoch.
const EPOCH_SHIFT_DAYS: i32 = 10_957;
// Engine timestamp validity window (timestamp.h IS_VALID_TIMESTAMP).
const MIN_TIMESTAMP_US: i64 = -211_813_488_000_000_000;
const END_TIMESTAMP_US: i64 = 9_223_371_331_200_000_000;
const MAX_TIME_US: i64 = 86_400_000_000;

const VARHDRSZ_I32: i32 = 4;

/// How one bound column's decoded values become datums.
#[derive(Clone, Copy)]
pub(crate) enum Conv {
    Bool,
    I16FromI32,
    I32,
    I64FromI32,
    I16FromI64,
    I32FromI64,
    I64,
    F32,
    F64FromF32,
    F64,
    DateFromDays,
    /// Timestamp/timestamptz target; value scaled to microseconds.
    Timestamp(TimeUnit),
    Time(TimeUnit),
    /// Unsigned annotations: zero-extend / reinterpret with range checks.
    U32AsI32,
    U32AsI64,
    U64AsI64,
    /// COERCE_EPOCH: plain-integer column read as Unix epoch seconds into a
    /// timestamp/timestamptz target.
    TsFromEpochSecs(EpochLane),
    /// COERCE_EPOCH: plain-integer column read as Unix epoch days into a
    /// date target.
    DateFromEpochDays(EpochLane),
    /// text / unconstrained varchar (UTF-8 validated at page decode).
    Text,
    /// varchar(n): char-length check with the blank-trim rule.
    VarcharN(i32),
    /// bpchar: the varchar rule + space padding to exactly n chars
    /// (-1 = unconstrained, stored as-is).
    Bpchar(i32),
    Bytea,
}

/// How an epoch-coerced integer lane widens to a signed 64-bit epoch number
/// (same widening rules as the plain-integer target mappings).
#[derive(Clone, Copy)]
pub(crate) enum EpochLane {
    /// INT32, signed annotation or none.
    I32,
    /// INT32 with an unsigned annotation: zero-extend.
    U32,
    /// INT64, signed / narrow-unsigned annotation or none.
    I64,
    /// INT64 annotated UINT_64: sign-bit values are out of range.
    U64,
}

pub(crate) struct Binding {
    /// 0-based physical attribute index in the target tuple descriptor.
    pub(crate) attidx: usize,
    pub(crate) conv: Conv,
    /// Dense index into the per-binding batch array.
    pub(crate) batch: usize,
}

/// The resolved schema-match + conversion plan: shareable across parallel
/// decode workers (plain data).
pub(crate) struct ParquetPlan {
    pub(crate) bindings: Vec<Binding>,
    /// Parquet schema ordinals + validation flags, in binding order (the
    /// row-group open feed).
    pub(crate) cols: Vec<usize>,
    pub(crate) vutf8: Vec<bool>,
}

impl ParquetPlan {
    pub(crate) fn make_batches(&self, meta: &parquet_read::FileMeta) -> Vec<ColumnBatch> {
        self.cols.iter().map(|&c| ColumnBatch::new_for(meta.columns[c].phys)).collect()
    }
}

pub(crate) struct ParquetSrc {
    reader: FileReader,
    plan: std::sync::Arc<ParquetPlan>,
    batches: Vec<ColumnBatch>,
    batch_len: usize,
    batch_pos: usize,
    rg_idx: usize,
    rg: Option<RowGroupReader>,
}

const BATCH_ROWS: usize = 1024;

#[track_caller]
#[cold]
#[inline(never)]
fn type_mismatch(attname: &str, pg_type: &str, pq_type: &str) -> Box<PgError> {
    // pg_parquet's error class for an uncoercible column.
    Box::new(
        PgError::error(format!(
            "type mismatch for column \"{attname}\" between table and parquet file"
        ))
        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
        .with_detail(format!("table has \"{pg_type}\", parquet file has \"{pq_type}\"")),
    )
}

/// Resolve the conversion for one (parquet column, table attribute) pair.
/// The mapped set is pg_parquet's read-side table restricted to flat
/// columns; anything else is a clean type-mismatch error.
fn resolve_conv(
    schema: &parquet_read::ColumnSchema,
    atttypid: Oid,
    atttypmod: i32,
    attname: &str,
    coerce_epoch: bool,
) -> PgResult<Conv> {
    use types_core::catalog::*;
    let phys = schema.phys;
    let logical = schema.logical;
    let plain_int = |l: Logical, bits: u8| -> bool {
        match l {
            Logical::None => true,
            Logical::Int { bits: b, signed: true } => b <= bits,
            _ => false,
        }
    };
    // Unsigned annotations up to `bits` wide: values are zero-extended into
    // the physical int, so a signed read of a STRICTLY NARROWER unsigned
    // width is always non-negative and in range.
    let small_uint = |l: Logical, bits: u8| -> bool {
        matches!(l, Logical::Int { bits: b, signed: false } if b < bits)
    };
    let textual = |l: Logical| {
        matches!(l, Logical::None | Logical::String | Logical::Enum | Logical::Json)
    };
    // COERCE_EPOCH admits exactly the plain-integer annotation classes (no
    // temporal/other logical); annotated temporal columns keep their mapping.
    let plain_integer = |l: Logical| matches!(l, Logical::None | Logical::Int { .. });
    let lane32 = |l: Logical| match l {
        Logical::Int { signed: false, .. } => EpochLane::U32,
        _ => EpochLane::I32,
    };
    let lane64 = |l: Logical| match l {
        Logical::Int { bits: 64, signed: false } => EpochLane::U64,
        _ => EpochLane::I64,
    };
    let conv = match (phys, atttypid) {
        (Phys::Boolean, BOOLOID) if logical == Logical::None => Some(Conv::Bool),
        (Phys::Int32, INT2OID) if plain_int(logical, 16) => Some(Conv::I16FromI32),
        (Phys::Int32, INT4OID) if plain_int(logical, 32) => Some(Conv::I32),
        (Phys::Int32, INT8OID) if plain_int(logical, 32) => Some(Conv::I64FromI32),
        (Phys::Int64, INT2OID) if plain_int(logical, 64) => Some(Conv::I16FromI64),
        (Phys::Int64, INT4OID) if plain_int(logical, 64) => Some(Conv::I32FromI64),
        (Phys::Int64, INT8OID) if plain_int(logical, 64) => Some(Conv::I64),
        // Unsigned annotations (range-checked; UINT_16 day-number columns
        // are the workload-class instance).
        (Phys::Int32, INT2OID) if small_uint(logical, 32) => Some(Conv::I16FromI32),
        (Phys::Int32, INT4OID) if small_uint(logical, 32) => Some(Conv::I32),
        (Phys::Int32, INT8OID) if small_uint(logical, 32) => Some(Conv::I64FromI32),
        (Phys::Int32, INT4OID) if logical == (Logical::Int { bits: 32, signed: false }) => {
            Some(Conv::U32AsI32)
        }
        (Phys::Int32, INT8OID) if logical == (Logical::Int { bits: 32, signed: false }) => {
            Some(Conv::U32AsI64)
        }
        (Phys::Int64, INT2OID) if small_uint(logical, 64) => Some(Conv::I16FromI64),
        (Phys::Int64, INT4OID) if small_uint(logical, 64) => Some(Conv::I32FromI64),
        (Phys::Int64, INT8OID) if small_uint(logical, 64) => Some(Conv::I64),
        (Phys::Int64, INT8OID) if logical == (Logical::Int { bits: 64, signed: false }) => {
            Some(Conv::U64AsI64)
        }
        (Phys::Float, FLOAT4OID) if logical == Logical::None => Some(Conv::F32),
        (Phys::Float, FLOAT8OID) if logical == Logical::None => Some(Conv::F64FromF32),
        (Phys::Double, FLOAT8OID) if logical == Logical::None => Some(Conv::F64),
        (Phys::Int32, DATEOID) if logical == Logical::Date => Some(Conv::DateFromDays),
        // COERCE_EPOCH (opt-in, temporal targets only): plain-integer
        // columns read as Unix epoch days into date...
        (Phys::Int32 | Phys::Int64, DATEOID) if coerce_epoch && plain_integer(logical) => {
            Some(Conv::DateFromEpochDays(match phys {
                Phys::Int32 => lane32(logical),
                _ => lane64(logical),
            }))
        }
        (Phys::Int64, TIMESTAMPOID | TIMESTAMPTZOID) => match logical {
            // Instant vs wall-clock annotation maps to timestamptz vs
            // timestamp on the write side; reads accept either target (the
            // datum representation is the same microsecond count).
            Logical::Timestamp { unit, .. } => Some(Conv::Timestamp(unit)),
            // ...and as Unix epoch seconds into timestamp/timestamptz (the
            // annotated arm above always wins over coercion).
            _ if coerce_epoch && plain_integer(logical) => {
                Some(Conv::TsFromEpochSecs(lane64(logical)))
            }
            _ => None,
        },
        (Phys::Int32, TIMESTAMPOID | TIMESTAMPTZOID)
            if coerce_epoch && plain_integer(logical) =>
        {
            Some(Conv::TsFromEpochSecs(lane32(logical)))
        }
        (Phys::Int64, TIMEOID) => match logical {
            Logical::Time { unit } => Some(Conv::Time(unit)),
            _ => None,
        },
        (Phys::ByteArray, TEXTOID) if textual(logical) => Some(Conv::Text),
        (Phys::ByteArray, VARCHAROID) if textual(logical) => {
            if atttypmod >= VARHDRSZ_I32 {
                Some(Conv::VarcharN(atttypmod - VARHDRSZ_I32))
            } else {
                Some(Conv::Text)
            }
        }
        (Phys::ByteArray, BPCHAROID) if textual(logical) => {
            if atttypmod >= VARHDRSZ_I32 {
                Some(Conv::Bpchar(atttypmod - VARHDRSZ_I32))
            } else {
                Some(Conv::Bpchar(-1))
            }
        }
        (Phys::ByteArray, BYTEAOID) => Some(Conv::Bytea),
        _ => None,
    };
    match conv {
        Some(c) => Ok(c),
        None => {
            let pg_type = format_type::format_type_with_typemod(atttypid, atttypmod)
                .unwrap_or_else(|_| format!("type {atttypid}"));
            Err(type_mismatch(attname, &pg_type, &schema.type_desc()))
        }
    }
}

/// Whether the conversion consumes string bytes (drives page-batched UTF-8
/// validation; bytea reads raw bytes unvalidated).
fn wants_utf8(conv: Conv) -> bool {
    matches!(conv, Conv::Text | Conv::VarcharN(_) | Conv::Bpchar(_))
}

#[track_caller]
#[cold]
#[inline(never)]
fn stdin_unsupported() -> Box<PgError> {
    Box::new(
        PgError::error("COPY FROM STDIN with parquet format is not supported")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_hint("Use a server-side file path."),
    )
}

/// Build the source at BeginCopyFrom time: open + footer parse + schema
/// match (pg_parquet match_by semantics) + conversion resolution. All
/// schema-level errors surface here, before any row is read.
pub(crate) fn open_source(
    filename: Option<&str>,
    tup_desc: &TupleDescData<'_>,
    attnumlist: &[i16],
    match_by_name: bool,
    coerce_epoch: bool,
) -> PgResult<(Box<ParquetSrc>, u64)> {
    let Some(filename) = filename else {
        return Err(stdin_unsupported());
    };
    let reader = FileReader::open(filename)?;
    let plan = resolve_plan(&reader.meta, tup_desc, attnumlist, match_by_name, coerce_epoch)?;
    let batches = plan.make_batches(&reader.meta);
    let file_len = reader.file_len();
    Ok((
        Box::new(ParquetSrc {
            reader,
            plan: std::sync::Arc::new(plan),
            batches,
            batch_len: 0,
            batch_pos: 0,
            rg_idx: 0,
            rg: None,
        }),
        file_len,
    ))
}

/// Schema match (pg_parquet match_by semantics) + conversion resolution.
/// All schema-level errors surface here, before any row is read.
pub(crate) fn resolve_plan(
    meta: &parquet_read::FileMeta,
    tup_desc: &TupleDescData<'_>,
    attnumlist: &[i16],
    match_by_name: bool,
    coerce_epoch: bool,
) -> PgResult<ParquetPlan> {
    let ncols = meta.columns.len();
    if !match_by_name && ncols != attnumlist.len() {
        // pg_parquet position-matching contract.
        return Err(Box::new(
            PgError::error(format!(
                "column count mismatch between table and parquet file. parquet file has \
                 {ncols} columns, but table has {} columns",
                attnumlist.len()
            ))
            .with_sqlstate(ERRCODE_BAD_COPY_FILE_FORMAT),
        ));
    }

    let mut bindings = Vec::with_capacity(attnumlist.len());
    let mut cols = Vec::with_capacity(attnumlist.len());
    let mut vutf8 = Vec::with_capacity(attnumlist.len());
    let mut any_text = false;
    for (i, &attnum) in attnumlist.iter().enumerate() {
        let attidx = attnum as usize - 1;
        let att = tup_desc.attr(attidx);
        let attname = String::from_utf8_lossy(att.attname.name_str()).into_owned();
        let col = if match_by_name {
            match meta.columns.iter().position(|c| c.name == attname) {
                Some(c) => c,
                None => {
                    return Err(Box::new(
                        PgError::error(format!(
                            "column \"{attname}\" is not found in parquet file"
                        ))
                        .with_sqlstate(ERRCODE_UNDEFINED_COLUMN),
                    ))
                }
            }
        } else {
            i
        };
        let schema = &meta.columns[col];
        let conv = resolve_conv(schema, att.atttypid, att.atttypmod, &attname, coerce_epoch)?;
        any_text |= wants_utf8(conv);
        cols.push(col);
        vutf8.push(wants_utf8(conv));
        bindings.push(Binding { attidx, conv, batch: bindings.len() });
    }

    if any_text && mbutils::GetDatabaseEncoding() != wchar::PG_UTF8 {
        // Parquet strings are UTF-8 by definition; transcoding into other
        // database encodings is a later increment.
        return Err(Box::new(
            PgError::error(
                "COPY FROM parquet with text columns requires a UTF8 database encoding",
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    Ok(ParquetPlan { bindings, cols, vutf8 })
}

/// bpchar(n): over-length values take the varchar blank-trim rule; shorter
/// values are space-padded to exactly n characters (input-function parity).
fn bpchar_datum<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8], maxlen: i32) -> PgResult<Datum> {
    if maxlen < 0 {
        return varlena_datum(mcx, bytes);
    }
    let maxlen = maxlen as usize;
    // Char count + byte cut at maxlen chars (UTF-8 validated at decode).
    let mut chars = 0usize;
    let mut cut = bytes.len();
    for (i, &b) in bytes.iter().enumerate() {
        if (b & 0xc0) != 0x80 {
            if chars == maxlen {
                cut = i;
                break;
            }
            chars += 1;
        }
    }
    if cut < bytes.len() {
        if bytes[cut..].iter().any(|&b| b != b' ') {
            return Err(Box::new(
                PgError::error(format!("value too long for type character({maxlen})"))
                    .with_sqlstate(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
            ));
        }
        return varlena_datum(mcx, &bytes[..cut]);
    }
    if chars >= maxlen {
        return varlena_datum(mcx, bytes);
    }
    // Pad with spaces to exactly maxlen characters.
    let pad = maxlen - chars;
    let mut image = varlena::image_with_header(mcx, bytes.len() + pad)?;
    mcx::vec_append_bytes(&mut image, bytes)
        .map_err(|_| Box::new(PgError::error("out of memory building bpchar datum")))?;
    for _ in 0..pad {
        image.push(b' ');
    }
    Ok(types_fmgr::varlena_result(datum::Varlena::from_image(image)))
}

impl ParquetSrc {
    pub(crate) fn reader(&self) -> &FileReader {
        &self.reader
    }

    pub(crate) fn plan_arc(&self) -> std::sync::Arc<ParquetPlan> {
        std::sync::Arc::clone(&self.plan)
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn out_of_range(what: &'static str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("{what} out of range"))
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn dt_out_of_range(what: &'static str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("{what} out of range"))
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
    )
}

#[inline]
fn ts_from_unix_us(us: i64) -> PgResult<Datum> {
    let t = us.checked_sub(EPOCH_SHIFT_US).ok_or_else(|| dt_out_of_range("timestamp"))?;
    if !(MIN_TIMESTAMP_US..END_TIMESTAMP_US).contains(&t) {
        return Err(dt_out_of_range("timestamp"));
    }
    Ok(Datum::from_i64(t))
}

/// Epoch-second coercion kernel. The epoch shift is subtracted in the
/// SECONDS domain before scaling: the top of the validity window is not
/// representable as Unix-epoch microseconds in i64, so scale-then-shift
/// would spuriously overflow inside the valid range.
#[inline]
fn ts_from_unix_secs(secs: i64) -> PgResult<Datum> {
    let t = secs
        .checked_sub(EPOCH_SHIFT_SECS)
        .and_then(|s| s.checked_mul(1_000_000))
        .ok_or_else(|| dt_out_of_range("timestamp"))?;
    if !(MIN_TIMESTAMP_US..END_TIMESTAMP_US).contains(&t) {
        return Err(dt_out_of_range("timestamp"));
    }
    Ok(Datum::from_i64(t))
}

/// Epoch-day coercion kernel: shift in i64, then the i32 datum bound (the
/// same accepted set as the annotated-DATE conversion for INT32 inputs).
#[inline]
fn date_from_unix_days(days: i64) -> PgResult<Datum> {
    let d = days
        .checked_sub(i64::from(EPOCH_SHIFT_DAYS))
        .and_then(|d| i32::try_from(d).ok())
        .ok_or_else(|| dt_out_of_range("date"))?;
    Ok(Datum::from_i32(d))
}

fn varlena_datum(mcx: Mcx<'_>, bytes: &[u8]) -> PgResult<Datum> {
    Ok(types_fmgr::varlena_result(varlena::cstring_to_text(mcx, bytes)?))
}

/// varchar(n) length rule: values longer than n characters are accepted only
/// when the excess is all spaces, which is trimmed (varchar input parity).
fn varchar_datum<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8], maxlen: i32) -> PgResult<Datum> {
    let maxlen = maxlen.max(0) as usize;
    if bytes.len() <= maxlen {
        // chars <= bytes: within limit for sure.
        return varlena_datum(mcx, bytes);
    }
    // Locate the byte end of the first `maxlen` characters (UTF-8 validated
    // at decode: continuation bytes are 0b10xxxxxx).
    let mut chars = 0usize;
    let mut cut = bytes.len();
    for (i, &b) in bytes.iter().enumerate() {
        if (b & 0xc0) != 0x80 {
            if chars == maxlen {
                cut = i;
                break;
            }
            chars += 1;
        }
    }
    if cut == bytes.len() {
        // Fewer than maxlen characters despite more bytes.
        return varlena_datum(mcx, bytes);
    }
    if bytes[cut..].iter().any(|&b| b != b' ') {
        return Err(Box::new(
            PgError::error(format!(
                "value too long for type character varying({maxlen})"
            ))
            .with_sqlstate(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
        ));
    }
    varlena_datum(mcx, &bytes[..cut])
}

impl ParquetSrc {
    /// Advance to the next non-empty batch. Returns false at file end.
    fn refill(&mut self, bytes_progress: &mut u64) -> PgResult<bool> {
        loop {
            if let Some(rg) = self.rg.as_mut() {
                if rg.rows_remaining() > 0 {
                    let n = rg.rows_remaining().min(BATCH_ROWS as u64) as usize;
                    rg.read_batches(&mut self.batches, n)?;
                    self.batch_len = n;
                    self.batch_pos = 0;
                    return Ok(true);
                }
            }
            if self.rg_idx >= self.reader.meta.row_groups.len() {
                return Ok(false);
            }
            let rg = self.reader.row_group(self.rg_idx, &self.plan.cols, &self.plan.vutf8)?;
            *bytes_progress += rg.compressed_bytes;
            self.rg_idx += 1;
            self.rg = Some(rg);
        }
    }
}

impl<'mcx> CopyFromState<'mcx, '_> {
    /// One parquet row into values/nulls; false at end of file. Mirrors the
    /// binary arm's contract (defaults for unlisted columns evaluated by the
    /// caller-shared defmap loop in next_copy_from).
    pub(crate) fn copy_from_parquet_one_row(
        &mut self,
        row_mcx: Mcx<'mcx>,
        values: &mut [Datum],
        nulls: &mut [bool],
    ) -> PgResult<bool> {
        self.cur_lineno += 1;
        let crate::from::CopySrc::Parquet(src) = &mut self.src else {
            unreachable!("parquet row read on a non-parquet source");
        };
        if src.batch_pos == src.batch_len {
            let mut progressed = self.bytes_processed;
            let more = src.refill(&mut progressed)?;
            if progressed != self.bytes_processed {
                self.bytes_processed = progressed;
                backend_progress::pgstat_progress_update_param(
                    backend_progress::progress::PROGRESS_COPY_BYTES_PROCESSED,
                    self.bytes_processed as i64,
                );
            }
            if !more {
                return Ok(false);
            }
        }
        let k = src.batch_pos;
        src.batch_pos += 1;

        // Split borrows: bindings/batches are read, cur_attidx written for
        // error context.
        let ParquetSrc { plan, batches, .. } = src.as_mut();
        for b in plan.bindings.iter() {
            let batch = &batches[b.batch];
            let m = b.attidx;
            if batch.is_null(k) {
                nulls[m] = true;
                continue;
            }
            self.cur_attidx = Some(m);
            let d = convert_cell(row_mcx, b.conv, batch, k)?;
            values[m] = d;
            nulls[m] = false;
        }
        self.cur_attidx = None;
        Ok(true)
    }
}

/// Decoded cell -> datum. Cheap by-value conversions inline; the checked
/// arms error with input-function-parity messages.
pub(crate) fn convert_cell<'mcx>(
    mcx: Mcx<'mcx>,
    conv: Conv,
    batch: &ColumnBatch,
    k: usize,
) -> PgResult<Datum> {
    #[track_caller]
    #[cold]
    #[inline(never)]
    fn batch_shape() -> Box<PgError> {
        Box::new(
            PgError::error("parquet batch type does not match resolved conversion")
                .with_sqlstate(ERRCODE_BAD_COPY_FILE_FORMAT),
        )
    }
    macro_rules! lane {
        ($variant:ident) => {
            match &batch.data {
                BatchData::$variant(v) => v[k],
                _ => return Err(batch_shape()),
            }
        };
    }
    Ok(match conv {
        Conv::Bool => Datum::from_bool(lane!(Bool)),
        Conv::I32 => Datum::from_i32(lane!(I32)),
        Conv::I64 => Datum::from_i64(lane!(I64)),
        Conv::I64FromI32 => Datum::from_i64(i64::from(lane!(I32))),
        Conv::I16FromI32 => {
            let v = lane!(I32);
            let v = i16::try_from(v).map_err(|_| out_of_range("smallint"))?;
            Datum::from_i16(v)
        }
        Conv::I16FromI64 => {
            let v = lane!(I64);
            let v = i16::try_from(v).map_err(|_| out_of_range("smallint"))?;
            Datum::from_i16(v)
        }
        Conv::I32FromI64 => {
            let v = lane!(I64);
            let v = i32::try_from(v).map_err(|_| out_of_range("integer"))?;
            Datum::from_i32(v)
        }
        Conv::U32AsI32 => {
            let v = lane!(I32) as u32;
            let v = i32::try_from(v).map_err(|_| out_of_range("integer"))?;
            Datum::from_i32(v)
        }
        Conv::U32AsI64 => Datum::from_i64(i64::from(lane!(I32) as u32)),
        Conv::U64AsI64 => {
            let v = lane!(I64);
            if v < 0 {
                return Err(out_of_range("bigint"));
            }
            Datum::from_i64(v)
        }
        Conv::F32 => Datum::from_f32(lane!(F32)),
        Conv::F64 => Datum::from_f64(lane!(F64)),
        Conv::F64FromF32 => Datum::from_f64(f64::from(lane!(F32))),
        Conv::DateFromDays => {
            let v = lane!(I32);
            let d = v.checked_sub(EPOCH_SHIFT_DAYS).ok_or_else(|| dt_out_of_range("date"))?;
            Datum::from_i32(d)
        }
        Conv::Timestamp(unit) => {
            let v = lane!(I64);
            let us = match unit {
                TimeUnit::Micros => v,
                TimeUnit::Millis => {
                    v.checked_mul(1000).ok_or_else(|| dt_out_of_range("timestamp"))?
                }
                // Floor division: pre-epoch nanos round down, matching the
                // timestamp number line rather than truncation toward zero.
                TimeUnit::Nanos => v.div_euclid(1000),
            };
            ts_from_unix_us(us)?
        }
        Conv::TsFromEpochSecs(lane) => {
            let secs = match lane {
                EpochLane::I32 => i64::from(lane!(I32)),
                EpochLane::U32 => i64::from(lane!(I32) as u32),
                EpochLane::I64 => lane!(I64),
                EpochLane::U64 => {
                    let v = lane!(I64);
                    if v < 0 {
                        // Sign-bit u64 values sit far above the window.
                        return Err(dt_out_of_range("timestamp"));
                    }
                    v
                }
            };
            ts_from_unix_secs(secs)?
        }
        Conv::DateFromEpochDays(lane) => {
            let days = match lane {
                EpochLane::I32 => i64::from(lane!(I32)),
                EpochLane::U32 => i64::from(lane!(I32) as u32),
                EpochLane::I64 => lane!(I64),
                EpochLane::U64 => {
                    let v = lane!(I64);
                    if v < 0 {
                        return Err(dt_out_of_range("date"));
                    }
                    v
                }
            };
            date_from_unix_days(days)?
        }
        Conv::Time(unit) => {
            let v = lane!(I64);
            let us = match unit {
                TimeUnit::Micros => v,
                TimeUnit::Millis => v.checked_mul(1000).ok_or_else(|| dt_out_of_range("time"))?,
                TimeUnit::Nanos => v.div_euclid(1000),
            };
            if !(0..=MAX_TIME_US).contains(&us) {
                return Err(dt_out_of_range("time"));
            }
            Datum::from_i64(us)
        }
        Conv::Text => varlena_datum(mcx, batch.bytes_at(k))?,
        Conv::VarcharN(maxlen) => varchar_datum(mcx, batch.bytes_at(k), maxlen)?,
        Conv::Bpchar(maxlen) => bpchar_datum(mcx, batch.bytes_at(k), maxlen)?,
        Conv::Bytea => varlena_datum(mcx, batch.bytes_at(k))?,
    })
}

#[cfg(test)]
mod epoch_tests {
    use super::*;
    use mcx::MemoryContext;
    use parquet_read::BatchData;

    // The validity window in Unix-seconds terms, derived from the µs window
    // and the 10,957-day epoch shift (independently recomputed here so a
    // constant typo cannot self-confirm).
    const UNIX_SECS_MIN: i64 = MIN_TIMESTAMP_US / 1_000_000 + 946_684_800;
    const UNIX_SECS_MAX: i64 = END_TIMESTAMP_US / 1_000_000 + 946_684_800 - 1;

    fn ts_us(secs: i64) -> i64 {
        match ts_from_unix_secs(secs) {
            Ok(d) => {
                assert_eq!(d, Datum::from_i64((secs - 946_684_800) * 1_000_000));
                (secs - 946_684_800) * 1_000_000
            }
            Err(e) => panic!("unexpected out-of-range for {secs}: {}", e.message()),
        }
    }

    #[test]
    fn epoch_seconds_values_and_window() {
        assert_eq!(ts_us(0), -946_684_800_000_000); // 1970-01-01
        assert_eq!(ts_us(946_684_800), 0); // the engine epoch
        assert_eq!(ts_us(-1), -946_684_801_000_000); // pre-1970
        assert_eq!(ts_us(2_147_483_648), 1_200_798_848_000_000); // past 2038
        // Exact window edges (inclusive both ends in seconds terms).
        assert_eq!(UNIX_SECS_MIN, -210_866_803_200);
        assert_eq!(UNIX_SECS_MAX, 9_224_318_015_999);
        ts_us(UNIX_SECS_MIN);
        ts_us(UNIX_SECS_MAX);
        for bad in [
            UNIX_SECS_MIN - 1,
            UNIX_SECS_MAX + 1,
            9_300_000_000_000_000, // scale overflow
            i64::MAX,
            i64::MIN, // shift underflow
        ] {
            assert!(ts_from_unix_secs(bad).is_err(), "{bad} must be out of range");
        }
    }

    #[test]
    fn epoch_days_values_and_bounds() {
        let day = |d: i64| date_from_unix_days(d).unwrap();
        assert_eq!(day(0), Datum::from_i32(-10_957)); // 1970-01-01
        assert_eq!(day(10_957), Datum::from_i32(0)); // 2000-01-01
        assert_eq!(day(-1), Datum::from_i32(-10_958)); // 1969-12-31
        assert_eq!(day(2_932_896), Datum::from_i32(2_921_939)); // 9999-12-31
        assert_eq!(day(-719_162), Datum::from_i32(-730_119)); // 0001-01-01
        // The i32 datum bound after the shift, and both i64 extremes.
        assert!(date_from_unix_days(i64::from(i32::MAX) + 10_958).is_err());
        assert!(date_from_unix_days(3_000_000_000).is_err());
        assert!(date_from_unix_days(i64::MIN).is_err());
        assert_eq!(
            day(i64::from(i32::MAX) + 10_957),
            Datum::from_i32(i32::MAX)
        );
    }

    #[test]
    fn epoch_lane_widening() {
        let ctx = MemoryContext::new("epoch-lane-test");
        let mcx = ctx.mcx();
        let b32 = ColumnBatch { nulls: vec![false; 2], data: BatchData::I32(vec![-1, 86_400]) };
        let b64 =
            ColumnBatch { nulls: vec![false; 2], data: BatchData::I64(vec![-1, 4_102_444_800]) };
        // Signed i32 lane: -1 is one second before 1970.
        assert_eq!(
            convert_cell(mcx, Conv::TsFromEpochSecs(EpochLane::I32), &b32, 0).unwrap(),
            Datum::from_i64(-946_684_801_000_000)
        );
        // Unsigned i32 lane: the same bits zero-extend to u32::MAX (2106).
        assert_eq!(
            convert_cell(mcx, Conv::TsFromEpochSecs(EpochLane::U32), &b32, 0).unwrap(),
            Datum::from_i64((4_294_967_295 - 946_684_800) * 1_000_000)
        );
        // i64 lane, past 2038.
        assert_eq!(
            convert_cell(mcx, Conv::TsFromEpochSecs(EpochLane::I64), &b64, 1).unwrap(),
            Datum::from_i64((4_102_444_800 - 946_684_800) * 1_000_000)
        );
        // u64 lane: the sign bit means > i64::MAX seconds — out of range.
        assert!(convert_cell(mcx, Conv::TsFromEpochSecs(EpochLane::U64), &b64, 0).is_err());
        assert!(convert_cell(mcx, Conv::DateFromEpochDays(EpochLane::U64), &b64, 0).is_err());
        // Day lanes through the same widening: u32::MAX days does not fit
        // the i32 date datum after the shift — out of range, not wrap.
        assert!(convert_cell(mcx, Conv::DateFromEpochDays(EpochLane::U32), &b32, 0).is_err());
        assert_eq!(
            convert_cell(mcx, Conv::DateFromEpochDays(EpochLane::U32), &b32, 1).unwrap(),
            Datum::from_i32(86_400 - 10_957)
        );
        assert_eq!(
            convert_cell(mcx, Conv::DateFromEpochDays(EpochLane::I32), &b32, 1).unwrap(),
            Datum::from_i32(86_400 - 10_957)
        );
    }

    #[test]
    fn epoch_admission_is_opt_in_and_annotation_wins() {
        use types_core::catalog::{DATEOID, INT8OID, TIMESTAMPOID, TIMESTAMPTZOID};
        let col = |phys, logical| parquet_read::ColumnSchema {
            name: "c".into(),
            phys,
            max_def: 0,
            logical,
        };
        let plain64 = col(Phys::Int64, Logical::None);
        // Opt-in: the temporal target admits only under the flag.
        assert!(matches!(
            resolve_conv(&plain64, TIMESTAMPOID, -1, "c", true),
            Ok(Conv::TsFromEpochSecs(EpochLane::I64))
        ));
        assert!(matches!(
            resolve_conv(&plain64, TIMESTAMPTZOID, -1, "c", true),
            Ok(Conv::TsFromEpochSecs(EpochLane::I64))
        ));
        assert!(matches!(
            resolve_conv(&plain64, DATEOID, -1, "c", true),
            Ok(Conv::DateFromEpochDays(EpochLane::I64))
        ));
        // The flag never disturbs non-temporal targets.
        assert!(matches!(resolve_conv(&plain64, INT8OID, -1, "c", true), Ok(Conv::I64)));
        // Unsigned annotations pick the unsigned lanes.
        let u16d = col(Phys::Int32, Logical::Int { bits: 16, signed: false });
        assert!(matches!(
            resolve_conv(&u16d, DATEOID, -1, "c", true),
            Ok(Conv::DateFromEpochDays(EpochLane::U32))
        ));
        let u64c = col(Phys::Int64, Logical::Int { bits: 64, signed: false });
        assert!(matches!(
            resolve_conv(&u64c, TIMESTAMPOID, -1, "c", true),
            Ok(Conv::TsFromEpochSecs(EpochLane::U64))
        ));
        let i32c = col(Phys::Int32, Logical::None);
        assert!(matches!(
            resolve_conv(&i32c, TIMESTAMPOID, -1, "c", true),
            Ok(Conv::TsFromEpochSecs(EpochLane::I32))
        ));
        // Temporal annotations keep their mapping under the flag.
        let annts =
            col(Phys::Int64, Logical::Timestamp { unit: TimeUnit::Millis, utc: true });
        assert!(matches!(
            resolve_conv(&annts, TIMESTAMPOID, -1, "c", true),
            Ok(Conv::Timestamp(TimeUnit::Millis))
        ));
        let annd = col(Phys::Int32, Logical::Date);
        assert!(matches!(
            resolve_conv(&annd, DATEOID, -1, "c", true),
            Ok(Conv::DateFromDays)
        ));
    }
}
