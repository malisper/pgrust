//! General parquet reader for the COPY ingest path (increment 1: the
//! single-threaded correctness reader with the staged kernel set).
//!
//! Owned per the build-vs-borrow ruling: a hand-rolled thrift-compact footer
//! SKIP-PARSER (schema + per-chunk offsets/codec/counts only; statistics and
//! page indexes never decoded), the RLE/bit-packed hybrid kernels, PLAIN
//! decoders, and per-page kernel dispatch. Borrowed: snap / lz4_flex / zstd
//! codecs, simdutf8 validation.
//!
//! Decodes in this increment: v1 data pages + dictionary pages; PLAIN,
//! PLAIN_DICTIONARY/RLE_DICTIONARY, RLE booleans; def levels at max_def=1
//! (flat OPTIONAL); snappy/zstd/lz4_raw/uncompressed. Everything else —
//! v2 pages, DELTA_* and BYTE_STREAM_SPLIT encodings, nesting, INT96, FLBA,
//! gzip/brotli/LZO/Hadoop-framed LZ4, encryption, external chunks — errors
//! with a one-line message naming the feature: never a wrong answer.
//!
//! The batch API deliberately separates page decode from batch fill so a
//! later increment can consume dictionary + index streams without
//! materializing values (the columnar-store transcode seam).

mod codec;
mod column;
mod meta;
mod page;
mod plain;
mod rle;
mod thrift;
#[cfg(test)]
mod tests;

use std::fs::File;

use types_error::{PgError, PgResult, ERRCODE_BAD_COPY_FILE_FORMAT};

pub use column::{BatchData, ColumnBatch, ColumnCursor};
pub use meta::{ColumnSchema, FileMeta, Logical, Phys, TimeUnit};

/// Parse a bare FileMetaData footer (the bytes between the leading magic and
/// the trailing length+magic) — footer-inspection tooling.
pub fn parse_footer_bytes(buf: &[u8]) -> PgResult<FileMeta> {
    meta::parse_file_meta(buf)
}

const MAGIC: &[u8; 4] = b"PAR1";
const MAGIC_ENCRYPTED: &[u8; 4] = b"PARE";

/// Hard cap on footer size we will materialize (a corrupt length field must
/// not drive a giant allocation). Generous: real footers of 100k-chunk files
/// run single-digit MB.
const MAX_FOOTER: u64 = 256 << 20;

#[track_caller]
#[cold]
#[inline(never)]
fn not_parquet(path: &str, what: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("\"{path}\" is not a parquet file: {what}"))
            .with_sqlstate(ERRCODE_BAD_COPY_FILE_FORMAT),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn io_error(path: &str, op: &str, e: std::io::Error) -> Box<PgError> {
    Box::new(PgError::error(format!("could not {op} \"{path}\": {e}")))
}

fn read_exact_at(file: &File, path: &str, buf: &mut [u8], off: u64) -> PgResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, off).map_err(|e| io_error(path, "read", e))
    }
    #[cfg(not(unix))]
    {
        let _ = (file, buf, off);
        Err(Box::new(PgError::error(format!(
            "parquet read is not supported on this platform (\"{path}\")"
        ))))
    }
}

pub struct FileReader {
    file: File,
    path: String,
    file_len: u64,
    /// Arc so a parallel driver can share the parsed footer across worker
    /// threads without reparsing (FileMeta is plain data: Send + Sync).
    pub meta: std::sync::Arc<FileMeta>,
}

impl FileReader {
    /// Open + footer skip-parse. Everything is validated against the file
    /// length before any chunk read.
    pub fn open(path: &str) -> PgResult<FileReader> {
        let file = File::open(path).map_err(|e| io_error(path, "open", e))?;
        let md = file.metadata().map_err(|e| io_error(path, "stat", e))?;
        if md.is_dir() {
            return Err(Box::new(
                PgError::error(format!("\"{path}\" is a directory"))
                    .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
            ));
        }
        let file_len = md.len();
        // Leading magic + footer length + trailing magic.
        if file_len < 12 {
            return Err(not_parquet(path, "file too small"));
        }
        let mut head = [0u8; 4];
        read_exact_at(&file, path, &mut head, 0)?;
        let mut tail = [0u8; 8];
        read_exact_at(&file, path, &mut tail, file_len - 8)?;
        let tail_magic: &[u8; 4] = tail[4..8].try_into().expect("4-byte slice");
        if tail_magic == MAGIC_ENCRYPTED {
            return Err(meta::unsupported("encrypted parquet file".into()));
        }
        if &head != MAGIC || tail_magic != MAGIC {
            return Err(not_parquet(path, "magic bytes missing"));
        }
        let footer_len = u64::from(u32::from_le_bytes(
            tail[0..4].try_into().expect("4-byte slice"),
        ));
        if footer_len > MAX_FOOTER || footer_len + 12 > file_len {
            return Err(not_parquet(path, "footer length out of range"));
        }
        let mut footer = vec![0u8; footer_len as usize];
        read_exact_at(&file, path, &mut footer, file_len - 8 - footer_len)?;
        let meta = meta::parse_file_meta(&footer)?;

        // Offset sanity for every chunk, before anything is read. Zero-value
        // chunks (zero-row groups) are never read; some writers emit them
        // with a zero data_page_offset.
        for rg in &meta.row_groups {
            for ch in &rg.chunks {
                if ch.num_values == 0 {
                    continue;
                }
                let start = ch.start_offset();
                let size = ch.total_compressed_size;
                if start < 4
                    || size < 0
                    || (start as u64).saturating_add(size as u64) > file_len
                {
                    return Err(not_parquet(path, "column chunk offsets out of range"));
                }
            }
        }
        Ok(FileReader {
            file,
            path: path.to_string(),
            file_len,
            meta: std::sync::Arc::new(meta),
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    pub fn meta_arc(&self) -> std::sync::Arc<FileMeta> {
        std::sync::Arc::clone(&self.meta)
    }

    /// Duplicate the underlying file handle (positioned reads only) for a
    /// parallel driver's worker threads.
    pub fn try_clone_file(&self) -> PgResult<File> {
        self.file.try_clone().map_err(|e| io_error(&self.path, "dup", e))
    }

    /// Open one row group, building cursors for the requested columns.
    /// `validate_utf8[i]` pairs with `columns[i]` (schema ordinals).
    pub fn row_group(
        &mut self,
        rg_idx: usize,
        columns: &[usize],
        validate_utf8: &[bool],
    ) -> PgResult<RowGroupReader> {
        RowGroupReader::open(&self.file, &self.path, &self.meta, rg_idx, columns, validate_utf8)
    }
}

/// Compressed bytes this row group's requested chunks occupy (the
/// parallel driver's memory-budget input).
pub fn rg_compressed_bytes(meta: &FileMeta, rg_idx: usize, columns: &[usize]) -> u64 {
    let Some(rg) = meta.row_groups.get(rg_idx) else { return 0 };
    columns
        .iter()
        .filter_map(|&col| rg.chunks.iter().find(|c| c.column == col))
        .map(|c| c.total_compressed_size.max(0) as u64)
        .sum()
}

/// Cursors over one row group's requested columns; all columns advance in
/// lock-step through `read_batch` calls made by the driver.
pub struct RowGroupReader {
    cursors: Vec<ColumnCursor>,
    num_rows: u64,
    rows_read: u64,
    /// Total compressed chunk bytes read for this group (progress metering).
    pub compressed_bytes: u64,
}

impl RowGroupReader {
    /// Shareable open (parallel drivers pass a duplicated handle + the Arc'd
    /// meta). When the requested chunks are DENSE in the file — the layout
    /// every columnar writer produces — the whole row-group span is read in
    /// ONE sequential request and demuxed in memory (per-chunk preads are
    /// wrong at the small median chunk sizes of wide tables); scattered
    /// layouts fall back to per-chunk reads.
    pub fn open(
        file: &File,
        path: &str,
        meta: &FileMeta,
        rg_idx: usize,
        columns: &[usize],
        validate_utf8: &[bool],
    ) -> PgResult<RowGroupReader> {
        let rg = meta
            .row_groups
            .get(rg_idx)
            .ok_or_else(|| not_parquet(path, "row group index out of range"))?;
        let mut cursors = Vec::new();
        cursors
            .try_reserve(columns.len())
            .map_err(|_| Box::new(PgError::error("out of memory opening row group")))?;

        // Resolve the chunk set once; compute the span for the dense test.
        let mut chunks = Vec::with_capacity(columns.len());
        let (mut lo, mut hi, mut sum) = (u64::MAX, 0u64, 0u64);
        for &col in columns.iter() {
            let ch = rg
                .chunks
                .iter()
                .find(|c| c.column == col)
                .ok_or_else(|| not_parquet(path, "column chunk missing from row group"))?;
            if ch.num_values > 0 {
                let start = ch.start_offset() as u64;
                let end = start + ch.total_compressed_size as u64;
                lo = lo.min(start);
                hi = hi.max(end);
                sum += ch.total_compressed_size as u64;
            }
            chunks.push(ch);
        }
        // Dense: the span wastes at most 25% + a page of slack over the sum.
        let span = hi.saturating_sub(if lo == u64::MAX { 0 } else { lo });
        let dense = sum > 0 && span <= sum + sum / 4 + 8192;
        let shared: Option<std::sync::Arc<Vec<u8>>> = if dense {
            let mut buf = Vec::new();
            buf.try_reserve(span as usize + codec::PAD)
                .map_err(|_| Box::new(PgError::error("out of memory reading row group")))?;
            buf.resize(span as usize, 0);
            read_exact_at(file, path, &mut buf, lo)?;
            // One pad for the whole shared buffer (initialized capacity).
            buf.resize(span as usize + codec::PAD, 0);
            buf.truncate(span as usize);
            Some(std::sync::Arc::new(buf))
        } else {
            None
        };

        let mut compressed_bytes: u64 = 0;
        for (ch, &vutf8) in chunks.iter().zip(validate_utf8.iter()) {
            let schema = &meta.columns[ch.column];
            let bytes = if ch.num_values == 0 {
                column::ChunkBytes::Owned(Vec::new())
            } else if let Some(buf) = &shared {
                let start = (ch.start_offset() as u64 - lo) as usize;
                compressed_bytes += ch.total_compressed_size as u64;
                column::ChunkBytes::Shared {
                    buf: std::sync::Arc::clone(buf),
                    start,
                    len: ch.total_compressed_size as usize,
                }
            } else {
                let start = ch.start_offset() as u64;
                let size = ch.total_compressed_size as usize;
                let mut buf = Vec::new();
                buf.try_reserve(size + codec::PAD).map_err(|_| {
                    Box::new(PgError::error("out of memory reading column chunk"))
                })?;
                buf.resize(size, 0);
                read_exact_at(file, path, &mut buf, start)?;
                compressed_bytes += size as u64;
                column::ChunkBytes::Owned(buf)
            };
            cursors.push(ColumnCursor::new(
                bytes,
                ch.codec,
                schema.phys,
                schema.max_def,
                vutf8,
                schema.name.clone(),
                ch.num_values,
            )?);
        }
        Ok(RowGroupReader {
            cursors,
            num_rows: rg.num_rows as u64,
            rows_read: 0,
            compressed_bytes,
        })
    }

    pub fn num_rows(&self) -> u64 {
        self.num_rows
    }

    pub fn rows_remaining(&self) -> u64 {
        self.num_rows - self.rows_read
    }

    /// Fill one batch per requested column, `n` rows each (the caller keeps
    /// `n <= rows_remaining()`). Batches are positionally parallel.
    pub fn read_batches(&mut self, out: &mut [ColumnBatch], n: usize) -> PgResult<()> {
        debug_assert_eq!(out.len(), self.cursors.len());
        for (cur, batch) in self.cursors.iter_mut().zip(out.iter_mut()) {
            cur.read_batch(batch, n)?;
        }
        self.rows_read += n as u64;
        Ok(())
    }
}
