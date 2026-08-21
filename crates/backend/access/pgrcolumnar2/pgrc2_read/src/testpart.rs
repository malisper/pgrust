//! Test scaffolding (compiled for unit tests and the loom models only):
//! assemble COMPLETE part files — header, stream sections through the frozen
//! reference encoders, aux sections, stream directory, section table,
//! footer, tail — plus manifest/CURRENT images for walk tests.
//!
//! This is NOT a writer (M3-D owns sealing); it exists so the reader's
//! gates run against real on-disk shapes built from nothing but frozen
//! `pgrc2_format` APIs, keeping wave-2 sibling independence intact.

use pgrc2_format::abi::{EncodeInput, GranuleEncoder};
use pgrc2_format::class::StorageClass;
use pgrc2_format::dict::DICT_INDEX_ENTRY_LEN;
use pgrc2_format::enc::{EncodingId, Wrapper};
use pgrc2_format::geom::{self, DICT_FRAME_ENTRIES, GRANULE_ROWS};
use pgrc2_format::manifest::{CommitPointer, Manifest, ManifestHeader, PartRecord};
use pgrc2_format::part::{
    FooterFixed, OverflowSink, PartHeader, PartTail, SectionEntry, SectionKind, StreamEntry,
    StreamRole, StreamSectionWriter, FOOTER_MAGIC, STREAMF_DICT_EXEC, STREAMF_HAS_OVERFLOW,
    STREAMF_SIGNED,
};
use pgrc2_format::verbatim::{encode_validity_bitmap, ConstEncoder, VerbatimEncoder};
use pgrc2_format::wire::{crc32c, pad_to, put_u16, put_u32, put_varlena_entry};
use pgrc2_format::FORMAT_VERSION;

// ---------------------------------------------------------------------------
// specs
// ---------------------------------------------------------------------------

/// One cell: `Word` for byval/float/bool classes, `Bytes` for fixed/varlena.
#[derive(Debug, Clone)]
pub enum CellValue {
    Word(u64),
    Bytes(Vec<u8>),
}

/// Reference encodings available to the builder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefEncoding {
    Verbatim,
    Const,
}

/// A byte-rank-sorted global dictionary to attach to the column.
#[derive(Debug, Clone)]
pub struct DictSpec {
    pub entries: Vec<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct ColSpec {
    pub attno: u32,
    pub path_ord: u32,
    pub class: StorageClass,
    pub encoding: RefEncoding,
    /// Per row; `None` = NULL. For child streams (`child_gcounts`), per
    /// VALUE (dense element order).
    pub values: Vec<Option<CellValue>>,
    /// Granule ordinals (besides 0) where a new extent starts.
    pub extent_cuts: Vec<u32>,
    /// `Some` ⇒ the value stream is role ChildValues with this per-granule
    /// value-count table (sizes-of-sizes nesting currency, spec §6.5).
    pub child_gcounts: Option<Vec<u32>>,
    pub dict: Option<DictSpec>,
    pub dict_exec_flag: bool,
    pub with_stats: bool,
    pub with_psma: bool,
    pub with_bloom: bool,
    pub with_ndv: bool,
}

impl ColSpec {
    pub fn new(attno: u32, class: StorageClass, values: Vec<Option<CellValue>>) -> ColSpec {
        ColSpec {
            attno,
            path_ord: 0,
            class,
            encoding: RefEncoding::Verbatim,
            values,
            extent_cuts: Vec::new(),
            child_gcounts: None,
            dict: None,
            dict_exec_flag: false,
            with_stats: false,
            with_psma: false,
            with_bloom: false,
            with_ndv: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PartSpec {
    pub rows: u64,
    pub part_no: u32,
    pub relfilenumber: u64,
    pub spc: u32,
    pub db: u32,
    pub schema_fingerprint: u64,
    pub cols: Vec<ColSpec>,
    pub path_table: Vec<String>,
    pub with_sort_key: bool,
    pub with_sidecar_dir: bool,
}

impl PartSpec {
    pub fn new(rows: u64, cols: Vec<ColSpec>) -> PartSpec {
        PartSpec {
            rows,
            part_no: 7,
            relfilenumber: 4242,
            spc: 1663,
            db: 5,
            schema_fingerprint: 0xF17E_0001_D00D_BEEF,
            cols,
            path_table: Vec::new(),
            with_sort_key: false,
            with_sidecar_dir: false,
        }
    }
}

// ---------------------------------------------------------------------------
// built output
// ---------------------------------------------------------------------------

/// One emitted section's identity facts (section-table order).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SectionInfo {
    pub kind: u16,
    pub attno: u32,
    pub path_ord: u32,
    /// Stream sections: the role byte. Non-stream sections: 255.
    pub role: u8,
    pub off: u64,
    pub len: u64,
}

/// Expected decode results for one column (canonical-bytes lens, spec
/// §18.1 — the same lens `datum_canonical_bytes` serves).
#[derive(Debug, Clone)]
pub struct ColExpected {
    pub attno: u32,
    pub path_ord: u32,
    pub class: StorageClass,
    pub valid: Vec<bool>,
    /// Canonical bytes per row/value (empty for nulls).
    pub canon: Vec<Vec<u8>>,
}

pub struct BuiltPart {
    pub bytes: Vec<u8>,
    pub sections: Vec<SectionInfo>,
    pub footer_off: u64,
    pub expected: Vec<ColExpected>,
    pub spec: PartSpec,
}

impl BuiltPart {
    pub fn mem_io(&self, dev: u64, ino: u64) -> crate::io::MemPartIo {
        crate::io::MemPartIo::new(self.bytes.clone(), dev, ino)
    }

    /// Section-table index of the first section matching.
    pub fn find_section(&self, kind: SectionKind, attno: u32, path_ord: u32) -> Option<usize> {
        self.sections
            .iter()
            .position(|s| s.kind == kind.as_u16() && s.attno == attno && s.path_ord == path_ord)
    }

    /// The stream section (kind Stream) for (attno, path_ord, role).
    pub fn find_stream_section(&self, attno: u32, path_ord: u32, role: StreamRole) -> Option<usize> {
        self.sections.iter().position(|s| {
            s.kind == SectionKind::Stream.as_u16()
                && s.attno == attno
                && s.path_ord == path_ord
                && s.role == role.as_u8()
        })
    }
}

// ---------------------------------------------------------------------------
// the builder
// ---------------------------------------------------------------------------

struct PendingStream {
    entry: StreamEntry,
    extents: Vec<pgrc2_format::part::ExtentRecord>,
}

struct SectionOut {
    kind: SectionKind,
    attno: u32,
    path_ord: u32,
    role: u8,
    body: Vec<u8>,
    optional: bool,
}

/// Build the complete part image per `spec`.
pub fn build_part(spec: &PartSpec) -> BuiltPart {
    let rows = spec.rows;
    let granule_count = geom::granule_count(rows);
    let mut sections: Vec<SectionOut> = Vec::new();
    let mut streams: Vec<PendingStream> = Vec::new();
    let mut expected: Vec<ColExpected> = Vec::new();

    for col in &spec.cols {
        build_column(col, rows, granule_count, &mut sections, &mut streams, &mut expected);
    }

    // Aux sections.
    if !spec.path_table.is_empty() {
        let mut body = Vec::new();
        put_u32(&mut body, spec.path_table.len() as u32);
        for p in &spec.path_table {
            pad_to(&mut body, 4);
            put_u16(&mut body, p.len() as u16);
            body.extend_from_slice(p.as_bytes());
        }
        sections.push(SectionOut {
            kind: SectionKind::PathTable,
            attno: 0,
            path_ord: 0,
            role: 255,
            body,
            optional: false,
        });
    }
    if spec.with_sort_key {
        // nkeys = 0 record (spec §9).
        let body = vec![0u8; 8];
        sections.push(SectionOut {
            kind: SectionKind::SortKey,
            attno: 0,
            path_ord: 0,
            role: 255,
            body,
            optional: false,
        });
    }
    if spec.with_sidecar_dir {
        // One zeroed 48-B slot record — opaque to this reader (M3-E/H parse
        // sidecar semantics), present so the CRC battery covers the kind.
        sections.push(SectionOut {
            kind: SectionKind::SidecarDir,
            attno: 0,
            path_ord: 0,
            role: 255,
            body: vec![0u8; 48],
            optional: false,
        });
    }

    assemble(spec, granule_count, sections, streams, expected)
}

fn build_column(
    col: &ColSpec,
    part_rows: u64,
    granule_count: u32,
    sections: &mut Vec<SectionOut>,
    streams: &mut Vec<PendingStream>,
    expected: &mut Vec<ColExpected>,
) {
    let is_child = col.child_gcounts.is_some();
    let role = if is_child {
        StreamRole::ChildValues
    } else {
        StreamRole::Values
    };
    // Per-granule value counts.
    let gvalues: Vec<u32> = match &col.child_gcounts {
        Some(gc) => {
            assert_eq!(gc.len(), granule_count as usize, "child gcounts per granule");
            gc.clone()
        }
        None => (0..granule_count)
            .map(|g| geom::rows_in_granule(part_rows, g))
            .collect(),
    };
    let total_values: u64 = gvalues.iter().map(|&v| v as u64).sum();
    assert_eq!(col.values.len() as u64, total_values, "values vs geometry");

    // Build datum images (stable heap boxes) + canonical expectations.
    let mut images: Vec<Box<[u8]>> = Vec::new();
    let mut datums: Vec<u64> = Vec::with_capacity(col.values.len());
    let mut valid: Vec<bool> = Vec::with_capacity(col.values.len());
    let mut canon: Vec<Vec<u8>> = Vec::with_capacity(col.values.len());
    let any_null = col.values.iter().any(|v| v.is_none());
    for v in &col.values {
        match v {
            None => {
                datums.push(0);
                valid.push(false);
                canon.push(Vec::new());
            }
            Some(CellValue::Word(w)) => {
                datums.push(*w);
                valid.push(true);
                canon.push(canonical_word(col.class, *w));
            }
            Some(CellValue::Bytes(b)) => {
                let img: Box<[u8]> = match col.class {
                    StorageClass::Fixed { len } => {
                        assert_eq!(b.len(), len as usize, "fixed image length");
                        b.clone().into_boxed_slice()
                    }
                    StorageClass::VarlenaVerbatim => {
                        let mut i = Vec::with_capacity(4 + b.len());
                        i.extend_from_slice(
                            &pgrc2_format::wire::varlena_header_4b_u(b.len() as u32).to_le_bytes(),
                        );
                        i.extend_from_slice(b);
                        i.into_boxed_slice()
                    }
                    _ => panic!("Bytes cell on word class"),
                };
                datums.push(img.as_ptr() as u64);
                valid.push(true);
                canon.push(b.clone());
                images.push(img);
            }
        }
    }

    // Cut extents.
    let mut starts: Vec<u32> = vec![0];
    starts.extend(col.extent_cuts.iter().copied());
    starts.sort_unstable();
    starts.dedup();
    let mut extents: Vec<(u32, u32)> = Vec::new(); // (granule_start, granule_count)
    for (i, &s) in starts.iter().enumerate() {
        let end = starts.get(i + 1).copied().unwrap_or(granule_count);
        assert!(s < end && end <= granule_count, "extent cut bounds");
        extents.push((s, end - s));
    }

    // Value-stream sections (one per extent) + validity sections.
    let mut ext_records = Vec::new();
    let mut val_ext_records = Vec::new();
    let mut val_sections: Vec<Vec<u8>> = Vec::new();
    let mut overflow_region: Vec<u8> = Vec::new();
    let key_encoding = match col.encoding {
        RefEncoding::Verbatim => EncodingId::Verbatim,
        RefEncoding::Const => EncodingId::Const,
    };
    let mut vstart: usize = 0; // running dense value index
    // ONE sink for the whole column: stored `ovf_off`s are region-relative
    // (spec §6.8), so the sink's base must be the region start even when the
    // value stream is cut into multiple extents.
    let mut ovf = OverflowSink::new(&mut overflow_region);
    for &(gs, gc) in &extents {
        let mut section = Vec::new();
        let mut w = StreamSectionWriter::begin(
            &mut section,
            key_encoding.as_u16(),
            col.class.width(),
            Wrapper::None,
        )
        .expect("begin section");
        let mut enc_verbatim = VerbatimEncoder { class: col.class };
        let mut enc_const = ConstEncoder::new(col.class);
        let mut ext_values: u64 = 0;
        let mut vbits: Vec<u8> = Vec::new();
        for gi in 0..gc {
            let g = gs + gi;
            let vals = gvalues[g as usize] as usize;
            let d = &datums[vstart..vstart + vals];
            let words = validity_words(&valid[vstart..vstart + vals]);
            let has_null = valid[vstart..vstart + vals].iter().any(|&b| !b);
            let input = EncodeInput {
                class: col.class,
                rows: vals as u32,
                datums: d,
                validity: if has_null { Some(&words) } else { None },
            };
            let enc: &mut dyn GranuleEncoder = match col.encoding {
                RefEncoding::Verbatim => &mut enc_verbatim,
                RefEncoding::Const => &mut enc_const,
            };
            enc.encode_granule(&input, &mut w, &mut ovf).expect("encode granule");
            encode_validity_bitmap(input.validity, vals as u32, &mut vbits);
            ext_values += vals as u64;
            vstart += vals;
        }
        let enc: &mut dyn GranuleEncoder = match col.encoding {
            RefEncoding::Verbatim => &mut enc_verbatim,
            RefEncoding::Const => &mut enc_const,
        };
        enc.finish_stream(&mut w).expect("finish stream");
        let closeout = w.finish(is_child).expect("finish section");
        ext_records.push((gs, gc, ext_values, section.len() as u64, closeout));
        sections.push(SectionOut {
            kind: SectionKind::Stream,
            attno: col.attno,
            path_ord: col.path_ord,
            role: role.as_u8(),
            body: section,
            optional: false,
        });

        if any_null {
            // Validity extent mirroring this extent's granule range
            // (BOOL_BITMAP layout, spec §6.6).
            let mut vsection = Vec::new();
            let mut vw = StreamSectionWriter::begin(
                &mut vsection,
                EncodingId::BoolBitmap.as_u16(),
                0,
                Wrapper::None,
            )
            .expect("begin validity");
            vw.payload().extend_from_slice(&vbits);
            for gi in 0..gc {
                vw.end_granule(gvalues[(gs + gi) as usize]);
            }
            let vcloseout = vw.finish(is_child).expect("finish validity");
            val_ext_records.push((gs, gc, ext_values, vsection.len() as u64, vcloseout));
            val_sections.push(vsection);
        }
    }

    drop(ovf);
    let overflow_entries = count_region_entries(&overflow_region);

    // Overflow stream (framed section; payload = the entry region).
    let has_overflow = !overflow_region.is_empty();
    if has_overflow {
        let mut osection = Vec::new();
        // Payload region starts at the 32-B header boundary (8-aligned), so
        // region-relative entry offsets stay valid payload-relative.
        let mut ow = StreamSectionWriter::begin(
            &mut osection,
            EncodingId::Verbatim.as_u16(),
            0,
            Wrapper::None,
        )
        .expect("begin overflow");
        ow.payload().extend_from_slice(&overflow_region);
        let ocloseout = ow.finish(false).expect("finish overflow");
        let olen = osection.len() as u64;
        streams.push(PendingStream {
            entry: StreamEntry {
                extent_table_off: 0,
                values: overflow_entries,
                attno: col.attno,
                path_ord: col.path_ord,
                fixed_len: 0,
                aux32: 0,
                extent_count: 1,
                encoding: EncodingId::Verbatim.as_u16(),
                flags: 0,
                role: StreamRole::Overflow.as_u8(),
                class: col.class.id(),
                width: 0,
                wrapper: 0,
                reserved: 0,
            },
            extents: vec![pgrc2_format::part::ExtentRecord {
                file_off: 0, // patched at assemble
                len: olen,
                values: overflow_entries,
                granule_start: 0,
                granule_count: 0,
                crc: ocloseout.crc,
                flags: 0,
            }],
        });
        sections.push(SectionOut {
            kind: SectionKind::Stream,
            attno: col.attno,
            path_ord: col.path_ord,
            role: StreamRole::Overflow.as_u8(),
            body: osection,
            optional: false,
        });
    }

    // The values stream entry.
    let mut flags = 0u16;
    if col.class.signed() {
        flags |= STREAMF_SIGNED;
    }
    if has_overflow {
        flags |= STREAMF_HAS_OVERFLOW;
    }
    if col.dict_exec_flag {
        flags |= STREAMF_DICT_EXEC;
    }
    streams.push(PendingStream {
        entry: StreamEntry {
            extent_table_off: 0,
            values: total_values,
            attno: col.attno,
            path_ord: col.path_ord,
            fixed_len: col.class.fixed_len(),
            aux32: 0,
            extent_count: ext_records.len() as u32,
            encoding: key_encoding.as_u16(),
            flags,
            role: role.as_u8(),
            class: col.class.id(),
            width: col.class.width(),
            wrapper: 0,
            reserved: 0,
        },
        extents: ext_records
            .iter()
            .map(|(gs, gc, vals, len, c)| pgrc2_format::part::ExtentRecord {
                file_off: 0,
                len: *len,
                values: *vals,
                granule_start: *gs,
                granule_count: *gc,
                crc: c.crc,
                flags: 0,
            })
            .collect(),
    });

    if any_null {
        for vs in val_sections {
            sections.push(SectionOut {
                kind: SectionKind::Stream,
                attno: col.attno,
                path_ord: col.path_ord,
                role: StreamRole::Validity.as_u8(),
                body: vs,
                optional: false,
            });
        }
        streams.push(PendingStream {
            entry: StreamEntry {
                extent_table_off: 0,
                values: total_values,
                attno: col.attno,
                path_ord: col.path_ord,
                fixed_len: 0,
                aux32: 0,
                extent_count: val_ext_records.len() as u32,
                encoding: EncodingId::BoolBitmap.as_u16(),
                flags: 0,
                role: StreamRole::Validity.as_u8(),
                class: col.class.id(),
                width: 0,
                wrapper: 0,
                reserved: 0,
            },
            extents: val_ext_records
                .iter()
                .map(|(gs, gc, vals, len, c)| pgrc2_format::part::ExtentRecord {
                    file_off: 0,
                    len: *len,
                    values: *vals,
                    granule_start: *gs,
                    granule_count: *gc,
                    crc: c.crc,
                    flags: 0,
                })
                .collect(),
        });
    }

    // Dictionary streams (spec §7): byte-rank-sorted, framed payload.
    if let Some(d) = &col.dict {
        for w in d.entries.windows(2) {
            assert!(w[0] < w[1], "dict entries must be byte-rank-sorted");
        }
        let mut payload_region: Vec<u8> = Vec::new();
        let mut index_bytes: Vec<u8> = Vec::new();
        let mut frame_marks: Vec<usize> = Vec::new();
        for (i, e) in d.entries.iter().enumerate() {
            if i as u32 % DICT_FRAME_ENTRIES == 0 {
                frame_marks.push(payload_region.len());
            }
            let off = put_varlena_entry(&mut payload_region, e);
            put_u32(&mut index_bytes, off as u32);
            put_u32(&mut index_bytes, e.len() as u32);
            put_u32(
                &mut index_bytes,
                String::from_utf8_lossy(e).chars().count() as u32,
            );
        }
        assert_eq!(index_bytes.len(), d.entries.len() * DICT_INDEX_ENTRY_LEN);

        let mut psection = Vec::new();
        let mut pw = StreamSectionWriter::begin(
            &mut psection,
            EncodingId::Verbatim.as_u16(),
            0,
            Wrapper::None,
        )
        .expect("begin dict payload");
        // Mark dict frames (payload frame table, spec §7). The payload
        // region begins at the header boundary so region offsets are
        // payload-relative as stored.
        let mut written = 0usize;
        for (fi, &mark) in frame_marks.iter().enumerate() {
            let next = frame_marks.get(fi + 1).copied().unwrap_or(payload_region.len());
            pw.begin_frame();
            pw.payload().extend_from_slice(&payload_region[mark..next]);
            written += next - mark;
        }
        assert_eq!(written, payload_region.len());
        let pcloseout = pw.finish(false).expect("finish dict payload");
        let plen = psection.len() as u64;

        let mut isection = Vec::new();
        let mut iw = StreamSectionWriter::begin(
            &mut isection,
            EncodingId::Verbatim.as_u16(),
            0,
            Wrapper::None,
        )
        .expect("begin dict index");
        iw.payload().extend_from_slice(&index_bytes);
        let icloseout = iw.finish(false).expect("finish dict index");
        let ilen = isection.len() as u64;

        // SB-7 (M3-L3): the UNWRAPPED DictPayload extent table is cut at
        // dict-frame boundaries with per-frame CRCs — the v4 writer's
        // format-default geometry (`pgrc2_write::seal::dict_frame_extents`
        // mirrored): extent 0 carries the section header with frame 0,
        // interior bounds sit at header_len + frame_mark, the last extent
        // carries the frame-table tail; `granule_start` carries the FRAME
        // ordinal (DictPayload is not granule-organized). Sub-frame dicts
        // keep the legacy single-extent record.
        let pextents: Vec<pgrc2_format::part::ExtentRecord> = if frame_marks.len() > 1 {
            let hdr_len = pgrc2_format::part::STREAM_SECTION_HDR_LEN;
            let mut bounds: Vec<usize> = vec![0];
            for &mark in frame_marks.iter().skip(1) {
                bounds.push(hdr_len + mark);
            }
            bounds.push(psection.len());
            let total_entries = d.entries.len() as u64;
            bounds
                .windows(2)
                .enumerate()
                .map(|(f, w)| {
                    let (lo, hi) = (w[0], w[1]);
                    let f_entries = (total_entries - (f as u64) * DICT_FRAME_ENTRIES as u64)
                        .min(DICT_FRAME_ENTRIES as u64);
                    pgrc2_format::part::ExtentRecord {
                        file_off: lo as u64, // section-relative; patched at assemble
                        len: (hi - lo) as u64,
                        values: f_entries,
                        granule_start: f as u32,
                        granule_count: 1,
                        crc: pgrc2_format::wire::crc32c(&psection[lo..hi]),
                        flags: 0,
                    }
                })
                .collect()
        } else {
            vec![pgrc2_format::part::ExtentRecord {
                file_off: 0,
                len: plen,
                values: d.entries.len() as u64,
                granule_start: 0,
                granule_count: 0,
                crc: pcloseout.crc,
                flags: 0,
            }]
        };
        for (role, body, len, crc, values, extents) in [
            (
                StreamRole::DictIndex,
                isection,
                ilen,
                icloseout.crc,
                d.entries.len() as u64,
                vec![pgrc2_format::part::ExtentRecord {
                    file_off: 0,
                    len: ilen,
                    values: d.entries.len() as u64,
                    granule_start: 0,
                    granule_count: 0,
                    crc: icloseout.crc,
                    flags: 0,
                }],
            ),
            (
                StreamRole::DictPayload,
                psection,
                plen,
                pcloseout.crc,
                d.entries.len() as u64,
                pextents,
            ),
        ] {
            let _ = (len, crc);
            streams.push(PendingStream {
                entry: StreamEntry {
                    extent_table_off: 0,
                    values,
                    attno: col.attno,
                    path_ord: col.path_ord,
                    fixed_len: 0,
                    aux32: 0,
                    extent_count: extents.len() as u32,
                    encoding: EncodingId::Verbatim.as_u16(),
                    flags: 0,
                    role: role.as_u8(),
                    class: col.class.id(),
                    width: 0,
                    wrapper: 0,
                    reserved: 0,
                },
                extents,
            });
            sections.push(SectionOut {
                kind: SectionKind::Stream,
                attno: col.attno,
                path_ord: col.path_ord,
                role: role.as_u8(),
                body,
                optional: false,
            });
        }
    }

    // Aux metadata sections (bodies are plausible framings; the reader
    // treats them as opaque CRC-validated bytes — M3-E owns semantics).
    if col.with_stats {
        let mut body = Vec::new();
        let bands = geom::band_count(part_rows);
        for _ in 0..(granule_count + bands + 1) {
            pgrc2_format::meta::StatsRecord::absent().encode_into(&mut body);
        }
        sections.push(SectionOut {
            kind: SectionKind::Stats,
            attno: col.attno,
            path_ord: col.path_ord,
            role: 255,
            body,
            optional: false,
        });
    }
    if col.with_psma {
        let body = vec![0u8; (granule_count as usize).div_ceil(8)];
        sections.push(SectionOut {
            kind: SectionKind::Psma,
            attno: col.attno,
            path_ord: col.path_ord,
            role: 255,
            body,
            optional: false,
        });
    }
    if col.with_bloom {
        let mut body = Vec::new();
        put_u32(&mut body, 1); // k
        put_u32(&mut body, 0); // bytes_per_granule
        body.extend(vec![0u8; (granule_count as usize).div_ceil(8)]);
        sections.push(SectionOut {
            kind: SectionKind::Bloom,
            attno: col.attno,
            path_ord: col.path_ord,
            role: 255,
            body,
            optional: false,
        });
    }
    if col.with_ndv {
        let mut body = Vec::new();
        body.push(1u8); // algo
        body.push(4u8); // precision
        put_u16(&mut body, 0);
        put_u32(&mut body, 0); // reg_len
        sections.push(SectionOut {
            kind: SectionKind::NdvRegisters,
            attno: col.attno,
            path_ord: col.path_ord,
            role: 255,
            body,
            optional: false,
        });
    }

    expected.push(ColExpected {
        attno: col.attno,
        path_ord: col.path_ord,
        class: col.class,
        valid,
        canon,
    });
    drop(images); // encode is done; canonical expectations are copies
}

fn assemble(
    spec: &PartSpec,
    granule_count: u32,
    mut sections: Vec<SectionOut>,
    mut streams: Vec<PendingStream>,
    expected: Vec<ColExpected>,
) -> BuiltPart {
    let mut buf = Vec::new();
    let header = PartHeader::new(
        spec.part_no,
        spec.schema_fingerprint,
        spec.spc,
        spec.db,
        spec.relfilenumber,
    );
    header.encode_into(&mut buf);

    // Lay out every section (8-aligned starts — writer law, spec §1).
    let mut placed: Vec<(usize, u64)> = Vec::new(); // (section idx, off)
    for (i, s) in sections.iter().enumerate() {
        pad_to(&mut buf, 8);
        let off = buf.len() as u64;
        buf.extend_from_slice(&s.body);
        placed.push((i, off));
    }

    // Patch stream extent file offsets: extents were recorded in emission
    // order per (attno, path_ord, role); walk placed stream sections in the
    // same order. A frame-cut DictPayload (SB-7) subdivides ONE section:
    // its extents carry section-relative offsets and are all rebased onto
    // that section's placement.
    for ps in &mut streams {
        let mut next = 0usize;
        for (i, s) in sections.iter().enumerate() {
            if s.kind == SectionKind::Stream
                && s.attno == ps.entry.attno
                && s.path_ord == ps.entry.path_ord
                && s.role == ps.entry.role
            {
                if ps.entry.role == StreamRole::DictPayload.as_u8() && ps.extents.len() > 1 {
                    for e in ps.extents.iter_mut() {
                        e.file_off += placed[i].1;
                    }
                    next = ps.extents.len();
                } else if next < ps.extents.len() {
                    ps.extents[next].file_off = placed[i].1;
                    next += 1;
                }
            }
        }
        assert_eq!(next, ps.extents.len(), "extent placement");
    }

    // StreamDir section (entries + extent tables, spec §6.2).
    let mut sd_body = Vec::new();
    let entries_len = streams.len() * pgrc2_format::part::STREAM_ENTRY_LEN;
    let mut table_off = entries_len as u64;
    let mut tables = Vec::new();
    for ps in &mut streams {
        ps.entry.extent_table_off = table_off;
        for r in &ps.extents {
            r.encode_into(&mut tables);
        }
        table_off += (ps.extents.len() * pgrc2_format::part::EXTENT_RECORD_LEN) as u64;
    }
    for ps in &streams {
        ps.entry.encode_into(&mut sd_body);
    }
    sd_body.extend_from_slice(&tables);
    sections.push(SectionOut {
        kind: SectionKind::StreamDir,
        attno: 0,
        path_ord: 0,
        role: 255,
        body: sd_body,
        optional: false,
    });
    pad_to(&mut buf, 8);
    let sd_off = buf.len() as u64;
    buf.extend_from_slice(&sections.last().expect("stream dir").body);
    placed.push((sections.len() - 1, sd_off));

    // Section table.
    let mut info: Vec<SectionInfo> = Vec::new();
    let mut st_bytes = Vec::new();
    for (i, off) in &placed {
        let s = &sections[*i];
        let e = SectionEntry {
            off: *off,
            len: s.body.len() as u64,
            kind: s.kind.as_u16(),
            flags: if s.optional {
                pgrc2_format::part::SECTION_OPTIONAL
            } else {
                0
            },
            attno: s.attno,
            path_ord: s.path_ord,
            crc: crc32c(&s.body),
        };
        e.encode_into(&mut st_bytes);
        info.push(SectionInfo {
            kind: s.kind.as_u16(),
            attno: s.attno,
            path_ord: s.path_ord,
            role: s.role,
            off: *off,
            len: s.body.len() as u64,
        });
    }
    pad_to(&mut buf, 8);
    let st_off = buf.len() as u64;
    buf.extend_from_slice(&st_bytes);

    // Footer + tail.
    let footer = FooterFixed {
        magic: FOOTER_MAGIC,
        format_version: FORMAT_VERSION,
        rows: spec.rows,
        granule_count,
        band_count: geom::band_count(spec.rows),
        section_count: placed.len() as u32,
        flags: 0,
        section_table_off: st_off,
        section_table_crc: crc32c(&st_bytes),
        part_no: spec.part_no,
        schema_fingerprint: spec.schema_fingerprint,
        stream_count: streams.len() as u32,
        // Test parts build at the DEFAULT grain (SB-10).
        granule_rows: geom::GRANULE_ROWS,
        reserved: [0; 28],
        footer_crc: 0,
    };
    pad_to(&mut buf, 8);
    let footer_off = buf.len() as u64;
    footer.encode_into(&mut buf);
    PartTail::new(footer_off).encode_into(&mut buf);

    BuiltPart {
        bytes: buf,
        sections: info,
        footer_off,
        expected,
        spec: spec.clone(),
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn canonical_word(class: StorageClass, w: u64) -> Vec<u8> {
    match class {
        StorageClass::ByvalWord { width, .. } => w.to_le_bytes()[..width as usize].to_vec(),
        StorageClass::F32 => w.to_le_bytes()[..4].to_vec(),
        StorageClass::F64 => w.to_le_bytes().to_vec(),
        StorageClass::Bool => vec![(w != 0) as u8],
        _ => panic!("word canonical on pointer class"),
    }
}

pub fn validity_words(valid: &[bool]) -> Vec<u64> {
    let mut words = vec![0u64; valid.len().div_ceil(64)];
    for (i, &v) in valid.iter().enumerate() {
        if v {
            words[i / 64] |= 1 << (i % 64);
        }
    }
    words
}

/// Count varlena entries in the complete overflow region: entries start
/// 8-aligned (region-relative), each is a 4B-U varlena header (total length
/// INCLUDING the 4-byte header) + payload. Builder-produced bytes — parse
/// failures are builder bugs, asserted hard.
fn count_region_entries(region: &[u8]) -> u64 {
    let mut n = 0u64;
    let mut pos = 0usize;
    while pos + 4 <= region.len() {
        let hdr = u32::from_le_bytes(region[pos..pos + 4].try_into().expect("len 4"));
        assert_eq!(hdr & 0b11, 0, "overflow entry header must be 4B-U");
        let total = (hdr >> 2) as usize;
        assert!(total >= 4 && pos + total <= region.len(), "overflow entry bounds");
        n += 1;
        pos = (pos + total).div_ceil(8) * 8;
    }
    n
}

/// Flip one byte (corruption batteries).
pub fn flip_byte(bytes: &mut [u8], off: usize) {
    bytes[off] ^= 0xA5;
}

// ---------------------------------------------------------------------------
// manifest / CURRENT images (walk tests)
// ---------------------------------------------------------------------------

pub struct ManifestSpec {
    pub gen: u64,
    pub prev_gen: u64,
    pub publisher_fxid: u64,
    pub parts: Vec<PartRecord>,
    pub relfilenumber: u64,
    pub spc: u32,
    pub db: u32,
    pub schema_fingerprint: u64,
    pub next_part_no: u32,
}

pub fn build_manifest(spec: &ManifestSpec) -> Vec<u8> {
    let m = Manifest {
        header: ManifestHeader {
            gen: spec.gen,
            prev_gen: spec.prev_gen,
            publisher_fxid: spec.publisher_fxid,
            relfilenumber: spec.relfilenumber,
            spc: spec.spc,
            db: spec.db,
            magic: pgrc2_format::manifest::MANIFEST_MAGIC,
            format_version: FORMAT_VERSION,
            part_count: spec.parts.len() as u32,
            next_part_no: spec.next_part_no,
            flags: 0,
            reserved: 0,
            schema_fingerprint: spec.schema_fingerprint,
        },
        parts: spec.parts.clone(),
    };
    m.encode()
}

pub fn build_current(manifest_bytes: &[u8], gen: u64) -> Vec<u8> {
    let trailing = u32::from_le_bytes(
        manifest_bytes[manifest_bytes.len() - 4..]
            .try_into()
            .expect("len 4"),
    );
    CommitPointer::new(gen, manifest_bytes.len() as u64, trailing)
        .encode()
        .to_vec()
}

/// A PartRecord for a built part.
pub fn part_record(b: &BuiltPart) -> PartRecord {
    PartRecord {
        rows: b.spec.rows,
        file_len: b.bytes.len() as u64,
        footer_off: b.footer_off,
        dv_gen: 0,
        dv_len: 0,
        part_no: b.spec.part_no,
        flags: 0,
        granule_count: geom::granule_count(b.spec.rows),
        band_count: geom::band_count(b.spec.rows),
        dv_crc: 0,
        granule_rows: 0,
    }
}

/// Convenience: a small all-word column of `rows` sequential i64s.
pub fn seq_i64_col(attno: u32, rows: u64) -> ColSpec {
    let values = (0..rows)
        .map(|r| Some(CellValue::Word(r.wrapping_mul(3).wrapping_sub(7))))
        .collect();
    ColSpec::new(
        attno,
        StorageClass::ByvalWord {
            width: 8,
            signed: true,
        },
        values,
    )
}

/// Convenience: a varlena text column with an optional null pattern.
pub fn text_col(attno: u32, rows: u64, null_every: Option<u64>) -> ColSpec {
    let values = (0..rows)
        .map(|r| {
            if let Some(n) = null_every {
                if n > 0 && r % n == 0 {
                    return None;
                }
            }
            Some(CellValue::Bytes(
                format!("value-{:06}-{}", r, "x".repeat((r % 17) as usize)).into_bytes(),
            ))
        })
        .collect();
    ColSpec::new(attno, StorageClass::VarlenaVerbatim, values)
}

const _: () = {
    assert!(GRANULE_ROWS == 8192);
};
