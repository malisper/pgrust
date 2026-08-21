//! The parsed stream directory (spec §6.2–§6.4): StreamEntry array + extent
//! tables, parsed once per part and shared (`Arc<StreamDirectory>` in the
//! part's state — the "parsed-metadata registry" content for streams).
//!
//! Note on `StreamEntry.width` (spec §6.3): it is the ENCODING width
//! (BYTE_FOR delta width, DICT_CODES max code width, ByvalWord width for
//! VERBATIM) — exactly what `KernelKey` dispatches on. This module therefore
//! never reconstructs a `StorageClass` from an entry; kernels own payload
//! semantics and bounds-check regardless (spec §19.2).

use pgrc2_format::part::{
    ExtentRecord, StreamEntry, StreamRole, EXTENT_RECORD_LEN, STREAM_ENTRY_LEN,
};
use pgrc2_format::class::CLASS_VARLENA;
use pgrc2_format::wire::Cur;
use pgrc2_format::{FormatError, FormatResult};

/// The class byte must name one of the six frozen storage classes (spec §3);
/// deeper class semantics stay with the kernels.
fn class_id_known(class: u8) -> FormatResult<()> {
    if class > CLASS_VARLENA {
        return Err(FormatError::UnknownStorageClass { class });
    }
    Ok(())
}

/// One stream with its parsed extent table.
#[derive(Debug, Clone)]
pub struct ParsedStream {
    pub entry: StreamEntry,
    pub role: StreamRole,
    pub extents: Vec<ExtentRecord>,
}

impl ParsedStream {
    /// The extent covering part-granule `g` (granule-organized roles only).
    pub fn extent_for_granule(&self, g: u32) -> FormatResult<(u32, &ExtentRecord)> {
        // Extents are validated ascending by granule_start at parse.
        let i = self
            .extents
            .partition_point(|r| r.granule_start <= g)
            .checked_sub(1)
            .ok_or(FormatError::Corrupt {
                at: "extent coverage",
            })?;
        let r = &self.extents[i];
        if g < r.granule_start || g - r.granule_start >= r.granule_count {
            return Err(FormatError::Corrupt {
                at: "extent coverage",
            });
        }
        Ok((i as u32, r))
    }

    /// Whether this role is granule-organized (extent granule ranges tile the
    /// part) vs stream-organized (dict/overflow: extents are byte runs).
    pub fn granule_organized(&self) -> bool {
        matches!(
            self.role,
            StreamRole::Values | StreamRole::Validity | StreamRole::Sizes | StreamRole::ChildValues
        )
    }
}

/// The parsed directory: every stream of the part, with a sorted lookup
/// index over (attno, path_ord, role).
#[derive(Debug)]
pub struct StreamDirectory {
    streams: Vec<ParsedStream>,
    /// (attno, path_ord, role) → index into `streams`, sorted.
    index: Vec<((u32, u32, u8), u32)>,
}

impl StreamDirectory {
    /// Parse a StreamDir section body (spec §6.2): `StreamEntry ×
    /// stream_count` then the extent tables the entries point into
    /// (section-relative offsets). Every structural fact is validated typed.
    pub fn parse(section: &[u8], stream_count: u32) -> FormatResult<StreamDirectory> {
        let entries_len = stream_count as usize * STREAM_ENTRY_LEN;
        if section.len() < entries_len {
            return Err(FormatError::Truncated { at: "StreamDir" });
        }
        let mut c = Cur::new(&section[..entries_len]);
        let mut streams = Vec::with_capacity(stream_count as usize);
        let mut index: Vec<((u32, u32, u8), u32)> = Vec::with_capacity(stream_count as usize);
        for si in 0..stream_count {
            let entry = StreamEntry::decode(&mut c)?;
            let role = StreamRole::from_u8(entry.role)?;
            class_id_known(entry.class)?;
            let extents = parse_extents(section, &entry, role)?;
            index.push(((entry.attno, entry.path_ord, entry.role), si));
            streams.push(ParsedStream {
                entry,
                role,
                extents,
            });
        }
        index.sort_unstable();
        for w in index.windows(2) {
            if w[0].0 == w[1].0 {
                return Err(FormatError::Corrupt {
                    at: "duplicate stream (attno, path, role)",
                });
            }
        }
        Ok(StreamDirectory { streams, index })
    }

    pub fn streams(&self) -> &[ParsedStream] {
        &self.streams
    }

    /// Look up the stream for (attno, path_ord, role).
    pub fn lookup(&self, attno: u32, path_ord: u32, role: StreamRole) -> Option<&ParsedStream> {
        let key = (attno, path_ord, role.as_u8());
        let i = self.index.binary_search_by_key(&key, |e| e.0).ok()?;
        Some(&self.streams[self.index[i].1 as usize])
    }
}

fn parse_extents(
    section: &[u8],
    entry: &StreamEntry,
    role: StreamRole,
) -> FormatResult<Vec<ExtentRecord>> {
    if entry.extent_count == 0 {
        // TY-1 (lanev4): the ArrayDual PARENT entry is a structural
        // election marker — its bytes live entirely in the Sizes/
        // ChildValues substreams, so ZERO extents is its correct shape.
        // Every other stream must carry extents.
        if role == StreamRole::Values
            && entry.encoding == pgrc2_format::enc::EncodingId::ArrayDual.as_u16()
        {
            return Ok(Vec::new());
        }
        return Err(FormatError::Corrupt {
            at: "stream with zero extents",
        });
    }
    let off = entry.extent_table_off as usize;
    let need = entry.extent_count as usize * EXTENT_RECORD_LEN;
    let end = off.checked_add(need).ok_or(FormatError::Bounds {
        at: "extent table",
    })?;
    if end > section.len() {
        return Err(FormatError::Bounds {
            at: "extent table",
        });
    }
    let mut c = Cur::new(&section[off..end]);
    let mut extents = Vec::with_capacity(entry.extent_count as usize);
    let mut values_sum: u64 = 0;
    let mut prev_end: Option<u64> = None;
    let granule_organized = matches!(
        role,
        StreamRole::Values | StreamRole::Validity | StreamRole::Sizes | StreamRole::ChildValues
    );
    for _ in 0..entry.extent_count {
        let r = ExtentRecord::decode(&mut c)?;
        values_sum = values_sum
            .checked_add(r.values)
            .ok_or(FormatError::Corrupt {
                at: "extent values overflow",
            })?;
        if granule_organized {
            let start = r.granule_start as u64;
            let g_end = start + r.granule_count as u64;
            if r.granule_count == 0 {
                return Err(FormatError::Corrupt {
                    at: "extent with zero granules",
                });
            }
            if let Some(pe) = prev_end {
                if start < pe {
                    return Err(FormatError::Corrupt {
                        at: "extent granule order",
                    });
                }
            }
            prev_end = Some(g_end);
        }
        extents.push(r);
    }
    if values_sum != entry.values {
        return Err(FormatError::Corrupt {
            at: "extent values vs stream values",
        });
    }
    Ok(extents)
}

/// Parse a PathTable section body (spec §6.5): `{ count: u32, entries:
/// [{ len: u16, bytes… (UTF-8) }] }`, each entry 4-byte-aligned. Returned
/// vector is positional: `paths[i]` is path_ord `i + 1` (path_ord 0 = root,
/// never in the table).
pub fn parse_path_table(section: &[u8]) -> FormatResult<Vec<String>> {
    if section.len() < 4 {
        return Err(FormatError::Truncated { at: "PathTable" });
    }
    let count = u32::from_le_bytes(section[..4].try_into().expect("len 4")) as usize;
    let mut out = Vec::with_capacity(count.min(4096));
    let mut off = 4usize;
    for _ in 0..count {
        off = off
            .checked_add(3)
            .map(|o| o & !3)
            .ok_or(FormatError::Bounds { at: "PathTable" })?;
        if off + 2 > section.len() {
            return Err(FormatError::Truncated { at: "PathTable" });
        }
        let len = u16::from_le_bytes(section[off..off + 2].try_into().expect("len 2")) as usize;
        off += 2;
        if off + len > section.len() {
            return Err(FormatError::Truncated { at: "PathTable" });
        }
        let s = core::str::from_utf8(&section[off..off + len]).map_err(|_| {
            FormatError::Corrupt {
                at: "PathTable utf8",
            }
        })?;
        out.push(s.to_string());
        off += len;
    }
    Ok(out)
}

/// Read a part's PathTable (spec §6.5): the shred-lane path map, positional
/// (`paths[i]` ↔ path_ord `i + 1`). `Ok(None)` = the part carries no shred
/// lanes (no PathTable section — the common, lanes-free shape). PathTable is
/// a raw part-level section (attno 0, path_ord 0), CRC-validated by the
/// section fault like every listed section.
pub fn read_path_table(part: &crate::openpart::OpenPart) -> crate::ReadResult<Option<Vec<String>>> {
    use pgrc2_format::part::SectionKind;
    let Some(idx) = part.find_section(SectionKind::PathTable, 0, 0) else {
        return Ok(None);
    };
    let raw = part.section_bytes(idx)?;
    Ok(Some(parse_path_table(raw.bytes())?))
}
