//! Dictionary vocabulary (spec §7; charter §6): one framed, byte-rank-sorted
//! GLOBAL dictionary per dict column per part. Global codes ARE the stored
//! codes — the stitch pre-pass, remap layer, and per-worker dict rebuilds are
//! structurally deleted.
//!
//! - **Epoch identity** = (part identity §11, attno, path_ord). Codes are
//!   meaningless across epochs (Law A); epochs are part-scoped, so whole-band
//!   claims (spec §2) can never split one.
//! - **Sorted order is contractual**: global code order == byte order of the
//!   stored images; where an order embedding exists, code compare = value
//!   compare.
//! - **Payload entries are varlena-shaped, 8-aligned** (spec §1, StrView
//!   §7b): dict materialization can be zero-copy views into the payload
//!   region, which the reader keeps generation-stable (M3-F pin).
//! - **Publishability lattice**: the `STREAMF_DICT_EXEC` stream flag records
//!   whether code-eq == value-eq (execution lanes) or storage-dedup-only.
//!   The O-6 zero-null-proof refusal carries at the execution boundary
//!   (M3-G) until the chartered consumer null-audit lands.

use crate::geom::DICT_FRAME_ENTRIES;
use crate::wire::{put_u32, varlena_4b_u_payload_len, varlena_entry_at, Cur};
use crate::{FormatError, FormatResult};

pub const DICT_INDEX_ENTRY_LEN: usize = 12;

/// The DictIndex `char_field` FORM, named by the stream dir entry's flags
/// (spec §6.3) — never guessed, never a process posture:
/// - `Absolute` (no flag): `char_field == char_len` — the blessed lineage.
/// - `Delta` (`STREAMF_CHARLEN_DELTA`): `char_field == byte_len − char_len`
///   — the S8 §2(a) arm, MEASURED DECLINE (M5d package §3).
/// - `Absent` (`STREAMF_CHARLEN_ABSENT`): nothing stored (zeros) — the
///   §2(b)/Option-C lengths-prepass arm; consumers recompute from entry
///   bytes. Probe-only (M5d package §9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DictCharLenForm {
    Absolute,
    Delta,
    Absent,
}

impl DictCharLenForm {
    /// Decode from stream dir-entry flags (both bits set is corruption).
    pub fn from_flags(flags: u16) -> FormatResult<DictCharLenForm> {
        use crate::part::{STREAMF_CHARLEN_ABSENT, STREAMF_CHARLEN_DELTA};
        match (flags & STREAMF_CHARLEN_DELTA != 0, flags & STREAMF_CHARLEN_ABSENT != 0) {
            (false, false) => Ok(DictCharLenForm::Absolute),
            (true, false) => Ok(DictCharLenForm::Delta),
            (false, true) => Ok(DictCharLenForm::Absent),
            (true, true) => Err(FormatError::Corrupt {
                at: "dict char_len form flags",
            }),
        }
    }

    pub fn flag_bits(self) -> u16 {
        use crate::part::{STREAMF_CHARLEN_ABSENT, STREAMF_CHARLEN_DELTA};
        match self {
            DictCharLenForm::Absolute => 0,
            DictCharLenForm::Delta => STREAMF_CHARLEN_DELTA,
            DictCharLenForm::Absent => STREAMF_CHARLEN_ABSENT,
        }
    }
}

/// UTF-8 lead-byte count — the char-length fact's one definition (the
/// write-side `char_len_of` Utf8Chars arm verbatim; ill-formed input
/// degrades to a byte count for the malformed tail).
#[inline]
pub fn utf8_char_count(bytes: &[u8]) -> u32 {
    bytes.iter().filter(|&&b| (b & 0xC0) != 0x80).count() as u32
}

/// One dictionary index entry (12 B, spec §7): `payload_off` is
/// DictPayload-payload-relative and points at the entry's varlena header;
/// stored byte- AND char-lengths make `length()` a table lookup.
///
/// `char_field` carries the char-length fact in one of the
/// [`DictCharLenForm`] FORMS, named by the owning DictIndex stream's
/// dir-entry flags (never guessed). Consumers never see the form:
/// [`dict_entry`] hands back absolute `char_len` under every form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct DictIndexEntry {
    pub payload_off: u32,
    pub byte_len: u32,
    pub char_field: u32,
}

impl DictIndexEntry {
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        put_u32(out, self.payload_off);
        put_u32(out, self.byte_len);
        put_u32(out, self.char_field);
    }

    pub fn decode(c: &mut Cur<'_>) -> FormatResult<DictIndexEntry> {
        Ok(DictIndexEntry {
            payload_off: c.u32("DictIndexEntry")?,
            byte_len: c.u32("DictIndexEntry")?,
            char_field: c.u32("DictIndexEntry")?,
        })
    }
}

/// The dict frame an entry lives in (lazy-fault grain, spec §7).
pub const fn dict_frame_of_entry(code: u32) -> u32 {
    code / DICT_FRAME_ENTRIES
}

/// Publishability level achieved by a dict stream (spec §7). Recorded on
/// disk as the `STREAMF_DICT_EXEC` flag; this enum is the vocabulary the
/// writer's election and the scan's publication share.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DictPublish {
    /// code-eq == value-eq: group-on-codes, once-per-code eval are sound.
    ExecutionLane,
    /// Storage-level dedup only (bpchar, numeric, interval, float,
    /// jsonb-as-value, extension types).
    StorageDedup,
}

/// The dict sections a kernel ctx carries (spec §19.2): raw section payloads
/// (past their StreamSectionHdr), CRC-validated by the reader.
#[derive(Debug, Clone, Copy)]
pub struct DictSections<'a> {
    /// DictIndex stream payload: `DictIndexEntry` × entry_count.
    pub index: &'a [u8],
    /// DictPayload stream payload: varlena-shaped entries in byte-rank order.
    pub payload: &'a [u8],
    pub entry_count: u32,
    /// The DictIndex stream's `char_field` form fact (spec §6.3 flags).
    /// Carried here so [`dict_entry`] stays pure — the flags are read
    /// from the stream dir by whoever builds the ctx.
    pub charlen_form: DictCharLenForm,
}

/// One resolved dictionary entry.
#[derive(Debug, Clone, Copy)]
pub struct DictEntryRef<'a> {
    /// The whole varlena image (header + payload) — a valid PG datum image.
    pub image: &'a [u8],
    /// The payload bytes (header excluded).
    pub bytes: &'a [u8],
    pub byte_len: u32,
    pub char_len: u32,
}

/// Resolve entry `code` (pure, bounds-validated; the reader's lazy framed
/// handle builds on this).
pub fn dict_entry<'a>(d: &DictSections<'a>, code: u32) -> FormatResult<DictEntryRef<'a>> {
    if code >= d.entry_count {
        return Err(FormatError::Bounds { at: "dict code" });
    }
    let off = code as usize * DICT_INDEX_ENTRY_LEN;
    if off + DICT_INDEX_ENTRY_LEN > d.index.len() {
        return Err(FormatError::Bounds { at: "dict index" });
    }
    let mut c = Cur::new(&d.index[off..off + DICT_INDEX_ENTRY_LEN]);
    let e = DictIndexEntry::decode(&mut c)?;
    let (image, bytes) = varlena_entry_at(d.payload, e.payload_off as usize, "dict payload")?;
    if bytes.len() != e.byte_len as usize {
        return Err(FormatError::Corrupt {
            at: "dict entry byte_len",
        });
    }
    let char_len = match d.charlen_form {
        DictCharLenForm::Absolute => e.char_field,
        DictCharLenForm::Delta => {
            e.byte_len
                .checked_sub(e.char_field)
                .ok_or(FormatError::Corrupt {
                    at: "dict entry char_len delta",
                })?
        }
        // Option-C: nothing stored — recompute from the entry bytes in
        // hand (one lead-count walk; the char-length fact's one
        // definition, `utf8_char_count`).
        DictCharLenForm::Absent => utf8_char_count(bytes),
    };
    Ok(DictEntryRef {
        image,
        bytes,
        byte_len: e.byte_len,
        char_len,
    })
}

/// Resolve entry `code` to its DATUM WORD — the pointer to the entry's
/// varlena image inside `d.payload` — with the same validation surface as
/// [`dict_entry`] (code bound, index bound, header/`byte_len` two-witness
/// agreement, payload bound; typed errors, never UB) but no slice
/// materialization: the hot gather face.
///
/// **Zero-copy aliasing law (StrView §7b / §4).** The returned datum
/// aliases the caller's `d.payload` slice; it is a valid plain 4B-U PG
/// datum for as long as those bytes stay resident. On the read path the
/// payload is a part-cached `SegBuf` (8-aligned, insert-only, alive for the
/// `OpenPart`'s life), so entry datums are 8-aligned and outlive every
/// granule-claim consumption window. Callers that build `DictSections` over
/// transient buffers (write-path verify, tests, harnesses) must keep those
/// buffers alive while the datums are read, and only get absolute 8-align
/// if their buffer base is 8-aligned.
#[inline]
pub fn dict_entry_datum(d: &DictSections<'_>, code: u32) -> FormatResult<u64> {
    if code >= d.entry_count {
        return Err(FormatError::Bounds { at: "dict code" });
    }
    let off = code as usize * DICT_INDEX_ENTRY_LEN;
    let Some(e) = d.index.get(off..off + DICT_INDEX_ENTRY_LEN) else {
        return Err(FormatError::Bounds { at: "dict index" });
    };
    let payload_off = u32::from_le_bytes(e[..4].try_into().expect("len 4")) as usize;
    let byte_len = u32::from_le_bytes(e[4..8].try_into().expect("len 4")) as usize;
    let Some(hdr) = d.payload.get(payload_off..payload_off + 4) else {
        return Err(FormatError::Bounds { at: "dict payload" });
    };
    let header = u32::from_le_bytes(hdr.try_into().expect("len 4"));
    let plen = varlena_4b_u_payload_len(header, "dict payload")? as usize;
    if plen != byte_len {
        return Err(FormatError::Corrupt {
            at: "dict entry byte_len",
        });
    }
    if payload_off + 4 + plen > d.payload.len() {
        return Err(FormatError::Bounds { at: "dict payload" });
    }
    Ok(d.payload[payload_off..].as_ptr() as u64)
}

/// Geometry facts the `dict_handle` face reports (spec §19.3) for the
/// reader's lazy handle construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DictLayout {
    pub entry_count: u32,
    pub frame_entries: u32,
}
