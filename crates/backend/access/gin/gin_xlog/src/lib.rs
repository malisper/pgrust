//! ginxlog.c — GIN rmgr redo + gin_mask, including ginRedoRecompress's
//! in-place conversion of pre-9.4 uncompressed leaves.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use gin_vocab::*;
use types_core::{BlockNumber, Buffer, InvalidBlockNumber, OffsetNumber, BLCKSZ};
use types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED};
use types_storage::bufpage::{PageMut, SizeOfPageHeaderData as SIZE_OF_PAGE_HEADER};
use types_storage::RelFileLocator;
use types_tuple::itemptr::{FirstOffsetNumber, InvalidOffsetNumber, ItemPointerData};
use xlogreader_seams::XLogReaderState;
use xlogutils::{XLogInitBufferForRedo, XLogReadBufferForRedo, BLK_NEEDS_REDO, BLK_RESTORED};

const XLR_INFO_MASK: u8 = 0x0F;
const SIZEOF_OPAQUE: usize = core::mem::size_of::<GinPageOpaqueData>();
const OPAQUE_OFF: usize = BLCKSZ - SIZEOF_OPAQUE;
const ITUP_SIZE_MASK: u16 = 0x1FFF;

/// DST fault-sweep RED hook (sim-cfg only, zero native surface): when armed,
/// `redo_insert_listpage` restores the pending-list page's STRUCTURE
/// (init, flags, rightlink) but SKIPS its tuple content — a deliberately
/// weakened redo modeling a lost pending-list content restore. The crash
/// sweep's gin red leg arms this and must CATCH the silent loss through its
/// index-coverage property (never through a replay failure).
#[cfg(pgrust_sim)]
pub mod sim_red {
    use core::sync::atomic::{AtomicBool, Ordering::Relaxed};
    pub static SKIP_LISTPAGE_CONTENT: AtomicBool = AtomicBool::new(false);
    pub fn armed() -> bool {
        SKIP_LISTPAGE_CONTENT.load(Relaxed)
    }
}

fn main_data<'a>(record: &'a XLogReaderState) -> &'a [u8] {
    let rec = record.record.as_ref().expect("gin redo with no decoded record");
    // SAFETY: points into the reader's decode buffer, valid for the redo
    // callback's duration.
    unsafe { rec.main_data_bytes() }
}

fn block_data<'a>(record: &'a XLogReaderState, block_id: u8) -> &'a [u8] {
    unsafe { record.block(block_id).data_bytes() }
}

// SAFETY contract shared by the redo arms: buffer pinned + exclusively locked
// (XLogReadBufferForRedo protocol) — sole writer until the unlock.
unsafe fn page_mut<'p>(buffer: Buffer) -> PageMut<'p> {
    unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) }
}

unsafe fn page_bytes_mut<'p>(buffer: Buffer) -> &'p mut [u8] {
    unsafe {
        core::slice::from_raw_parts_mut(
            bufmgr_seams::buffer_get_page::call(buffer).as_ptr(),
            BLCKSZ,
        )
    }
}

fn unlock_release(buffer: Buffer) -> PgResult<()> {
    bufmgr_seams::lock_buffer::call(buffer, bufmgr_seams::BUFFER_LOCK_UNLOCK)?;
    bufmgr_seams::release_buffer::call(buffer)
}

fn opaque_of(bytes: &[u8]) -> GinPageOpaqueData {
    // SAFETY: in-bounds 4-aligned special area of a BLCKSZ image.
    unsafe { bytes.as_ptr().add(OPAQUE_OFF).cast::<GinPageOpaqueData>().read() }
}

fn write_opaque_to(bytes: &mut [u8], o: &GinPageOpaqueData) {
    // SAFETY: as opaque_of; exclusive access.
    unsafe {
        bytes
            .as_mut_ptr()
            .add(OPAQUE_OFF)
            .cast::<GinPageOpaqueData>()
            .write(*o)
    }
}

fn gin_init_page_bytes(bytes: &mut [u8], flags: u16) {
    // SAFETY: owned exclusive BLCKSZ image.
    let mut page =
        unsafe { PageMut::from_raw(core::ptr::NonNull::new(bytes.as_mut_ptr()).unwrap()) };
    page.init(SIZEOF_OPAQUE);
    write_opaque_to(
        bytes,
        &GinPageOpaqueData {
            rightlink: InvalidBlockNumber,
            maxoff: 0,
            flags,
        },
    );
}

fn set_data_page_data_size(bytes: &mut [u8], size: usize) {
    debug_assert!(size <= GinDataPageMaxDataSize);
    let lower = (size + GinDataPageDataOffset) as u16;
    bytes[12..14].copy_from_slice(&lower.to_ne_bytes());
}

fn set_meta(bytes: &mut [u8], meta: &[u8]) {
    bytes[SizeOfPageHeaderData..SizeOfPageHeaderData + 56].copy_from_slice(meta);
    let lower = (SizeOfPageHeaderData + 56) as u16;
    bytes[12..14].copy_from_slice(&lower.to_ne_bytes());
}

fn itup_size(stream: &[u8]) -> usize {
    (u16::from_ne_bytes([stream[6], stream[7]]) & ITUP_SIZE_MASK) as usize
}

fn set_lsn(buffer: Buffer, lsn: u64) {
    // SAFETY: pin + exclusive lock held.
    unsafe { page_mut(buffer) }.set_lsn(lsn);
}

#[track_caller]
#[cold]
fn error_err(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg))
}

/// ginxlog.c:132-164: convert a pre-9.4 (uncompressed) posting-tree leaf to
/// the compressed format in place — the raw ItemPointerData array at
/// GinDataPageGetData (maxoff items) becomes one posting-list segment
/// (ginCompressPostingList with maxsize BLCKSZ packs every item), pd_lower
/// records its size, GIN_COMPRESSED is set and maxoff cleared. An empty leaf
/// (leftmost/rightmost pages are never deleted, so pg_upgrade'd instances may
/// carry them) converts to an empty posting list. maxoff and the encoded size
/// are checked against the data area: C asserts npacked == nuncompressed and
/// memcpys unchecked, so an overrun is corruption here.
#[cold]
#[inline(never)]
fn redo_convert_uncompressed_leaf(bytes: &mut [u8]) -> PgResult<()> {
    let mut o = opaque_of(bytes);
    let nuncompressed = o.maxoff as usize;
    if nuncompressed * 6 > GinDataPageMaxDataSize {
        return Err(corrupt_err(format!(
            "GIN redo recompress: pre-9.4 leaf item count {nuncompressed} exceeds the page data area"
        )));
    }
    let totalsize = if nuncompressed > 0 {
        let mut items: Vec<u64> = Vec::with_capacity(nuncompressed);
        for i in 0..nuncompressed {
            items.push(read_item(bytes, GinDataPageDataOffset + i * 6));
        }
        let plist = encode_items(&items);
        if plist.len() > GinDataPageMaxDataSize {
            return Err(corrupt_err(format!(
                "GIN redo recompress: pre-9.4 leaf posting list size {} exceeds page capacity {GinDataPageMaxDataSize}",
                plist.len()
            )));
        }
        bytes[GinDataPageDataOffset..GinDataPageDataOffset + plist.len()].copy_from_slice(&plist);
        plist.len()
    } else {
        0
    };
    set_data_page_data_size(bytes, totalsize);
    o.flags |= GIN_COMPRESSED;
    o.maxoff = InvalidOffsetNumber;
    write_opaque_to(bytes, &o);
    Ok(())
}

/// Malformed replayed WAL is a corruption condition, not a bug: report it as a
/// catchable ERRCODE_DATA_CORRUPTED error so the startup/recovery thread fails
/// the record instead of panicking (which would SIGABRT and re-panic at the
/// same LSN on every restart — a persistent crash loop).
#[track_caller]
#[cold]
fn corrupt_err(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_DATA_CORRUPTED))
}

/// Validate that a WAL payload (main-data or block-data, both attacker-declared
/// in length — 0 is legal and passes all xlogreader validation) covers at least
/// `need` bytes before a fixed-offset decode. C reads these fields through raw
/// pointer casts and tolerates a short record by reading adjacent garbage; the
/// Rust port must turn the same input into a controlled error rather than a
/// slice-bounds panic.
fn require_len(data: &[u8], need: usize, what: &str) -> PgResult<()> {
    if data.len() < need {
        return Err(corrupt_err(format!(
            "GIN redo: {what} record too short: {} bytes, need at least {need}",
            data.len()
        )));
    }
    Ok(())
}

/// Read an embedded IndexTuple size word and validate it against the remaining
/// payload. Rejects a truncated header, a size word that overruns the payload,
/// and a zero size (which would also spin the ntuples loops forever).
fn checked_itup_size(stream: &[u8], what: &str) -> PgResult<usize> {
    if stream.len() < 8 {
        return Err(corrupt_err(format!(
            "GIN redo: {what} truncated index tuple: {} bytes, need at least 8",
            stream.len()
        )));
    }
    let n = itup_size(stream);
    if n == 0 || n > stream.len() {
        return Err(corrupt_err(format!(
            "GIN redo: {what} index tuple size {n} out of range (payload {} bytes)",
            stream.len()
        )));
    }
    Ok(n)
}

/// Read a GIN posting-list segment size from the front of `b`, validating the
/// header is present and the whole segment fits within `b`.
fn seg_size_checked(b: &[u8], what: &str) -> PgResult<usize> {
    if b.len() < SizeOfGinPostingListHeader {
        return Err(corrupt_err(format!(
            "GIN redo recompress: {what} truncated posting-list header: {} bytes",
            b.len()
        )));
    }
    let n = size_of_gin_posting_list(u16::from_ne_bytes([b[6], b[7]]) as usize);
    if n > b.len() {
        return Err(corrupt_err(format!(
            "GIN redo recompress: {what} posting-list segment size {n} exceeds {} available bytes",
            b.len()
        )));
    }
    Ok(n)
}

/// ginRedoClearIncompleteSplit.
fn clear_incomplete_split(record: &XLogReaderState, block_id: u8) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let (action, buffer) = XLogReadBufferForRedo(record, block_id)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: redo lock protocol.
        let bytes = unsafe { page_bytes_mut(buffer) };
        let mut o = opaque_of(bytes);
        o.flags &= !GIN_INCOMPLETE_SPLIT;
        write_opaque_to(bytes, &o);
        set_lsn(buffer, lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != types_core::InvalidBuffer {
        unlock_release(buffer)?;
    }
    Ok(())
}

/// ginRedoCreatePTree.
fn redo_create_ptree(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let data = main_data(record);
    require_len(data, 4, "create-ptree")?;
    let size = u32::from_ne_bytes(data[0..4].try_into().unwrap()) as usize;
    require_len(data, 4 + size, "create-ptree posting list")?;
    if size > GinDataPageMaxDataSize {
        return Err(corrupt_err(format!(
            "GIN redo: create-ptree posting list size {size} exceeds page capacity {GinDataPageMaxDataSize}"
        )));
    }

    let buffer = XLogInitBufferForRedo(record, 0)?;
    // SAFETY: redo lock protocol.
    let bytes = unsafe { page_bytes_mut(buffer) };
    gin_init_page_bytes(bytes, GIN_DATA | GIN_LEAF | GIN_COMPRESSED);
    bytes[GinDataPageDataOffset..GinDataPageDataOffset + size].copy_from_slice(&data[4..4 + size]);
    set_data_page_data_size(bytes, size);
    set_lsn(buffer, lsn);
    bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    unlock_release(buffer)
}

/// ginRedoInsertEntry.
fn redo_insert_entry(
    buffer: Buffer,
    locator: RelFileLocator,
    rightblkno: BlockNumber,
    rdata: &[u8],
) -> PgResult<()> {
    // ginxlogInsertEntry: offset @0, isDelete @2, tuple @4 (variable length).
    require_len(rdata, 4, "insert-entry")?;
    let offset = u16::from_ne_bytes([rdata[0], rdata[1]]) as OffsetNumber;
    let is_delete = rdata[2] != 0;
    let tuple = &rdata[4..];
    let tuplen = checked_itup_size(tuple, "insert-entry")?;

    // SAFETY: redo lock protocol.
    let mut page = unsafe { page_mut(buffer) };

    if rightblkno != InvalidBlockNumber {
        let id = page.as_ref().item_id(offset);
        let itup = page.as_ref().item_raw(id).0.cast_mut();
        // GinSetDownlink: t_tid = (rightblkno, InvalidOffsetNumber).
        // SAFETY: itup within the exclusively held page.
        unsafe {
            let mut tid = ItemPointerData::invalid();
            types_tuple::itemptr::ItemPointerSet(
                &mut tid,
                rightblkno,
                types_tuple::itemptr::InvalidOffsetNumber,
            );
            itup.cast::<ItemPointerData>().write_unaligned(tid);
        }
    }

    if is_delete {
        page.index_tuple_delete(offset);
    }

    if page.add_item(&tuple[..tuplen], offset, 0).is_none() {
        // ginxlog.c:96-104: BufferGetTag(buffer) -> the block's locator (the
        // record's block reference names the same relation file).
        return Err(error_err(format!(
            "failed to add item to index page in {}/{}/{}",
            locator.spcOid, locator.dbOid, locator.relNumber
        )));
    }
    Ok(())
}

/// ginRedoRecompress.
fn redo_recompress(buffer: Buffer, rdata: &[u8]) -> PgResult<()> {
    // SAFETY: redo lock protocol.
    let bytes = unsafe { page_bytes_mut(buffer) };
    if opaque_of(bytes).flags & GIN_COMPRESSED == 0 {
        // ginxlog.c:132-164: convert the pre-9.4 page first.
        redo_convert_uncompressed_leaf(bytes)?;
    }

    // ginxlogRecompressDataLeaf: nactions @0 (uint16), action stream follows.
    require_len(rdata, 2, "recompress")?;
    let nactions = u16::from_ne_bytes([rdata[0], rdata[1]]) as usize;
    let mut walbuf = &rdata[2..];

    let list_start = GinDataPageDataOffset;
    // The posting-list area ends at the page's special/opaque pointer; every
    // write extent must stay within it (C asserts writePtr + n <= special).
    let page_end = OPAQUE_OFF;
    let pd_lower = u16::from_ne_bytes([bytes[12], bytes[13]]) as usize;
    // pd_lower is restorable from a hostile full-page image; bound it before it
    // is used to slice the original posting-list area.
    if pd_lower < list_start || pd_lower > page_end {
        return Err(corrupt_err(format!(
            "GIN redo recompress: pd_lower {pd_lower} outside posting-list bounds [{list_start}, {page_end}]"
        )));
    }
    let orig: Vec<u8> = bytes[list_start..pd_lower].to_vec();

    let mut oldoff = 0usize; // offset into orig
    let mut write_ptr = list_start;
    let mut segno = 0usize;

    for _ in 0..nactions {
        require_len(walbuf, 2, "recompress action header")?;
        let a_segno = walbuf[0] as usize;
        let mut a_action = walbuf[1];
        walbuf = &walbuf[2..];

        let mut newseg: &[u8] = &[];
        if a_action == GIN_SEGMENT_INSERT || a_action == GIN_SEGMENT_REPLACE {
            let n = seg_size_checked(walbuf, "new segment")?;
            newseg = &walbuf[..n];
            // n == size_of_gin_posting_list(..) is already short-aligned, so
            // SHORTALIGN(n) == n and stays within walbuf (checked above).
            walbuf = &walbuf[SHORTALIGN(n)..];
        }

        let mut additems: &[u8] = &[];
        if a_action == GIN_SEGMENT_ADDITEMS {
            require_len(walbuf, 2, "recompress additems count")?;
            let nitems = u16::from_ne_bytes([walbuf[0], walbuf[1]]) as usize;
            let end = 2 + nitems * 6;
            require_len(walbuf, end, "recompress additems")?;
            additems = &walbuf[2..end];
            walbuf = &walbuf[end..];
        }

        if segno > a_segno {
            return Err(corrupt_err(format!(
                "GIN redo recompress: action segment {a_segno} precedes current segment {segno}"
            )));
        }
        while segno < a_segno {
            let n = seg_size_checked(&orig[oldoff..], "unmodified segment")?;
            if write_ptr + n > page_end {
                return Err(corrupt_err(
                    "GIN redo recompress: unmodified segment overflows page".into(),
                ));
            }
            bytes[write_ptr..write_ptr + n].copy_from_slice(&orig[oldoff..oldoff + n]);
            write_ptr += n;
            oldoff += n;
            segno += 1;
        }

        let merged;
        if a_action == GIN_SEGMENT_ADDITEMS {
            let oldn = seg_size_checked(&orig[oldoff..], "additems target segment")?;
            merged = recompress_additems(&orig[oldoff..oldoff + oldn], additems)?;
            newseg = &merged;
            a_action = GIN_SEGMENT_REPLACE;
        }

        let at_end = oldoff >= orig.len();
        let segsize = if at_end {
            if a_action != GIN_SEGMENT_INSERT {
                return Err(corrupt_err(format!(
                    "GIN redo recompress: action {a_action} past last segment (only INSERT expected)"
                )));
            }
            0
        } else {
            seg_size_checked(&orig[oldoff..], "current segment")?
        };

        match a_action {
            GIN_SEGMENT_DELETE => {
                oldoff += segsize;
                segno += 1;
            }
            GIN_SEGMENT_INSERT => {
                if write_ptr + newseg.len() > page_end {
                    return Err(corrupt_err(
                        "GIN redo recompress: inserted segment overflows page".into(),
                    ));
                }
                bytes[write_ptr..write_ptr + newseg.len()].copy_from_slice(newseg);
                write_ptr += newseg.len();
            }
            GIN_SEGMENT_REPLACE => {
                if write_ptr + newseg.len() > page_end {
                    return Err(corrupt_err(
                        "GIN redo recompress: replacement segment overflows page".into(),
                    ));
                }
                bytes[write_ptr..write_ptr + newseg.len()].copy_from_slice(newseg);
                write_ptr += newseg.len();
                oldoff += segsize;
                segno += 1;
            }
            // ginxlog.c:298 elog(ERROR) XX000.
            other => return Err(error_err(format!("unexpected GIN leaf action: {other}"))),
        }
    }

    if oldoff < orig.len() {
        let rest = orig.len() - oldoff;
        if write_ptr + rest > page_end {
            return Err(corrupt_err(
                "GIN redo recompress: trailing segments overflow page".into(),
            ));
        }
        bytes[write_ptr..write_ptr + rest].copy_from_slice(&orig[oldoff..]);
        write_ptr += rest;
    }

    set_data_page_data_size(bytes, write_ptr - list_start);
    Ok(())
}

fn recompress_additems(oldseg: &[u8], items: &[u8]) -> PgResult<Vec<u8>> {
    let mut old: Vec<u64> = Vec::new();
    {
        let first = read_item(oldseg, 0);
        let nbytes = u16::from_ne_bytes([oldseg[6], oldseg[7]]) as usize;
        let mut val = first;
        old.push(val);
        let payload = &oldseg[8..8 + nbytes];
        let mut pos = 0usize;
        while pos < nbytes {
            let mut delta = 0u64;
            let mut shift = 0u32;
            loop {
                if pos >= nbytes {
                    return Err(corrupt_err(format!(
                        "GIN redo recompress: ADDITEMS segment ends inside a varbyte item at {nbytes} bytes"
                    )));
                }
                let c = payload[pos] as u64;
                pos += 1;
                if shift == 42 {
                    delta |= c << 42;
                    break;
                }
                delta |= (c & 0x7F) << shift;
                if c & 0x80 == 0 {
                    break;
                }
                shift += 7;
            }
            val += delta;
            old.push(val);
        }
    }
    let mut new: Vec<u64> = Vec::with_capacity(items.len() / 6);
    let mut off = 0usize;
    while off < items.len() {
        new.push(read_item(items, off));
        off += 6;
    }

    let mut all: Vec<u64> = Vec::with_capacity(old.len() + new.len());
    let (mut i, mut j) = (0usize, 0usize);
    while i < old.len() && j < new.len() {
        if old[i] < new[j] {
            all.push(old[i]);
            i += 1;
        } else {
            all.push(new[j]);
            j += 1;
        }
    }
    all.extend_from_slice(&old[i..]);
    all.extend_from_slice(&new[j..]);

    Ok(encode_items(&all))
}

/// One posting-list segment (ginCompressPostingList's image, no size cap) over
/// sorted item values: first item verbatim, nbytes, varbyte deltas, SHORTALIGN.
fn encode_items(all: &[u64]) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::with_capacity(8 + all.len() * 7);
    out.extend_from_slice(&[0u8; 8]);
    write_item(&mut out, 0, all[0]);
    let mut prev = all[0];
    for &v in &all[1..] {
        let mut delta = v - prev;
        while delta > 0x7F {
            out.push(0x80 | (delta & 0x7F) as u8);
            delta >>= 7;
        }
        out.push(delta as u8);
        prev = v;
    }
    let nbytes = out.len() - 8;
    out[6..8].copy_from_slice(&(nbytes as u16).to_ne_bytes());
    if nbytes & 1 != 0 {
        out.push(0);
    }
    out
}

fn read_item(b: &[u8], off: usize) -> u64 {
    let hi = u16::from_ne_bytes([b[off], b[off + 1]]) as u64;
    let lo = u16::from_ne_bytes([b[off + 2], b[off + 3]]) as u64;
    let posid = u16::from_ne_bytes([b[off + 4], b[off + 5]]) as u64;
    (((hi << 16) | lo) << 11) | posid
}

fn write_item(out: &mut [u8], off: usize, val: u64) {
    let blk = (val >> 11) as u32;
    let posid = (val & 0x7FF) as u16;
    out[off..off + 2].copy_from_slice(&((blk >> 16) as u16).to_ne_bytes());
    out[off + 2..off + 4].copy_from_slice(&((blk & 0xffff) as u16).to_ne_bytes());
    out[off + 4..off + 6].copy_from_slice(&posid.to_ne_bytes());
}

/// Validate a WAL-supplied posting-item offset for an internal-page insert
/// before it drives raw-pointer writes of a 10-byte PostingItem into the page.
/// C's ginRedoInsertData / GinDataPageAddPostingItem trust `data->offset`
/// under Assert only (compiled out in release), so a hostile or corrupt record
/// can place the write far past the page. Mirror the C contract
/// (dataPlaceToPageInternal produces 1..=maxoff+1) as a hard, catchable check:
/// reject offset 0 or > maxoff+1, and reject a maxoff whose resulting posting
/// items would not fit the page (a full-page image can plant an arbitrary
/// maxoff). Callers pass the current on-page maxoff.
fn checked_posting_item_offset(offset: OffsetNumber, maxoff: OffsetNumber) -> PgResult<()> {
    let maxoff = maxoff as usize;
    if offset == 0 || offset as usize > maxoff + 1 {
        return Err(corrupt_err(format!(
            "GIN redo: insert-data internal offset {offset} out of range [1, {}]",
            maxoff + 1
        )));
    }
    if (maxoff + 1) * 10 > GinDataPageMaxDataSize {
        return Err(corrupt_err(format!(
            "GIN redo: insert-data internal item extent {} exceeds page capacity {GinDataPageMaxDataSize}",
            (maxoff + 1) * 10
        )));
    }
    Ok(())
}

/// ginRedoInsertData (internal page arm) + ginRedoRecompress (leaf arm).
fn redo_insert_data(
    buffer: Buffer,
    is_leaf: bool,
    rightblkno: BlockNumber,
    rdata: &[u8],
) -> PgResult<()> {
    if is_leaf {
        redo_recompress(buffer, rdata)
    } else {
        // ginxlogInsertDataInternal: offset @0 (uint16) + PostingItem @2 (10-byte POD).
        require_len(rdata, 12, "insert-data internal")?;
        let offset = u16::from_ne_bytes([rdata[0], rdata[1]]) as OffsetNumber;
        // SAFETY: PostingItem is a 10-byte POD in the WAL image.
        let newitem =
            unsafe { rdata.as_ptr().add(2).cast::<PostingItem>().read_unaligned() };
        // SAFETY: redo lock protocol.
        let bytes = unsafe { page_bytes_mut(buffer) };

        let mut o = opaque_of(bytes);
        let maxoff = o.maxoff;
        // Validate the WAL-supplied offset against the page's maxoff BEFORE any
        // write: the two unsafe PostingItem writes below index the page at
        // `offset` and shift maxoff-offset+1 items, so an unchecked offset (up
        // to 65535) is an out-of-page write into neighbouring buffer-pool
        // memory. C guards this with Assert only.
        checked_posting_item_offset(offset, maxoff)?;

        let p = GinDataPageDataOffset + (offset as usize - 1) * 10;
        // SAFETY: offset validated in [1, maxoff+1] and the page has room for
        // maxoff+1 posting items, so p is an in-bounds slot.
        unsafe {
            let mut old = bytes.as_ptr().add(p).cast::<PostingItem>().read_unaligned();
            PostingItemSetBlockNumber(&mut old, rightblkno);
            bytes.as_mut_ptr().add(p).cast::<PostingItem>().write_unaligned(old);
        }

        if offset != maxoff + 1 {
            let start = GinDataPageDataOffset + (offset as usize - 1) * 10;
            // offset <= maxoff here (offset != maxoff+1 and validated <= maxoff+1),
            // so this does not underflow.
            let n = (maxoff as usize - offset as usize + 1) * 10;
            bytes.copy_within(start..start + n, start + 10);
        }
        let dst = GinDataPageDataOffset + (offset as usize - 1) * 10;
        // SAFETY: in-bounds slot after the shift.
        unsafe {
            bytes
                .as_mut_ptr()
                .add(dst)
                .cast::<PostingItem>()
                .write_unaligned(newitem)
        };
        o.maxoff = maxoff + 1;
        write_opaque_to(bytes, &o);
        set_data_page_data_size(bytes, o.maxoff as usize * 10);
        Ok(())
    }
}

/// ginRedoInsert.
fn redo_insert(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let data = main_data(record);
    // ginxlogInsert: flags @0 (uint16); for a non-leaf, BlockIdData[2] (left,
    // right child) follows at @2 and @6.
    require_len(data, 2, "insert")?;
    let flags = u16::from_ne_bytes([data[0], data[1]]);
    let is_leaf = flags & GIN_INSERT_ISLEAF != 0;
    let is_data = flags & GIN_INSERT_ISDATA != 0;

    let mut right_child_blkno = InvalidBlockNumber;
    if !is_leaf {
        require_len(data, 10, "insert non-leaf child links")?;
        let rc_hi = u16::from_ne_bytes([data[6], data[7]]) as u32;
        let rc_lo = u16::from_ne_bytes([data[8], data[9]]) as u32;
        right_child_blkno = (rc_hi << 16) | rc_lo;
        clear_incomplete_split(record, 1)?;
    }

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        let payload = block_data(record, 0);
        if is_data {
            redo_insert_data(buffer, is_leaf, right_child_blkno, payload)?;
        } else {
            redo_insert_entry(buffer, record.block(0).rlocator, right_child_blkno, payload)?;
        }
        set_lsn(buffer, lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != types_core::InvalidBuffer {
        unlock_release(buffer)?;
    }
    Ok(())
}

/// ginRedoSplit.
fn redo_split(record: &XLogReaderState) -> PgResult<()> {
    let data = main_data(record);
    // ginxlogSplit: locator(12) + rrlink(4) + leftChild(4) + rightChild(4) + flags @24.
    require_len(data, 26, "split")?;
    let flags = u16::from_ne_bytes([data[24], data[25]]);
    let is_leaf = flags & GIN_INSERT_ISLEAF != 0;
    let is_root = flags & GIN_SPLIT_ROOT != 0;

    if !is_leaf {
        clear_incomplete_split(record, 3)?;
    }

    let (laction, lbuffer) = XLogReadBufferForRedo(record, 0)?;
    if laction != BLK_RESTORED {
        return Err(error_err(
            "GIN split record did not contain a full-page image of left page".into(),
        ));
    }
    let (raction, rbuffer) = XLogReadBufferForRedo(record, 1)?;
    if raction != BLK_RESTORED {
        return Err(error_err(
            "GIN split record did not contain a full-page image of right page".into(),
        ));
    }
    if is_root {
        let (rootaction, rootbuf) = XLogReadBufferForRedo(record, 2)?;
        if rootaction != BLK_RESTORED {
            return Err(error_err(
                "GIN split record did not contain a full-page image of root page".into(),
            ));
        }
        unlock_release(rootbuf)?;
    }
    unlock_release(rbuffer)?;
    unlock_release(lbuffer)
}

/// ginRedoUpdateMetapage.
fn redo_update_metapage(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let data = main_data(record);
    // ginxlogUpdateMeta: metadata @16, prevTail @72, newRightlink @76, ntuples @80.
    require_len(data, 84, "update-metapage")?;
    let metadata = &data[16..72];
    let prev_tail = u32::from_ne_bytes(data[72..76].try_into().unwrap());
    let new_rightlink = u32::from_ne_bytes(data[76..80].try_into().unwrap());
    let ntuples = i32::from_ne_bytes(data[80..84].try_into().unwrap());

    let metabuffer = XLogInitBufferForRedo(record, 0)?;
    {
        // SAFETY: redo lock protocol.
        let bytes = unsafe { page_bytes_mut(metabuffer) };
        gin_init_page_bytes(bytes, GIN_META);
        set_meta(bytes, metadata);
    }
    set_lsn(metabuffer, lsn);
    bufmgr_seams::mark_buffer_dirty::call(metabuffer)?;

    if ntuples > 0 {
        let (action, buffer) = XLogReadBufferForRedo(record, 1)?;
        if action == BLK_NEEDS_REDO {
            let payload = block_data(record, 1);
            // SAFETY: redo lock protocol.
            let mut page = unsafe { page_mut(buffer) };
            let mut off = if page.as_ref().pd_lower() as usize <= SIZE_OF_PAGE_HEADER {
                FirstOffsetNumber
            } else {
                page.as_ref().max_offset_number() + 1
            };
            let mut p = 0usize;
            for _ in 0..ntuples {
                let tupsize = checked_itup_size(&payload[p..], "update-metapage tuple")?;
                if page.add_item(&payload[p..p + tupsize], off, 0).is_none() {
                    return Err(error_err("failed to add item to index page".into()));
                }
                p += tupsize;
                off += 1;
            }
            debug_assert!(p == payload.len());
            // SAFETY: redo lock protocol.
            let bytes = unsafe { page_bytes_mut(buffer) };
            let mut o = opaque_of(bytes);
            o.maxoff += 1;
            write_opaque_to(bytes, &o);
            set_lsn(buffer, lsn);
            bufmgr_seams::mark_buffer_dirty::call(buffer)?;
        }
        if buffer != types_core::InvalidBuffer {
            unlock_release(buffer)?;
        }
    } else if prev_tail != InvalidBlockNumber {
        let (action, buffer) = XLogReadBufferForRedo(record, 1)?;
        if action == BLK_NEEDS_REDO {
            // SAFETY: redo lock protocol.
            let bytes = unsafe { page_bytes_mut(buffer) };
            let mut o = opaque_of(bytes);
            o.rightlink = new_rightlink;
            write_opaque_to(bytes, &o);
            set_lsn(buffer, lsn);
            bufmgr_seams::mark_buffer_dirty::call(buffer)?;
        }
        if buffer != types_core::InvalidBuffer {
            unlock_release(buffer)?;
        }
    }

    unlock_release(metabuffer)
}

/// ginRedoInsertListPage.
fn redo_insert_listpage(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let data = main_data(record);
    // ginxlogInsertListPage: rightlink @0, ntuples @4.
    require_len(data, 8, "insert-listpage")?;
    let rightlink = u32::from_ne_bytes(data[0..4].try_into().unwrap());
    let ntuples = i32::from_ne_bytes(data[4..8].try_into().unwrap());

    let buffer = XLogInitBufferForRedo(record, 0)?;
    {
        // SAFETY: redo lock protocol.
        let bytes = unsafe { page_bytes_mut(buffer) };
        gin_init_page_bytes(bytes, GIN_LIST);
        let mut o = opaque_of(bytes);
        o.rightlink = rightlink;
        if rightlink == InvalidBlockNumber {
            o.flags |= GIN_LIST_FULLROW;
            o.maxoff = 1;
        } else {
            o.maxoff = 0;
        }
        write_opaque_to(bytes, &o);
    }
    // DST RED (sim-cfg only): the deliberately weakened redo — page structure
    // restored above, tuple content skipped. See sim_red.
    #[cfg(pgrust_sim)]
    if crate::sim_red::armed() {
        set_lsn(buffer, lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
        return unlock_release(buffer);
    }
    {
        let payload = block_data(record, 0);
        // SAFETY: redo lock protocol.
        let mut page = unsafe { page_mut(buffer) };
        let mut off = FirstOffsetNumber;
        let mut p = 0usize;
        for _ in 0..ntuples {
            let tupsize = checked_itup_size(&payload[p..], "insert-listpage tuple")?;
            if page.add_item(&payload[p..p + tupsize], off, 0).is_none() {
                return Err(error_err("failed to add item to index page".into()));
            }
            p += tupsize;
            off += 1;
        }
        debug_assert!(p == payload.len());
    }
    set_lsn(buffer, lsn);
    bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    unlock_release(buffer)
}

/// ginRedoDeleteListPages.
fn redo_delete_listpages(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let data = main_data(record);
    // ginxlogDeleteListPages: metadata @0 (56), ndeleted @56.
    require_len(data, 60, "delete-listpages")?;
    let metadata = &data[0..56];
    let ndeleted = i32::from_ne_bytes(data[56..60].try_into().unwrap());

    let metabuffer = XLogInitBufferForRedo(record, 0)?;
    {
        // SAFETY: redo lock protocol.
        let bytes = unsafe { page_bytes_mut(metabuffer) };
        gin_init_page_bytes(bytes, GIN_META);
        set_meta(bytes, metadata);
    }
    set_lsn(metabuffer, lsn);
    bufmgr_seams::mark_buffer_dirty::call(metabuffer)?;

    for i in 0..ndeleted {
        let buffer = XLogInitBufferForRedo(record, (i + 1) as u8)?;
        {
            // SAFETY: redo lock protocol.
            let bytes = unsafe { page_bytes_mut(buffer) };
            gin_init_page_bytes(bytes, GIN_DELETED);
        }
        set_lsn(buffer, lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
        unlock_release(buffer)?;
    }
    unlock_release(metabuffer)
}

/// ginRedoVacuumDataLeafPage.
fn redo_vacuum_data_leaf_page(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        let payload = block_data(record, 0);
        redo_recompress(buffer, payload)?;
        set_lsn(buffer, lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != types_core::InvalidBuffer {
        unlock_release(buffer)?;
    }
    Ok(())
}

fn redo_vacuum_page(record: &XLogReaderState) -> PgResult<()> {
    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action != BLK_RESTORED {
        return Err(error_err(
            "replay of gin entry tree page vacuum did not restore the page".into(),
        ));
    }
    unlock_release(buffer)
}

/// ginRedoDeletePage.
fn redo_delete_page(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let data = main_data(record);
    // ginxlogDeletePage: parentOffset @0, rightLink @4, deleteXid @8.
    require_len(data, 12, "delete-page")?;
    let parent_offset = u16::from_ne_bytes([data[0], data[1]]) as OffsetNumber;
    let right_link = u32::from_ne_bytes(data[4..8].try_into().unwrap());
    let delete_xid = u32::from_ne_bytes(data[8..12].try_into().unwrap());

    let (laction, lbuffer) = XLogReadBufferForRedo(record, 2)?;
    if laction == BLK_NEEDS_REDO {
        // SAFETY: redo lock protocol.
        let bytes = unsafe { page_bytes_mut(lbuffer) };
        let mut o = opaque_of(bytes);
        o.rightlink = right_link;
        write_opaque_to(bytes, &o);
        set_lsn(lbuffer, lsn);
        bufmgr_seams::mark_buffer_dirty::call(lbuffer)?;
    }

    let (daction, dbuffer) = XLogReadBufferForRedo(record, 0)?;
    if daction == BLK_NEEDS_REDO {
        // SAFETY: redo lock protocol.
        let bytes = unsafe { page_bytes_mut(dbuffer) };
        let mut o = opaque_of(bytes);
        o.flags |= GIN_DELETED;
        write_opaque_to(bytes, &o);
        // GinPageSetDeleteXid: pd_prune_xid.
        bytes[20..24].copy_from_slice(&delete_xid.to_ne_bytes());
        set_lsn(dbuffer, lsn);
        bufmgr_seams::mark_buffer_dirty::call(dbuffer)?;
    }

    let (paction, pbuffer) = XLogReadBufferForRedo(record, 1)?;
    if paction == BLK_NEEDS_REDO {
        // SAFETY: redo lock protocol.
        let bytes = unsafe { page_bytes_mut(pbuffer) };
        let mut o = opaque_of(bytes);
        let maxoff = o.maxoff;
        if parent_offset != maxoff {
            let dst = GinDataPageDataOffset + (parent_offset as usize - 1) * 10;
            let src = dst + 10;
            let n = (maxoff - parent_offset) as usize * 10;
            bytes.copy_within(src..src + n, dst);
        }
        o.maxoff = maxoff - 1;
        write_opaque_to(bytes, &o);
        set_data_page_data_size(bytes, o.maxoff as usize * 10);
        set_lsn(pbuffer, lsn);
        bufmgr_seams::mark_buffer_dirty::call(pbuffer)?;
    }

    if lbuffer != types_core::InvalidBuffer {
        unlock_release(lbuffer)?;
    }
    if pbuffer != types_core::InvalidBuffer {
        unlock_release(pbuffer)?;
    }
    if dbuffer != types_core::InvalidBuffer {
        unlock_release(dbuffer)?;
    }
    Ok(())
}

pub fn gin_redo(record: &mut XLogReaderState) -> PgResult<()> {
    let info = record.record.as_ref().expect("gin_redo with no decoded record").xl_info
        & !XLR_INFO_MASK;
    match info {
        XLOG_GIN_CREATE_PTREE => redo_create_ptree(record),
        XLOG_GIN_INSERT => redo_insert(record),
        XLOG_GIN_SPLIT => redo_split(record),
        XLOG_GIN_VACUUM_PAGE => redo_vacuum_page(record),
        XLOG_GIN_VACUUM_DATA_LEAF_PAGE => redo_vacuum_data_leaf_page(record),
        XLOG_GIN_DELETE_PAGE => redo_delete_page(record),
        XLOG_GIN_UPDATE_META_PAGE => redo_update_metapage(record),
        XLOG_GIN_INSERT_LISTPAGE => redo_insert_listpage(record),
        XLOG_GIN_DELETE_LISTPAGE => redo_delete_listpages(record),
        other => Err(Box::new(PgError::new(
            types_error::PANIC,
            format!("gin_redo: unknown op code {other}"),
        ))),
    }
}

pub fn gin_mask(pagedata: &mut [u8], _blkno: BlockNumber) -> PgResult<()> {
    bufmask::mask_page_lsn_and_checksum(pagedata);
    let opaque = opaque_of(pagedata);
    bufmask::mask_page_hint_bits(pagedata);
    if opaque.flags & GIN_DELETED != 0 {
        bufmask::mask_page_content(pagedata);
    } else if u16::from_ne_bytes([pagedata[12], pagedata[13]]) as usize > SIZE_OF_PAGE_HEADER {
        bufmask::mask_unused_space(pagedata)?;
    }
    Ok(())
}

pub fn init_seams() {}

#[cfg(test)]
mod tests {
    #[test]
    fn additems_decoder_refuses_truncated_varbyte_item() {
        // 6-byte first item, nbytes = 1, payload = one byte with bit 7 set:
        // the delta claims a continuation byte the segment does not hold.
        let mut seg = vec![0u8; 9];
        seg[6..8].copy_from_slice(&1u16.to_ne_bytes());
        seg[8] = 0x80;
        let err = super::recompress_additems(&seg, &[]).unwrap_err();
        assert_eq!(err.sqlstate(), ::types_error::ERRCODE_DATA_CORRUPTED);
    }

    use super::*;

    // A short / truncated / size-inflated GIN record must surface as a
    // catchable ERRCODE_DATA_CORRUPTED error, never a slice-bounds panic in the
    // recovery thread. These exercise the shared validation helpers that guard
    // every redo arm's fixed-offset reads and the recompress stream walk.

    #[test]
    fn require_len_short_record_is_corruption_not_panic() {
        // Empty main-data (0 bytes is legal and passes xlogreader validation)
        // against an arm that needs its fixed struct.
        let err = require_len(&[], 84, "update-metapage").err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);

        // One byte short of the struct.
        let buf = vec![0u8; 83];
        let err = require_len(&buf, 84, "update-metapage").err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);

        // Exactly enough is accepted.
        let buf = vec![0u8; 84];
        assert!(require_len(&buf, 84, "update-metapage").is_ok());
    }

    #[test]
    fn checked_itup_size_rejects_truncated_zero_and_inflated() {
        // Truncated tuple header (< 8 bytes).
        let err = checked_itup_size(&[0u8; 4], "tuple").err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);

        // Size word of 0 would also spin the ntuples loop forever.
        let mut stream = vec![0u8; 8];
        stream[6..8].copy_from_slice(&0u16.to_ne_bytes());
        let err = checked_itup_size(&stream, "tuple").err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);

        // Size word declaring more than the payload provides.
        let mut stream = vec![0u8; 8];
        stream[6..8].copy_from_slice(&(100u16).to_ne_bytes());
        let err = checked_itup_size(&stream, "tuple").err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);

        // Well-formed: declared size within the payload.
        let mut stream = vec![0u8; 16];
        stream[6..8].copy_from_slice(&(16u16).to_ne_bytes());
        assert_eq!(checked_itup_size(&stream, "tuple").unwrap(), 16);
    }

    #[test]
    fn checked_posting_item_offset_rejects_zero_over_maxoff_and_overflow() {
        // offset 0 (InvalidOffsetNumber) is not a valid slot for the redo
        // insert-data internal write (C's first raw read would underflow).
        let err = checked_posting_item_offset(0, 5).err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);

        // The out-of-bounds-write finding: offset 65535 into an 8 KB page.
        let err = checked_posting_item_offset(65535, 5).err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);

        // One slot past the append position (maxoff+1) is rejected.
        let err = checked_posting_item_offset(7, 5).err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);

        // A hostile full-page image can plant a maxoff whose posting items
        // would overrun the page; reject it even at a small offset.
        let big = (GinDataPageMaxDataSize / 10) as OffsetNumber;
        let err = checked_posting_item_offset(1, big).err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);

        // Valid: any existing slot and the append position (maxoff+1).
        assert!(checked_posting_item_offset(1, 5).is_ok());
        assert!(checked_posting_item_offset(5, 5).is_ok());
        assert!(checked_posting_item_offset(6, 5).is_ok());
    }

    // --- audit-18.6 remediation b084 witnesses (redo arms over a fake buffer) ---

    /// Fake buffer table for the redo arms: buffer id -> leaked BLCKSZ image.
    /// Process-wide (the seam is a global), keyed so concurrent tests never
    /// share a buffer id.
    static REDO_PAGES: std::sync::Mutex<std::collections::BTreeMap<Buffer, usize>> =
        std::sync::Mutex::new(std::collections::BTreeMap::new());

    #[repr(C, align(8))]
    struct FakePage([u8; BLCKSZ]);

    fn install_fake_page(buffer: Buffer, flags: u16) -> &'static mut [u8] {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            bufmgr_seams::buffer_get_page::set(|buf| {
                let addr = *REDO_PAGES
                    .lock()
                    .unwrap()
                    .get(&buf)
                    .unwrap_or_else(|| panic!("no fake page for buffer {buf}"));
                core::ptr::NonNull::new(addr as *mut u8).unwrap()
            });
        });
        let page: &'static mut FakePage = Box::leak(Box::new(FakePage([0u8; BLCKSZ])));
        gin_init_page_bytes(&mut page.0, flags);
        REDO_PAGES.lock().unwrap().insert(buffer, page.0.as_mut_ptr() as usize);
        &mut page.0
    }

    // ginxlog.c:96-104: a PageAddItem failure names the relation file
    // (BufferGetTag) — "failed to add item to index page in %u/%u/%u".
    #[test]
    fn insert_entry_add_item_failure_names_relation_file() {
        let bytes = install_fake_page(9101, GIN_LEAF);
        // A full page: pd_upper == pd_lower leaves no room for any tuple.
        let lower = u16::from_ne_bytes([bytes[12], bytes[13]]);
        bytes[14..16].copy_from_slice(&lower.to_ne_bytes());

        // ginxlogInsertEntry: offset 1, no delete, one 16-byte index tuple.
        let mut rdata = vec![0u8; 4 + 16];
        rdata[0..2].copy_from_slice(&1u16.to_ne_bytes());
        rdata[4 + 6..4 + 8].copy_from_slice(&16u16.to_ne_bytes());

        let err = redo_insert_entry(
            9101,
            RelFileLocator::new(1663, 5, 16391),
            InvalidBlockNumber,
            &rdata,
        )
        .err()
        .expect("PageAddItem on a full page fails");
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
        assert_eq!(err.message(), "failed to add item to index page in 1663/5/16391");
    }

    // ginxlog.c:298: an unknown action code is elog(ERROR) XX000
    // "unexpected GIN leaf action: %u" (not a data-corruption XX001).
    #[test]
    fn recompress_unknown_action_is_internal_error_with_c_message() {
        let bytes = install_fake_page(9102, GIN_DATA | GIN_LEAF | GIN_COMPRESSED);
        // One well-formed segment on the page so the action addresses a
        // current segment (past-the-end is a different, corruption arm).
        let nbytes = 4usize;
        let total = size_of_gin_posting_list(nbytes);
        bytes[GinDataPageDataOffset + 6..GinDataPageDataOffset + 8]
            .copy_from_slice(&(nbytes as u16).to_ne_bytes());
        set_data_page_data_size(bytes, total);

        // nactions = 1; action stream: segno 0, action 5 (unknown).
        let rdata = [1u8, 0, 0, 5];
        let err = redo_recompress(9102, &rdata).err().expect("unknown action is an error");
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
        assert_eq!(err.message(), "unexpected GIN leaf action: 5");
    }

    // ginxlog.c:132-164: a pre-9.4 (uncompressed) leaf is converted to the
    // compressed format in place before the actions replay: the raw TID
    // array becomes one posting-list segment, GIN_COMPRESSED is set and
    // maxoff cleared. An empty pre-9.4 leaf (leftmost/rightmost pages are
    // never deleted, ginxlog.c:139-143) converts to an empty posting list.
    #[test]
    fn recompress_converts_uncompressed_leaf_like_c() {
        let bytes = install_fake_page(9103, GIN_DATA | GIN_LEAF);
        // Raw TIDs (1,1) (2,1) (3,1) at GinDataPageGetData, count in maxoff.
        for (i, blk) in [1u64, 2, 3].iter().enumerate() {
            write_item(bytes, GinDataPageDataOffset + i * 6, (blk << 11) | 1);
        }
        let mut o = opaque_of(bytes);
        o.maxoff = 3;
        write_opaque_to(bytes, &o);
        // pd_lower is meaningless on a pre-9.4 page: leave it at the data
        // offset (the shape a pg_upgrade'd page carries).

        let rdata = [0u8, 0];
        redo_recompress(9103, &rdata).unwrap();

        let o = opaque_of(bytes);
        assert_ne!(o.flags & GIN_COMPRESSED, 0, "flags {:#x}", o.flags);
        assert_eq!(o.maxoff, types_tuple::itemptr::InvalidOffsetNumber);
        // ginCompressPostingList shape: first item verbatim, nbytes, then the
        // varbyte deltas of (blk << 11 | posid): 2048 = 0x80 0x10 twice.
        let mut expect = vec![0u8; 8];
        write_item(&mut expect, 0, (1u64 << 11) | 1);
        expect[6..8].copy_from_slice(&4u16.to_ne_bytes());
        expect.extend_from_slice(&[0x80, 0x10, 0x80, 0x10]);
        let pd_lower = u16::from_ne_bytes([bytes[12], bytes[13]]) as usize;
        assert_eq!(pd_lower, GinDataPageDataOffset + expect.len());
        assert_eq!(&bytes[GinDataPageDataOffset..pd_lower], &expect[..]);

        // Empty pre-9.4 leaf.
        let bytes = install_fake_page(9104, GIN_DATA | GIN_LEAF);
        let mut o = opaque_of(bytes);
        o.maxoff = 0;
        write_opaque_to(bytes, &o);
        redo_recompress(9104, &rdata).unwrap();
        let o = opaque_of(bytes);
        assert_ne!(o.flags & GIN_COMPRESSED, 0);
        assert_eq!(o.maxoff, types_tuple::itemptr::InvalidOffsetNumber);
        let pd_lower = u16::from_ne_bytes([bytes[12], bytes[13]]) as usize;
        assert_eq!(pd_lower, GinDataPageDataOffset);
    }

    #[test]
    fn seg_size_checked_rejects_truncated_and_inflated() {
        // Truncated posting-list header.
        let err = seg_size_checked(&[0u8; 4], "seg").err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);

        // Header declares nbytes that overrun the buffer.
        let mut b = vec![0u8; 8];
        b[6..8].copy_from_slice(&(4096u16).to_ne_bytes());
        let err = seg_size_checked(&b, "seg").err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);

        // Well-formed single-segment buffer: header + short-aligned payload.
        let nbytes = 4usize;
        let total = size_of_gin_posting_list(nbytes);
        let mut b = vec![0u8; total];
        b[6..8].copy_from_slice(&(nbytes as u16).to_ne_bytes());
        assert_eq!(seg_size_checked(&b, "seg").unwrap(), total);
    }
}
