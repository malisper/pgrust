//! Layout pins + pure format laws (sizeof-priced gates need layout pins:
//! every constant that prices engagement is pinned HERE, so a change is a
//! visible diff, never a silent drift).

use crate::page::{
    init_row_page, init_var_page, validate_page, PageHdr, PageKind, RowLayout, VarRef, HDR_LEN,
    MAX_ROW_REFS, PAGE_SIZE,
};
use crate::set::{SpillExtent, SEG_BYTES};

#[test]
fn size_pins() {
    // The page header IS the on-disk format prefix.
    assert_eq!(core::mem::size_of::<PageHdr>(), 32);
    assert_eq!(HDR_LEN, 32);
    // Ref words are embedded in row images byte-for-byte.
    assert_eq!(core::mem::size_of::<VarRef>(), 8);
    // Extent directory entries ride every spilling Local.
    assert_eq!(core::mem::size_of::<SpillExtent>(), 16);
    // Page grain and segment law (the BufFile 1 GiB segment size).
    assert_eq!(PAGE_SIZE, 64 * 1024);
    assert_eq!(SEG_BYTES, 0x4000_0000);
    assert_eq!(SEG_BYTES % PAGE_SIZE as u64, 0);
    assert_eq!(MAX_ROW_REFS, 8);
    // Metrics are plain counters (riders on Locals).
    assert_eq!(core::mem::size_of::<crate::SpillMetrics>(), 8 * 8);
}

#[test]
fn row_layout_validation() {
    super::setup_process();
    // Good: 64-byte rows, two 8-aligned refs.
    let l = RowLayout::new(64, &[8, 24]).unwrap();
    assert_eq!(l.row_size(), 64);
    assert_eq!(l.refs(), &[8, 24]);
    assert_eq!(l.rows_per_page(), ((PAGE_SIZE - HDR_LEN) / 64) as u32);
    // Zero-ref layouts are legal (pure fixed-width rows).
    assert_eq!(RowLayout::new(16, &[]).unwrap().refs(), &[] as &[u16]);
    // Bad: width not a multiple of 8 / zero / page-oversized.
    assert!(RowLayout::new(12, &[]).is_err());
    assert!(RowLayout::new(0, &[]).is_err());
    assert!(RowLayout::new((PAGE_SIZE - HDR_LEN + 8) as u32, &[]).is_err());
    // Bad: unaligned ref, ref past row end, unsorted/duplicate refs.
    assert!(RowLayout::new(64, &[4]).is_err());
    assert!(RowLayout::new(64, &[64]).is_err());
    assert!(RowLayout::new(64, &[24, 8]).is_err());
    assert!(RowLayout::new(64, &[8, 8]).is_err());
    // Bad: more than MAX_ROW_REFS.
    let too_many: Vec<u16> = (0..9).map(|i| i * 8).collect();
    assert!(RowLayout::new(128, &too_many).is_err());
}

#[test]
fn varref_encoding_laws() {
    // NULL: unswizzled-tagged, unreachable (page 0, offset 0).
    assert!(VarRef::NULL.is_unswizzled());
    assert!(VarRef::NULL.is_null());
    // Encode/decode roundtrip across the field ranges.
    for page in [0u32, 1, 7, 255, 65535, u32::MAX] {
        for off in [HDR_LEN as u32, 40, 65528, (1 << 31) - 8] {
            let r = VarRef::encode(page, off);
            assert!(r.is_unswizzled());
            assert!(!r.is_null());
            assert_eq!(r.decode(), (page, off));
        }
    }
    // The tag law: swizzled words are 8-aligned addresses, bit 0 clear.
    let cell = [0u8; 16];
    let addr = (cell.as_ptr() as usize & !7) as *const u8; // 8-aligned
    let s = VarRef::swizzled(unsafe { addr.add(8) });
    assert!(!s.is_unswizzled());
}

#[test]
fn page_header_roundtrip() {
    let mut buf = vec![0u8; PAGE_SIZE];
    let layout = RowLayout::new(48, &[0, 16, 40]).unwrap();
    init_row_page(&mut buf, &layout);
    let h = PageHdr::read(&buf);
    assert_eq!(h.kind, PageKind::Row as u8);
    assert_eq!(h.row_size, 48);
    assert_eq!(h.nrefs, 3);
    assert_eq!(&h.ref_offs[..3], &[0, 16, 40]);
    assert_eq!(h.count, 0);
    assert_eq!(h.used, HDR_LEN as u32);
    // Write-back identity.
    let mut buf2 = vec![0u8; PAGE_SIZE];
    h.write(&mut buf2);
    assert_eq!(&buf[..HDR_LEN], &buf2[..HDR_LEN]);
}

#[test]
fn validate_page_fail_closed() {
    super::setup_process();
    let layout = RowLayout::new(64, &[8]).unwrap();
    let mut buf = vec![0u8; PAGE_SIZE];
    init_row_page(&mut buf, &layout);
    assert!(validate_page(&buf, PageKind::Row).is_ok());

    // Wrong kind for the slot's record.
    assert!(validate_page(&buf, PageKind::Var).is_err());

    // Torn magic.
    let mut bad = buf.clone();
    bad[0] ^= 0xFF;
    assert!(validate_page(&bad, PageKind::Row).is_err());

    // used/count inconsistency (row pages pin used == HDR + count*row).
    let mut bad = buf.clone();
    let mut h = PageHdr::read(&bad);
    h.count = 3;
    h.write(&mut bad);
    assert!(validate_page(&bad, PageKind::Row).is_err());

    // used beyond the buffer.
    let mut bad = buf.clone();
    let mut h = PageHdr::read(&bad);
    h.used = (PAGE_SIZE + 8) as u32;
    h.write(&mut bad);
    assert!(validate_page(&bad, PageKind::Row).is_err());

    // Unaligned ref offset smuggled into the header.
    let mut bad = buf.clone();
    let mut h = PageHdr::read(&bad);
    h.ref_offs[0] = 4;
    h.write(&mut bad);
    assert!(validate_page(&bad, PageKind::Row).is_err());

    // Var pages: row fields must be zero.
    let mut vbuf = vec![0u8; PAGE_SIZE];
    init_var_page(&mut vbuf);
    assert!(validate_page(&vbuf, PageKind::Var).is_ok());
    let mut bad = vbuf.clone();
    let mut h = PageHdr::read(&bad);
    h.row_size = 8;
    h.write(&mut bad);
    assert!(validate_page(&bad, PageKind::Var).is_err());

    // Short buffer.
    assert!(validate_page(&buf[..16], PageKind::Row).is_err());
}
