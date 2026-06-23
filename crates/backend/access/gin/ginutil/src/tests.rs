//! Pure-logic unit tests for `ginutil.c` functions that do not touch the
//! seam-routed substrate.

use super::*;

#[test]
fn ginbuildphasename_known_and_unknown() {
    assert_eq!(ginbuildphasename(1), Some("initializing"));
    assert_eq!(ginbuildphasename(2), Some("scanning table"));
    assert_eq!(ginbuildphasename(3), Some("sorting tuples (workers)"));
    assert_eq!(ginbuildphasename(4), Some("merging tuples (workers)"));
    assert_eq!(ginbuildphasename(5), Some("sorting tuples"));
    assert_eq!(ginbuildphasename(6), Some("merging tuples"));
    assert_eq!(ginbuildphasename(0), None);
    assert_eq!(ginbuildphasename(7), None);
}

#[test]
fn ginhandler_flags_match_c() {
    let r = ginhandler();
    assert_eq!(r.type_, T_IndexAmRoutine);
    assert_eq!(r.amstrategies, 0);
    assert_eq!(r.amsupport, GINNProcs as u16);
    assert_eq!(r.amoptsprocnum, GIN_OPTIONS_PROC as u16);
    assert!(!r.amcanorder);
    assert!(r.amcanmulticol);
    assert!(r.amoptionalkey);
    assert!(!r.amsearchnulls);
    assert!(r.amstorage);
    assert!(r.ampredlocks);
    assert!(!r.amcanparallel);
    assert!(r.amcanbuildparallel);
    assert!(r.amusemaintenanceworkmem);
    assert!(!r.amsummarizing);
    assert_eq!(r.amkeytype, InvalidOid);
    // bitmap-only AM
    assert!(r.amgettuple.is_none());
    assert!(r.amgetbitmap.is_some());
    assert!(r.amcanreturn.is_none());
    assert!(r.aminsertcleanup.is_none());
    assert!(r.ammarkpos.is_none());
    assert!(r.amrestrpos.is_none());
    assert!(r.amvalidate.is_none());
    assert!(r.amtranslatestrategy.is_none());
    assert!(r.amtranslatecmptype.is_none());
    assert!(r.amestimateparallelscan.is_none());
}

#[test]
fn size_of_gin_meta_page_data_is_56() {
    assert_eq!(size_of_gin_meta_page_data(), 56);
    assert_eq!(meta_offset(), 24);
}

#[test]
fn gin_init_page_writes_opaque() {
    // A page large enough for header + special. Use BLCKSZ.
    let mut page = vec![0u8; BLCKSZ];
    GinInitPage(&mut page, GIN_META as u32, BLCKSZ).unwrap();

    let special = read_pd_special(&page);
    let rightlink = u32::from_ne_bytes([
        page[special],
        page[special + 1],
        page[special + 2],
        page[special + 3],
    ]);
    let maxoff = u16::from_ne_bytes([page[special + 4], page[special + 5]]);
    let flags = u16::from_ne_bytes([page[special + 6], page[special + 7]]);
    assert_eq!(rightlink, InvalidBlockNumber);
    assert_eq!(maxoff, 0);
    assert_eq!(flags, GIN_META);
}

#[test]
fn gin_init_metabuffer_sets_pd_lower_and_version() {
    let mut page = vec![0u8; BLCKSZ];
    GinInitMetabuffer(&mut page, BLCKSZ).unwrap();

    let pd_lower = u16::from_ne_bytes([page[OFF_PD_LOWER], page[OFF_PD_LOWER + 1]]);
    assert_eq!(pd_lower as usize, meta_offset() + size_of_gin_meta_page_data());

    let off = meta_offset();
    let ver = i32::from_ne_bytes([
        page[off + OFF_GIN_VERSION],
        page[off + OFF_GIN_VERSION + 1],
        page[off + OFF_GIN_VERSION + 2],
        page[off + OFF_GIN_VERSION + 3],
    ]);
    assert_eq!(ver, GIN_CURRENT_VERSION);

    // head/tail are InvalidBlockNumber.
    let head = u32::from_ne_bytes([
        page[off + OFF_GIN_HEAD],
        page[off + OFF_GIN_HEAD + 1],
        page[off + OFF_GIN_HEAD + 2],
        page[off + OFF_GIN_HEAD + 3],
    ]);
    assert_eq!(head, InvalidBlockNumber);
}
