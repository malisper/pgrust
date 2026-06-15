//! Seam-free unit tests for the pure MultiXact page/offset math, wraparound
//! comparators, and message formatting (no SLRU / shmem / seam calls).

use super::*;

#[test]
fn offset_page_entry_segment_math() {
    // The first multixact of page 1 has entry 0.
    let m = MULTIXACT_OFFSETS_PER_PAGE;
    assert_eq!(MultiXactIdToOffsetPage(m), 1);
    assert_eq!(MultiXactIdToOffsetEntry(m), 0);
    assert_eq!(MultiXactIdToOffsetPage(m + 3), 1);
    assert_eq!(MultiXactIdToOffsetEntry(m + 3), 3);
    assert_eq!(
        MultiXactIdToOffsetSegment(m),
        1 / SLRU_PAGES_PER_SEGMENT
    );
}

#[test]
fn member_layout_constants() {
    // 8 bits per xact, 4 members per group; group = 4 flag bytes + 4*4 xid.
    assert_eq!(MXACT_MEMBER_BITS_PER_XACT, 8);
    assert_eq!(MULTIXACT_MEMBERS_PER_MEMBERGROUP, 4);
    assert_eq!(MULTIXACT_MEMBERGROUP_SIZE, 4 + 4 * 4);
    // members-per-page derived from group size.
    assert_eq!(
        MULTIXACT_MEMBERS_PER_PAGE,
        MULTIXACT_MEMBERGROUPS_PER_PAGE * 4
    );
}

#[test]
fn member_offset_within_group() {
    // First member of offset 0: flags at 0, member xid right after flag bytes.
    assert_eq!(MXOffsetToFlagsOffset(0), 0);
    assert_eq!(MXOffsetToFlagsBitShift(0), 0);
    assert_eq!(MXOffsetToMemberOffset(0), MULTIXACT_FLAGBYTES_PER_GROUP);
    // Second member of the group: shift advances by 8 bits, xid by 4.
    assert_eq!(MXOffsetToFlagsBitShift(1), MXACT_MEMBER_BITS_PER_XACT);
    assert_eq!(
        MXOffsetToMemberOffset(1),
        MULTIXACT_FLAGBYTES_PER_GROUP + SIZEOF_TRANSACTION_ID
    );
}

#[test]
fn wraparound_comparators() {
    assert!(MultiXactIdPrecedes(1, 2));
    assert!(!MultiXactIdPrecedes(2, 1));
    assert!(MultiXactIdPrecedesOrEquals(2, 2));
    assert!(MultiXactIdPrecedesOrEquals(1, 2));
    assert!(!MultiXactIdPrecedesOrEquals(3, 2));
    // wraparound: a large id "precedes" a small one across the boundary.
    assert!(MultiXactIdPrecedes(0xFFFF_FFFF, 1));
    assert!(MultiXactOffsetPrecedes(0xFFFF_FFFF, 1));
}

#[test]
fn previous_multixact_id_wraps() {
    assert_eq!(PreviousMultiXactId(FirstMultiXactId), MaxMultiXactId);
    assert_eq!(PreviousMultiXactId(5), 4);
}

#[test]
fn would_wrap_basic() {
    // start below boundary, distance crossing it -> wraps.
    assert!(MultiXactOffsetWouldWrap(100, 90, 20));
    // start below boundary, distance staying below -> no wrap.
    assert!(!MultiXactOffsetWouldWrap(100, 10, 20));
}

#[test]
fn offset_page_precedes_truncation() {
    // page 0 precedes page 1 (older).
    assert!(MultiXactOffsetPagePrecedes(0, 1));
    assert!(!MultiXactOffsetPagePrecedes(1, 0));
}

#[test]
fn status_word_and_isupdate() {
    assert_eq!(status_word(MultiXactStatus::ForKeyShare), 0x00);
    assert_eq!(status_word(MultiXactStatus::Update), 0x05);
    assert!(!ISUPDATE_from_mxstatus(MultiXactStatus::ForUpdate));
    assert!(ISUPDATE_from_mxstatus(MultiXactStatus::NoKeyUpdate));
    assert!(ISUPDATE_from_mxstatus(MultiXactStatus::Update));
}

#[test]
fn mxstatus_names() {
    assert_eq!(mxstatus_to_string(MultiXactStatus::ForKeyShare), "keysh");
    assert_eq!(mxstatus_to_string(MultiXactStatus::ForShare), "sh");
    assert_eq!(mxstatus_to_string(MultiXactStatus::Update), "upd");
}

#[test]
fn mxid_to_string_formats() {
    let members = [
        MultiXactMember {
            xid: 100,
            status: Some(MultiXactStatus::ForShare),
        },
        MultiXactMember {
            xid: 200,
            status: Some(MultiXactStatus::Update),
        },
    ];
    assert_eq!(
        mxid_to_string(42, &members),
        "42 2[100 (sh), 200 (upd)]"
    );
    assert_eq!(mxid_to_string(7, &[]), "7 0[]");
}

#[test]
fn member_cmp_orders_by_xid_then_status() {
    let mut v = vec![
        MultiXactMember {
            xid: 200,
            status: Some(MultiXactStatus::ForShare),
        },
        MultiXactMember {
            xid: 100,
            status: Some(MultiXactStatus::Update),
        },
        MultiXactMember {
            xid: 100,
            status: Some(MultiXactStatus::ForKeyShare),
        },
    ];
    v.sort_by(mxact_member_cmp);
    assert_eq!(v[0].xid, 100);
    assert_eq!(v[0].status, Some(MultiXactStatus::ForKeyShare));
    assert_eq!(v[1].xid, 100);
    assert_eq!(v[1].status, Some(MultiXactStatus::Update));
    assert_eq!(v[2].xid, 200);
}

#[test]
fn warning_message_plurals() {
    assert!(multixactid_warning_msg_oid(5, 1).contains("1 more MultiXactId is used"));
    assert!(multixactid_warning_msg_oid(5, 2).contains("2 more MultiXactIds are used"));
    assert!(members_limit_detail(1, 3).contains("only enough for 1 member."));
    assert!(members_limit_detail(2, 3).contains("only enough for 2 members."));
}
