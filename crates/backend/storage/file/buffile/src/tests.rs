//! Pure-logic tests for the `buffile.c` port — the parts that need no real
//! temp-file I/O (the fd.c VFD layer is not yet ported, so the end-to-end
//! round-trips that exercise it cannot run here). Covered: the
//! [`MAX_PHYSICAL_FILESIZE`] / [`BUFFILE_SEG_SIZE`] constants, the wait-event
//! selectors, the segment-name formatting, and [`BufFileTell`] arithmetic on a
//! hand-built [`BufFile`].

use super::*;

#[test]
fn seg_size_matches_c_formula() {
    // buffile.c:62-63.
    assert_eq!(MAX_PHYSICAL_FILESIZE, 0x4000_0000);
    assert_eq!(BUFFILE_SEG_SIZE, MAX_PHYSICAL_FILESIZE / 8192);
    assert_eq!(BUFFILE_SEG_SIZE, 131_072);
}

#[test]
fn segment_name_is_dot_decimal() {
    // buffile.c:221-225 -- "%s.%d".
    assert_eq!(FileSetSegmentName("logical-tape", 0), "logical-tape.0");
    assert_eq!(FileSetSegmentName("foo", 7), "foo.7");
    assert_eq!(FileSetSegmentName("a.b", 12), "a.b.12");
}

#[test]
fn wait_event_values_match_generated_enum() {
    // Generated WaitEventIO values: 167772166/7/8.
    assert_eq!(WAIT_EVENT_BUFFILE_READ, 167_772_166);
    assert_eq!(WAIT_EVENT_BUFFILE_TRUNCATE, 167_772_167);
    assert_eq!(WAIT_EVENT_BUFFILE_WRITE, 167_772_168);
}

#[test]
fn tell_reports_curfile_and_curoffset_plus_pos() {
    // buffile.c:832-837 -- *fileno = curFile; *offset = curOffset + pos.
    let mut file = makeBufFile(File(0));
    file.curFile = 2;
    file.curOffset = 5000;
    file.pos = 123;
    let (fileno, offset) = BufFileTell(&file);
    assert_eq!(fileno, 2);
    assert_eq!(offset, 5000 + 123);
}

#[test]
fn make_buf_file_initializes_one_segment() {
    // buffile.c:138-150 -- one segment, not read-only, no fileset/name.
    let file = makeBufFile(File(42));
    assert_eq!(file.numFiles, 1);
    assert_eq!(file.files, vec![File(42)]);
    assert!(!file.readOnly);
    assert!(file.fileset.is_none());
    assert!(file.name.is_none());
    assert_eq!(file.buffer.data.len(), BLCKSZ);
}
