use mcx::MemoryContext;
use types_error::ERRCODE_INTERNAL_ERROR;

use super::*;

// amvalidate.c:53-54: an unordered pg_amop/pg_amproc list is a catchable
// elog(ERROR) "cannot validate operator family without ordered data"
// (XX000), never a backend panic (audit-18.6 b068).
#[test]
fn unordered_lists_raise_the_c_error() {
    let cx = MemoryContext::new("amvalidate-test");
    for (opr_ordered, proc_ordered) in [(false, true), (true, false), (false, false)] {
        let e = identify_opfamily_groups(cx.mcx(), &[], opr_ordered, &[], proc_ordered)
            .expect_err("unordered lists must be an error");
        assert_eq!(e.message, "cannot validate operator family without ordered data");
        assert_eq!(e.sqlstate(), ERRCODE_INTERNAL_ERROR);
    }
    let groups = identify_opfamily_groups(cx.mcx(), &[], true, &[], true).unwrap();
    assert!(groups.is_empty());
}
