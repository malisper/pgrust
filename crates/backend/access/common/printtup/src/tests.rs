//! Tests for printtup. The executor slot / lsyscache / fmgr externals are
//! reached through their owners' `-seams` crates (panic-until-installed); these
//! exercise the seam-free surface: receiver setup, the descriptor-identity
//! trigger, the format-code rejection (this crate's own logic, which fires
//! before any seam call), and the interactive `printatt` / `debugStartup`
//! formatting (which read only the descriptor).

use super::*;
use mcx::{MemoryContext, PgVec};
use ::types_tuple::heaptuple::{CompactAttribute, FormData_pg_attribute, NameData, TupleDescData};

fn name(s: &str) -> NameData {
    let mut n = NameData::default();
    n.data[..s.len()].copy_from_slice(s.as_bytes());
    n
}

fn attr(s: &str, atttypid: Oid, attlen: i16, atttypmod: i32, attbyval: bool) -> FormData_pg_attribute {
    FormData_pg_attribute {
        attname: name(s),
        atttypid,
        attlen,
        atttypmod,
        attbyval,
        ..FormData_pg_attribute::default()
    }
}

fn one_col_desc<'mcx>(mcx: Mcx<'mcx>, a: FormData_pg_attribute) -> TupleDescData<'mcx> {
    let mut attrs = PgVec::new_in(mcx);
    attrs.push(a);
    let mut compact = PgVec::new_in(mcx);
    compact.push(CompactAttribute { attlen: a.attlen, attbyval: a.attbyval, ..CompactAttribute::default() });
    TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: 0,
        constr: None,
        compact_attrs: compact,
        attrs,
    }
}

#[test]
fn create_dr_send_descrip() {
    let r = DR_printtup::printtup_create_DR(CommandDest::Remote);
    assert!(r.sendDescrip);
    assert_eq!(r.mydest, CommandDest::Remote);
    let r = DR_printtup::printtup_create_DR(CommandDest::RemoteExecute);
    assert!(!r.sendDescrip);
}

#[test]
fn attrinfo_identity_trigger() {
    let cx = MemoryContext::new("t");
    let d1 = one_col_desc(cx.mcx(), attr("a", 23, 4, -1, true));
    let mut r = DR_printtup::printtup_create_DR(CommandDest::Remote);
    // Not set up yet.
    assert!(!r.attrinfo_matches(&d1));
    // After recording d1's identity it matches d1 but not a different desc.
    r.attrinfo = Some(descriptor_identity(&d1));
    assert!(r.attrinfo_matches(&d1));
    let d2 = one_col_desc(cx.mcx(), attr("a", 23, 4, -1, true));
    assert!(!r.attrinfo_matches(&d2));
}

#[test]
fn printatt_formatting() {
    // No value (debugStartup line).
    let a = attr("id", 23, 4, -1, true);
    let line = printatt(1, &a, None);
    assert_eq!(line, "\t 1: id\t(typeid = 23, len = 4, typmod = -1, byval = t)\n");

    // With a value (debugtup line); byval = f for a varlena.
    let a = attr("name", 25, -1, -1, false);
    let line = printatt(2, &a, Some("hi"));
    assert_eq!(
        line,
        "\t 2: name = \"hi\"\t(typeid = 25, len = -1, typmod = -1, byval = f)\n"
    );
}

#[test]
fn debug_startup_renders_columns() {
    let cx = MemoryContext::new("t");
    let d = one_col_desc(cx.mcx(), attr("col", 23, 4, -1, true));
    let out = debugStartup(&d);
    assert_eq!(
        out,
        "\t 1: col\t(typeid = 23, len = 4, typmod = -1, byval = t)\n\t----\n"
    );
}

/// `printtup_prepare_info` rejects a format code other than 0/1 with the C
/// `ERRCODE_INVALID_PARAMETER_VALUE` "unsupported format code" error. This is
/// this crate's own logic, reached before any owner seam call.
#[test]
fn prepare_info_rejects_bad_format() {
    let cx = MemoryContext::new("t");
    let d = one_col_desc(cx.mcx(), attr("c", 23, 4, -1, true));
    let mut r = DR_printtup::printtup_create_DR(CommandDest::Remote);
    let formats = [2i16];
    let err = printtup_prepare_info(&mut r, cx.mcx(), &d, Some(&formats), 1).unwrap_err();
    assert!(format!("{err:?}").contains("unsupported format code"));

    // numAttrs <= 0 short-circuits before any per-column work (no seam call).
    let mut r = DR_printtup::printtup_create_DR(CommandDest::Remote);
    printtup_prepare_info(&mut r, cx.mcx(), &d, None, 0).unwrap();
    assert_eq!(r.nattrs, 0);
    assert!(r.attrinfo_matches(&d));
}
