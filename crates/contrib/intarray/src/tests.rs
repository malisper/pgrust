use crate::boolquery::{self, parse_items, parse_query};
use crate::tool::{self, IntArray};
use types_tuple::varatt;

fn q(s: &str) -> Result<String, String> {
    match parse_query(s.as_bytes()) {
        Ok(img) => {
            let items = parse_items(&img);
            Ok(String::from_utf8(boolquery::deparse(&items).unwrap()).unwrap())
        }
        Err(e) => Err(e.err.message().to_string()),
    }
}

#[test]
fn parse_deparse_roundtrips() {
    assert_eq!(q("1").unwrap(), "1");
    assert_eq!(q(" 1 ").unwrap(), "1");
    assert_eq!(q("!1").unwrap(), "!1");
    assert_eq!(q(" ! 1 ").unwrap(), "!1");
    assert_eq!(q("1|2").unwrap(), "1 | 2");
    assert_eq!(q("1&2").unwrap(), "1 & 2");
    assert_eq!(q("1|2&3").unwrap(), "1 | 2 & 3");
    assert_eq!(q("(1|2)&3").unwrap(), "( 1 | 2 ) & 3");
    assert_eq!(q("!(1&2)").unwrap(), "!( 1 & 2 )");
    assert_eq!(q("!(!1|!2)").unwrap(), "!( !1 | !2 )");
    assert_eq!(q("1&(2|3)").unwrap(), "1 & ( 2 | 3 )");
    assert_eq!(q("-1").unwrap(), "-1");
    assert_eq!(q("((((((1))))))").unwrap(), "1");
}

#[test]
fn parse_number_strtol_base0() {
    // strtol(nnn, NULL, 0): leading zero selects octal; the parse stops at
    // the first invalid digit and endptr is ignored.
    assert_eq!(q("012").unwrap(), "10");
    assert_eq!(q("08").unwrap(), "0");
    assert_eq!(q("-012").unwrap(), "-10");
    assert_eq!(q("2147483647").unwrap(), "2147483647");
    assert_eq!(q("-2147483648").unwrap(), "-2147483648");
}

#[test]
fn parse_errors() {
    // Ends while an operand is expected.
    assert_eq!(q("").unwrap_err(), "syntax error");
    assert_eq!(q("  ").unwrap_err(), "syntax error");
    assert_eq!(q("1&").unwrap_err(), "syntax error");
    assert_eq!(q("()").unwrap_err(), "syntax error");
    assert_eq!(q("(1").unwrap_err(), "syntax error");
    assert_eq!(q("1)").unwrap_err(), "syntax error");
    assert_eq!(q("a").unwrap_err(), "syntax error");
    // Out-of-int32 value.
    assert_eq!(q("3000000000").unwrap_err(), "syntax error");
    // 16-digit token overruns C's nnn[16] accumulator.
    assert_eq!(q("1111111111111111").unwrap_err(), "syntax error");
}

#[test]
fn parser_operator_stack_depth() {
    // STACKDEPTH is 16 pending prefix operators. A NOT over an operator
    // operand deparses parenthesized: 16 bangs print as !( !( ... !1 ) ).
    let ok = format!("{}1", "!".repeat(16));
    assert_eq!(
        q(&ok).unwrap(),
        format!("{}!1{}", "!( ".repeat(15), " )".repeat(15))
    );
    let too_deep = format!("{}1", "!".repeat(17));
    assert_eq!(q(&too_deep).unwrap_err(), "statement too complex");
}

#[test]
fn deep_paren_nesting_is_not_statement_too_complex() {
    // '(' recurses makepol (fresh 16-slot stack per level); moderate depth
    // parses fine.
    let s = format!("{}1{}", "(".repeat(200), ")".repeat(200));
    assert_eq!(q(&s).unwrap(), "1");
}

fn boolop(arr: &[i32], query: &str) -> bool {
    let img = parse_query(query.as_bytes()).unwrap();
    let items = parse_items(&img);
    let mut sorted = arr.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    boolquery::execute(&items, items.len() as i32 - 1, true, &mut |_, it| {
        boolquery::checkcondition_arr(&sorted, it.val)
    })
    .unwrap()
}

#[test]
fn execute_semantics() {
    assert!(boolop(&[1, 2], "1&2"));
    assert!(!boolop(&[1], "1&2"));
    assert!(boolop(&[1], "1|2"));
    assert!(boolop(&[1], "!2"));
    assert!(!boolop(&[1, 2], "!2"));
    assert!(boolop(&[1, 2], "1&!3"));
    assert!(boolop(&[20, 23, 50], "(20&23)|! 50"));
    assert!(!boolop(&[20, 23], "(20&23)&!20"));
}

#[test]
fn gin_bool_consistent_maps_positionally() {
    let img = parse_query(b"1&(2|3)").unwrap();
    let items = parse_items(&img);
    // VAL items in item order: 1, 2, 3.
    assert!(boolquery::gin_bool_consistent(&items, &[true, true, false]).unwrap());
    assert!(boolquery::gin_bool_consistent(&items, &[true, false, true]).unwrap());
    assert!(!boolquery::gin_bool_consistent(&items, &[true, false, false]).unwrap());
    assert!(!boolquery::gin_bool_consistent(&items, &[false, true, true]).unwrap());
}

#[test]
fn query_has_required_values() {
    let has = |s: &str| {
        let img = parse_query(s.as_bytes()).unwrap();
        boolquery::query_has_required_values(&parse_items(&img)).unwrap()
    };
    assert!(has("1"));
    assert!(has("1&!2"));
    assert!(has("1|2"));
    assert!(!has("!1"));
    assert!(!has("!1|!2"));
    // OR requires both sides; the left has no required value.
    assert!(!has("!1|2"));
}

#[test]
fn signconsistent_hashes() {
    let img = parse_query(b"7").unwrap();
    let items = parse_items(&img);
    let siglen = 4usize;
    let mut sign = vec![0u8; siglen];
    tool::gensign(&mut sign, &[7], siglen);
    assert!(boolquery::signconsistent(&items, &sign, siglen, false).unwrap());
    let empty = vec![0u8; siglen];
    assert!(!boolquery::signconsistent(&items, &empty, siglen, false).unwrap());
}

// ---------------------------------------------------------------------------
// tool / array image
// ---------------------------------------------------------------------------

#[test]
fn int_array_layout_matches_c() {
    let mut a = IntArray::new(2);
    a.elems_mut().copy_from_slice(&[7, 9]);
    let b = a.as_bytes();
    assert_eq!(b.len(), 32); // 24-byte 1-D header + 2 * 4
    assert_eq!(&b[24..28], &7i32.to_ne_bytes());
    let e = IntArray::empty();
    assert_eq!(e.as_bytes().len(), 16);
    assert_eq!(e.ndim(), 0);
    assert_eq!(e.nelems(), 0);
}

#[test]
fn resize_multidim_dims_rewrite() {
    // C's resize loop sets dims[0]=num and every later dim to 1.
    let mut w: Vec<u8> = Vec::new();
    let nbytes = 32 + 4 * 4; // 2-D header (MAXALIGN(16+16)=32) + 4 elems
    w.extend_from_slice(&varatt::set_varsize_4b_word(nbytes as u32).to_ne_bytes());
    w.extend_from_slice(&2i32.to_ne_bytes()); // ndim
    w.extend_from_slice(&0i32.to_ne_bytes()); // dataoffset
    w.extend_from_slice(&23i32.to_ne_bytes()); // elemtype
    w.extend_from_slice(&2i32.to_ne_bytes()); // dims[0]
    w.extend_from_slice(&2i32.to_ne_bytes()); // dims[1]
    w.extend_from_slice(&1i32.to_ne_bytes()); // lb[0]
    w.extend_from_slice(&1i32.to_ne_bytes()); // lb[1]
    for v in [4, 3, 2, 1] {
        w.extend_from_slice(&(v as i32).to_ne_bytes());
    }
    let mut a = IntArray::from_image(&w);
    assert_eq!(a.nelems(), 4);
    assert_eq!(a.elems(), &[4, 3, 2, 1]);
    a.resize(3);
    assert_eq!(a.dims(), &[3, 1]);
    assert_eq!(a.elems(), &[4, 3, 2]);
}

fn null_bearing_array() -> IntArray {
    // 1-D, 2 items, second is NULL: dataoffset = MAXALIGN(24 + 1) = 32,
    // bitmap byte 0b01, one physical value.
    let mut w: Vec<u8> = Vec::new();
    let nbytes = 32 + 4;
    w.extend_from_slice(&varatt::set_varsize_4b_word(nbytes as u32).to_ne_bytes());
    w.extend_from_slice(&1i32.to_ne_bytes());
    w.extend_from_slice(&32i32.to_ne_bytes());
    w.extend_from_slice(&23i32.to_ne_bytes());
    w.extend_from_slice(&2i32.to_ne_bytes());
    w.extend_from_slice(&1i32.to_ne_bytes());
    w.push(0b01);
    w.extend_from_slice(&[0u8; 7]);
    w.extend_from_slice(&5i32.to_ne_bytes());
    IntArray::from_image(&w)
}

#[test]
fn checkarrvalid_rejects_nulls() {
    let a = null_bearing_array();
    let err = a.check_valid().unwrap_err();
    assert_eq!(err.message(), "array must not contain nulls");
    assert_eq!(err.sqlstate(), types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED);
}

#[test]
fn checkarrvalid_accepts_bitmap_without_nulls() {
    // Null bitmap present but all-ones: array_contains_nulls() is false.
    let mut w: Vec<u8> = Vec::new();
    let nbytes = 32 + 8;
    w.extend_from_slice(&varatt::set_varsize_4b_word(nbytes as u32).to_ne_bytes());
    w.extend_from_slice(&1i32.to_ne_bytes());
    w.extend_from_slice(&32i32.to_ne_bytes());
    w.extend_from_slice(&23i32.to_ne_bytes());
    w.extend_from_slice(&2i32.to_ne_bytes());
    w.extend_from_slice(&1i32.to_ne_bytes());
    w.push(0b11);
    w.extend_from_slice(&[0u8; 7]);
    w.extend_from_slice(&5i32.to_ne_bytes());
    w.extend_from_slice(&6i32.to_ne_bytes());
    let a = IntArray::from_image(&w);
    a.check_valid().unwrap();
    assert_eq!(a.elems(), &[5, 6]);
}

#[test]
fn unique_is_adjacent_dedup() {
    let mut a = IntArray::new(5);
    a.elems_mut().copy_from_slice(&[1234234, -30, -30, 234234, -30]);
    a.unique();
    assert_eq!(a.elems(), &[1234234, -30, 234234, -30]);
}

#[test]
fn inner_set_ops() {
    let mk = |v: &[i32]| {
        let mut a = IntArray::new(v.len());
        a.elems_mut().copy_from_slice(v);
        a
    };
    assert!(tool::inner_int_contains(&[1, 2, 3], &[2, 3]));
    assert!(!tool::inner_int_contains(&[1, 2, 3], &[2, 4]));
    assert!(tool::inner_int_overlap(&[1, 5], &[5, 9]));
    assert!(!tool::inner_int_overlap(&[1, 5], &[2, 9]));
    let u = tool::inner_int_union(&mk(&[1, 3]), &mk(&[2, 3])).unwrap();
    assert_eq!(u.elems(), &[1, 2, 3]);
    let i = tool::inner_int_inter(&mk(&[1, 3, 5]), &mk(&[3, 5, 7]));
    assert_eq!(i.elems(), &[3, 5]);
    let e = tool::inner_int_inter(&mk(&[1]), &mk(&[2]));
    assert_eq!(e.ndim(), 0);
}

#[test]
fn internal_size_overflow_is_minus_one() {
    assert_eq!(tool::internal_size(&[1, 10]), 10);
    assert_eq!(tool::internal_size(&[1, 10, 12, 12]), 11);
    assert_eq!(tool::internal_size(&[i32::MIN, i32::MAX]), -1);
}
