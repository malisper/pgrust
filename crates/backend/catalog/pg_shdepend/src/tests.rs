use super::*;

#[test]
fn deptype_chars_match_dependency_h() {
    assert_eq!(SharedDependencyType::Owner.as_char(), b'o' as i8);
    assert_eq!(SharedDependencyType::Acl.as_char(), b'a' as i8);
    assert_eq!(SharedDependencyType::InitAcl.as_char(), b'i' as i8);
    assert_eq!(SharedDependencyType::Policy.as_char(), b'r' as i8);
    assert_eq!(SharedDependencyType::Tablespace.as_char(), b't' as i8);
}

#[test]
fn attnums_match_pg_shdepend_h() {
    assert_eq!(Natts_pg_shdepend, 7);
    assert_eq!(Anum_pg_shdepend_dbid, 1);
    assert_eq!(Anum_pg_shdepend_classid, 2);
    assert_eq!(Anum_pg_shdepend_objid, 3);
    assert_eq!(Anum_pg_shdepend_objsubid, 4);
    assert_eq!(Anum_pg_shdepend_refclassid, 5);
    assert_eq!(Anum_pg_shdepend_refobjid, 6);
    assert_eq!(Anum_pg_shdepend_deptype, 7);
    assert_eq!(catalog::SharedDependRelationId, 1214);
    assert_eq!(catalog::SharedDependDependerIndexId, 1232);
    assert_eq!(catalog::SharedDependReferenceIndexId, 1233);
}

fn diff(mut a: Vec<Oid>, mut b: Vec<Oid>) -> (Vec<Oid>, Vec<Oid>) {
    let mut na = a.len();
    let mut nb = b.len();
    getOidListDiff(&mut a, &mut na, &mut b, &mut nb);
    a.truncate(na);
    b.truncate(nb);
    (a, b)
}

#[test]
fn oid_list_diff_removes_common_entries() {
    assert_eq!(
        diff(vec![1, 3, 5, 7], vec![2, 3, 6, 7]),
        (vec![1, 5], vec![2, 6])
    );
    assert_eq!(diff(vec![], vec![4, 9]), (vec![], vec![4, 9]));
    assert_eq!(diff(vec![4, 9], vec![]), (vec![4, 9], vec![]));
    assert_eq!(diff(vec![2, 8], vec![2, 8]), (vec![], vec![]));
    assert_eq!(diff(vec![1, 2], vec![3, 4]), (vec![1, 2], vec![3, 4]));
}

fn info(classId: Oid, objectId: Oid, objectSubId: i32, deptype: u8) -> ShDependObjectInfo {
    ShDependObjectInfo { classId, objectId, objectSubId, deptype: deptype as i8 }
}

#[test]
fn comparator_orders_oid_class_subid_deptype() {
    use core::cmp::Ordering;
    assert_eq!(
        shared_dependency_comparator(&info(1259, 10, 0, b'o'), &info(1259, 20, 0, b'o')),
        Ordering::Less
    );
    assert_eq!(
        shared_dependency_comparator(&info(1259, 10, 0, b'o'), &info(2615, 10, 0, b'o')),
        Ordering::Less
    );
    // subId compares as unsigned: 0 (whole object) sorts before -1.
    assert_eq!(
        shared_dependency_comparator(&info(1259, 10, 0, b'o'), &info(1259, 10, -1, b'o')),
        Ordering::Less
    );
    assert_eq!(
        shared_dependency_comparator(&info(1259, 10, 2, b'a'), &info(1259, 10, 2, b'o')),
        Ordering::Less
    );
    assert_eq!(
        shared_dependency_comparator(&info(1259, 10, 2, b'a'), &info(1259, 10, 2, b'a')),
        Ordering::Equal
    );
}

