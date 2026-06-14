//! Branch-decision tests for the in-crate control flow of comment.c. The
//! genuine catalog/objectaddress/fmgr primitives are installed ONCE (the seam
//! slots are install-once `OnceLock`s) as mocks that route through shared
//! statics; each test sets the statics it needs. This asserts the empty-string
//! -> NULL reduction, the found-vs-not-found upsert decision, the
//! shared-vs-local dispatch, the OBJECT_COLUMN relkind whitelist, the
//! 3-vs-2 delete scan keys, and the GetComment `!isnull` branch.

use super::*;
use backend_commands_comment_seams::{DescriptionColumn, DescriptionTupleId, ResolvedObject};
use std::sync::{Mutex, MutexGuard, Once};
use types_catalog::catalog_dependency::ObjectAddress;
use types_nodes::parsenodes::{ObjectType, OBJECT_TABLE};
use types_storage::lock::LOCKMODE;
use types_tuple::heaptuple::ItemPointerData;

#[derive(Clone, Debug, PartialEq, Eq)]
enum CatalogAction {
    Insert { values: Vec<Datum>, nulls: Vec<bool> },
    Update {
        tuple: DescriptionTupleId,
        values: Vec<Datum>,
        nulls: Vec<bool>,
        replaces: Vec<bool>,
    },
    Delete { tuple: DescriptionTupleId },
    DeleteAll { objoid: Oid, classoid: Oid, objsubid: Option<i32> },
}

// Behaviour knobs the install-once mocks route through.
static ACTION: Mutex<Option<CatalogAction>> = Mutex::new(None);
static CLOSED_LOCK: Mutex<Option<LOCKMODE>> = Mutex::new(None);
static FOUND: Mutex<Option<DescriptionTupleId>> = Mutex::new(None);
static FOUND_COLUMN: Mutex<Option<DescriptionColumn>> = Mutex::new(None);
static RESOLVED: Mutex<Option<ResolvedObject>> = Mutex::new(None);
static RELKIND: Mutex<u8> = Mutex::new(b'r');
static DB_OID: Mutex<Oid> = Mutex::new(0);
static FIND_KEY: Mutex<Option<(Oid, Oid, i32)>> = Mutex::new(None);
static SHFIND_KEY: Mutex<Option<(Oid, Oid)>> = Mutex::new(None);
static CLOSED_REL: Mutex<Option<Oid>> = Mutex::new(None);

// The seam slots are process-global, so tests run serially.
static SEAM_LOCK: Mutex<()> = Mutex::new(());
fn lock() -> MutexGuard<'static, ()> {
    install_once();
    SEAM_LOCK.lock().unwrap_or_else(|p| p.into_inner())
}

fn reset() {
    *ACTION.lock().unwrap() = None;
    *CLOSED_LOCK.lock().unwrap() = None;
    *FOUND.lock().unwrap() = None;
    *FOUND_COLUMN.lock().unwrap() = None;
    *RESOLVED.lock().unwrap() = None;
    *RELKIND.lock().unwrap() = b'r';
    *DB_OID.lock().unwrap() = 0;
    *FIND_KEY.lock().unwrap() = None;
    *SHFIND_KEY.lock().unwrap() = None;
    *CLOSED_REL.lock().unwrap() = None;
}

fn install_once() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        backend_commands_dbcommands_seams::get_database_oid::set(|_n, _m| Ok(*DB_OID.lock().unwrap()));
        get_user_id::set(|| 10);
        seam::database_name::set(|_stmt| "db".to_string());
        seam::get_object_address::set(|_stmt, _lockmode| {
            Ok(RESOLVED.lock().unwrap().expect("RESOLVED not set"))
        });
        seam::check_object_ownership::set(|_r, _s, _a, _rel| Ok(()));
        seam::relation_get_relkind::set(|_rel| Ok(*RELKIND.lock().unwrap()));
        seam::relation_get_relation_name::set(|_rel| Ok("my_index".to_string()));
        seam::relation_close::set(|rel, _lockmode| {
            *CLOSED_REL.lock().unwrap() = Some(rel);
            Ok(())
        });
        // Sentinel text Datum: the comment's byte length.
        seam::cstring_get_text_datum::set(|comment| Ok(Datum::from_usize(comment.len())));
        seam::text_datum_get_cstring::set(|_value| Ok("the comment".to_string()));

        seam::description_open::set(|_lockmode| Ok(1));
        seam::description_close::set(|_rel, lockmode| {
            *CLOSED_LOCK.lock().unwrap() = Some(lockmode);
            Ok(())
        });
        seam::description_find_one::set(|_rel, o, c, s| {
            *FIND_KEY.lock().unwrap() = Some((o, c, s));
            Ok(*FOUND.lock().unwrap())
        });
        seam::description_delete::set(|_rel, tuple| {
            *ACTION.lock().unwrap() = Some(CatalogAction::Delete { tuple });
            Ok(())
        });
        seam::description_update::set(|_rel, tuple, values, nulls, replaces| {
            *ACTION.lock().unwrap() = Some(CatalogAction::Update {
                tuple,
                values: values.to_vec(),
                nulls: nulls.to_vec(),
                replaces: replaces.to_vec(),
            });
            Ok(())
        });
        seam::description_insert::set(|_rel, values, nulls| {
            *ACTION.lock().unwrap() = Some(CatalogAction::Insert {
                values: values.to_vec(),
                nulls: nulls.to_vec(),
            });
            Ok(())
        });
        seam::description_delete_all::set(|_rel, objoid, classoid, objsubid| {
            *ACTION.lock().unwrap() = Some(CatalogAction::DeleteAll { objoid, classoid, objsubid });
            Ok(())
        });
        seam::description_get_description::set(|_rel, _o, _c, _s| Ok(*FOUND_COLUMN.lock().unwrap()));

        seam::shdescription_open::set(|_lockmode| Ok(2));
        seam::shdescription_close::set(|_rel, lockmode| {
            *CLOSED_LOCK.lock().unwrap() = Some(lockmode);
            Ok(())
        });
        seam::shdescription_find_one::set(|_rel, o, c| {
            *SHFIND_KEY.lock().unwrap() = Some((o, c));
            Ok(*FOUND.lock().unwrap())
        });
        seam::shdescription_delete::set(|_rel, tuple| {
            *ACTION.lock().unwrap() = Some(CatalogAction::Delete { tuple });
            Ok(())
        });
        seam::shdescription_update::set(|_rel, tuple, values, nulls, replaces| {
            *ACTION.lock().unwrap() = Some(CatalogAction::Update {
                tuple,
                values: values.to_vec(),
                nulls: nulls.to_vec(),
                replaces: replaces.to_vec(),
            });
            Ok(())
        });
        seam::shdescription_insert::set(|_rel, values, nulls| {
            *ACTION.lock().unwrap() = Some(CatalogAction::Insert {
                values: values.to_vec(),
                nulls: nulls.to_vec(),
            });
            Ok(())
        });
        seam::shdescription_delete_all::set(|_rel, objoid, classoid| {
            *ACTION.lock().unwrap() = Some(CatalogAction::DeleteAll {
                objoid,
                classoid,
                objsubid: None,
            });
            Ok(())
        });
    });
}

fn tid(n: u16) -> DescriptionTupleId {
    DescriptionTupleId(ItemPointerData::new(0, n))
}

fn comment_stmt(objtype: ObjectType, comment: Option<&str>) -> CommentStmt {
    CommentStmt {
        objtype,
        object: None,
        comment: comment.map(|c| c.to_string()),
    }
}

fn addr(class_id: Oid, object_id: Oid, sub_id: i32) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: sub_id,
    }
}

// --- reduce_empty / comment_str pure helpers -----------------------------

#[test]
fn reduce_empty_folds_empty_string_to_none() {
    assert_eq!(reduce_empty(None), None);
    assert_eq!(reduce_empty(Some("")), None);
    assert_eq!(reduce_empty(Some("hi")), Some("hi"));
}

#[test]
fn comment_str_maps_absent_to_none() {
    assert_eq!(comment_str(&comment_stmt(OBJECT_TABLE, None)), None);
    assert_eq!(comment_str(&comment_stmt(OBJECT_TABLE, Some("note"))), Some("note"));
}

// --- CreateComments upsert decision --------------------------------------

#[test]
fn create_comments_not_found_inserts() {
    let _g = lock();
    reset();
    CreateComments(42, 1259, 0, Some("hello")).unwrap();
    match ACTION.lock().unwrap().clone() {
        Some(CatalogAction::Insert { values, nulls }) => {
            assert_eq!(values.len(), NATTS_PG_DESCRIPTION);
            assert_eq!(values[ANUM_PG_DESCRIPTION_OBJOID - 1], Datum::from_oid(42));
            assert_eq!(values[ANUM_PG_DESCRIPTION_CLASSOID - 1], Datum::from_oid(1259));
            assert_eq!(values[ANUM_PG_DESCRIPTION_OBJSUBID - 1], Datum::from_i32(0));
            assert_eq!(values[ANUM_PG_DESCRIPTION_DESCRIPTION - 1], Datum::from_usize(5));
            assert_eq!(nulls, vec![false; NATTS_PG_DESCRIPTION]);
        }
        other => panic!("expected Insert, got {other:?}"),
    }
    assert_eq!(*CLOSED_LOCK.lock().unwrap(), Some(NoLock));
}

#[test]
fn create_comments_found_updates() {
    let _g = lock();
    reset();
    *FOUND.lock().unwrap() = Some(tid(77));
    CreateComments(42, 1259, 0, Some("hi")).unwrap();
    match ACTION.lock().unwrap().clone() {
        Some(CatalogAction::Update { tuple, values, nulls, replaces }) => {
            assert_eq!(tuple, tid(77));
            assert_eq!(values[ANUM_PG_DESCRIPTION_OBJOID - 1], Datum::from_oid(42));
            assert_eq!(values[ANUM_PG_DESCRIPTION_DESCRIPTION - 1], Datum::from_usize(2));
            assert_eq!(nulls, vec![false; NATTS_PG_DESCRIPTION]);
            assert_eq!(replaces, vec![true; NATTS_PG_DESCRIPTION]);
        }
        other => panic!("expected Update, got {other:?}"),
    }
}

#[test]
fn create_comments_empty_comment_deletes_existing() {
    let _g = lock();
    reset();
    *FOUND.lock().unwrap() = Some(tid(88));
    CreateComments(42, 1259, 0, Some("")).unwrap();
    assert_eq!(*ACTION.lock().unwrap(), Some(CatalogAction::Delete { tuple: tid(88) }));
}

#[test]
fn create_comments_null_comment_not_found_is_noop() {
    let _g = lock();
    reset();
    CreateComments(42, 1259, 0, None).unwrap();
    assert_eq!(*ACTION.lock().unwrap(), None);
    assert_eq!(*CLOSED_LOCK.lock().unwrap(), Some(NoLock));
}

// --- CreateSharedComments ------------------------------------------------

#[test]
fn create_shared_comments_not_found_inserts() {
    let _g = lock();
    reset();
    CreateSharedComments(1234, 1262, Some("dbnote")).unwrap();
    match ACTION.lock().unwrap().clone() {
        Some(CatalogAction::Insert { values, nulls }) => {
            assert_eq!(values.len(), NATTS_PG_SHDESCRIPTION);
            assert_eq!(values[ANUM_PG_SHDESCRIPTION_OBJOID - 1], Datum::from_oid(1234));
            assert_eq!(values[ANUM_PG_SHDESCRIPTION_CLASSOID - 1], Datum::from_oid(1262));
            assert_eq!(values[ANUM_PG_SHDESCRIPTION_DESCRIPTION - 1], Datum::from_usize(6));
            assert_eq!(nulls, vec![false; NATTS_PG_SHDESCRIPTION]);
        }
        other => panic!("expected Insert, got {other:?}"),
    }
    assert_eq!(*CLOSED_LOCK.lock().unwrap(), Some(NoLock));
}

// --- CommentObject dispatch ----------------------------------------------

#[test]
fn comment_object_database_routes_to_shared() {
    let _g = lock();
    reset();
    *DB_OID.lock().unwrap() = 1234;
    *RESOLVED.lock().unwrap() = Some(ResolvedObject::new(addr(1262, 1234, 0), None));

    let stmt = comment_stmt(OBJECT_DATABASE, Some("hello"));
    let got = CommentObject(&stmt).unwrap();
    assert_eq!(got, addr(1262, 1234, 0));
    // CreateSharedComments(address.objectId, address.classId, comment).
    assert_eq!(SHFIND_KEY.lock().unwrap().clone(), Some((1234, 1262)));
    assert!(matches!(*ACTION.lock().unwrap(), Some(CatalogAction::Insert { .. })));
    // relation is None => relation_close not called.
    assert_eq!(*CLOSED_REL.lock().unwrap(), None);
}

#[test]
fn comment_object_table_routes_to_local_and_closes_relation() {
    let _g = lock();
    reset();
    *RESOLVED.lock().unwrap() = Some(ResolvedObject::new(addr(1259, 42, 0), Some(7)));

    let stmt = comment_stmt(OBJECT_TABLE, Some("tbl note"));
    let got = CommentObject(&stmt).unwrap();
    assert_eq!(got, addr(1259, 42, 0));
    // CreateComments(address.objectId, address.classId, address.objectSubId, comment).
    assert_eq!(FIND_KEY.lock().unwrap().clone(), Some((42, 1259, 0)));
    assert_eq!(*CLOSED_REL.lock().unwrap(), Some(7));
}

#[test]
fn comment_object_column_on_index_relkind_errors() {
    let _g = lock();
    reset();
    *RESOLVED.lock().unwrap() = Some(ResolvedObject::new(addr(1259, 99, 1), Some(3)));
    *RELKIND.lock().unwrap() = b'i'; // RELKIND_INDEX, not whitelisted

    let stmt = comment_stmt(OBJECT_COLUMN, Some("x"));
    let err = CommentObject(&stmt).expect_err("index column comment must error");
    assert_eq!(err.sqlstate(), ERRCODE_WRONG_OBJECT_TYPE);
    assert!(err.message().contains("cannot set comment on relation \"my_index\""));
}

#[test]
fn comment_object_column_on_table_relkind_passes() {
    let _g = lock();
    reset();
    *RESOLVED.lock().unwrap() = Some(ResolvedObject::new(addr(1259, 99, 2), Some(4)));
    *RELKIND.lock().unwrap() = RELKIND_RELATION;

    let stmt = comment_stmt(OBJECT_COLUMN, Some("col note"));
    CommentObject(&stmt).unwrap();
    // The objsubid (column number) must reach the local catalog key.
    assert_eq!(FIND_KEY.lock().unwrap().clone(), Some((99, 1259, 2)));
}

// --- DeleteComments: subid != 0 -> 3-vs-2 scan keys ----------------------

#[test]
fn delete_comments_with_subid_passes_objsubid_key() {
    let _g = lock();
    reset();
    DeleteComments(1, 2, 3).unwrap();
    assert_eq!(
        *ACTION.lock().unwrap(),
        Some(CatalogAction::DeleteAll { objoid: 1, classoid: 2, objsubid: Some(3) })
    );
    assert_eq!(*CLOSED_LOCK.lock().unwrap(), Some(RowExclusiveLock));
}

#[test]
fn delete_comments_subid_zero_omits_objsubid_key() {
    let _g = lock();
    reset();
    DeleteComments(1, 2, 0).unwrap();
    assert_eq!(
        *ACTION.lock().unwrap(),
        Some(CatalogAction::DeleteAll { objoid: 1, classoid: 2, objsubid: None })
    );
}

#[test]
fn delete_shared_comments_uses_two_keys() {
    let _g = lock();
    reset();
    DeleteSharedComments(4, 5).unwrap();
    assert_eq!(SHFIND_KEY.lock().unwrap().clone(), None); // delete_all path, not find_one
    assert_eq!(
        *ACTION.lock().unwrap(),
        Some(CatalogAction::DeleteAll { objoid: 4, classoid: 5, objsubid: None })
    );
    assert_eq!(*CLOSED_LOCK.lock().unwrap(), Some(RowExclusiveLock));
}

// --- GetComment: !isnull -> TextDatumGetCString --------------------------

#[test]
fn get_comment_returns_non_null_description() {
    let _g = lock();
    reset();
    *FOUND_COLUMN.lock().unwrap() = Some(DescriptionColumn { value: Datum::from_usize(7), isnull: false });
    assert_eq!(GetComment(1, 2, 0).unwrap(), Some("the comment".to_string()));
}

#[test]
fn get_comment_no_match_returns_none() {
    let _g = lock();
    reset();
    assert_eq!(GetComment(1, 2, 0).unwrap(), None);
}

#[test]
fn get_comment_null_description_returns_none() {
    let _g = lock();
    reset();
    *FOUND_COLUMN.lock().unwrap() = Some(DescriptionColumn { value: Datum::null(), isnull: true });
    assert_eq!(GetComment(1, 2, 0).unwrap(), None);
}
