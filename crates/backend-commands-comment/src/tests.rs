//! Branch-decision tests for the in-crate control flow of comment.c. The
//! genuine catalog/objectaddress/fmgr primitives are installed ONCE (the seam
//! slots are install-once `OnceLock`s) as mocks that route through shared
//! statics; each test sets the statics it needs. This asserts the empty-string
//! -> NULL reduction, the found-vs-not-found upsert decision, the
//! shared-vs-local dispatch, the OBJECT_COLUMN relkind whitelist, the
//! 3-vs-2 delete scan keys, and the GetComment `!isnull` branch.

use super::*;
use backend_catalog_objectaddress_seams::ResolvedObjectAddress;
use backend_commands_comment_seams::{DescriptionColumn, DescriptionTupleId};
use mcx::MemoryContext;
use std::sync::{Mutex, MutexGuard, Once};
use types_catalog::catalog_dependency::ObjectAddress;
use types_nodes::parsenodes::{ObjectType, OBJECT_TABLE};
use types_parsenodes::{Node, StringNode};
use types_rel::{FormData_pg_class, Relation, RelationData};
use types_storage::lock::LOCKMODE;
use types_storage::RelFileLocator;
use types_tuple::heaptuple::{ItemPointerData, TupleDescData};

#[derive(Clone, Debug, PartialEq, Eq)]
enum CatalogAction {
    Insert { values: Vec<usize>, nulls: Vec<bool> },
    Update {
        tuple: DescriptionTupleId,
        values: Vec<usize>,
        nulls: Vec<bool>,
        replaces: Vec<bool>,
    },
    Delete { tuple: DescriptionTupleId },
    DeleteAll { objoid: Oid, classoid: Oid, objsubid: Option<i32> },
}

/// The seam values arrays carry canonical `Datum<'mcx>`; the in-crate control
/// flow only ever stores by-value scalar words (every `values[i]` is a
/// `Datum::from_*` by-value codec), so the mocks snapshot the raw machine word
/// (`as_usize`) into the `'static` statics, sidestepping the borrowed-`'mcx`
/// lifetime. Assertions compare against `Datum::from_*(...).as_usize()`.
fn words(values: &[Datum]) -> Vec<usize> {
    values.iter().map(Datum::as_usize).collect()
}

// Behaviour knobs the install-once mocks route through.
static ACTION: Mutex<Option<CatalogAction>> = Mutex::new(None);
static CLOSED_LOCK: Mutex<Option<LOCKMODE>> = Mutex::new(None);
static FOUND: Mutex<Option<DescriptionTupleId>> = Mutex::new(None);
// The found description column carries a canonical `Datum<'mcx>`, which borrows
// its memory context and so is not `Sync`/`'static`-storable. The control flow
// only ever reads a by-value scalar word, so the static snapshots the raw word
// plus the isnull flag; the mock rebuilds the `Datum` in the seam's `'mcx`.
static FOUND_COLUMN: Mutex<Option<(usize, bool)>> = Mutex::new(None);
// The resolved ObjectAddress (always present once set) plus the Oid of the
// relation `get_object_address` "opened" (`None` for non-relation objects). The
// mock rebuilds a real `Relation<'mcx>` from the seam's `mcx` (it borrows the
// context, so it can't be stored `'static`).
static RESOLVED: Mutex<Option<(ObjectAddress, Option<Oid>)>> = Mutex::new(None);
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
        backend_catalog_objectaddress_seams::get_object_address::set(
            |mcx, _objtype, _object, _lockmode, _missing_ok| {
                let (address, rel_oid) = RESOLVED.lock().unwrap().expect("RESOLVED not set");
                let relation = rel_oid.map(|oid| make_rel(mcx, oid, *RELKIND.lock().unwrap()));
                Ok(ResolvedObjectAddress { address, relation })
            },
        );
        backend_catalog_objectaddress_seams::check_object_ownership::set(
            |_r, _objtype, _a, _object, _rel| Ok(()),
        );
        // Sentinel text Datum: the comment's byte length.
        seam::cstring_get_text_datum::set(|_mcx, comment| Ok(Datum::from_usize(comment.len())));
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
                values: words(values),
                nulls: nulls.to_vec(),
                replaces: replaces.to_vec(),
            });
            Ok(())
        });
        seam::description_insert::set(|_rel, values, nulls| {
            *ACTION.lock().unwrap() = Some(CatalogAction::Insert {
                values: words(values),
                nulls: nulls.to_vec(),
            });
            Ok(())
        });
        seam::description_delete_all::set(|_rel, objoid, classoid, objsubid| {
            *ACTION.lock().unwrap() = Some(CatalogAction::DeleteAll { objoid, classoid, objsubid });
            Ok(())
        });
        seam::description_get_description::set(|_mcx, _rel, _o, _c, _s| {
            Ok(FOUND_COLUMN
                .lock()
                .unwrap()
                .map(|(word, isnull)| DescriptionColumn { value: Datum::from_usize(word), isnull }))
        });

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
                values: words(values),
                nulls: nulls.to_vec(),
                replaces: replaces.to_vec(),
            });
            Ok(())
        });
        seam::shdescription_insert::set(|_rel, values, nulls| {
            *ACTION.lock().unwrap() = Some(CatalogAction::Insert {
                values: words(values),
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

/// `relation_close(rel, NoLock)` — the closer the test relation carries; records
/// which relation Oid was closed so the dispatch tests can assert it happened.
fn rel_closer(oid: Oid, _lockmode: LOCKMODE) -> PgResult<()> {
    *CLOSED_REL.lock().unwrap() = Some(oid);
    Ok(())
}

/// Build a cell-less test `Relation<'mcx>` (relname "my_index"), the relation
/// the canonical `get_object_address` mock "opens". Carries [`rel_closer`] so
/// `CommentObject`'s `relation_close` is observable.
fn make_rel(mcx: Mcx<'_>, oid: Oid, relkind: u8) -> Relation<'_> {
    let td = TupleDescData {
        natts: 0,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: 1,
        constr: None,
        compact_attrs: mcx::PgVec::new_in(mcx),
        attrs: mcx::PgVec::new_in(mcx),
    };
    let data = RelationData {
        rd_id: oid,
        rd_locator: RelFileLocator {
            spcOid: 0,
            dbOid: 0,
            relNumber: 0,
        },
        rd_backend: types_core::primitive::INVALID_PROC_NUMBER,
        rd_rel: FormData_pg_class {
            relname: mcx::PgString::from_str_in("my_index", mcx).unwrap(),
            relnamespace: 0,
            relowner: 0,
            relrowsecurity: false,
            relpages: 0,
            reltuples: 0.0,
            relallvisible: 0,
            reltoastrelid: 0,
            reltablespace: 0,
            relfilenode: 0,
            relisshared: false,
            relhasindex: false,
            relhassubclass: false,
            relpersistence: b'p',
            relkind,
            relam: 0,
            relispopulated: true,
            relreplident: b'd',
            relispartition: false,
            relfrozenxid: 0,
        },
        rd_att: mcx::alloc_in(mcx, td).unwrap(),
        rd_options: None,
        rd_index: None,
        rd_opcintype: mcx::PgVec::new_in(mcx),
        rd_opfamily: mcx::PgVec::new_in(mcx),
        rd_indoption: mcx::PgVec::new_in(mcx),
        rd_indcollation: mcx::PgVec::new_in(mcx),
        rd_trigdesc: None,
    };
    Relation::open(data, Some(rel_closer))
}

/// A `String` value node naming the object (`strVal(stmt->object)`).
fn string_node(name: &str) -> Box<Node> {
    Box::new(Node::String(StringNode {
        sval: Some(name.to_string()),
    }))
}

fn comment_stmt(objtype: ObjectType, comment: Option<&str>) -> CommentStmt {
    CommentStmt {
        objtype,
        // `strVal(stmt->object)` is read for OBJECT_DATABASE; a String value
        // node serves every objtype the dispatch tests exercise.
        object: Some(string_node("the_object")),
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
    let cx = MemoryContext::new("t");
    CreateComments(cx.mcx(), 42, 1259, 0, Some("hello")).unwrap();
    match ACTION.lock().unwrap().clone() {
        Some(CatalogAction::Insert { values, nulls }) => {
            assert_eq!(values.len(), NATTS_PG_DESCRIPTION);
            assert_eq!(values[ANUM_PG_DESCRIPTION_OBJOID - 1], Datum::from_oid(42).as_usize());
            assert_eq!(values[ANUM_PG_DESCRIPTION_CLASSOID - 1], Datum::from_oid(1259).as_usize());
            assert_eq!(values[ANUM_PG_DESCRIPTION_OBJSUBID - 1], Datum::from_i32(0).as_usize());
            assert_eq!(values[ANUM_PG_DESCRIPTION_DESCRIPTION - 1], Datum::from_usize(5).as_usize());
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
    let cx = MemoryContext::new("t");
    CreateComments(cx.mcx(), 42, 1259, 0, Some("hi")).unwrap();
    match ACTION.lock().unwrap().clone() {
        Some(CatalogAction::Update { tuple, values, nulls, replaces }) => {
            assert_eq!(tuple, tid(77));
            assert_eq!(values[ANUM_PG_DESCRIPTION_OBJOID - 1], Datum::from_oid(42).as_usize());
            assert_eq!(values[ANUM_PG_DESCRIPTION_DESCRIPTION - 1], Datum::from_usize(2).as_usize());
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
    let cx = MemoryContext::new("t");
    CreateComments(cx.mcx(), 42, 1259, 0, Some("")).unwrap();
    assert_eq!(*ACTION.lock().unwrap(), Some(CatalogAction::Delete { tuple: tid(88) }));
}

#[test]
fn create_comments_null_comment_not_found_is_noop() {
    let _g = lock();
    reset();
    let cx = MemoryContext::new("t");
    CreateComments(cx.mcx(), 42, 1259, 0, None).unwrap();
    assert_eq!(*ACTION.lock().unwrap(), None);
    assert_eq!(*CLOSED_LOCK.lock().unwrap(), Some(NoLock));
}

// --- CreateSharedComments ------------------------------------------------

#[test]
fn create_shared_comments_not_found_inserts() {
    let _g = lock();
    reset();
    let cx = MemoryContext::new("t");
    CreateSharedComments(cx.mcx(), 1234, 1262, Some("dbnote")).unwrap();
    match ACTION.lock().unwrap().clone() {
        Some(CatalogAction::Insert { values, nulls }) => {
            assert_eq!(values.len(), NATTS_PG_SHDESCRIPTION);
            assert_eq!(values[ANUM_PG_SHDESCRIPTION_OBJOID - 1], Datum::from_oid(1234).as_usize());
            assert_eq!(values[ANUM_PG_SHDESCRIPTION_CLASSOID - 1], Datum::from_oid(1262).as_usize());
            assert_eq!(values[ANUM_PG_SHDESCRIPTION_DESCRIPTION - 1], Datum::from_usize(6).as_usize());
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
    *RESOLVED.lock().unwrap() = Some((addr(1262, 1234, 0), None));

    let stmt = comment_stmt(OBJECT_DATABASE, Some("hello"));
    let cx = MemoryContext::new("t");
    let got = CommentObject(cx.mcx(), &stmt).unwrap();
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
    *RESOLVED.lock().unwrap() = Some((addr(1259, 42, 0), Some(7)));

    let stmt = comment_stmt(OBJECT_TABLE, Some("tbl note"));
    let cx = MemoryContext::new("t");
    let got = CommentObject(cx.mcx(), &stmt).unwrap();
    assert_eq!(got, addr(1259, 42, 0));
    // CreateComments(address.objectId, address.classId, address.objectSubId, comment).
    assert_eq!(FIND_KEY.lock().unwrap().clone(), Some((42, 1259, 0)));
    assert_eq!(*CLOSED_REL.lock().unwrap(), Some(7));
}

#[test]
fn comment_object_column_on_index_relkind_errors() {
    let _g = lock();
    reset();
    *RESOLVED.lock().unwrap() = Some((addr(1259, 99, 1), Some(3)));
    *RELKIND.lock().unwrap() = b'i'; // RELKIND_INDEX, not whitelisted

    let stmt = comment_stmt(OBJECT_COLUMN, Some("x"));
    let cx = MemoryContext::new("t");
    let err = CommentObject(cx.mcx(), &stmt).expect_err("index column comment must error");
    assert_eq!(err.sqlstate(), ERRCODE_WRONG_OBJECT_TYPE);
    assert!(err.message().contains("cannot set comment on relation \"my_index\""));
}

#[test]
fn comment_object_column_on_table_relkind_passes() {
    let _g = lock();
    reset();
    *RESOLVED.lock().unwrap() = Some((addr(1259, 99, 2), Some(4)));
    *RELKIND.lock().unwrap() = RELKIND_RELATION;

    let stmt = comment_stmt(OBJECT_COLUMN, Some("col note"));
    let cx = MemoryContext::new("t");
    CommentObject(cx.mcx(), &stmt).unwrap();
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
    *FOUND_COLUMN.lock().unwrap() = Some((7, false));
    let cx = MemoryContext::new("t");
    assert_eq!(GetComment(cx.mcx(), 1, 2, 0).unwrap(), Some("the comment".to_string()));
}

#[test]
fn get_comment_no_match_returns_none() {
    let _g = lock();
    reset();
    let cx = MemoryContext::new("t");
    assert_eq!(GetComment(cx.mcx(), 1, 2, 0).unwrap(), None);
}

#[test]
fn get_comment_null_description_returns_none() {
    let _g = lock();
    reset();
    // value is NULL ((Datum) 0 ByVal word), isnull set; the scan returns it but
    // GetComment must NOT call TextDatumGetCString on a null description.
    *FOUND_COLUMN.lock().unwrap() = Some((Datum::null().as_usize(), true));
    let cx = MemoryContext::new("t");
    assert_eq!(GetComment(cx.mcx(), 1, 2, 0).unwrap(), None);
}
