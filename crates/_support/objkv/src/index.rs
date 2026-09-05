//! Index reads over the store. The key format lives in `index_key`; writes are
//! staged by the table AM into the same commit object as the row changes, so
//! what is durable of an index and its table never disagrees. The parts that
//! can still disagree are above this layer: the unique-constraint read check
//! in `objkv_index::insert` and the collector's liveness rule in `db`.

use std::io;

use crate::db::View;
use crate::index_key;

/// An index scan in progress: a window of rowids in index order, and where to
/// get the next. Here rather than in the scan-descriptor vocabulary, which
/// sits above the table AM and so cannot be seen by both.
#[derive(Debug, Default)]
pub struct ScanState {
    /// Rowids from the window in hand, in index order.
    pub rows: Vec<u64>,
    pub pos: usize,
    /// Where the next window starts -- reading backwards, where it ends. `None`
    /// before the first and after the last, told apart by `started`.
    pub resume: Option<Vec<u8>>,
    pub started: bool,
    /// Reading from the top of the range down. Rows within a window are still
    /// ascending; this says which end to take them from.
    pub backward: bool,
    /// Keep each row's entry key alongside its row id. Only an index-only scan
    /// wants them -- they are the values, and reading them is the point.
    pub want_keys: bool,
    pub keys: Vec<Vec<u8>>,
    /// Which entry `next` last handed out, so its key can be asked for after.
    pub last: Option<usize>,
    /// The index tuple image handed back for an index-only scan, rebuilt in
    /// place for each row. Eight-byte units because the tuple layout needs
    /// that alignment. Valid until the next row, as an AM's image is.
    pub itup: Vec<u64>,
}

impl ScanState {
    pub fn next(&mut self) -> Option<u64> {
        let at = if self.backward {
            self.rows.len().checked_sub(self.pos + 1)?
        } else {
            self.pos
        };
        let id = self.rows.get(at).copied()?;
        self.pos += 1;
        self.last = Some(at);
        Some(id)
    }

    /// The entry key behind the row `next` just returned.
    pub fn key(&self) -> Option<&[u8]> {
        self.keys.get(self.last?).map(|k| k.as_slice())
    }

    /// Whether the caller should fetch another window before giving up.
    pub fn wants_more(&self) -> bool {
        self.pos >= self.rows.len() && (!self.started || self.resume.is_some())
    }

    pub fn reset(&mut self) {
        self.rows.clear();
        self.keys.clear();
        self.last = None;
        self.pos = 0;
        self.resume = None;
        self.started = false;
        self.backward = false;
    }
}

/// Rowids of every entry under `prefix`, as of `snapshot`, in index order.
/// `prefix` is a whole tuple for an equality lookup, or a leading subset.
pub fn lookup(db: &View, prefix: &[u8], snapshot: u64) -> io::Result<Vec<u64>> {
    Ok(db
        .scan_prefix_at(prefix, snapshot)?
        .into_iter()
        .filter_map(|(k, v)| index_key::rowid_of(&k, &v))
        .collect())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::*;
    use crate::commit::Op;
    use crate::db::{Conflict, Db, Outcome};
    use crate::index_key::{entry_key, payload, seek_prefix, Col};
    use crate::key::LATEST;
    use crate::s3::PutOutcome;
    use crate::store::{MemStore, Store};

    const IDX: u32 = 7;
    const DB: u32 = 0x2a;

    fn writes(rows: &[(&str, u64)], unique: bool) -> BTreeMap<Vec<u8>, Op> {
        let mut w = BTreeMap::new();
        for (name, rowid) in rows {
            let cols = [Col::Text(name.as_bytes())];
            w.insert(
                entry_key(DB, IDX, &cols, *rowid, unique).unwrap(),
                Op::Put(payload(*rowid)),
            );
        }
        w
    }

    fn db() -> (Arc<dyn Store>, Db) {
        let s = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let d = Db::open(Arc::clone(&s)).unwrap();
        (s, d)
    }

    /// Does the PUT the AM's writer would do, and reports back.
    fn fly(d: &mut Db, s: &Arc<dyn Store>) {
        if let Some(f) = d.take_flight() {
            match s.put_if_absent(&f.key, &f.bytes).unwrap() {
                PutOutcome::Written => d.flight_written(f.first),
                PutOutcome::AlreadyExists => d.flight_lost(&f).unwrap(),
            }
        }
    }

    /// One backend's whole transaction on the shared `Db`: stages against
    /// `snap` as `xid`, writes, waits, confirms. `Err` is the conflict
    /// detector refusing it.
    fn commit_at(
        d: &mut Db,
        s: &Arc<dyn Store>,
        w: BTreeMap<Vec<u8>, Op>,
        xid: u32,
        snap: u64,
    ) -> Result<u64, Conflict> {
        let (t, seq) = d.stage_commit(w, xid, snap, true).unwrap()?.expect("non-empty");
        fly(d, s);
        match d.take_outcome(t) {
            Some(Outcome::Durable(x)) => assert_eq!(x, seq),
            other => panic!("expected Durable, got {other:?}"),
        }
        d.mark_confirmed(seq);
        Ok(seq)
    }

    /// A transaction that read nothing, so nothing can conflict with it.
    fn commit(d: &mut Db, s: &Arc<dyn Store>, w: BTreeMap<Vec<u8>, Op>) -> u64 {
        commit_at(d, s, w, 0, LATEST).unwrap()
    }

    #[test]
    fn an_equality_lookup_finds_every_matching_row() {
        let (s, mut db) = db();
        commit(&mut db, &s, writes(&[("bob", 1), ("bob", 2), ("carol", 3)], false));

        let p = seek_prefix(DB, IDX, &[Col::Text(b"bob")], false).unwrap();
        let mut ids = lookup(&db.view(), &p, LATEST).unwrap();
        ids.sort();
        assert_eq!(ids, vec![1, 2], "both rows with this value, and only those");

        let p = seek_prefix(DB, IDX, &[Col::Text(b"carol")], false).unwrap();
        assert_eq!(lookup(&db.view(), &p, LATEST).unwrap(), vec![3]);

        let p = seek_prefix(DB, IDX, &[Col::Text(b"nobody")], false).unwrap();
        assert!(lookup(&db.view(), &p, LATEST).unwrap().is_empty());
    }

    #[test]
    fn a_unique_lookup_reads_the_rowid_out_of_the_payload() {
        // The unique shape has nowhere in the key to put the rowid, so this is
        // the path that proves the payload carries it.
        let (s, mut db) = db();
        commit(&mut db, &s, writes(&[("bob", 42)], true));

        let p = seek_prefix(DB, IDX, &[Col::Text(b"bob")], true).unwrap();
        assert_eq!(lookup(&db.view(), &p, LATEST).unwrap(), vec![42]);
    }

    #[test]
    fn a_deleted_entry_stops_matching() {
        let (s, mut db) = db();
        commit(&mut db, &s, writes(&[("bob", 1), ("bob", 2)], false));

        // What an UPDATE that moves a value away, or a DELETE, writes.
        let mut w = BTreeMap::new();
        w.insert(entry_key(DB, IDX, &[Col::Text(b"bob")], 1, false).unwrap(), Op::Delete);
        commit(&mut db, &s, w);

        let p = seek_prefix(DB, IDX, &[Col::Text(b"bob")], false).unwrap();
        assert_eq!(lookup(&db.view(), &p, LATEST).unwrap(), vec![2]);
    }

    #[test]
    fn a_lookup_reads_as_of_its_snapshot() {
        let (s, mut db) = db();
        commit(&mut db, &s, writes(&[("bob", 1)], false));
        let before = db.current_seq();

        commit(&mut db, &s, writes(&[("bob", 2)], false));

        let p = seek_prefix(DB, IDX, &[Col::Text(b"bob")], false).unwrap();
        assert_eq!(lookup(&db.view(), &p, before).unwrap(), vec![1], "the older snapshot");
        let mut now = lookup(&db.view(), &p, LATEST).unwrap();
        now.sort();
        assert_eq!(now, vec![1, 2]);
    }

    #[test]
    fn two_writers_of_one_unique_value_collide() {
        // The uniqueness mechanism, at the layer where it actually happens.
        // Two backends (xids 1 and 2) on the one shared `Db` insert 'bob' for
        // different rows from the same snapshot, without seeing each other;
        // because a unique entry is keyed on the value alone, they write the
        // same key and the conflict detector refuses the second.
        let (s, mut db) = db();
        let snap = db.current_seq();

        commit_at(&mut db, &s, writes(&[("bob", 1)], true), 1, snap).unwrap();

        let lost = commit_at(&mut db, &s, writes(&[("bob", 2)], true), 2, snap);
        let conflict = lost.expect_err("the second insert of one unique value must be refused");
        assert_eq!(
            conflict.key,
            entry_key(DB, IDX, &[Col::Text(b"bob")], 0, true).unwrap(),
            "the collision is on the index entry, not on a row"
        );
    }

    #[test]
    fn two_writers_of_null_into_a_unique_index_both_succeed() {
        // Postgres allows any number of NULLs in a unique column. NULL entries
        // keep their rowid, so they land on different keys and nothing collides.
        let (s, mut db) = db();
        let snap = db.current_seq();

        let mut wa = BTreeMap::new();
        wa.insert(entry_key(DB, IDX, &[Col::Null], 1, true).unwrap(), Op::Put(payload(1)));
        commit_at(&mut db, &s, wa, 1, snap).unwrap();

        let mut wb = BTreeMap::new();
        wb.insert(entry_key(DB, IDX, &[Col::Null], 2, true).unwrap(), Op::Put(payload(2)));
        assert!(
            commit_at(&mut db, &s, wb, 2, snap).is_ok(),
            "a second NULL in a unique column is legal"
        );
    }

    #[test]
    fn a_nonunique_index_does_not_refuse_a_duplicate() {
        // The other half of the shape rule: two rows sharing a value in a
        // non-unique index must not look like a conflict.
        let (s, mut db) = db();
        let snap = db.current_seq();

        commit_at(&mut db, &s, writes(&[("bob", 1)], false), 1, snap).unwrap();
        assert!(commit_at(&mut db, &s, writes(&[("bob", 2)], false), 2, snap).is_ok());
    }
}
