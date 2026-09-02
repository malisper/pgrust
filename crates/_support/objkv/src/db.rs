//! The core loop: memtable, commits, sorted runs, compaction, and the read
//! path that stitches them together.
//!
//! Reads run newest-to-oldest and stop at the first answer: memtable, then
//! commits since the base run, then runs. Only the last stage touches the
//! network, and the bloom filters mean it usually touches it once.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use crate::commit::{self, Commit, Entry, Op};
use std::collections::HashMap;
use crate::index_key;
use crate::key::{self, LATEST};
use crate::lease;
use crate::run::{self, Run};
use crate::s3::PutOutcome;
use crate::store::{ObjectRange, Store};

/// Compact once this many commits have piled up on top of the base run, so a
/// read never walks an unbounded number of them.
pub const COMPACT_AFTER_COMMITS: usize = 100;

pub const COMMIT_ATTEMPTS: u32 = 8;

/// Why a commit object that is already durable must never be applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Discard {
    /// The transaction aborted after its object had already landed.
    Aborted,
    /// Validation refused the commit, so the client was told it failed.
    Refused,
    /// The process died before recording the transaction's fate, so nothing
    /// ever told the client it had committed.
    Unconfirmed,
}

impl Discard {
    pub fn tag(self) -> &'static str {
        match self {
            Discard::Aborted => "aborted",
            Discard::Refused => "refused",
            Discard::Unconfirmed => "unconfirmed",
        }
    }
}

pub fn discard_body(why: Discard) -> Vec<u8> {
    format!("discard:{}", why.tag()).into_bytes()
}

fn parse_discard(body: &[u8]) -> Discard {
    match body.strip_prefix(b"discard:") {
        Some(b"aborted") => Discard::Aborted,
        Some(b"refused") => Discard::Refused,
        _ => Discard::Unconfirmed,
    }
}

pub fn discard_key(seq: u64) -> String {
    format!("resolve/{seq:016x}")
}

/// A commit that has a sequence number but is not yet in the bucket.
///
/// Every transaction commits this way under Postgres: it is numbered and
/// validated at pre-commit, then written by whoever drains the queue next --
/// one PUT for everything queued behind the one in flight. A synchronous
/// transaction waits for its ticket's outcome; an asynchronous one does not.
#[derive(Debug, Clone)]
pub struct Pending {
    pub ticket: u64,
    pub seq: u64,
    pub sync: bool,
    /// What the writes were validated against; a lost sequence race
    /// re-validates against the same point.
    pub snapshot: u64,
    /// The copy that gets written. `staged` holds another for readers and
    /// conflict detection, since that one moves on at confirmation.
    commit: Commit,
}

/// What a ticket-holder learns once its commit has been dealt with.
#[derive(Debug, Clone)]
pub enum Outcome {
    /// In the bucket, at this sequence number -- which may differ from the
    /// one staging handed out if the sequence race was lost and won again.
    Durable(u64),
    /// Refused on re-validation after a lost sequence race. Nothing landed.
    Refused(Conflict),
    /// The PUT failed and was given up on. The object may or may not exist;
    /// either way nothing vouches for it.
    Failed(String),
    /// Another writer took a sequence number this process had already
    /// acknowledged to a client. Nothing more can be trusted from here.
    Fenced(String),
}

/// One PUT's worth of pending commits, encoded. The members stay in the
/// [`Db`] while the bytes travel; `flight_written` or `flight_lost` resolves
/// them.
#[derive(Debug)]
pub struct Flight {
    pub key: String,
    pub bytes: Vec<u8>,
    /// The first member's sequence number: how the writer reports back.
    pub first: u64,
}

/// One object in flight and who rides in it.
#[derive(Debug)]
struct InFlight {
    members: Vec<Pending>,
    /// The PUT landed, but an earlier flight has not: the members wait for
    /// it, so what a client is told is durable is always a prefix.
    landed: bool,
}

/// How many objects may be in flight at once. Throughput is otherwise one
/// round trip per batch however fast the store is.
pub const MAX_IN_FLIGHT: usize = 8;

/// A row this transaction wrote was already written by a newer commit.
#[derive(Debug, Clone)]
pub struct Conflict {
    pub key: Vec<u8>,
    pub by: u64,
}

/// Drops the versions no readable snapshot can reach, given a horizon.
///
/// The inverted sequence suffix makes a row's versions contiguous and newest
/// first, so this walks each row once. Above the horizon everything stays;
/// from at-or-below only the newest, which is what a read just above resolves
/// to -- and if that is a tombstone, nothing under it is reachable either.
///
/// A tombstone that is the newest version at or below the horizon goes too
/// when every version of the row is in view: nothing is left for it to hide.
/// In a delta an older run may still hold the row, so the tombstone stays,
/// or the row would come back.
fn retain_above(entries: Vec<(Vec<u8>, Op)>, horizon: u64, keep_tombstones: bool) -> Vec<(Vec<u8>, Op)> {
    let mut out = Vec::with_capacity(entries.len());
    let mut row: Option<Vec<u8>> = None;
    let mut base_taken = false;
    for (k, op) in entries {
        let this = key::row_of(&k).map(<[u8]>::to_vec);
        if this != row {
            row = this;
            base_taken = false;
        }
        match key::seq_of(&k) {
            Some(seq) if seq > horizon => out.push((k, op)),
            Some(_) if !base_taken => {
                base_taken = true;
                if keep_tombstones || !matches!(op, Op::Delete) {
                    out.push((k, op));
                }
            }
            Some(_) => {}
            // Not a versioned key; nothing writes those, so it is kept.
            None => out.push((k, op)),
        }
    }
    out
}

/// Drops index entries whose row is gone. Nothing else removes one -- the
/// table AM has no hook and objkv has no vacuum -- so they accumulate.
///
/// Dead means the row it names has no surviving version at all: narrow, but
/// it needs no catalog lookup (this holds the storage lock, which a catalog
/// read would want too) and an UPDATE is a delete plus an insert at a fresh
/// row id, so every update strands an entry. An index whose table the caller
/// could not name is left alone; guessing would delete live entries.
fn drop_dead_entries(
    entries: Vec<(Vec<u8>, Op)>,
    index_tables: &BTreeMap<u32, u32>,
) -> Vec<(Vec<u8>, Op)> {
    if index_tables.is_empty() {
        return entries;
    }
    // A row whose newest version is a tombstone still counts as present:
    // readers below it can see the row, and will look through its entries.
    let mut live: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
    for (k, _) in &entries {
        if let Some(row) = key::row_of(k) {
            if index_key::entry_of(row, &[]).is_none() {
                live.insert(row);
            }
        }
    }

    let mut keep = Vec::with_capacity(entries.len());
    for (k, op) in &entries {
        let dead = (|| {
            let row = key::row_of(k)?;
            let payload = match op {
                Op::Put(v) => v.as_slice(),
                Op::Delete => &[],
            };
            let e = index_key::entry_of(row, payload)?;
            let relid = index_tables.get(&e.index)?;
            Some(!live.contains(index_key::row_key_of(&e, *relid).as_slice()))
        })()
        .unwrap_or(false);
        if !dead {
            keep.push((k.clone(), op.clone()));
        }
    }
    keep
}

pub fn watermark_key(seq: u64) -> String {
    format!("watermark/{seq:016x}")
}

/// Records that history at or below `seq` has been collected, so a later boot
/// knows which reads it can no longer answer.
pub fn horizon_key(seq: u64) -> String {
    format!("horizon/{seq:016x}")
}

fn oid_block_key(next: u32) -> String {
    format!("oidnext/{next:08x}")
}

pub struct Db {
    store: Arc<dyn Store>,
    /// Pending writes, not yet committed. Sorted, so a flush is already sorted.
    memtable: BTreeMap<Vec<u8>, Op>,
    /// Committed but not yet compacted, oldest first. Shared with views.
    commits: Vec<Arc<Commit>>,
    /// Sorted runs, newest first. Shared with views.
    runs: Vec<Arc<Run<ObjectRange>>>,
    /// Runs a fold replaced, kept until no view still reads them; the
    /// sweep deletes them then. A crash forgets the list, and the next open
    /// deletes what is older than the newest run.
    retired: Vec<(Arc<Run<ObjectRange>>, String)>,
    next_seq: u64,
    base_run_id: u64,
    /// Highest sequence number known to have really committed. Stamped into
    /// each new commit object, so commits vouch for the ones before them.
    confirmed_through: u64,
    /// Written to the bucket but not yet committed by Postgres. Held out of
    /// `commits` so other backends cannot read them: the object lands at
    /// pre-commit, and until Postgres commits, those rows are a dirty read.
    /// Durable, though: a crash from here keeps them, an abort marks them.
    staged: BTreeMap<u64, Commit>,
    /// Commit objects that are durable but must never be applied, by sequence
    /// number. The bucket holds the same set as `resolve/` markers.
    discarded: BTreeMap<u64, Discard>,
    /// History at or below this is gone: collection kept one version per row
    /// and dropped the rest. Reads below it are refused rather than answered
    /// from what happens to be left.
    collected_through: u64,
    /// Numbered and validated, not yet written. In sequence order.
    unwritten: Vec<Pending>,
    /// The objects being written right now, by first sequence number. Up to
    /// `MAX_IN_FLIGHT` of them; they may land in any order, but their members
    /// learn of it in order.
    in_flight: BTreeMap<u64, InFlight>,
    /// Ticket outcomes not yet collected by their holders.
    outcomes: HashMap<u64, Outcome>,
    next_ticket: u64,
    /// Every object under `commit/`, first member's sequence number to last.
    /// A batch object holds several commits and can only go when all of them
    /// have been folded.
    objects: BTreeMap<u64, u64>,
    /// Set once another writer has taken a sequence number this process had
    /// already told a client was committed. Every operation errors from then
    /// on: the bucket has a second owner and nothing here is trustworthy.
    fenced: Option<String>,
}

impl Db {

    /// A consistent read view: the runs and commits as they stand, shared
    /// rather than copied, so a reader can leave the lock behind before it
    /// touches the network. Runs are immutable objects and commits are
    /// appended and folded, never edited, so the view stays true; only what
    /// commits after it is missing, which is what a snapshot means anyway.
    pub fn view(&self) -> View {
        View {
            runs: self.runs.clone(),
            commits: self.commits.clone(),
            memtable: self.memtable.clone(),
            collected_through: self.collected_through,
        }
    }

    pub fn get_at(&self, row_key: &[u8], snapshot: u64) -> io::Result<Option<Vec<u8>>> {
        self.view().get_at(row_key, snapshot)
    }
    pub fn get_stamped_at(&self, row_key: &[u8], snapshot: u64) -> io::Result<Option<(Vec<u8>, u64)>> {
        self.view().get_stamped_at(row_key, snapshot)
    }
    pub fn get(&self, row_key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.view().get(row_key)
    }
    pub fn get_any(&self, row_key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.view().get_any(row_key)
    }
    pub fn emptied_at(&self, marker_key: &[u8], snapshot: u64) -> io::Result<Option<u64>> {
        self.view().emptied_at(marker_key, snapshot)
    }
    pub fn scan_range_at(&self, lo: &[u8], hi: &[u8], snapshot: u64) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.view().scan_range_at(lo, hi, snapshot)
    }
    pub fn scan_window_at(&self, lo: &[u8], hi: &[u8], snapshot: u64, limit: usize) -> io::Result<(Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>)> {
        self.view().scan_window_at(lo, hi, snapshot, limit)
    }
    pub fn scan_window_back_at(&self, lo: &[u8], hi: &[u8], snapshot: u64, limit: usize) -> io::Result<(Vec<(Vec<u8>, Vec<u8>, u64)>, Option<Vec<u8>>)> {
        self.view().scan_window_back_at(lo, hi, snapshot, limit)
    }
    pub fn scan_window_stamped_at(&self, lo: &[u8], hi: &[u8], snapshot: u64, limit: usize) -> io::Result<(Vec<(Vec<u8>, Vec<u8>, u64)>, Option<Vec<u8>>)> {
        self.view().scan_window_stamped_at(lo, hi, snapshot, limit)
    }
    pub fn scan_prefix_at(&self, prefix: &[u8], snapshot: u64) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.view().scan_prefix_at(prefix, snapshot)
    }
    pub fn scan_prefix(&self, prefix: &[u8]) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.view().scan_prefix(prefix)
    }

    /// Opens for writing, taking the single-writer lease first.
    pub fn open(store: Arc<dyn Store>) -> io::Result<Db> {
        lease::acquire(&store)?;
        Db::open_with(store)
    }

    /// Rebuilds state from the bucket: newest run, then every commit on top of
    /// it. A self-confirmed object is a commit unless a discard marker says
    /// its transaction aborted after it landed. An object without the flag
    /// proves only that its data was staged, and needs the commit after it,
    /// or a watermark, to vouch for it.
    /// Does not take the lease; writers use [`Db::open`].
    pub fn open_with(store: Arc<dyn Store>) -> io::Result<Db> {
        let mut runs = Vec::new();
        let mut base_run_id = 0u64;
        let mut listed = store.list("run/")?;
        listed.sort_by(|a, b| b.key.cmp(&a.key)); // newest first
        // A full run covers every commit up to its number; a delta covers
        // the commits between the run before it and its own number. Live is
        // the newest full run and every delta above it. The rest are
        // leftovers of a merge whose sweep never finished, and go now.
        let newest_full = listed
            .iter()
            .filter(|i| !run::is_delta(&i.key))
            .map(|i| id_from(&i.key))
            .max()
            .unwrap_or(0);
        for info in &listed {
            let id = id_from(&info.key);
            let live = if run::is_delta(&info.key) { id > newest_full } else { id == newest_full };
            if !live {
                let _ = store.delete(&info.key);
                continue;
            }
            base_run_id = base_run_id.max(id);
            runs.push(Arc::new(Run::open(ObjectRange {
                store: Arc::clone(&store),
                key: info.key.clone(),
                size: info.size,
            })?));
        }

        let mut decoded = Vec::new();
        let mut next_seq = 1u64;
        let mut confirmed_through = base_run_id;
        let mut objects = BTreeMap::new();
        let mut ckeys = store.list("commit/")?;
        ckeys.sort_by(|a, b| a.key.cmp(&b.key));
        for info in ckeys {
            let first = id_from(&info.key);
            let bytes = store
                .get(&info.key)?
                .ok_or_else(|| io::Error::other("listed commit disappeared"))?;
            // Read even when the first member is folded: a batch can straddle
            // the run boundary, and the members past it are still live.
            let members = commit::decode_object(&bytes)?;
            let last = members.last().map_or(first, |c| c.seq);
            next_seq = next_seq.max(last + 1);
            objects.insert(first, last);
            for c in members {
                if c.seq <= base_run_id {
                    continue; // already folded into the run
                }
                confirmed_through = confirmed_through.max(c.confirmed_through);
                decoded.push(c);
            }
        }

        // Watermarks published outside the commit stream.
        for info in store.list("watermark/")? {
            confirmed_through = confirmed_through.max(id_from(&info.key));
        }

        // A run consumes a sequence number too. Without this, compact,
        // restart, compact reuses the id and fails quietly as "run already
        // exists", and the commit chain then grows for ever.
        next_seq = next_seq.max(base_run_id + 1);

        let mut collected_through = 0u64;
        for info in store.list("horizon/")? {
            collected_through = collected_through.max(id_from(&info.key));
        }

        let mut discarded: BTreeMap<u64, Discard> = BTreeMap::new();
        for info in store.list("resolve/")? {
            let seq = id_from(&info.key);
            let body = store.get(&info.key)?.unwrap_or_default();
            discarded.insert(seq, parse_discard(&body));
        }

        let mut commits = Vec::with_capacity(decoded.len());
        let mut orphans: Vec<(u64, u32)> = Vec::new();
        for c in decoded {
            // The marker first: a commit that aborted after landing can still
            // be vouched for by a later header, when a commit numbered above
            // it confirmed before it aborted. The marker is the recorded fate.
            if discarded.contains_key(&c.seq) {
                // Already known dead. Left in the bucket for the collector.
            } else if c.self_confirmed || c.seq <= confirmed_through {
                confirmed_through = confirmed_through.max(c.seq);
                commits.push(Arc::new(c));
            } else {
                // Died between this object landing and the commit that would
                // have vouched for it, so no client was ever told. The torn
                // tail of a WAL: it did not happen.
                orphans.push((c.seq, c.xid));
            }
        }

        for &(seq, _) in &orphans {
            // Stops the next open re-deciding, and tells the collector the
            // object is garbage. Failing to write it reaches the same verdict
            // from the same evidence.
            if let Err(e) = store.put_if_absent(&discard_key(seq), &discard_body(Discard::Unconfirmed))
            {
                eprintln!("objkv: commit {seq} discarded, but its marker did not write ({e})");
            }
            discarded.insert(seq, Discard::Unconfirmed);
        }
        if !orphans.is_empty() {
            let list = orphans
                .iter()
                .map(|(seq, xid)| format!("seq {seq} (xid {xid})"))
                .collect::<Vec<_>>()
                .join(", ");
            eprintln!(
                "objkv: discarded {} unconfirmed commit object(s) left behind by a crash: \
                 {list}",
                orphans.len()
            );
        }

        Ok(Db {
            store,
            memtable: BTreeMap::new(),
            commits,
            runs,
            retired: Vec::new(),
            next_seq,
            base_run_id,
            confirmed_through,
            staged: BTreeMap::new(),
            discarded,
            collected_through,
            unwritten: Vec::new(),
            in_flight: BTreeMap::new(),
            outcomes: HashMap::new(),
            next_ticket: 1,
            objects,
            fenced: None,
        })
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.memtable.insert(key.to_vec(), Op::Put(value.to_vec()));
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.memtable.insert(key.to_vec(), Op::Delete);
    }

    /// A reader captures this to pin a snapshot.
    pub fn current_seq(&self) -> u64 {
        self.next_seq.saturating_sub(1)
    }

    /// Commits the memtable, confirming it immediately: a direct caller has no
    /// second commit decision, so the PUT is the whole transaction. Under
    /// Postgres there is one, which is why `commit_batch` does not.
    pub fn commit(&mut self) -> io::Result<Option<u64>> {
        let writes = std::mem::take(&mut self.memtable);
        let seq = self.commit_batch_flagged(writes, 0, true)?;
        if let Some(seq) = seq {
            self.mark_confirmed(seq);
        }
        Ok(seq)
    }

    /// Called once Postgres has committed, not before. This is where a staged
    /// commit becomes visible to other backends.
    pub fn mark_confirmed(&mut self, seq: u64) {
        if let Some(c) = self.staged.remove(&seq) {
            self.commits.push(Arc::new(c));
            self.commits.sort_by_key(|c| c.seq);
        }
        self.confirmed_through = self.confirmed_through.max(seq);
    }

    /// Drops a staged commit that will never become real.
    ///
    /// The object is already durable and cannot be taken back, so the bucket
    /// records why it must never be applied. Without the marker the next open
    /// still discards it, but as an orphan rather than a refusal.
    pub fn discard_staged(&mut self, seq: u64, why: Discard) {
        self.staged.remove(&seq);
        // Not written yet: it never will be, and a number nothing was ever
        // written under needs no marker. Nothing lists it, so nothing can
        // find it and wonder.
        if let Some(i) = self.unwritten.iter().position(|p| p.seq == seq) {
            let p = self.unwritten.remove(i);
            self.outcomes.remove(&p.ticket);
            return;
        }
        match self.store.put_if_absent(&discard_key(seq), &discard_body(why)) {
            Ok(_) => {
                self.discarded.insert(seq, why);
            }
            // Not fatal: an unmarked commit is unvouched-for, so the next open
            // discards it anyway. The marker only records why.
            Err(e) => eprintln!(
                "objkv: commit {seq} was {} but its discard marker did not write ({e})",
                why.tag()
            ),
        }
    }

    pub fn confirmed_through(&self) -> u64 {
        self.confirmed_through
    }

    /// Publishes the watermark as its own object, so the newest commit is not
    /// left waiting for traffic that never comes after a clean shutdown.
    pub fn flush_watermark(&self) -> io::Result<bool> {
        // Never past what is in the bucket: an asynchronous commit is
        // confirmed before it is written, and a watermark must not vouch for
        // an object that may never land.
        let through = self.confirmed_through.min(self.durable_floor().saturating_sub(1));
        if through == 0 {
            return Ok(false);
        }
        Ok(self.store.put_if_absent(&watermark_key(through), through.to_string().as_bytes())?
            == PutOutcome::Written)
    }

    /// The lowest sequence number that is not yet in the bucket, or `MAX`
    /// when everything numbered has landed. Nothing at or above it may be
    /// folded into a run or vouched for by a watermark.
    fn durable_floor(&self) -> u64 {
        self.unwritten
            .iter()
            .chain(self.in_flight.values().filter(|f| !f.landed).flat_map(|f| f.members.iter()))
            .map(|p| p.seq)
            .min()
            .unwrap_or(u64::MAX)
    }

    fn in_flight_members(&self) -> impl Iterator<Item = &Pending> {
        self.in_flight.values().flat_map(|f| f.members.iter())
    }

    /// Whether a sequence number is already accounted for here, in any state.
    fn known(&self, seq: u64) -> bool {
        self.staged.contains_key(&seq)
            || self.commits.iter().any(|c| c.seq == seq)
            || self.unwritten.iter().chain(self.in_flight_members()).any(|p| p.seq == seq)
    }

    fn check_fenced(&self) -> io::Result<()> {
        match &self.fenced {
            Some(why) => Err(io::Error::other(why.clone())),
            None => Ok(()),
        }
    }

    pub fn is_fenced(&self) -> bool {
        self.fenced.is_some()
    }

    /// Numbers and validates a commit without writing it. The write is a
    /// separate step so that several transactions' commits share one PUT.
    ///
    /// The object is self-confirmed: once it lands, the commit is durable and
    /// nothing later need vouch for it, so a synchronous COMMIT returns as
    /// soon as the PUT does and a crash cannot take back what a client saw.
    /// A transaction that aborts after its object landed writes a discard
    /// marker, and the marker outranks the object on the next open.
    ///
    /// Validation is first-committer-wins against every commit newer than
    /// `snapshot`, including the ones queued ahead of this one: two
    /// transactions in one group writing one row still conflict. Returns the
    /// ticket to wait on and the sequence number handed out -- final for an
    /// asynchronous commit, provisional for a synchronous one, whose outcome
    /// says where it landed.
    pub fn stage_commit(
        &mut self,
        writes: BTreeMap<Vec<u8>, Op>,
        xid: u32,
        snapshot: u64,
        sync: bool,
    ) -> io::Result<Result<Option<(u64, u64)>, Conflict>> {
        self.check_fenced()?;
        if writes.is_empty() {
            return Ok(Ok(None));
        }
        if let Some(c) = self.find_conflict(&writes, snapshot, None)? {
            return Ok(Err(c));
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        let ticket = self.next_ticket;
        self.next_ticket += 1;
        let entries: Vec<Entry> =
            writes.into_iter().map(|(key, op)| Entry { key, op }).collect();
        let c = Commit {
            seq,
            base_run_id: self.base_run_id,
            xid,
            confirmed_through: self.confirmed_through,
            self_confirmed: true,
            entries,
        };
        self.staged.insert(seq, c.clone());
        self.unwritten.push(Pending { ticket, seq, sync, snapshot, commit: c });
        Ok(Ok(Some((ticket, seq))))
    }

    /// Everything queued, as one object, while fewer than `MAX_IN_FLIGHT`
    /// are being written. The caller does the PUT -- outside whatever lock
    /// guards this `Db`, so readers are not held up by the network -- and
    /// reports back with `flight_written`, `flight_lost` or `flight_failed`.
    pub fn take_flight(&mut self) -> Option<Flight> {
        if self.unwritten.is_empty() || self.in_flight.len() >= MAX_IN_FLIGHT || self.fenced.is_some() {
            return None;
        }
        let members = std::mem::take(&mut self.unwritten);
        let mut commits: Vec<Commit> = Vec::with_capacity(members.len());
        for p in &members {
            let mut c = p.commit.clone();
            // Stamped now, not at staging: vouching is for what is known
            // committed at the moment the object is written, and the commit
            // ahead of this one may have confirmed in between.
            c.confirmed_through = self.confirmed_through;
            commits.push(c);
        }
        let first = commits[0].seq;
        let key = commit::key_for(first);
        let bytes = if commits.len() == 1 {
            commits[0].encode()
        } else {
            commit::encode_batch(&commits)
        };
        self.in_flight.insert(first, InFlight { members, landed: false });
        Some(Flight { key, bytes, first })
    }

    /// How many commits have been acknowledged to a client but are not yet
    /// in the bucket. What a crash loses, and what a failed PUT fences over;
    /// the caller caps it by making the next asynchronous commit wait.
    pub fn async_backlog(&self) -> usize {
        self.unwritten.iter().chain(self.in_flight_members()).filter(|p| !p.sync).count()
    }

    /// Whether a write is still owed to the bucket. Never true once fenced:
    /// nothing more will be written, and a caller draining before exit must
    /// not wait for it.
    pub fn has_unwritten(&self) -> bool {
        self.fenced.is_none() && (!self.unwritten.is_empty() || !self.in_flight.is_empty())
    }

    /// The PUT landed: every member is durable at the number it holds. They
    /// are told once every earlier flight has landed too, so that what has
    /// been acknowledged is always an unbroken prefix of the sequence -- a
    /// crash then loses a tail, never a hole a later commit was built on.
    pub fn flight_written(&mut self, first: u64) {
        if let Some(f) = self.in_flight.get_mut(&first) {
            f.landed = true;
        }
        self.release_landed_prefix();
    }

    fn release_landed_prefix(&mut self) {
        while let Some((&first, f)) = self.in_flight.iter().next() {
            if !f.landed {
                return;
            }
            let f = self.in_flight.remove(&first).expect("just seen");
            if let Some(last) = f.members.last() {
                self.objects.insert(first, last.seq);
            }
            for m in f.members {
                self.outcomes.insert(m.ticket, Outcome::Durable(m.seq));
            }
        }
    }

    /// The PUT could not be made and will not be retried. Nothing vouches for
    /// the object if it did land, so the next open discards it. A member
    /// already acknowledged to a client cannot be un-acknowledged, which is
    /// why that case fences the process rather than reporting an error nobody
    /// is waiting for. Later flights that had landed behind this one are
    /// released: the hole is in a commit nobody was told about.
    pub fn flight_failed(&mut self, first: u64, why: &str) {
        let members = self.in_flight.remove(&first).map(|f| f.members).unwrap_or_default();
        self.resolve_failed(members, why);
        self.release_landed_prefix();
    }

    fn resolve_failed(&mut self, members: Vec<Pending>, why: &str) {
        let lost: Vec<u64> = members.iter().filter(|m| !m.sync).map(|m| m.seq).collect();
        for m in &members {
            self.staged.remove(&m.seq);
        }
        if !lost.is_empty() {
            self.fence(format!(
                "objkv: commit object could not be written ({why}) and it carried commits \
                 already acknowledged to clients ({lost:?}); this server can no longer be \
                 trusted with the bucket. Restart it."
            ));
            for m in members {
                self.outcomes.insert(m.ticket, Outcome::Fenced(why.to_string()));
            }
            return;
        }
        for m in members {
            self.outcomes.insert(m.ticket, Outcome::Failed(why.to_string()));
        }
    }

    /// The PUT found the key taken.
    ///
    /// Usually that is our own object, written by an attempt whose response
    /// was lost: compared byte for byte and counted as written. Otherwise
    /// another writer holds the number. Members nobody has been told about
    /// are re-validated and re-numbered behind whatever that writer did, as
    /// a single commit always has been; a member already acknowledged cannot
    /// be, and fences the process.
    pub fn flight_lost(&mut self, flight: &Flight) -> io::Result<()> {
        match self.store.get(&flight.key) {
            Ok(Some(b)) if b == flight.bytes => {
                self.flight_written(flight.first);
                return Ok(());
            }
            Ok(_) => {}
            Err(e) => {
                self.flight_failed(flight.first, &e.to_string());
                return Err(e);
            }
        }
        let members = self.in_flight.remove(&flight.first).map(|f| f.members).unwrap_or_default();
        for m in &members {
            self.staged.remove(&m.seq);
        }
        // Every member is resolved one way or another before this returns:
        // a ticket-holder is waiting on each, and a member dropped on the
        // floor here would wait for ever.
        if let Err(e) = self.catch_up() {
            self.resolve_failed(members, &e.to_string());
            return Err(e);
        }

        let acknowledged: Vec<u64> = members.iter().filter(|m| !m.sync).map(|m| m.seq).collect();
        if !acknowledged.is_empty() {
            let why = format!(
                "objkv: another writer took commit number {} while this server held commits \
                 already acknowledged to clients ({acknowledged:?}). The bucket has two \
                 owners; this one stops. Those transactions are lost.",
                id_from(&flight.key)
            );
            for m in &members {
                self.outcomes.insert(m.ticket, Outcome::Fenced(why.clone()));
            }
            self.fence(why);
            return Ok(());
        }

        for mut m in members {
            if self.discarded.contains_key(&m.seq) {
                continue; // aborted while in flight; never landed, nothing to redo
            }
            let writes: BTreeMap<Vec<u8>, Op> =
                m.commit.entries.iter().map(|e| (e.key.clone(), e.op.clone())).collect();
            if let Some(c) = self.find_conflict(&writes, m.snapshot, None)? {
                self.outcomes.insert(m.ticket, Outcome::Refused(c));
                continue;
            }
            let seq = self.next_seq;
            self.next_seq += 1;
            m.seq = seq;
            m.commit.seq = seq;
            self.staged.insert(seq, m.commit.clone());
            self.unwritten.push(m);
        }
        self.release_landed_prefix();
        Ok(())
    }

    fn fence(&mut self, why: String) {
        eprintln!("{why}");
        if self.fenced.is_none() {
            self.fenced = Some(why);
        }
    }

    /// The outcome for `ticket`, once there is one. Taken, not peeked: each
    /// ticket has exactly one holder.
    pub fn take_outcome(&mut self, ticket: u64) -> Option<Outcome> {
        self.outcomes.remove(&ticket)
    }

    /// One batch of changes as one object: one put-if-absent, all-or-nothing.
    ///
    /// Losing the PUT means another writer took that sequence number — a name
    /// collision, not a data conflict — so catch up and retry at the next free
    /// one. Does not confirm; the caller owns that (see [`Db::commit`]).
    pub fn commit_batch(
        &mut self,
        writes: BTreeMap<Vec<u8>, Op>,
        xid: u32,
    ) -> io::Result<Option<u64>> {
        self.commit_batch_flagged(writes, xid, false)
    }

    /// First-committer-wins validation: refuses if any key we wrote was also
    /// written by a commit newer than `snapshot`.
    ///
    /// `Err(Conflict)` is the cue to raise a serialization failure. Without it
    /// the later commit lands on top and the earlier update disappears.
    pub fn commit_batch_at(
        &mut self,
        writes: BTreeMap<Vec<u8>, Op>,
        xid: u32,
        snapshot: u64,
    ) -> io::Result<Result<Option<u64>, Conflict>> {
        if let Some(c) = self.find_conflict(&writes, snapshot, None)? {
            return Ok(Err(c));
        }
        // Another writer can claim our sequence number between here and the
        // PUT, so re-check against whatever the retry learned -- skipping our
        // own staged commit, whose entries are the very writes being checked.
        let before = self.next_seq;
        let seq = self.commit_batch_flagged(writes.clone(), xid, false)?;
        if self.next_seq > before + 1 {
            if let Some(c) = self.find_conflict(&writes, snapshot, seq)? {
                if let Some(s) = seq {
                    self.discard_staged(s, Discard::Refused);
                }
                return Ok(Err(c));
            }
        }
        Ok(Ok(seq))
    }

    fn find_conflict(
        &self,
        writes: &BTreeMap<Vec<u8>, Op>,
        snapshot: u64,
        ours: Option<u64>,
    ) -> io::Result<Option<Conflict>> {
        for c in self.commits.iter().map(|c| &**c).chain(self.staged.values()) {
            if c.seq <= snapshot || Some(c.seq) == ours {
                continue;
            }
            for e in &c.entries {
                if writes.contains_key(&e.key) {
                    return Ok(Some(Conflict { key: e.key.clone(), by: c.seq }));
                }
            }
        }

        // Compaction empties `commits` of everything it folded, so a snapshot
        // from below the base run leaves a gap the loop above cannot see: the
        // conflicting version now exists only inside the run. Ask the run for
        // the newest version of each key we are writing. Snapshots at or above
        // the base run need none of this, which is the ordinary case.
        if snapshot < self.base_run_id {
            for key in writes.keys() {
                for r in &self.runs {
                    match r.seq_at(key, LATEST)? {
                        Some(seq) if seq > snapshot && Some(seq) != ours => {
                            return Ok(Some(Conflict { key: key.clone(), by: seq }));
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(None)
    }

    fn commit_batch_flagged(
        &mut self,
        writes: BTreeMap<Vec<u8>, Op>,
        xid: u32,
        self_confirmed: bool,
    ) -> io::Result<Option<u64>> {
        if writes.is_empty() {
            return Ok(None);
        }
        let entries: Vec<Entry> =
            writes.into_iter().map(|(key, op)| Entry { key, op }).collect();

        for _ in 0..COMMIT_ATTEMPTS {
            let c = Commit {
                seq: self.next_seq,
                base_run_id: self.base_run_id,
                xid,
                confirmed_through: self.confirmed_through,
                self_confirmed,
                entries: entries.clone(),
            };
            match self.store.put_if_absent(&commit::key_for(c.seq), &c.encode())? {
                PutOutcome::Written => {
                    let seq = c.seq;
                    if self_confirmed {
                        self.commits.push(Arc::new(c));
                    } else {
                        self.staged.insert(seq, c);
                    }
                    self.objects.insert(seq, seq);
                    self.next_seq += 1;
                    return Ok(Some(seq));
                }
                PutOutcome::AlreadyExists => self.catch_up()?,
            }
        }
        Err(io::Error::other(format!(
            "objkv: could not claim a commit sequence number after {COMMIT_ATTEMPTS} attempts \
             (next was {}); another writer is committing faster than we can",
            self.next_seq
        )))
    }

    fn catch_up(&mut self) -> io::Result<()> {
        let mut keys = self.store.list("commit/")?;
        keys.sort_by(|a, b| a.key.cmp(&b.key));
        let mut fresh = Vec::new();
        for info in keys {
            let first = id_from(&info.key);
            if self.objects.contains_key(&first) || first <= self.base_run_id {
                continue;
            }
            let bytes = self
                .store
                .get(&info.key)?
                .ok_or_else(|| io::Error::other("listed commit disappeared"))?;
            let members = commit::decode_object(&bytes)?;
            let last = members.last().map_or(first, |c| c.seq);
            self.objects.insert(first, last);
            self.next_seq = self.next_seq.max(last + 1);
            for c in members {
                if self.known(c.seq) {
                    continue;
                }
                // Discarded objects stay in the bucket for the collector, so
                // they are listed here; a commit some boot ruled dead stays dead.
                if self.discarded.contains_key(&c.seq) {
                    continue;
                }
                // Commits vouch for the ones before them, so read the whole
                // batch before judging any of it, exactly as `open_with` does.
                self.confirmed_through = self.confirmed_through.max(c.confirmed_through);
                fresh.push(c);
            }
        }

        for c in fresh {
            if c.self_confirmed || c.seq <= self.confirmed_through {
                self.confirmed_through = self.confirmed_through.max(c.seq);
                self.commits.push(Arc::new(c));
            } else {
                // In flight: this runs when another writer took our sequence
                // number, so not seeing their commit is not detecting the
                // conflict. `staged` is where a write that is durable but not
                // yet committed belongs -- `find_conflict` reads it, while
                // reads and the compaction fold do not, so an abort over there
                // cannot become a dirty read or permanent run data here.
                self.staged.insert(c.seq, c);
            }
        }
        Ok(())
    }

    pub fn needs_compaction(&self) -> bool {
        self.commits.len() >= COMPACT_AFTER_COMMITS
    }

    /// Folds the settled commits into a new run, keeping every version. What
    /// the bucket has always done; synchronous, for direct callers and tests.
    /// The server runs the same steps from its compactor thread, with the
    /// network work outside the storage lock.
    pub fn compact(&mut self) -> io::Result<u64> {
        self.compact_retaining(0, &BTreeMap::new())
    }

    /// Folds, and on the way drops what no readable snapshot can reach.
    ///
    /// History at or below `horizon` is forfeit: one version per row survives
    /// from down there. 0 collects nothing, and choosing it is the caller's
    /// job -- it depends on open snapshots and on how far back reads are
    /// promised, neither knowable here. This is the only place collection
    /// happens, so a database nobody writes to never shrinks.
    pub fn compact_retaining(
        &mut self,
        horizon: u64,
        index_tables: &BTreeMap<u32, u32>,
    ) -> io::Result<u64> {
        let Some(plan) = self.fold_plan() else {
            return Ok(self.base_run_id); // nothing settled enough to fold
        };
        let new_id = plan.new_id;
        let folded = build_fold(&plan, horizon, index_tables)?;
        put_fold(&self.store, &folded)?;
        let sweep = self.apply_fold(plan, &folded, horizon)?;
        let result = execute_sweep(&self.store, sweep);
        self.sweep_done(result);
        Ok(new_id)
    }

    /// What the next fold would do, if anything: which commits it takes,
    /// whether it writes a delta run of just those or merges every run into
    /// one. Cheap, and taken under the lock; the work it describes is done
    /// without it by `build_fold`.
    ///
    /// A run stands for "every commit up to my number", so it may only cover
    /// an unbroken prefix. Folding at or above a written-but-unconfirmed
    /// commit makes the next open skip it, losing it. Nor at or above a
    /// commit not yet in the bucket: an asynchronous commit is confirmed
    /// before it is written, and a run that claimed to cover it would make
    /// the next open skip the object it finally lands in.
    pub fn fold_plan(&self) -> Option<FoldPlan> {
        let cutoff = self
            .staged
            .keys()
            .min()
            .copied()
            .unwrap_or(u64::MAX)
            .min(self.durable_floor());
        let new_id = self.commits.iter().map(|c| c.seq).filter(|&s| s < cutoff).max()?;
        let commits: Vec<Arc<Commit>> =
            self.commits.iter().filter(|c| c.seq <= new_id).cloned().collect();
        // A delta is cheap: only the new commits, no run is read. The runs
        // are merged into one when there are `MAX_RUNS` of them, or when the
        // deltas together outweigh the full run they sit on -- so a large
        // table is rewritten once per its own size of new data, and a small
        // one under churn is merged nearly every fold. The merge is where
        // versions below the horizon and entries of dead rows are dropped
        // for good; that needs every run in view.
        let full_bytes = self.runs.last().map_or(0, |r| r.source().size);
        let delta_bytes: u64 =
            self.runs.iter().take(self.runs.len().saturating_sub(1)).map(|r| r.source().size).sum();
        let merge = self.runs.is_empty() || self.runs.len() >= MAX_RUNS || delta_bytes >= full_bytes;
        Some(FoldPlan {
            new_id,
            merge,
            commits,
            runs: if merge { self.runs.clone() } else { Vec::new() },
        })
    }

    /// The new run is durable: swap it in. Returns what the sweep may now
    /// delete, for `execute_sweep` to do outside the lock.
    pub fn apply_fold(
        &mut self,
        plan: FoldPlan,
        folded: &Folded,
        horizon: u64,
    ) -> io::Result<SweepPlan> {
        // The plan's hold on the runs it merged ends here, or the sweep would
        // count it as a reader and keep them.
        let FoldPlan { new_id, merge, commits: _, runs: plan_runs } = plan;
        drop(plan_runs);
        let new_run = Arc::new(Run::open(ObjectRange {
            store: Arc::clone(&self.store),
            key: folded.key.clone(),
            size: folded.bytes.len() as u64,
        })?);
        if merge {
            let replaced = std::mem::replace(&mut self.runs, vec![new_run]);
            for r in replaced {
                let k = r.source().key.clone();
                self.retired.push((r, k));
            }
        } else {
            self.runs.insert(0, new_run);
        }
        let folded_seqs: Vec<u64> =
            self.commits.iter().map(|c| c.seq).filter(|&s| s <= new_id).collect();
        self.commits.retain(|c| c.seq > new_id);
        self.base_run_id = new_id;
        self.confirmed_through = self.confirmed_through.max(new_id);

        let mut publish_horizon = None;
        if horizon > self.collected_through {
            // Raise the bar first, publish second. Those versions are already
            // gone from the run just written, so a failed marker write in the
            // other order would leave collected_through low and let a read
            // below the horizon be answered from the survivors -- fewer rows,
            // nothing reported wrong.
            self.collected_through = horizon;
            publish_horizon = Some(horizon);
        }

        // A replaced run goes once nothing reads it. A view holding it keeps
        // it for a later sweep.
        let (gone, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut self.retired)
            .into_iter()
            .partition(|(r, _)| Arc::strong_count(r) == 1);
        self.retired = kept;

        // An object goes once every commit in it is at or below the run: each
        // is then folded, or was discarded and never folded into anything --
        // and nothing else would ever remove those. A batch with one live
        // member past the boundary stays whole.
        let done: Vec<(u64, u64)> = self
            .objects
            .iter()
            .filter(|(_, &last)| last <= self.base_run_id)
            .map(|(&first, &last)| (first, last))
            .collect();
        let markers: Vec<u64> = done
            .iter()
            .flat_map(|&(first, last)| self.discarded.range(first..=last).map(|(&s, _)| s))
            .collect();

        Ok(SweepPlan {
            run_key: folded.key.clone(),
            expected: folded.kept,
            retired: gone,
            folded: folded_seqs,
            done,
            markers,
            publish_horizon,
            collected_through: self.collected_through,
        })
    }

    /// Bookkeeping for what `execute_sweep` deleted.
    pub fn sweep_done(&mut self, result: SweepResult) {
        for first in result.objects {
            self.objects.remove(&first);
        }
        for seq in result.markers {
            self.discarded.remove(&seq);
        }
    }

    /// How far back reads are still answerable. Below this, history was
    /// collected and the honest answer is an error.
    pub fn collected_through(&self) -> u64 {
        self.collected_through
    }

    /// Claims a block of object ids, and records in the bucket that everything
    /// below its end is spoken for.
    ///
    /// Postgres persists this in the WAL and re-reads it from the control
    /// file; a cluster whose catalogs live here has neither that survives a
    /// blank machine. Same technique, different medium: the boundary is
    /// written before any id inside the block is handed out, so a boot resumes
    /// above everything the previous one could possibly have used.
    ///
    /// Returns the first id of the block, which is never below what the bucket
    /// already promised -- that is what stops a fresh counter handing out ids
    /// a previous cluster already gave to live rows.
    pub fn claim_oid_block(&mut self, want: u32, prefetch: u32) -> io::Result<u32> {
        let mut floor = 0u32;
        let mut old: Vec<String> = Vec::new();
        for info in self.store.list("oidnext/")? {
            if let Some(v) = info.key.rsplit('/').next() {
                if let Ok(n) = u32::from_str_radix(v, 16) {
                    floor = floor.max(n);
                }
            }
            old.push(info.key);
        }
        let start = want.max(floor);
        let end = start.saturating_add(prefetch);
        self.store.put_if_absent(&oid_block_key(end), b"")?;
        // Only after the new boundary is durable, and never fatal: a leftover
        // is one extra key and one extra comparison next time.
        for key in old {
            let _ = self.store.delete(&key);
        }
        Ok(start)
    }

    pub fn commit_backlog(&self) -> usize {
        self.commits.len()
    }
    pub fn run_count(&self) -> usize {
        self.runs.len()
    }
}

/// What a read needs, taken from the `Db` under its lock and used without it.
/// See [`Db::view`].
#[derive(Clone)]
pub struct View {
    /// Sorted runs, newest first.
    runs: Vec<Arc<Run<ObjectRange>>>,
    /// Committed but not yet compacted, oldest first.
    commits: Vec<Arc<Commit>>,
    /// Uncommitted writes of a direct caller; empty under Postgres, which
    /// stages its own.
    memtable: BTreeMap<Vec<u8>, Op>,
    collected_through: u64,
}

impl View {
    fn readable_at(&self, snapshot: u64) -> io::Result<()> {
        if snapshot != LATEST && snapshot < self.collected_through {
            return Err(io::Error::other(format!(
                "objkv: cannot read as of commit {snapshot}: history at or below {} has been \
                 collected. The oldest readable point is {}.",
                self.collected_through, self.collected_through
            )));
        }
        Ok(())
    }

    /// `row_key` as it stood at `snapshot`, newest layer first. The memtable
    /// holds writes with no sequence number yet, so only a present-tense read
    /// consults it.
    pub fn get_at(&self, row_key: &[u8], snapshot: u64) -> io::Result<Option<Vec<u8>>> {
        self.readable_at(snapshot)?;
        if snapshot == LATEST {
            if let Some(op) = self.memtable.get(row_key) {
                return Ok(resolve(op));
            }
        }
        for c in self.commits.iter().rev() {
            if c.seq > snapshot {
                continue;
            }
            if let Some(op) = c.lookup(row_key) {
                return Ok(resolve(op));
            }
        }
        for r in &self.runs {
            if let Some(op) = r.get_at(row_key, snapshot)? {
                return Ok(resolve(&op));
            }
        }
        Ok(None)
    }

    /// The same, and which commit the version came from.
    pub fn get_stamped_at(
        &self,
        row_key: &[u8],
        snapshot: u64,
    ) -> io::Result<Option<(Vec<u8>, u64)>> {
        self.readable_at(snapshot)?;
        if snapshot == LATEST {
            if let Some(op) = self.memtable.get(row_key) {
                return Ok(resolve(op).map(|v| (v, LATEST)));
            }
        }
        for c in self.commits.iter().rev() {
            if c.seq > snapshot {
                continue;
            }
            if let Some(op) = c.lookup(row_key) {
                return Ok(resolve(op).map(|v| (v, c.seq)));
            }
        }
        for r in &self.runs {
            if let Some((vk, op)) = r.locate_stamped_at(row_key, snapshot)? {
                let seq = key::seq_of(&vk).unwrap_or(0);
                return Ok(resolve(&op).map(|v| (v, seq)));
            }
        }
        Ok(None)
    }

    /// Reads the present.
    pub fn get(&self, row_key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.get_at(row_key, LATEST)
    }

    /// The newest value this row ever had, seeing through tombstones.
    ///
    /// What SnapshotAny means: the executor asks for "the tuple at this tid"
    /// while re-checking a row another transaction has since deleted, and a
    /// tombstone is not an answer it can use.
    pub fn get_any(&self, row_key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let mut at = LATEST;
        // Bounded: each step moves strictly below the tombstone it just found.
        for _ in 0..64 {
            match self.locate(row_key, at)? {
                None => return Ok(None),
                Some((_, Op::Put(v))) => return Ok(Some(v)),
                Some((seq, Op::Delete)) => {
                    if seq == 0 {
                        return Ok(None);
                    }
                    at = seq - 1;
                }
            }
        }
        Ok(None)
    }

    /// The version live at `snapshot` with the sequence number that produced it.
    fn locate(&self, row_key: &[u8], snapshot: u64) -> io::Result<Option<(u64, Op)>> {
        for c in self.commits.iter().rev() {
            if c.seq > snapshot {
                continue;
            }
            if let Some(op) = c.lookup(row_key) {
                return Ok(Some((c.seq, op.clone())));
            }
        }
        for r in &self.runs {
            if let Some(op) = r.get_at(row_key, snapshot)? {
                let seq = r.seq_at(row_key, snapshot)?.unwrap_or(0);
                return Ok(Some((seq, op)));
            }
        }
        Ok(None)
    }

    /// The sequence number at or below `snapshot` where this relation was last
    /// emptied, if it ever was.
    ///
    /// TRUNCATE writes one small object saying "empty as of here" rather than
    /// a tombstone per row: a hundred million rows would otherwise be a
    /// hundred million tombstones in one commit. The marker is an ordinary
    /// versioned key, so it rolls back with its transaction, a snapshot from
    /// before it still sees the rows, and collection can drop everything under
    /// it.
    pub fn emptied_at(&self, marker_key: &[u8], snapshot: u64) -> io::Result<Option<u64>> {
        let mut best: Option<u64> = None;
        let mut consider = |seq: u64, op: &Op| {
            if seq <= snapshot && matches!(op, Op::Put(_)) {
                best = Some(best.map_or(seq, |b: u64| b.max(seq)));
            }
        };
        for c in &self.commits {
            for e in c.prefixed(marker_key) {
                if e.key == marker_key {
                    consider(c.seq, &e.op);
                }
            }
        }
        for r in &self.runs {
            for (vk, op) in r.scan_prefix(marker_key)? {
                let (Some(row), Some(seq)) = (key::row_of(&vk), key::seq_of(&vk)) else {
                    continue;
                };
                if row == marker_key {
                    consider(seq, &op);
                }
            }
        }
        Ok(best)
    }

    /// Every live key with `prefix`, oldest layer first so newer versions win;
    /// tombstones are applied and dropped. Materialises the whole prefix,
    /// which suits the sequential scans the table AM needs today; a streaming
    /// merge of run blocks is the version
    /// that would survive real data volumes.
    /// Every live key in `[lo, hi)`, newest version of each.
    ///
    /// Half-open with plain byte bounds: because the key encoding makes byte
    /// order value order, "id > 40" and "id <= 40" differ only in where the
    /// caller puts the bound. A stored key carries a version suffix, so it is
    /// never equal to a bound -- it sorts inside the range exactly when the
    /// row it belongs to does.
    pub fn scan_range_at(
        &self,
        lo: &[u8],
        hi: &[u8],
        snapshot: u64,
    ) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self.window(lo, hi, snapshot, usize::MAX)?.0)
    }

    /// One window of a range: at most `limit` rows, and where to carry on.
    ///
    /// A tombstone fills a place in the window and yields nothing, so a window
    /// can come back short without the range being finished. `resume` is what
    /// says which: `None` means there is nothing left. Without it, a window
    /// made entirely of deletes would send the caller back to the same place
    /// for ever.
    pub fn scan_window_at(
        &self,
        lo: &[u8],
        hi: &[u8],
        snapshot: u64,
        limit: usize,
    ) -> io::Result<(Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>)> {
        let (rows, seen, last) = self.window(lo, hi, snapshot, limit)?;
        let resume = if seen == limit {
            last.map(|mut k| {
                // The next key that can exist: keys hold no zero byte, so
                // nothing sorts between this and the one it came from.
                k.push(0);
                k
            })
        } else {
            None
        };
        Ok((rows, resume))
    }

    /// One window taken from the top of the range instead of the bottom.
    ///
    /// Rows come back in ascending order either way; only which end is read
    /// first differs. `resume` is the new upper bound.
    pub fn scan_window_back_at(
        &self,
        lo: &[u8],
        hi: &[u8],
        snapshot: u64,
        limit: usize,
    ) -> io::Result<(Vec<(Vec<u8>, Vec<u8>, u64)>, Option<Vec<u8>>)> {
        let (rows, seen, first) = self.window_back(lo, hi, snapshot, limit)?;
        let resume = if seen == limit { first } else { None };
        Ok((rows, resume))
    }

    fn window_back(
        &self,
        lo: &[u8],
        hi: &[u8],
        snapshot: u64,
        limit: usize,
    ) -> io::Result<(Vec<(Vec<u8>, Vec<u8>, u64)>, usize, Option<Vec<u8>>)> {
        self.readable_at(snapshot)?;
        let mut merged: BTreeMap<Vec<u8>, (u64, Op)> = BTreeMap::new();
        for r in self.runs.iter().rev() {
            for (vk, op) in r.scan_range_back(lo, hi, snapshot, limit)? {
                let (Some(row), Some(seq)) = (key::row_of(&vk), key::seq_of(&vk)) else {
                    continue;
                };
                if seq > snapshot {
                    continue;
                }
                match merged.get(row) {
                    Some((have, _)) if *have >= seq => {}
                    _ => {
                        merged.insert(row.to_vec(), (seq, op));
                    }
                }
            }
        }
        for c in &self.commits {
            if c.seq > snapshot {
                continue;
            }
            let r = c.ranged(lo, hi);
            for e in &r[r.len().saturating_sub(limit)..] {
                merged.insert(e.key.clone(), (c.seq, e.op.clone()));
            }
        }
        if snapshot == LATEST {
            for (k, op) in self.memtable.range(lo.to_vec()..hi.to_vec()).rev().take(limit) {
                merged.insert(k.clone(), (LATEST, op.clone()));
            }
        }
        let mut taken: Vec<(Vec<u8>, (u64, Op))> = merged.into_iter().collect();
        if taken.len() > limit {
            taken.drain(..taken.len() - limit);
        }
        let seen = taken.len();
        let first = taken.first().map(|(k, _)| k.clone());
        Ok((
            taken
                .into_iter()
                .filter_map(|(k, (seq, op))| match op {
                    Op::Put(v) => Some((k, v, seq)),
                    Op::Delete => None,
                })
                .collect(),
            seen,
            first,
        ))
    }

    /// A range window that also says which commit each row's newest version
    /// came from, so a caller can drop the ones a truncation covers.
    pub fn scan_window_stamped_at(
        &self,
        lo: &[u8],
        hi: &[u8],
        snapshot: u64,
        limit: usize,
    ) -> io::Result<(Vec<(Vec<u8>, Vec<u8>, u64)>, Option<Vec<u8>>)> {
        let (rows, seen, last) = self.window_stamped(lo, hi, snapshot, limit)?;
        let resume = if seen == limit {
            last.map(|mut k| {
                k.push(0);
                k
            })
        } else {
            None
        };
        Ok((rows, resume))
    }

    fn window_stamped(
        &self,
        lo: &[u8],
        hi: &[u8],
        snapshot: u64,
        limit: usize,
    ) -> io::Result<(Vec<(Vec<u8>, Vec<u8>, u64)>, usize, Option<Vec<u8>>)> {
        self.readable_at(snapshot)?;
        let mut merged: BTreeMap<Vec<u8>, (u64, Op)> = BTreeMap::new();
        for r in self.runs.iter().rev() {
            for (vk, op) in r.scan_range_limited(lo, hi, snapshot, limit)? {
                let (Some(row), Some(seq)) = (key::row_of(&vk), key::seq_of(&vk)) else {
                    continue;
                };
                if seq > snapshot {
                    continue;
                }
                match merged.get(row) {
                    Some((have, _)) if *have >= seq => {}
                    _ => {
                        merged.insert(row.to_vec(), (seq, op));
                    }
                }
            }
        }
        for c in &self.commits {
            if c.seq > snapshot {
                continue;
            }
            for e in c.ranged(lo, hi).iter().take(limit) {
                merged.insert(e.key.clone(), (c.seq, e.op.clone()));
            }
        }
        if snapshot == LATEST {
            for (k, op) in self.memtable.range(lo.to_vec()..hi.to_vec()).take(limit) {
                merged.insert(k.clone(), (LATEST, op.clone()));
            }
        }
        let taken: Vec<(Vec<u8>, (u64, Op))> = merged.into_iter().take(limit).collect();
        let seen = taken.len();
        let last = taken.last().map(|(k, _)| k.clone());
        Ok((
            taken
                .into_iter()
                .filter_map(|(k, (seq, op))| match op {
                    Op::Put(v) => Some((k, v, seq)),
                    Op::Delete => None,
                })
                .collect(),
            seen,
            last,
        ))
    }

    fn window(
        &self,
        lo: &[u8],
        hi: &[u8],
        snapshot: u64,
        limit: usize,
    ) -> io::Result<(Vec<(Vec<u8>, Vec<u8>)>, usize, Option<Vec<u8>>)> {
        let (rows, seen, last) = self.window_stamped(lo, hi, snapshot, limit)?;
        Ok((rows.into_iter().map(|(k, v, _)| (k, v)).collect(), seen, last))
    }

    pub fn scan_prefix_at(
        &self,
        prefix: &[u8],
        snapshot: u64,
    ) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Every read path, not just the single-row one: a scan that skipped
        // this returned whatever survived collection and called it an answer.
        self.readable_at(snapshot)?;
        let mut merged: BTreeMap<Vec<u8>, (u64, Op)> = BTreeMap::new();

        for r in self.runs.iter().rev() {
            // Seek, not scan: a run holds every table, so reading it whole
            // makes one table's cost depend on all the others.
            for (vk, op) in r.scan_prefix(prefix)? {
                let (Some(row), Some(seq)) = (key::row_of(&vk), key::seq_of(&vk)) else {
                    continue;
                };
                if seq > snapshot {
                    continue;
                }
                match merged.get(row) {
                    Some((have, _)) if *have >= seq => {}
                    _ => {
                        merged.insert(row.to_vec(), (seq, op));
                    }
                }
            }
        }
        for c in &self.commits {
            if c.seq > snapshot {
                continue;
            }
            // Binary search: after a bulk load one commit holds every row it
            // wrote, and this runs on every read until compaction folds it.
            for e in c.prefixed(prefix) {
                merged.insert(e.key.clone(), (c.seq, e.op.clone()));
            }
        }
        if snapshot == LATEST {
            for (k, op) in &self.memtable {
                if k.starts_with(prefix) {
                    merged.insert(k.clone(), (LATEST, op.clone()));
                }
            }
        }
        Ok(merged
            .into_iter()
            .filter_map(|(k, (_, op))| match op {
                Op::Put(v) => Some((k, v)),
                Op::Delete => None,
            })
            .collect())
    }

    /// Scans the present.
    pub fn scan_prefix(&self, prefix: &[u8]) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_prefix_at(prefix, LATEST)
    }
}


/// How many runs may stand before a fold merges them into one.
pub const MAX_RUNS: usize = 4;

/// What one fold does. See [`Db::fold_plan`].
pub struct FoldPlan {
    pub new_id: u64,
    pub merge: bool,
    commits: Vec<Arc<Commit>>,
    runs: Vec<Arc<Run<ObjectRange>>>,
}

/// A run built and ready to write.
pub struct Folded {
    pub key: String,
    pub bytes: Vec<u8>,
    pub kept: usize,
}

/// What a fold left for deletion, decided under the lock.
pub struct SweepPlan {
    run_key: String,
    expected: usize,
    retired: Vec<(Arc<Run<ObjectRange>>, String)>,
    folded: Vec<u64>,
    done: Vec<(u64, u64)>,
    markers: Vec<u64>,
    publish_horizon: Option<u64>,
    collected_through: u64,
}

/// What the sweep managed to delete, for `Db::sweep_done`.
#[derive(Default)]
pub struct SweepResult {
    objects: Vec<u64>,
    markers: Vec<u64>,
}

/// Builds the run a plan describes. Reads the runs it merges over the
/// network, so it is meant to run with no lock held; nothing it reads can
/// change under it, since runs are immutable and a commit is only ever
/// appended or folded.
pub fn build_fold(
    plan: &FoldPlan,
    horizon: u64,
    index_tables: &BTreeMap<u32, u32>,
) -> io::Result<Folded> {
    // Keyed by versioned key, so folding adds a version rather than
    // replacing one. Oldest run first, so a newer run's entries win -- they
    // never share a versioned key anyway, but the order costs nothing.
    let mut merged: BTreeMap<Vec<u8>, Op> = BTreeMap::new();
    for r in plan.runs.iter().rev() {
        for (k, op) in r.scan()? {
            merged.insert(k, op); // already versioned
        }
    }
    for c in &plan.commits {
        for e in &c.entries {
            merged.insert(key::versioned(&e.key, c.seq), e.op.clone());
        }
    }
    let mut entries: Vec<(Vec<u8>, Op)> = merged.into_iter().collect();
    if horizon > 0 {
        // Within a delta this keeps the newest version at or below the
        // horizon that the delta holds; an older run may hold older ones
        // still, and the merge drops those. Reads below the horizon are
        // refused either way.
        entries = retain_above(entries, horizon, !plan.merge);
    }
    if plan.merge {
        // Only with every run in view: a row's image may sit in an older run
        // than its entries, and a delta alone would call it dead.
        entries = drop_dead_entries(entries, index_tables);
    }
    let kept = entries.len();
    let key = if plan.merge { run::key_for(plan.new_id) } else { run::delta_key_for(plan.new_id) };
    Ok(Folded { key, bytes: run::build(&entries), kept })
}

/// Writes the run. Its number is the newest commit it holds, so a second
/// writer folding the same commits would collide here rather than overwrite.
pub fn put_fold(store: &Arc<dyn Store>, folded: &Folded) -> io::Result<()> {
    if store.put_if_absent(&folded.key, &folded.bytes)? == PutOutcome::AlreadyExists {
        return Err(io::Error::other("run already exists"));
    }
    Ok(())
}

/// Deletes what the new run replaced, but only after reading that run back
/// and finding everything in it. The one place that destroys data, so the
/// gate is a read: a PUT that returned 200 and a GET that returns the bytes
/// are different claims. A failure here is not the caller's problem --
/// better a bucket too big than one too small. Meant to run with no lock
/// held; `Db::sweep_done` records the outcome.
pub fn execute_sweep(store: &Arc<dyn Store>, plan: SweepPlan) -> SweepResult {
    let mut result = SweepResult::default();
    if let Some(h) = plan.publish_horizon {
        // Published after the surviving run is durable, so a crash between
        // leaves history intact and merely unrecorded.
        if let Err(e) = store.put_if_absent(&horizon_key(h), b"") {
            eprintln!("objkv: collected through {h}, but the marker did not write ({e})");
        }
    }
    // Not `?`: a failed read-back keeps the old objects, it does not fail a
    // compaction that already succeeded.
    let readback = (|| -> io::Result<usize> {
        let size = store.get(&plan.run_key)?.map_or(0, |b| b.len() as u64);
        let r = Run::open(ObjectRange { store: Arc::clone(store), key: plan.run_key.clone(), size })?;
        Ok(r.scan()?.len())
    })();
    match readback {
        Ok(n) if n == plan.expected => {}
        Ok(n) => {
            eprintln!(
                "objkv: not collecting: {} read back with {n} entries, not {}",
                plan.run_key, plan.expected
            );
            return result;
        }
        Err(e) => {
            eprintln!("objkv: not collecting: {} could not be read back ({e})", plan.run_key);
            return result;
        }
    }

    let mut doomed: Vec<String> = plan.retired.iter().map(|(_, k)| k.clone()).collect();
    for &seq in &plan.folded {
        doomed.push(watermark_key(seq));
    }
    for (first, last) in plan.done {
        // Object first: the other order leaves the next open finding an
        // unexplained object and discarding it again.
        if store.delete(&commit::key_for(first)).is_err() {
            continue;
        }
        result.objects.push(first);
        for &seq in plan.markers.iter().filter(|&&s| s >= first && s <= last) {
            if store.delete(&discard_key(seq)).is_ok() {
                result.markers.push(seq);
            }
        }
    }
    match store.list("horizon/") {
        Ok(infos) => {
            for info in infos {
                if id_from(&info.key) < plan.collected_through {
                    doomed.push(info.key);
                }
            }
        }
        Err(e) => eprintln!("objkv: could not list horizon markers ({e})"),
    }
    for k in doomed {
        if let Err(e) = store.delete(&k) {
            eprintln!("objkv: could not delete {k} ({e}); it is garbage, not data");
        }
    }
    drop(plan.retired);
    result
}


fn resolve(op: &Op) -> Option<Vec<u8>> {
    match op {
        Op::Put(v) => Some(v.clone()),
        Op::Delete => None,
    }
}

/// `commit/00000000000000ff` -> 255.
fn id_from(key: &str) -> u64 {
    key.rsplit('/')
        .next()
        .map(|h| h.strip_suffix(run::DELTA_SUFFIX).unwrap_or(h))
        .and_then(|h| u64::from_str_radix(h, 16).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemStore;

    fn db() -> (Arc<MemStore>, Db) {
        let s = Arc::new(MemStore::new());
        let d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        (s, d)
    }

    #[test]
    fn reads_see_uncommitted_writes() {
        let (_s, mut d) = db();
        d.put(b"a", b"1");
        assert_eq!(d.get(b"a").unwrap(), Some(b"1".to_vec()));
    }

    #[test]
    fn commit_then_read_through_the_commit_layer() {
        let (_s, mut d) = db();
        d.put(b"a", b"1");
        assert_eq!(d.commit().unwrap(), Some(1));
        assert!(d.memtable.is_empty());
        assert_eq!(d.get(b"a").unwrap(), Some(b"1".to_vec()));
    }

    #[test]
    fn newest_layer_wins() {
        let (_s, mut d) = db();
        d.put(b"a", b"old");
        d.commit().unwrap();
        d.put(b"a", b"new");
        d.commit().unwrap();
        assert_eq!(d.get(b"a").unwrap(), Some(b"new".to_vec()));
        d.delete(b"a");
        d.commit().unwrap();
        assert_eq!(d.get(b"a").unwrap(), None);
    }

    #[test]
    fn state_survives_reopen() {
        let s = Arc::new(MemStore::new());
        {
            let mut d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
            for i in 0..50 {
                d.put(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes());
                d.commit().unwrap();
            }
        }
        let d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        assert_eq!(d.commit_backlog(), 50);
        assert_eq!(d.get(b"k0007").unwrap(), Some(b"v7".to_vec()));
        assert_eq!(d.get(b"k9999").unwrap(), None);
    }

    #[test]
    fn compaction_folds_commits_into_a_run() {
        let (_s, mut d) = db();
        for i in 0..200 {
            d.put(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes());
            d.commit().unwrap();
        }
        assert!(d.needs_compaction());
        d.compact().unwrap();
        assert_eq!(d.commit_backlog(), 0);
        assert_eq!(d.run_count(), 1);
        for i in 0..200 {
            let want = format!("v{i}").into_bytes();
            assert_eq!(d.get(format!("k{i:04}").as_bytes()).unwrap(), Some(want));
        }
    }

    #[test]
    fn compaction_survives_reopen_and_drops_the_walk() {
        let s = Arc::new(MemStore::new());
        {
            let mut d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
            for i in 0..150 {
                d.put(format!("k{i:04}").as_bytes(), b"v");
                d.commit().unwrap();
            }
            d.compact().unwrap();
        }
        let d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        assert_eq!(d.commit_backlog(), 0, "commits below the base run are skipped");
        assert_eq!(d.get(b"k0100").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn tombstones_survive_compaction() {
        let (_s, mut d) = db();
        d.put(b"gone", b"x");
        d.commit().unwrap();
        d.delete(b"gone");
        d.commit().unwrap();
        d.compact().unwrap();
        assert_eq!(d.get(b"gone").unwrap(), None);
    }

    #[test]
    fn scan_prefix_merges_layers_and_drops_tombstones() {
        let (_s, mut d) = db();
        for i in 0..10 {
            d.put(format!("t1/{i:03}").as_bytes(), b"old");
            d.put(format!("t2/{i:03}").as_bytes(), b"other");
        }
        d.commit().unwrap();
        d.compact().unwrap();
        d.put(b"t1/003", b"new");
        d.delete(b"t1/005");
        d.commit().unwrap();
        d.put(b"t1/007", b"pending"); // uncommitted, must still be visible

        let got = d.scan_prefix(b"t1/").unwrap();
        assert_eq!(got.len(), 9, "one key was deleted");
        assert!(got.iter().all(|(k, _)| k.starts_with(b"t1/")), "prefix respected");
        assert!(got.windows(2).all(|w| w[0].0 < w[1].0), "sorted");
        let find = |k: &str| got.iter().find(|(a, _)| a == k.as_bytes()).map(|(_, v)| v.clone());
        assert_eq!(find("t1/003"), Some(b"new".to_vec()));
        assert_eq!(find("t1/005"), None);
        assert_eq!(find("t1/007"), Some(b"pending".to_vec()));
        assert_eq!(find("t1/000"), Some(b"old".to_vec()));
    }

    #[test]
    fn a_sequence_collision_retries_at_the_next_free_number() {
        let s = Arc::new(MemStore::new());
        let mut a = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        let mut b = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        a.put(b"ka", b"from-a");
        b.put(b"kb", b"from-b");
        assert_eq!(a.commit().unwrap(), Some(1));
        // b lost the put-if-absent and must land on 2: a name collision.
        assert_eq!(b.commit().unwrap(), Some(2));

        let d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        assert_eq!(d.get(b"ka").unwrap(), Some(b"from-a".to_vec()));
        assert_eq!(d.get(b"kb").unwrap(), Some(b"from-b".to_vec()));
    }

    #[test]
    fn concurrent_writers_to_one_key_lose_updates() {
        // A real gap, not correct behaviour: both commit and the later wins,
        // where Postgres would have blocked the second.
        let s = Arc::new(MemStore::new());
        let mut a = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        let mut b = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        a.put(b"k", b"from-a");
        b.put(b"k", b"from-b");
        a.commit().unwrap();
        b.commit().unwrap();
        assert_eq!(
            Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap().get(b"k").unwrap(),
            Some(b"from-b".to_vec()),
            "known gap: no write-write conflict detection"
        );
    }

    #[test]
    fn a_batch_commit_writes_exactly_one_object() {
        let s = Arc::new(MemStore::new());
        let mut d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        let mut writes = BTreeMap::new();
        for i in 0..1_000 {
            writes.insert(format!("k{i:04}").into_bytes(), Op::Put(b"v".to_vec()));
        }
        assert_eq!(d.commit_batch(writes, 0).unwrap(), Some(1));

        let objects = s.list("commit/").unwrap();
        assert_eq!(objects.len(), 1, "1000 rows must cost one PUT, not 1000");

        // commit_batch does not self-confirm, so standing in for the AM:
        // confirm, then let a later commit carry the watermark.
        d.mark_confirmed(1);
        d.put(b"later", b"x");
        d.commit().unwrap();

        let reopened = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        assert_eq!(reopened.get(b"k0000").unwrap(), Some(b"v".to_vec()));
        assert_eq!(reopened.get(b"k0999").unwrap(), Some(b"v".to_vec()));
    }

    /// Wraps a store and makes one key unreadable, so the collector's
    /// read-back gate can be tested for what it is meant to prevent.
    struct Unreadable {
        inner: Arc<MemStore>,
        key: std::sync::Mutex<Option<String>>,
    }

    impl Store for Unreadable {
        fn put_if_absent(&self, key: &str, body: &[u8]) -> io::Result<PutOutcome> {
            self.inner.put_if_absent(key, body)
        }
        fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
            if self.key.lock().unwrap().as_deref() == Some(key) {
                return Err(io::Error::other("injected: object unreadable"));
            }
            self.inner.get(key)
        }
        // Only whole-object reads fail, which is what the collector's gate does.
        fn get_range(&self, key: &str, off: u64, len: u64) -> io::Result<Option<Vec<u8>>> {
            self.inner.get_range(key, off, len)
        }
        fn list(&self, prefix: &str) -> io::Result<Vec<crate::s3::ObjectInfo>> {
            self.inner.list(prefix)
        }
        fn delete(&self, key: &str) -> io::Result<()> {
            self.inner.delete(key)
        }
    }

    /// The two shapes an index entry comes in, built the way the AM builds
    /// them: value-keyed for a unique index, rowid-keyed otherwise.
    fn entry(db: u32, index: u32, value: &str, rowid: u64, unique: bool) -> Vec<u8> {
        index_key::entry_key(db, index, &[index_key::Col::Text(value.as_bytes())], rowid, unique)
            .unwrap()
    }

    fn row(db: u32, relid: u32, rowid: u64) -> Vec<u8> {
        format!("{db:08x}/{relid:08x}/{rowid:016x}").into_bytes()
    }

    #[test]
    fn entries_go_when_their_row_does() {
        // An UPDATE is a delete plus an insert at a fresh row id, so every
        // update strands the entry that pointed at the old one.
        const DB: u32 = 5;
        const TBL: u32 = 100;
        const IDX: u32 = 200;
        let s = Arc::new(MemStore::new());
        let mut d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();

        // Row 1 with its entry, then "updated": row 1 tombstoned, row 2 written.
        d.put(&row(DB, TBL, 1), b"alice-v1");
        d.put(&entry(DB, IDX, "alice", 1, false), b"");
        d.commit().unwrap();
        d.delete(&row(DB, TBL, 1));
        d.put(&row(DB, TBL, 2), b"alice-v2");
        d.put(&entry(DB, IDX, "alice", 2, false), b"");
        d.commit().unwrap();
        d.put(b"unrelated", b"x");
        d.commit().unwrap();

        let tables = BTreeMap::from([(IDX, TBL)]);
        // Past the tombstone, so row 1 goes and its entry becomes collectable.
        d.compact_retaining(3, &tables).unwrap();

        let keys: Vec<Vec<u8>> = d.runs[0]
            .scan()
            .unwrap()
            .into_iter()
            .filter_map(|(k, _)| key::row_of(&k).map(<[u8]>::to_vec))
            .collect();
        assert!(
            !keys.contains(&entry(DB, IDX, "alice", 1, false)),
            "the entry for the vanished row must go"
        );
        assert!(
            keys.contains(&entry(DB, IDX, "alice", 2, false)),
            "the entry for the live row must stay"
        );
        assert_eq!(d.get(&row(DB, TBL, 2)).unwrap(), Some(b"alice-v2".to_vec()));
    }

    #[test]
    fn a_unique_entry_is_judged_by_the_row_id_in_its_payload() {
        // A unique index keys on the value alone with the row id in the
        // payload, so the collector must read it from there or delete the lot.
        const DB: u32 = 5;
        const TBL: u32 = 100;
        const IDX: u32 = 200;
        let s = Arc::new(MemStore::new());
        let mut d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();

        d.put(&row(DB, TBL, 7), b"bob");
        d.put(&entry(DB, IDX, "bob", 7, true), format!("{:016x}", 7u64).as_bytes());
        d.commit().unwrap();
        d.put(b"unrelated", b"x");
        d.commit().unwrap();

        let tables = BTreeMap::from([(IDX, TBL)]);
        d.compact_retaining(2, &tables).unwrap();

        assert!(
            d.get(&entry(DB, IDX, "bob", 7, true)).unwrap().is_some(),
            "a unique entry whose row is alive must survive"
        );
    }

    #[test]
    fn an_index_with_no_known_table_is_left_alone() {
        // Guessing which relation an entry belongs to would delete live
        // entries, so an index the caller could not name is not touched.
        const DB: u32 = 5;
        const IDX: u32 = 200;
        let s = Arc::new(MemStore::new());
        let mut d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        d.put(&entry(DB, IDX, "orphan", 1, false), b"");
        d.commit().unwrap();
        d.put(b"unrelated", b"x");
        d.commit().unwrap();

        d.compact_retaining(2, &BTreeMap::new()).unwrap();
        assert!(d.get(&entry(DB, IDX, "orphan", 1, false)).unwrap().is_some());
    }

    #[test]
    fn a_run_never_claims_a_commit_it_did_not_fold() {
        // Compaction runs during a transaction's own pre-commit, so that
        // transaction's object is in the bucket and not foldable yet.
        // Numbering the run above it drops it on the next restart.
        let s = Arc::new(MemStore::new());
        let mut d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        for i in 1..=3 {
            d.put(b"settled", format!("v{i}").as_bytes());
            d.commit().unwrap();
        }

        // In flight: durable, fate unrecorded -- where the AM triggers this.
        let mut w = BTreeMap::new();
        w.insert(b"inflight".to_vec(), Op::Put(b"live".to_vec()));
        let staged = d.commit_batch(w, 77).unwrap().unwrap();

        let run_id = d.compact().unwrap();
        assert!(
            run_id < staged,
            "the run must not number itself above an unfolded commit ({run_id} vs {staged})"
        );

        d.mark_confirmed(staged);
        d.flush_watermark().unwrap();

        let d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        assert_eq!(
            d.get(b"inflight").unwrap(),
            Some(b"live".to_vec()),
            "the in-flight transaction survived the restart"
        );
        assert_eq!(d.get(b"settled").unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn compaction_deletes_what_it_replaced() {
        let s = Arc::new(MemStore::new());
        let mut d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        for i in 1..=10 {
            d.put(b"row", format!("v{i}").as_bytes());
            d.commit().unwrap();
        }
        assert_eq!(s.list("commit/").unwrap().len(), 10);

        d.compact().unwrap();
        assert!(s.list("commit/").unwrap().is_empty(), "folded commits are gone");
        assert_eq!(s.list("run/").unwrap().len(), 1, "one run, not a pile");
        assert_eq!(d.get(b"row").unwrap(), Some(b"v10".to_vec()));

        // The next folds are deltas beside it, until there are MAX_RUNS runs;
        // the fold after that merges them into one and the rest are deleted.
        for i in 11..11 + MAX_RUNS {
            d.put(b"row", format!("v{i}").as_bytes());
            d.commit().unwrap();
            d.compact().unwrap();
            let runs = s.list("run/").unwrap().len();
            let expect = if i == 10 + MAX_RUNS { 1 } else { i - 9 };
            assert_eq!(runs, expect, "after fold {i}");
            assert_eq!(
                Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap().get(b"row").unwrap(),
                Some(format!("v{i}").into_bytes()),
                "readable from the bucket after fold {i}"
            );
        }
        assert_eq!(d.run_count(), 1);
    }

    #[test]
    fn nothing_is_deleted_when_the_replacement_cannot_be_read_back() {
        // A PUT that returned success and an object that reads back are
        // different claims. A bucket too big is recoverable, too small is not.
        let mem = Arc::new(MemStore::new());
        let s = Arc::new(Unreadable {
            inner: Arc::clone(&mem),
            key: std::sync::Mutex::new(None),
        });
        let mut d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        for i in 1..=5 {
            d.put(b"row", format!("v{i}").as_bytes());
            d.commit().unwrap();
        }
        // The run compaction is about to write: it takes the number of the
        // newest commit it folds, which is the fifth.
        *s.key.lock().unwrap() = Some(run::key_for(5));

        d.compact().unwrap();
        assert_eq!(
            mem.list("commit/").unwrap().len(),
            5,
            "the commits must survive a replacement that could not be verified"
        );

        // With the injection lifted, the data is all still there.
        *s.key.lock().unwrap() = None;
        let d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        assert_eq!(d.get(b"row").unwrap(), Some(b"v5".to_vec()));
    }

    /// Writes `n` successive values to one row, each its own commit, and
    /// returns the store.
    fn history(n: u64) -> Arc<MemStore> {
        let s = Arc::new(MemStore::new());
        let mut d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        for i in 1..=n {
            d.put(b"row", format!("v{i}").as_bytes());
            d.commit().unwrap();
        }
        s
    }

    fn versions_of(d: &Db, row: &[u8]) -> Vec<u64> {
        let mut seqs: Vec<u64> = d.runs[0]
            .scan()
            .unwrap()
            .into_iter()
            .filter(|(k, _)| key::row_of(k) == Some(row))
            .filter_map(|(k, _)| key::seq_of(&k))
            .collect();
        seqs.sort_unstable();
        seqs
    }

    #[test]
    fn collection_keeps_one_version_from_below_the_horizon() {
        // Ten writes, collect at or below 6. The four above stay; from below
        // only the newest survives, which is what a read at 6 resolves to.
        let s = history(10);
        let mut d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        d.compact_retaining(6, &BTreeMap::new()).unwrap();
        assert_eq!(versions_of(&d, b"row"), vec![6, 7, 8, 9, 10]);
        assert_eq!(d.get(b"row").unwrap(), Some(b"v10".to_vec()));
        assert_eq!(d.get_at(b"row", 6).unwrap(), Some(b"v6".to_vec()));
        assert_eq!(d.get_at(b"row", 8).unwrap(), Some(b"v8".to_vec()));
    }

    #[test]
    fn a_row_deleted_below_the_horizon_goes_entirely() {
        // The tombstone is the newest thing below the horizon, so nothing
        // under it is reachable: a later read finds no version, which is the
        // same answer the tombstone was there to give.
        let s = Arc::new(MemStore::new());
        let mut d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        d.put(b"gone", b"here");
        d.commit().unwrap();
        d.delete(b"gone");
        d.commit().unwrap();
        d.put(b"kept", b"x");
        d.commit().unwrap();

        d.compact_retaining(3, &BTreeMap::new()).unwrap();
        assert!(versions_of(&d, b"gone").is_empty(), "row and tombstone both go");
        assert_eq!(d.get(b"gone").unwrap(), None);
        assert_eq!(d.get(b"kept").unwrap(), Some(b"x".to_vec()));
    }

    #[test]
    fn reads_below_the_horizon_are_refused_rather_than_guessed() {
        // The dangerous version of this is answering from whatever survived,
        // which would be a wrong answer with no error attached.
        let s = history(10);
        let mut d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        d.compact_retaining(6, &BTreeMap::new()).unwrap();
        let err = d.get_at(b"row", 3).unwrap_err().to_string();
        assert!(err.contains("has been collected"), "got: {err}");
        assert!(err.contains("oldest readable point is 6"), "got: {err}");
        assert!(d.get_at(b"row", 6).is_ok(), "the horizon itself is readable");
        assert!(d.get(b"row").is_ok(), "and so is the present");

        // Scans too. This is the one that matters: a scan answering from
        // whatever survived returns fewer rows and reports nothing wrong.
        assert!(
            d.scan_prefix_at(b"", 3).is_err(),
            "a scan below the horizon must refuse, not return a short answer"
        );
        assert!(d.scan_prefix_at(b"", 6).is_ok());
    }

    #[test]
    fn the_horizon_survives_a_reopen() {
        let s = history(10);
        {
            let mut d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
            d.compact_retaining(6, &BTreeMap::new()).unwrap();
            d.flush_watermark().unwrap();
        }
        let d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        assert_eq!(d.collected_through(), 6);
        assert!(d.get_at(b"row", 3).is_err(), "a fresh process refuses it too");
        assert_eq!(d.get(b"row").unwrap(), Some(b"v10".to_vec()));
    }

    #[test]
    fn a_horizon_of_zero_collects_nothing() {
        let s = history(10);
        let mut d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        d.compact_retaining(0, &BTreeMap::new()).unwrap();
        assert_eq!(versions_of(&d, b"row").len(), 10);
        assert_eq!(d.collected_through(), 0);
        assert!(d.get_at(b"row", 1).is_ok(), "all of history is still readable");
    }

    #[test]
    fn an_unvouched_commit_is_discarded_and_marked() {
        // A commit written through the Postgres path and never confirmed is
        // not data. It goes, and the bucket records that it went.
        let s = Arc::new(MemStore::new());
        {
            let mut d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
            d.put(b"safe", b"1");
            d.commit().unwrap(); // self-confirmed
            let mut torn = BTreeMap::new();
            torn.insert(b"torn".to_vec(), Op::Put(b"2".to_vec()));
            d.commit_batch(torn, 4242).unwrap(); // never confirmed
        }

        let d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        assert_eq!(d.get(b"safe").unwrap(), Some(b"1".to_vec()));
        assert_eq!(d.get(b"torn").unwrap(), None, "unvouched rows must be invisible");
        assert_eq!(d.discarded.get(&2), Some(&Discard::Unconfirmed));
        assert_eq!(
            s.get(&discard_key(2)).unwrap().unwrap(),
            b"discard:unconfirmed".to_vec(),
            "the verdict is written to the bucket, not re-derived every boot"
        );
        assert_eq!(
            s.list("commit/").unwrap().len(),
            2,
            "discarding is a decision, not a delete -- the collector frees it"
        );

        // The second open reads the marker rather than reaching the verdict
        // again, and says nothing.
        let again = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        assert_eq!(again.get(b"torn").unwrap(), None);
        assert_eq!(again.discarded.get(&2), Some(&Discard::Unconfirmed));
    }

    fn one(k: &[u8], v: &[u8]) -> BTreeMap<Vec<u8>, Op> {
        let mut w = BTreeMap::new();
        w.insert(k.to_vec(), Op::Put(v.to_vec()));
        w
    }

    /// Does the PUT the AM's writer would do, and reports back.
    fn fly(d: &mut Db, s: &Arc<dyn Store>) -> Option<Flight> {
        let f = d.take_flight()?;
        match s.put_if_absent(&f.key, &f.bytes).unwrap() {
            PutOutcome::Written => d.flight_written(f.first),
            PutOutcome::AlreadyExists => d.flight_lost(&f).unwrap(),
        }
        Some(f)
    }

    #[test]
    fn queued_commits_share_one_object_and_each_keeps_its_number() {
        let s = Arc::new(MemStore::new());
        let st = Arc::clone(&s) as Arc<dyn Store>;
        let mut d = Db::open(Arc::clone(&st)).unwrap();
        let snap = d.current_seq();
        let mut tickets = Vec::new();
        for i in 0..3u8 {
            let (t, seq) = d
                .stage_commit(one(&[b'r', i], b"v"), i as u32, snap, true)
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(seq, 1 + i as u64);
            tickets.push((t, seq));
        }
        assert!(d.take_outcome(tickets[0].0).is_none(), "nothing is durable yet");
        assert_eq!(s.list("commit/").unwrap().len(), 0);

        let f = fly(&mut d, &st).unwrap();
        assert_eq!(s.list("commit/").unwrap().len(), 1, "one PUT for three commits");
        assert_eq!(f.key, commit::key_for(1));
        for (t, seq) in &tickets {
            assert!(matches!(d.take_outcome(*t), Some(Outcome::Durable(x)) if x == *seq));
            d.mark_confirmed(*seq);
        }
        assert!(d.take_flight().is_none(), "nothing left to write");
        d.flush_watermark().unwrap();

        let again = Db::open_with(Arc::clone(&st)).unwrap();
        for i in 0..3u8 {
            assert_eq!(again.get(&[b'r', i]).unwrap(), Some(b"v".to_vec()));
        }
        assert_eq!(again.next_seq, 4, "the batch consumed three numbers");
    }

    #[test]
    fn two_queued_commits_on_one_row_still_conflict() {
        let (_, mut d) = db();
        let snap = d.current_seq();
        d.stage_commit(one(b"hot", b"a"), 1, snap, true).unwrap().unwrap();
        let r = d.stage_commit(one(b"hot", b"b"), 2, snap, true).unwrap();
        assert!(r.is_err(), "the earlier member of the group wins");
        assert!(d.stage_commit(one(b"cold", b"c"), 3, snap, true).unwrap().is_ok());
    }

    #[test]
    fn a_commit_discarded_before_its_write_never_lands_and_leaves_no_marker() {
        let s = Arc::new(MemStore::new());
        let st = Arc::clone(&s) as Arc<dyn Store>;
        let mut d = Db::open(Arc::clone(&st)).unwrap();
        let (_, gone) = d.stage_commit(one(b"a", b"1"), 1, 0, false).unwrap().unwrap().unwrap();
        let (t, kept) = d.stage_commit(one(b"b", b"2"), 2, 0, true).unwrap().unwrap().unwrap();
        d.discard_staged(gone, Discard::Aborted);
        fly(&mut d, &st).unwrap();
        assert!(matches!(d.take_outcome(t), Some(Outcome::Durable(x)) if x == kept));
        assert!(s.list("resolve/").unwrap().is_empty(), "nothing to explain");
        d.mark_confirmed(kept);
        d.flush_watermark().unwrap();
        let again = Db::open_with(st).unwrap();
        assert_eq!(again.get(b"a").unwrap(), None);
        assert_eq!(again.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn a_batch_straddling_the_run_boundary_is_kept_whole() {
        let s = Arc::new(MemStore::new());
        let st = Arc::clone(&s) as Arc<dyn Store>;
        let mut d = Db::open(Arc::clone(&st)).unwrap();
        let (_, first) = d.stage_commit(one(b"a", b"1"), 1, 0, true).unwrap().unwrap().unwrap();
        let (_, second) = d.stage_commit(one(b"b", b"2"), 2, 0, true).unwrap().unwrap().unwrap();
        let f = fly(&mut d, &st).unwrap();
        d.mark_confirmed(first);
        // The second is durable but its transaction has not committed, so the
        // fold stops below it -- and the object holding both must survive.
        d.compact().unwrap();
        assert_eq!(d.base_run_id, first);
        assert!(s.get(&f.key).unwrap().is_some(), "the batch still holds a live commit");

        d.mark_confirmed(second);
        d.flush_watermark().unwrap();
        let again = Db::open_with(Arc::clone(&st)).unwrap();
        assert_eq!(again.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(again.get(b"b").unwrap(), Some(b"2".to_vec()));

        d.compact().unwrap();
        assert!(s.get(&f.key).unwrap().is_none(), "folded whole, so collected");
    }

    #[test]
    fn a_lost_race_renumbers_members_nobody_has_been_told_about() {
        let s = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let mut a = Db::open(Arc::clone(&s)).unwrap();
        let mut b = Db::open_with(Arc::clone(&s)).unwrap();
        let snap = a.current_seq();
        let (t1, s1) = a.stage_commit(one(b"x", b"1"), 1, snap, true).unwrap().unwrap().unwrap();
        let (t2, _) = a.stage_commit(one(b"y", b"2"), 2, snap, true).unwrap().unwrap().unwrap();
        // b takes a's first number, and touches y on the way.
        let theirs = b.commit_batch(one(b"y", b"theirs"), 9).unwrap().unwrap();
        assert_eq!(theirs, s1);
        b.mark_confirmed(theirs);
        b.flush_watermark().unwrap();

        fly(&mut a, &s).unwrap(); // loses, renumbers, does not write
        assert!(a.take_outcome(t1).is_none(), "x is queued again, not decided");
        assert!(matches!(a.take_outcome(t2), Some(Outcome::Refused(_))), "y was changed under it");
        fly(&mut a, &s).unwrap();
        match a.take_outcome(t1) {
            Some(Outcome::Durable(seq)) => {
                assert!(seq > theirs);
                a.mark_confirmed(seq);
            }
            other => panic!("x should have landed: {other:?}"),
        }
        a.flush_watermark().unwrap();
        let again = Db::open_with(s).unwrap();
        assert_eq!(again.get(b"x").unwrap(), Some(b"1".to_vec()));
        assert_eq!(again.get(b"y").unwrap(), Some(b"theirs".to_vec()));
    }

    #[test]
    fn a_lost_race_over_an_acknowledged_commit_fences_the_process() {
        let s = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let mut a = Db::open(Arc::clone(&s)).unwrap();
        let mut b = Db::open_with(Arc::clone(&s)).unwrap();
        let (t, seq) = a.stage_commit(one(b"x", b"1"), 1, 0, false).unwrap().unwrap().unwrap();
        a.mark_confirmed(seq); // the client was told
        b.commit_batch(one(b"z", b"foreign"), 9).unwrap();
        fly(&mut a, &s).unwrap();
        assert!(matches!(a.take_outcome(t), Some(Outcome::Fenced(_))));
        assert!(a.is_fenced());
        assert!(a.stage_commit(one(b"q", b"1"), 3, 0, true).is_err(), "nothing more is accepted");
        assert!(a.take_flight().is_none());
    }

    #[test]
    fn our_own_object_written_by_a_lost_response_counts_as_written() {
        let s = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let (t, seq) = d.stage_commit(one(b"x", b"1"), 1, 0, true).unwrap().unwrap().unwrap();
        let f = d.take_flight().unwrap();
        // The first attempt landed but its response did not come back.
        s.put_if_absent(&f.key, &f.bytes).unwrap();
        assert_eq!(s.put_if_absent(&f.key, &f.bytes).unwrap(), PutOutcome::AlreadyExists);
        d.flight_lost(&f).unwrap();
        assert!(matches!(d.take_outcome(t), Some(Outcome::Durable(x)) if x == seq));
        assert!(!d.is_fenced());
    }

    #[test]
    fn compaction_and_the_watermark_stop_below_an_unwritten_commit() {
        let s = Arc::new(MemStore::new());
        let st = Arc::clone(&s) as Arc<dyn Store>;
        let mut d = Db::open(Arc::clone(&st)).unwrap();
        d.put(b"old", b"v");
        d.commit().unwrap();
        let (_, seq) = d.stage_commit(one(b"new", b"v"), 1, 0, false).unwrap().unwrap().unwrap();
        d.mark_confirmed(seq); // acknowledged, visible, not yet in the bucket
        assert_eq!(d.get(b"new").unwrap(), Some(b"v".to_vec()));

        d.compact().unwrap();
        assert!(d.base_run_id < seq, "an unwritten commit must not be folded");
        assert!(d.commits.iter().any(|c| c.seq == seq));
        d.flush_watermark().unwrap();
        for w in s.list("watermark/").unwrap() {
            assert!(id_from(&w.key) < seq, "the watermark must not vouch for it either");
        }

        fly(&mut d, &st).unwrap();
        d.compact().unwrap();
        assert_eq!(d.base_run_id, seq);
        let again = Db::open_with(st).unwrap();
        assert_eq!(again.get(b"new").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn a_discard_marker_outranks_a_later_vouch() {
        // Commit 1 lands, commit 2 lands and confirms first; 1 then aborts.
        // The next object's header vouches for everything through 2, which
        // includes 1 -- the marker is what says it aborted.
        let s = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let one_ = d.commit_batch(one(b"a", b"aborted"), 1).unwrap().unwrap();
        let two = d.commit_batch(one(b"b", b"fine"), 2).unwrap().unwrap();
        d.mark_confirmed(two);
        d.discard_staged(one_, Discard::Aborted);
        let three = d.commit_batch(one(b"c", b"later"), 3).unwrap().unwrap();
        d.mark_confirmed(three);
        d.flush_watermark().unwrap();

        let again = Db::open_with(s).unwrap();
        assert_eq!(again.get(b"a").unwrap(), None, "an aborted commit stays aborted");
        assert_eq!(again.get(b"b").unwrap(), Some(b"fine".to_vec()));
    }

    #[test]
    fn losing_the_sequence_race_is_not_a_conflict_with_yourself() {
        // The second writer loses the race, catches up, and re-validates --
        // against a staged map now holding its own commit, with exactly the
        // keys it is checking. Counting that made every race a spurious 40001.
        let s = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let mut a = Db::open(Arc::clone(&s)).unwrap();
        let mut b = Db::open_with(Arc::clone(&s)).unwrap();
        let snap = a.current_seq();

        let mut wa = BTreeMap::new();
        wa.insert(b"row-a".to_vec(), Op::Put(b"1".to_vec()));
        let seq = a.commit_batch_at(wa, 1, snap).unwrap().unwrap().unwrap();
        a.mark_confirmed(seq);

        let mut wb = BTreeMap::new();
        wb.insert(b"row-b".to_vec(), Op::Put(b"2".to_vec()));
        let seq = b
            .commit_batch_at(wb, 2, snap)
            .unwrap()
            .expect("different rows are not a conflict")
            .unwrap();
        b.mark_confirmed(seq);
        b.flush_watermark().unwrap();

        let d = Db::open_with(Arc::clone(&s)).unwrap();
        assert_eq!(d.get(b"row-a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(d.get(b"row-b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn a_refused_or_aborted_commit_records_why_it_died() {
        // The object is durable before we know the transaction's fate. Either
        // way it is discarded, but a commit refused by validation and one
        // orphaned by a crash are different events, and the bucket says which.
        for (why, tag) in [(Discard::Refused, "refused"), (Discard::Aborted, "aborted")] {
            let s = Arc::new(MemStore::new());
            let mut d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
            let mut w = BTreeMap::new();
            w.insert(b"row".to_vec(), Op::Put(b"v".to_vec()));
            let seq = d.commit_batch(w, 9).unwrap().unwrap();
            d.discard_staged(seq, why);

            let body = s.get(&discard_key(seq)).unwrap().unwrap();
            assert_eq!(body, format!("discard:{tag}").into_bytes());

            let d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
            assert_eq!(d.get(b"row").unwrap(), None);
            assert_eq!(d.discarded.get(&seq), Some(&why), "the reason survives a reopen");
        }
    }

    #[test]
    fn a_conflicting_version_folded_into_a_run_is_still_a_conflict() {
        // Compaction empties `commits` of everything it folded. A snapshot
        // taken below the new run then has nothing left to validate against,
        // and the earlier update disappears with no serialization error.
        let s = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        d.put(b"hot", b"v0");
        d.commit().unwrap();
        let snap = d.current_seq();

        // Someone else changes the row, and compaction folds that change away.
        d.put(b"hot", b"v1");
        d.commit().unwrap();
        d.compact().unwrap();
        assert!(d.commits.is_empty(), "the conflicting commit is only in the run now");
        assert!(snap < d.base_run_id);

        let mut w = BTreeMap::new();
        w.insert(b"hot".to_vec(), Op::Put(b"stale".to_vec()));
        let r = d.commit_batch_at(w, 7, snap).unwrap();
        assert!(r.is_err(), "the folded version must still refuse the stale write");
        assert_eq!(d.get(b"hot").unwrap(), Some(b"v1".to_vec()), "no lost update");
    }

    #[test]
    fn catching_up_does_not_expose_another_backends_unconfirmed_commit() {
        // `catch_up` runs when another writer took our sequence number, so its
        // commit is in flight by definition: conflict detection must see it,
        // reads must not. Admitting it to `commits` is a dirty read, and
        // compaction would fold an aborted transaction into a run for ever.
        let s = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let mut a = Db::open(Arc::clone(&s)).unwrap();
        let mut b = Db::open_with(Arc::clone(&s)).unwrap();

        let mut wa = BTreeMap::new();
        wa.insert(b"a-row".to_vec(), Op::Put(b"in-flight".to_vec()));
        let staged = a.commit_batch(wa, 1).unwrap().unwrap(); // durable, unconfirmed

        // b loses the race for that number, catches up, and lands elsewhere.
        let mut wb = BTreeMap::new();
        wb.insert(b"b-row".to_vec(), Op::Put(b"mine".to_vec()));
        b.commit_batch(wb, 2).unwrap().unwrap();

        assert_eq!(
            b.get(b"a-row").unwrap(),
            None,
            "a transaction that has not committed is not readable"
        );

        // It is still a conflict, which is why catch_up keeps it at all.
        let mut wc = BTreeMap::new();
        wc.insert(b"a-row".to_vec(), Op::Put(b"clobber".to_vec()));
        assert!(
            b.commit_batch_at(wc, 3, 0).unwrap().is_err(),
            "an in-flight write on the same row is still a conflict"
        );

        // And it must never become permanent run data.
        a.discard_staged(staged, Discard::Aborted);
        b.compact().unwrap();
        assert_eq!(
            b.get(b"a-row").unwrap(),
            None,
            "an aborted foreign commit must not be folded into the run"
        );
    }

    #[test]
    fn a_conflict_refusal_marks_its_own_object_dead() {
        // Two writers race the same row. The loser's object is already in the
        // bucket when validation refuses it, so the refusal has to be recorded.
        let s = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let mut a = Db::open(Arc::clone(&s)).unwrap();
        let mut b = Db::open_with(Arc::clone(&s)).unwrap();
        let snap = a.current_seq();

        let mut w = BTreeMap::new();
        w.insert(b"hot".to_vec(), Op::Put(b"from-a".to_vec()));
        let seq = a.commit_batch_at(w, 1, snap).unwrap().unwrap().unwrap();
        a.mark_confirmed(seq);
        a.flush_watermark().unwrap(); // vouch for the winner, as a clean shutdown would

        let mut w = BTreeMap::new();
        w.insert(b"hot".to_vec(), Op::Put(b"from-b".to_vec()));
        let lost = b.commit_batch_at(w, 2, snap).unwrap();
        assert!(lost.is_err(), "second writer must be refused");

        let reopened = Db::open_with(Arc::clone(&s)).unwrap();
        assert_eq!(reopened.discarded.get(&2), Some(&Discard::Refused));
        assert_eq!(reopened.get(b"hot").unwrap(), Some(b"from-a".to_vec()));
    }

    #[test]
    fn an_uncommitted_batch_leaves_nothing_behind() {
        // The atomicity half of group commit: buffered writes that never reach
        // `commit_batch` leave no trace in the bucket, so a crash mid-statement
        // cannot make half a transaction durable.
        let s = Arc::new(MemStore::new());
        {
            let mut d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
            for i in 0..500 {
                d.put(format!("k{i:04}").as_bytes(), b"v");
            }
            // no commit: simulate the process dying here
        }
        assert!(s.list("commit/").unwrap().is_empty(), "nothing durable");
        let d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        assert_eq!(d.get(b"k0000").unwrap(), None);
        assert_eq!(d.get(b"k0499").unwrap(), None);
    }

    #[test]
    fn reads_at_an_old_snapshot_see_old_data() {
        // Time travel through the whole layered read path, not just one run.
        let (_s, mut d) = db();
        d.put(b"t/1", b"one");
        d.put(b"t/2", b"two");
        assert_eq!(d.commit().unwrap(), Some(1));
        d.put(b"t/1", b"ONE");
        assert_eq!(d.commit().unwrap(), Some(2));
        d.delete(b"t/2");
        assert_eq!(d.commit().unwrap(), Some(3));

        assert_eq!(d.get_at(b"t/1", 1).unwrap(), Some(b"one".to_vec()));
        assert_eq!(d.get_at(b"t/1", 2).unwrap(), Some(b"ONE".to_vec()));
        assert_eq!(d.get(b"t/1").unwrap(), Some(b"ONE".to_vec()));
        assert_eq!(d.get_at(b"t/2", 2).unwrap(), Some(b"two".to_vec()));
        assert_eq!(d.get(b"t/2").unwrap(), None, "deleted at 3");
        assert_eq!(d.get_at(b"t/1", 0).unwrap(), None, "before it existed");

        let at = |snap| {
            let mut v: Vec<String> = d
                .scan_prefix_at(b"t/", snap)
                .unwrap()
                .into_iter()
                .map(|(k, val)| {
                    format!("{}={}", String::from_utf8(k).unwrap(), String::from_utf8(val).unwrap())
                })
                .collect();
            v.sort();
            v.join(",")
        };
        assert_eq!(at(1), "t/1=one,t/2=two");
        assert_eq!(at(2), "t/1=ONE,t/2=two");
        assert_eq!(at(LATEST), "t/1=ONE");
    }

    #[test]
    fn history_survives_compaction() {
        // The failure this guards against is subtle: time travel works, then
        // stops working the first time a table is compacted, because the fold
        // collapsed every row to its newest version.
        let (_s, mut d) = db();
        for v in ["v1", "v2", "v3"] {
            d.put(b"t/1", v.as_bytes());
            d.commit().unwrap();
        }
        for i in 0..120 {
            d.put(format!("t/pad{i:04}").as_bytes(), b"x");
            d.commit().unwrap();
        }
        assert!(d.needs_compaction());
        d.compact().unwrap();
        assert_eq!(d.run_count(), 1);
        assert_eq!(d.commit_backlog(), 0, "everything folded into the run");

        assert_eq!(d.get_at(b"t/1", 1).unwrap(), Some(b"v1".to_vec()));
        assert_eq!(d.get_at(b"t/1", 2).unwrap(), Some(b"v2".to_vec()));
        assert_eq!(d.get_at(b"t/1", 3).unwrap(), Some(b"v3".to_vec()));
        assert_eq!(d.get(b"t/1").unwrap(), Some(b"v3".to_vec()));
        assert_eq!(d.scan_prefix_at(b"t/1", 2).unwrap().len(), 1);
    }

    #[test]
    fn history_survives_reopen_from_the_bucket() {
        let s = Arc::new(MemStore::new());
        {
            let mut d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
            d.put(b"t/1", b"yesterday");
            d.commit().unwrap();
            d.put(b"t/1", b"today");
            d.commit().unwrap();
            d.compact().unwrap();
        }
        let d = Db::open(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        assert_eq!(d.get_at(b"t/1", 1).unwrap(), Some(b"yesterday".to_vec()));
        assert_eq!(d.get(b"t/1").unwrap(), Some(b"today".to_vec()));
    }

    #[test]
    fn a_write_to_a_row_someone_else_changed_is_refused() {
        let s = Arc::new(MemStore::new());
        let mut a = Db::open_with(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        a.put(b"k", b"original");
        a.commit().unwrap();

        // Both read at seq 1.
        let snap = a.current_seq();
        let mut b = Db::open_with(Arc::clone(&s) as Arc<dyn Store>).unwrap();

        // a wins the race.
        let mut w = BTreeMap::new();
        w.insert(b"k".to_vec(), Op::Put(b"from-a".to_vec()));
        let seq = a.commit_batch_at(w, 0, snap).unwrap().unwrap();
        a.mark_confirmed(seq.unwrap());

        // b, still on the old snapshot, must be refused rather than overwrite.
        b.catch_up().unwrap();
        let mut w = BTreeMap::new();
        w.insert(b"k".to_vec(), Op::Put(b"from-b".to_vec()));
        let conflict = b.commit_batch_at(w, 0, snap).unwrap().unwrap_err();
        assert_eq!(conflict.key, b"k".to_vec());

        assert_eq!(a.get(b"k").unwrap(), Some(b"from-a".to_vec()));
    }

    #[test]
    fn writes_to_different_rows_do_not_conflict() {
        let s = Arc::new(MemStore::new());
        let mut a = Db::open_with(Arc::clone(&s) as Arc<dyn Store>).unwrap();
        a.put(b"seed", b"1");
        a.commit().unwrap();
        let snap = a.current_seq();

        let mut w = BTreeMap::new();
        w.insert(b"x".to_vec(), Op::Put(b"1".to_vec()));
        let seq = a.commit_batch_at(w, 0, snap).unwrap().unwrap().unwrap();
        a.mark_confirmed(seq);

        let mut w = BTreeMap::new();
        w.insert(b"y".to_vec(), Op::Put(b"2".to_vec()));
        assert!(a.commit_batch_at(w, 0, snap).unwrap().is_ok());
    }

    #[test]
    fn a_window_walks_a_range_and_stops() {
        let store = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let mut db = Db::open(Arc::clone(&store)).unwrap();
        for i in 0..50u32 {
            db.put(format!("k/{i:04}").as_bytes(), &[i as u8]);
        }
        db.commit().unwrap();
        // Half of them removed, so most windows come back short of their
        // limit without the range being finished.
        for i in (0..50u32).step_by(2) {
            db.delete(format!("k/{i:04}").as_bytes());
        }
        db.commit().unwrap();

        let mut lo = b"k/".to_vec();
        let hi = b"k0".to_vec();
        let mut seen = Vec::new();
        let mut windows = 0;
        loop {
            let (rows, resume) = db.scan_window_at(&lo, &hi, LATEST, 7).unwrap();
            windows += 1;
            assert!(windows < 100, "the walk must end");
            seen.extend(rows.into_iter().map(|(k, _)| k));
            match resume {
                Some(next) => lo = next,
                None => break,
            }
        }
        assert_eq!(seen.len(), 25, "every surviving row, once");
        let mut sorted = seen.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted, seen, "in order, no repeats");
        assert!(windows > 3, "the limit really did split it up: {windows}");
    }

    #[test]
    fn a_window_asked_for_everything_is_one_window() {
        let store = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let mut db = Db::open(Arc::clone(&store)).unwrap();
        for i in 0..5u32 {
            db.put(format!("k/{i:04}").as_bytes(), b"");
        }
        db.commit().unwrap();
        let (rows, resume) = db.scan_window_at(b"k/", b"k0", LATEST, 100).unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(resume, None);
    }

    #[test]
    fn a_backward_walk_covers_the_same_rows_from_the_other_end() {
        let store = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let mut db = Db::open(Arc::clone(&store)).unwrap();
        for i in 0..40u32 {
            db.put(format!("k/{i:04}").as_bytes(), &[i as u8]);
        }
        db.commit().unwrap();
        db.compact().unwrap();
        for i in (0..40u32).step_by(3) {
            db.delete(format!("k/{i:04}").as_bytes());
        }
        db.commit().unwrap();

        let forward: Vec<Vec<u8>> = db
            .scan_range_at(b"k/", b"k0", LATEST)
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();

        let mut hi = b"k0".to_vec();
        let mut back: Vec<Vec<u8>> = Vec::new();
        let mut windows = 0;
        loop {
            let (rows, resume) = db.scan_window_back_at(b"k/", &hi, LATEST, 6).unwrap();
            windows += 1;
            assert!(windows < 100, "the walk must end");
            let mut got: Vec<Vec<u8>> = rows.into_iter().map(|(k, _, _)| k).collect();
            got.extend(back);
            back = got;
            match resume {
                Some(next) => hi = next,
                None => break,
            }
        }
        assert_eq!(back, forward, "same rows, same order, read from the top");
        assert!(windows > 3, "the limit split it up: {windows}");
    }

    #[test]
    fn a_backward_window_returns_the_highest_rows_first() {
        let store = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let mut db = Db::open(Arc::clone(&store)).unwrap();
        for i in 0..100u32 {
            db.put(format!("k/{i:04}").as_bytes(), b"");
        }
        db.commit().unwrap();
        let (rows, _) = db.scan_window_back_at(b"k/", b"k0", LATEST, 3).unwrap();
        let keys: Vec<Vec<u8>> = rows.into_iter().map(|(k, _, _)| k).collect();
        assert_eq!(
            keys,
            vec![b"k/0097".to_vec(), b"k/0098".to_vec(), b"k/0099".to_vec()],
            "the top three, still in ascending order"
        );
    }

    #[test]
    fn a_window_under_an_old_snapshot_is_not_cut_short_by_newer_versions_in_the_run() {
        // The run counts rows toward the page size. Versions newer than the
        // snapshot used to count too, then be dropped by the caller, and the
        // short page read as the end of the range: rows past it were missed.
        let s = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        for i in 0..20u32 {
            d.put(format!("k/{i:04}").as_bytes(), b"old");
        }
        d.commit().unwrap();
        let snap = d.current_seq();

        // Rewrite the first half, then fold everything into one run.
        for i in 0..10u32 {
            d.put(format!("k/{i:04}").as_bytes(), b"new");
        }
        d.commit().unwrap();
        d.compact().unwrap();
        assert!(snap < d.base_run_id, "the newer versions live only in the run");

        let (lo, hi) = (b"k/".to_vec(), b"k0".to_vec());
        for (at, want) in [(snap, &b"old"[..]), (LATEST, &b"new"[..])] {
            let mut got = Vec::new();
            let mut from = lo.clone();
            loop {
                let (rows, resume) = d.scan_window_at(&from, &hi, at, 4).unwrap();
                got.extend(rows);
                match resume {
                    Some(next) => from = next,
                    None => break,
                }
            }
            // Once, each: a page resumed from a run must not repeat the row
            // it stopped on, and a page cut short by invisible versions must
            // not end the walk.
            assert_eq!(got.len(), 20, "every row as of {at}, forwards, once each");
            assert!(got.windows(2).all(|w| w[0].0 < w[1].0), "in order, no repeats");
            assert!(got[..10].iter().all(|(_, v)| v == want), "and the version seen at {at}");

            let mut got = Vec::new();
            let mut top = hi.clone();
            loop {
                let (rows, resume) = d.scan_window_back_at(&lo, &top, at, 4).unwrap();
                got.extend(rows);
                match resume {
                    Some(next) => top = next,
                    None => break,
                }
            }
            assert_eq!(got.len(), 20, "every row as of {at}, backwards, once each");
            got.sort(); // pages arrive top first
            assert!(got[..10].iter().all(|(_, v, _)| v == want), "and the version seen at {at}");
        }
    }

    #[test]
    fn a_staged_commit_is_a_commit_once_its_object_lands() {
        // The object is self-confirmed: a crash between the PUT and the
        // Postgres commit keeps it, as a WAL commit record would.
        let s = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let mut w = BTreeMap::new();
        w.insert(b"a".to_vec(), Op::Put(b"1".to_vec()));
        let (_, seq) = d.stage_commit(w, 0, LATEST, true).unwrap().unwrap().unwrap();
        let f = d.take_flight().unwrap();
        s.put_if_absent(&f.key, &f.bytes).unwrap();
        d.flight_written(f.first);
        assert_eq!(d.get(b"a").unwrap(), None, "staged, so not yet readable here");
        drop(d); // no mark_confirmed, no watermark: the crash

        let r = Db::open_with(Arc::clone(&s)).unwrap();
        assert_eq!(r.get(b"a").unwrap(), Some(b"1".to_vec()), "the object is the commit");
        assert!(s.list("resolve/").unwrap().is_empty(), "nothing was discarded");

        // An abort after landing is recorded, and the record wins.
        let mut d = Db::open_with(Arc::clone(&s)).unwrap();
        let mut w = BTreeMap::new();
        w.insert(b"b".to_vec(), Op::Put(b"2".to_vec()));
        let (_, seq2) = d.stage_commit(w, 0, LATEST, true).unwrap().unwrap().unwrap();
        assert!(seq2 > seq);
        let f = d.take_flight().unwrap();
        s.put_if_absent(&f.key, &f.bytes).unwrap();
        d.flight_written(f.first);
        d.discard_staged(seq2, Discard::Aborted);
        drop(d);
        let r = Db::open_with(Arc::clone(&s)).unwrap();
        assert_eq!(r.get(b"b").unwrap(), None, "aborted after landing: the marker rules");
        assert_eq!(r.get(b"a").unwrap(), Some(b"1".to_vec()));
    }

    #[test]
    fn the_async_backlog_counts_only_what_was_acknowledged_early() {
        let s = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let w = |k: &[u8]| {
            let mut m = BTreeMap::new();
            m.insert(k.to_vec(), Op::Put(b"v".to_vec()));
            m
        };
        d.stage_commit(w(b"a"), 0, LATEST, false).unwrap().unwrap();
        d.stage_commit(w(b"b"), 0, LATEST, true).unwrap().unwrap();
        d.stage_commit(w(b"c"), 0, LATEST, false).unwrap().unwrap();
        assert_eq!(d.async_backlog(), 2, "two acknowledged, one waiting");
        let f = d.take_flight().unwrap();
        assert_eq!(d.async_backlog(), 2, "in flight still counts: not in the bucket yet");
        s.put_if_absent(&f.key, &f.bytes).unwrap();
        d.flight_written(f.first);
        assert_eq!(d.async_backlog(), 0);
    }

    #[test]
    fn flights_land_in_any_order_but_are_acknowledged_in_sequence() {
        let s = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let (t1, _) = d.stage_commit(one(b"a", b"1"), 0, LATEST, true).unwrap().unwrap().unwrap();
        let f1 = d.take_flight().unwrap();
        let (t2, _) = d.stage_commit(one(b"b", b"2"), 0, LATEST, true).unwrap().unwrap().unwrap();
        let f2 = d.take_flight().expect("a second flight while the first is out");
        assert!(f2.first > f1.first);

        // The second lands first.
        s.put_if_absent(&f2.key, &f2.bytes).unwrap();
        d.flight_written(f2.first);
        assert!(d.take_outcome(t2).is_none(), "not told: the one before it is still out");
        assert!(d.has_unwritten());

        s.put_if_absent(&f1.key, &f1.bytes).unwrap();
        d.flight_written(f1.first);
        assert!(matches!(d.take_outcome(t1), Some(Outcome::Durable(_))));
        assert!(matches!(d.take_outcome(t2), Some(Outcome::Durable(_))), "released with it");
        assert!(!d.has_unwritten());

        let r = Db::open_with(Arc::clone(&s)).unwrap();
        assert_eq!(r.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(r.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn a_failed_flight_releases_the_landed_ones_behind_it() {
        let s = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let (t1, _) = d.stage_commit(one(b"a", b"1"), 0, LATEST, true).unwrap().unwrap().unwrap();
        let f1 = d.take_flight().unwrap();
        let (t2, _) = d.stage_commit(one(b"b", b"2"), 0, LATEST, true).unwrap().unwrap().unwrap();
        let f2 = d.take_flight().unwrap();
        s.put_if_absent(&f2.key, &f2.bytes).unwrap();
        d.flight_written(f2.first);
        d.flight_failed(f1.first, "no route to the bucket");
        assert!(matches!(d.take_outcome(t1), Some(Outcome::Failed(_))));
        assert!(matches!(d.take_outcome(t2), Some(Outcome::Durable(_))), "its hole is in an untold commit");
        assert!(!d.is_fenced(), "nobody had been told about the failed one");
        assert_eq!(d.in_flight.len(), 0);
    }

    #[test]
    fn the_in_flight_limit_holds() {
        let s = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        for i in 0..MAX_IN_FLIGHT + 1 {
            d.stage_commit(one(format!("k{i}").as_bytes(), b"v"), 0, LATEST, true).unwrap().unwrap();
            if i < MAX_IN_FLIGHT {
                assert!(d.take_flight().is_some());
            }
        }
        assert!(d.take_flight().is_none(), "the limit is the limit");
        assert_eq!(d.unwritten.len(), 1, "the last one waits");
    }

    #[test]
    fn delta_runs_answer_every_snapshot_and_a_merge_keeps_them() {
        // Folds after the first write deltas beside the full run; reads at
        // any snapshot see the version that was live then, across runs and
        // commits alike; the merge folds the deltas away with nothing lost.
        let s = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let mut seq_of_version = Vec::new();
        for round in 0..=MAX_RUNS {
            for i in 0..3 {
                d.put(b"row", format!("r{round}i{i}").as_bytes());
                d.put(format!("only-{round}-{i}").as_bytes(), b"x");
                seq_of_version.push((d.commit().unwrap().unwrap(), format!("r{round}i{i}")));
            }
            d.compact().unwrap();
            assert!(d.run_count() >= 1 && d.run_count() <= MAX_RUNS, "runs after fold {round}");
            for (seq, v) in &seq_of_version {
                assert_eq!(d.get_at(b"row", *seq).unwrap(), Some(v.clone().into_bytes()), "at {seq}");
            }
        }
        let r = Db::open_with(Arc::clone(&s)).unwrap();
        assert_eq!(r.run_count(), d.run_count(), "open sees the same live runs");
        for (seq, v) in &seq_of_version {
            assert_eq!(r.get_at(b"row", *seq).unwrap(), Some(v.clone().into_bytes()));
        }
        let keys = r.scan_prefix(b"only-").unwrap().len();
        assert_eq!(keys, 3 * (MAX_RUNS + 1), "every row from every round");
    }

    #[test]
    fn a_crash_between_a_merge_and_its_sweep_leaves_only_leftovers() {
        let s = Arc::new(MemStore::new()) as Arc<dyn Store>;
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        // A first fold is a full run; the ones after are deltas until the
        // merge rule fires. Stop at the first merge after that, with its run
        // written and nothing swept: the crash.
        let mut last = String::new();
        for i in 0..10 * MAX_RUNS {
            d.put(b"k", format!("v{i}").as_bytes());
            d.commit().unwrap();
            last = format!("v{i}");
            let plan = d.fold_plan().unwrap();
            let folded = build_fold(&plan, 0, &BTreeMap::new()).unwrap();
            put_fold(&s, &folded).unwrap();
            if i > 0 && plan.merge {
                break;
            }
            let sweep = d.apply_fold(plan, &folded, 0).unwrap();
            d.sweep_done(execute_sweep(&s, sweep));
        }
        assert!(s.list("run/").unwrap().len() > 1, "old runs still there");
        let r = Db::open_with(Arc::clone(&s)).unwrap();
        assert_eq!(r.run_count(), 1, "only the merged run is live");
        assert_eq!(s.list("run/").unwrap().len(), 1, "and the leftovers were deleted on open");
        assert_eq!(r.get(b"k").unwrap(), Some(last.into_bytes()));
    }
}
