//! The core loop: commits, sorted runs, compaction, and the read path that
//! stitches them together.
//!
//! Reads run newest-to-oldest and stop at the first answer: commits since
//! the base run, then runs. Only the last stage touches the network, and the
//! bloom filters mean it usually touches it once.
//!
//! Numbering and visibility. A transaction is numbered at `stage_commit`,
//! written by whoever drains the queue next, and becomes visible at
//! `mark_confirmed` -- which can happen out of number order, since the
//! confirmations come from separate backends. A snapshot is therefore not
//! "the highest number handed out" but the *decided prefix*: the highest
//! number such that it and everything below it has confirmed or been
//! discarded. Nothing at or below a snapshot changes after it is taken, so a
//! reader holding one `View` and one snapshot number reads the same database
//! however many times it asks, and a write validated against that number
//! conflicts with exactly what the reader could not see.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io;
use std::sync::Arc;

use crate::commit::{self, Commit, Entry, Op};
use crate::index_key;
use crate::key::{self, LATEST};
use crate::lease::Lease;
use crate::run::{self, Run};
use crate::s3::PutOutcome;
use crate::store::{ObjectRange, Store};

/// Compact once this many commits have piled up on top of the base run, so a
/// read never walks an unbounded number of them.
pub const COMPACT_AFTER_COMMITS: usize = 100;

/// Why a commit object that is (or may be) durable must never be applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Discard {
    /// The transaction aborted after its object had already landed.
    Aborted,
    /// Validation refused the commit, so the client was told it failed.
    Refused,
    /// The PUT was given up on and the client told so. The object was not in
    /// the bucket when the writer looked, but a late arrival is still
    /// possible, and the marker makes sure it is never applied.
    Failed,
    /// The process fenced itself with this commit in flight; its client was
    /// told the outcome is unknown, so the object must not count.
    Fenced,
}

impl Discard {
    pub fn tag(self) -> &'static str {
        match self {
            Discard::Aborted => "aborted",
            Discard::Refused => "refused",
            Discard::Failed => "failed",
            Discard::Fenced => "fenced",
        }
    }
    fn from_tag(t: &str) -> Option<Discard> {
        Some(match t {
            "aborted" => Discard::Aborted,
            "refused" => Discard::Refused,
            "failed" => Discard::Failed,
            "fenced" => Discard::Fenced,
            _ => return None,
        })
    }
}

/// What a `resolve/<seq>` object says about that sequence number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Marker {
    /// The object named by `xid` and fingerprint must never be applied. Bound
    /// to that object, not to the number: a number handed out again after a
    /// restart, when the object it was first given to never landed, gets a
    /// different fingerprint and is not judged by a marker meant for another.
    Discard { why: Discard, xid: u32, crc: u32 },
    /// Nothing at this number is ever valid: an open found a gap below the
    /// numbers in use and nailed it shut, so a stale writer's object landing
    /// there later is not mistaken for history.
    Hole { epoch: u64 },
}

impl Marker {
    fn encode(&self) -> Vec<u8> {
        match self {
            Marker::Discard { why, xid, crc } => {
                format!("discard:{}\nxid:{xid}\ncrc:{crc:08x}\n", why.tag()).into_bytes()
            }
            Marker::Hole { epoch } => format!("hole:{epoch:016x}\n").into_bytes(),
        }
    }

    fn decode(body: &[u8]) -> Option<Marker> {
        let s = std::str::from_utf8(body).ok()?;
        let mut lines = s.lines();
        let first = lines.next()?;
        if let Some(tag) = first.strip_prefix("discard:") {
            let why = Discard::from_tag(tag)?;
            let xid = lines.next()?.strip_prefix("xid:")?.parse().ok()?;
            let crc = u32::from_str_radix(lines.next()?.strip_prefix("crc:")?, 16).ok()?;
            return Some(Marker::Discard { why, xid, crc });
        }
        let epoch = u64::from_str_radix(first.strip_prefix("hole:")?, 16).ok()?;
        Some(Marker::Hole { epoch })
    }

    /// Whether this marker judges the given object.
    fn matches(&self, c: &Commit, crc: u32) -> bool {
        match self {
            Marker::Discard { xid, crc: want, .. } => *xid == c.xid && *want == crc,
            Marker::Hole { .. } => true,
        }
    }
}

pub fn discard_key(seq: u64) -> String {
    format!("resolve/{seq:016x}")
}

/// Records, at a takeover, the first sequence number the new owner will use.
/// Any object numbered at or above it whose epoch is lower was written by a
/// writer that had already lost the lease, and is never applied.
pub fn fence_key(epoch: u64) -> String {
    format!("fence/{epoch:016x}")
}

/// A discard marker prepared under the lock and written without it. See
/// [`Db::begin_discard`].
#[derive(Debug)]
pub struct DiscardMarker {
    pub seq: u64,
    xid: u32,
    marker: Marker,
}

impl DiscardMarker {
    /// Puts the marker in the bucket. A marker already there means the number
    /// is dead already, which is the same verdict.
    pub fn write(&self, store: &Arc<dyn Store>) -> io::Result<()> {
        store.put_if_absent(&discard_key(self.seq), &self.marker.encode()).map(|_| ())
    }

    /// What an operator would have to create by hand if the write failed.
    fn by_hand(&self) -> String {
        format!(
            "create the object `{}` with the body {:?} before restarting",
            discard_key(self.seq),
            String::from_utf8_lossy(&self.marker.encode())
        )
    }
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
    /// What the writes were validated against.
    pub snapshot: u64,
    xid: u32,
    /// The fingerprint of `bytes`, for a discard marker.
    crc: u32,
    /// The object as it will land: encoded once, at staging.
    bytes: Vec<u8>,
}

/// What a ticket-holder learns once its commit has been dealt with.
#[derive(Debug, Clone)]
pub enum Outcome {
    /// In the bucket, at this sequence number: the one staging handed out.
    Durable(u64),
    /// Reserved. Nothing produces it: a lost sequence race fences the
    /// process rather than re-validating and re-numbering, since a second
    /// writer on the bucket is a failure of the lease, not a race to win.
    /// The table AM's match still names it.
    Refused(Conflict),
    /// The PUT failed and was given up on, and the bucket did not hold the
    /// object when the writer looked. A marker makes sure a late arrival is
    /// never applied.
    Failed(String),
    /// The process can no longer be trusted with the bucket: its lease is
    /// gone, or a commit it had acknowledged was lost. Whether this commit
    /// is in the bucket is unknown or irrelevant; it does not count.
    Fenced(String),
}

/// One PUT's worth of pending commits, encoded. The members stay in the
/// [`Db`] while the bytes travel; `flight_written`, `flight_lost` or
/// `flight_failed` resolves them.
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
    key: String,
    /// A copy of what was sent, so a failed PUT can be checked against what
    /// the bucket holds.
    bytes: Vec<u8>,
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

/// A commit whose object is in the bucket, or on its way, but whose
/// transaction has not yet committed in Postgres.
struct Staged {
    commit: Commit,
    crc: u32,
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
/// it needs no catalog lookup (this runs on the compactor, and a catalog
/// read would want the storage lock) and an UPDATE is a delete plus an
/// insert at a fresh row id, so every update strands an entry. An index
/// whose table the caller could not name is left alone; guessing would
/// delete live entries.
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
    /// Numbered, and written or on the way, but not yet committed by
    /// Postgres. Held out of `commits` so other backends cannot read them:
    /// the object lands at pre-commit, and until Postgres commits, those
    /// rows are a dirty read. Durable, though: a crash from here keeps them,
    /// an abort marks them.
    staged: BTreeMap<u64, Staged>,
    /// Sequence numbers under a `resolve/` marker: whatever is or lands
    /// there is never applied.
    discarded: BTreeMap<u64, Marker>,
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
    /// Set once this process can no longer be trusted with the bucket: the
    /// lease is gone, or a commit it acknowledged was lost. Every write
    /// errors from then on; reads go on answering from what is in memory.
    fenced: Option<String>,
    /// The single-writer lease, or none for a read-only open.
    lease: Option<Lease>,
    /// The lease's epoch, stamped into every commit this process writes.
    epoch: u64,
}

impl Db {
    /// A consistent read view: the runs and commits as they stand, shared
    /// rather than copied, so a reader can leave the lock behind before it
    /// touches the network. Runs are immutable objects and commits are
    /// appended and folded, never edited. Read at a snapshot number taken
    /// from [`Db::current_seq`] the view answers the same way for ever,
    /// whether it was taken with the snapshot or later: everything at or
    /// below the snapshot had already been decided when the number was
    /// handed out, and a decision is never revised.
    pub fn view(&self) -> View {
        View {
            runs: self.runs.clone(),
            commits: self.commits.clone(),
            collected_through: self.collected_through,
            base_run_id: self.base_run_id,
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
    pub fn scan_prefix_at(&self, prefix: &[u8], snapshot: u64) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.view().scan_prefix_at(prefix, snapshot)
    }
    pub fn scan_prefix(&self, prefix: &[u8]) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.view().scan_prefix(prefix)
    }

    /// Opens for writing: takes the single-writer lease, with a heartbeat
    /// thread renewing it, then rebuilds state from the bucket.
    pub fn open(store: Arc<dyn Store>) -> io::Result<Db> {
        let lease = Lease::acquire_with_heartbeat(&store)?;
        Db::open_inner(store, Some(lease))
    }

    /// Opens for writing under a lease the caller took: how a test drives
    /// the clock by hand.
    pub fn open_with_lease(store: Arc<dyn Store>, lease: Lease) -> io::Result<Db> {
        Db::open_inner(store, Some(lease))
    }

    /// Opens to read only. Takes no lease, and so writes nothing to the
    /// bucket and deletes nothing from it; every write through it is
    /// refused. A second process looking at a bucket, or a test checking
    /// what a restart would see.
    pub fn open_with(store: Arc<dyn Store>) -> io::Result<Db> {
        Db::open_inner(store, None)
    }

    /// Rebuilds state from the bucket: newest run, then every commit on top
    /// of it. An object is a commit once it lands, unless a marker says its
    /// number is dead or the epoch fences say a stale writer wrote it.
    ///
    /// With the lease held this also records the takeover fence for the new
    /// epoch, nails every gap below the numbers in use shut with a hole
    /// marker, drops markers for tail numbers that never got an object (they
    /// will be handed out again), and deletes the leftovers of an
    /// unfinished merge.
    fn open_inner(store: Arc<dyn Store>, lease: Option<Lease>) -> io::Result<Db> {
        let owner = lease.is_some();
        let epoch = lease.as_ref().map_or(0, Lease::epoch);

        let mut runs = Vec::new();
        let mut base_run_id = 0u64;
        let mut listed = store.list("run/")?;
        listed.sort_by(|a, b| b.key.cmp(&a.key)); // newest first
        // A full run covers every commit up to its number; a delta covers
        // the commits between the run before it and its own number. Live is
        // the newest full run and every delta above it. The rest are
        // leftovers of a merge whose sweep never finished, and go now.
        let mut ids = Vec::with_capacity(listed.len());
        for info in &listed {
            ids.push(parse_id(&info.key)?);
        }
        let newest_full = listed
            .iter()
            .zip(&ids)
            .filter(|(i, _)| !run::is_delta(&i.key))
            .map(|(_, &id)| id)
            .max()
            .unwrap_or(0);
        for (info, &id) in listed.iter().zip(&ids) {
            let live = if run::is_delta(&info.key) { id > newest_full } else { id == newest_full };
            if !live {
                if owner {
                    let _ = store.delete(&info.key);
                }
                continue;
            }
            base_run_id = base_run_id.max(id);
            runs.push(Arc::new(Run::open(ObjectRange {
                store: Arc::clone(&store),
                key: info.key.clone(),
                size: info.size,
            })?));
        }

        let mut decoded: Vec<(Commit, u32)> = Vec::new();
        let mut present: BTreeSet<u64> = BTreeSet::new();
        let mut next_seq = 1u64;
        let mut objects = BTreeMap::new();
        let mut ckeys = store.list("commit/")?;
        ckeys.sort_by(|a, b| a.key.cmp(&b.key));
        for info in ckeys {
            let first = parse_id(&info.key)?;
            let bytes = store
                .get(&info.key)?
                .ok_or_else(|| io::Error::other(format!("listed commit `{}` disappeared", info.key)))?;
            // Read even when the first member is folded: a batch can straddle
            // the run boundary, and the members past it are still live.
            let members = commit::decode_members(&bytes)?;
            let last = members.last().map_or(first, |(c, _)| c.seq);
            next_seq = next_seq.max(last + 1);
            objects.insert(first, last);
            for (c, crc) in members {
                present.insert(c.seq);
                if c.seq <= base_run_id {
                    continue; // already folded into the run
                }
                decoded.push((c, crc));
            }
        }
        // A run consumes a sequence number too. Without this, compact,
        // restart, compact reuses the id and fails quietly as "run already
        // exists", and the commit chain then grows for ever.
        next_seq = next_seq.max(base_run_id + 1);

        let mut collected_through = 0u64;
        for info in store.list("horizon/")? {
            collected_through = collected_through.max(parse_id(&info.key)?);
        }

        // Takeover fences: (epoch, first number that epoch will use).
        let mut fences: Vec<(u64, u64)> = Vec::new();
        for info in store.list("fence/")? {
            let e = parse_id(&info.key)?;
            let body = store.get(&info.key)?.unwrap_or_default();
            let seq = std::str::from_utf8(&body)
                .ok()
                .and_then(|s| s.trim().parse::<u64>().ok())
                .ok_or_else(|| io::Error::other(format!("fence record `{}` does not parse", info.key)))?;
            fences.push((e, seq));
        }
        if owner {
            // Everything this epoch writes is numbered from `next_seq` up;
            // anything a lower epoch puts at or above it from now on was
            // written after losing the lease.
            let k = fence_key(epoch);
            match store.put_if_absent(&k, next_seq.to_string().as_bytes())? {
                PutOutcome::Written => fences.push((epoch, next_seq)),
                // Only this epoch writes its own record, so it is ours from a
                // PUT whose response was lost.
                PutOutcome::AlreadyExists => {
                    let body = store.get(&k)?.unwrap_or_default();
                    let seq = std::str::from_utf8(&body)
                        .ok()
                        .and_then(|s| s.trim().parse::<u64>().ok())
                        .ok_or_else(|| io::Error::other(format!("fence record `{k}` does not parse")))?;
                    fences.push((epoch, seq));
                }
            }
        }

        let mut markers: BTreeMap<u64, Marker> = BTreeMap::new();
        for info in store.list("resolve/")? {
            let seq = parse_id(&info.key)?;
            let body = store.get(&info.key)?.unwrap_or_default();
            let m = Marker::decode(&body).ok_or_else(|| {
                io::Error::other(format!(
                    "discard marker `{}` does not parse ({:?}); refusing to guess whether commit \
                     {seq} happened",
                    info.key,
                    String::from_utf8_lossy(&body)
                ))
            })?;
            markers.insert(seq, m);
        }

        let mut commits = Vec::with_capacity(decoded.len());
        let mut stale: Vec<(u64, u64)> = Vec::new();
        let mut mismatched: Vec<u64> = Vec::new();
        for (c, crc) in decoded {
            if is_stale(&fences, c.epoch, c.seq) {
                stale.push((c.seq, c.epoch));
                continue;
            }
            match markers.get(&c.seq) {
                Some(m) if m.matches(&c, crc) => {
                    // Known dead. Left in the bucket for the collector.
                }
                Some(_) => {
                    // A marker for an object that never landed, whose number
                    // was then handed out again after a restart. It judges
                    // the object that never came, not this one.
                    mismatched.push(c.seq);
                    commits.push(Arc::new(c));
                }
                None => commits.push(Arc::new(c)),
            }
        }
        if !stale.is_empty() {
            let list = stale.iter().map(|(s, e)| format!("seq {s} (epoch {e})")).collect::<Vec<_>>().join(", ");
            eprintln!(
                "objkv: ignoring {} commit object(s) written by a writer that had lost the \
                 lease: {list}",
                stale.len()
            );
        }
        for seq in mismatched {
            eprintln!("objkv: discard marker for commit {seq} names an object that never landed; ignoring it");
            markers.remove(&seq);
            if owner {
                let _ = store.delete(&discard_key(seq));
            }
        }
        // A marker above every object: its number will be handed out again,
        // and it must not be there to condemn the newcomer. Below the
        // numbers in use it stays -- that number is never reused, and a
        // late arrival there is exactly what the marker is for.
        let tail: Vec<u64> = markers.keys().copied().filter(|&s| s >= next_seq && !present.contains(&s)).collect();
        for seq in tail {
            markers.remove(&seq);
            if owner {
                store.delete(&discard_key(seq))?;
            }
        }
        if owner {
            // Gaps below the numbers in use: a commit discarded before its
            // write, or a flight that never landed. Nothing legitimate can
            // ever appear there, so say so, or a stale writer's late object
            // would be applied by the open after this one.
            for seq in base_run_id + 1..next_seq {
                if present.contains(&seq) || markers.contains_key(&seq) {
                    continue;
                }
                let m = Marker::Hole { epoch };
                store.put_if_absent(&discard_key(seq), &m.encode())?;
                markers.insert(seq, m);
            }
        }

        Ok(Db {
            store,
            commits,
            runs,
            retired: Vec::new(),
            next_seq,
            base_run_id,
            staged: BTreeMap::new(),
            discarded: markers,
            collected_through,
            unwritten: Vec::new(),
            in_flight: BTreeMap::new(),
            outcomes: HashMap::new(),
            next_ticket: 1,
            objects,
            fenced: None,
            lease,
            epoch,
        })
    }

    /// The lease epoch this process writes under; 0 when opened read-only.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Gives the lease up so the next open, on any host, need not wait for
    /// it to expire. For a clean shutdown, after the last write has been
    /// drained; every write after this is refused.
    pub fn release_lease(&mut self) -> io::Result<()> {
        match &self.lease {
            Some(l) => l.release(),
            None => Ok(()),
        }
    }

    /// A reader captures this to pin a snapshot: the decided prefix. Every
    /// number at or below it has confirmed or been discarded, so what a
    /// read at it returns never changes afterwards. A number handed out but
    /// not yet confirmed holds the snapshot below it, however many commits
    /// above it have confirmed since -- they are visible only once it is.
    pub fn current_seq(&self) -> u64 {
        let undecided = self.staged.keys().next().copied().unwrap_or(self.next_seq);
        undecided.min(self.next_seq).saturating_sub(1)
    }

    /// Called once Postgres has committed, not before. This is where a staged
    /// commit becomes visible to other backends.
    pub fn mark_confirmed(&mut self, seq: u64) {
        if let Some(s) = self.staged.remove(&seq) {
            let at = self.commits.partition_point(|c| c.seq < seq);
            self.commits.insert(at, Arc::new(s.commit));
        }
    }

    /// Drops a staged commit that will never become real, and records that
    /// in the bucket if its object is or may be there.
    ///
    /// The marker is what stops the next open applying an object that
    /// landed for a transaction that then aborted. If it cannot be written
    /// the abort cannot be made to hold, so this fences the process and
    /// panics with what an operator must do before restarting. Callers
    /// holding a lock across this pay for one PUT under it; the pair
    /// [`Db::begin_discard`] / [`Db::discard_written`] does the same with
    /// the PUT outside.
    pub fn discard_staged(&mut self, seq: u64, why: Discard) {
        if let Some(m) = self.begin_discard(seq, why) {
            match m.write(&self.store) {
                Ok(()) => self.discard_written(m),
                Err(e) => self.discard_failed(&m, &e),
            }
        }
    }

    /// The first half of [`Db::discard_staged`]: forgets the staged commit
    /// and, if its object is or may be in the bucket, hands back the marker
    /// to write for it. `None` means nothing was ever written under that
    /// number and nothing need be. The caller writes the marker with
    /// [`DiscardMarker::write`] -- with no lock held -- and then reports
    /// [`Db::discard_written`], or [`Db::discard_failed`], which does not
    /// return.
    pub fn begin_discard(&mut self, seq: u64, why: Discard) -> Option<DiscardMarker> {
        let staged = self.staged.remove(&seq);
        // Not written yet: it never will be, and a number nothing was ever
        // written under needs no marker here. The gap is nailed shut by
        // the next open.
        if let Some(i) = self.unwritten.iter().position(|p| p.seq == seq) {
            let p = self.unwritten.remove(i);
            self.outcomes.remove(&p.ticket);
            return None;
        }
        let (xid, crc) = match staged {
            Some(s) => (s.commit.xid, s.crc),
            None => {
                let m = self.in_flight_members().find(|m| m.seq == seq)?;
                (m.xid, m.crc)
            }
        };
        Some(DiscardMarker { seq, xid, marker: Marker::Discard { why, xid, crc } })
    }

    pub fn discard_written(&mut self, m: DiscardMarker) {
        self.discarded.insert(m.seq, m.marker);
    }

    /// The marker could not be written: the object is in the bucket with
    /// nothing saying its transaction aborted, and the next open would apply
    /// it. The process must not go on as if the abort held.
    pub fn discard_failed(&mut self, m: &DiscardMarker, err: &io::Error) -> ! {
        let why = format!(
            "objkv PANIC: commit {} (xid {}) aborted after its object landed, and its discard \
             marker could not be written ({err}). Without the marker the next open applies the \
             aborted transaction. This server is fenced; {}.",
            m.seq,
            m.xid,
            m.by_hand()
        );
        self.fence(why.clone(), false);
        panic!("{why}");
    }

    /// Kept for the table AM's shutdown path: there is no watermark to
    /// publish any more. An object is the commit once it lands, so nothing
    /// vouches for anything; the lease is given back by [`Db::release_lease`].
    /// Always reports that nothing was written.
    pub fn flush_watermark(&self) -> io::Result<bool> {
        Ok(false)
    }

    /// The lowest sequence number that is not yet in the bucket, or `MAX`
    /// when everything numbered has landed. Nothing at or above it may be
    /// folded into a run.
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

    fn check_fenced(&self) -> io::Result<()> {
        match &self.fenced {
            Some(why) => Err(io::Error::other(why.clone())),
            None => Ok(()),
        }
    }

    /// Whether this process may write to the bucket right now. A lease that
    /// has expired or been taken over fences the process: whatever it wrote
    /// from here on could be a stale writer's object.
    fn check_lease(&mut self) -> io::Result<()> {
        let why = match &self.lease {
            None => return Err(io::Error::other("objkv: opened read-only (no lease); writes are refused")),
            Some(l) => l.why_invalid(),
        };
        if let Some(why) = why {
            let why = format!("objkv: {why}; this server can no longer write to the bucket. Restart it.");
            self.fence(why.clone(), false);
            return Err(io::Error::other(why));
        }
        Ok(())
    }

    pub fn is_fenced(&self) -> bool {
        self.fenced.is_some()
    }

    /// Numbers and validates a commit without writing it. The write is a
    /// separate step so that several transactions' commits share one PUT.
    ///
    /// The object is the commit: once it lands, nothing later need vouch for
    /// it, so a synchronous COMMIT returns as soon as the PUT does and a
    /// crash cannot take back what a client saw. A transaction that aborts
    /// after its object landed writes a discard marker, and the marker
    /// outranks the object on the next open.
    ///
    /// Validation is first-committer-wins against every commit newer than
    /// `snapshot`, including the ones queued ahead of this one: two
    /// transactions in one group writing one row still conflict. When the
    /// snapshot predates the base run the conflicting version may exist
    /// only inside a run, and this probes the runs -- network reads, under
    /// whatever lock the caller holds. [`Db::stage_commit_checked`] lets the
    /// caller do that probe first through a [`View`], without the lock.
    ///
    /// Returns the ticket to wait on and the sequence number handed out.
    pub fn stage_commit(
        &mut self,
        writes: BTreeMap<Vec<u8>, Op>,
        xid: u32,
        snapshot: u64,
        sync: bool,
    ) -> io::Result<Result<Option<(u64, u64)>, Conflict>> {
        self.stage_inner(writes, xid, snapshot, sync, None)
    }

    /// [`Db::stage_commit`] for a caller that has already run
    /// [`View::find_run_conflict`] against a view whose
    /// [`View::base_run_id`] was `probed_base_run_id`. The runs are probed
    /// again here only if a fold has replaced them since; the in-memory
    /// layers are always checked here, since only they can have moved.
    pub fn stage_commit_checked(
        &mut self,
        writes: BTreeMap<Vec<u8>, Op>,
        xid: u32,
        snapshot: u64,
        sync: bool,
        probed_base_run_id: u64,
    ) -> io::Result<Result<Option<(u64, u64)>, Conflict>> {
        self.stage_inner(writes, xid, snapshot, sync, Some(probed_base_run_id))
    }

    fn stage_inner(
        &mut self,
        writes: BTreeMap<Vec<u8>, Op>,
        xid: u32,
        snapshot: u64,
        sync: bool,
        probed: Option<u64>,
    ) -> io::Result<Result<Option<(u64, u64)>, Conflict>> {
        self.check_fenced()?;
        self.check_lease()?;
        if writes.is_empty() {
            return Ok(Ok(None));
        }
        let probe_runs = probed != Some(self.base_run_id);
        if let Some(c) = self.find_conflict(&writes, snapshot, probe_runs)? {
            return Ok(Err(c));
        }
        let seq = self.next_seq;
        let entries: Vec<Entry> =
            writes.into_iter().map(|(key, op)| Entry { key, op }).collect();
        let commit = Commit { seq, base_run_id: self.base_run_id, xid, epoch: self.epoch, entries };
        // Encoded once, here: the fingerprint a discard marker carries is of
        // these bytes, and the flight sends them as they are.
        let bytes = commit.encode_checked()?;
        let crc = Commit::fingerprint(&bytes);
        self.next_seq += 1;
        let ticket = self.next_ticket;
        self.next_ticket += 1;
        self.staged.insert(seq, Staged { commit, crc });
        self.unwritten.push(Pending { ticket, seq, sync, snapshot, xid, crc, bytes });
        Ok(Ok(Some((ticket, seq))))
    }

    /// Everything queued, as one object, while fewer than `MAX_IN_FLIGHT`
    /// are being written. The caller does the PUT -- outside whatever lock
    /// guards this `Db`, so readers are not held up by the network -- and
    /// reports back with `flight_written`, `flight_lost` or `flight_failed`.
    /// Nothing is handed out once the lease is gone.
    pub fn take_flight(&mut self) -> Option<Flight> {
        if self.unwritten.is_empty() || self.in_flight.len() >= MAX_IN_FLIGHT || self.fenced.is_some() {
            return None;
        }
        self.check_lease().ok()?;
        let members = std::mem::take(&mut self.unwritten);
        let first = members[0].seq;
        let key = commit::key_for(first);
        let bytes = if members.len() == 1 {
            members[0].bytes.clone()
        } else {
            let parts: Vec<&[u8]> = members.iter().map(|m| m.bytes.as_slice()).collect();
            commit::encode_batch_members(&parts)
        };
        self.in_flight.insert(first, InFlight { key: key.clone(), bytes: bytes.clone(), members, landed: false });
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
    ///
    /// Landing is not enough if the lease ran out while the object was on
    /// the wire: a new owner may have taken over and fenced it, and whether
    /// the object counts is then unknown. That fences this process instead
    /// of acknowledging.
    pub fn flight_written(&mut self, first: u64) {
        if self.fenced.is_some() {
            return; // resolved when the fence went up
        }
        if self.check_lease().is_err() {
            return;
        }
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
                // A member discarded while in flight has no holder waiting.
                if !self.discarded.contains_key(&m.seq) {
                    self.outcomes.insert(m.ticket, Outcome::Durable(m.seq));
                }
            }
        }
    }

    /// The PUT could not be made and will not be retried.
    ///
    /// Whether it landed is not something a failed response can say, so the
    /// bucket is asked. If the object is there, the flight is written. If it
    /// is not, every member's number gets a discard marker first -- a PUT
    /// the store applies after the client gave up would otherwise become a
    /// commit at the next open -- and then the members learn they failed.
    /// If it cannot be asked, the outcome is unknown and the process fences.
    ///
    /// A member already acknowledged to a client (an asynchronous commit)
    /// cannot be un-acknowledged. That fences the process, and every flight
    /// behind the hole with it: those transactions may have read what was
    /// acknowledged, so they are marked dead rather than released.
    pub fn flight_failed(&mut self, first: u64, why: &str) {
        let Some(mut f) = self.in_flight.remove(&first) else { return };
        match self.store.get(&f.key) {
            Ok(Some(b)) if b == f.bytes => {
                f.landed = true;
                self.in_flight.insert(first, f);
                self.flight_written(first);
            }
            Ok(Some(other)) => self.foreign_object_at(f, &other),
            Ok(None) => {
                for m in &f.members {
                    let marker = DiscardMarker {
                        seq: m.seq,
                        xid: m.xid,
                        marker: Marker::Discard { why: Discard::Failed, xid: m.xid, crc: m.crc },
                    };
                    self.must_write_marker(&marker);
                    self.staged.remove(&m.seq);
                }
                let lost: Vec<u64> = f.members.iter().filter(|m| !m.sync).map(|m| m.seq).collect();
                if !lost.is_empty() {
                    let why = format!(
                        "objkv: commit object {} could not be written ({why}) and it carried \
                         commits already acknowledged to clients ({lost:?}); this server can no \
                         longer be trusted with the bucket. Restart it.",
                        f.key
                    );
                    for m in f.members {
                        self.outcomes.insert(m.ticket, Outcome::Fenced(why.clone()));
                    }
                    self.fence(why, true);
                    return;
                }
                for m in f.members {
                    self.outcomes.insert(m.ticket, Outcome::Failed(why.to_string()));
                }
                self.release_landed_prefix();
            }
            Err(e) => {
                let seqs: Vec<u64> = f.members.iter().map(|m| m.seq).collect();
                let why = format!(
                    "objkv: commit object {} could not be written ({why}) and the bucket could not \
                     be asked whether it landed ({e}); the outcome of commits {seqs:?} is unknown \
                     and this server can no longer be trusted with the bucket. Restart it.",
                    f.key
                );
                for m in f.members {
                    self.staged.remove(&m.seq);
                    self.outcomes.insert(m.ticket, Outcome::Fenced(why.clone()));
                }
                self.fence(why, false);
            }
        }
    }

    /// The PUT found the key taken.
    ///
    /// Usually that is our own object, written by an attempt whose response
    /// was lost: compared byte for byte and counted as written. Otherwise
    /// another writer holds the number, which the lease should have made
    /// impossible; see [`Db::foreign_object_at`].
    pub fn flight_lost(&mut self, flight: &Flight) -> io::Result<()> {
        match self.store.get(&flight.key) {
            Ok(Some(b)) if b == flight.bytes => {
                self.flight_written(flight.first);
                Ok(())
            }
            Ok(Some(other)) => {
                if let Some(f) = self.in_flight.remove(&flight.first) {
                    self.foreign_object_at(f, &other);
                }
                Ok(())
            }
            Ok(None) => {
                // Refused as existing, then gone: nothing this design does.
                if let Some(f) = self.in_flight.remove(&flight.first) {
                    self.foreign_object_at(f, &[]);
                }
                Ok(())
            }
            Err(e) => {
                self.flight_failed(flight.first, &e.to_string());
                Err(e)
            }
        }
    }

    /// Another writer's object sits at one of our numbers. If its epoch is
    /// above ours, the lease was lost and the outcome of everything in
    /// flight is unknown. If below, a writer that had already lost the lease
    /// wrote after we took over: its object is stale by the takeover fence
    /// and will be ignored at the next open, but our numbers are burnt and
    /// this design does not renumber, so the process stops either way. In
    /// that second case we are still the owner, and everything we had in
    /// flight is marked dead so that no client told "error" has its
    /// transaction applied by the next open.
    fn foreign_object_at(&mut self, f: InFlight, other: &[u8]) {
        let theirs = commit::decode_members(other).ok().and_then(|m| m.first().map(|(c, _)| c.epoch));
        let why = match theirs {
            Some(e) if e > self.epoch => format!(
                "objkv: commit object {} was written by lease epoch {e}; this server (epoch {}) has \
                 lost the bucket to another writer and stops. The outcome of commits {:?} is unknown.",
                f.key,
                self.epoch,
                f.members.iter().map(|m| m.seq).collect::<Vec<_>>()
            ),
            Some(e) => format!(
                "objkv: commit object {} was written by lease epoch {e} after this server (epoch \
                 {}) took over; that object is stale and will be ignored, but this server's \
                 commits {:?} cannot take their numbers and it stops. Restart it.",
                f.key,
                self.epoch,
                f.members.iter().map(|m| m.seq).collect::<Vec<_>>()
            ),
            None => format!(
                "objkv: commit object {} exists and is not ours nor a readable commit object; \
                 this server stops. Restart it.",
                f.key
            ),
        };
        let still_owner = theirs.is_some_and(|e| e < self.epoch);
        for m in f.members {
            self.staged.remove(&m.seq);
            self.outcomes.insert(m.ticket, Outcome::Fenced(why.clone()));
        }
        self.fence(why, still_owner);
    }

    /// Stops this process writing for good. Every queued and in-flight
    /// member is resolved as fenced. With `mark`, every in-flight object --
    /// landed or not -- gets a discard marker per member, so nothing a
    /// client was told had failed can be applied by the next open; without
    /// it (the lease is gone, so this process may no longer speak for the
    /// bucket) the objects are left to the next owner. The lease is given
    /// back so a restart need not wait for it.
    fn fence(&mut self, why: String, mark: bool) {
        eprintln!("{why}");
        if self.fenced.is_none() {
            self.fenced = Some(why.clone());
        }
        for p in std::mem::take(&mut self.unwritten) {
            self.staged.remove(&p.seq);
            self.outcomes.insert(p.ticket, Outcome::Fenced(why.clone()));
        }
        for (_, f) in std::mem::take(&mut self.in_flight) {
            for m in f.members {
                if mark {
                    let marker = DiscardMarker {
                        seq: m.seq,
                        xid: m.xid,
                        marker: Marker::Discard { why: Discard::Fenced, xid: m.xid, crc: m.crc },
                    };
                    self.must_write_marker(&marker);
                }
                self.staged.remove(&m.seq);
                self.outcomes.insert(m.ticket, Outcome::Fenced(why.clone()));
            }
        }
        if let Some(l) = &self.lease {
            l.stop_heartbeat();
            let _ = l.release();
        }
    }

    /// A marker whose absence would let the next open apply a transaction
    /// somebody was told had failed. It is written or the process panics,
    /// as with [`Db::discard_failed`].
    fn must_write_marker(&mut self, m: &DiscardMarker) {
        if let Err(e) = m.write(&self.store) {
            let why = format!(
                "objkv PANIC: commit {} (xid {}) must not be applied, and its discard marker could \
                 not be written ({e}). This server is fenced; {}.",
                m.seq,
                m.xid,
                m.by_hand()
            );
            self.fenced.get_or_insert(why.clone());
            if let Some(l) = &self.lease {
                l.stop_heartbeat();
            }
            panic!("{why}");
        }
        self.discarded.insert(m.seq, m.marker);
    }

    /// The outcome for `ticket`, once there is one. Taken, not peeked: each
    /// ticket has exactly one holder.
    pub fn take_outcome(&mut self, ticket: u64) -> Option<Outcome> {
        self.outcomes.remove(&ticket)
    }

    fn find_conflict(
        &self,
        writes: &BTreeMap<Vec<u8>, Op>,
        snapshot: u64,
        probe_runs: bool,
    ) -> io::Result<Option<Conflict>> {
        for c in self.commits.iter().map(|c| &**c).chain(self.staged.values().map(|s| &s.commit)) {
            if c.seq <= snapshot {
                continue;
            }
            for e in &c.entries {
                if writes.contains_key(&e.key) {
                    return Ok(Some(Conflict { key: e.key.clone(), by: c.seq }));
                }
            }
        }
        if probe_runs {
            return run_conflict(&self.runs, self.base_run_id, writes, snapshot);
        }
        Ok(None)
    }

    pub fn needs_compaction(&self) -> bool {
        self.commits.len() >= COMPACT_AFTER_COMMITS
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

    /// The new run is durable: swap it in. Memory only -- the run is opened
    /// from the bytes just written, not read back -- so it can run under
    /// the lock. Returns what the sweep may now delete, for `execute_sweep`
    /// to do outside it.
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
        let new_run = Arc::new(Run::open_from_bytes(
            ObjectRange {
                store: Arc::clone(&self.store),
                key: folded.key.clone(),
                size: folded.bytes.len() as u64,
            },
            &folded.bytes,
        )?);
        if merge {
            let replaced = std::mem::replace(&mut self.runs, vec![new_run]);
            for r in replaced {
                let k = r.source().key.clone();
                self.retired.push((r, k));
            }
        } else {
            self.runs.insert(0, new_run);
        }
        self.commits.retain(|c| c.seq > new_id);
        self.base_run_id = new_id;

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
        // Markers for numbers below the run that no object holds -- gaps
        // nailed shut, flights that never landed -- have done their work:
        // anything landing there now is skipped as folded anyway.
        let orphan_markers: Vec<u64> = self
            .discarded
            .range(..=self.base_run_id)
            .map(|(&s, _)| s)
            .filter(|&s| !self.objects.range(..=s).next_back().is_some_and(|(_, &last)| s <= last))
            .collect();

        Ok(SweepPlan {
            run_key: folded.key.clone(),
            expected: folded.kept,
            retired: gone,
            done,
            markers,
            orphan_markers,
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

    /// [`claim_oid_block`] through the `Db`: one LIST, one PUT and a few
    /// DELETEs under whatever lock the caller holds. The free function needs
    /// no lock at all, since the bucket is the arbiter.
    pub fn claim_oid_block(&mut self, want: u32, prefetch: u32) -> io::Result<u32> {
        claim_oid_block(&self.store, want, prefetch)
    }

    pub fn commit_backlog(&self) -> usize {
        self.commits.len()
    }
    pub fn run_count(&self) -> usize {
        self.runs.len()
    }
}

impl Drop for Db {
    /// A dropped writer gives its lease back, so the next open in a test or
    /// a tool need not wait for it to expire. The server never drops its
    /// `Db`; it calls [`Db::release_lease`] on the way out.
    fn drop(&mut self) {
        if let Some(l) = &self.lease {
            l.stop_heartbeat();
            let _ = l.release();
        }
    }
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
pub fn claim_oid_block(store: &Arc<dyn Store>, want: u32, prefetch: u32) -> io::Result<u32> {
    let mut floor = 0u32;
    let mut old: Vec<String> = Vec::new();
    for info in store.list("oidnext/")? {
        if let Some(v) = info.key.rsplit('/').next() {
            if let Ok(n) = u32::from_str_radix(v, 16) {
                floor = floor.max(n);
            }
        }
        old.push(info.key);
    }
    let start = want.max(floor);
    let end = start.saturating_add(prefetch);
    let new_key = oid_block_key(end);
    // An existing boundary at exactly `end` promises the same thing.
    store.put_if_absent(&new_key, b"")?;
    // Only after the new boundary is durable, and never fatal: a leftover
    // is one extra key and one extra comparison next time. The boundary
    // just relied on is not a leftover.
    for key in old.into_iter().filter(|k| *k != new_key) {
        let _ = store.delete(&key);
    }
    Ok(start)
}

/// Whether an object with this epoch and number was written by a writer that
/// had already lost the lease: a later epoch's fence sits at or below it.
fn is_stale(fences: &[(u64, u64)], epoch: u64, seq: u64) -> bool {
    fences.iter().any(|&(e, first)| e > epoch && first <= seq)
}

/// The newest version of each written key in the runs, if it is newer than
/// the snapshot: the part of validation that reads the network.
///
/// Compaction empties `commits` of everything it folded, so a snapshot from
/// below the base run leaves a gap the in-memory check cannot see: the
/// conflicting version now exists only inside a run. Snapshots at or above
/// the base run need none of this, which is the ordinary case.
fn run_conflict(
    runs: &[Arc<Run<ObjectRange>>],
    base_run_id: u64,
    writes: &BTreeMap<Vec<u8>, Op>,
    snapshot: u64,
) -> io::Result<Option<Conflict>> {
    if snapshot >= base_run_id {
        return Ok(None);
    }
    for key in writes.keys() {
        for r in runs {
            if let Some(seq) = r.seq_at(key, LATEST)? {
                if seq > snapshot {
                    return Ok(Some(Conflict { key: key.clone(), by: seq }));
                }
            }
        }
    }
    Ok(None)
}

/// What a read needs, taken from the `Db` under its lock and used without it.
/// See [`Db::view`].
#[derive(Clone)]
pub struct View {
    /// Sorted runs, newest first.
    runs: Vec<Arc<Run<ObjectRange>>>,
    /// Committed but not yet compacted, oldest first.
    commits: Vec<Arc<Commit>>,
    collected_through: u64,
    base_run_id: u64,
}

impl View {
    /// The base run this view's commits sit on. [`Db::stage_commit_checked`]
    /// takes it to know whether a [`View::find_run_conflict`] still stands.
    pub fn base_run_id(&self) -> u64 {
        self.base_run_id
    }

    /// The run half of first-committer-wins validation, done here so the
    /// network reads it makes happen with no lock held. The in-memory half
    /// is done by `stage_commit_checked` under the lock.
    pub fn find_run_conflict(
        &self,
        writes: &BTreeMap<Vec<u8>, Op>,
        snapshot: u64,
    ) -> io::Result<Option<Conflict>> {
        run_conflict(&self.runs, self.base_run_id, writes, snapshot)
    }

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

    /// `row_key` as it stood at `snapshot`, newest layer first.
    pub fn get_at(&self, row_key: &[u8], snapshot: u64) -> io::Result<Option<Vec<u8>>> {
        self.readable_at(snapshot)?;
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
        for c in self.commits.iter().rev() {
            if c.seq > snapshot {
                continue;
            }
            if let Some(op) = c.lookup(row_key) {
                return Ok(resolve(op).map(|v| (v, c.seq)));
            }
        }
        for r in &self.runs {
            if let Some((vk, op)) = r.locate_at(row_key, snapshot)? {
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
            if let Some((vk, op)) = r.locate_at(row_key, snapshot)? {
                return Ok(Some((key::seq_of(&vk).unwrap_or(0), op)));
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

    /// Every live key in `[lo, hi)`, newest version of each. Materialises the
    /// whole range, which suits the sequential scans the table AM needs
    /// today; a streaming merge of run blocks is the version that would
    /// survive real data volumes.
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

    /// Every live key with `prefix`, oldest layer first so newer versions
    /// win; tombstones are applied and dropped.
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
    done: Vec<(u64, u64)>,
    markers: Vec<u64>,
    orphan_markers: Vec<u64>,
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
    for seq in plan.orphan_markers {
        if store.delete(&discard_key(seq)).is_ok() {
            result.markers.push(seq);
        }
    }
    match store.list("horizon/") {
        Ok(infos) => {
            for info in infos {
                if parse_id(&info.key).is_ok_and(|id| id < plan.collected_through) {
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

/// `commit/00000000000000ff` -> 255, `run/0000000000000010.d` -> 16. A key
/// that does not parse is an error naming it, not a zero: a stray object
/// under one of these prefixes is somebody's mistake to look at, not
/// something to delete or skip quietly.
fn parse_id(key: &str) -> io::Result<u64> {
    key.rsplit('/')
        .next()
        .map(|h| h.strip_suffix(run::DELTA_SUFFIX).unwrap_or(h))
        .and_then(|h| u64::from_str_radix(h, 16).ok())
        .ok_or_else(|| io::Error::other(format!("objkv: unexpected object `{key}` in the bucket")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::testing::FakeClock;
    use crate::lease::{SKEW_MARGIN_MS, TTL_MS};
    use crate::store::MemStore;
    use std::sync::Mutex;

    fn mem() -> (Arc<MemStore>, Arc<dyn Store>) {
        let m = Arc::new(MemStore::new());
        let s = Arc::clone(&m) as Arc<dyn Store>;
        (m, s)
    }

    fn db() -> (Arc<dyn Store>, Db) {
        let (_, s) = mem();
        let d = Db::open(Arc::clone(&s)).unwrap();
        (s, d)
    }

    fn one(k: &[u8], v: &[u8]) -> BTreeMap<Vec<u8>, Op> {
        let mut w = BTreeMap::new();
        w.insert(k.to_vec(), Op::Put(v.to_vec()));
        w
    }

    fn del(k: &[u8]) -> BTreeMap<Vec<u8>, Op> {
        let mut w = BTreeMap::new();
        w.insert(k.to_vec(), Op::Delete);
        w
    }

    fn many(pairs: &[(&str, &str)]) -> BTreeMap<Vec<u8>, Op> {
        pairs.iter().map(|(k, v)| (k.as_bytes().to_vec(), Op::Put(v.as_bytes().to_vec()))).collect()
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

    /// Stages against `snap`, writes, waits, confirms: one whole transaction.
    fn commit_at(d: &mut Db, s: &Arc<dyn Store>, w: BTreeMap<Vec<u8>, Op>, snap: u64) -> Result<u64, Conflict> {
        let (t, seq) = d.stage_commit(w, 0, snap, true).unwrap()?.expect("non-empty");
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
        commit_at(d, s, w, LATEST).unwrap()
    }

    /// One fold, the way the compactor thread does it. `None` when nothing
    /// is settled enough to fold.
    fn fold(d: &mut Db, s: &Arc<dyn Store>, horizon: u64, tables: &BTreeMap<u32, u32>) -> Option<u64> {
        let plan = d.fold_plan()?;
        let id = plan.new_id;
        let folded = build_fold(&plan, horizon, tables).unwrap();
        put_fold(s, &folded).unwrap();
        let sweep = d.apply_fold(plan, &folded, horizon).unwrap();
        let result = execute_sweep(s, sweep);
        d.sweep_done(result);
        Some(id)
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

    /// Writes `n` successive values to one row, each its own commit.
    fn history(n: u64) -> Arc<dyn Store> {
        let (s, mut d) = db();
        for i in 1..=n {
            commit(&mut d, &s, one(b"row", format!("v{i}").as_bytes()));
        }
        s
    }

    /// Wraps a store and injects failures: PUTs under a prefix fail, or one
    /// key is unreadable.
    struct Flaky {
        inner: Arc<MemStore>,
        fail_put_prefix: Mutex<Option<String>>,
        unreadable: Mutex<Option<String>>,
    }

    impl Flaky {
        fn new() -> (Arc<Flaky>, Arc<dyn Store>) {
            let f = Arc::new(Flaky {
                inner: Arc::new(MemStore::new()),
                fail_put_prefix: Mutex::new(None),
                unreadable: Mutex::new(None),
            });
            let s = Arc::clone(&f) as Arc<dyn Store>;
            (f, s)
        }
        fn fail_puts_under(&self, prefix: Option<&str>) {
            *self.fail_put_prefix.lock().unwrap() = prefix.map(str::to_string);
        }
        fn unreadable(&self, key: Option<&str>) {
            *self.unreadable.lock().unwrap() = key.map(str::to_string);
        }
    }

    impl Store for Flaky {
        fn put_if_absent(&self, key: &str, body: &[u8]) -> io::Result<PutOutcome> {
            if self.fail_put_prefix.lock().unwrap().as_deref().is_some_and(|p| key.starts_with(p)) {
                return Err(io::Error::other("injected: store unavailable"));
            }
            self.inner.put_if_absent(key, body)
        }
        fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
            if self.unreadable.lock().unwrap().as_deref() == Some(key) {
                return Err(io::Error::other("injected: object unreadable"));
            }
            self.inner.get(key)
        }
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

    // ---- the layered read path -------------------------------------------

    #[test]
    fn commit_then_read_through_the_commit_layer() {
        let (s, mut d) = db();
        assert_eq!(commit(&mut d, &s, one(b"a", b"1")), 1);
        assert_eq!(d.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(d.get(b"b").unwrap(), None);
    }

    #[test]
    fn newest_layer_wins() {
        let (s, mut d) = db();
        commit(&mut d, &s, one(b"a", b"old"));
        commit(&mut d, &s, one(b"a", b"new"));
        assert_eq!(d.get(b"a").unwrap(), Some(b"new".to_vec()));
        commit(&mut d, &s, del(b"a"));
        assert_eq!(d.get(b"a").unwrap(), None);
    }

    #[test]
    fn state_survives_reopen() {
        let (_, s) = mem();
        {
            let mut d = Db::open(Arc::clone(&s)).unwrap();
            for i in 0..50 {
                commit(&mut d, &s, one(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes()));
            }
        }
        let d = Db::open(Arc::clone(&s)).unwrap();
        assert_eq!(d.commit_backlog(), 50);
        assert_eq!(d.get(b"k0007").unwrap(), Some(b"v7".to_vec()));
        assert_eq!(d.get(b"k9999").unwrap(), None);
        assert_eq!(d.current_seq(), 50);
    }

    #[test]
    fn compaction_folds_commits_into_a_run() {
        let (s, mut d) = db();
        for i in 0..200 {
            commit(&mut d, &s, one(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes()));
        }
        assert!(d.needs_compaction());
        fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
        assert_eq!(d.commit_backlog(), 0);
        assert_eq!(d.run_count(), 1);
        for i in 0..200 {
            let want = format!("v{i}").into_bytes();
            assert_eq!(d.get(format!("k{i:04}").as_bytes()).unwrap(), Some(want));
        }
    }

    #[test]
    fn compaction_survives_reopen_and_drops_the_walk() {
        let (_, s) = mem();
        {
            let mut d = Db::open(Arc::clone(&s)).unwrap();
            for i in 0..150 {
                commit(&mut d, &s, one(format!("k{i:04}").as_bytes(), b"v"));
            }
            fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
        }
        let d = Db::open(Arc::clone(&s)).unwrap();
        assert_eq!(d.commit_backlog(), 0, "commits below the base run are skipped");
        assert_eq!(d.get(b"k0100").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn tombstones_survive_compaction() {
        let (s, mut d) = db();
        commit(&mut d, &s, one(b"gone", b"x"));
        commit(&mut d, &s, del(b"gone"));
        fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
        assert_eq!(d.get(b"gone").unwrap(), None);
    }

    #[test]
    fn scan_prefix_merges_layers_and_drops_tombstones() {
        let (s, mut d) = db();
        let mut w = BTreeMap::new();
        for i in 0..10 {
            w.insert(format!("t1/{i:03}").into_bytes(), Op::Put(b"old".to_vec()));
            w.insert(format!("t2/{i:03}").into_bytes(), Op::Put(b"other".to_vec()));
        }
        commit(&mut d, &s, w);
        fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
        let mut w = one(b"t1/003", b"new");
        w.insert(b"t1/005".to_vec(), Op::Delete);
        commit(&mut d, &s, w);

        let got = d.scan_prefix(b"t1/").unwrap();
        assert_eq!(got.len(), 9, "one key was deleted");
        assert!(got.iter().all(|(k, _)| k.starts_with(b"t1/")), "prefix respected");
        assert!(got.windows(2).all(|w| w[0].0 < w[1].0), "sorted");
        let find = |k: &str| got.iter().find(|(a, _)| a == k.as_bytes()).map(|(_, v)| v.clone());
        assert_eq!(find("t1/003"), Some(b"new".to_vec()));
        assert_eq!(find("t1/005"), None);
        assert_eq!(find("t1/000"), Some(b"old".to_vec()));
    }

    #[test]
    fn a_batch_commit_writes_exactly_one_object() {
        let (m, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let mut writes = BTreeMap::new();
        for i in 0..1_000 {
            writes.insert(format!("k{i:04}").into_bytes(), Op::Put(b"v".to_vec()));
        }
        assert_eq!(commit(&mut d, &s, writes), 1);
        assert_eq!(m.list("commit/").unwrap().len(), 1, "1000 rows must cost one PUT, not 1000");
        drop(d);
        let reopened = Db::open(Arc::clone(&s)).unwrap();
        assert_eq!(reopened.get(b"k0000").unwrap(), Some(b"v".to_vec()));
        assert_eq!(reopened.get(b"k0999").unwrap(), Some(b"v".to_vec()));
    }

    // ---- collection of index entries -------------------------------------

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
        let (s, mut d) = db();

        // Row 1 with its entry, then "updated": row 1 tombstoned, row 2 written.
        let mut w = one(&row(DB, TBL, 1), b"alice-v1");
        w.insert(entry(DB, IDX, "alice", 1, false), Op::Put(vec![]));
        commit(&mut d, &s, w);
        let mut w = del(&row(DB, TBL, 1));
        w.insert(row(DB, TBL, 2), Op::Put(b"alice-v2".to_vec()));
        w.insert(entry(DB, IDX, "alice", 2, false), Op::Put(vec![]));
        commit(&mut d, &s, w);
        commit(&mut d, &s, one(b"unrelated", b"x"));

        let tables = BTreeMap::from([(IDX, TBL)]);
        // Past the tombstone, so row 1 goes and its entry becomes collectable.
        fold(&mut d, &s, 3, &tables).unwrap();

        let keys: Vec<Vec<u8>> = d.runs[0]
            .scan()
            .unwrap()
            .into_iter()
            .filter_map(|(k, _)| key::row_of(&k).map(<[u8]>::to_vec))
            .collect();
        assert!(!keys.contains(&entry(DB, IDX, "alice", 1, false)), "the entry for the vanished row must go");
        assert!(keys.contains(&entry(DB, IDX, "alice", 2, false)), "the entry for the live row must stay");
        assert_eq!(d.get(&row(DB, TBL, 2)).unwrap(), Some(b"alice-v2".to_vec()));
    }

    #[test]
    fn a_unique_entry_is_judged_by_the_row_id_in_its_payload() {
        const DB: u32 = 5;
        const TBL: u32 = 100;
        const IDX: u32 = 200;
        let (s, mut d) = db();
        let mut w = one(&row(DB, TBL, 7), b"bob");
        w.insert(entry(DB, IDX, "bob", 7, true), Op::Put(format!("{:016x}", 7u64).into_bytes()));
        commit(&mut d, &s, w);
        commit(&mut d, &s, one(b"unrelated", b"x"));
        fold(&mut d, &s, 2, &BTreeMap::from([(IDX, TBL)])).unwrap();
        assert!(
            d.get(&entry(DB, IDX, "bob", 7, true)).unwrap().is_some(),
            "a unique entry whose row is alive must survive"
        );
    }

    #[test]
    fn an_index_with_no_known_table_is_left_alone() {
        const DB: u32 = 5;
        const IDX: u32 = 200;
        let (s, mut d) = db();
        commit(&mut d, &s, one(&entry(DB, IDX, "orphan", 1, false), b""));
        commit(&mut d, &s, one(b"unrelated", b"x"));
        fold(&mut d, &s, 2, &BTreeMap::new()).unwrap();
        assert!(d.get(&entry(DB, IDX, "orphan", 1, false)).unwrap().is_some());
    }

    // ---- folds, sweeps and what they may not touch ------------------------

    #[test]
    fn a_run_never_claims_a_commit_it_did_not_fold() {
        // Compaction runs during a transaction's own pre-commit, so that
        // transaction's object is in the bucket and not foldable yet.
        // Numbering the run above it drops it on the next restart.
        let (_, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        for i in 1..=3 {
            commit(&mut d, &s, one(b"settled", format!("v{i}").as_bytes()));
        }
        // Durable, fate unrecorded -- where the AM triggers this.
        let (_, staged) = d.stage_commit(one(b"inflight", b"live"), 77, LATEST, true).unwrap().unwrap().unwrap();
        fly(&mut d, &s).unwrap();

        let run_id = fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
        assert!(run_id < staged, "the run must not number itself above an unfolded commit ({run_id} vs {staged})");

        d.mark_confirmed(staged);
        drop(d);
        let d = Db::open(Arc::clone(&s)).unwrap();
        assert_eq!(d.get(b"inflight").unwrap(), Some(b"live".to_vec()), "the in-flight transaction survived the restart");
        assert_eq!(d.get(b"settled").unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn compaction_deletes_what_it_replaced() {
        let (m, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        for i in 1..=10 {
            commit(&mut d, &s, one(b"row", format!("v{i}").as_bytes()));
        }
        assert_eq!(m.list("commit/").unwrap().len(), 10);

        fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
        assert!(m.list("commit/").unwrap().is_empty(), "folded commits are gone");
        assert_eq!(m.list("run/").unwrap().len(), 1, "one run, not a pile");
        assert_eq!(d.get(b"row").unwrap(), Some(b"v10".to_vec()));

        // The next folds are deltas beside it, until there are MAX_RUNS runs;
        // the fold after that merges them into one and the rest are deleted.
        for i in 11..11 + MAX_RUNS {
            commit(&mut d, &s, one(b"row", format!("v{i}").as_bytes()));
            fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
            let runs = m.list("run/").unwrap().len();
            let expect = if i == 10 + MAX_RUNS { 1 } else { i - 9 };
            assert_eq!(runs, expect, "after fold {i}");
            assert_eq!(
                Db::open_with(Arc::clone(&s)).unwrap().get(b"row").unwrap(),
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
        let (f, s) = Flaky::new();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        for i in 1..=5 {
            commit(&mut d, &s, one(b"row", format!("v{i}").as_bytes()));
        }
        // The run compaction is about to write takes the number of the
        // newest commit it folds, which is the fifth.
        f.unreadable(Some(&run::key_for(5)));
        fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
        assert_eq!(f.inner.list("commit/").unwrap().len(), 5, "the commits must survive a replacement that could not be verified");

        f.unreadable(None);
        drop(d);
        let d = Db::open(Arc::clone(&s)).unwrap();
        assert_eq!(d.get(b"row").unwrap(), Some(b"v5".to_vec()));
    }

    #[test]
    fn collection_keeps_one_version_from_below_the_horizon() {
        // Ten writes, collect at or below 6. The four above stay; from below
        // only the newest survives, which is what a read at 6 resolves to.
        let s = history(10);
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        fold(&mut d, &s, 6, &BTreeMap::new()).unwrap();
        assert_eq!(versions_of(&d, b"row"), vec![6, 7, 8, 9, 10]);
        assert_eq!(d.get(b"row").unwrap(), Some(b"v10".to_vec()));
        assert_eq!(d.get_at(b"row", 6).unwrap(), Some(b"v6".to_vec()));
        assert_eq!(d.get_at(b"row", 8).unwrap(), Some(b"v8".to_vec()));
    }

    #[test]
    fn a_row_deleted_below_the_horizon_goes_entirely() {
        let (s, mut d) = db();
        commit(&mut d, &s, one(b"gone", b"here"));
        commit(&mut d, &s, del(b"gone"));
        commit(&mut d, &s, one(b"kept", b"x"));
        fold(&mut d, &s, 3, &BTreeMap::new()).unwrap();
        assert!(versions_of(&d, b"gone").is_empty(), "row and tombstone both go");
        assert_eq!(d.get(b"gone").unwrap(), None);
        assert_eq!(d.get(b"kept").unwrap(), Some(b"x".to_vec()));
    }

    #[test]
    fn reads_below_the_horizon_are_refused_rather_than_guessed() {
        let s = history(10);
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        fold(&mut d, &s, 6, &BTreeMap::new()).unwrap();
        let err = d.get_at(b"row", 3).unwrap_err().to_string();
        assert!(err.contains("has been collected"), "got: {err}");
        assert!(err.contains("oldest readable point is 6"), "got: {err}");
        assert!(d.get_at(b"row", 6).is_ok(), "the horizon itself is readable");
        assert!(d.get(b"row").is_ok(), "and so is the present");
        assert!(d.scan_prefix_at(b"", 3).is_err(), "a scan below the horizon must refuse, not return a short answer");
        assert!(d.scan_prefix_at(b"", 6).is_ok());
    }

    #[test]
    fn the_horizon_survives_a_reopen() {
        let s = history(10);
        {
            let mut d = Db::open(Arc::clone(&s)).unwrap();
            fold(&mut d, &s, 6, &BTreeMap::new()).unwrap();
        }
        let d = Db::open(Arc::clone(&s)).unwrap();
        assert_eq!(d.collected_through(), 6);
        assert!(d.get_at(b"row", 3).is_err(), "a fresh process refuses it too");
        assert_eq!(d.get(b"row").unwrap(), Some(b"v10".to_vec()));
    }

    #[test]
    fn a_horizon_of_zero_collects_nothing() {
        let s = history(10);
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
        assert_eq!(versions_of(&d, b"row").len(), 10);
        assert_eq!(d.collected_through(), 0);
        assert!(d.get_at(b"row", 1).is_ok(), "all of history is still readable");
    }

    // ---- staging, flights and outcomes ------------------------------------

    #[test]
    fn queued_commits_share_one_object_and_each_keeps_its_number() {
        let (m, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let snap = d.current_seq();
        let mut tickets = Vec::new();
        for i in 0..3u8 {
            let (t, seq) = d.stage_commit(one(&[b'r', i], b"v"), i as u32, snap, true).unwrap().unwrap().unwrap();
            assert_eq!(seq, 1 + i as u64);
            tickets.push((t, seq));
        }
        assert!(d.take_outcome(tickets[0].0).is_none(), "nothing is durable yet");
        assert_eq!(m.list("commit/").unwrap().len(), 0);

        let f = fly(&mut d, &s).unwrap();
        assert_eq!(m.list("commit/").unwrap().len(), 1, "one PUT for three commits");
        assert_eq!(f.key, commit::key_for(1));
        for (t, seq) in &tickets {
            assert!(matches!(d.take_outcome(*t), Some(Outcome::Durable(x)) if x == *seq));
            d.mark_confirmed(*seq);
        }
        assert!(d.take_flight().is_none(), "nothing left to write");

        let again = Db::open_with(Arc::clone(&s)).unwrap();
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
    fn a_commit_discarded_before_its_write_never_lands_and_the_next_open_nails_the_gap_shut() {
        let (m, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let (_, gone) = d.stage_commit(one(b"a", b"1"), 1, 0, false).unwrap().unwrap().unwrap();
        let (t, kept) = d.stage_commit(one(b"b", b"2"), 2, 0, true).unwrap().unwrap().unwrap();
        d.discard_staged(gone, Discard::Aborted);
        fly(&mut d, &s).unwrap();
        assert!(matches!(d.take_outcome(t), Some(Outcome::Durable(x)) if x == kept));
        assert!(m.list("resolve/").unwrap().is_empty(), "nothing was written under it, nothing to explain");
        d.mark_confirmed(kept);
        drop(d);

        let again = Db::open(Arc::clone(&s)).unwrap();
        assert_eq!(again.get(b"a").unwrap(), None);
        assert_eq!(again.get(b"b").unwrap(), Some(b"2".to_vec()));
        // The gap below the numbers in use is a place a stale writer's late
        // object could land, so the open says nothing there is ever valid.
        assert_eq!(again.discarded.get(&gone), Some(&Marker::Hole { epoch: again.epoch() }));
        let body = m.get(&discard_key(gone)).unwrap().unwrap();
        assert!(body.starts_with(b"hole:"), "{:?}", String::from_utf8_lossy(&body));
    }

    #[test]
    fn a_batch_straddling_the_run_boundary_is_kept_whole() {
        let (m, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let (_, first) = d.stage_commit(one(b"a", b"1"), 1, 0, true).unwrap().unwrap().unwrap();
        let (_, second) = d.stage_commit(one(b"b", b"2"), 2, 0, true).unwrap().unwrap().unwrap();
        let f = fly(&mut d, &s).unwrap();
        d.mark_confirmed(first);
        // The second is durable but its transaction has not committed, so the
        // fold stops below it -- and the object holding both must survive.
        fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
        assert_eq!(d.base_run_id, first);
        assert!(m.get(&f.key).unwrap().is_some(), "the batch still holds a live commit");

        d.mark_confirmed(second);
        let again = Db::open_with(Arc::clone(&s)).unwrap();
        assert_eq!(again.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(again.get(b"b").unwrap(), Some(b"2".to_vec()));

        fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
        assert!(m.get(&f.key).unwrap().is_none(), "folded whole, so collected");
    }

    #[test]
    fn our_own_object_written_by_a_lost_response_counts_as_written() {
        let (_, s) = mem();
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
    fn compaction_stops_below_an_unwritten_commit() {
        let (_, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        commit(&mut d, &s, one(b"old", b"v"));
        let (_, seq) = d.stage_commit(one(b"new", b"v"), 1, 0, false).unwrap().unwrap().unwrap();
        d.mark_confirmed(seq); // acknowledged, visible, not yet in the bucket
        assert_eq!(d.get(b"new").unwrap(), Some(b"v".to_vec()));

        fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
        assert!(d.base_run_id < seq, "an unwritten commit must not be folded");
        assert!(d.commits.iter().any(|c| c.seq == seq));

        fly(&mut d, &s).unwrap();
        fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
        assert_eq!(d.base_run_id, seq);
        let again = Db::open_with(Arc::clone(&s)).unwrap();
        assert_eq!(again.get(b"new").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn a_refused_or_aborted_commit_records_why_it_died_and_which_object() {
        for why in [Discard::Refused, Discard::Aborted] {
            let (m, s) = mem();
            let mut d = Db::open(Arc::clone(&s)).unwrap();
            let (t, seq) = d.stage_commit(one(b"row", b"v"), 9, LATEST, true).unwrap().unwrap().unwrap();
            let f = fly(&mut d, &s).unwrap();
            assert!(matches!(d.take_outcome(t), Some(Outcome::Durable(_))));
            d.discard_staged(seq, why);

            let body = m.get(&discard_key(seq)).unwrap().unwrap();
            let crc = Commit::fingerprint(&f.bytes);
            assert_eq!(body, format!("discard:{}\nxid:9\ncrc:{crc:08x}\n", why.tag()).into_bytes());
            assert_eq!(d.get(b"row").unwrap(), None);
            drop(d);

            let d = Db::open(Arc::clone(&s)).unwrap();
            assert_eq!(d.get(b"row").unwrap(), None);
            assert_eq!(d.discarded.get(&seq), Some(&Marker::Discard { why, xid: 9, crc }), "the reason survives a reopen");
            assert_eq!(m.list("commit/").unwrap().len(), 1, "discarding is a decision, not a delete -- the collector frees it");
        }
    }

    #[test]
    fn a_conflicting_version_folded_into_a_run_is_still_a_conflict() {
        // Compaction empties `commits` of everything it folded. A snapshot
        // taken below the new run then has nothing left to validate against,
        // and the earlier update disappears with no serialization error.
        let (s, mut d) = db();
        commit(&mut d, &s, one(b"hot", b"v0"));
        let snap = d.current_seq();

        commit(&mut d, &s, one(b"hot", b"v1"));
        fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
        assert!(d.commits.is_empty(), "the conflicting commit is only in the run now");
        assert!(snap < d.base_run_id);

        let r = d.stage_commit(one(b"hot", b"stale"), 7, snap, true).unwrap();
        assert!(r.is_err(), "the folded version must still refuse the stale write");
        assert_eq!(d.get(b"hot").unwrap(), Some(b"v1".to_vec()), "no lost update");

        // The same check, split so the run probe runs through a view with
        // no lock held: the caller probes, then stages naming the base run
        // it probed against.
        let v = d.view();
        let c = v.find_run_conflict(&one(b"hot", b"stale"), snap).unwrap().expect("the run says so");
        assert_eq!(c.key, b"hot".to_vec());
        assert!(v.find_run_conflict(&one(b"cold", b"x"), snap).unwrap().is_none());
        let r = d.stage_commit_checked(one(b"cold", b"x"), 7, snap, true, v.base_run_id()).unwrap();
        assert!(r.is_ok(), "a probed, clean write is accepted without a second probe");
    }

    #[test]
    fn a_stale_probe_is_redone_when_a_fold_has_happened_since() {
        let (s, mut d) = db();
        commit(&mut d, &s, one(b"hot", b"v0"));
        let snap = d.current_seq();
        let v = d.view();
        assert!(v.find_run_conflict(&one(b"hot", b"stale"), snap).unwrap().is_none(), "nothing folded yet");
        // Between the probe and the stage, the row changes and is folded away.
        commit(&mut d, &s, one(b"hot", b"v1"));
        fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
        let r = d.stage_commit_checked(one(b"hot", b"stale"), 7, snap, true, v.base_run_id()).unwrap();
        assert!(r.is_err(), "the base run moved, so the runs are probed again under the lock");
    }

    #[test]
    fn a_write_to_a_row_someone_else_changed_is_refused() {
        let (s, mut d) = db();
        commit(&mut d, &s, one(b"k", b"original"));
        let snap = d.current_seq();
        commit_at(&mut d, &s, one(b"k", b"from-a"), snap).unwrap();
        let conflict = commit_at(&mut d, &s, one(b"k", b"from-b"), snap).unwrap_err();
        assert_eq!(conflict.key, b"k".to_vec());
        assert_eq!(conflict.by, 2);
        assert_eq!(d.get(b"k").unwrap(), Some(b"from-a".to_vec()));
    }

    #[test]
    fn writes_to_different_rows_do_not_conflict() {
        let (s, mut d) = db();
        commit(&mut d, &s, one(b"seed", b"1"));
        let snap = d.current_seq();
        commit_at(&mut d, &s, one(b"x", b"1"), snap).unwrap();
        assert!(commit_at(&mut d, &s, one(b"y", b"2"), snap).is_ok());
    }

    // ---- the snapshot is the decided prefix (finding #1) --------------------

    #[test]
    fn a_snapshot_never_covers_a_number_that_has_not_decided() {
        // The reviewer's reproduction: T1 stages at 2 and its PUT is in the
        // air; T2 takes a snapshot, reads k, and later writes k against that
        // snapshot. The old snapshot was 2, which T2 could not see and
        // validation then skipped -- a lost update with no error.
        let (s, mut d) = db();
        assert_eq!(commit(&mut d, &s, one(b"k", b"old")), 1);
        let (t1, s2) = d.stage_commit(one(b"k", b"t1"), 1, 1, true).unwrap().unwrap().unwrap();
        assert_eq!(s2, 2);
        let f = d.take_flight().unwrap(); // in the air, lock released

        let snap = d.current_seq();
        assert_eq!(snap, 1, "2 has not decided, so the snapshot stops below it");
        let view = d.view();
        assert_eq!(view.get_at(b"k", snap).unwrap(), Some(b"old".to_vec()));

        s.put_if_absent(&f.key, &f.bytes).unwrap();
        d.flight_written(f.first);
        assert!(matches!(d.take_outcome(t1), Some(Outcome::Durable(2))));
        d.mark_confirmed(2);
        assert_eq!(d.current_seq(), 2, "decided now");

        // Same snapshot, same answer: through the old view and a new one.
        assert_eq!(view.get_at(b"k", snap).unwrap(), Some(b"old".to_vec()));
        assert_eq!(d.view().get_at(b"k", snap).unwrap(), Some(b"old".to_vec()));
        assert_eq!(d.get_at(b"k", 2).unwrap(), Some(b"t1".to_vec()));

        // And a write built on what T2 saw is refused, not landed on top.
        let r = d.stage_commit(one(b"k", b"t2-from-old"), 2, snap, true).unwrap();
        let c = r.expect_err("T1's update must not be lost");
        assert_eq!((c.key.as_slice(), c.by), (&b"k"[..], 2));
        assert_eq!(d.get(b"k").unwrap(), Some(b"t1".to_vec()));
    }

    #[test]
    fn a_later_number_confirmed_first_waits_for_the_earlier_one() {
        let (s, mut d) = db();
        let (_, a) = d.stage_commit(one(b"a", b"1"), 1, LATEST, true).unwrap().unwrap().unwrap();
        let (_, b) = d.stage_commit(one(b"b", b"2"), 2, LATEST, true).unwrap().unwrap().unwrap();
        fly(&mut d, &s).unwrap();
        d.mark_confirmed(b);
        assert_eq!(d.current_seq(), a - 1, "b is confirmed but a is not decided, so b is not visible yet");
        assert_eq!(d.get_at(b"b", d.current_seq()).unwrap(), None);
        d.discard_staged(a, Discard::Aborted);
        assert_eq!(d.current_seq(), b, "a decided (discarded), so b is in the prefix");
        assert_eq!(d.get_at(b"b", d.current_seq()).unwrap(), Some(b"2".to_vec()));
        assert_eq!(d.get_at(b"a", d.current_seq()).unwrap(), None);
    }

    // ---- discard markers (findings #3 and #4) ------------------------------

    #[test]
    fn a_failed_discard_marker_write_fences_the_process_and_panics() {
        let (f, s) = Flaky::new();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let (t, seq) = d.stage_commit(one(b"k", b"aborted-value"), 42, LATEST, true).unwrap().unwrap().unwrap();
        fly(&mut d, &s).unwrap();
        assert!(matches!(d.take_outcome(t), Some(Outcome::Durable(_))));

        f.fail_puts_under(Some("resolve/"));
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            d.discard_staged(seq, Discard::Aborted);
        }));
        let msg = *outcome.unwrap_err().downcast::<String>().unwrap();
        assert!(msg.starts_with("objkv PANIC:"), "{msg}");
        assert!(msg.contains("commit 1 (xid 42)"), "{msg}");
        assert!(msg.contains("resolve/0000000000000001"), "must say what to create by hand: {msg}");
        assert!(d.is_fenced(), "nothing must go on as if the abort held");
        assert!(d.stage_commit(one(b"q", b"1"), 3, LATEST, true).is_err());

        // The split form behaves the same, with the PUT outside the lock.
        let (f, s) = Flaky::new();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let (_, seq) = d.stage_commit(one(b"k", b"v"), 43, LATEST, true).unwrap().unwrap().unwrap();
        fly(&mut d, &s).unwrap();
        let m = d.begin_discard(seq, Discard::Aborted).expect("the object landed, so a marker is owed");
        f.fail_puts_under(Some("resolve/"));
        let err = m.write(&s).unwrap_err();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| d.discard_failed(&m, &err)));
        assert!(outcome.is_err());
        assert!(d.is_fenced());
        f.fail_puts_under(None);
        m.write(&s).unwrap();
        assert!(Db::open_with(Arc::clone(&s)).unwrap().get(b"k").unwrap().is_none(), "the marker by hand holds");
    }

    #[test]
    fn a_marker_for_an_object_that_never_landed_does_not_condemn_the_number_when_it_is_reused() {
        // T1 staged at 2 (the tail), its flight went out, T1 aborted, and
        // the flight then never landed. The marker outlives the number; a
        // restart hands 2 out again.
        let (m, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        commit(&mut d, &s, one(b"seed", b"1"));
        let (_, seq) = d.stage_commit(one(b"k", b"aborted"), 5, LATEST, true).unwrap().unwrap().unwrap();
        assert_eq!(seq, 2);
        let f = d.take_flight().unwrap();
        d.discard_staged(seq, Discard::Aborted);
        assert!(m.get(&discard_key(2)).unwrap().is_some());
        d.flight_failed(f.first, "no route to the bucket"); // never landed
        assert!(!d.is_fenced());
        drop(d);

        let mut d = Db::open(Arc::clone(&s)).unwrap();
        assert_eq!(d.next_seq, 2, "the tail number is handed out again");
        assert!(m.get(&discard_key(2)).unwrap().is_none(), "the tail marker was dropped at open");
        assert_eq!(commit(&mut d, &s, one(b"k", b"second try")), 2);
        drop(d);
        let d = Db::open(Arc::clone(&s)).unwrap();
        assert_eq!(d.get(b"k").unwrap(), Some(b"second try".to_vec()), "the new commit at 2 stands");
    }

    #[test]
    fn a_marker_judges_its_object_by_fingerprint_not_by_number() {
        // Even if a stale marker survived, it names an object by xid and
        // fingerprint; a different object at the same number is not it.
        let (m, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        commit(&mut d, &s, one(b"seed", b"1"));
        let stale = Marker::Discard { why: Discard::Aborted, xid: 77, crc: 0xdead_beef };
        m.put_if_absent(&discard_key(2), &stale.encode()).unwrap();
        // The open sees a marker at the tail and drops it; plant it again
        // after the commit lands to model one that came back somehow.
        assert_eq!(commit(&mut d, &s, one(b"k", b"real")), 2);
        drop(d);
        let _ = m.delete(&discard_key(2));
        m.put_if_absent(&discard_key(2), &stale.encode()).unwrap();
        let d = Db::open(Arc::clone(&s)).unwrap();
        assert_eq!(d.get(b"k").unwrap(), Some(b"real".to_vec()), "a marker for another object does not apply");
        assert!(m.get(&discard_key(2)).unwrap().is_none(), "and the misfit marker is cleared");
    }

    #[test]
    fn a_late_object_at_a_dead_interior_number_is_never_applied() {
        // Flight 2 fails (not landed), flight 3 lands. 2 is interior now;
        // its marker stays, and when 2's PUT finally lands it is dead.
        let (m, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        commit(&mut d, &s, one(b"seed", b"1"));
        let (t2, _) = d.stage_commit(one(b"k", b"late"), 2, LATEST, true).unwrap().unwrap().unwrap();
        let f2 = d.take_flight().unwrap();
        let (t3, s3) = d.stage_commit(one(b"other", b"x"), 3, LATEST, true).unwrap().unwrap().unwrap();
        let f3 = d.take_flight().unwrap();
        s.put_if_absent(&f3.key, &f3.bytes).unwrap();
        d.flight_written(f3.first);
        d.flight_failed(f2.first, "timeout");
        assert!(matches!(d.take_outcome(t2), Some(Outcome::Failed(_))));
        assert!(matches!(d.take_outcome(t3), Some(Outcome::Durable(x)) if x == s3), "released behind a marked hole");
        d.mark_confirmed(s3);
        drop(d);

        // The store applies the PUT it had reported as failed.
        s.put_if_absent(&f2.key, &f2.bytes).unwrap();
        let d = Db::open(Arc::clone(&s)).unwrap();
        assert_eq!(d.get(b"k").unwrap(), None, "the client was told it failed; it stays failed");
        assert_eq!(d.get(b"other").unwrap(), Some(b"x".to_vec()));
        assert!(m.get(&discard_key(2)).unwrap().is_some(), "an interior marker is kept");
    }

    // ---- flight_failed asks the bucket (finding #5) -------------------------

    #[test]
    fn a_failed_put_that_did_land_is_a_commit() {
        let (_, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let (t, seq) = d.stage_commit(one(b"x", b"1"), 1, LATEST, true).unwrap().unwrap().unwrap();
        let f = d.take_flight().unwrap();
        // Applied by the store; every response lost.
        s.put_if_absent(&f.key, &f.bytes).unwrap();
        d.flight_failed(f.first, "timed out three times");
        assert!(matches!(d.take_outcome(t), Some(Outcome::Durable(x)) if x == seq), "the bucket says it landed");
        assert!(!d.is_fenced());
        assert!(s.list("resolve/").unwrap().is_empty(), "no marker for a commit that stands");
    }

    #[test]
    fn a_failed_put_that_did_not_land_marks_its_numbers_before_reporting() {
        let (m, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let (t1, s1) = d.stage_commit(one(b"a", b"1"), 1, LATEST, true).unwrap().unwrap().unwrap();
        let (t2, s2) = d.stage_commit(one(b"b", b"2"), 2, LATEST, true).unwrap().unwrap().unwrap();
        let f = d.take_flight().unwrap();
        d.flight_failed(f.first, "no route to the bucket");
        for (t, seq) in [(t1, s1), (t2, s2)] {
            assert!(matches!(d.take_outcome(t), Some(Outcome::Failed(_))));
            let body = m.get(&discard_key(seq)).unwrap().expect("marker written");
            assert!(body.starts_with(b"discard:failed\n"), "{:?}", String::from_utf8_lossy(&body));
        }
        assert!(!d.is_fenced(), "nobody had been told about either");
        assert_eq!(d.current_seq(), 2, "both decided (dead), so the prefix moves on");
    }

    #[test]
    fn a_failed_flight_releases_the_landed_ones_behind_it() {
        let (_, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let (t1, _) = d.stage_commit(one(b"a", b"1"), 0, LATEST, true).unwrap().unwrap().unwrap();
        let f1 = d.take_flight().unwrap();
        let (t2, _) = d.stage_commit(one(b"b", b"2"), 0, LATEST, true).unwrap().unwrap().unwrap();
        let f2 = d.take_flight().unwrap();
        s.put_if_absent(&f2.key, &f2.bytes).unwrap();
        d.flight_written(f2.first);
        d.flight_failed(f1.first, "no route to the bucket");
        assert!(matches!(d.take_outcome(t1), Some(Outcome::Failed(_))));
        assert!(matches!(d.take_outcome(t2), Some(Outcome::Durable(_))), "its hole is marked and was in an untold commit");
        assert!(!d.is_fenced());
        assert_eq!(d.in_flight.len(), 0);
    }

    #[test]
    fn a_failed_flight_carrying_an_acknowledged_commit_fences_forward() {
        // T1 (async) was told COMMIT and is visible; T2 read T1's row and
        // committed sync in a later flight that landed. T1's flight fails.
        // Releasing T2 as Durable would make a copy persist whose source
        // never existed; both are fenced and neither is applied later.
        let (m, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let (t1, s1) = d.stage_commit(one(b"src", b"1"), 1, LATEST, false).unwrap().unwrap().unwrap();
        d.mark_confirmed(s1); // acknowledged early
        let f1 = d.take_flight().unwrap();
        assert_eq!(d.get(b"src").unwrap(), Some(b"1".to_vec()), "visible to T2");
        let (t2, _) = d.stage_commit(one(b"copy", b"1"), 2, d.current_seq(), true).unwrap().unwrap().unwrap();
        let f2 = d.take_flight().unwrap();
        s.put_if_absent(&f2.key, &f2.bytes).unwrap();
        d.flight_written(f2.first);
        assert!(d.take_outcome(t2).is_none(), "waits for the prefix");

        d.flight_failed(f1.first, "no route to the bucket");
        assert!(d.is_fenced());
        assert!(matches!(d.take_outcome(t1), Some(Outcome::Fenced(_))));
        assert!(matches!(d.take_outcome(t2), Some(Outcome::Fenced(_))), "not Durable behind a hole somebody saw");
        assert!(d.take_flight().is_none());
        assert!(!d.has_unwritten());
        // Both numbers are dead in the bucket, whatever lands there.
        assert!(m.get(&discard_key(1)).unwrap().is_some());
        assert!(m.get(&discard_key(2)).unwrap().is_some());
        drop(d);
        s.put_if_absent(&f1.key, &f1.bytes).unwrap(); // the late arrival
        let d = Db::open(Arc::clone(&s)).unwrap();
        assert_eq!(d.get(b"src").unwrap(), None);
        assert_eq!(d.get(b"copy").unwrap(), None, "T2's object is in the bucket and is not a commit");
    }

    #[test]
    fn a_failed_flight_whose_fate_cannot_be_read_fences_with_an_unknown_outcome() {
        let (f, s) = Flaky::new();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let (t, _) = d.stage_commit(one(b"a", b"1"), 0, LATEST, true).unwrap().unwrap().unwrap();
        let fl = d.take_flight().unwrap();
        f.unreadable(Some(&fl.key));
        d.flight_failed(fl.first, "no route to the bucket");
        match d.take_outcome(t) {
            Some(Outcome::Fenced(why)) => assert!(why.contains("unknown"), "{why}"),
            other => panic!("{other:?}"),
        }
        assert!(d.is_fenced());
    }

    // ---- the lease and the epoch (finding #8) --------------------------------

    fn leased(s: &Arc<dyn Store>, clock: &Arc<FakeClock>) -> Db {
        let clock: Arc<dyn crate::lease::Clock> = Arc::clone(clock) as Arc<dyn crate::lease::Clock>;
        let lease = Lease::acquire(s, clock).unwrap();
        Db::open_with_lease(Arc::clone(s), lease).unwrap()
    }

    #[test]
    fn every_commit_carries_the_epoch_and_the_open_records_a_fence() {
        let (m, s) = mem();
        let clock = FakeClock::at(1_000);
        let mut a = leased(&s, &clock);
        assert_eq!(a.epoch(), 1);
        commit(&mut a, &s, one(b"k", b"1"));
        let members = commit::decode_members(&m.get(&commit::key_for(1)).unwrap().unwrap()).unwrap();
        assert_eq!(members[0].0.epoch, 1);
        assert_eq!(m.get(&fence_key(1)).unwrap().unwrap(), b"1".to_vec(), "epoch 1 started at 1");
        drop(a);
        let b = leased(&s, &clock);
        assert_eq!(b.epoch(), 2);
        assert_eq!(m.get(&fence_key(2)).unwrap().unwrap(), b"2".to_vec(), "epoch 2 starts above what epoch 1 wrote");
        assert_eq!(b.get(b"k").unwrap(), Some(b"1".to_vec()), "a previous epoch's commits below the fence stand");
    }

    #[test]
    fn an_object_a_stale_writer_lands_above_the_fence_is_ignored() {
        let (m, s) = mem();
        let clock = FakeClock::at(1_000);
        let mut a = leased(&s, &clock);
        commit(&mut a, &s, one(b"k", b"from-a"));
        // A pauses. Its lease runs out; B takes over on another machine.
        clock.set(1_000 + TTL_MS + 1);
        let mut b = leased(&s, &clock);
        assert_eq!(b.epoch(), 2);
        assert!(!a.lease.as_ref().unwrap().valid());

        // A wakes and, with a wrong clock, writes what it had queued: a
        // commit at 2 under epoch 1. Modelled by hand, since A's own Db
        // refuses to write once its lease is over.
        let stale = Commit { seq: 2, base_run_id: 0, xid: 9, epoch: 1, entries: vec![Entry { key: b"k".to_vec(), op: Op::Put(b"stale".to_vec()) }] };
        m.put_if_absent(&commit::key_for(2), &stale.encode()).unwrap();

        // B's own commit wants number 2 and finds it taken: B stops, and
        // says by whom.
        let (t, _) = b.stage_commit(one(b"k", b"from-b"), 1, LATEST, true).unwrap().unwrap().unwrap();
        fly(&mut b, &s);
        match b.take_outcome(t) {
            Some(Outcome::Fenced(why)) => assert!(why.contains("epoch 1 after this server (epoch 2) took over"), "{why}"),
            other => panic!("{other:?}"),
        }
        assert!(b.is_fenced());
        drop(b);

        // The next open sees the stale object above epoch 2's fence and
        // ignores it: A's commit 1 stands, its commit 2 does not.
        let c = leased(&s, &clock);
        assert_eq!(c.epoch(), 3);
        assert_eq!(c.get(b"k").unwrap(), Some(b"from-a".to_vec()));
        assert_eq!(c.next_seq, 3, "and the burnt number is not reused");
    }

    #[test]
    fn an_expired_lease_refuses_to_stage_and_fences() {
        let (_, s) = mem();
        let clock = FakeClock::at(1_000);
        let mut a = leased(&s, &clock);
        commit(&mut a, &s, one(b"k", b"1"));
        clock.set(1_000 + TTL_MS - SKEW_MARGIN_MS);
        let err = a.stage_commit(one(b"k", b"2"), 1, LATEST, true).unwrap_err().to_string();
        assert!(err.contains("expired"), "{err}");
        assert!(a.is_fenced());
        assert_eq!(a.get(b"k").unwrap(), Some(b"1".to_vec()), "reads go on");
    }

    #[test]
    fn a_lease_that_runs_out_with_an_object_in_flight_fences_instead_of_acknowledging() {
        let (_, s) = mem();
        let clock = FakeClock::at(1_000);
        let mut a = leased(&s, &clock);
        let (t, _) = a.stage_commit(one(b"k", b"1"), 1, LATEST, true).unwrap().unwrap().unwrap();
        let f = a.take_flight().unwrap();
        clock.set(1_000 + TTL_MS + 1); // the PUT took longer than the lease
        s.put_if_absent(&f.key, &f.bytes).unwrap();
        a.flight_written(f.first);
        match a.take_outcome(t) {
            Some(Outcome::Fenced(why)) => assert!(why.contains("expired"), "{why}"),
            other => panic!("landed under a lost lease must not be Durable: {other:?}"),
        }
        assert!(a.is_fenced());
        assert!(a.take_flight().is_none());
    }

    #[test]
    fn a_renewed_lease_keeps_writing_and_release_lets_the_next_open_claim_at_once() {
        let (m, s) = mem();
        let clock = FakeClock::at(1_000);
        let mut a = leased(&s, &clock);
        for i in 0..5 {
            clock.advance(TTL_MS / 2);
            a.lease.as_ref().unwrap().renew().unwrap();
            commit(&mut a, &s, one(b"k", format!("{i}").as_bytes()));
        }
        a.release_lease().unwrap();
        assert_eq!(crate::lease::current(&s).unwrap().unwrap().body.unwrap().expires_ms, 0, "released: expired at once");
        assert!(a.stage_commit(one(b"k", b"late"), 1, LATEST, true).is_err(), "released means no more writes");
        drop(a);
        let b = leased(&s, &clock);
        assert_eq!(b.epoch(), 2, "claimed at once, on the next epoch");
        assert_eq!(b.get(b"k").unwrap(), Some(b"4".to_vec()));
        let _ = m;
    }

    #[test]
    fn a_read_only_open_writes_nothing_and_refuses_writes() {
        let (m, s) = mem();
        let mut a = Db::open(Arc::clone(&s)).unwrap();
        commit(&mut a, &s, one(b"k", b"1"));
        let before: Vec<String> = m.list("").unwrap().into_iter().map(|i| i.key).collect();
        let mut r = Db::open_with(Arc::clone(&s)).unwrap();
        let after: Vec<String> = m.list("").unwrap().into_iter().map(|i| i.key).collect();
        assert_eq!(before, after, "a reader leaves the bucket as it found it");
        assert_eq!(r.get(b"k").unwrap(), Some(b"1".to_vec()));
        assert!(r.stage_commit(one(b"k", b"2"), 1, LATEST, true).unwrap_err().to_string().contains("no lease"));
        assert!(a.lease.as_ref().unwrap().valid(), "the writer is untouched");
    }

    #[test]
    fn a_second_writer_is_refused_while_the_first_renews() {
        // Another host's live claim, planted by hand: this process would be
        // allowed to retake its own.
        let (m, s) = mem();
        // Anchored to the real clock, since `Db::open` reads that one.
        let now = crate::lease::Clock::now_ms(&crate::lease::SystemClock);
        let clock = FakeClock::at(now);
        let expires_ms = now + TTL_MS;
        m.put_if_absent(&crate::lease::key(1, 0), &format!("objkv-lease\nsome-other-machine\n4242\n{expires_ms}\n").into_bytes()).unwrap();
        let err = Db::open(Arc::clone(&s)).err().expect("refused").to_string();
        assert!(err.contains("owned by some-other-machine:4242"), "{err}");
        // With a clock we can move, the takeover happens exactly at expiry.
        let c: Arc<dyn crate::lease::Clock> = Arc::clone(&clock) as Arc<dyn crate::lease::Clock>;
        assert!(Lease::acquire(&s, Arc::clone(&c)).is_err());
        clock.set(expires_ms + 1);
        let l = Lease::acquire(&s, c).unwrap();
        assert_eq!(l.epoch(), 2);
        let d = Db::open_with_lease(Arc::clone(&s), l).unwrap();
        assert_eq!(d.epoch(), 2);
    }

    // ---- reads at old snapshots -----------------------------------------------

    #[test]
    fn reads_at_an_old_snapshot_see_old_data() {
        let (s, mut d) = db();
        assert_eq!(commit(&mut d, &s, many(&[("t/1", "one"), ("t/2", "two")])), 1);
        assert_eq!(commit(&mut d, &s, one(b"t/1", b"ONE")), 2);
        assert_eq!(commit(&mut d, &s, del(b"t/2")), 3);

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
                .map(|(k, val)| format!("{}={}", String::from_utf8(k).unwrap(), String::from_utf8(val).unwrap()))
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
        let (s, mut d) = db();
        for v in ["v1", "v2", "v3"] {
            commit(&mut d, &s, one(b"t/1", v.as_bytes()));
        }
        for i in 0..120 {
            commit(&mut d, &s, one(format!("t/pad{i:04}").as_bytes(), b"x"));
        }
        assert!(d.needs_compaction());
        fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
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
        let (_, s) = mem();
        {
            let mut d = Db::open(Arc::clone(&s)).unwrap();
            commit(&mut d, &s, one(b"t/1", b"yesterday"));
            commit(&mut d, &s, one(b"t/1", b"today"));
            fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
        }
        let d = Db::open(Arc::clone(&s)).unwrap();
        assert_eq!(d.get_at(b"t/1", 1).unwrap(), Some(b"yesterday".to_vec()));
        assert_eq!(d.get(b"t/1").unwrap(), Some(b"today".to_vec()));
    }

    // ---- windows -------------------------------------------------------------

    #[test]
    fn a_window_walks_a_range_and_stops() {
        let (s, mut d) = db();
        let mut w = BTreeMap::new();
        for i in 0..50u32 {
            w.insert(format!("k/{i:04}").into_bytes(), Op::Put(vec![i as u8]));
        }
        commit(&mut d, &s, w);
        // Half of them removed, so most windows come back short of their
        // limit without the range being finished.
        let mut w = BTreeMap::new();
        for i in (0..50u32).step_by(2) {
            w.insert(format!("k/{i:04}").into_bytes(), Op::Delete);
        }
        commit(&mut d, &s, w);

        let v = d.view();
        let mut lo = b"k/".to_vec();
        let hi = b"k0".to_vec();
        let mut seen = Vec::new();
        let mut windows = 0;
        loop {
            let (rows, resume) = v.scan_window_at(&lo, &hi, LATEST, 7).unwrap();
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
        let (s, mut d) = db();
        let mut w = BTreeMap::new();
        for i in 0..5u32 {
            w.insert(format!("k/{i:04}").into_bytes(), Op::Put(vec![]));
        }
        commit(&mut d, &s, w);
        let (rows, resume) = d.view().scan_window_at(b"k/", b"k0", LATEST, 100).unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(resume, None);
    }

    #[test]
    fn a_backward_walk_covers_the_same_rows_from_the_other_end() {
        let (s, mut d) = db();
        let mut w = BTreeMap::new();
        for i in 0..40u32 {
            w.insert(format!("k/{i:04}").into_bytes(), Op::Put(vec![i as u8]));
        }
        commit(&mut d, &s, w);
        fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
        let mut w = BTreeMap::new();
        for i in (0..40u32).step_by(3) {
            w.insert(format!("k/{i:04}").into_bytes(), Op::Delete);
        }
        commit(&mut d, &s, w);

        let v = d.view();
        let forward: Vec<Vec<u8>> = v.scan_range_at(b"k/", b"k0", LATEST).unwrap().into_iter().map(|(k, _)| k).collect();
        let mut hi = b"k0".to_vec();
        let mut back: Vec<Vec<u8>> = Vec::new();
        let mut windows = 0;
        loop {
            let (rows, resume) = v.scan_window_back_at(b"k/", &hi, LATEST, 6).unwrap();
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
        let (s, mut d) = db();
        let mut w = BTreeMap::new();
        for i in 0..100u32 {
            w.insert(format!("k/{i:04}").into_bytes(), Op::Put(vec![]));
        }
        commit(&mut d, &s, w);
        let (rows, _) = d.view().scan_window_back_at(b"k/", b"k0", LATEST, 3).unwrap();
        let keys: Vec<Vec<u8>> = rows.into_iter().map(|(k, _, _)| k).collect();
        assert_eq!(keys, vec![b"k/0097".to_vec(), b"k/0098".to_vec(), b"k/0099".to_vec()], "the top three, still in ascending order");
    }

    #[test]
    fn a_window_under_an_old_snapshot_is_not_cut_short_by_newer_versions_in_the_run() {
        let (s, mut d) = db();
        let mut w = BTreeMap::new();
        for i in 0..20u32 {
            w.insert(format!("k/{i:04}").into_bytes(), Op::Put(b"old".to_vec()));
        }
        commit(&mut d, &s, w);
        let snap = d.current_seq();

        // Rewrite the first half, then fold everything into one run.
        let mut w = BTreeMap::new();
        for i in 0..10u32 {
            w.insert(format!("k/{i:04}").into_bytes(), Op::Put(b"new".to_vec()));
        }
        commit(&mut d, &s, w);
        fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
        assert!(snap < d.base_run_id, "the newer versions live only in the run");

        let v = d.view();
        let (lo, hi) = (b"k/".to_vec(), b"k0".to_vec());
        for (at, want) in [(snap, &b"old"[..]), (LATEST, &b"new"[..])] {
            let mut got = Vec::new();
            let mut from = lo.clone();
            loop {
                let (rows, resume) = v.scan_window_at(&from, &hi, at, 4).unwrap();
                got.extend(rows);
                match resume {
                    Some(next) => from = next,
                    None => break,
                }
            }
            assert_eq!(got.len(), 20, "every row as of {at}, forwards, once each");
            assert!(got.windows(2).all(|w| w[0].0 < w[1].0), "in order, no repeats");
            assert!(got[..10].iter().all(|(_, v)| v == want), "and the version seen at {at}");

            let mut got = Vec::new();
            let mut top = hi.clone();
            loop {
                let (rows, resume) = v.scan_window_back_at(&lo, &top, at, 4).unwrap();
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

    // ---- the object is the commit ----------------------------------------------

    #[test]
    fn a_staged_commit_is_a_commit_once_its_object_lands() {
        // A crash between the PUT and the Postgres commit keeps it, as a WAL
        // commit record would.
        let (_, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let (_, seq) = d.stage_commit(one(b"a", b"1"), 0, LATEST, true).unwrap().unwrap().unwrap();
        fly(&mut d, &s).unwrap();
        assert_eq!(d.get(b"a").unwrap(), None, "staged, so not yet readable here");
        std::mem::forget(d); // no mark_confirmed, no release: the crash

        let r = Db::open_with(Arc::clone(&s)).unwrap();
        assert_eq!(r.get(b"a").unwrap(), Some(b"1".to_vec()), "the object is the commit");
        assert!(s.list("resolve/").unwrap().is_empty(), "nothing was discarded");

        // An abort after landing is recorded, and the record wins.
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let (_, seq2) = d.stage_commit(one(b"b", b"2"), 0, LATEST, true).unwrap().unwrap().unwrap();
        assert!(seq2 > seq);
        fly(&mut d, &s).unwrap();
        d.discard_staged(seq2, Discard::Aborted);
        drop(d);
        let r = Db::open_with(Arc::clone(&s)).unwrap();
        assert_eq!(r.get(b"b").unwrap(), None, "aborted after landing: the marker rules");
        assert_eq!(r.get(b"a").unwrap(), Some(b"1".to_vec()));
    }

    #[test]
    fn the_async_backlog_counts_only_what_was_acknowledged_early() {
        let (_, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        d.stage_commit(one(b"a", b"v"), 0, LATEST, false).unwrap().unwrap();
        d.stage_commit(one(b"b", b"v"), 0, LATEST, true).unwrap().unwrap();
        d.stage_commit(one(b"c", b"v"), 0, LATEST, false).unwrap().unwrap();
        assert_eq!(d.async_backlog(), 2, "two acknowledged, one waiting");
        let f = d.take_flight().unwrap();
        assert_eq!(d.async_backlog(), 2, "in flight still counts: not in the bucket yet");
        s.put_if_absent(&f.key, &f.bytes).unwrap();
        d.flight_written(f.first);
        assert_eq!(d.async_backlog(), 0);
    }

    #[test]
    fn flights_land_in_any_order_but_are_acknowledged_in_sequence() {
        let (_, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let (t1, _) = d.stage_commit(one(b"a", b"1"), 0, LATEST, true).unwrap().unwrap().unwrap();
        let f1 = d.take_flight().unwrap();
        let (t2, _) = d.stage_commit(one(b"b", b"2"), 0, LATEST, true).unwrap().unwrap().unwrap();
        let f2 = d.take_flight().expect("a second flight while the first is out");
        assert!(f2.first > f1.first);

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
    fn the_in_flight_limit_holds() {
        let (_, s) = mem();
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
        let (_, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let mut seq_of_version = Vec::new();
        for round in 0..=MAX_RUNS {
            for i in 0..3 {
                let mut w = one(b"row", format!("r{round}i{i}").as_bytes());
                w.insert(format!("only-{round}-{i}").into_bytes(), Op::Put(b"x".to_vec()));
                seq_of_version.push((commit(&mut d, &s, w), format!("r{round}i{i}")));
            }
            fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
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
        assert_eq!(r.scan_prefix(b"only-").unwrap().len(), 3 * (MAX_RUNS + 1), "every row from every round");
    }

    #[test]
    fn a_crash_between_a_merge_and_its_sweep_leaves_only_leftovers() {
        let (_, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        // A first fold is a full run; the ones after are deltas until the
        // merge rule fires. Stop at the first merge after that, with its run
        // written and nothing swept: the crash.
        let mut last = String::new();
        for i in 0..10 * MAX_RUNS {
            commit(&mut d, &s, one(b"k", format!("v{i}").as_bytes()));
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
        std::mem::forget(d); // the crash: no release either
        let r = Db::open_with(Arc::clone(&s)).unwrap();
        assert_eq!(r.run_count(), 1, "only the merged run is live");
        assert!(s.list("run/").unwrap().len() > 1, "a reader deletes nothing");
        drop(r);
        // The lease the crashed process held is ours to retake (same host, same
        // process), and the owner's open clears the leftovers.
        let w = Db::open(Arc::clone(&s)).unwrap();
        assert_eq!(s.list("run/").unwrap().len(), 1, "the leftovers were deleted on open");
        assert_eq!(w.get(b"k").unwrap(), Some(last.into_bytes()));
    }

    #[test]
    fn claiming_an_oid_block_keeps_the_boundary_it_relies_on() {
        let (m, s) = mem();
        assert_eq!(claim_oid_block(&s, 100, 8).unwrap(), 100);
        assert_eq!(m.list("oidnext/").unwrap()[0].key, oid_block_key(108));
        // A lower request is raised to the floor; the old boundary goes.
        assert_eq!(claim_oid_block(&s, 50, 8).unwrap(), 108);
        let keys: Vec<String> = m.list("oidnext/").unwrap().into_iter().map(|i| i.key).collect();
        assert_eq!(keys, vec![oid_block_key(116)]);
        // A boundary that already exists at exactly the new end is kept, not
        // deleted right after being relied on.
        assert_eq!(claim_oid_block(&s, 116, 0).unwrap(), 116);
        let keys: Vec<String> = m.list("oidnext/").unwrap().into_iter().map(|i| i.key).collect();
        assert_eq!(keys, vec![oid_block_key(116)]);
    }

    // ---- snapshots across compaction, and under load (finding #22) -------------

    #[test]
    fn an_open_view_survives_a_merge_and_its_sweep() {
        let (m, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        for i in 0..10 {
            commit(&mut d, &s, one(b"row", format!("v{i}").as_bytes()));
        }
        fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
        let first_run = m.list("run/").unwrap()[0].key.clone();

        // A long scan pins a snapshot and a view while the world moves on:
        // more commits, a delta, then a merge that replaces the run the
        // view is reading, with a horizon that would collect its version.
        let snap = d.current_seq();
        let view = d.view();
        assert_eq!(view.get_at(b"row", snap).unwrap(), Some(b"v9".to_vec()));
        for i in 10..40 {
            commit(&mut d, &s, one(b"row", format!("v{i}").as_bytes()));
            if i % 5 == 4 {
                // The horizon is clamped to the oldest snapshot in use, as
                // the AM's `oldest_in_use` does: never past `snap`.
                fold(&mut d, &s, snap, &BTreeMap::new()).unwrap();
            }
        }
        assert!(d.runs.iter().all(|r| r.source().key != first_run), "a merge replaced the run the view reads");
        assert!(m.get(&first_run).unwrap().is_some(), "held by the view, so not swept");
        assert_eq!(view.get_at(b"row", snap).unwrap(), Some(b"v9".to_vec()), "the pinned snapshot still answers");
        assert_eq!(view.scan_prefix_at(b"row", snap).unwrap(), vec![(b"row".to_vec(), b"v9".to_vec())]);
        assert_eq!(d.get_at(b"row", snap).unwrap(), Some(b"v9".to_vec()), "and so does a fresh view: the clamp kept the version");

        drop(view);
        for i in 40..60 {
            commit(&mut d, &s, one(b"row", format!("v{i}").as_bytes()));
            if i % 5 == 4 {
                fold(&mut d, &s, 0, &BTreeMap::new()).unwrap();
            }
        }
        assert!(m.get(&first_run).unwrap().is_none(), "released, so a later sweep takes it");
    }

    /// Readers, writers and a compactor on one database, with money moving
    /// between accounts. Every reader's sum must be the total, whatever it
    /// interleaves with; conflicts are retried, nothing else is tolerated.
    #[test]
    fn readers_writers_and_a_compactor_keep_the_sum() {
        const ACCOUNTS: u64 = 16;
        const WRITERS: usize = 4;
        const READERS: usize = 2;
        const TRANSFERS: usize = 120;
        fn acct(i: u64) -> Vec<u8> {
            format!("acct/{i:02}").into_bytes()
        }
        fn read(view: &View, snap: u64, i: u64) -> u64 {
            std::str::from_utf8(&view.get_at(&acct(i), snap).unwrap().unwrap()).unwrap().parse().unwrap()
        }
        let (_, s) = mem();
        let mut d = Db::open(Arc::clone(&s)).unwrap();
        let mut w = BTreeMap::new();
        for i in 0..ACCOUNTS {
            w.insert(acct(i), Op::Put(100u64.to_string().into_bytes()));
        }
        commit(&mut d, &s, w);
        let total = ACCOUNTS * 100;

        // The Db and the set of snapshots in use, under one lock as the AM
        // keeps them, so the compactor's horizon never passes a live read.
        let shared = Arc::new(Mutex::new((d, BTreeMap::<u64, usize>::new())));
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let mut threads = Vec::new();
        for wi in 0..WRITERS {
            let shared = Arc::clone(&shared);
            let s = Arc::clone(&s);
            threads.push(std::thread::spawn(move || {
                let mut landed = 0;
                for n in 0..TRANSFERS {
                    let from = ((wi * 7 + n * 3) as u64) % ACCOUNTS;
                    let to = ((wi * 11 + n * 5 + 1) as u64) % ACCOUNTS;
                    if from == to {
                        continue;
                    }
                    loop {
                        let (snap, view) = {
                            let g = shared.lock().unwrap();
                            (g.0.current_seq(), g.0.view())
                        };
                        let (a, b) = (read(&view, snap, from), read(&view, snap, to));
                        let mut w = BTreeMap::new();
                        w.insert(acct(from), Op::Put((a - 1).to_string().into_bytes()));
                        w.insert(acct(to), Op::Put((b + 1).to_string().into_bytes()));
                        let staged = shared.lock().unwrap().0.stage_commit(w, n as u32, snap, true).unwrap();
                        let (ticket, seq) = match staged {
                            Ok(Some(x)) => x,
                            Ok(None) => unreachable!(),
                            Err(_) => continue, // first committer won; retry
                        };
                        let flight = shared.lock().unwrap().0.take_flight();
                        if let Some(f) = flight {
                            let r = s.put_if_absent(&f.key, &f.bytes).unwrap();
                            let mut g = shared.lock().unwrap();
                            match r {
                                PutOutcome::Written => g.0.flight_written(f.first),
                                PutOutcome::AlreadyExists => g.0.flight_lost(&f).unwrap(),
                            }
                        }
                        loop {
                            let outcome = shared.lock().unwrap().0.take_outcome(ticket);
                            match outcome {
                                Some(Outcome::Durable(x)) => {
                                    assert_eq!(x, seq);
                                    shared.lock().unwrap().0.mark_confirmed(seq);
                                    break;
                                }
                                Some(other) => panic!("unexpected outcome {other:?}"),
                                None => std::thread::yield_now(),
                            }
                        }
                        landed += 1;
                        break;
                    }
                }
                landed
            }));
        }
        let mut readers = Vec::new();
        for _ in 0..READERS {
            let shared = Arc::clone(&shared);
            let done = Arc::clone(&done);
            readers.push(std::thread::spawn(move || {
                let mut reads = 0;
                while !done.load(std::sync::atomic::Ordering::Relaxed) || reads < 20 {
                    let (snap, view) = {
                        let mut g = shared.lock().unwrap();
                        let snap = g.0.current_seq();
                        *g.1.entry(snap).or_insert(0) += 1;
                        (snap, g.0.view())
                    };
                    let sum: u64 = (0..ACCOUNTS).map(|i| read(&view, snap, i)).sum();
                    assert_eq!(sum, total, "a snapshot at {snap} saw money appear or vanish");
                    let scanned: u64 = view
                        .scan_prefix_at(b"acct/", snap)
                        .unwrap()
                        .into_iter()
                        .map(|(_, v)| std::str::from_utf8(&v).unwrap().parse::<u64>().unwrap())
                        .sum();
                    assert_eq!(scanned, total, "a scan at {snap} disagrees with point reads");
                    {
                        let mut g = shared.lock().unwrap();
                        let n = g.1.get_mut(&snap).unwrap();
                        *n -= 1;
                        if *n == 0 {
                            g.1.remove(&snap);
                        }
                    }
                    reads += 1;
                }
                reads
            }));
        }
        let compactor = {
            let shared = Arc::clone(&shared);
            let done = Arc::clone(&done);
            let s = Arc::clone(&s);
            std::thread::spawn(move || {
                let mut folds = 0;
                loop {
                    let plan = {
                        let g = shared.lock().unwrap();
                        // Collect up to the oldest snapshot still in use, and
                        // never past it.
                        let horizon = g.1.keys().next().map_or(g.0.current_seq(), |&o| o.saturating_sub(1));
                        (g.0.commits.len() >= 20).then(|| g.0.fold_plan()).flatten().map(|p| (p, horizon))
                    };
                    match plan {
                        Some((plan, horizon)) => {
                            let folded = build_fold(&plan, horizon, &BTreeMap::new()).unwrap();
                            put_fold(&s, &folded).unwrap();
                            let sweep = shared.lock().unwrap().0.apply_fold(plan, &folded, horizon).unwrap();
                            let result = execute_sweep(&s, sweep);
                            shared.lock().unwrap().0.sweep_done(result);
                            folds += 1;
                        }
                        None if done.load(std::sync::atomic::Ordering::Relaxed) => return folds,
                        None => std::thread::yield_now(),
                    }
                }
            })
        };
        let landed: usize = threads.into_iter().map(|t| t.join().unwrap()).sum();
        done.store(true, std::sync::atomic::Ordering::Relaxed);
        let reads: usize = readers.into_iter().map(|t| t.join().unwrap()).sum();
        let folds = compactor.join().unwrap();
        assert!(landed > 0 && reads > 0);
        assert!(folds > 0, "the compactor never ran; the test proved nothing about it");

        let d = std::mem::replace(&mut shared.lock().unwrap().0, Db::open_with(Arc::clone(&s)).unwrap());
        assert!(!d.is_fenced());
        assert_eq!(d.current_seq(), 1 + landed as u64);
        drop(d);
        let r = Db::open_with(Arc::clone(&s)).unwrap();
        let sum: u64 = (0..ACCOUNTS).map(|i| read(&r.view(), LATEST, i)).sum();
        assert_eq!(sum, total, "and the bucket agrees after {landed} transfers and {folds} folds");
    }
}
