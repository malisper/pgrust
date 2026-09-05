//! The single-writer lease: which process may write to a bucket, and until
//! when.
//!
//! Two processes on one bucket would each hand out the same sequence numbers
//! and row ids and overwrite each other's rows with no error. The lease makes
//! a second process refuse to start while the first is renewing, and lets it
//! take over -- from any host -- once the first has stopped.
//!
//! Layout: `owner/<epoch>/<renewal>` objects, each created with put-if-absent.
//! Claiming `owner/<E+1>/0` is the takeover, and exactly one claimant wins it
//! because the key is new. The holder renews by writing `owner/<E>/<n+1>`
//! with a fresh expiry, then lists `owner/` to see whether a higher epoch has
//! appeared: if one has, the lease is lost and the holder stops writing.
//!
//! Time: the body carries an absolute expiry in unix milliseconds. A claimant
//! takes over once that has passed on its own clock; the holder treats its
//! lease as over `SKEW_MARGIN_MS` before the expiry it wrote. Clocks have to
//! agree to within that margin, which is what every wall-clock lease assumes,
//! and clean shutdowns do not depend on it at all: `release` writes a renewal
//! that has already expired, and the next open claims at once.
//!
//! Epochs never repeat. The newest claim always stays in the bucket -- a
//! release expires it rather than deleting it, and only the next claim
//! clears it -- so every claimant counts on from the highest epoch ever
//! taken, and an object stamped with an old epoch can always be told from
//! the current owner's.
//!
//! The lease alone cannot stop a writer whose clock is wrong or which was
//! paused past its expiry from making one more PUT. That is what the epoch
//! is for: it is stamped into every commit object, and `db.rs` records a
//! fence at each takeover so such an object is recognised and never applied.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crate::s3::PutOutcome;
use crate::store::Store;

/// How long a renewal is good for.
pub const TTL_MS: u64 = 30_000;
/// How often the holder renews: well inside the TTL, so one failed renewal
/// does not cost the lease.
pub const HEARTBEAT_MS: u64 = 10_000;
/// How far apart two clocks may be. The holder stops this much before its
/// written expiry, so a claimant whose clock runs ahead by less than this
/// still cannot take over a writer that is about to PUT.
pub const SKEW_MARGIN_MS: u64 = 5_000;

/// Wall-clock time, so a test can move it by hand.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        let since = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
        since.map_or(0, |d| d.as_millis() as u64)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owner {
    pub host: String,
    pub pid: u32,
}

impl Owner {
    pub fn me() -> Owner {
        Owner { host: hostname(), pid: std::process::id() }
    }
}

/// What an owner object says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Body {
    pub owner: Owner,
    pub expires_ms: u64,
}

impl Body {
    fn encode(&self) -> Vec<u8> {
        format!("objkv-lease\n{}\n{}\n{}\n", self.owner.host, self.owner.pid, self.expires_ms)
            .into_bytes()
    }
    fn decode(b: &[u8]) -> Option<Body> {
        let s = String::from_utf8_lossy(b);
        let mut it = s.lines();
        if it.next()? != "objkv-lease" {
            return None;
        }
        Some(Body {
            owner: Owner { host: it.next()?.to_string(), pid: it.next()?.parse().ok()? },
            expires_ms: it.next()?.parse().ok()?,
        })
    }
}

pub(crate) fn key(epoch: u64, renewal: u64) -> String {
    format!("owner/{epoch:016x}/{renewal:016x}")
}

/// `owner/<epoch>/<renewal>` -> (epoch, renewal).
fn parse_key(k: &str) -> Option<(u64, u64)> {
    let mut parts = k.strip_prefix("owner/")?.split('/');
    let epoch = u64::from_str_radix(parts.next()?, 16).ok()?;
    let renewal = u64::from_str_radix(parts.next()?, 16).ok()?;
    parts.next().is_none().then_some((epoch, renewal))
}

/// The newest claim in the bucket.
#[derive(Debug)]
pub struct Held {
    pub epoch: u64,
    pub renewal: u64,
    pub key: String,
    /// `None` when the object exists but does not decode. The epoch is taken
    /// either way; who took it and until when is the unreadable part.
    pub body: Option<Body>,
    /// Every `owner/` key, so a successful claim can clear the older ones.
    keys: Vec<String>,
}

/// The newest epoch anything has claimed and its newest renewal.
///
/// The epoch comes from the key and the rest from the body, because they
/// fail separately: an owner object that does not decode still proves its
/// epoch is taken.
pub fn current(store: &Arc<dyn Store>) -> io::Result<Option<Held>> {
    let keys: Vec<String> = store.list("owner/")?.into_iter().map(|i| i.key).collect();
    let Some((epoch, renewal, k)) = keys
        .iter()
        .filter_map(|k| parse_key(k).map(|(e, n)| (e, n, k.clone())))
        .max()
    else {
        return Ok(None);
    };
    let body = store.get(&k)?.and_then(|b| Body::decode(&b));
    Ok(Some(Held { epoch, renewal, key: k, body, keys }))
}

struct Inner {
    store: Arc<dyn Store>,
    clock: Arc<dyn Clock>,
    epoch: u64,
    me: Owner,
    /// The expiry of the newest renewal that landed.
    expires_ms: AtomicU64,
    renewal: AtomicU64,
    /// A higher epoch was seen in the bucket: someone took over. 0 = no.
    lost_to: AtomicU64,
    released: AtomicBool,
    /// Tells the heartbeat thread to stop.
    stop: AtomicBool,
}

/// A held lease. Cloning shares it; the heartbeat thread holds one clone.
#[derive(Clone)]
pub struct Lease {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Lease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lease")
            .field("epoch", &self.inner.epoch)
            .field("expires_ms", &self.inner.expires_ms.load(Ordering::Relaxed))
            .field("lost_to", &self.inner.lost_to.load(Ordering::Relaxed))
            .field("released", &self.inner.released.load(Ordering::Relaxed))
            .finish()
    }
}

impl Lease {
    /// Takes the lease with a real clock and a heartbeat thread renewing it.
    pub fn acquire_with_heartbeat(store: &Arc<dyn Store>) -> io::Result<Lease> {
        let lease = Lease::acquire(store, Arc::new(SystemClock))?;
        let beat = lease.clone();
        std::thread::Builder::new()
            .name("objkv-lease".into())
            .spawn(move || beat.heartbeat())
            .map_err(|e| io::Error::other(format!("objkv: cannot start the lease heartbeat: {e}")))?;
        Ok(lease)
    }

    /// Takes the lease, or explains why it cannot. No heartbeat: the caller
    /// renews, which is what a test with a fake clock wants.
    pub fn acquire(store: &Arc<dyn Store>, clock: Arc<dyn Clock>) -> io::Result<Lease> {
        let me = Owner::me();
        for _ in 0..8 {
            let held = current(store)?;
            let now = clock.now_ms();
            let next = held.as_ref().map_or(1, |h| h.epoch + 1);
            if let Some(h) = &held {
                match &h.body {
                    // An owner object this version cannot read is never assumed
                    // expired: a live server and a corrupt object look the same.
                    None => {
                        return Err(io::Error::other(format!(
                            "this bucket is owned by a lease this version cannot read \\
                             (`{}`). If that server is definitely gone, delete the object \\
                             and start again.",
                            h.key
                        )))
                    }
                    Some(b) if now <= b.expires_ms && b.owner != me => {
                        return Err(io::Error::other(format!(
                            "this bucket is owned by {}:{} (lease epoch {}, valid for another \\
                             {} ms). Two servers sharing one bucket would overwrite each \\
                             other's rows. A server that stops renewing is taken over \\
                             automatically once its lease expires.",
                            b.owner.host,
                            b.owner.pid,
                            h.epoch,
                            b.expires_ms - now
                        )))
                    }
                    // Expired, or our own process reopening: take over.
                    Some(_) => {}
                }
            }
            let expires_ms = now + TTL_MS;
            let body = Body { owner: me.clone(), expires_ms };
            // Losing this means someone claimed the same epoch first; look again.
            if store.put_if_absent(&key(next, 0), &body.encode())? == PutOutcome::Written {
                // Older claims are garbage now. Best effort: a leftover is
                // one more key in the next listing, nothing else.
                if let Some(h) = held {
                    for k in h.keys {
                        let _ = store.delete(&k);
                    }
                }
                return Ok(Lease {
                    inner: Arc::new(Inner {
                        store: Arc::clone(store),
                        clock,
                        epoch: next,
                        me,
                        expires_ms: AtomicU64::new(expires_ms),
                        renewal: AtomicU64::new(0),
                        lost_to: AtomicU64::new(0),
                        released: AtomicBool::new(false),
                        stop: AtomicBool::new(false),
                    }),
                });
            }
        }
        Err(io::Error::other("could not claim the objkv lease; too many claimants competing"))
    }

    pub fn epoch(&self) -> u64 {
        self.inner.epoch
    }

    pub fn expires_ms(&self) -> u64 {
        self.inner.expires_ms.load(Ordering::Acquire)
    }

    /// Whether this process may write right now: not released, not taken
    /// over, and the newest renewal has more than the skew margin left.
    pub fn valid(&self) -> bool {
        self.why_invalid().is_none()
    }

    /// `None` when valid; otherwise what a fence message should say.
    pub fn why_invalid(&self) -> Option<String> {
        let i = &self.inner;
        if i.released.load(Ordering::Acquire) {
            return Some(format!("the lease (epoch {}) was released", i.epoch));
        }
        let lost = i.lost_to.load(Ordering::Acquire);
        if lost != 0 {
            return Some(format!(
                "the lease (epoch {}) was taken over by epoch {lost}; another server owns \\
                 the bucket now",
                i.epoch
            ));
        }
        let now = i.clock.now_ms();
        let expires = i.expires_ms.load(Ordering::Acquire);
        if now + SKEW_MARGIN_MS >= expires {
            return Some(format!(
                "the lease (epoch {}) expired: last renewed to {expires} ms, now {now} ms; \\
                 another server may own the bucket",
                i.epoch
            ));
        }
        None
    }

    /// `Ok` while writing is allowed.
    pub fn check(&self) -> io::Result<()> {
        match self.why_invalid() {
            Some(why) => Err(io::Error::other(format!("objkv: {why}"))),
            None => Ok(()),
        }
    }

    /// Writes the next renewal, then looks for a takeover. Either failure
    /// leaves the lease as it was; an expiry that is not renewed in time
    /// simply runs out.
    pub fn renew(&self) -> io::Result<()> {
        let i = &self.inner;
        if i.released.load(Ordering::Acquire) {
            return Err(io::Error::other("objkv: the lease was released"));
        }
        if let Some(why) = self.why_invalid() {
            // Once expired or lost the lease is not renewed: the takeover
            // window belongs to the claimants, and writing a fresh expiry
            // into it would let a paused writer come back to life.
            return Err(io::Error::other(format!("objkv: not renewing: {why}")));
        }
        let now = i.clock.now_ms();
        let n = i.renewal.load(Ordering::Acquire) + 1;
        let expires_ms = now + TTL_MS;
        let body = Body { owner: i.me.clone(), expires_ms };
        match i.store.put_if_absent(&key(i.epoch, n), &body.encode())? {
            PutOutcome::Written => {}
            // Only this process writes under its epoch, so an existing
            // object is our own, from an attempt whose response was lost.
            PutOutcome::AlreadyExists => {
                let found = i.store.get(&key(i.epoch, n))?.and_then(|b| Body::decode(&b));
                match found {
                    Some(b) if b.owner == i.me => {
                        i.expires_ms.fetch_max(b.expires_ms, Ordering::AcqRel);
                        i.renewal.store(n, Ordering::Release);
                        let _ = i.store.delete(&key(i.epoch, n - 1));
                        return self.check_takeover();
                    }
                    _ => {
                        return Err(io::Error::other(format!(
                            "objkv: renewal object `{}` exists and is not ours",
                            key(i.epoch, n)
                        )))
                    }
                }
            }
        }
        i.expires_ms.fetch_max(expires_ms, Ordering::AcqRel);
        i.renewal.store(n, Ordering::Release);
        let _ = i.store.delete(&key(i.epoch, n - 1));
        self.check_takeover()
    }

    fn check_takeover(&self) -> io::Result<()> {
        let i = &self.inner;
        if let Some(h) = current(&i.store)? {
            if h.epoch > i.epoch {
                i.lost_to.fetch_max(h.epoch, Ordering::AcqRel);
                return Err(io::Error::other(format!(
                    "objkv: the lease (epoch {}) was taken over by epoch {}",
                    i.epoch, h.epoch
                )));
            }
        }
        Ok(())
    }

    /// Gives the lease up: the next open, on any host, claims at once. Every
    /// write is refused from here on.
    ///
    /// The claim is expired, not deleted: a renewal whose expiry is already
    /// past goes in as the newest object under this epoch, so the epoch
    /// stays visible (and is never handed out again) until the next claim
    /// supersedes it. A renewal already on the wire when this runs can land
    /// after it, which costs the next open a wait of at most the TTL; the
    /// heartbeat checks `released` before each renewal, so that needs the
    /// two to cross within one round trip.
    pub fn release(&self) -> io::Result<()> {
        let i = &self.inner;
        i.stop.store(true, Ordering::Release);
        if i.released.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        if i.lost_to.load(Ordering::Acquire) != 0 {
            return Ok(()); // the next epoch's claim has superseded ours already
        }
        let n = i.renewal.load(Ordering::Acquire) + 1;
        let body = Body { owner: i.me.clone(), expires_ms: 0 };
        i.store.put_if_absent(&key(i.epoch, n), &body.encode())?;
        i.renewal.store(n, Ordering::Release);
        i.expires_ms.store(0, Ordering::Release);
        i.store.delete(&key(i.epoch, n - 1))
    }

    /// Stops the heartbeat without releasing: for a process that has been
    /// fenced and must not touch the bucket again.
    pub fn stop_heartbeat(&self) {
        self.inner.stop.store(true, Ordering::Release);
    }

    fn heartbeat(self) {
        const SLICE_MS: u64 = 250;
        loop {
            for _ in 0..HEARTBEAT_MS / SLICE_MS {
                if self.inner.stop.load(Ordering::Acquire) {
                    return;
                }
                std::thread::sleep(std::time::Duration::from_millis(SLICE_MS));
            }
            if self.inner.released.load(Ordering::Acquire) {
                return;
            }
            if let Err(e) = self.renew() {
                eprintln!("objkv lease: renewal failed: {e}");
                if self.inner.lost_to.load(Ordering::Acquire) != 0 {
                    return;
                }
            }
        }
    }
}

fn hostname() -> String {
    // libc::c_char, not i8: it is unsigned on aarch64 Linux among others, and
    // a hard-coded i8 array will not compile there.
    let mut buf = [0 as libc::c_char; 256];
    // SAFETY: buf is a valid writable array of the length passed.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr(), buf.len()) };
    if rc != 0 {
        return "unknown".into();
    }
    let bytes: Vec<u8> = buf.iter().take_while(|&&c| c != 0).map(|&c| c as u8).collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;

    /// A clock a test moves by hand.
    #[derive(Default)]
    pub struct FakeClock(pub AtomicU64);

    impl FakeClock {
        pub fn at(ms: u64) -> Arc<FakeClock> {
            Arc::new(FakeClock(AtomicU64::new(ms)))
        }
        pub fn advance(&self, ms: u64) {
            self.0.fetch_add(ms, Ordering::SeqCst);
        }
        pub fn set(&self, ms: u64) {
            self.0.store(ms, Ordering::SeqCst);
        }
    }

    impl Clock for FakeClock {
        fn now_ms(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::FakeClock;
    use super::*;
    use crate::store::MemStore;

    fn store() -> Arc<dyn Store> {
        Arc::new(MemStore::new()) as Arc<dyn Store>
    }

    fn foreign(expires_ms: u64) -> Body {
        Body { owner: Owner { host: "some-other-machine".into(), pid: 12345 }, expires_ms }
    }

    #[test]
    fn the_first_process_takes_epoch_one() {
        let s = store();
        let c = FakeClock::at(1_000);
        let l = Lease::acquire(&s, c).unwrap();
        assert_eq!(l.epoch(), 1);
        assert!(l.valid());
        assert_eq!(l.expires_ms(), 1_000 + TTL_MS);
        let held = current(&s).unwrap().unwrap();
        assert_eq!((held.epoch, held.renewal), (1, 0));
        assert_eq!(held.body.unwrap().owner, Owner::me());
    }

    #[test]
    fn a_live_owner_on_another_host_blocks_a_claim() {
        let s = store();
        let c = FakeClock::at(1_000);
        s.put_if_absent(&key(1, 0), &foreign(1_000 + TTL_MS).encode()).unwrap();
        let err = Lease::acquire(&s, c).unwrap_err().to_string();
        assert!(err.contains("owned by some-other-machine:12345"), "got: {err}");
        assert!(err.contains("epoch 1"), "got: {err}");
    }

    #[test]
    fn an_expired_owner_on_any_host_is_taken_over() {
        let s = store();
        let c = FakeClock::at(1_000);
        s.put_if_absent(&key(1, 0), &foreign(1_000 + TTL_MS).encode()).unwrap();
        s.put_if_absent(&key(1, 1), &foreign(1_000 + 2 * TTL_MS).encode()).unwrap();
        c.set(1_000 + 2 * TTL_MS); // exactly at the expiry: still theirs
        assert!(Lease::acquire(&s, c.clone()).is_err());
        c.advance(1);
        let l = Lease::acquire(&s, c).unwrap();
        assert_eq!(l.epoch(), 2, "takeover claims the next epoch");
        let keys = s.list("owner/").unwrap();
        assert_eq!(keys.len(), 1, "the old claim's objects are gone: {keys:?}");
        assert_eq!(keys[0].key, key(2, 0));
    }

    #[test]
    fn renewal_extends_the_expiry_and_keeps_one_object() {
        let s = store();
        let c = FakeClock::at(1_000);
        let l = Lease::acquire(&s, c.clone()).unwrap();
        c.advance(HEARTBEAT_MS);
        l.renew().unwrap();
        assert_eq!(l.expires_ms(), 1_000 + HEARTBEAT_MS + TTL_MS);
        let keys = s.list("owner/").unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].key, key(1, 1), "renewal 1 replaced renewal 0");

        // Someone reading the bucket sees the new expiry.
        let held = current(&s).unwrap().unwrap();
        assert_eq!(held.body.unwrap().expires_ms, l.expires_ms());
    }

    #[test]
    fn the_holder_stops_before_its_written_expiry() {
        let s = store();
        let c = FakeClock::at(1_000);
        let l = Lease::acquire(&s, c.clone()).unwrap();
        c.set(1_000 + TTL_MS - SKEW_MARGIN_MS - 1);
        assert!(l.valid(), "inside the margin");
        c.advance(1);
        assert!(!l.valid(), "the margin is the holder's, not the claimant's");
        let why = l.check().unwrap_err().to_string();
        assert!(why.contains("expired"), "{why}");
        // And it will not renew itself back into the claimants' window.
        assert!(l.renew().is_err());
        assert_eq!(current(&s).unwrap().unwrap().body.unwrap().expires_ms, 1_000 + TTL_MS);
    }

    #[test]
    fn a_takeover_is_noticed_at_the_next_renewal() {
        let s = store();
        let c = FakeClock::at(1_000);
        let a = Lease::acquire(&s, c.clone()).unwrap();
        // The owner pauses past its expiry; a second process takes over.
        c.set(1_000 + TTL_MS + 1);
        let b = Lease::acquire(&s, c.clone()).unwrap();
        assert_eq!(b.epoch(), 2);
        assert!(!a.valid(), "expired on its own clock too");
        assert!(a.renew().is_err(), "an expired lease is not renewed");

        // Even a holder whose clock is behind (still thinks it is valid)
        // learns of the takeover from the listing after its renewal.
        c.set(1_000 + HEARTBEAT_MS);
        let err = a.renew().unwrap_err().to_string();
        assert!(err.contains("taken over by epoch 2"), "{err}");
        assert!(!a.valid());
        assert!(a.why_invalid().unwrap().contains("taken over"));
        assert!(b.valid());
    }

    #[test]
    fn release_expires_the_claim_so_the_next_open_claims_at_once() {
        let s = store();
        let c = FakeClock::at(1_000);
        let l = Lease::acquire(&s, c.clone()).unwrap();
        l.renew().unwrap();
        l.release().unwrap();
        let keys = s.list("owner/").unwrap();
        assert_eq!(keys.len(), 1, "the claim stays, expired: {keys:?}");
        assert_eq!(current(&s).unwrap().unwrap().body.unwrap().expires_ms, 0);
        assert!(!l.valid());
        assert!(l.renew().is_err());
        assert!(l.release().is_ok(), "releasing twice is fine");
        // The next claimant, right away, no waiting for a TTL -- and on the
        // next epoch, never on the released one again.
        let next = Lease::acquire(&s, c).unwrap();
        assert_eq!(next.epoch(), 2, "epochs are never reused");
        let keys: Vec<String> = s.list("owner/").unwrap().into_iter().map(|i| i.key).collect();
        assert_eq!(keys, vec![key(2, 0)], "the new claim cleared the old one");
    }

    #[test]
    fn epochs_stay_monotone_across_a_crash_a_takeover_and_a_release() {
        // A (1) crashes; B (2) takes over after the TTL and later releases
        // cleanly; C must be 3, or A's objects could pass for C's.
        let s = store();
        let c = FakeClock::at(1_000);
        let a = Lease::acquire(&s, c.clone()).unwrap();
        assert_eq!(a.epoch(), 1);
        std::mem::forget(a);
        c.set(1_000 + TTL_MS + 1);
        let b = Lease::acquire(&s, c.clone()).unwrap();
        assert_eq!(b.epoch(), 2);
        b.release().unwrap();
        let cc = Lease::acquire(&s, c).unwrap();
        assert_eq!(cc.epoch(), 3);
    }

    #[test]
    fn an_owner_object_that_does_not_decode_still_holds_its_epoch() {
        let s = store();
        s.put_if_absent(&key(3, 0), b"not a lease object").unwrap();
        let held = current(&s).unwrap().expect("the epoch is taken");
        assert_eq!(held.epoch, 3);
        assert!(held.body.is_none());
        let err = Lease::acquire(&s, FakeClock::at(1)).unwrap_err().to_string();
        assert!(err.contains(&key(3, 0)), "must name the object: {err}");
        assert!(!err.contains("too many"), "must not blame contention: {err}");
    }

    #[test]
    fn a_lost_renewal_response_is_recognised_as_ours() {
        let s = store();
        let c = FakeClock::at(1_000);
        let l = Lease::acquire(&s, c.clone()).unwrap();
        // The first attempt landed but its response did not come back.
        c.advance(100);
        let mine = Body { owner: Owner::me(), expires_ms: c.now_ms() + TTL_MS };
        s.put_if_absent(&key(1, 1), &mine.encode()).unwrap();
        l.renew().unwrap();
        assert_eq!(l.expires_ms(), mine.expires_ms);
        assert!(l.valid());
    }

    #[test]
    fn keys_and_bodies_round_trip() {
        assert_eq!(parse_key(&key(7, 9)), Some((7, 9)));
        assert_eq!(parse_key("owner/zz"), None);
        assert_eq!(parse_key("owner/0000000000000001"), None, "the old one-level layout is not a claim");
        let b = foreign(42);
        assert_eq!(Body::decode(&b.encode()), Some(b));
        assert_eq!(Body::decode(b"garbage"), None);
    }
}
