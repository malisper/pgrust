//! Two processes on one bucket each seed their row-id counter from a scan and
//! then hand out the same ids, overwriting each other's rows with no error.
//! This makes the second fail to start instead.
//!
//! Claims are `owner/<epoch>` objects created with put-if-absent, so exactly
//! one process takes an epoch. A crashed owner is taken over when it was a
//! process on this host that no longer exists; anything else needs a human,
//! since a live owner and an unreachable one look identical from here.

use std::io;
use std::sync::Arc;

use crate::s3::PutOutcome;
use crate::store::Store;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owner {
    pub host: String,
    pub pid: u32,
}

impl Owner {
    pub fn me() -> Owner {
        Owner {
            host: hostname(),
            pid: std::process::id(),
        }
    }
    fn encode(&self) -> Vec<u8> {
        format!("{}\n{}\n", self.host, self.pid).into_bytes()
    }
    fn decode(b: &[u8]) -> Option<Owner> {
        let s = String::from_utf8_lossy(b);
        let mut it = s.lines();
        Some(Owner {
            host: it.next()?.to_string(),
            pid: it.next()?.parse().ok()?,
        })
    }
    /// Whether this owner is definitely gone. Only answerable for a process on
    /// our own host; a different host is always "maybe alive".
    fn is_definitely_dead(&self, me: &Owner) -> bool {
        self.host == me.host && self.pid != me.pid && !pid_alive(self.pid)
    }
}

fn key(epoch: u64) -> String {
    format!("owner/{epoch:016x}")
}

fn epoch_of(k: &str) -> u64 {
    k.rsplit('/').next().and_then(|h| u64::from_str_radix(h, 16).ok()).unwrap_or(0)
}

#[derive(Debug)]
pub struct Lease {
    pub epoch: u64,
    /// `None` when the object exists but its body does not decode. The epoch
    /// is taken either way; who took it is the part that is unreadable.
    pub owner: Option<Owner>,
}

/// The newest epoch anything has claimed, and who claimed it if the object
/// says so.
///
/// The epoch comes from the key and the owner from the body, because they fail
/// separately: an owner object that does not decode still proves its epoch is
/// taken. Reporting no claim there sent `next` back to 1, where put-if-absent
/// lost every attempt and `acquire` blamed "too many writers competing" for
/// ever.
pub fn current(store: &Arc<dyn Store>) -> io::Result<Option<Lease>> {
    let mut keys = store.list("owner/")?;
    keys.sort_by(|a, b| b.key.cmp(&a.key));
    let Some(newest) = keys.first() else { return Ok(None) };
    let body = store.get(&newest.key)?.unwrap_or_default();
    Ok(Some(Lease { epoch: epoch_of(&newest.key), owner: Owner::decode(&body) }))
}

/// Takes the lease, or explains why it cannot.
pub fn acquire(store: &Arc<dyn Store>) -> io::Result<Lease> {
    let me = Owner::me();
    for _ in 0..8 {
        let held = current(store)?;
        let next = held.as_ref().map_or(1, |l| l.epoch + 1);

        if let Some(l) = &held {
            // An unreadable owner is never assumed dead: it is a live server
            // whose object we cannot parse just as easily as a corrupt one.
            let takeable = l.owner.as_ref().is_some_and(|o| *o == me || o.is_definitely_dead(&me));
            if !takeable {
                let who = match &l.owner {
                    Some(o) => format!("{}:{}", o.host, o.pid),
                    None => "an owner this version cannot read".to_string(),
                };
                return Err(io::Error::other(format!(
                    "this bucket is already owned by {who} (lease {}). Two servers \
                     sharing one bucket would overwrite each other's rows. If that \
                     process is definitely gone, delete the object `owner/{:016x}` \
                     from the bucket and start again.",
                    l.epoch, l.epoch
                )));
            }
            if l.owner.as_ref() == Some(&me) {
                return Ok(Lease { epoch: l.epoch, owner: Some(me) });
            }
        }

        // Losing this means someone claimed the same epoch first; look again.
        if store.put_if_absent(&key(next), &me.encode())? == PutOutcome::Written {
            return Ok(Lease { epoch: next, owner: Some(me) });
        }
    }
    Err(io::Error::other("could not claim the objkv lease; too many writers competing"))
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

/// Only ESRCH means gone. EPERM means the process exists and belongs to another
/// user -- treating that as dead would let one server steal a live lease.
fn pid_alive(pid: u32) -> bool {
    // SAFETY: signal 0 performs error checking only; it sends nothing.
    if unsafe { libc::kill(pid as i32, 0) } == 0 {
        return true;
    }
    io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemStore;

    fn store() -> Arc<dyn Store> {
        Arc::new(MemStore::new()) as Arc<dyn Store>
    }

    #[test]
    fn the_first_process_takes_the_lease() {
        let s = store();
        let l = acquire(&s).unwrap();
        assert_eq!(l.epoch, 1);
        assert_eq!(l.owner, Some(Owner::me()));
    }

    #[test]
    fn reopening_in_the_same_process_keeps_the_lease() {
        let s = store();
        assert_eq!(acquire(&s).unwrap().epoch, 1);
        assert_eq!(acquire(&s).unwrap().epoch, 1, "must not burn epochs");
    }

    #[test]
    fn a_live_owner_blocks_a_second_process() {
        let s = store();
        // A different pid on this host that is definitely running: our parent's
        // view of us is unavailable, so use pid 1, which always exists.
        let other = Owner { host: hostname(), pid: 1 };
        s.put_if_absent(&key(1), &other.encode()).unwrap();

        let err = acquire(&s).unwrap_err().to_string();
        assert!(err.contains("already owned by"), "got: {err}");
        assert!(err.contains("owner/0000000000000001"), "must say how to resolve");
    }

    #[test]
    fn a_dead_owner_is_taken_over_automatically() {
        let s = store();
        // A pid that cannot be running: above the platform maximum.
        let dead = Owner { host: hostname(), pid: 0x7fff_fffe };
        s.put_if_absent(&key(1), &dead.encode()).unwrap();

        let l = acquire(&s).unwrap();
        assert_eq!(l.epoch, 2, "takeover claims the next epoch");
        assert_eq!(l.owner, Some(Owner::me()));
    }

    #[test]
    fn another_host_is_never_assumed_dead() {
        let s = store();
        let elsewhere = Owner { host: "some-other-machine".into(), pid: 12345 };
        s.put_if_absent(&key(1), &elsewhere.encode()).unwrap();
        // No takeover, and no override either: an unreachable host and a busy
        // one are the same picture from here. The error names the object to
        // delete, which is a decision only a human can make.
        let err = acquire(&s).unwrap_err().to_string();
        assert!(err.contains("owner/0000000000000001"), "got: {err}");
    }

    #[test]
    fn an_owner_object_that_does_not_decode_still_holds_its_epoch() {
        // `current` used to answer "no claim" for this, so `next` was 1, every
        // put-if-absent lost, and the operator was told eight writers were
        // competing. The epoch is in the key; only the owner is unreadable.
        let s = store();
        s.put_if_absent(&key(1), b"not an owner object").unwrap();

        let held = current(&s).unwrap().expect("the epoch is taken");
        assert_eq!(held.epoch, 1);
        assert!(held.owner.is_none());

        let err = acquire(&s).unwrap_err().to_string();
        assert!(err.contains("owner/0000000000000001"), "must name the object: {err}");
        assert!(!err.contains("too many writers"), "must not blame contention: {err}");
    }

    #[test]
    fn owner_round_trips() {
        let o = Owner { host: "h".into(), pid: 7 };
        assert_eq!(Owner::decode(&o.encode()), Some(o));
        assert_eq!(Owner::decode(b"garbage"), None);
    }
}
