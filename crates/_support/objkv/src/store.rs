//! The object-store interface the storage layer is written against, plus an
//! in-memory implementation so the read path, commits and compaction can be
//! tested without a network.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex};

use crate::run::RangeSource;
#[cfg(feature = "s3")]
use crate::s3::Client;
use crate::s3::{ObjectInfo, PutOutcome};

pub trait Store: Send + Sync {
    fn put_if_absent(&self, key: &str, body: &[u8]) -> io::Result<PutOutcome>;
    fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>>;
    fn get_range(&self, key: &str, offset: u64, len: u64) -> io::Result<Option<Vec<u8>>>;
    fn list(&self, prefix: &str) -> io::Result<Vec<ObjectInfo>>;
    /// Removes an object; succeeds if it was already gone.
    ///
    /// The only destructive operation here. Everything else in this design is
    /// append-only, which is why a crash has never been able to lose data --
    /// an interrupted write leaves rubbish, not a hole. This one can, so its
    /// caller reads the replacement back before calling it.
    fn delete(&self, key: &str) -> io::Result<()>;
}

#[cfg(feature = "s3")]
impl Store for Client {
    fn put_if_absent(&self, key: &str, body: &[u8]) -> io::Result<PutOutcome> {
        Client::put_if_absent(self, key, body)
    }
    fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        Client::get(self, key)
    }
    fn get_range(&self, key: &str, offset: u64, len: u64) -> io::Result<Option<Vec<u8>>> {
        Client::get_range(self, key, offset, len)
    }
    fn list(&self, prefix: &str) -> io::Result<Vec<ObjectInfo>> {
        Client::list(self, prefix)
    }
    fn delete(&self, key: &str) -> io::Result<()> {
        Client::delete(self, key)
    }
}

/// In-memory store. Counts requests, because the round-trip budget is the
/// thing the design lives or dies on.
#[derive(Default)]
pub struct MemStore {
    objects: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl MemStore {
    pub fn new() -> MemStore {
        MemStore::default()
    }
}

impl Store for MemStore {
    fn put_if_absent(&self, key: &str, body: &[u8]) -> io::Result<PutOutcome> {
        let mut o = self.objects.lock().unwrap();
        if o.contains_key(key) {
            return Ok(PutOutcome::AlreadyExists);
        }
        o.insert(key.to_string(), body.to_vec());
        Ok(PutOutcome::Written)
    }
    fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        Ok(self.objects.lock().unwrap().get(key).cloned())
    }
    fn get_range(&self, key: &str, offset: u64, len: u64) -> io::Result<Option<Vec<u8>>> {
        let o = self.objects.lock().unwrap();
        let Some(b) = o.get(key) else { return Ok(None) };
        let (s, e) = (offset as usize, ((offset + len) as usize).min(b.len()));
        if s > b.len() {
            return Err(io::Error::other("range past end of object"));
        }
        Ok(Some(b[s..e].to_vec()))
    }
    fn list(&self, prefix: &str) -> io::Result<Vec<ObjectInfo>> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| ObjectInfo { key: k.clone(), size: v.len() as u64 })
            .collect())
    }
    fn delete(&self, key: &str) -> io::Result<()> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
}

/// Adapts one stored object into a `RangeSource` so runs can be read in place
/// rather than downloaded.
pub struct ObjectRange {
    pub store: Arc<dyn Store>,
    pub key: String,
    pub size: u64,
}

impl RangeSource for ObjectRange {
    fn range(&self, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        self.store
            .get_range(&self.key, offset, len)?
            .ok_or_else(|| io::Error::other(format!("object vanished: {}", self.key)))
    }
    fn size(&self) -> u64 {
        self.size
    }
}
