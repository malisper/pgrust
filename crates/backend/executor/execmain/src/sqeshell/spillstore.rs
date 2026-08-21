//! [P6-1 spill] The server's spill substrate for the sqe grouped spill
//! arm (spill-design.md §3): `sqe::spill::SpillStore` backed by
//! `sqe_spill`'s fd/`pgsql_tmp` FileSet — temp_file_limit accounting,
//! resowner registration, and the pgsql_tmp reaper engage by
//! construction (O-M2-3). One store per statement engagement; the last
//! drop of the set deletes the whole tree (teardown paths 1-4; path 5 is
//! the startup reaper). Media are plain data between events: every
//! append/read call opens, works, and closes within the call on the
//! calling thread (the m3.5 §2 handle law) — pool workers are armed for
//! temp-file access by `arm_pool_worker_fd`.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Register the fd-backed factory (idempotent; called with the pool
/// arming install). A backend that cannot create a `SpillSet` at
/// statement time yields `None` from the factory — the engine's planner
/// law then refuses typed (fail-closed, never a stumble mid-I/O).
pub(crate) fn register_spill_store() {
    sqe::spill::set_store_factory(factory);
}

fn factory() -> Option<Arc<dyn sqe::spill::SpillStore>> {
    static GEN: AtomicU64 = AtomicU64::new(0);
    let set = sqe_spill::SpillSet::create().ok()?;
    Some(Arc::new(FdStore {
        set,
        generation: GEN.fetch_add(1, Ordering::Relaxed),
        ctr: AtomicU32::new(0),
    }))
}

struct FdStore {
    set: Arc<sqe_spill::SpillSet>,
    generation: u64,
    ctr: AtomicU32,
}

impl sqe::spill::SpillStore for FdStore {
    fn file(
        &self,
        purpose: &'static str,
        worker: usize,
    ) -> std::io::Result<Box<dyn sqe::spill::SpillMedium>> {
        // The collision-free naming law: (node, generation, purpose,
        // worker) — the store's own counter serves as the node ordinal.
        let name = sqe_spill::spill_file_name(
            self.ctr.fetch_add(1, Ordering::Relaxed),
            self.generation,
            purpose,
            worker as u32,
        );
        let file = sqe_spill::SpillFile::new(self.set.clone(), name);
        Ok(Box::new(FdMedium { f: Mutex::new(file) }))
    }
}

struct FdMedium {
    /// The descriptor + committed watermark. Single WRITER per medium by
    /// the naming law; the mutex is only the `&self` seam (readers take
    /// it briefly to clone the committed snapshot).
    f: Mutex<sqe_spill::SpillFile>,
}

fn to_io(e: Box<::types_error::PgError>) -> std::io::Error {
    std::io::Error::other(format!("spill io: {}", e.message()))
}

impl sqe::spill::SpillMedium for FdMedium {
    fn append(&self, bytes: &[u8]) -> std::io::Result<u64> {
        let mut f = self.f.lock().unwrap();
        let off = f.committed();
        let mut w = f.append().map_err(to_io)?;
        w.write(bytes).map_err(to_io)?;
        w.finish().map_err(to_io)?;
        Ok(off)
    }

    fn read_at(&self, off: u64, buf: &mut [u8]) -> std::io::Result<()> {
        // One read event over the committed snapshot, on this thread.
        let mut rd = self.f.lock().unwrap().open_read();
        let r = rd.read_at(off, buf).map_err(to_io);
        let _ = rd.close();
        r
    }
}
