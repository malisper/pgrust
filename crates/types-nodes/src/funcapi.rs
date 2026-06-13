//! The `Tuplestorestate *` carrier.
//!
//! `Tuplestorestate` is private to `utils/sort/tuplestore.c`; everything else
//! in PostgreSQL holds it as an opaque pointer and threads it through the
//! `tuplestore_*` API. The owned model keeps that contract: this carrier's
//! payload is type-erased and reachable only through the [`begin`] /
//! [`payload`] surface, and only the owning tuplestore unit (when it lands)
//! names the concrete engine type, downcasting with a loud panic on mismatch.
//! Consumers never inspect the payload.
//!
//! [`begin`]: Tuplestorestate::begin
//! [`payload`]: Tuplestorestate::payload

use core::any::Any;

use mcx::{Mcx, PgBox};
use types_error::PgResult;

pub struct Tuplestorestate<'mcx> {
    /// The real owned store, type-erased and context-allocated (C:
    /// `tuplestore_begin_common` pallocs the state in the caller's current
    /// context); `None` for a default-constructed (not-yet-begun) carrier —
    /// the C `NULL` `Tuplestorestate *`.
    store: Option<PgBox<'mcx, dyn Any>>,
}

impl<'mcx> Tuplestorestate<'mcx> {
    /// `tuplestore_begin_*`-shaped construction: allocate the concrete engine
    /// state in `mcx` (C: palloc in `CurrentMemoryContext`) and type-erase
    /// it. Fallible: allocating. Only the owning tuplestore unit (or a test
    /// mock standing in for it) calls this.
    pub fn begin<T: Any>(mcx: Mcx<'mcx>, store: T) -> PgResult<Self> {
        let boxed = mcx::alloc_in(mcx, store)?;
        let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
        // Unsizing through the raw pointer: `PgBox` has no `CoerceUnsized` on
        // stable. SAFETY: `ptr` came from `into_raw_with_allocator` with the
        // same allocator; the cast only attaches the `dyn Any` vtable.
        let erased: PgBox<'mcx, dyn Any> = unsafe { PgBox::from_raw_in(ptr as *mut dyn Any, alloc) };
        Ok(Tuplestorestate {
            store: Some(erased),
        })
    }

    /// The type-erased engine state (the tuplestore owner downcasts; loud
    /// panic on mismatch is its job). `None` is the C `NULL` store.
    pub fn payload(&self) -> Option<&dyn Any> {
        self.store.as_deref()
    }

    /// Mutable [`Self::payload`].
    pub fn payload_mut(&mut self) -> Option<&mut (dyn Any + 'static)> {
        self.store.as_deref_mut()
    }
}

impl Default for Tuplestorestate<'_> {
    /// The C `Tuplestorestate *tuplestorestate = NULL` initial state.
    fn default() -> Self {
        Tuplestorestate { store: None }
    }
}

impl core::fmt::Debug for Tuplestorestate<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.store {
            Some(_) => f.write_str("Tuplestorestate(<owned store>)"),
            None => f.write_str("Tuplestorestate(<empty>)"),
        }
    }
}

impl Clone for Tuplestorestate<'_> {
    /// An empty carrier clones freely. A live store has no C clone counterpart
    /// — `tuplestore.c` never copies a store, and C struct assignment of the
    /// holder would alias the pointer, which owned values cannot express — so
    /// cloning a live store stops loud.
    fn clone(&self) -> Self {
        match self.store {
            None => Tuplestorestate { store: None },
            Some(_) => panic!(
                "Tuplestorestate: cannot clone a live tuplestore \
                 (tuplestore.c has no copy operation; C would alias the pointer)"
            ),
        }
    }
}
