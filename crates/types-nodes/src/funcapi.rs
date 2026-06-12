//! The `Tuplestorestate *` carrier.
//!
//! `Tuplestorestate` is private to `utils/sort/tuplestore.c`; everything else
//! in PostgreSQL holds it as an opaque pointer and threads it through the
//! `tuplestore_*` API. The owned model keeps that contract: this carrier's
//! payload is type-erased, and only the owning tuplestore unit (when it lands)
//! names the concrete engine type, downcasting with a loud panic on mismatch.
//! Consumers never inspect the payload.

use alloc::boxed::Box;

pub struct Tuplestorestate {
    /// The real owned store, type-erased; `None` for a default-constructed
    /// (not-yet-begun) carrier — the C `NULL` `Tuplestorestate *`.
    pub store: Option<Box<dyn core::any::Any>>,
}

impl Default for Tuplestorestate {
    /// The C `Tuplestorestate *tuplestorestate = NULL` initial state.
    fn default() -> Self {
        Tuplestorestate { store: None }
    }
}

impl core::fmt::Debug for Tuplestorestate {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.store {
            Some(_) => f.write_str("Tuplestorestate(<owned store>)"),
            None => f.write_str("Tuplestorestate(<empty>)"),
        }
    }
}

impl Clone for Tuplestorestate {
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

/// `ReturnSetInfo` (nodes/execnodes.h) — the node passed as
/// `fcinfo->resultinfo` when calling a function that might return a set.
/// Trimmed (docs/types.md rule 3) to the Materialize-mode result fields
/// current ports consume — the fields `InitMaterializedSRF` fills and SRF
/// bodies hand to `tuplestore_putvalues`. The funcapi/executor ports widen it
/// (`econtext`, `expectedDesc`, `allowedModes`, `returnMode`, `isDone`).
#[derive(Debug, Default)]
pub struct ReturnSetInfo<'mcx> {
    /// `Tuplestorestate *setResult` — holds the complete returned tuple set.
    /// The carrier's empty state is the C `NULL` pointer.
    pub setResult: Tuplestorestate,
    /// `TupleDesc setDesc` — actual descriptor for returned tuples (`None`
    /// is the C `NULL`).
    pub setDesc: types_tuple::heaptuple::TupleDesc<'mcx>,
}
