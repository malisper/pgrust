/*
 * pg_oracle_guard.c: holder bookkeeping for the oracle serialization lock
 * (fuzz plumbing, NOT Postgres code). See pg_oracle_guard.h for the
 * contract and provenance.
 *
 * pg_oracle_guard_enter/exit are called by the Rust OracleSerial guard
 * (fuzz/core/src/lib.rs) at depth-0 acquire/release, both while the mutex
 * is held, so the stores never race each other; the check's load races
 * only in the defect case it exists to catch (an unserialized entry), and
 * any torn/stale read there still compares != pthread_self() and fires.
 */
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>

static _Atomic(uintptr_t) pg_oracle_guard_holder;

void
pg_oracle_guard_enter(void)
{
	atomic_store_explicit(&pg_oracle_guard_holder,
						  (uintptr_t) pthread_self(),
						  memory_order_release);
}

void
pg_oracle_guard_exit(void)
{
	atomic_store_explicit(&pg_oracle_guard_holder, 0, memory_order_release);
}

/* Rust hook (fuzz/core/src/lib.rs): panics naming entry + calling test. */
extern void pgf_oracle_guard_violation(const char *entry);

void
pg_oracle_guard_check(const char *entry)
{
	if (atomic_load_explicit(&pg_oracle_guard_holder, memory_order_acquire)
		!= (uintptr_t) pthread_self())
		pgf_oracle_guard_violation(entry);
}

/*
 * Must-fail control target: an "oracle entry" with no state, so the
 * enforcement tests (fuzz/core/src/oracle_guard_tests.rs) can drive the
 * check in both directions without touching a real oracle.
 */
#include "pg_oracle_guard.h"

void
pg_oracle_guard_probe(void)
{
	PG_ORACLE_GUARD_CHECK(__func__);
}
