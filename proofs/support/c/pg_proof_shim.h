/*
 * pg_proof_shim.h — shared C-side shim boilerplate for the Kani proof suite.
 *
 * Family crates include it via a relative path, e.g. from
 * proofs/<family>/c/pg_<family>.c:
 *
 *     #include "../../support/c/pg_proof_shim.h"
 *
 * SCOPE RULE (prove-target skill): this header supplies PLUMBING ONLY —
 * typedefs, no-op Assert, ctype helpers, tiny utility macros. Vendored
 * PostgreSQL function BODIES stay verbatim in each family's .c file; a
 * family's file header still documents its provenance and every
 * function-local shim (ereport rewires, fmgr unwrapping, palloc->buffer).
 * Nothing here may replace logic under proof.
 *
 * Deliberately NOT defined here: Datum / fmgr types (proofs unwrap fmgr on
 * the C side into plain scalar signatures), palloc/StringInfo (families
 * shim those per call site with caller-provided buffers), ereport (see the
 * PROOF_EREPORT_FLAG convention below).
 */

#ifndef PG_PROOF_SHIM_H
#define PG_PROOF_SHIM_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/* ---- postgres c.h scalar typedefs ---- */
typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef uint32_t Oid;           /* postgres: typedef unsigned int Oid */
typedef size_t Size;

/* ---- assertions: compiled out, exactly like a production (non-cassert)
 * postgres build. An Assert whose condition matters to the proof must be
 * turned into an explicit check by the family (it is then a finding that
 * C relies on it). ---- */
#define Assert(condition) ((void) 0)
#define AssertMacro(condition) ((void) 0)

/* ---- branch hints: value-neutral ---- */
#define likely(x) (x)
#define unlikely(x) (x)

/* ---- c.h / varatt.h utility macros, verbatim semantics ---- */
#define VARHDRSZ ((size_t) 4)
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))

/*
 * ---- PROOF_EREPORT_FLAG: the suite's ereport(ERROR, ...) convention ----
 *
 * Where vendored C raises ereport(ERROR, ...), the shimmed function takes an
 * `int *err` out-param (initialized to 0 by the harness) and replaces the
 * ereport with:
 *
 *     PROOF_EREPORT_FLAG(err);        // then `return <sentinel>;`
 *
 * at the EXACT program point of the ereport (C's control flow aborts via
 * longjmp there; the shim aborts via early return at the same point). The
 * harness asserts flag-parity against the Rust Err arm. Error message TEXT
 * never crosses this seam — only the verdict does; families wanting
 * sqlstate parity report a distinct flag value per errcode.
 */
#define PROOF_EREPORT_FLAG(errp) do { *(errp) = 1; } while (0)

/*
 * ---- C-locale <ctype.h> replacements ----
 *
 * Kani/CBMC has no libc model, so vendored C calling isspace/isdigit/... is
 * otherwise blocked. Postgres parses in the C locale, where these have the
 * fixed ASCII meaning implemented here (bytes >= 128 are never
 * space/digit/hexdigit, and tolower only folds A-Z). Families textually
 * replace the libc call with the pg_proof_* spelling and log that as a shim;
 * the replacement is total over unsigned char so no behavior leaves the
 * proof. Signatures take int like libc (values 0..=255; EOF not supported —
 * postgres never passes it from parser loops).
 */
static inline int
pg_proof_isspace(int c)
{
	return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r';
}

static inline int
pg_proof_isdigit(int c)
{
	return c >= '0' && c <= '9';
}

static inline int
pg_proof_isxdigit(int c)
{
	return (c >= '0' && c <= '9') ||
		(c >= 'a' && c <= 'f') ||
		(c >= 'A' && c <= 'F');
}

static inline int
pg_proof_tolower(int c)
{
	return (c >= 'A' && c <= 'Z') ? c + ('a' - 'A') : c;
}

#endif							/* PG_PROOF_SHIM_H */
