/*
 * pgcryptofam_shim.h — internal interface between the pgcryptofam oracle's
 * driver entries (pg_diff_pgcryptofam.c) and the harness plumbing
 * (pgcryptofam_shim.c). NOT PostgreSQL code.
 */
#ifndef PGCRYPTOFAM_SHIM_H
#define PGCRYPTOFAM_SHIM_H

#include <setjmp.h>
#include <stddef.h>
#include <stdint.h>

#define PGCRYPTOFAM_MSG_CAP 512

/*
 * Status struct shared with the Rust driver (fuzz/core/src/pgcryptofam.rs
 * mirrors this layout exactly; repr(C), all-i32 head + two fixed byte
 * arrays).
 *
 *   ok                    1 = returned normally, 0 = ereport(>=ERROR) raised
 *   sqlstate              MAKE_SQLSTATE-encoded code of the raised error
 *   error_elevel          21 ERROR / 22 FATAL / 23 PANIC; 0 when ok
 *   notice_count          number of NOTICE/WARNING reports recorded
 *   notice_sqlstate       sqlstate of the LAST notice (0 if none)
 *   elevel_of_last_notice 18 NOTICE / 19 WARNING (0 if none)
 *   notice_text           errmsg text of the last notice (NUL-terminated)
 *   msg                   errmsg text of the raised error (triage plane)
 */
typedef struct PgcryptofamStatus
{
	int32_t		ok;
	int32_t		sqlstate;
	int32_t		error_elevel;
	int32_t		notice_count;
	int32_t		notice_sqlstate;
	int32_t		elevel_of_last_notice;
	char		notice_text[PGCRYPTOFAM_MSG_CAP];
	char		msg[PGCRYPTOFAM_MSG_CAP];
} PgcryptofamStatus;

/* arena */
extern void pgcryptofam_arena_reset(void);

/* error channel: arm returns the jmp_buf the entry must sigsetjmp on */
extern sigjmp_buf *pgcryptofam_arm(PgcryptofamStatus *st);

/* entropy injection consumed by the pg_strong_random mock */
extern void pgcryptofam_set_entropy(const unsigned char *buf, size_t len);

#endif							/* PGCRYPTOFAM_SHIM_H */
