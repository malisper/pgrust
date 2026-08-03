/*
 * jsonfam shim miscadmin.h — check_stack_depth() for the vendored parser.
 *
 * CONTRACT (all citations = vendor/postgres-src, PostgreSQL 18.3):
 *   src/backend/utils/misc/stack_depth.c:108-137  stack_is_too_deep():
 *     measures |stack_base_ptr - &own_local| in BYTES against
 *     max_stack_depth_bytes; the base==NULL test comes last (inert until
 *     armed). stack_depth.c:94-106 check_stack_depth(): raises a CATCHABLE
 *     ereport(ERROR, ERRCODE_STATEMENT_TOO_COMPLEX, "stack depth limit
 *     exceeded"). The base is armed once per backend (set_stack_base,
 *     stack_depth.c:43-65, called from main()).
 *
 * THE BOUND IS 2048 kB — the EFFECTIVE server default, not the 100 kB boot
 * value:
 *   src/backend/utils/misc/stack_depth.c:26,29: boot value 100 kB;
 *   src/backend/utils/misc/guc_tables.c:2615-2618: "We use the
 *     hopefully-safely-small value of 100kB as the compiled-in default for
 *     max_stack_depth.  InitializeGUCOptions will increase it if possible";
 *   src/backend/utils/misc/guc.c:1589 InitializeGUCOptionsFromEnvironment,
 *     rlimit block at :1613-1635: raises it to
 *     min((RLIMIT_STACK - STACK_DEPTH_SLOP)/1024, 2048) kB whenever that
 *     exceeds 100, with STACK_DEPTH_SLOP = 512*1024
 *     (src/include/miscadmin.h:297). On any normal >= 2.5 MiB-rlimit
 *     platform a real backend therefore runs with max_stack_depth = 2048 kB.
 * A 100 kB shim bound (the refuted first fix, 515fffe6d6a) fires 54001 where
 * a real server keeps parsing — it manufactures oracle-side errors PG never
 * raises. 2048 kB matches the server default and the in-tree precedent
 * (nodesfam_diff.rs rearm_stack_bases pins BOTH sides to 2048 kB).
 *
 * WHY BYTES AND NOT A FRAME COUNT: the previous shim counted frames and
 * fired at 100000. Frames relate to bytes by a per-frame size the shim does
 * not know and the compiler is free to change; measured on this TU (clang
 * -O2, arm64, task #131 probe) one JSON nesting level costs 96.0 bytes (the
 * 2048 kB budget first fires at nesting 21846), so the counter needed
 * ~9.2 MiB to fire while the oracle SIGBUSes near 2 MiB on a default Rust
 * spawned-thread stack. The guard was DEAD on every stack the harness uses:
 * an oracle CRASH stood where PostgreSQL raises a catchable 54001. Same
 * defect class as task #76 (regex frame-count cap dead on 2 MiB stacks).
 *
 * HARNESS GEOMETRY: the guard can only fire before physical exhaustion on a
 * stack larger than 2048 kB + slop, i.e. the libFuzzer main thread or a
 * dedicated big-stack test thread (json_diff.rs spawns 16 MiB for the deep
 * pins, mirroring nodesfam). On a default 2 MiB libtest thread the bound is
 * unreachable — and also irrelevant: json_diff caps input at MAX_LEN = 1024
 * => nesting <= 512 => ~49 kB, far under the bound on BOTH sides. The
 * driver arms the Rust side to the same 2048 kB (json_diff.rs), so the
 * stack-depth plane is two-sided, unlike the refuted fix which armed only C.
 */
#ifndef PG_JSONFAM_MISCADMIN_H
#define PG_JSONFAM_MISCADMIN_H

#include "postgres.h"

/* PG's effective max_stack_depth default in bytes (2048 kB; see header). */
#define PG_JSONFAM_MAX_STACK_DEPTH_BYTES ((ptrdiff_t) (2048 * 1024))

/* Armed per oracle entry by PG_JSONFAM_ENTRY (pg_json_io.c), the way a
 * backend arms it once in main(). NULL => inert, exactly C's base==NULL
 * arm (stack_depth.c:132-133). */
extern _Thread_local const char *pg_jsonfam_stack_base;

extern void pg_jsonfam_set_stack_base(void);

/* Deliberately NOT inline: like C's stack_is_too_deep it must have a real
 * frame of its own to sample, and check_stack_depth calling it adds the same
 * one constant frame upstream does. */
extern bool pg_jsonfam_stack_is_too_deep(void);

static inline void
check_stack_depth(void)
{
	if (pg_jsonfam_stack_is_too_deep())
		pg_jsonfam_error_fire(ERRCODE_STATEMENT_TOO_COMPLEX);
}

#endif
