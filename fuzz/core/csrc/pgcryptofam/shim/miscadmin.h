/*
 * SHIM miscadmin.h — NOT PostgreSQL code.
 *
 * CHECK_FOR_INTERRUPTS(): no-op. In the backend this services statement
 * cancel/terminate during the long crypt loops (crypt-des do_des,
 * crypt-blowfish key schedule, crypt-sha rounds). The fuzz harness has no
 * signal plane; runaway cost is prevented up front by the driver via
 * pg_diff_pgcryptofam_cost_probe, so the interrupt hook is dead by
 * construction, not silently swallowed.
 */
#ifndef PGCRYPTOFAM_SHIM_MISCADMIN_H
#define PGCRYPTOFAM_SHIM_MISCADMIN_H

#define CHECK_FOR_INTERRUPTS() ((void) 0)

#endif
