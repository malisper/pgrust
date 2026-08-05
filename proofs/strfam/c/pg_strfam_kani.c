/*
 * Vendored PostgreSQL C for the strfam Kani equivalence harnesses — the
 * CBMC-tractable subset of the p1-lanec string-family batch (the string/
 * snprintf/longjmp members are fuzz-routed; see fuzz/core/csrc/pg_strfam.c).
 *
 * Provenance, from ../pgrust-fabled/vendor/postgres-src @
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (REL_18 "Stamp 18.3"):
 *   - src/common/wait_error.c: wait_result_is_signal,
 *     wait_result_is_any_signal, wait_result_to_exit_code — VERBATIM.
 *     The W* observables are the host <sys/wait.h> macros, exactly what the
 *     C build uses (and what the shipped Rust binds through the libc crate).
 *   - src/common/relpath.c: forkNames — VERBATIM (the strings
 *     forkname_chars must agree with).
 *
 * pg_kani_* wrappers are shim plumbing (macro observables + array reads);
 * bodies above them are the vendored logic.
 */

#include <stdbool.h>
#include <string.h>
#include <sys/wait.h>

/* ---- src/common/wait_error.c — VERBATIM ---- */

bool
wait_result_is_signal(int exit_status, int signum)
{
	if (WIFSIGNALED(exit_status) && WTERMSIG(exit_status) == signum)
		return true;
	if (WIFEXITED(exit_status) && WEXITSTATUS(exit_status) == 128 + signum)
		return true;
	return false;
}

bool
wait_result_is_any_signal(int exit_status, bool include_command_not_found)
{
	if (WIFSIGNALED(exit_status))
		return true;
	if (WIFEXITED(exit_status) &&
		WEXITSTATUS(exit_status) > (include_command_not_found ? 125 : 128))
		return true;
	return false;
}

int
wait_result_to_exit_code(int exit_status)
{
	if (exit_status == -1)
		return -1;				/* failure of pclose() or system() */
	if (WIFEXITED(exit_status))
		return WEXITSTATUS(exit_status);
	if (WIFSIGNALED(exit_status))
		return 128 + WTERMSIG(exit_status);
	/* On many systems, this is unreachable */
	return -1;
}

/* ---- src/common/relpath.c forkNames — VERBATIM (indices 0..3) ---- */

static const char *const forkNames[] = {
	"main",
	"fsm",
	"vm",
	"init",
};

/* ---- pg_kani_* shim wrappers ---- */

int
pg_kani_wifexited(int status)
{
	return WIFEXITED(status) ? 1 : 0;
}

int
pg_kani_wexitstatus(int status)
{
	return WEXITSTATUS(status);
}

int
pg_kani_wifsignaled(int status)
{
	return WIFSIGNALED(status) ? 1 : 0;
}

int
pg_kani_wtermsig(int status)
{
	return WTERMSIG(status);
}

int
pg_kani_wait_result_is_signal(int exit_status, int signum)
{
	return wait_result_is_signal(exit_status, signum) ? 1 : 0;
}

int
pg_kani_wait_result_is_any_signal(int exit_status, int include_cnf)
{
	return wait_result_is_any_signal(exit_status, include_cnf != 0) ? 1 : 0;
}

int
pg_kani_wait_result_to_exit_code(int exit_status)
{
	return wait_result_to_exit_code(exit_status);
}

/* forkNames[i] byte at position j (0-terminated); avoids symbolic strcmp */
int
pg_kani_forkname_byte(int fork, int j)
{
	return (int) (unsigned char) forkNames[fork][j];
}
