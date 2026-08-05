/*
 * Link shims for cref_tidstore: the backend surface the vendored
 * mcxt/aset/slab/bump/tidstore TUs demand. The bench never errors and never
 * touches the shared (dsa) flavor, so the ereport surface aborts on ERROR and
 * every dsa/LWLock symbol is an abort stub.
 */
#include "postgres.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"
#include "utils/palloc.h"
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#undef vsnprintf
#undef snprintf
#undef vfprintf
#undef fprintf
#undef vprintf
#undef printf
#undef vsprintf
#undef sprintf
#undef strerror_r
#undef qsort

static int cur_elevel;

ErrorContextCallback *error_context_stack = NULL;
volatile uint32 InterruptPending = 0;
bool LogMemoryContextPending = false;
int MyProcPid = 12345;
sigjmp_buf *PG_exception_stack = NULL;

bool errstart(int elevel, const char *domain) { cur_elevel = elevel; return true; }
bool errstart_cold(int elevel, const char *domain) { cur_elevel = elevel; return true; }
int errcode(int sqlerrcode) { return 0; }
int errdetail(const char *fmt, ...) { return 0; }
int errhint(const char *fmt, ...) { return 0; }
static char err_msg[4096];
int errmsg(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(err_msg, sizeof(err_msg), fmt, ap);
	va_end(ap);
	return 0;
}
int errmsg_internal(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(err_msg, sizeof(err_msg), fmt, ap);
	va_end(ap);
	return 0;
}
void errfinish(const char *filename, int lineno, const char *funcname)
{
	if (cur_elevel >= ERROR)
	{
		fprintf(stderr, "cref_tidstore: unexpected ERROR: %s\n", err_msg);
		abort();
	}
}
void pg_re_throw(void)
{
	fprintf(stderr, "cref_tidstore: unexpected re-throw\n");
	abort();
}
bool stack_is_too_deep(void) { return false; }
void check_stack_depth(void) {}

int pg_fprintf(FILE *stream, const char *fmt, ...)
{
	va_list ap;
	int r;
	va_start(ap, fmt);
	r = vfprintf(stream, fmt, ap);
	va_end(ap);
	return r;
}
int pg_printf(const char *fmt, ...)
{
	va_list ap;
	int r;
	va_start(ap, fmt);
	r = vprintf(fmt, ap);
	va_end(ap);
	return r;
}
int pg_vsnprintf(char *str, size_t count, const char *fmt, va_list args)
{
	return vsnprintf(str, count, fmt, args);
}
int pg_snprintf(char *str, size_t count, const char *fmt, ...)
{
	va_list ap;
	int r;
	va_start(ap, fmt);
	r = vsnprintf(str, count, fmt, ap);
	va_end(ap);
	return r;
}
int pg_vfprintf(FILE *stream, const char *fmt, va_list args)
{
	return vfprintf(stream, fmt, args);
}

char *psprintf(const char *fmt, ...)
{
	char buf[1024];
	va_list ap;
	int n;
	char *result;
	va_start(ap, fmt);
	n = vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);
	if (n < 0 || n >= (int) sizeof(buf))
		abort();
	result = palloc(n + 1);
	memcpy(result, buf, n + 1);
	return result;
}

char *pg_strerror_r(int errnum, char *buf, size_t buflen)
{ strerror_r(errnum, buf, buflen); return buf; }

#define PANIC_STUB(name) \
	{ fprintf(stderr, "cref_tidstore: unexpected call to " #name "\n"); abort(); }

dsa_area *dsa_create_ext(int tranche_id, size_t init_segment_size, size_t max_segment_size) PANIC_STUB(dsa_create_ext)
dsa_area *dsa_attach(dsa_handle handle) PANIC_STUB(dsa_attach)
void dsa_detach(dsa_area *area) PANIC_STUB(dsa_detach)
dsa_pointer dsa_allocate_extended(dsa_area *area, size_t size, int flags) PANIC_STUB(dsa_allocate_extended)
void dsa_free(dsa_area *area, dsa_pointer dp) PANIC_STUB(dsa_free)
void *dsa_get_address(dsa_area *area, dsa_pointer dp) PANIC_STUB(dsa_get_address)
size_t dsa_get_total_size(dsa_area *area) PANIC_STUB(dsa_get_total_size)
void LWLockInitialize(LWLock *lock, int tranche_id) PANIC_STUB(LWLockInitialize)
bool LWLockAcquire(LWLock *lock, LWLockMode mode) PANIC_STUB(LWLockAcquire)
void LWLockRelease(LWLock *lock) PANIC_STUB(LWLockRelease)
bool LWLockHeldByMe(LWLock *lock) PANIC_STUB(LWLockHeldByMe)
bool LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode) PANIC_STUB(LWLockHeldByMeInMode)

int errhidecontext(bool h) { return 0; }
int errhidestmt(bool h) { return 0; }
int pg_mbcliplen(const char *mbstr, int len, int limit)
{ return len < limit ? len : limit; }
