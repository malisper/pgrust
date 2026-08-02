/*
 * pgcryptofam_shim.c — harness plumbing for the pgcryptofam_diff oracle
 * (lane p1-pgcryptofam). NOT comparison logic; the compared bodies are the
 * verbatim 18.3 TUs under vendor/.
 *
 * Contents, labeled per function:
 *   [ARENA]    per-exec bump arena behind palloc/pfree (contribb precedent)
 *   [CHANNEL]  setjmp ereport channel recording (sqlstate, elevel, text)
 *   [VERBATIM] CheckBuiltinCryptoMode — copied byte-for-byte from
 *              contrib/pgcrypto/openssl.c (18.3); builtin_crypto_enabled
 *              carries its verbatim pgcrypto.c initializer BC_ON, under
 *              which the function returns immediately and the FIPS arm is
 *              dead code.
 *   [MOCK]     px_find_digest — ENVIRONMENT MOCK of openssl.c's EVP-backed
 *              digest provider, implemented over the vendored verbatim
 *              src/common cryptohash primitives. The digests themselves
 *              (md5/sha1/sha2 families) are algorithm-standard, so the
 *              PX_MD observable behavior (result_size/block_size/reset/
 *              update/finish bytes) is identical to the EVP provider for
 *              every algorithm this cone requests ("md5", "sha256",
 *              "sha512" — all fixed literals in crypt-md5.c/crypt-sha.c).
 *   [MOCK]     px_find_cipher — cipher registry stub (PXE_NO_CIPHER);
 *              reachable only through px_find_combo, which no exported
 *              entry calls. Exists so the verbatim px.c TU links.
 *   [MOCK]     CheckFIPSMode — OpenSSL FIPS introspection; unreachable
 *              under the pinned BC_ON (see above). Returns false.
 *   [MOCK]     pg_strong_random — satisfied from the caller-injected
 *              entropy buffer (see pgcryptofam_set_entropy); returns false
 *              when the injected bytes run out, which surfaces as PG's own
 *              PXE_NO_RANDOM path.
 *   [MOCK]     pg_mblen_cstr — returns 1 == pg_wchar_table[PG_SQL_ASCII]
 *              .mblen under the campaign's pinned single-byte encoding;
 *              feeds only errmsg text (triage plane).
 *   [MOCK]     pgcryptofam_strerror_r — GNU-semantics strerror_r for
 *              snprintf.c's %m arm; unreachable from every format string
 *              in this cone.
 */
#include "postgres.h"

#include <setjmp.h>

#include "pgcryptofam_shim.h"

#include "common/cryptohash.h"
#include "vendor/px.h"

/* ------------------------------------------------------------------ */
/* [ARENA] per-exec bump arena                                         */
/* ------------------------------------------------------------------ */

#define PGCRYPTOFAM_ARENA_CAP (64u << 20)	/* 64 MiB; PX_MAX_CRYPT-scale
											 * allocations plus StringInfo
											 * doubling for armor of multi-MiB
											 * frames */

static _Thread_local unsigned char *arena_base = NULL;
static _Thread_local size_t arena_used = 0;

/* every chunk carries its size so repalloc can copy the old contents */
typedef struct
{
	size_t		size;
	/* payload MAXALIGNed after this header (header is 16 bytes) */
	size_t		pad;
} arena_hdr;

void
pgcryptofam_arena_reset(void)
{
	if (arena_base == NULL)
	{
		arena_base = malloc(PGCRYPTOFAM_ARENA_CAP);
		if (arena_base == NULL)
		{
			fprintf(stderr, "pgcryptofam arena: initial malloc failed\n");
			abort();
		}
	}
	arena_used = 0;
}

static void *
arena_alloc(size_t size)
{
	arena_hdr  *h;
	size_t		need;

	/* MAXALIGN the payload size (8-byte alignment, both target ABIs) */
	need = sizeof(arena_hdr) + ((size + 7u) & ~(size_t) 7u);
	if (arena_base == NULL)
		pgcryptofam_arena_reset();
	if (need > PGCRYPTOFAM_ARENA_CAP - arena_used)
	{
		/*
		 * Arena exhaustion is a harness capacity failure, not an oracle
		 * verdict: fail loud instead of fabricating an OOM error plane the
		 * real backend would not produce at these input sizes.
		 */
		fprintf(stderr, "pgcryptofam arena: exhausted (%zu + %zu)\n",
				arena_used, need);
		abort();
	}
	h = (arena_hdr *) (arena_base + arena_used);
	arena_used += need;
	h->size = size;
	return (void *) (h + 1);
}

void *
pgcryptofam_palloc(Size size)
{
	return arena_alloc(size);
}

void *
pgcryptofam_palloc0(Size size)
{
	void	   *p = arena_alloc(size);

	memset(p, 0, size);
	return p;
}

void *
pgcryptofam_palloc_extended(Size size, int flags)
{
	/* flags (NO_OOM / ZERO / HUGE) unreachable from this cone's callers */
	(void) flags;
	return arena_alloc(size);
}

void *
pgcryptofam_repalloc(void *ptr, Size size)
{
	arena_hdr  *h = ((arena_hdr *) ptr) - 1;
	void	   *p;

	if (h->size >= size)
	{
		h->size = size;
		return ptr;
	}
	p = arena_alloc(size);
	memcpy(p, ptr, h->size);
	return p;
}

void
pgcryptofam_pfree(void *ptr)
{
	/* bump arena: reclaimed wholesale at the next entry's arena_reset */
	(void) ptr;
}

char *
pgcryptofam_pstrdup(const char *s)
{
	size_t		n = strlen(s) + 1;
	char	   *p = arena_alloc(n);

	memcpy(p, s, n);
	return p;
}

/* ------------------------------------------------------------------ */
/* [CHANNEL] setjmp ereport channel                                    */
/* ------------------------------------------------------------------ */

static _Thread_local sigjmp_buf *err_jmp = NULL;

/* pending report being assembled between errstart and errfinish */
static _Thread_local int pending_sqlstate;
static _Thread_local int pending_elevel;
static _Thread_local char pending_msg[PGCRYPTOFAM_MSG_CAP];
static _Thread_local char pending_detail[PGCRYPTOFAM_MSG_CAP];

/* landing state read by the armed entry after a longjmp */
static _Thread_local PgcryptofamStatus *cur_status = NULL;

sigjmp_buf *
pgcryptofam_arm(PgcryptofamStatus *st)
{
	static _Thread_local sigjmp_buf jmp;

	cur_status = st;
	memset(st, 0, sizeof(*st));
	st->ok = 1;
	pending_sqlstate = 0;
	pending_elevel = 0;
	pending_msg[0] = '\0';
	pending_detail[0] = '\0';
	err_jmp = &jmp;
	return &jmp;
}

bool
pgcryptofam_errstart(int elevel)
{
	if (elevel < NOTICE)
		return false;			/* suppressed; aux args not evaluated, as at
								 * default log_min_messages */
	pending_elevel = elevel;

	/*
	 * Default sqlstate per elog.c errstart: ERRCODE_INTERNAL_ERROR for
	 * elevel >= ERROR, ERRCODE_WARNING for WARNING, else successful
	 * completion.
	 */
	if (elevel >= ERROR)
		pending_sqlstate = ERRCODE_INTERNAL_ERROR;
	else if (elevel >= WARNING)
		pending_sqlstate = ERRCODE_WARNING;
	else
		pending_sqlstate = ERRCODE_SUCCESSFUL_COMPLETION;
	pending_msg[0] = '\0';
	pending_detail[0] = '\0';
	return true;
}

void
pgcryptofam_errfinish(int elevel)
{
	PgcryptofamStatus *st = cur_status;

	if (elevel >= ERROR)
	{
		if (st != NULL)
		{
			st->ok = 0;
			st->sqlstate = pending_sqlstate;
			st->error_elevel = elevel;
			memcpy(st->msg, pending_msg, sizeof(st->msg));
		}
		if (err_jmp == NULL)
		{
			fprintf(stderr, "pgcryptofam: ereport(%d) with no armed entry\n",
					elevel);
			abort();
		}
		siglongjmp(*err_jmp, 1);
	}

	/* NOTICE / WARNING: record and return (compared observable plane) */
	if (st != NULL)
	{
		st->notice_count++;
		st->notice_sqlstate = pending_sqlstate;
		st->elevel_of_last_notice = elevel;
		memcpy(st->notice_text, pending_msg, sizeof(st->notice_text));
	}
	pending_sqlstate = 0;
	pending_elevel = 0;
}

int
pgcryptofam_errcode(int sqlerrcode)
{
	pending_sqlstate = sqlerrcode;
	return 0;
}

int
pgcryptofam_errmsg(const char *fmt,...)
{
	va_list		args;

	va_start(args, fmt);
	pg_vsnprintf(pending_msg, sizeof(pending_msg), fmt, args);
	va_end(args);
	return 0;
}

int
pgcryptofam_errdetail(const char *fmt,...)
{
	va_list		args;

	va_start(args, fmt);
	pg_vsnprintf(pending_detail, sizeof(pending_detail), fmt, args);
	va_end(args);
	return 0;
}

int
pgcryptofam_errhint(const char *fmt,...)
{
	va_list		args;

	va_start(args, fmt);
	/* hint text: triage only; folded into the detail slot */
	pg_vsnprintf(pending_detail, sizeof(pending_detail), fmt, args);
	va_end(args);
	return 0;
}

/* ------------------------------------------------------------------ */
/* [MOCK] entropy injection for pg_strong_random                       */
/* ------------------------------------------------------------------ */

static _Thread_local const unsigned char *entropy_buf = NULL;
static _Thread_local size_t entropy_len = 0;

void
pgcryptofam_set_entropy(const unsigned char *buf, size_t len)
{
	entropy_buf = buf;
	entropy_len = len;
}

bool
pg_strong_random(void *buf, size_t len)
{
	if (entropy_buf == NULL || entropy_len < len)
		return false;			/* surfaces as PG's own PXE_NO_RANDOM */
	memcpy(buf, entropy_buf, len);
	entropy_buf += len;
	entropy_len -= len;
	return true;
}

/* ------------------------------------------------------------------ */
/* crypto-mode plumbing                                                */
/* ------------------------------------------------------------------ */

/*
 * [VERBATIM] initializer from contrib/pgcrypto/pgcrypto.c line 62
 * (`int builtin_crypto_enabled = BC_ON;`) — the GUC default; the fuzz
 * environment never flips it.
 */
int			builtin_crypto_enabled = BC_ON;

/* [MOCK] OpenSSL FIPS introspection; dead under BC_ON (see file banner). */
bool
CheckFIPSMode(void)
{
	return false;
}

/*
 * [VERBATIM] contrib/pgcrypto/openssl.c CheckBuiltinCryptoMode()
 * (PostgreSQL 18.3, upstream 62d6c7d3df) — copied byte-for-byte.
 */
void
CheckBuiltinCryptoMode(void)
{
	if (builtin_crypto_enabled == BC_ON)
		return;

	if (builtin_crypto_enabled == BC_OFF)
		ereport(ERROR,
				errmsg("use of built-in crypto functions is disabled"));

	Assert(builtin_crypto_enabled == BC_FIPS);

	if (CheckFIPSMode() == true)
		ereport(ERROR,
				errmsg("use of non-FIPS validated crypto not allowed when OpenSSL is in FIPS mode"));
}

/* ------------------------------------------------------------------ */
/* [MOCK] px_find_digest over verbatim pg_cryptohash                   */
/* ------------------------------------------------------------------ */

typedef struct
{
	pg_cryptohash_ctx *ctx;
	unsigned	result_size;
	unsigned	block_size;
} pgcryptofam_digest;

static unsigned
digest_result_size(PX_MD *h)
{
	return ((pgcryptofam_digest *) h->p.ptr)->result_size;
}

static unsigned
digest_block_size(PX_MD *h)
{
	return ((pgcryptofam_digest *) h->p.ptr)->block_size;
}

static void
digest_reset(PX_MD *h)
{
	pgcryptofam_digest *d = (pgcryptofam_digest *) h->p.ptr;

	if (pg_cryptohash_init(d->ctx) < 0)
		elog(ERROR, "pg_cryptohash_init() failed");
}

static void
digest_update(PX_MD *h, const uint8 *data, unsigned dlen)
{
	pgcryptofam_digest *d = (pgcryptofam_digest *) h->p.ptr;

	if (pg_cryptohash_update(d->ctx, data, dlen) < 0)
		elog(ERROR, "pg_cryptohash_update() failed");
}

static void
digest_finish(PX_MD *h, uint8 *dst)
{
	pgcryptofam_digest *d = (pgcryptofam_digest *) h->p.ptr;

	if (pg_cryptohash_final(d->ctx, dst, d->result_size) < 0)
		elog(ERROR, "pg_cryptohash_final() failed");
}

static void
digest_free(PX_MD *h)
{
	pgcryptofam_digest *d = (pgcryptofam_digest *) h->p.ptr;

	pg_cryptohash_free(d->ctx);
	pfree(d);
	pfree(h);
}

int
px_find_digest(const char *name, PX_MD **res)
{
	pg_cryptohash_type type;
	unsigned	result_size;
	unsigned	block_size;
	pg_cryptohash_ctx *ctx;
	pgcryptofam_digest *d;
	PX_MD	   *h;

	/*
	 * Name -> algorithm map. The EVP registry accepts more names/aliases,
	 * but every px_find_digest call in this cone passes a fixed literal
	 * ("md5" in crypt-md5.c, "sha256"/"sha512" in crypt-sha.c), so the
	 * mock's smaller alias table is unobservable through the exported
	 * entries. result_size/block_size are the EVP_MD_CTX_size /
	 * EVP_MD_CTX_block_size constants for each algorithm.
	 */
	if (strcmp(name, "md5") == 0)
	{
		type = PG_MD5;
		result_size = 16;
		block_size = 64;
	}
	else if (strcmp(name, "sha1") == 0)
	{
		type = PG_SHA1;
		result_size = 20;
		block_size = 64;
	}
	else if (strcmp(name, "sha224") == 0)
	{
		type = PG_SHA224;
		result_size = 28;
		block_size = 64;
	}
	else if (strcmp(name, "sha256") == 0)
	{
		type = PG_SHA256;
		result_size = 32;
		block_size = 64;
	}
	else if (strcmp(name, "sha384") == 0)
	{
		type = PG_SHA384;
		result_size = 48;
		block_size = 128;
	}
	else if (strcmp(name, "sha512") == 0)
	{
		type = PG_SHA512;
		result_size = 64;
		block_size = 128;
	}
	else
		return PXE_NO_HASH;

	ctx = pg_cryptohash_create(type);
	if (ctx == NULL)
		return PXE_CIPHER_INIT;
	if (pg_cryptohash_init(ctx) < 0)
	{
		pg_cryptohash_free(ctx);
		return PXE_CIPHER_INIT;
	}

	d = palloc(sizeof(*d));
	d->ctx = ctx;
	d->result_size = result_size;
	d->block_size = block_size;

	h = palloc(sizeof(*h));
	h->result_size = digest_result_size;
	h->block_size = digest_block_size;
	h->reset = digest_reset;
	h->update = digest_update;
	h->finish = digest_finish;
	h->free = digest_free;
	h->p.ptr = d;

	*res = h;
	return 0;
}

/* ------------------------------------------------------------------ */
/* [MOCK] cipher registry stub (px.c px_find_combo linkage only)       */
/* ------------------------------------------------------------------ */

int
px_find_cipher(const char *name, PX_Cipher **res)
{
	(void) name;
	(void) res;
	return PXE_NO_CIPHER;
}

/* ------------------------------------------------------------------ */
/* [MOCK] misc environment                                             */
/* ------------------------------------------------------------------ */

int
pg_mblen_cstr(const char *mbstr)
{
	(void) mbstr;
	return 1;					/* PG_SQL_ASCII mblen (pinned encoding) */
}

/*
 * [MOCK] encoding selection for the vendored scansup.c downcase path
 * (digest()/hmac() find_provider). The campaign pins a SINGLE-BYTE
 * database encoding, so this is 1 — the value a SQL_ASCII/LATIN1 server
 * reports. Only the SELECTION is mocked; the computation below is
 * verbatim.
 */
int
pg_database_encoding_max_length(void)
{
	return 1;
}

/*
 * [VERBATIM] src/backend/utils/mb/mbutils.c `cliplen` (PostgreSQL 18.3,
 * upstream 62d6c7d3df) — copied byte-for-byte.
 */
static int
cliplen(const char *str, int len, int limit)
{
	int			l = 0;

	len = Min(len, limit);
	while (l < len && str[l])
		l++;
	return l;
}

/*
 * pg_mbcliplen: under the pinned single-byte encoding the verbatim
 * pg_encoding_mbcliplen body (mbutils.c) takes its
 * `pg_encoding_max_length(encoding) == 1` early return, which IS
 * `cliplen(mbstr, len, limit)` above. The [MOCK] here is the encoding
 * pin, not the computation.
 */
int
pg_mbcliplen(const char *mbstr, int len, int limit)
{
	return cliplen(mbstr, len, limit);
}

char *
pgcryptofam_strerror_r(int errnum, char *buf, size_t buflen)
{
	pg_snprintf(buf, buflen, "error %d", errnum);
	return buf;
}
