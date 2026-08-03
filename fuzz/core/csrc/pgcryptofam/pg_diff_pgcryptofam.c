/*
 * pg_diff_pgcryptofam.c — C-side driver entries for the pgcryptofam_diff
 * oracle (lane p1-pgcryptofam, contrib/pgcrypto crypt()/gen_salt()/armor
 * family).
 *
 * Every compared body is verbatim PostgreSQL 18.3 (upstream 62d6c7d3df):
 * px_crypt/px_gen_salt (vendor/px-crypt.c) over the verbatim crypt-des/
 * crypt-md5/crypt-blowfish/crypt-sha/crypt-gensalt engines, and
 * pgp_armor_encode/pgp_armor_decode/pgp_extract_armor_headers
 * (vendor/pgp-armor.c) over the verbatim src/common stringinfo layer.
 *
 * The entries below add ONLY:
 *   - {ptr,len} -> NUL-terminated cstring framing (the same bytes
 *     text_to_cstring hands the SQL wrappers),
 *   - arena reset + setjmp arming (see pgcryptofam_shim.c),
 *   - the SQL wrappers' own error translations, transcribed line-for-line
 *     from contrib/pgcrypto/pgcrypto.c (pg_crypt, pg_gen_salt) and
 *     contrib/pgcrypto/pgp-pgsql.c (pg_dearmor, pgp_armor_headers) and
 *     marked [VERBATIM-WRAPPER] at each site,
 *   - exhaustive-diff exporters for file-static helpers (implemented in the
 *     wrap_*.c inclusion TUs; declared here),
 *   - pg_diff_pgcryptofam_cost_probe, a HARNESS FACILITY (not an oracle
 *     body): parses a crypt setting the way the vendored parsers do and
 *     reports (algorithm, iteration count) WITHOUT running it, so the
 *     driver can refuse cost bombs (bf cost 31 = 2^31 blowfish schedules,
 *     sha rounds up to 999999999, xdes count up to 0xFFFFFF).
 *
 * Return convention (all int64 entries): >= 0 bytes written to out;
 * -1 = ereport(>=ERROR) raised, see status; -2 = outcap too small for the
 * produced result (harness sizing bug, not an oracle verdict).
 */
#include "postgres.h"

#include <setjmp.h>

#include "pgcryptofam_shim.h"

#include "lib/stringinfo.h"
#include "vendor/px.h"
#include "vendor/px-crypt.h"
#include "vendor/pgp.h"
#include "common/string.h"
#include "parser/scansup.h"
#include "../pg_oracle_guard.h"	/* oracle-serialization holder check */

/* exporters defined in the wrap_*.c inclusion TUs */
extern void pg_diff_pgcryptofam_to64(char *s, unsigned long v, int n);
extern int	pg_diff_pgcryptofam_ascii_to_bin(char ch);
extern void pg_diff_pgcryptofam_bf_encode(char *dst, const uint32 *src, int size);
extern int	pg_diff_pgcryptofam_bf_decode(uint32 *dst, const char *src, int size);
extern void pg_diff_pgcryptofam_xdes_count_encode(unsigned long count, char *out);

#define ENTER(st) \
	do { \
		pgcryptofam_arena_reset(); \
		if (sigsetjmp(*pgcryptofam_arm(st), 0) != 0) \
			return -1; \
	} while (0)

/* {ptr,len} frame -> NUL-terminated arena cstring (text_to_cstring bytes) */
static char *
frame_to_cstring(const unsigned char *p, size_t len)
{
	char	   *s = palloc(len + 1);

	if (len > 0)
		memcpy(s, p, len);
	s[len] = '\0';
	return s;
}

static int64_t
copy_out(const char *data, size_t len, unsigned char *out, size_t outcap)
{
	if (len > outcap)
		return -2;
	if (len > 0)
		memcpy(out, data, len);
	return (int64_t) len;
}

/*
 * crypt(password, setting) — the SQL crypt() surface.
 */
int64_t
pg_diff_pgcryptofam_crypt(const unsigned char *pw, size_t pwlen,
						  const unsigned char *setting, size_t settinglen,
						  unsigned char *out, size_t outcap,
						  PgcryptofamStatus *st)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	char	   *buf0;
	char	   *buf1;
	char	   *resbuf;
	char	   *cres;

	ENTER(st);

	buf0 = frame_to_cstring(pw, pwlen);
	buf1 = frame_to_cstring(setting, settinglen);

	/* [VERBATIM-WRAPPER] pgcrypto.c pg_crypt(): palloc0'd PX_MAX_CRYPT
	 * result buffer, NULL -> ereport 39000 "crypt(3) returned NULL" */
	resbuf = palloc0(PX_MAX_CRYPT);

	cres = px_crypt(buf0, buf1, resbuf, PX_MAX_CRYPT);

	pfree(buf0);
	pfree(buf1);

	if (cres == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
				 errmsg("crypt(3) returned NULL")));

	return copy_out(cres, strlen(cres), out, outcap);
}

/*
 * gen_salt(algo[, rounds]) — the SQL gen_salt() surface with injectable
 * entropy. rounds = 0 models the 1-argument form (px_gen_salt applies the
 * per-algorithm default).
 */
int64_t
pg_diff_pgcryptofam_gen_salt(const unsigned char *algo, size_t algolen,
							 int32_t rounds,
							 const unsigned char *entropy, size_t entropylen,
							 unsigned char *out, size_t outcap,
							 PgcryptofamStatus *st)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	char		buf[PX_MAX_SALT_LEN + 1];
	size_t		ncopy;
	int			len;

	ENTER(st);

	pgcryptofam_set_entropy(entropy, entropylen);

	/* [VERBATIM-WRAPPER] pgcrypto.c pg_gen_salt/pg_gen_salt_rounds():
	 * text_to_cstring_buffer(arg0, buf, sizeof(buf)) truncation, then
	 * px_gen_salt(buf, buf, rounds); len < 0 -> ereport 22023
	 * "gen_salt: %s". (text_to_cstring_buffer copies at most
	 * sizeof(buf)-1 bytes and NUL-terminates.) */
	ncopy = Min(algolen, sizeof(buf) - 1);
	if (ncopy > 0)
		memcpy(buf, algo, ncopy);
	buf[ncopy] = '\0';

	len = px_gen_salt(buf, buf, rounds);
	if (len < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("gen_salt: %s", px_strerror(len))));

	return copy_out(buf, (size_t) len, out, outcap);
}

/*
 * armor(data[, keys, values]) — pgp_armor_encode over caller-framed header
 * arrays. Key/value SQL-array validation (parse_key_value_arrays) is the
 * driver's plane; this entry exercises the verbatim encoder itself.
 */
int64_t
pg_diff_pgcryptofam_armor(const unsigned char *data, size_t datalen,
						  const unsigned char *const *keys, const size_t *keylens,
						  const unsigned char *const *values, const size_t *vallens,
						  int32_t nheaders,
						  unsigned char *out, size_t outcap,
						  PgcryptofamStatus *st)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	StringInfoData buf;
	/* volatile: these live across the ENTER sigsetjmp (silences gcc
	 * -Wclobbered; they are never read on the longjmp path anyway) */
	char	  **volatile ckeys = NULL;
	char	  **volatile cvalues = NULL;
	int			i;

	ENTER(st);

	if (nheaders > 0)
	{
		ckeys = palloc(sizeof(char *) * nheaders);
		cvalues = palloc(sizeof(char *) * nheaders);
		for (i = 0; i < nheaders; i++)
		{
			ckeys[i] = frame_to_cstring(keys[i], keylens[i]);
			cvalues[i] = frame_to_cstring(values[i], vallens[i]);
		}
	}

	initStringInfo(&buf);

	pgp_armor_encode((const uint8 *) data, (unsigned) datalen, &buf,
					 nheaders, ckeys, cvalues);

	return copy_out(buf.data, (size_t) buf.len, out, outcap);
}

/*
 * dearmor(text) — pgp_armor_decode with the SQL wrapper's error
 * translation.
 */
int64_t
pg_diff_pgcryptofam_dearmor(const unsigned char *text, size_t textlen,
							unsigned char *out, size_t outcap,
							PgcryptofamStatus *st)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	StringInfoData buf;
	int			ret;

	ENTER(st);

	initStringInfo(&buf);

	/* [VERBATIM-WRAPPER] pgp-pgsql.c pg_dearmor(): ret < 0 ->
	 * px_THROW_ERROR(ret) */
	ret = pgp_armor_decode((const uint8 *) text, (int) textlen, &buf);
	if (ret < 0)
		px_THROW_ERROR(ret);

	return copy_out(buf.data, (size_t) buf.len, out, outcap);
}

/*
 * pgp_armor_headers(text) — pgp_extract_armor_headers with the SQL
 * wrapper's error translation. Output framing: nheaders records of
 * key\0value\0 concatenated into out; returns total bytes used.
 */
int64_t
pg_diff_pgcryptofam_armor_headers(const unsigned char *text, size_t textlen,
								  unsigned char *out, size_t outcap,
								  int32_t *nheaders,
								  PgcryptofamStatus *st)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	int			nh = 0;
	char	  **keys;
	char	  **values;
	int			res;
	size_t		used = 0;
	int			i;

	ENTER(st);
	*nheaders = 0;

	/* [VERBATIM-WRAPPER] pgp-pgsql.c pgp_armor_headers(): res < 0 ->
	 * px_THROW_ERROR(res) */
	res = pgp_extract_armor_headers((const uint8 *) text, (unsigned) textlen,
									&nh, &keys, &values);
	if (res < 0)
		px_THROW_ERROR(res);

	for (i = 0; i < nh; i++)
	{
		size_t		klen = strlen(keys[i]) + 1;
		size_t		vlen = strlen(values[i]) + 1;

		if (used + klen + vlen > outcap)
			return -2;
		memcpy(out + used, keys[i], klen);
		used += klen;
		memcpy(out + used, values[i], vlen);
		used += vlen;
	}
	*nheaders = nh;
	return (int64_t) used;
}

/*
 * digest(data, type) / hmac(data, key, type) — pgcrypto.c's pg_digest /
 * pg_hmac over the verbatim px_find_digest / px_find_hmac providers.
 * The name goes through the VERBATIM scansup.c downcase_truncate_identifier
 * exactly as find_provider does, so the fold/truncate behavior is compared
 * rather than assumed.
 */

/*
 * [VERBATIM-WRAPPER] pgcrypto.c find_provider(): downcase the name, look
 * it up, ereport 22023 `Cannot use "%s": %s` with px_strerror on failure.
 * Split in two here only because C's PFN cast (void ** vs PX_MD **) has no
 * portable single spelling; each copy is line-for-line the original.
 */
static PX_MD *
find_digest_provider(const char *nameptr, int namelen)
{
	PX_MD	   *res;
	char	   *buf;
	int			err;

	buf = downcase_truncate_identifier(nameptr, namelen, false);

	err = px_find_digest(buf, &res);

	if (err)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Cannot use \"%s\": %s", buf, px_strerror(err))));

	pfree(buf);

	return res;
}

static PX_HMAC *
find_hmac_provider(const char *nameptr, int namelen)
{
	PX_HMAC    *res;
	char	   *buf;
	int			err;

	buf = downcase_truncate_identifier(nameptr, namelen, false);

	err = px_find_hmac(buf, &res);

	if (err)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Cannot use \"%s\": %s", buf, px_strerror(err))));

	pfree(buf);

	return res;
}

int64_t
pg_diff_pgcryptofam_digest(const unsigned char *name, size_t namelen,
						   const unsigned char *data, size_t datalen,
						   unsigned char *out, size_t outcap,
						   PgcryptofamStatus *st)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PX_MD	   *md;
	unsigned	hlen;
	unsigned char *res;

	ENTER(st);

	/* [VERBATIM-WRAPPER] pgcrypto.c pg_digest() */
	md = find_digest_provider((const char *) name, (int) namelen);

	hlen = px_md_result_size(md);

	res = palloc(hlen);

	px_md_update(md, data, (unsigned) datalen);
	px_md_finish(md, res);
	px_md_free(md);

	return copy_out((const char *) res, hlen, out, outcap);
}

int64_t
pg_diff_pgcryptofam_hmac(const unsigned char *name, size_t namelen,
						 const unsigned char *key, size_t keylen,
						 const unsigned char *data, size_t datalen,
						 unsigned char *out, size_t outcap,
						 PgcryptofamStatus *st)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	PX_HMAC    *h;
	unsigned	hlen;
	unsigned char *res;

	ENTER(st);

	/* [VERBATIM-WRAPPER] pgcrypto.c pg_hmac() */
	h = find_hmac_provider((const char *) name, (int) namelen);

	hlen = px_hmac_result_size(h);

	res = palloc(hlen);

	px_hmac_init(h, key, (unsigned) keylen);
	px_hmac_update(h, data, (unsigned) datalen);
	px_hmac_finish(h, res);
	px_hmac_free(h);

	return copy_out((const char *) res, hlen, out, outcap);
}

/* ------------------------------------------------------------------ */
/* cost probe — HARNESS FACILITY, see file banner                      */
/* ------------------------------------------------------------------ */

/* out_kind values */
#define PGCRYPTOFAM_KIND_DES	0
#define PGCRYPTOFAM_KIND_XDES	1
#define PGCRYPTOFAM_KIND_MD5	2
#define PGCRYPTOFAM_KIND_BF		3
#define PGCRYPTOFAM_KIND_SHA256 4
#define PGCRYPTOFAM_KIND_SHA512 5
#define PGCRYPTOFAM_KIND_NONE	6	/* "$2$": px_crypt_list crypt == NULL */

/*
 * Parse `setting` exactly the way px_crypt's dispatch table and the
 * per-engine preambles do, and report the iteration count the engine's
 * main loop would run. out_cost = 0 means the engine errors (or returns
 * NULL) before reaching its expensive loop. This function never runs any
 * crypt work and never raises; it is shim code by design (harness
 * facility), commented as such.
 */
int32_t
pg_diff_pgcryptofam_cost_probe(const unsigned char *setting, size_t settinglen,
							   int32_t *out_kind, int64_t *out_cost)
{
	PG_ORACLE_GUARD_CHECK(__func__);
	char	   *s;
	size_t		n;

	pgcryptofam_arena_reset();
	s = frame_to_cstring(setting, settinglen);
	n = strlen(s);				/* engines all stop at the first NUL */

	/* px_crypt_list order: "$2a$", "$2x$", "$2$", "$1$", "$5$"/"$6$"
	 * (via run_crypt_sha), "_", "" (catch-all DES) */
	if (strncmp(s, "$2a$", 4) == 0 || strncmp(s, "$2x$", 4) == 0)
	{
		*out_kind = PGCRYPTOFAM_KIND_BF;
		/* _crypt_blowfish_rn preamble: strlen >= 29, digits '0'..'3' /
		 * '0'..'9', combined <= 31, count >= 16 (i.e. cost >= 4) */
		if (n >= 29 &&
			s[4] >= '0' && s[4] <= '3' &&
			s[5] >= '0' && s[5] <= '9' &&
			!(s[4] == '3' && s[5] > '1') &&
			s[6] == '$' &&
			((s[4] - '0') * 10 + (s[5] - '0')) >= 4)
			*out_cost = (int64_t) 1 << ((s[4] - '0') * 10 + (s[5] - '0'));
		else
			*out_cost = 0;
		return 0;
	}
	if (strncmp(s, "$2$", 3) == 0)
	{
		*out_kind = PGCRYPTOFAM_KIND_NONE;
		*out_cost = 0;
		return 0;
	}
	if (strncmp(s, "$1$", 3) == 0)
	{
		*out_kind = PGCRYPTOFAM_KIND_MD5;
		*out_cost = 1000;		/* crypt-md5.c fixed strengthening loop */
		return 0;
	}
	if (strncmp(s, "$5$", 3) == 0 || strncmp(s, "$6$", 3) == 0)
	{
		const char *p = s + 3;
		long long	rounds = PX_SHACRYPT_ROUNDS_DEFAULT;

		*out_kind = (s[1] == '5') ? PGCRYPTOFAM_KIND_SHA256
			: PGCRYPTOFAM_KIND_SHA512;
		/* crypt-sha.c rounds= parse: strtoint then '$' required; out-of-
		 * range values are CLAMPED (with a NOTICE), not rejected */
		if (strncmp(p, "rounds=", 7) == 0)
		{
			char	   *endp;
			int			srounds;

			errno = 0;
			srounds = strtoint(p + 7, &endp, 10);
			if (*endp != '$')
			{
				*out_cost = 0;	/* ereport(ERROR 42601) before the loop */
				return 0;
			}
			if (srounds > PX_SHACRYPT_ROUNDS_MAX)
				srounds = PX_SHACRYPT_ROUNDS_MAX;
			else if (srounds < PX_SHACRYPT_ROUNDS_MIN)
				srounds = PX_SHACRYPT_ROUNDS_MIN;
			rounds = srounds;
		}
		*out_cost = rounds;
		return 0;
	}
	if (s[0] == '_')
	{
		uint32		count = 0;
		int			i;

		*out_kind = PGCRYPTOFAM_KIND_XDES;
		if (n < 9)
			*out_cost = 0;		/* px_crypt_des ereports before do_des */
		else
		{
			/* px_crypt_des XDES arm: 4 chars of ascii_to_bin -> 24-bit
			 * iteration count */
			for (i = 1; i < 5; i++)
				count |= (uint32) pg_diff_pgcryptofam_ascii_to_bin(s[i])
					<< (i - 1) * 6;
			*out_cost = count;
		}
		return 0;
	}
	*out_kind = PGCRYPTOFAM_KIND_DES;
	/* px_crypt_des classic arm: fixed count 25 (errors first if n < 2) */
	*out_cost = (n < 2) ? 0 : 25;
	return 0;
}
