/*
 * pg_hba_token_io.c: standalone libFuzzer + AddressSanitizer/UBSan harness
 * for the VERBATIM PostgreSQL 18.3 pg_hba.conf / pg_ident.conf tokenizer
 * (authfuzz campaign, sub-target 1 — docs/authfuzz/charter.md).
 *
 * This is the C-oracle / memory-safety half of the ST1 harness. It byte-
 * mutates untrusted auth-config file contents and drives them through the
 * verbatim next_token() lexer under ASan/UBSan — the phase-1 method that
 * found bug-104 (guc-file DeescapeQuotedString under-write). next_token is
 * the direct structural analog of that surface: an unbounded quote / NUL /
 * comma / comment byte lexer over attacker-controlled config text.
 *
 * Provenance (VERBATIM, byte-for-byte from the vendor tree at
 * ~/dev/pgrust-reference/vendor/postgres-src, Stamp-18.3, upstream sha
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 — the same checkout the
 * guc_file oracle and cpg-ref build use):
 *   - pg_isblank()  : src/backend/libpq/hba.c:145
 *   - next_token()  : src/backend/libpq/hba.c:187  (pasted UNMODIFIED below)
 *
 * Shims (plumbing only, never logic):
 *   - StringInfo + initStringInfo/resetStringInfo/appendStringInfoChar/
 *     enlargeStringInfo: the growable-buffer contract next_token relies on,
 *     matching src/include/lib/stringinfo.h + src/common/stringinfo.c
 *     semantics (palloc -> malloc). A real OOB inside next_token writes
 *     into this buffer, so ASan instruments the exact allocation the
 *     verbatim body touches.
 *
 * The driver reproduces next_field_expand()'s per-field loop
 * (src/backend/libpq/hba.c:381): each physical line (split on '\n', the
 * trailing '\n'/'\r' stripped, NUL-terminated as tokenize_auth_file builds
 * its StringInfo) is walked by repeated next_token() calls until it returns
 * false. Backslash line-continuation and @-file / include expansion are out
 * of scope for this TU (they are file-IO surfaces, carved exactly as the
 * pgrust-side harness carves them); the whole quote/NUL/comma/comment lexer
 * is fully exercised.
 *
 * Build + run: see run-asan.sh in this directory.
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* ---- StringInfo shim (semantics per PG stringinfo.h/.c) ---------------- */

typedef struct StringInfoData
{
	char   *data;
	int		len;
	int		maxlen;
	int		cursor;
} StringInfoData;

typedef StringInfoData *StringInfo;

static void
initStringInfo(StringInfo str)
{
	int size = 1024;			/* PG's initStringInfo default */

	str->data = (char *) malloc(size);
	if (str->data == NULL)
		abort();
	str->maxlen = size;
	str->len = 0;
	str->cursor = 0;
	str->data[0] = '\0';
}

static void
resetStringInfo(StringInfo str)
{
	str->data[0] = '\0';
	str->len = 0;
	str->cursor = 0;
}

static void
enlargeStringInfo(StringInfo str, int needed)
{
	int newlen;

	/* Guard against overflow, as PG does (int arithmetic). */
	if (needed < 0)
		abort();
	if ((size_t) str->len + (size_t) needed >= (size_t) INT32_MAX)
		abort();
	needed += str->len + 1;		/* total space required now */
	if (needed <= str->maxlen)
		return;
	newlen = 2 * str->maxlen;
	while (needed > newlen)
		newlen = 2 * newlen;
	str->data = (char *) realloc(str->data, newlen);
	if (str->data == NULL)
		abort();
	str->maxlen = newlen;
}

static void
appendStringInfoChar(StringInfo str, char ch)
{
	/* Make more room if needed */
	if (str->len + 1 >= str->maxlen)
		enlargeStringInfo(str, 1);
	str->data[str->len] = ch;
	str->len++;
	str->data[str->len] = '\0';
}

/* ===================================================================== *
 * SECTION: VERBATIM PostgreSQL 18.3 bodies (do not edit)                *
 * ===================================================================== */

/* src/backend/libpq/hba.c:145 — pg_isblank (VERBATIM) */
static bool
pg_isblank(const char c)
{
	return c == ' ' || c == '\t' || c == '\r';
}

/* src/backend/libpq/hba.c:187 — next_token (VERBATIM) */
static bool
next_token(char **lineptr, StringInfo buf,
		   bool *initial_quote, bool *terminating_comma)
{
	int			c;
	bool		in_quote = false;
	bool		was_quote = false;
	bool		saw_quote = false;

	/* Initialize output parameters */
	resetStringInfo(buf);
	*initial_quote = false;
	*terminating_comma = false;

	/* Move over any whitespace and commas preceding the next token */
	while ((c = (*(*lineptr)++)) != '\0' && (pg_isblank(c) || c == ','))
		;

	/*
	 * Build a token in buf of next characters up to EOL, unquoted comma, or
	 * unquoted whitespace.
	 */
	while (c != '\0' &&
		   (!pg_isblank(c) || in_quote))
	{
		/* skip comments to EOL */
		if (c == '#' && !in_quote)
		{
			while ((c = (*(*lineptr)++)) != '\0')
				;
			break;
		}

		/* we do not pass back a terminating comma in the token */
		if (c == ',' && !in_quote)
		{
			*terminating_comma = true;
			break;
		}

		if (c != '"' || was_quote)
			appendStringInfoChar(buf, c);

		/* Literal double-quote is two double-quotes */
		if (in_quote && c == '"')
			was_quote = !was_quote;
		else
			was_quote = false;

		if (c == '"')
		{
			in_quote = !in_quote;
			saw_quote = true;
			if (buf->len == 0)
				*initial_quote = true;
		}

		c = *(*lineptr)++;
	}

	/*
	 * Un-eat the char right after the token (critical in case it is '\0',
	 * else next call will read past end of string).
	 */
	(*lineptr)--;

	return (saw_quote || buf->len > 0);
}

/* ===================================================================== *
 * SECTION: fuzz driver (NOT Postgres code)                              *
 * ===================================================================== */

/*
 * Drive next_token over one NUL-terminated line to EOL, exactly as
 * next_field_expand() does. `buf` is reused across calls (PG resets it).
 */
static void
drive_line(char *line, StringInfo buf)
{
	char   *lineptr = line;
	bool	initial_quote;
	bool	terminating_comma;

	/* Bound the loop defensively: a correct next_token always advances to
	 * the NUL and returns false there; this only backstops a hypothetical
	 * non-terminating divergence so the fuzzer reports a hang cleanly. */
	for (;;)
	{
		bool got = next_token(&lineptr, buf, &initial_quote, &terminating_comma);
		if (!got && *lineptr == '\0')
			break;
		if (*lineptr == '\0' && !terminating_comma)
			break;
	}
}

int
LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
	StringInfoData buf;
	size_t		i = 0;

	initStringInfo(&buf);

	/* Split into physical lines on '\n'; strip trailing '\r'/'\n'; the line
	 * is copied into a NUL-terminated scratch buffer (tokenize_auth_file
	 * hands next_token a NUL-terminated StringInfo->data). Embedded NULs are
	 * preserved so the strlen-boundary surface is reachable. */
	while (i <= size)
	{
		size_t start = i;
		while (i < size && data[i] != '\n')
			i++;
		size_t linelen = i - start;
		/* strip a single trailing '\r' (pg_strip_crlf strips \n then \r) */
		while (linelen > 0 &&
			   (data[start + linelen - 1] == '\r'))
			linelen--;

		char *line = (char *) malloc(linelen + 1);
		if (line == NULL)
			abort();
		memcpy(line, data + start, linelen);
		line[linelen] = '\0';
		drive_line(line, &buf);
		free(line);

		if (i >= size)
			break;
		i++;					/* skip the '\n' */
	}

	free(buf.data);
	return 0;
}

/*
 * Standalone ASan/UBSan campaign driver (used when libFuzzer's runtime
 * archive is unavailable, e.g. Apple clang CLT). Compile WITHOUT
 * -fsanitize=fuzzer and WITHOUT -DAUTHFUZZ_LIBFUZZER; this main() runs a
 * deterministic splitmix64 byte-mutation loop over a built-in adversarial
 * seed set, feeding each mutant to LLVMFuzzerTestOneInput under ASan/UBSan.
 * The CI cluster/Linux build defines AUTHFUZZ_LIBFUZZER (libFuzzer provides main).
 */
#ifndef AUTHFUZZ_LIBFUZZER
#include <stdio.h>

static uint64_t sm64_state;
static uint64_t
sm64(void)
{
	uint64_t z = (sm64_state += 0x9E3779B97F4A7C15ULL);
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
	z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
	return z ^ (z >> 31);
}

static const char *const AF_SEEDS[] = {
	"local all all trust\n",
	"host all all 0.0.0.0/0 scram-sha-256\n",
	"local \"a\"\"b\" all trust\n",
	"local \"unterminated all trust\n",
	"local all,+group,\"q\" all trust\n",
	"x #comment \"still#in\n",
	"\"\"\n",
	"local all /^ali.*$ trust\n",
	"map1 /^(.*)@ex$ \\1\n",
	"host \"db,with,commas\" all ::1/128 md5\n",
};
#define AF_NSEEDS ((int) (sizeof(AF_SEEDS) / sizeof(AF_SEEDS[0])))

static const unsigned char AF_INTERESTING[] =
	" \t\r\n\",#@/+=\\.:0123456789abchilmostu\x00\xff";
#define AF_NINT ((int) (sizeof(AF_INTERESTING) - 1))

int
main(int argc, char **argv)
{
	unsigned long long runs = 20000000ULL;	/* default campaign size */
	const char *env = getenv("PGRUST_AUTHFUZZ_RUNS");

	if (env)
		runs = strtoull(env, NULL, 10);
	if (argc > 1)
		runs = strtoull(argv[1], NULL, 10);
	sm64_state = 0xA1B2C3D4E5F60917ULL;

	unsigned char *buf = (unsigned char *) malloc(65536);
	if (buf == NULL)
		abort();

	for (unsigned long long r = 0; r < runs; r++)
	{
		const char *seed = AF_SEEDS[sm64() % AF_NSEEDS];
		size_t len = strlen(seed);

		if (len > 65536)
			len = 65536;
		memcpy(buf, seed, len);

		int nmut = 1 + (int) (sm64() % 8);
		for (int m = 0; m < nmut && len > 0; m++)
		{
			switch (sm64() % 5)
			{
				case 0:			/* flip to interesting byte */
					buf[sm64() % len] = AF_INTERESTING[sm64() % AF_NINT];
					break;
				case 1:			/* insert */
					if (len < 65536)
					{
						size_t p = sm64() % (len + 1);
						memmove(buf + p + 1, buf + p, len - p);
						buf[p] = AF_INTERESTING[sm64() % AF_NINT];
						len++;
					}
					break;
				case 2:			/* delete */
				{
					size_t p = sm64() % len;
					memmove(buf + p, buf + p + 1, len - p - 1);
					len--;
					break;
				}
				case 3:			/* duplicate a span */
					if (len < 60000)
					{
						size_t p = sm64() % len;
						size_t n = 1 + sm64() % 32;
						if (p + n > len)
							n = len - p;
						memmove(buf + p + n, buf + p, len - p);
						len += n;
					}
					break;
				default:		/* truncate */
					len = sm64() % len;
					break;
			}
		}
		LLVMFuzzerTestOneInput(buf, len);

		if ((r & 0x1FFFFF) == 0x1FFFFF)
			fprintf(stderr, "authfuzz(asan): %llu execs\n", r + 1);
	}

	free(buf);
	fprintf(stderr, "authfuzz(asan): DONE %llu execs, no ASan/UBSan fault\n", runs);
	return 0;
}
#endif							/* AUTHFUZZ_LIBFUZZER */
