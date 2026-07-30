/*
 * Vendored PostgreSQL C for Kani dual-execution parity proofs.
 *
 * Provenance:
 *   - src/backend/utils/adt/name.c   (postgres/postgres master, fetched 2026-07-28)
 *   - src/backend/utils/adt/ascii.c  (postgres/postgres master, fetched 2026-07-28)
 * REL_18_STABLE conformance: zero code drift vs REL_18_STABLE (provenance
 * audit, proofs/PROVENANCE-AUDIT.md, 2026-07-28).
 * Functions vendored (pg_ prefix, bodies verbatim modulo the shims below):
 *   namein, nameout, namecmp, nameeq/namene/namelt/namele/namegt/namege,
 *   btnamecmp (name.c); pg_to_ascii (ascii.c).
 *
 * Shims (plumbing only, never logic):
 *   1. PG_FUNCTION_ARGS unwrapping -> plain C signatures; PG_RETURN_BOOL -> int.
 *   2. libc strncmp -> pg_ref_strncmp: classic unsigned-char byte loop
 *      returning the RAW difference at the first mismatch and stopping at a
 *      shared NUL — the glibc convention, which is also what the pgrust core
 *      documents (raw magnitude is SQL-visible via btnamecmp; C 18.3:
 *      btnamecmp('a','c') -> -2). CBMC has no libc model.
 *   3. libc strlen -> pg_ref_strlen (byte loop).
 *   4. palloc0(NAMEDATALEN) -> caller-provided buffer, zeroed here with an
 *      explicit loop (memset shim); memcpy -> byte loop.
 *   5. pstrdup(NameStr(*s)) in nameout -> strcpy-shaped loop into a
 *      caller-provided buffer; returns the copied length.
 *   6. pg_mbcliplen (mbutils.c, database-encoding dependent) -> SINGLE-BYTE
 *      ENCODING MODEL: min(len, limit). This is exactly C pg_mbcliplen's
 *      result when the database encoding has max_length == 1 (each char is
 *      one byte, so the longest char-boundary prefix <= limit is limit bytes
 *      when len > limit). The Rust side delegates to the pg_mbcliplen SEAM;
 *      the harness installs the identical model, so the seam is the proof
 *      boundary: namein is proved C==Rust modulo an identical mbcliplen.
 *      Multibyte clipping parity is the mbutils family's proof, not name's.
 *   7. ereport(ERROR) in pg_to_ascii -> return -1 (0 on success).
 *   8. varstr_cmp path of namecmp -> poison sentinel; the harnesses fence
 *      collid == C_COLLATION_OID (the C-locale fast path). Non-C collations
 *      route to varstr_cmp/locale on BOTH sides and are out of proof scope.
 *   9. Buffer params are unsigned char* (matches the Rust *const u8 extern
 *      decls; strncmp semantics are defined over unsigned char anyway).
 */

typedef unsigned int Oid;

#define NAMEDATALEN 64
#define C_COLLATION_OID 950	/* catalog/pg_collation.h */

/* --- shim 2: strncmp reference --- */
static int
pg_ref_strncmp(const unsigned char *s1, const unsigned char *s2, unsigned long n)
{
	unsigned long i;

	for (i = 0; i < n; i++)
	{
		if (s1[i] != s2[i])
			return (int) s1[i] - (int) s2[i];
		if (s1[i] == 0)
			return 0;
	}
	return 0;
}

/* --- shim 3: strlen reference --- */
static int
pg_ref_strlen(const unsigned char *s)
{
	int			n = 0;

	while (s[n] != 0)
		n++;
	return n;
}

/* --- shim 6: pg_mbcliplen, single-byte-encoding model --- */
static int
pg_mbcliplen(const unsigned char *mbstr, int len, int limit)
{
	(void) mbstr;
	return len < limit ? len : limit;
}

/*
 * namein - converts cstring to internal representation (name.c, verbatim
 * modulo shims 1/3/4/6). Returns the copied length (diagnostic only).
 */
int
pg_namein(const unsigned char *s, unsigned char *result /* [NAMEDATALEN] */ )
{
	int			len;
	int			i;

	len = pg_ref_strlen(s);

	/* Truncate oversize input */
	if (len >= NAMEDATALEN)
		len = pg_mbcliplen(s, len, NAMEDATALEN - 1);

	/* We use palloc0 here to ensure result is zero-padded */
	for (i = 0; i < NAMEDATALEN; i++)	/* palloc0 shim */
		result[i] = 0;
	for (i = 0; i < len; i++)	/* memcpy shim */
		result[i] = s[i];

	return len;
}

/*
 * nameout - converts internal representation to cstring (name.c:
 * pstrdup(NameStr(*s)) per shim 5). PRECONDITION (C invariant, name.c
 * comment: "we do not allow NAME values that don't have a '\0' terminator"):
 * name has a NUL within NAMEDATALEN bytes. Returns strlen(out).
 */
int
pg_nameout(const unsigned char *name, unsigned char *out /* [NAMEDATALEN] */ )
{
	int			i = 0;

	while (name[i] != 0)
	{
		out[i] = name[i];
		i++;
	}
	out[i] = 0;
	return i;
}

/*
 * namecmp (name.c, verbatim modulo shims 2/8).
 */
static int
pg_namecmp(const unsigned char *arg1, const unsigned char *arg2, Oid collid)
{
	/* Fast path for common case used in system catalogs */
	if (collid == C_COLLATION_OID)
		return pg_ref_strncmp(arg1, arg2, NAMEDATALEN);

	/* Else rely on the varstr infrastructure -- shim 8: out of proof scope */
	return -2147483647;
}

int
pg_nameeq(const unsigned char *arg1, const unsigned char *arg2, Oid collid)
{
	return pg_namecmp(arg1, arg2, collid) == 0;
}

int
pg_namene(const unsigned char *arg1, const unsigned char *arg2, Oid collid)
{
	return pg_namecmp(arg1, arg2, collid) != 0;
}

int
pg_namelt(const unsigned char *arg1, const unsigned char *arg2, Oid collid)
{
	return pg_namecmp(arg1, arg2, collid) < 0;
}

int
pg_namele(const unsigned char *arg1, const unsigned char *arg2, Oid collid)
{
	return pg_namecmp(arg1, arg2, collid) <= 0;
}

int
pg_namegt(const unsigned char *arg1, const unsigned char *arg2, Oid collid)
{
	return pg_namecmp(arg1, arg2, collid) > 0;
}

int
pg_namege(const unsigned char *arg1, const unsigned char *arg2, Oid collid)
{
	return pg_namecmp(arg1, arg2, collid) >= 0;
}

int
pg_btnamecmp(const unsigned char *arg1, const unsigned char *arg2, Oid collid)
{
	return pg_namecmp(arg1, arg2, collid);
}

/* ================= ascii.c ================= */

#define PG_LATIN1	8			/* mb/pg_wchar.h */
#define PG_LATIN2	9
#define PG_LATIN9	16
#define PG_WIN1250	29

/*
 * pg_to_ascii (ascii.c, verbatim modulo shim 7: ereport -> return -1;
 * returns 0 on success). Encoding-specific maps vendored verbatim.
 */
int
pg_c_to_ascii(unsigned char *src, unsigned char *src_end, unsigned char *dest, int enc)
{
	unsigned char *x;
	const unsigned char *ascii;
	int			range;

	/*
	 * relevant start for an encoding
	 */
#define RANGE_128	128
#define RANGE_160	160

	if (enc == PG_LATIN1)
	{
		/*
		 * ISO-8859-1 <range: 160 -- 255>
		 */
		ascii = (const unsigned char *) "  cL Y  \"Ca  -R     'u .,      ?AAAAAAACEEEEIIII NOOOOOxOUUUUYTBaaaaaaaceeeeiiii nooooo/ouuuuyty";
		range = RANGE_160;
	}
	else if (enc == PG_LATIN2)
	{
		/*
		 * ISO-8859-2 <range: 160 -- 255>
		 */
		ascii = (const unsigned char *) " A L LS \"SSTZ-ZZ a,l'ls ,sstz\"zzRAAAALCCCEEEEIIDDNNOOOOxRUUUUYTBraaaalccceeeeiiddnnoooo/ruuuuyt.";
		range = RANGE_160;
	}
	else if (enc == PG_LATIN9)
	{
		/*
		 * ISO-8859-15 <range: 160 -- 255>
		 */
		ascii = (const unsigned char *) "  cL YS sCa  -R     Zu .z   EeY?AAAAAAACEEEEIIII NOOOOOxOUUUUYTBaaaaaaaceeeeiiii nooooo/ouuuuyty";
		range = RANGE_160;
	}
	else if (enc == PG_WIN1250)
	{
		/*
		 * Window CP1250 <range: 128 -- 255>
		 */
		ascii = (const unsigned char *) "  ' \"    %S<STZZ `'\"\".--  s>stzz   L A  \"CS  -RZ  ,l'u .,as L\"lzRAAAALCCCEEEEIIDDNNOOOOxRUUUUYTBraaaalccceeeeiiddnnoooo/ruuuuyt ";
		range = RANGE_128;
	}
	else
	{
		return -1;				/* shim 7: ereport(ERROR, ...) */
	}

	/*
	 * Encode
	 */
	for (x = src; x < src_end; x++)
	{
		if (*x < 128)
			*dest++ = *x;
		else if (*x < range)
			*dest++ = ' ';		/* bogus 128 to 'range' */
		else
			*dest++ = ascii[*x - range];
	}

	return 0;
}

/*
 * text_name (varlena.c, REL_18_STABLE, fetched 2026-07-28; pg_proc oids
 * 407/1400): namein's clip logic over an explicit-length text payload
 * (NO strlen walk — embedded NULs are data).
 * SHIMS: PG_GETARG_TEXT_PP + VARDATA_ANY/VARSIZE_ANY_EXHDR -> (s, len)
 * pair (pre-detoasted caller contract); palloc0(NAMEDATALEN) ->
 * caller buffer zeroed here; memcpy -> byte loop; pg_mbcliplen -> the
 * same single-byte-encoding model as pg_namein (shim 6 above).
 */
int
pg_text_name(const unsigned char *s, int len,
			 unsigned char *result /* [NAMEDATALEN] */ )
{
	int			i;

	/* Truncate oversize input */
	if (len >= NAMEDATALEN)
		len = pg_mbcliplen(s, len, NAMEDATALEN - 1);

	/* We use palloc0 here to ensure result is zero-padded */
	for (i = 0; i < NAMEDATALEN; i++)	/* palloc0 shim */
		result[i] = 0;
	for (i = 0; i < len; i++)	/* memcpy shim */
		result[i] = s[i];

	return len;
}

/*
 * name_text (varlena.c, REL_18_STABLE, fetched 2026-07-28; pg_proc oids
 * 406/1401): PG_RETURN_TEXT_P(cstring_to_text(NameStr(*s))).
 * SHIMS: cstring_to_text's text construction (palloc + SET_VARSIZE +
 * memcpy) reduced to its VALUE CORE: the payload is exactly the strlen
 * prefix of the name; shimmed to copy that prefix into a caller buffer
 * and return its length. Same NUL-terminator precondition as pg_nameout.
 */
int
pg_name_text(const unsigned char *name, unsigned char *out /* [NAMEDATALEN] */ )
{
	int			len = pg_ref_strlen(name);
	int			i;

	for (i = 0; i < len; i++)	/* memcpy shim */
		out[i] = name[i];

	return len;
}
