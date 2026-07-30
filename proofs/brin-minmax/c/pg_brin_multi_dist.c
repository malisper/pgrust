/*
 * Vendored from postgres REL_18_STABLE (fetched 2026-07-28 via
 * raw.githubusercontent.com/postgres/postgres/REL_18_STABLE):
 *   src/backend/access/brin/brin_minmax_multi.c —
 *       brin_minmax_multi_distance_{float4,float8,int2,int4,int8,tid,uuid,
 *       date,time,timetz,timestamp,pg_lsn,macaddr,macaddr8,inet} and
 *       brin_minmax_multi_summary_{in,recv} (all bodies verbatim)
 *   src/include/utils/float.h — get_float8_infinity (C99 arm, verbatim)
 *
 * This is a SEPARATE c-lib from pg_brin_minmax.c: distance harnesses link
 * only this file (mbconv law: whole-family linking adds a fixed CBMC read
 * cost that fakes solver walls). The two files share no symbols.
 *
 * SHIMS (everything else is verbatim; nothing here replaces logic under
 * proof):
 *
 *  - fmgr unwrapping: PG_FUNCTION_ARGS entry points become plain C
 *    signatures (PG_GETARG_FLOAT4/FLOAT8/INT16/INT32/INT64/DATEADT/
 *    TIMEADT/TIMESTAMP/LSN become typed parameters; the by-ref
 *    PG_GETARG_*_P/PP prologues become caller-filled structs mirroring
 *    the exact on-disk field layout the Rust wrapper reads);
 *    PG_RETURN_FLOAT8(x) -> `return x;` (functions return double),
 *    PG_RETURN_VOID() -> `return 0;`.
 *  - struct shims: ItemPointerData/BlockIdData (+ the NoCheck accessor
 *    macros, itemptr.h/block.h semantics), pg_uuid_t, TimeTzADT, macaddr,
 *    macaddr8, inet/inet_struct + ip_family/ip_bits/ip_addr/ip_addrsize
 *    (inet.h semantics) are redeclared with only the fields the vendored
 *    bodies read.
 *  - MaxHeapTuplesPerPage: the BLCKSZ-8192 production value, same formula
 *    as htup_details.h ((8192 - 24) / (24 + 4) = 291); pgrust's
 *    types_storage::bufpage constant is the same expression — the
 *    eq_dist_tid harness asserts the two constants agree (wiring theorem)
 *    via pg_max_heap_tuples_per_page().
 *  - palloc (inet only) -> two static 16-byte buffers, index reset per
 *    call; pfree -> no-op. Allocation strategy is out of proof.
 *  - ereport(ERROR, ...) (summary_in/recv only) -> records the errcode in
 *    pg_dist_errcode and returns 1 at the exact program point (C aborts
 *    via longjmp there); MAKE_SQLSTATE/PGSIXBIT are the verbatim
 *    elog.h encoders, so the recorded int is comparable against pgrust's
 *    SqlState(i32) representation bit-for-bit.
 *  - Assert compiled out (production postgres posture, pg_proof_shim.h).
 *    The Assert(a1 <= a2) / Assert(delta >= 0) caller contracts that the
 *    SHIPPED Rust keeps as debug_assert! are enforced as kani::assume
 *    fences in the harnesses (documented per harness).
 *  - CANONICAL-NAN SHIM: screened NOT NEEDED — no vendored section here
 *    reaches the NAN macro or get_float8_nan(); the float4/float8
 *    distances only TEST isnan on inputs and return 0.0/get_float8_infinity
 *    (INFINITY models exactly; only the NAN header constant is defective).
 */

#include "../../support/c/pg_proof_shim.h"
#include <math.h>

typedef double float8;
typedef float float4;

/* ---- float.h get_float8_infinity, body verbatim ---- */
static inline float8
get_float8_infinity(void)
{
	/* C99 standard way */
	return (float8) INFINITY;
}

/* ---- result-protocol shim (all distance fns return float8) ---- */
#define PG_RETURN_FLOAT8(x) return (x)

/* ==================== scalar distances ==================== */

/* brin_minmax_multi_distance_float4, body verbatim */
float8
pg_dist_float4(float4 a1, float4 a2)
{
	/* if both values are NaN, then we consider them the same */
	if (isnan(a1) && isnan(a2))
		PG_RETURN_FLOAT8(0.0);

	/* if one value is NaN, use infinite distance */
	if (isnan(a1) || isnan(a2))
		PG_RETURN_FLOAT8(get_float8_infinity());

	/*
	 * We know the values are range boundaries, but the range may be collapsed
	 * (i.e. single points), with equal values.
	 */
	Assert(a1 <= a2);

	PG_RETURN_FLOAT8((double) a2 - (double) a1);
}

/* brin_minmax_multi_distance_float8, body verbatim */
float8
pg_dist_float8(double a1, double a2)
{
	/* if both values are NaN, then we consider them the same */
	if (isnan(a1) && isnan(a2))
		PG_RETURN_FLOAT8(0.0);

	/* if one value is NaN, use infinite distance */
	if (isnan(a1) || isnan(a2))
		PG_RETURN_FLOAT8(get_float8_infinity());

	/*
	 * We know the values are range boundaries, but the range may be collapsed
	 * (i.e. single points), with equal values.
	 */
	Assert(a1 <= a2);

	PG_RETURN_FLOAT8(a2 - a1);
}

/* brin_minmax_multi_distance_int2, body verbatim */
float8
pg_dist_int2(int16 a1, int16 a2)
{
	/*
	 * We know the values are range boundaries, but the range may be collapsed
	 * (i.e. single points), with equal values.
	 */
	Assert(a1 <= a2);

	PG_RETURN_FLOAT8((double) a2 - (double) a1);
}

/* brin_minmax_multi_distance_int4, body verbatim */
float8
pg_dist_int4(int32 a1, int32 a2)
{
	/*
	 * We know the values are range boundaries, but the range may be collapsed
	 * (i.e. single points), with equal values.
	 */
	Assert(a1 <= a2);

	PG_RETURN_FLOAT8((double) a2 - (double) a1);
}

/* brin_minmax_multi_distance_int8, body verbatim */
float8
pg_dist_int8(int64 a1, int64 a2)
{
	/*
	 * We know the values are range boundaries, but the range may be collapsed
	 * (i.e. single points), with equal values.
	 */
	Assert(a1 <= a2);

	PG_RETURN_FLOAT8((double) a2 - (double) a1);
}

/* ==================== tid ==================== */

typedef uint32 BlockNumber;
typedef uint16 OffsetNumber;

typedef struct BlockIdData
{
	uint16		bi_hi;
	uint16		bi_lo;
} BlockIdData;

typedef struct ItemPointerData
{
	BlockIdData ip_blkid;
	OffsetNumber ip_posid;
} ItemPointerData;
typedef ItemPointerData *ItemPointer;

#define BlockIdGetBlockNumber(blockId) \
	((((BlockNumber) (blockId)->bi_hi) << 16) | ((BlockNumber) (blockId)->bi_lo))
#define ItemPointerGetBlockNumberNoCheck(pointer) \
	BlockIdGetBlockNumber(&(pointer)->ip_blkid)
#define ItemPointerGetOffsetNumberNoCheck(pointer) \
	((pointer)->ip_posid)

/* htup_details.h formula at the BLCKSZ-8192 production posture (= 291) */
#define MaxHeapTuplesPerPage ((int) ((8192 - 24) / (24 + 4)))

int
pg_max_heap_tuples_per_page(void)
{
	return MaxHeapTuplesPerPage;
}

/* brin_minmax_multi_distance_tid, body verbatim (struct-fill prologue) */
float8
pg_dist_tid(uint16 hi1, uint16 lo1, uint16 pos1,
			uint16 hi2, uint16 lo2, uint16 pos2)
{
	double		da1,
				da2;

	ItemPointerData p1;
	ItemPointerData p2;
	ItemPointer pa1 = &p1;
	ItemPointer pa2 = &p2;

	p1.ip_blkid.bi_hi = hi1;
	p1.ip_blkid.bi_lo = lo1;
	p1.ip_posid = pos1;
	p2.ip_blkid.bi_hi = hi2;
	p2.ip_blkid.bi_lo = lo2;
	p2.ip_posid = pos2;

	/*
	 * We know the values are range boundaries, but the range may be collapsed
	 * (i.e. single points), with equal values.
	 */
	Assert(ItemPointerCompare(pa1, pa2) <= 0);

	/*
	 * We use the no-check variants here, because user-supplied values may
	 * have (ip_posid == 0). See ItemPointerCompare.
	 */
	da1 = ItemPointerGetBlockNumberNoCheck(pa1) * MaxHeapTuplesPerPage +
		ItemPointerGetOffsetNumberNoCheck(pa1);

	da2 = ItemPointerGetBlockNumberNoCheck(pa2) * MaxHeapTuplesPerPage +
		ItemPointerGetOffsetNumberNoCheck(pa2);

	PG_RETURN_FLOAT8(da2 - da1);
}

/* ==================== uuid ==================== */

#define UUID_LEN 16

typedef struct pg_uuid_t
{
	unsigned char data[UUID_LEN];
} pg_uuid_t;

/* brin_minmax_multi_distance_uuid, body verbatim (struct-fill prologue) */
float8
pg_dist_uuid(const unsigned char *d1, const unsigned char *d2)
{
	int			i;
	float8		delta = 0;

	pg_uuid_t	uu1;
	pg_uuid_t	uu2;
	pg_uuid_t  *u1 = &uu1;
	pg_uuid_t  *u2 = &uu2;

	for (i = 0; i < UUID_LEN; i++)
	{
		uu1.data[i] = d1[i];
		uu2.data[i] = d2[i];
	}

	/*
	 * We know the values are range boundaries, but the range may be collapsed
	 * (i.e. single points), with equal values.
	 */
	Assert(DatumGetBool(DirectFunctionCall2(uuid_le, a1, a2)));

	/* compute approximate delta as a double precision value */
	for (i = UUID_LEN - 1; i >= 0; i--)
	{
		delta += (int) u2->data[i] - (int) u1->data[i];
		delta /= 256;
	}

	Assert(delta >= 0);

	PG_RETURN_FLOAT8(delta);
}

/* ==================== date / time / timetz / timestamp / lsn ==================== */

typedef int32 DateADT;
typedef int64 TimeADT;
typedef int64 Timestamp;
typedef uint64 XLogRecPtr;

#define INT64CONST(x) (x##LL)
#define USECS_PER_SEC INT64CONST(1000000)

/* brin_minmax_multi_distance_date, body verbatim */
float8
pg_dist_date(DateADT dateVal1, DateADT dateVal2)
{
	float8		delta = 0;

	delta = (float8) dateVal2 - (float8) dateVal1;

	Assert(delta >= 0);

	PG_RETURN_FLOAT8(delta);
}

/* brin_minmax_multi_distance_time, body verbatim */
float8
pg_dist_time(TimeADT ta, TimeADT tb)
{
	float8		delta = 0;

	delta = (tb - ta);

	Assert(delta >= 0);

	PG_RETURN_FLOAT8(delta);
}

typedef struct TimeTzADT
{
	TimeADT		time;
	int32		zone;
} TimeTzADT;

/* brin_minmax_multi_distance_timetz, body verbatim (struct-fill prologue).
 * NOTE the C evaluation order: (tb->zone - ta->zone) subtracts in int32
 * (wraps at 32 bits under -fwrapv) BEFORE widening for the int64 multiply,
 * where the shipped Rust widens each zone to i64 first — divergent ONLY on
 * the zone-wrap plane, unreachable from validated timetz values (zone is
 * bounded by timetz_in); the harnesses fence to |zone| <= 86400 (superset
 * of any real displacement) and a probe harness documents the wrap plane. */
float8
pg_dist_timetz(int64 ta_time, int32 ta_zone, int64 tb_time, int32 tb_zone)
{
	float8		delta = 0;

	TimeTzADT	t_a;
	TimeTzADT	t_b;
	TimeTzADT  *ta = &t_a;
	TimeTzADT  *tb = &t_b;

	t_a.time = ta_time;
	t_a.zone = ta_zone;
	t_b.time = tb_time;
	t_b.zone = tb_zone;

	delta = (tb->time - ta->time) + (tb->zone - ta->zone) * USECS_PER_SEC;

	Assert(delta >= 0);

	PG_RETURN_FLOAT8(delta);
}

/* brin_minmax_multi_distance_timestamp, body verbatim */
float8
pg_dist_timestamp(Timestamp dt1, Timestamp dt2)
{
	float8		delta = 0;

	delta = (float8) dt2 - (float8) dt1;

	Assert(delta >= 0);

	PG_RETURN_FLOAT8(delta);
}

/* brin_minmax_multi_distance_pg_lsn, body verbatim */
float8
pg_dist_pg_lsn(XLogRecPtr lsna, XLogRecPtr lsnb)
{
	float8		delta = 0;

	delta = (lsnb - lsna);

	Assert(delta >= 0);

	PG_RETURN_FLOAT8(delta);
}

/* ==================== macaddr / macaddr8 ==================== */

typedef struct macaddr
{
	unsigned char a;
	unsigned char b;
	unsigned char c;
	unsigned char d;
	unsigned char e;
	unsigned char f;
} macaddr;

typedef struct macaddr8
{
	unsigned char a;
	unsigned char b;
	unsigned char c;
	unsigned char d;
	unsigned char e;
	unsigned char f;
	unsigned char g;
	unsigned char h;
} macaddr8;

/* brin_minmax_multi_distance_macaddr, body verbatim (struct-fill prologue;
 * bytes 0..5 of the on-disk image are fields a..f) */
float8
pg_dist_macaddr(const unsigned char *ba, const unsigned char *bb)
{
	float8		delta;

	macaddr		m_a;
	macaddr		m_b;
	macaddr    *a = &m_a;
	macaddr    *b = &m_b;

	m_a.a = ba[0];
	m_a.b = ba[1];
	m_a.c = ba[2];
	m_a.d = ba[3];
	m_a.e = ba[4];
	m_a.f = ba[5];
	m_b.a = bb[0];
	m_b.b = bb[1];
	m_b.c = bb[2];
	m_b.d = bb[3];
	m_b.e = bb[4];
	m_b.f = bb[5];

	delta = ((float8) b->f - (float8) a->f);
	delta /= 256;

	delta += ((float8) b->e - (float8) a->e);
	delta /= 256;

	delta += ((float8) b->d - (float8) a->d);
	delta /= 256;

	delta += ((float8) b->c - (float8) a->c);
	delta /= 256;

	delta += ((float8) b->b - (float8) a->b);
	delta /= 256;

	delta += ((float8) b->a - (float8) a->a);
	delta /= 256;

	Assert(delta >= 0);

	PG_RETURN_FLOAT8(delta);
}

/* brin_minmax_multi_distance_macaddr8, body verbatim (struct-fill prologue) */
float8
pg_dist_macaddr8(const unsigned char *ba, const unsigned char *bb)
{
	float8		delta;

	macaddr8	m_a;
	macaddr8	m_b;
	macaddr8   *a = &m_a;
	macaddr8   *b = &m_b;

	m_a.a = ba[0];
	m_a.b = ba[1];
	m_a.c = ba[2];
	m_a.d = ba[3];
	m_a.e = ba[4];
	m_a.f = ba[5];
	m_a.g = ba[6];
	m_a.h = ba[7];
	m_b.a = bb[0];
	m_b.b = bb[1];
	m_b.c = bb[2];
	m_b.d = bb[3];
	m_b.e = bb[4];
	m_b.f = bb[5];
	m_b.g = bb[6];
	m_b.h = bb[7];

	delta = ((float8) b->h - (float8) a->h);
	delta /= 256;

	delta += ((float8) b->g - (float8) a->g);
	delta /= 256;

	delta += ((float8) b->f - (float8) a->f);
	delta /= 256;

	delta += ((float8) b->e - (float8) a->e);
	delta /= 256;

	delta += ((float8) b->d - (float8) a->d);
	delta /= 256;

	delta += ((float8) b->c - (float8) a->c);
	delta /= 256;

	delta += ((float8) b->b - (float8) a->b);
	delta /= 256;

	delta += ((float8) b->a - (float8) a->a);
	delta /= 256;

	Assert(delta >= 0);

	PG_RETURN_FLOAT8(delta);
}

/* ==================== inet ==================== */

#define PGSQL_AF_INET	2
#define PGSQL_AF_INET6	3

typedef struct inet_struct
{
	unsigned char family;
	unsigned char bits;
	unsigned char ipaddr[16];
} inet_struct;

typedef struct inet
{
	int32		vl_len_;
	inet_struct inet_data;
} inet;

#define ip_family(inetptr) ((inetptr)->inet_data.family)
#define ip_bits(inetptr) ((inetptr)->inet_data.bits)
#define ip_addr(inetptr) ((inetptr)->inet_data.ipaddr)
#define ip_addrsize(inetptr) \
	(ip_family(inetptr) == PGSQL_AF_INET ? 4 : 16)

/* palloc -> two static 16-byte buffers (allocation out of proof) */
static unsigned char pg_inet_palloc_buf[2][16];
static int	pg_inet_palloc_idx;
#define palloc(sz) (pg_inet_palloc_buf[pg_inet_palloc_idx++ & 1])
#define pfree(p) ((void) 0)

static void *
pg_proof_memcpy(void *dst, const void *src, Size n)
{
	unsigned char *d = (unsigned char *) dst;
	const unsigned char *s = (const unsigned char *) src;
	Size		i;

	for (i = 0; i < n; i++)
		d[i] = s[i];
	return dst;
}

#define memcpy(d, s, n) pg_proof_memcpy((d), (s), (n))

/* brin_minmax_multi_distance_inet, body verbatim (struct-fill prologue) */
float8
pg_dist_inet(unsigned char fam_a, unsigned char bits_a, const unsigned char *addr_a,
			 unsigned char fam_b, unsigned char bits_b, const unsigned char *addr_b)
{
	float8		delta;
	int			i;
	int			len;
	unsigned char *addra,
			   *addrb;

	inet		ina;
	inet		inb;
	inet	   *ipa = &ina;
	inet	   *ipb = &inb;

	int			lena,
				lenb;

	ina.inet_data.family = fam_a;
	ina.inet_data.bits = bits_a;
	inb.inet_data.family = fam_b;
	inb.inet_data.bits = bits_b;
	for (i = 0; i < 16; i++)
	{
		ina.inet_data.ipaddr[i] = addr_a[i];
		inb.inet_data.ipaddr[i] = addr_b[i];
	}
	pg_inet_palloc_idx = 0;

	/*
	 * If the addresses are from different families, consider them to be in
	 * maximal possible distance (which is 1.0).
	 */
	if (ip_family(ipa) != ip_family(ipb))
		PG_RETURN_FLOAT8(1.0);

	addra = (unsigned char *) palloc(ip_addrsize(ipa));
	memcpy(addra, ip_addr(ipa), ip_addrsize(ipa));

	addrb = (unsigned char *) palloc(ip_addrsize(ipb));
	memcpy(addrb, ip_addr(ipb), ip_addrsize(ipb));

	/*
	 * The length is calculated from the mask length, because we sort the
	 * addresses by first address in the range, so A.B.C.D/24 < A.B.C.1 (the
	 * first range starts at A.B.C.0, which is before A.B.C.1). We don't want
	 * to produce a negative delta in this case, so we just cut the extra
	 * bytes.
	 *
	 * XXX Maybe this should be a bit more careful and cut the bits, not just
	 * whole bytes.
	 */
	lena = ip_bits(ipa);
	lenb = ip_bits(ipb);

	len = ip_addrsize(ipa);

	/* apply the network mask to both addresses */
	for (i = 0; i < len; i++)
	{
		unsigned char mask;
		int			nbits;

		nbits = Max(0, lena - (i * 8));
		if (nbits < 8)
		{
			mask = (0xFF << (8 - nbits));
			addra[i] = (addra[i] & mask);
		}

		nbits = Max(0, lenb - (i * 8));
		if (nbits < 8)
		{
			mask = (0xFF << (8 - nbits));
			addrb[i] = (addrb[i] & mask);
		}
	}

	/* Calculate the difference between the addresses. */
	delta = 0;
	for (i = len - 1; i >= 0; i--)
	{
		unsigned char a = addra[i];
		unsigned char b = addrb[i];

		delta += (float8) b - (float8) a;
		delta /= 256;
	}

	Assert((delta >= 0) && (delta <= 1));

	pfree(addra);
	pfree(addrb);

	PG_RETURN_FLOAT8(delta);
}

/* ==================== summary_in / summary_recv ==================== */

/* elog.h encoders, verbatim */
#define PGSIXBIT(ch)	(((ch) - '0') & 0x3F)
#define MAKE_SQLSTATE(ch1,ch2,ch3,ch4,ch5)	\
	(PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + \
	 (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24))
#define ERRCODE_FEATURE_NOT_SUPPORTED MAKE_SQLSTATE('0','A','0','0','0')

/* ereport rewire (header comment): record errcode, return 1 at the exact
 * program point. Message text never crosses the seam. */
static int32 pg_dist_errcode;
#define errcode(c) ((void) (pg_dist_errcode = (c)))
#define errmsg(...) ((void) 0)
#define ereport(elevel, rest) do { rest; return 1; } while (0)
#define PG_RETURN_VOID() return 0

int32
pg_dist_errcode_get(void)
{
	return pg_dist_errcode;
}

/* brin_minmax_multi_summary_in, body verbatim */
int
pg_summary_in(void)
{
	pg_dist_errcode = 0;

	/*
	 * brin_minmax_multi_summary stores the data in binary form and parsing
	 * text input is not needed, so disallow this.
	 */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type %s", "brin_minmax_multi_summary")));

	PG_RETURN_VOID();			/* keep compiler quiet */
}

/* brin_minmax_multi_summary_recv, body verbatim */
int
pg_summary_recv(void)
{
	pg_dist_errcode = 0;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type %s", "brin_minmax_multi_summary")));

	PG_RETURN_VOID();			/* keep compiler quiet */
}

/* ==================== negative control (NOT postgres code) ==================== */

/* control_dist_int4_swapped: argument order deliberately inverted — the
 * harness comparing this against the shipped fc_dist_int4 MUST FAIL. */
float8
pg_dist_int4_wrong(int32 a1, int32 a2)
{
	return (double) a1 - (double) a2;
}
