/* Emits Rust parity vectors by running the real vendored wchar.c.
 * Regenerate: clang -O2 -I tests/data/cref \
 *   -I ../pgrust-fabled/vendor/postgres-src/src/include \
 *   -o gen tests/data/cref/gen_vectors.c \
 *   ../pgrust-fabled/vendor/postgres-src/src/common/wchar.c && ./gen > tests/data/c_vectors.rs */
#include <stdio.h>
#include <stdlib.h>
#include "c.h"
#include "mb/pg_wchar.h"

static uint64_t prng = 0x243f6a8885a308d3ULL;
static uint64_t next64(void)
{
	prng = prng * 6364136223846793005ULL + 1442695040888963407ULL;
	return prng;
}

static uint64_t h;
static void hash_reset(void) { h = 0xcbf29ce484222325ULL; }
static void hash32(uint32_t v)
{
	for (int i = 0; i < 4; i++)
	{
		h ^= (v >> (8 * i)) & 0xff;
		h *= 0x100000001b3ULL;
	}
}

/* ---- corpora ---- */
#define MAXCORP 400
#define MAXLEN 4096
static unsigned char corp[MAXCORP][MAXLEN];
static int corplen[MAXCORP];
static int ncorp = 0;

static unsigned char *begin_corpus(void) { return corp[ncorp]; }
static void end_corpus(int len) { corplen[ncorp++] = len; }

static void add_bytes(const unsigned char *b, int len)
{
	unsigned char *p = begin_corpus();
	for (int i = 0; i < len; i++) p[i] = b[i];
	end_corpus(len);
}

static int put_ascii(unsigned char *p, int n)
{
	for (int i = 0; i < n; i++) p[i] = 1 + (next64() % 127);
	return n;
}

static int put_cp(unsigned char *p, uint32_t cp)
{
	unicode_to_utf8(cp, p);
	return pg_utf_mblen(p);
}

static uint32_t rand_cp(void)
{
	for (;;)
	{
		uint32_t cp = 1 + (next64() % 0x10ffff);
		if (cp >= 0xd800 && cp <= 0xdfff) continue;
		return cp;
	}
}

static void build_corpora(void)
{
	static const int ascii_lens[] = {1, 2, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 256, 1024};
	for (size_t i = 0; i < sizeof(ascii_lens) / sizeof(int); i++)
	{
		unsigned char *p = begin_corpus();
		end_corpus(put_ascii(p, ascii_lens[i]));
	}
	/* ASCII with embedded NULs */
	static const int nulpos[] = {0, 5, 31, 32, 39};
	for (size_t i = 0; i < sizeof(nulpos) / sizeof(int); i++)
	{
		unsigned char *p = begin_corpus();
		put_ascii(p, 40);
		p[nulpos[i]] = 0;
		end_corpus(40);
	}
	/* random valid UTF-8 of target byte lengths */
	static const int u8lens[] = {5, 16, 32, 33, 64, 257, 1024};
	for (size_t i = 0; i < sizeof(u8lens) / sizeof(int); i++)
	{
		unsigned char *p = begin_corpus();
		int n = 0;
		while (n + 4 <= u8lens[i]) n += put_cp(p + n, rand_cp());
		while (n < u8lens[i]) { p[n++] = 1 + (next64() % 127); }
		end_corpus(n);
	}
	/* dense multibyte: 3-byte CJK, 4-byte emoji */
	{
		unsigned char *p = begin_corpus();
		int n = 0;
		for (int i = 0; i < 128; i++) n += put_cp(p + n, 0x4e00 + (next64() % 0x1000));
		end_corpus(n);
	}
	{
		unsigned char *p = begin_corpus();
		int n = 0;
		for (int i = 0; i < 128; i++) n += put_cp(p + n, 0x1f300 + (next64() % 0x100));
		end_corpus(n);
	}
	/* boundary code points: alone, embedded, and at stride offsets */
	static const uint32_t bcps[] = {0x7f, 0x80, 0x7ff, 0x800, 0xd7ff, 0xe000, 0xfffd,
									0xffff, 0x10000, 0x10ffff};
	for (size_t i = 0; i < sizeof(bcps) / sizeof(uint32_t); i++)
	{
		unsigned char *p = begin_corpus();
		end_corpus(put_cp(p, bcps[i]));
		for (int pre = 29; pre <= 34; pre++)
		{
			unsigned char *q = begin_corpus();
			int n = put_ascii(q, pre);
			n += put_cp(q + n, bcps[i]);
			n += put_ascii(q + n, 8);
			end_corpus(n);
		}
	}
	/* invalid / truncated sequences, alone and at stride offsets */
	static const struct { int len; unsigned char b[4]; } bad[] = {
		{2, {0xc0, 0x80}}, {2, {0xc1, 0xbf}}, {3, {0xe0, 0x80, 0x80}},
		{3, {0xe0, 0x9f, 0xbf}}, {4, {0xf0, 0x80, 0x80, 0x80}},
		{4, {0xf0, 0x8f, 0xbf, 0xbf}}, {3, {0xed, 0xa0, 0x80}},
		{3, {0xed, 0xbf, 0xbf}}, {3, {0xed, 0x9f, 0xbf}},
		{4, {0xf4, 0x8f, 0xbf, 0xbf}}, {4, {0xf4, 0x90, 0x80, 0x80}},
		{4, {0xf5, 0x80, 0x80, 0x80}}, {1, {0xff}}, {1, {0xfe}},
		{1, {0x80}}, {1, {0xbf}}, {1, {0xc3}}, {2, {0xe2, 0x82}},
		{3, {0xf0, 0x9f, 0x98}}, {2, {0xc3, 0x28}}, {3, {0xe2, 0x28, 0xa1}},
		{4, {0xf0, 0x28, 0x8c, 0xbc}}, {2, {0x8d, 0x20}}, {2, {0xc0, 0x20}},
	};
	for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++)
	{
		add_bytes(bad[i].b, bad[i].len);
		static const int pres[] = {12, 30, 31, 32, 33, 61, 64};
		for (size_t j = 0; j < sizeof(pres) / sizeof(int); j++)
		{
			unsigned char *p = begin_corpus();
			int n = put_ascii(p, pres[j]);
			for (int k = 0; k < bad[i].len; k++) p[n++] = bad[i].b[k];
			n += put_ascii(p + n, 8);
			end_corpus(n);
		}
	}
	/* random byte soup */
	static const int souplens[] = {1, 2, 3, 4, 16, 33, 64, 100, 256};
	for (int seed = 0; seed < 4; seed++)
		for (size_t i = 0; i < sizeof(souplens) / sizeof(int); i++)
		{
			unsigned char *p = begin_corpus();
			for (int k = 0; k < souplens[i]; k++) p[k] = next64() & 0xff;
			end_corpus(souplens[i]);
		}
	/* EUC-flavored soup: bytes biased into 0xa1..0xfe plus SS2/SS3 */
	for (int seed = 0; seed < 4; seed++)
	{
		unsigned char *p = begin_corpus();
		int n = 96;
		for (int k = 0; k < n; k++)
		{
			uint64_t r = next64();
			switch (r % 5)
			{
				case 0: p[k] = 0xa1 + (r >> 8) % 0x5e; break;
				case 1: p[k] = 0x8e; break;
				case 2: p[k] = 0x8f; break;
				case 3: p[k] = 0x81 + (r >> 8) % 0x7e; break;
				default: p[k] = 1 + (r >> 8) % 127; break;
			}
		}
		end_corpus(n);
	}
}

int main(void)
{
	build_corpora();

	printf("// Generated by gen_vectors.c from the vendored src/common/wchar.c. Do not edit.\n");

	printf("pub static MAXMBLEN: [i32; 42] = [");
	for (int e = 0; e < 42; e++) printf("%d,", pg_encoding_max_length(e));
	printf("];\n");

	/* mblen for every first byte; two second-byte variants for GB18030 */
	printf("pub static MBLEN_B1_41: [[i8; 256]; 42] = [\n");
	for (int e = 0; e < 42; e++)
	{
		printf("[");
		for (int b = 0; b < 256; b++)
		{
			unsigned char buf[5] = {(unsigned char) b, 0x41, 0x42, 0x43, 0};
			printf("%d,", pg_encoding_mblen(e, (const char *) buf));
		}
		printf("],\n");
	}
	printf("];\n");
	printf("pub static MBLEN_B1_30: [[i8; 256]; 42] = [\n");
	for (int e = 0; e < 42; e++)
	{
		printf("[");
		for (int b = 0; b < 256; b++)
		{
			unsigned char buf[5] = {(unsigned char) b, 0x30, 0x42, 0x43, 0};
			printf("%d,", pg_encoding_mblen(e, (const char *) buf));
		}
		printf("],\n");
	}
	printf("];\n");

	printf("pub static MBLEN_BOUNDED: [[i8; 256]; 42] = [\n");
	for (int e = 0; e < 42; e++)
	{
		printf("[");
		for (int b = 0; b < 256; b++)
		{
			unsigned char buf[5] = {(unsigned char) b, 0x31, 0, 0x33, 0};
			printf("%d,", pg_encoding_mblen_bounded(e, (const char *) buf));
		}
		printf("],\n");
	}
	printf("];\n");

	printf("pub static DSPLEN: [[i8; 256]; 42] = [\n");
	for (int e = 0; e < 42; e++)
	{
		printf("[");
		for (int b = 0; b < 256; b++)
		{
			unsigned char buf[5] = {(unsigned char) b, 0xa4, 0xb9, 0x8e, 0};
			printf("%d,", pg_encoding_dsplen(e, (const char *) buf));
		}
		printf("],\n");
	}
	printf("];\n");

	/* verifychar over all 2-byte pairs at len 1 and 2, per encoding */
	printf("pub static VERIFYCHAR2_HASH: [u64; 42] = [");
	for (int e = 0; e < 42; e++)
	{
		hash_reset();
		for (int b0 = 0; b0 < 256; b0++)
			for (int b1 = 0; b1 < 256; b1++)
			{
				unsigned char buf[2] = {(unsigned char) b0, (unsigned char) b1};
				hash32((uint32_t) pg_encoding_verifymbchar(e, (const char *) buf, 2));
				hash32((uint32_t) pg_encoding_verifymbchar(e, (const char *) buf, 1));
			}
		printf("0x%016llx,", (unsigned long long) h);
	}
	printf("];\n");

	/* verifychar+verifystr over 3-byte triples (b2 sampled), per encoding */
	static const unsigned char b2s[] = {0x00, 0x20, 0x2f, 0x30, 0x39, 0x40, 0x7e, 0x7f,
										0x80, 0x8f, 0x9f, 0xa0, 0xa1, 0xbf, 0xc0, 0xfe};
	printf("pub static VERIFY3_HASH: [u64; 42] = [");
	for (int e = 0; e < 42; e++)
	{
		hash_reset();
		for (int b0 = 0; b0 < 256; b0++)
			for (int b1 = 0; b1 < 256; b1++)
				for (size_t k = 0; k < sizeof(b2s); k++)
				{
					unsigned char buf[3] = {(unsigned char) b0, (unsigned char) b1, b2s[k]};
					hash32((uint32_t) pg_encoding_verifymbchar(e, (const char *) buf, 3));
					hash32((uint32_t) pg_encoding_verifymbstr(e, (const char *) buf, 3));
				}
		printf("0x%016llx,", (unsigned long long) h);
	}
	printf("];\n");

	/* 4-byte sequences: full b1/b3, sampled b0 lead + b2, per encoding */
	static const unsigned char b0s4[] = {0x8e, 0x8f, 0x9a, 0x9c, 0xe0, 0xed, 0xf0,
										 0xf1, 0xf4, 0xf5, 0x81, 0xfe};
	printf("pub static VERIFY4_HASH: [u64; 42] = [");
	for (int e = 0; e < 42; e++)
	{
		hash_reset();
		for (size_t i = 0; i < sizeof(b0s4); i++)
			for (int b1 = 0; b1 < 256; b1++)
				for (size_t k = 0; k < sizeof(b2s); k++)
					for (int b3 = 0; b3 < 256; b3 += 17)
					{
						unsigned char buf[4] = {b0s4[i], (unsigned char) b1, b2s[k],
												(unsigned char) b3};
						hash32((uint32_t) pg_encoding_verifymbchar(e, (const char *) buf, 4));
						hash32((uint32_t) pg_encoding_verifymbstr(e, (const char *) buf, 4));
					}
		printf("0x%016llx,", (unsigned long long) h);
	}
	printf("];\n");

	/* exhaustive UTF-8 3-byte verifychar/verifystr */
	hash_reset();
	for (int b0 = 0; b0 < 256; b0++)
		for (int b1 = 0; b1 < 256; b1++)
			for (int b2 = 0; b2 < 256; b2++)
			{
				unsigned char buf[3] = {(unsigned char) b0, (unsigned char) b1,
										(unsigned char) b2};
				hash32((uint32_t) pg_encoding_verifymbchar(PG_UTF8, (const char *) buf, 3));
				hash32((uint32_t) pg_encoding_verifymbstr(PG_UTF8, (const char *) buf, 3));
			}
	printf("pub static UTF8_VERIFY3_EXHAUSTIVE_HASH: u64 = 0x%016llx;\n",
		   (unsigned long long) h);

	/* pg_utf_dsplen + utf8_to_unicode round trip over every code point */
	hash_reset();
	for (uint32_t cp = 1; cp <= 0x10ffff; cp++)
	{
		unsigned char buf[5] = {0, 0, 0, 0, 0};
		unicode_to_utf8(cp, buf);
		hash32((uint32_t) pg_encoding_dsplen(PG_UTF8, (const char *) buf));
		hash32(utf8_to_unicode(buf));
		hash32((uint32_t) pg_utf_mblen(buf));
		hash32((uint32_t) pg_utf8_islegal(buf, pg_utf_mblen(buf)));
	}
	printf("pub static UTF8_CODEPOINT_HASH: u64 = 0x%016llx;\n", (unsigned long long) h);

	/* corpora + per-encoding verifystr results */
	printf("pub static CORPORA: &[&[u8]] = &[\n");
	for (int i = 0; i < ncorp; i++)
	{
		printf("&[");
		for (int k = 0; k < corplen[i]; k++) printf("%d,", corp[i][k]);
		printf("],\n");
	}
	printf("];\n");
	printf("pub static VERIFYSTR_EXPECT: &[[i32; 42]] = &[\n");
	for (int i = 0; i < ncorp; i++)
	{
		printf("[");
		for (int e = 0; e < 42; e++)
			printf("%d,", pg_encoding_verifymbstr(e, (const char *) corp[i], corplen[i]));
		printf("],\n");
	}
	printf("];\n");

	/* mb2wchar for all server encodings over every corpus */
	printf("pub static MB2WCHAR_HASH: [u64; 35] = [");
	for (int e = 0; e <= PG_ENCODING_BE_LAST; e++)
	{
		hash_reset();
		for (int i = 0; i < ncorp; i++)
		{
			static pg_wchar to[MAXLEN + 1];
			int cnt = pg_wchar_table[e].mb2wchar_with_len(corp[i], to, corplen[i]);
			hash32((uint32_t) cnt);
			for (int k = 0; k <= cnt; k++) hash32(to[k]);
		}
		printf("0x%016llx,", (unsigned long long) h);
	}
	printf("];\n");

	/* wchar2mb over deterministic pseudo-random wchar strings */
	printf("pub static WCHAR2MB_HASH: [u64; 35] = [");
	prng = 0x0123456789abcdefULL;
	static pg_wchar wsrc[64][129];
	for (int i = 0; i < 64; i++)
	{
		for (int k = 0; k < 128; k++)
		{
			uint64_t r = next64();
			uint32_t w = (uint32_t) r;
			switch (r % 4)
			{
				case 0: w &= 0x7f; break;
				case 1: w &= 0xffff; break;
				case 2: w &= 0xffffff; break;
				default: break;
			}
			wsrc[i][k] = w ? w : 1;
		}
		wsrc[i][128] = 0;
	}
	for (int e = 0; e <= PG_ENCODING_BE_LAST; e++)
	{
		hash_reset();
		for (int i = 0; i < 64; i++)
		{
			static unsigned char to[4 * 128 + 1];
			int cnt = pg_wchar_table[e].wchar2mb_with_len(wsrc[i], to, 128);
			hash32((uint32_t) cnt);
			for (int k = 0; k <= cnt; k++) hash32(to[k]);
		}
		printf("0x%016llx,", (unsigned long long) h);
	}
	printf("];\n");

	{
		char inv_utf8[2], inv_other[2];
		pg_encoding_set_invalid(PG_UTF8, inv_utf8);
		pg_encoding_set_invalid(PG_EUC_JP, inv_other);
		printf("pub static SET_INVALID_UTF8: [u8; 2] = [%d, %d];\n",
			   (unsigned char) inv_utf8[0], (unsigned char) inv_utf8[1]);
		printf("pub static SET_INVALID_OTHER: [u8; 2] = [%d, %d];\n",
			   (unsigned char) inv_other[0], (unsigned char) inv_other[1]);
	}

	return 0;
}
