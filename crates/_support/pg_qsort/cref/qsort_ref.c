/*
 * Verbatim-C tie-order reference driver for the pg_qsort crate's
 * equivalence test (tests/c_tie_order.rs). Instantiates the vendored
 * lib/sort_template.h (PostgreSQL's pg_qsort algorithm) over
 * (int32 key, int32 idx) elements, comparing key only so duplicate keys
 * exercise the equal-partition path, and prints the resulting idx
 * permutation. The Rust side must reproduce it bit-exactly.
 *
 * Protocol: stdin = n, then n keys (whitespace-separated decimal);
 * stdout = n lines, the idx of each output slot in order.
 */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define CppConcat(x, y) x##y
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define pg_noinline __attribute__((noinline))

typedef struct
{
	int32_t		key;
	int32_t		idx;
} Elem;

static inline int
cmp_key(const Elem *a, const Elem *b)
{
	return (a->key > b->key) - (a->key < b->key);
}

#define ST_SORT pg_qsort_ref
#define ST_ELEMENT_TYPE Elem
#define ST_COMPARE(a, b) cmp_key(a, b)
#define ST_SCOPE static
#define ST_DECLARE
#define ST_DEFINE
#include "sort_template.h"

int
main(void)
{
	long		n;

	if (scanf("%ld", &n) != 1 || n < 0)
		return 1;
	Elem	   *v = malloc(sizeof(Elem) * (n ? n : 1));

	if (!v)
		return 1;
	for (long i = 0; i < n; i++)
	{
		if (scanf("%d", &v[i].key) != 1)
			return 1;
		v[i].idx = (int32_t) i;
	}
	pg_qsort_ref(v, (size_t) n);
	for (long i = 0; i < n; i++)
		printf("%d\n", v[i].idx);
	free(v);
	return 0;
}
