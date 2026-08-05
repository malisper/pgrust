/*
 * pg_strlcpy_compat.c — WEAK-linkage strlcpy for pre-2.38 glibc fleet pods.
 *
 * The fuzz workspace's vendored oracles call strlcpy (e.g. the jsonpath
 * family's pg_formatting_min.c). macOS and glibc >= 2.38 provide it; the
 * fleet pod glibc does not, which broke the Linux link of the test binary
 * (fast-tests job pgrust-fast-tests-0758904159-1785602170-47e3).
 *
 * The BODY is VERBATIM PostgreSQL src/port/strlcpy.c lines 60-85 @
 * 62d6c7d3df (18.3) — the same fallback PG itself ships for platforms
 * without strlcpy. The __attribute__((weak)) is the only addition: where
 * libc defines strlcpy, the strong libc symbol wins and this object is
 * inert; where it doesn't, this definition satisfies the link.
 */
#include <stddef.h>

__attribute__((weak))
size_t
strlcpy(char *dst, const char *src, size_t siz)
{
	char	   *d = dst;
	const char *s = src;
	size_t		n = siz;

	/* Copy as many bytes as will fit */
	if (n != 0)
	{
		while (--n != 0)
		{
			if ((*d++ = *s++) == '\0')
				break;
		}
	}

	/* Not enough room in dst, add NUL and traverse rest of src */
	if (n == 0)
	{
		if (siz != 0)
			*d = '\0';			/* NUL-terminate dst */
		while (*s++)
			;
	}

	return (s - src - 1);		/* count does not include NUL */
}
