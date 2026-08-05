/* VERBATIM from src/common/string.c @ 62d6c7d3df (18.3): strtoint —
 * the C-locale strtol wrapper the jsonb path-subscript parse uses
 * (jsonb_get_element / setPathArray / push_path). NOTE: this is exactly
 * the surface of the 2026-07-30 VT-whitespace subscript bug family; the
 * oracle keeps platform strtol semantics (fuzz process runs LC_ALL=C).
 */

/* --- string.c strtoint (lines 46-58) --- */
/*
 * strtoint --- just like strtol, but returns int not long
 */
int
strtoint(const char *pg_restrict str, char **pg_restrict endptr, int base)
{
	long		val;

	val = strtol(str, endptr, base);
	if (val != (int) val)
		errno = ERANGE;
	return (int) val;
}
