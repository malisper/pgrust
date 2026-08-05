/* shim: scanner support (formatting.c uses scanner_isspace) */
#ifndef FMTV_SCANSUP_H
#define FMTV_SCANSUP_H
static inline bool
scanner_isspace(char ch)
{
	return (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f');
}
#endif
