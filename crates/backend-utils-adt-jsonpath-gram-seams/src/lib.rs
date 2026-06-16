//! Seam declarations for the `backend-utils-adt-jsonpath-gram` /
//! `-scan` unit (`utils/adt/jsonpath_gram.y` + `jsonpath_scan.l`), the
//! bison/flex grammar + scanner that parses jsonpath text into a
//! `JsonPathParseResult`. The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `parsejsonpath(const char *str, int len, struct Node *escontext)`
    /// (`jsonpath_gram.y`) — parse the `len`-byte jsonpath text `str` into a
    /// [`JsonPathParseResult`].  Returns `None` for a parse that produced no
    /// result (C `NULL`); a soft error is recorded in `escontext` exactly as
    /// the C `ereturn`/`yyerror` path does.  `Err` is a hard `ereport(ERROR)`.
    ///
    /// [`JsonPathParseResult`]: types_jsonpath::parse::JsonPathParseResult
    pub fn parse(
        str: &[u8],
        escontext: Option<&mut types_error::SoftErrorContext>,
    ) -> types_error::PgResult<Option<types_jsonpath::parse::JsonPathParseResult>>
);
