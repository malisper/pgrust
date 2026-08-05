//! quote.c + ruleutils.c quote_identifier: quote_ident/quote_literal/
//! quote_nullable value cores. format_type carries a GUC-free local copy of
//! quote_identifier; it can delegate here once dependency direction allows.

pub mod builtins;
#[cfg(test)]
mod tests;

use ::datum::Varlena;
use ::keywords::{KeywordCategory, ScanKeywordCategories, ScanKeywordLookup, ScanKeywords};
use ::mcx::{Mcx, PgVec};
use ::types_error::PgResult;

pub enum QuotedIdent<'a, 'mcx> {
    Plain(&'a [u8]),
    Quoted(PgVec<'mcx, u8>),
}

impl QuotedIdent<'_, '_> {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            QuotedIdent::Plain(s) => s,
            QuotedIdent::Quoted(v) => v,
        }
    }
}

pub fn quote_identifier<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    ident: &'a [u8],
) -> PgResult<QuotedIdent<'a, 'mcx>> {
    let mut nquotes = 0usize;
    let mut safe = matches!(ident.first(), Some(b'a'..=b'z' | b'_'));
    for &ch in ident {
        match ch {
            b'a'..=b'z' | b'0'..=b'9' | b'_' => {}
            b'"' => {
                safe = false;
                nquotes += 1;
            }
            _ => safe = false,
        }
    }

    if guc_tables::vars::quote_all_identifiers.read() {
        safe = false;
    }

    if safe {
        let kwnum = ScanKeywordLookup(ident, &ScanKeywords);
        if kwnum >= 0 && ScanKeywordCategories[kwnum as usize] != KeywordCategory::Unreserved {
            safe = false;
        }
    }

    if safe {
        return Ok(QuotedIdent::Plain(ident));
    }

    let mut out = mcx::vec_with_capacity_in(mcx, ident.len() + nquotes + 2)?;
    out.push(b'"');
    let mut start = 0;
    for (i, &ch) in ident.iter().enumerate() {
        if ch == b'"' {
            mcx::vec_append_bytes(&mut out, &ident[start..=i])?;
            out.push(b'"');
            start = i + 1;
        }
    }
    mcx::vec_append_bytes(&mut out, &ident[start..])?;
    out.push(b'"');
    Ok(QuotedIdent::Quoted(out))
}

pub fn quote_ident<'mcx>(mcx: Mcx<'mcx>, ident: &[u8]) -> PgResult<Varlena<'mcx>> {
    match quote_identifier(mcx, ident)? {
        QuotedIdent::Plain(s) => varlena::cstring_to_text(mcx, s),
        QuotedIdent::Quoted(v) => varlena::cstring_to_text(mcx, &v),
    }
}

// Behavior must not depend on standard_conforming_strings (quote.c note): the
// E'' prefix appears whenever a backslash forces doubled-backslash escaping.
pub fn quote_literal<'mcx>(mcx: Mcx<'mcx>, src: &[u8]) -> PgResult<Varlena<'mcx>> {
    let mut out = varlena::image_with_header(mcx, 2 * src.len() + 3)?;
    if src.contains(&b'\\') {
        out.push(b'E');
    }
    out.push(b'\'');
    let mut start = 0;
    for (i, &ch) in src.iter().enumerate() {
        if ch == b'\'' || ch == b'\\' {
            mcx::vec_append_bytes(&mut out, &src[start..=i])?;
            out.push(ch);
            start = i + 1;
        }
    }
    mcx::vec_append_bytes(&mut out, &src[start..])?;
    out.push(b'\'');
    Ok(Varlena::from_image(out))
}
