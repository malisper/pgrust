//! The D2 mint-on-connect name grammar (docs/design/test-views.md D2) —
//! pure string logic, no catalog/GUC I/O, so both sides of the mint path
//! (the backend seam impl and the janitor) parse ONE definition,
//! unit-tested here.
//!
//! Grammar, defined in terms of the configured prefix (never a literal):
//!
//! ```text
//!   <prefix><template>__<token>   selects the sealed template named
//!                                 <template> (the RAW database name, e.g.
//!                                 tpl_5677ece429c5 — templates conventionally
//!                                 live OUTSIDE the prefix).
//! ```
//!
//! This is the ONLY mint form (ruling 2026-08-05: the template is ALWAYS in
//! the name; `pgrust.ephemeral_db_default_template` and the bare
//! `<prefix><token>` form are deleted — and with them the hazard that a
//! generated bare token containing `__` silently selected a template). A
//! prefix-matching name that is NOT the template form parses as
//! `Malformed`: the mint path gives mint-AUTHORIZED roles a clear FATAL
//! naming the required form, while unauthorized roles keep the stock
//! does-not-exist error (authorization opacity).
//!
//! Resolution rules, decidable from the NAME ALONE (the input-decidability
//! law):
//!
//! - The 63-byte (NAMEDATALEN-1) identifier limit applies to the WHOLE name.
//!   Longer names never match: CREATE DATABASE would truncate them, so the
//!   minted datname could not byte-equal the requested name and the connect
//!   retry would re-miss. Refusing input-side keeps the stock FATAL.
//! - The FIRST `__` splits template from token; both sides must be
//!   non-empty. A rest with no `__`, or one where a side would be empty
//!   (e.g. `tdb___x` or `tdb_tpl__`), is `Malformed`. Because the FIRST
//!   `__` always wins the split, a template whose OWN name contains `__`
//!   is unreachable by the grammar (`tdb_my__tpl__x` selects template `my`,
//!   token `tpl__x` — never a template named `my__tpl`).
//! - The empty prefix means the feature is off: nothing matches, even
//!   though every string starts with `""` (the reap_candidate convention).

/// NAMEDATALEN - 1: the datname byte budget.
pub const MAX_NAME_BYTES: usize = types_core::fmgr::NAMEDATALEN as usize - 1;

/// A parsed prefix-matching name.
#[derive(Debug, PartialEq, Eq)]
pub enum MintShape<'a> {
    /// `<prefix><template>__<token>`: template selected by name.
    Template { template: &'a str, token: &'a str },
    /// Prefix-matching but not the template form (no `__` separator, or an
    /// empty template/token side): never mints. The mint path FATALs with
    /// the required form for authorized roles and falls through to the
    /// stock does-not-exist error for everyone else.
    Malformed,
}

/// Parse `name` against the configured `prefix`. `None` = the name is not
/// mint-relevant at all (feature off, prefix mismatch, over-long, or a
/// reserved warm-pool spare name) — the caller falls through to the stock
/// does-not-exist FATAL unconditionally.
pub fn parse_mint_name<'a>(prefix: &str, name: &'a str) -> Option<MintShape<'a>> {
    if prefix.is_empty() || name.len() > MAX_NAME_BYTES {
        return None;
    }
    let rest = name.strip_prefix(prefix)?;
    if rest.is_empty() {
        return None;
    }
    // D3 warm-pool namespace reservation: `<prefix>spare_<seq>` (seq =
    // decimal digits) is the janitor's own spare namespace. It can never
    // mint anyway (no `__`), but it keeps the STOCK error rather than the
    // authorized-role form FATAL: the pool's namespace stays opaque, and no
    // client Ensure can ever collide with a spare name. Exact-shape
    // reservation only: `spare_x` and templates named `spare` stay legal.
    if let Some(seq) = rest.strip_prefix("spare_") {
        if !seq.is_empty() && seq.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
    }
    if let Some((template, token)) = rest.split_once("__") {
        if !template.is_empty() && !token.is_empty() {
            return Some(MintShape::Template { template, token });
        }
    }
    Some(MintShape::Malformed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_form_splits_at_first_double_underscore() {
        assert_eq!(
            parse_mint_name("tdb_", "tdb_tpl_abc__w1"),
            Some(MintShape::Template {
                template: "tpl_abc",
                token: "w1"
            })
        );
        // FIRST __ wins; the remainder (underscores included) is the token.
        assert_eq!(
            parse_mint_name("tdb_", "tdb_a__b__c"),
            Some(MintShape::Template {
                template: "a",
                token: "b__c"
            })
        );
    }

    #[test]
    fn non_template_forms_are_malformed() {
        // No `__` separator: the old bare form is gone (ruling 2026-08-05).
        assert_eq!(parse_mint_name("tdb_", "tdb_w1"), Some(MintShape::Malformed));
        assert_eq!(
            parse_mint_name("tdb_", "tdb_a_b_c"),
            Some(MintShape::Malformed)
        );
        // Empty template side (`tdb___x`) and empty token side (`tdb_tpl__`):
        // refused as template form, malformed — never a silent bare mint.
        assert_eq!(parse_mint_name("tdb_", "tdb___x"), Some(MintShape::Malformed));
        assert_eq!(
            parse_mint_name("tdb_", "tdb_tpl__"),
            Some(MintShape::Malformed)
        );
    }

    #[test]
    fn template_names_containing_double_underscore_are_unreachable() {
        // The FIRST __ always wins the split: an intended template
        // "my__tpl" can never be selected — its would-be clone resolves to
        // template "my" (likely does-not-exist at mint time).
        assert_eq!(
            parse_mint_name("tdb_", "tdb_my__tpl__x"),
            Some(MintShape::Template {
                template: "my",
                token: "tpl__x"
            })
        );
    }

    #[test]
    fn spare_namespace_is_reserved() {
        // `<prefix>spare_<digits>` is None (STOCK error, not the form
        // FATAL): the warm pool owns that namespace and stays opaque.
        assert_eq!(parse_mint_name("tdb_", "tdb_spare_1"), None);
        assert_eq!(parse_mint_name("tdb_", "tdb_spare_007"), None);
        assert_eq!(
            parse_mint_name("tdb_", "tdb_spare_18446744073709551615"),
            None
        );
        // Exact shape only: non-digit tails and a bare `spare_` (empty seq)
        // are ordinary malformed names; the template form keeps its meaning.
        assert_eq!(
            parse_mint_name("tdb_", "tdb_spare_x"),
            Some(MintShape::Malformed)
        );
        assert_eq!(
            parse_mint_name("tdb_", "tdb_spare_"),
            Some(MintShape::Malformed)
        );
        assert_eq!(
            parse_mint_name("tdb_", "tdb_spare_1a"),
            Some(MintShape::Malformed)
        );
        assert_eq!(
            parse_mint_name("tdb_", "tdb_spare__x"),
            Some(MintShape::Template {
                template: "spare",
                token: "x"
            })
        );
    }

    #[test]
    fn prefix_scoping_is_exact_and_empty_prefix_is_off() {
        assert_eq!(parse_mint_name("tdb_", "tx_w1"), None);
        assert_eq!(parse_mint_name("tdb_", "tdb_"), None); // empty rest
        assert_eq!(parse_mint_name("tdb_", "tv"), None);
        // Feature off: nothing matches (every string starts with "").
        assert_eq!(parse_mint_name("", "tdb_w1"), None);
    }

    #[test]
    fn whole_name_byte_limit_is_enforced() {
        let ok = format!("tdb_a__{}", "a".repeat(MAX_NAME_BYTES - 7));
        assert_eq!(ok.len(), MAX_NAME_BYTES);
        assert!(matches!(
            parse_mint_name("tdb_", &ok),
            Some(MintShape::Template { .. })
        ));
        let long = format!("tdb_a__{}", "a".repeat(MAX_NAME_BYTES - 6));
        assert_eq!(long.len(), MAX_NAME_BYTES + 1);
        assert_eq!(parse_mint_name("tdb_", &long), None);
        // The limit is BYTES, not chars.
        let wide = format!("tdb_{}", "é".repeat(31)); // 4 + 62 = 66 bytes
        assert_eq!(parse_mint_name("tdb_", &wide), None);
    }
}
