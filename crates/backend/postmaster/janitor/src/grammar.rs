//! The D2 mint-on-connect name grammar (docs/design/test-views.md D2) —
//! pure string logic, no catalog/GUC I/O, so both sides of the mint path
//! (the backend seam impl and the janitor's reap-side grace attribution)
//! parse ONE definition, unit-tested here.
//!
//! Grammar, defined in terms of the configured prefix (never a literal):
//!
//! ```text
//!   <prefix><template>__<token>   selects the sealed template named
//!                                 <template> (the RAW database name, e.g.
//!                                 tpl_5677ece429c5 — templates conventionally
//!                                 live OUTSIDE the prefix);
//!   <prefix><token>               bare form: the template comes from
//!                                 pgrust.ephemeral_db_default_template
//!                                 ('' = bare tokens refuse to mint).
//! ```
//!
//! Resolution rules, decidable from the NAME ALONE (the input-decidability
//! law — reap-side grace attribution must never depend on catalog state):
//!
//! - The 63-byte (NAMEDATALEN-1) identifier limit applies to the WHOLE name.
//!   Longer names never match: CREATE DATABASE would truncate them, so the
//!   minted datname could not byte-equal the requested name and the connect
//!   retry would re-miss. Refusing input-side keeps the stock FATAL.
//! - The FIRST `__` splits template from token; both sides must be
//!   non-empty. A rest with no `__` (or one where a side would be empty,
//!   e.g. `tv___x` or `tv_tpl__`) is the bare form. Consequences,
//!   documented deliberately: a bare token contains `__` only via that
//!   empty-side fallback (`tv___x` -> bare `__x`, `tv_tpl__` -> bare
//!   `tpl__`), never otherwise; and because the FIRST `__` always wins the
//!   split, a template whose OWN name contains `__` is unreachable by the
//!   grammar (`tv_my__tpl__x` selects template `my`, token `tpl__x` —
//!   never a template named `my__tpl`), so grace overrides set for such a
//!   template can never match a clone.
//! - The empty prefix means the feature is off: nothing matches, even
//!   though every string starts with `""` (the reap_candidate convention).

/// NAMEDATALEN - 1: the datname byte budget.
pub const MAX_NAME_BYTES: usize = types_core::fmgr::NAMEDATALEN as usize - 1;

/// A successfully parsed mint-eligible name.
#[derive(Debug, PartialEq, Eq)]
pub enum MintShape<'a> {
    /// `<prefix><template>__<token>`: template selected by name.
    Template { template: &'a str, token: &'a str },
    /// `<prefix><token>`: template comes from
    /// pgrust.ephemeral_db_default_template.
    Bare { token: &'a str },
}

/// Parse `name` against the configured `prefix`. `None` = the name is not
/// mint-eligible (feature off, prefix mismatch, over-long, or an empty
/// token) — the caller falls through to the stock does-not-exist FATAL.
pub fn parse_mint_name<'a>(prefix: &str, name: &'a str) -> Option<MintShape<'a>> {
    if prefix.is_empty() || name.len() > MAX_NAME_BYTES {
        return None;
    }
    let rest = name.strip_prefix(prefix)?;
    if rest.is_empty() {
        return None;
    }
    if let Some((template, token)) = rest.split_once("__") {
        if !template.is_empty() && !token.is_empty() {
            return Some(MintShape::Template { template, token });
        }
    }
    Some(MintShape::Bare { token: rest })
}

/// Reap-side grace attribution: the template a database name declares
/// membership in, or `None` for bare/non-matching names (which reap on the
/// default grace). "The grammar defines template membership" (spec D2) —
/// bare tokens are deliberately NOT attributed to the default template:
/// attribution must be decidable from the name alone, and the default-
/// template GUC can change between mint and reap.
pub fn template_of<'a>(prefix: &str, name: &'a str) -> Option<&'a str> {
    match parse_mint_name(prefix, name) {
        Some(MintShape::Template { template, .. }) => Some(template),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_form_splits_at_first_double_underscore() {
        assert_eq!(
            parse_mint_name("tv_", "tv_tpl_abc__w1"),
            Some(MintShape::Template {
                template: "tpl_abc",
                token: "w1"
            })
        );
        // FIRST __ wins; the remainder (underscores included) is the token.
        assert_eq!(
            parse_mint_name("tv_", "tv_a__b__c"),
            Some(MintShape::Template {
                template: "a",
                token: "b__c"
            })
        );
    }

    #[test]
    fn bare_form_takes_the_whole_rest() {
        assert_eq!(
            parse_mint_name("tv_", "tv_w1"),
            Some(MintShape::Bare { token: "w1" })
        );
        // Single underscores never trigger the template form.
        assert_eq!(
            parse_mint_name("tv_", "tv_a_b_c"),
            Some(MintShape::Bare { token: "a_b_c" })
        );
    }

    #[test]
    fn empty_sides_fall_back_to_bare() {
        // Empty template side: the ("", "x") split is refused, so the whole
        // rest becomes the bare token.
        assert_eq!(
            parse_mint_name("tv_", "tv___x"),
            Some(MintShape::Bare { token: "__x" })
        );
        // Empty token side (`tpl__` + ""): refused as template, bare form.
        assert_eq!(
            parse_mint_name("tv_", "tv_tpl__"),
            Some(MintShape::Bare { token: "tpl__" })
        );
    }

    #[test]
    fn template_names_containing_double_underscore_are_unreachable() {
        // The FIRST __ always wins the split: an intended template
        // "my__tpl" can never be selected — its would-be clone resolves to
        // template "my". Consequences (module doc + M3 addendum item 2):
        // mint_one will look up a template named "my" (likely
        // does-not-exist), reap-side grace attributes tv_my__tpl__x to any
        // "my" override, and pgrust_set_template_grace('my__tpl', ...)
        // can never match a clone.
        assert_eq!(
            parse_mint_name("tv_", "tv_my__tpl__x"),
            Some(MintShape::Template {
                template: "my",
                token: "tpl__x"
            })
        );
        assert_eq!(template_of("tv_", "tv_my__tpl__x"), Some("my"));
    }

    #[test]
    fn prefix_scoping_is_exact_and_empty_prefix_is_off() {
        assert_eq!(parse_mint_name("tv_", "tx_w1"), None);
        assert_eq!(parse_mint_name("tv_", "tv_"), None); // empty token
        assert_eq!(parse_mint_name("tv_", "tv"), None);
        // Feature off: nothing matches (every string starts with "").
        assert_eq!(parse_mint_name("", "tv_w1"), None);
    }

    #[test]
    fn whole_name_byte_limit_is_enforced() {
        let ok = format!("tv_{}", "a".repeat(MAX_NAME_BYTES - 3));
        assert_eq!(ok.len(), MAX_NAME_BYTES);
        assert!(parse_mint_name("tv_", &ok).is_some());
        let long = format!("tv_{}", "a".repeat(MAX_NAME_BYTES - 2));
        assert_eq!(long.len(), MAX_NAME_BYTES + 1);
        assert_eq!(parse_mint_name("tv_", &long), None);
        // The limit is BYTES, not chars.
        let wide = format!("tv_{}", "é".repeat(31)); // 3 + 62 = 65 bytes
        assert_eq!(parse_mint_name("tv_", &wide), None);
    }

    #[test]
    fn template_of_attributes_only_the_template_form() {
        assert_eq!(template_of("tv_", "tv_tpl_a__x"), Some("tpl_a"));
        assert_eq!(template_of("tv_", "tv_bare"), None);
        assert_eq!(template_of("tv_", "other"), None);
        assert_eq!(template_of("", "tv_tpl_a__x"), None);
    }
}
