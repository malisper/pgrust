use crate::setup::monetary_and_numeric_are_c;

pub const CHAR_MAX: i8 = 127;

pub struct PgLconv {
    pub mon_decimal_point: &'static str,
    pub mon_thousands_sep: &'static str,
    pub mon_grouping: &'static str,
    pub currency_symbol: &'static str,
    pub positive_sign: &'static str,
    pub negative_sign: &'static str,
    pub frac_digits: i8,
    pub p_cs_precedes: i8,
    pub n_cs_precedes: i8,
    pub p_sep_by_space: i8,
    pub n_sep_by_space: i8,
    pub p_sign_posn: i8,
    pub n_sign_posn: i8,
}

static C_LOCALE_LCONV: PgLconv = PgLconv {
    mon_decimal_point: "",
    mon_thousands_sep: "",
    mon_grouping: "",
    currency_symbol: "",
    positive_sign: "",
    negative_sign: "",
    frac_digits: CHAR_MAX,
    p_cs_precedes: CHAR_MAX,
    n_cs_precedes: CHAR_MAX,
    p_sep_by_space: CHAR_MAX,
    n_sep_by_space: CHAR_MAX,
    p_sign_posn: CHAR_MAX,
    n_sign_posn: CHAR_MAX,
};

// C caches one converted copy guarded by CurrentLocaleConvValid (the monetary/
// numeric assign hooks invalidate it); constant here until the pg_localeconv_r
// port admits non-C locales.
pub fn pglc_localeconv() -> &'static PgLconv {
    if !monetary_and_numeric_are_c() {
        panic!("PGLC_localeconv: non-C lc_monetary/lc_numeric requires pg_localeconv_r (unported)");
    }
    &C_LOCALE_LCONV
}
