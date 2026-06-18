use super::*;

pub(crate) static unicode_opt_ascii: [pg_unicode_properties; 128] = [
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_WHITE_SPACE as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_WHITE_SPACE as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_WHITE_SPACE as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_WHITE_SPACE as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_WHITE_SPACE as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_SPACE_SEPARATOR as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_WHITE_SPACE as uint8,
    },
    pg_unicode_properties {
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_CASE_IGNORABLE as uint8,
    },
    pg_unicode_properties {
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_CASE_IGNORABLE as uint8,
    },
    pg_unicode_properties {
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_HEX_DIGIT as uint8,
    },
    pg_unicode_properties {
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_HEX_DIGIT as uint8,
    },
    pg_unicode_properties {
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_HEX_DIGIT as uint8,
    },
    pg_unicode_properties {
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_HEX_DIGIT as uint8,
    },
    pg_unicode_properties {
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_HEX_DIGIT as uint8,
    },
    pg_unicode_properties {
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_HEX_DIGIT as uint8,
    },
    pg_unicode_properties {
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_HEX_DIGIT as uint8,
    },
    pg_unicode_properties {
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_HEX_DIGIT as uint8,
    },
    pg_unicode_properties {
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_HEX_DIGIT as uint8,
    },
    pg_unicode_properties {
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_HEX_DIGIT as uint8,
    },
    pg_unicode_properties {
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_CASE_IGNORABLE as uint8,
    },
    pg_unicode_properties {
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_HEX_DIGIT
            | PG_U_PROP_ALPHABETIC
            | PG_U_PROP_UPPERCASE
            | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_HEX_DIGIT
            | PG_U_PROP_ALPHABETIC
            | PG_U_PROP_UPPERCASE
            | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_HEX_DIGIT
            | PG_U_PROP_ALPHABETIC
            | PG_U_PROP_UPPERCASE
            | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_HEX_DIGIT
            | PG_U_PROP_ALPHABETIC
            | PG_U_PROP_UPPERCASE
            | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_HEX_DIGIT
            | PG_U_PROP_ALPHABETIC
            | PG_U_PROP_UPPERCASE
            | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_HEX_DIGIT
            | PG_U_PROP_ALPHABETIC
            | PG_U_PROP_UPPERCASE
            | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_UPPERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_UPPERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_UPPERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_UPPERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_UPPERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_UPPERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_UPPERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_UPPERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_UPPERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_UPPERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_UPPERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_UPPERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_UPPERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_UPPERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_UPPERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_UPPERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_UPPERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_UPPERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_UPPERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_UPPERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_CASE_IGNORABLE as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONNECTOR_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
        properties: PG_U_PROP_CASE_IGNORABLE as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_HEX_DIGIT
            | PG_U_PROP_ALPHABETIC
            | PG_U_PROP_LOWERCASE
            | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_HEX_DIGIT
            | PG_U_PROP_ALPHABETIC
            | PG_U_PROP_LOWERCASE
            | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_HEX_DIGIT
            | PG_U_PROP_ALPHABETIC
            | PG_U_PROP_LOWERCASE
            | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_HEX_DIGIT
            | PG_U_PROP_ALPHABETIC
            | PG_U_PROP_LOWERCASE
            | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_HEX_DIGIT
            | PG_U_PROP_ALPHABETIC
            | PG_U_PROP_LOWERCASE
            | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_HEX_DIGIT
            | PG_U_PROP_ALPHABETIC
            | PG_U_PROP_LOWERCASE
            | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_LOWERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_LOWERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_LOWERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_LOWERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_LOWERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_LOWERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_LOWERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_LOWERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_LOWERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_LOWERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_LOWERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_LOWERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_LOWERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_LOWERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_LOWERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_LOWERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_LOWERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_LOWERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_LOWERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
        properties: (PG_U_PROP_ALPHABETIC | PG_U_PROP_LOWERCASE | PG_U_PROP_CASED) as uint8,
    },
    pg_unicode_properties {
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
    pg_unicode_properties {
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
        properties: 0 as uint8,
    },
];
pub(crate) static unicode_categories: [pg_category_range; 3368] = [
    pg_category_range {
        first: 0 as uint32,
        last: 0x1f as uint32,
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x20 as uint32,
        last: 0x20 as uint32,
        category: PG_U_SPACE_SEPARATOR as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21 as uint32,
        last: 0x23 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x24 as uint32,
        last: 0x24 as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x25 as uint32,
        last: 0x27 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x28 as uint32,
        last: 0x28 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x29 as uint32,
        last: 0x29 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2a as uint32,
        last: 0x2a as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2b as uint32,
        last: 0x2b as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c as uint32,
        last: 0x2c as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2d as uint32,
        last: 0x2d as uint32,
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e as uint32,
        last: 0x2f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x30 as uint32,
        last: 0x39 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3a as uint32,
        last: 0x3b as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3c as uint32,
        last: 0x3e as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3f as uint32,
        last: 0x40 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x41 as uint32,
        last: 0x5a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x5b as uint32,
        last: 0x5b as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x5c as uint32,
        last: 0x5c as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x5d as uint32,
        last: 0x5d as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x5e as uint32,
        last: 0x5e as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x5f as uint32,
        last: 0x5f as uint32,
        category: PG_U_CONNECTOR_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x60 as uint32,
        last: 0x60 as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x61 as uint32,
        last: 0x7a as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x7b as uint32,
        last: 0x7b as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x7c as uint32,
        last: 0x7c as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x7d as uint32,
        last: 0x7d as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x7e as uint32,
        last: 0x7e as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x7f as uint32,
        last: 0x9f as uint32,
        category: PG_U_CONTROL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa0 as uint32,
        last: 0xa0 as uint32,
        category: PG_U_SPACE_SEPARATOR as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa1 as uint32,
        last: 0xa1 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa2 as uint32,
        last: 0xa5 as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa6 as uint32,
        last: 0xa6 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7 as uint32,
        last: 0xa7 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa8 as uint32,
        last: 0xa8 as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa9 as uint32,
        last: 0xa9 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa as uint32,
        last: 0xaa as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xab as uint32,
        last: 0xab as uint32,
        category: PG_U_INITIAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xac as uint32,
        last: 0xac as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xad as uint32,
        last: 0xad as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xae as uint32,
        last: 0xae as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaf as uint32,
        last: 0xaf as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb0 as uint32,
        last: 0xb0 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb1 as uint32,
        last: 0xb1 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb2 as uint32,
        last: 0xb3 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb4 as uint32,
        last: 0xb4 as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb5 as uint32,
        last: 0xb5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb6 as uint32,
        last: 0xb7 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb8 as uint32,
        last: 0xb8 as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb9 as uint32,
        last: 0xb9 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xba as uint32,
        last: 0xba as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xbb as uint32,
        last: 0xbb as uint32,
        category: PG_U_FINAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xbc as uint32,
        last: 0xbe as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xbf as uint32,
        last: 0xbf as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc0 as uint32,
        last: 0xd6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd7 as uint32,
        last: 0xd7 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd8 as uint32,
        last: 0xde as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xdf as uint32,
        last: 0xf6 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf7 as uint32,
        last: 0xf7 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf8 as uint32,
        last: 0xff as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x100 as uint32,
        last: 0x100 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x101 as uint32,
        last: 0x101 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x102 as uint32,
        last: 0x102 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x103 as uint32,
        last: 0x103 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x104 as uint32,
        last: 0x104 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x105 as uint32,
        last: 0x105 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x106 as uint32,
        last: 0x106 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x107 as uint32,
        last: 0x107 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x108 as uint32,
        last: 0x108 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x109 as uint32,
        last: 0x109 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10a as uint32,
        last: 0x10a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10b as uint32,
        last: 0x10b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10c as uint32,
        last: 0x10c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10d as uint32,
        last: 0x10d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10e as uint32,
        last: 0x10e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10f as uint32,
        last: 0x10f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x110 as uint32,
        last: 0x110 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x111 as uint32,
        last: 0x111 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x112 as uint32,
        last: 0x112 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x113 as uint32,
        last: 0x113 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x114 as uint32,
        last: 0x114 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x115 as uint32,
        last: 0x115 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x116 as uint32,
        last: 0x116 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x117 as uint32,
        last: 0x117 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x118 as uint32,
        last: 0x118 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x119 as uint32,
        last: 0x119 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a as uint32,
        last: 0x11a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11b as uint32,
        last: 0x11b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11c as uint32,
        last: 0x11c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d as uint32,
        last: 0x11d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11e as uint32,
        last: 0x11e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11f as uint32,
        last: 0x11f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x120 as uint32,
        last: 0x120 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x121 as uint32,
        last: 0x121 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x122 as uint32,
        last: 0x122 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x123 as uint32,
        last: 0x123 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x124 as uint32,
        last: 0x124 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x125 as uint32,
        last: 0x125 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x126 as uint32,
        last: 0x126 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x127 as uint32,
        last: 0x127 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x128 as uint32,
        last: 0x128 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x129 as uint32,
        last: 0x129 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x12a as uint32,
        last: 0x12a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x12b as uint32,
        last: 0x12b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x12c as uint32,
        last: 0x12c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x12d as uint32,
        last: 0x12d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x12e as uint32,
        last: 0x12e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x12f as uint32,
        last: 0x12f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x130 as uint32,
        last: 0x130 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x131 as uint32,
        last: 0x131 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x132 as uint32,
        last: 0x132 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x133 as uint32,
        last: 0x133 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x134 as uint32,
        last: 0x134 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x135 as uint32,
        last: 0x135 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x136 as uint32,
        last: 0x136 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x137 as uint32,
        last: 0x138 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x139 as uint32,
        last: 0x139 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x13a as uint32,
        last: 0x13a as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x13b as uint32,
        last: 0x13b as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x13c as uint32,
        last: 0x13c as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x13d as uint32,
        last: 0x13d as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x13e as uint32,
        last: 0x13e as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x13f as uint32,
        last: 0x13f as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x140 as uint32,
        last: 0x140 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x141 as uint32,
        last: 0x141 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x142 as uint32,
        last: 0x142 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x143 as uint32,
        last: 0x143 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x144 as uint32,
        last: 0x144 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x145 as uint32,
        last: 0x145 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x146 as uint32,
        last: 0x146 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x147 as uint32,
        last: 0x147 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x148 as uint32,
        last: 0x149 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x14a as uint32,
        last: 0x14a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x14b as uint32,
        last: 0x14b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x14c as uint32,
        last: 0x14c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x14d as uint32,
        last: 0x14d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x14e as uint32,
        last: 0x14e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x14f as uint32,
        last: 0x14f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x150 as uint32,
        last: 0x150 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x151 as uint32,
        last: 0x151 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x152 as uint32,
        last: 0x152 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x153 as uint32,
        last: 0x153 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x154 as uint32,
        last: 0x154 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x155 as uint32,
        last: 0x155 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x156 as uint32,
        last: 0x156 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x157 as uint32,
        last: 0x157 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x158 as uint32,
        last: 0x158 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x159 as uint32,
        last: 0x159 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x15a as uint32,
        last: 0x15a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x15b as uint32,
        last: 0x15b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x15c as uint32,
        last: 0x15c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x15d as uint32,
        last: 0x15d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x15e as uint32,
        last: 0x15e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x15f as uint32,
        last: 0x15f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x160 as uint32,
        last: 0x160 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x161 as uint32,
        last: 0x161 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x162 as uint32,
        last: 0x162 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x163 as uint32,
        last: 0x163 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x164 as uint32,
        last: 0x164 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x165 as uint32,
        last: 0x165 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x166 as uint32,
        last: 0x166 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x167 as uint32,
        last: 0x167 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x168 as uint32,
        last: 0x168 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x169 as uint32,
        last: 0x169 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16a as uint32,
        last: 0x16a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16b as uint32,
        last: 0x16b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16c as uint32,
        last: 0x16c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16d as uint32,
        last: 0x16d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16e as uint32,
        last: 0x16e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16f as uint32,
        last: 0x16f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x170 as uint32,
        last: 0x170 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x171 as uint32,
        last: 0x171 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x172 as uint32,
        last: 0x172 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x173 as uint32,
        last: 0x173 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x174 as uint32,
        last: 0x174 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x175 as uint32,
        last: 0x175 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x176 as uint32,
        last: 0x176 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x177 as uint32,
        last: 0x177 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x178 as uint32,
        last: 0x179 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17a as uint32,
        last: 0x17a as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17b as uint32,
        last: 0x17b as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17c as uint32,
        last: 0x17c as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17d as uint32,
        last: 0x17d as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17e as uint32,
        last: 0x180 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x181 as uint32,
        last: 0x182 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x183 as uint32,
        last: 0x183 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x184 as uint32,
        last: 0x184 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x185 as uint32,
        last: 0x185 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x186 as uint32,
        last: 0x187 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x188 as uint32,
        last: 0x188 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x189 as uint32,
        last: 0x18b as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x18c as uint32,
        last: 0x18d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x18e as uint32,
        last: 0x191 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x192 as uint32,
        last: 0x192 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x193 as uint32,
        last: 0x194 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x195 as uint32,
        last: 0x195 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x196 as uint32,
        last: 0x198 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x199 as uint32,
        last: 0x19b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x19c as uint32,
        last: 0x19d as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x19e as uint32,
        last: 0x19e as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x19f as uint32,
        last: 0x1a0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a1 as uint32,
        last: 0x1a1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a2 as uint32,
        last: 0x1a2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a3 as uint32,
        last: 0x1a3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a4 as uint32,
        last: 0x1a4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a5 as uint32,
        last: 0x1a5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a6 as uint32,
        last: 0x1a7 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a8 as uint32,
        last: 0x1a8 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a9 as uint32,
        last: 0x1a9 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1aa as uint32,
        last: 0x1ab as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ac as uint32,
        last: 0x1ac as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ad as uint32,
        last: 0x1ad as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ae as uint32,
        last: 0x1af as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b0 as uint32,
        last: 0x1b0 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b1 as uint32,
        last: 0x1b3 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b4 as uint32,
        last: 0x1b4 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b5 as uint32,
        last: 0x1b5 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b6 as uint32,
        last: 0x1b6 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b7 as uint32,
        last: 0x1b8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b9 as uint32,
        last: 0x1ba as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bb as uint32,
        last: 0x1bb as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bc as uint32,
        last: 0x1bc as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bd as uint32,
        last: 0x1bf as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c0 as uint32,
        last: 0x1c3 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c4 as uint32,
        last: 0x1c4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c5 as uint32,
        last: 0x1c5 as uint32,
        category: PG_U_TITLECASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c6 as uint32,
        last: 0x1c6 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c7 as uint32,
        last: 0x1c7 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c8 as uint32,
        last: 0x1c8 as uint32,
        category: PG_U_TITLECASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c9 as uint32,
        last: 0x1c9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ca as uint32,
        last: 0x1ca as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1cb as uint32,
        last: 0x1cb as uint32,
        category: PG_U_TITLECASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1cc as uint32,
        last: 0x1cc as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1cd as uint32,
        last: 0x1cd as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ce as uint32,
        last: 0x1ce as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1cf as uint32,
        last: 0x1cf as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d0 as uint32,
        last: 0x1d0 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d1 as uint32,
        last: 0x1d1 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d2 as uint32,
        last: 0x1d2 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d3 as uint32,
        last: 0x1d3 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d4 as uint32,
        last: 0x1d4 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d5 as uint32,
        last: 0x1d5 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d6 as uint32,
        last: 0x1d6 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d7 as uint32,
        last: 0x1d7 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d8 as uint32,
        last: 0x1d8 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d9 as uint32,
        last: 0x1d9 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1da as uint32,
        last: 0x1da as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1db as uint32,
        last: 0x1db as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1dc as uint32,
        last: 0x1dd as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1de as uint32,
        last: 0x1de as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1df as uint32,
        last: 0x1df as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e0 as uint32,
        last: 0x1e0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e1 as uint32,
        last: 0x1e1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e2 as uint32,
        last: 0x1e2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e3 as uint32,
        last: 0x1e3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e4 as uint32,
        last: 0x1e4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e5 as uint32,
        last: 0x1e5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e6 as uint32,
        last: 0x1e6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e7 as uint32,
        last: 0x1e7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e8 as uint32,
        last: 0x1e8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e9 as uint32,
        last: 0x1e9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ea as uint32,
        last: 0x1ea as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eb as uint32,
        last: 0x1eb as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ec as uint32,
        last: 0x1ec as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ed as uint32,
        last: 0x1ed as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee as uint32,
        last: 0x1ee as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ef as uint32,
        last: 0x1f0 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f1 as uint32,
        last: 0x1f1 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f2 as uint32,
        last: 0x1f2 as uint32,
        category: PG_U_TITLECASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f3 as uint32,
        last: 0x1f3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f4 as uint32,
        last: 0x1f4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f5 as uint32,
        last: 0x1f5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f6 as uint32,
        last: 0x1f8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f9 as uint32,
        last: 0x1f9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fa as uint32,
        last: 0x1fa as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fb as uint32,
        last: 0x1fb as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fc as uint32,
        last: 0x1fc as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fd as uint32,
        last: 0x1fd as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fe as uint32,
        last: 0x1fe as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ff as uint32,
        last: 0x1ff as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x200 as uint32,
        last: 0x200 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x201 as uint32,
        last: 0x201 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x202 as uint32,
        last: 0x202 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x203 as uint32,
        last: 0x203 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x204 as uint32,
        last: 0x204 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x205 as uint32,
        last: 0x205 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x206 as uint32,
        last: 0x206 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x207 as uint32,
        last: 0x207 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x208 as uint32,
        last: 0x208 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x209 as uint32,
        last: 0x209 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x20a as uint32,
        last: 0x20a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x20b as uint32,
        last: 0x20b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x20c as uint32,
        last: 0x20c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x20d as uint32,
        last: 0x20d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x20e as uint32,
        last: 0x20e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x20f as uint32,
        last: 0x20f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x210 as uint32,
        last: 0x210 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x211 as uint32,
        last: 0x211 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x212 as uint32,
        last: 0x212 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x213 as uint32,
        last: 0x213 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x214 as uint32,
        last: 0x214 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x215 as uint32,
        last: 0x215 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x216 as uint32,
        last: 0x216 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x217 as uint32,
        last: 0x217 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x218 as uint32,
        last: 0x218 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x219 as uint32,
        last: 0x219 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21a as uint32,
        last: 0x21a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21b as uint32,
        last: 0x21b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21c as uint32,
        last: 0x21c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21d as uint32,
        last: 0x21d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21e as uint32,
        last: 0x21e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21f as uint32,
        last: 0x21f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x220 as uint32,
        last: 0x220 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x221 as uint32,
        last: 0x221 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x222 as uint32,
        last: 0x222 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x223 as uint32,
        last: 0x223 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x224 as uint32,
        last: 0x224 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x225 as uint32,
        last: 0x225 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x226 as uint32,
        last: 0x226 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x227 as uint32,
        last: 0x227 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x228 as uint32,
        last: 0x228 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x229 as uint32,
        last: 0x229 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x22a as uint32,
        last: 0x22a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x22b as uint32,
        last: 0x22b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x22c as uint32,
        last: 0x22c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x22d as uint32,
        last: 0x22d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x22e as uint32,
        last: 0x22e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x22f as uint32,
        last: 0x22f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x230 as uint32,
        last: 0x230 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x231 as uint32,
        last: 0x231 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x232 as uint32,
        last: 0x232 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x233 as uint32,
        last: 0x239 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x23a as uint32,
        last: 0x23b as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x23c as uint32,
        last: 0x23c as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x23d as uint32,
        last: 0x23e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x23f as uint32,
        last: 0x240 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x241 as uint32,
        last: 0x241 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x242 as uint32,
        last: 0x242 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x243 as uint32,
        last: 0x246 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x247 as uint32,
        last: 0x247 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x248 as uint32,
        last: 0x248 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x249 as uint32,
        last: 0x249 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x24a as uint32,
        last: 0x24a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x24b as uint32,
        last: 0x24b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x24c as uint32,
        last: 0x24c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x24d as uint32,
        last: 0x24d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x24e as uint32,
        last: 0x24e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x24f as uint32,
        last: 0x293 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x294 as uint32,
        last: 0x294 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x295 as uint32,
        last: 0x2af as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2b0 as uint32,
        last: 0x2c1 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c2 as uint32,
        last: 0x2c5 as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c6 as uint32,
        last: 0x2d1 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2d2 as uint32,
        last: 0x2df as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e0 as uint32,
        last: 0x2e4 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e5 as uint32,
        last: 0x2eb as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ec as uint32,
        last: 0x2ec as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ed as uint32,
        last: 0x2ed as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ee as uint32,
        last: 0x2ee as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ef as uint32,
        last: 0x2ff as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x300 as uint32,
        last: 0x36f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x370 as uint32,
        last: 0x370 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x371 as uint32,
        last: 0x371 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x372 as uint32,
        last: 0x372 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x373 as uint32,
        last: 0x373 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x374 as uint32,
        last: 0x374 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x375 as uint32,
        last: 0x375 as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x376 as uint32,
        last: 0x376 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x377 as uint32,
        last: 0x377 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x37a as uint32,
        last: 0x37a as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x37b as uint32,
        last: 0x37d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x37e as uint32,
        last: 0x37e as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x37f as uint32,
        last: 0x37f as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x384 as uint32,
        last: 0x385 as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x386 as uint32,
        last: 0x386 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x387 as uint32,
        last: 0x387 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x388 as uint32,
        last: 0x38a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x38c as uint32,
        last: 0x38c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x38e as uint32,
        last: 0x38f as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x390 as uint32,
        last: 0x390 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x391 as uint32,
        last: 0x3a1 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3a3 as uint32,
        last: 0x3ab as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3ac as uint32,
        last: 0x3ce as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3cf as uint32,
        last: 0x3cf as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3d0 as uint32,
        last: 0x3d1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3d2 as uint32,
        last: 0x3d4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3d5 as uint32,
        last: 0x3d7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3d8 as uint32,
        last: 0x3d8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3d9 as uint32,
        last: 0x3d9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3da as uint32,
        last: 0x3da as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3db as uint32,
        last: 0x3db as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3dc as uint32,
        last: 0x3dc as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3dd as uint32,
        last: 0x3dd as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3de as uint32,
        last: 0x3de as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3df as uint32,
        last: 0x3df as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3e0 as uint32,
        last: 0x3e0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3e1 as uint32,
        last: 0x3e1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3e2 as uint32,
        last: 0x3e2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3e3 as uint32,
        last: 0x3e3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3e4 as uint32,
        last: 0x3e4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3e5 as uint32,
        last: 0x3e5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3e6 as uint32,
        last: 0x3e6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3e7 as uint32,
        last: 0x3e7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3e8 as uint32,
        last: 0x3e8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3e9 as uint32,
        last: 0x3e9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3ea as uint32,
        last: 0x3ea as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3eb as uint32,
        last: 0x3eb as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3ec as uint32,
        last: 0x3ec as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3ed as uint32,
        last: 0x3ed as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3ee as uint32,
        last: 0x3ee as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3ef as uint32,
        last: 0x3f3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3f4 as uint32,
        last: 0x3f4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3f5 as uint32,
        last: 0x3f5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3f6 as uint32,
        last: 0x3f6 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3f7 as uint32,
        last: 0x3f7 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3f8 as uint32,
        last: 0x3f8 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3f9 as uint32,
        last: 0x3fa as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3fb as uint32,
        last: 0x3fc as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3fd as uint32,
        last: 0x42f as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x430 as uint32,
        last: 0x45f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x460 as uint32,
        last: 0x460 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x461 as uint32,
        last: 0x461 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x462 as uint32,
        last: 0x462 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x463 as uint32,
        last: 0x463 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x464 as uint32,
        last: 0x464 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x465 as uint32,
        last: 0x465 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x466 as uint32,
        last: 0x466 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x467 as uint32,
        last: 0x467 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x468 as uint32,
        last: 0x468 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x469 as uint32,
        last: 0x469 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x46a as uint32,
        last: 0x46a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x46b as uint32,
        last: 0x46b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x46c as uint32,
        last: 0x46c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x46d as uint32,
        last: 0x46d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x46e as uint32,
        last: 0x46e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x46f as uint32,
        last: 0x46f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x470 as uint32,
        last: 0x470 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x471 as uint32,
        last: 0x471 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x472 as uint32,
        last: 0x472 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x473 as uint32,
        last: 0x473 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x474 as uint32,
        last: 0x474 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x475 as uint32,
        last: 0x475 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x476 as uint32,
        last: 0x476 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x477 as uint32,
        last: 0x477 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x478 as uint32,
        last: 0x478 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x479 as uint32,
        last: 0x479 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x47a as uint32,
        last: 0x47a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x47b as uint32,
        last: 0x47b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x47c as uint32,
        last: 0x47c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x47d as uint32,
        last: 0x47d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x47e as uint32,
        last: 0x47e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x47f as uint32,
        last: 0x47f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x480 as uint32,
        last: 0x480 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x481 as uint32,
        last: 0x481 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x482 as uint32,
        last: 0x482 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x483 as uint32,
        last: 0x487 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x488 as uint32,
        last: 0x489 as uint32,
        category: PG_U_ENCLOSING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x48a as uint32,
        last: 0x48a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x48b as uint32,
        last: 0x48b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x48c as uint32,
        last: 0x48c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x48d as uint32,
        last: 0x48d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x48e as uint32,
        last: 0x48e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x48f as uint32,
        last: 0x48f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x490 as uint32,
        last: 0x490 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x491 as uint32,
        last: 0x491 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x492 as uint32,
        last: 0x492 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x493 as uint32,
        last: 0x493 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x494 as uint32,
        last: 0x494 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x495 as uint32,
        last: 0x495 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x496 as uint32,
        last: 0x496 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x497 as uint32,
        last: 0x497 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x498 as uint32,
        last: 0x498 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x499 as uint32,
        last: 0x499 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x49a as uint32,
        last: 0x49a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x49b as uint32,
        last: 0x49b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x49c as uint32,
        last: 0x49c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x49d as uint32,
        last: 0x49d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x49e as uint32,
        last: 0x49e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x49f as uint32,
        last: 0x49f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4a0 as uint32,
        last: 0x4a0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4a1 as uint32,
        last: 0x4a1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4a2 as uint32,
        last: 0x4a2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4a3 as uint32,
        last: 0x4a3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4a4 as uint32,
        last: 0x4a4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4a5 as uint32,
        last: 0x4a5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4a6 as uint32,
        last: 0x4a6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4a7 as uint32,
        last: 0x4a7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4a8 as uint32,
        last: 0x4a8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4a9 as uint32,
        last: 0x4a9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4aa as uint32,
        last: 0x4aa as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4ab as uint32,
        last: 0x4ab as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4ac as uint32,
        last: 0x4ac as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4ad as uint32,
        last: 0x4ad as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4ae as uint32,
        last: 0x4ae as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4af as uint32,
        last: 0x4af as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4b0 as uint32,
        last: 0x4b0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4b1 as uint32,
        last: 0x4b1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4b2 as uint32,
        last: 0x4b2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4b3 as uint32,
        last: 0x4b3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4b4 as uint32,
        last: 0x4b4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4b5 as uint32,
        last: 0x4b5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4b6 as uint32,
        last: 0x4b6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4b7 as uint32,
        last: 0x4b7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4b8 as uint32,
        last: 0x4b8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4b9 as uint32,
        last: 0x4b9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4ba as uint32,
        last: 0x4ba as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4bb as uint32,
        last: 0x4bb as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4bc as uint32,
        last: 0x4bc as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4bd as uint32,
        last: 0x4bd as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4be as uint32,
        last: 0x4be as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4bf as uint32,
        last: 0x4bf as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4c0 as uint32,
        last: 0x4c1 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4c2 as uint32,
        last: 0x4c2 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4c3 as uint32,
        last: 0x4c3 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4c4 as uint32,
        last: 0x4c4 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4c5 as uint32,
        last: 0x4c5 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4c6 as uint32,
        last: 0x4c6 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4c7 as uint32,
        last: 0x4c7 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4c8 as uint32,
        last: 0x4c8 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4c9 as uint32,
        last: 0x4c9 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4ca as uint32,
        last: 0x4ca as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4cb as uint32,
        last: 0x4cb as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4cc as uint32,
        last: 0x4cc as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4cd as uint32,
        last: 0x4cd as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4ce as uint32,
        last: 0x4cf as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4d0 as uint32,
        last: 0x4d0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4d1 as uint32,
        last: 0x4d1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4d2 as uint32,
        last: 0x4d2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4d3 as uint32,
        last: 0x4d3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4d4 as uint32,
        last: 0x4d4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4d5 as uint32,
        last: 0x4d5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4d6 as uint32,
        last: 0x4d6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4d7 as uint32,
        last: 0x4d7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4d8 as uint32,
        last: 0x4d8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4d9 as uint32,
        last: 0x4d9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4da as uint32,
        last: 0x4da as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4db as uint32,
        last: 0x4db as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4dc as uint32,
        last: 0x4dc as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4dd as uint32,
        last: 0x4dd as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4de as uint32,
        last: 0x4de as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4df as uint32,
        last: 0x4df as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4e0 as uint32,
        last: 0x4e0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4e1 as uint32,
        last: 0x4e1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4e2 as uint32,
        last: 0x4e2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4e3 as uint32,
        last: 0x4e3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4e4 as uint32,
        last: 0x4e4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4e5 as uint32,
        last: 0x4e5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4e6 as uint32,
        last: 0x4e6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4e7 as uint32,
        last: 0x4e7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4e8 as uint32,
        last: 0x4e8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4e9 as uint32,
        last: 0x4e9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4ea as uint32,
        last: 0x4ea as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4eb as uint32,
        last: 0x4eb as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4ec as uint32,
        last: 0x4ec as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4ed as uint32,
        last: 0x4ed as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4ee as uint32,
        last: 0x4ee as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4ef as uint32,
        last: 0x4ef as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4f0 as uint32,
        last: 0x4f0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4f1 as uint32,
        last: 0x4f1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4f2 as uint32,
        last: 0x4f2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4f3 as uint32,
        last: 0x4f3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4f4 as uint32,
        last: 0x4f4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4f5 as uint32,
        last: 0x4f5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4f6 as uint32,
        last: 0x4f6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4f7 as uint32,
        last: 0x4f7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4f8 as uint32,
        last: 0x4f8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4f9 as uint32,
        last: 0x4f9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4fa as uint32,
        last: 0x4fa as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4fb as uint32,
        last: 0x4fb as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4fc as uint32,
        last: 0x4fc as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4fd as uint32,
        last: 0x4fd as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4fe as uint32,
        last: 0x4fe as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4ff as uint32,
        last: 0x4ff as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x500 as uint32,
        last: 0x500 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x501 as uint32,
        last: 0x501 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x502 as uint32,
        last: 0x502 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x503 as uint32,
        last: 0x503 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x504 as uint32,
        last: 0x504 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x505 as uint32,
        last: 0x505 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x506 as uint32,
        last: 0x506 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x507 as uint32,
        last: 0x507 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x508 as uint32,
        last: 0x508 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x509 as uint32,
        last: 0x509 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x50a as uint32,
        last: 0x50a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x50b as uint32,
        last: 0x50b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x50c as uint32,
        last: 0x50c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x50d as uint32,
        last: 0x50d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x50e as uint32,
        last: 0x50e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x50f as uint32,
        last: 0x50f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x510 as uint32,
        last: 0x510 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x511 as uint32,
        last: 0x511 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x512 as uint32,
        last: 0x512 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x513 as uint32,
        last: 0x513 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x514 as uint32,
        last: 0x514 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x515 as uint32,
        last: 0x515 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x516 as uint32,
        last: 0x516 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x517 as uint32,
        last: 0x517 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x518 as uint32,
        last: 0x518 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x519 as uint32,
        last: 0x519 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x51a as uint32,
        last: 0x51a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x51b as uint32,
        last: 0x51b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x51c as uint32,
        last: 0x51c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x51d as uint32,
        last: 0x51d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x51e as uint32,
        last: 0x51e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x51f as uint32,
        last: 0x51f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x520 as uint32,
        last: 0x520 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x521 as uint32,
        last: 0x521 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x522 as uint32,
        last: 0x522 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x523 as uint32,
        last: 0x523 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x524 as uint32,
        last: 0x524 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x525 as uint32,
        last: 0x525 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x526 as uint32,
        last: 0x526 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x527 as uint32,
        last: 0x527 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x528 as uint32,
        last: 0x528 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x529 as uint32,
        last: 0x529 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x52a as uint32,
        last: 0x52a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x52b as uint32,
        last: 0x52b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x52c as uint32,
        last: 0x52c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x52d as uint32,
        last: 0x52d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x52e as uint32,
        last: 0x52e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x52f as uint32,
        last: 0x52f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x531 as uint32,
        last: 0x556 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x559 as uint32,
        last: 0x559 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x55a as uint32,
        last: 0x55f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x560 as uint32,
        last: 0x588 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x589 as uint32,
        last: 0x589 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x58a as uint32,
        last: 0x58a as uint32,
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x58d as uint32,
        last: 0x58e as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x58f as uint32,
        last: 0x58f as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x591 as uint32,
        last: 0x5bd as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x5be as uint32,
        last: 0x5be as uint32,
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x5bf as uint32,
        last: 0x5bf as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x5c0 as uint32,
        last: 0x5c0 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x5c1 as uint32,
        last: 0x5c2 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x5c3 as uint32,
        last: 0x5c3 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x5c4 as uint32,
        last: 0x5c5 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x5c6 as uint32,
        last: 0x5c6 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x5c7 as uint32,
        last: 0x5c7 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x5d0 as uint32,
        last: 0x5ea as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x5ef as uint32,
        last: 0x5f2 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x5f3 as uint32,
        last: 0x5f4 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x600 as uint32,
        last: 0x605 as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x606 as uint32,
        last: 0x608 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x609 as uint32,
        last: 0x60a as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x60b as uint32,
        last: 0x60b as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x60c as uint32,
        last: 0x60d as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x60e as uint32,
        last: 0x60f as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x610 as uint32,
        last: 0x61a as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x61b as uint32,
        last: 0x61b as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x61c as uint32,
        last: 0x61c as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x61d as uint32,
        last: 0x61f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x620 as uint32,
        last: 0x63f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x640 as uint32,
        last: 0x640 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x641 as uint32,
        last: 0x64a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x64b as uint32,
        last: 0x65f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x660 as uint32,
        last: 0x669 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x66a as uint32,
        last: 0x66d as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x66e as uint32,
        last: 0x66f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x670 as uint32,
        last: 0x670 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x671 as uint32,
        last: 0x6d3 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x6d4 as uint32,
        last: 0x6d4 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x6d5 as uint32,
        last: 0x6d5 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x6d6 as uint32,
        last: 0x6dc as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x6dd as uint32,
        last: 0x6dd as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x6de as uint32,
        last: 0x6de as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x6df as uint32,
        last: 0x6e4 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x6e5 as uint32,
        last: 0x6e6 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x6e7 as uint32,
        last: 0x6e8 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x6e9 as uint32,
        last: 0x6e9 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x6ea as uint32,
        last: 0x6ed as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x6ee as uint32,
        last: 0x6ef as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x6f0 as uint32,
        last: 0x6f9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x6fa as uint32,
        last: 0x6fc as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x6fd as uint32,
        last: 0x6fe as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x6ff as uint32,
        last: 0x6ff as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x700 as uint32,
        last: 0x70d as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x70f as uint32,
        last: 0x70f as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x710 as uint32,
        last: 0x710 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x711 as uint32,
        last: 0x711 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x712 as uint32,
        last: 0x72f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x730 as uint32,
        last: 0x74a as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x74d as uint32,
        last: 0x7a5 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x7a6 as uint32,
        last: 0x7b0 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x7b1 as uint32,
        last: 0x7b1 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x7c0 as uint32,
        last: 0x7c9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x7ca as uint32,
        last: 0x7ea as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x7eb as uint32,
        last: 0x7f3 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x7f4 as uint32,
        last: 0x7f5 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x7f6 as uint32,
        last: 0x7f6 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x7f7 as uint32,
        last: 0x7f9 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x7fa as uint32,
        last: 0x7fa as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x7fd as uint32,
        last: 0x7fd as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x7fe as uint32,
        last: 0x7ff as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x800 as uint32,
        last: 0x815 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x816 as uint32,
        last: 0x819 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x81a as uint32,
        last: 0x81a as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x81b as uint32,
        last: 0x823 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x824 as uint32,
        last: 0x824 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x825 as uint32,
        last: 0x827 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x828 as uint32,
        last: 0x828 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x829 as uint32,
        last: 0x82d as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x830 as uint32,
        last: 0x83e as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x840 as uint32,
        last: 0x858 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x859 as uint32,
        last: 0x85b as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x85e as uint32,
        last: 0x85e as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x860 as uint32,
        last: 0x86a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x870 as uint32,
        last: 0x887 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x888 as uint32,
        last: 0x888 as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x889 as uint32,
        last: 0x88e as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x890 as uint32,
        last: 0x891 as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x897 as uint32,
        last: 0x89f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x8a0 as uint32,
        last: 0x8c8 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x8c9 as uint32,
        last: 0x8c9 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x8ca as uint32,
        last: 0x8e1 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x8e2 as uint32,
        last: 0x8e2 as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x8e3 as uint32,
        last: 0x902 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x903 as uint32,
        last: 0x903 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x904 as uint32,
        last: 0x939 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x93a as uint32,
        last: 0x93a as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x93b as uint32,
        last: 0x93b as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x93c as uint32,
        last: 0x93c as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x93d as uint32,
        last: 0x93d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x93e as uint32,
        last: 0x940 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x941 as uint32,
        last: 0x948 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x949 as uint32,
        last: 0x94c as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x94d as uint32,
        last: 0x94d as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x94e as uint32,
        last: 0x94f as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x950 as uint32,
        last: 0x950 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x951 as uint32,
        last: 0x957 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x958 as uint32,
        last: 0x961 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x962 as uint32,
        last: 0x963 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x964 as uint32,
        last: 0x965 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x966 as uint32,
        last: 0x96f as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x970 as uint32,
        last: 0x970 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x971 as uint32,
        last: 0x971 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x972 as uint32,
        last: 0x980 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x981 as uint32,
        last: 0x981 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x982 as uint32,
        last: 0x983 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x985 as uint32,
        last: 0x98c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x98f as uint32,
        last: 0x990 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x993 as uint32,
        last: 0x9a8 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9aa as uint32,
        last: 0x9b0 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9b2 as uint32,
        last: 0x9b2 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9b6 as uint32,
        last: 0x9b9 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9bc as uint32,
        last: 0x9bc as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9bd as uint32,
        last: 0x9bd as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9be as uint32,
        last: 0x9c0 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9c1 as uint32,
        last: 0x9c4 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9c7 as uint32,
        last: 0x9c8 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9cb as uint32,
        last: 0x9cc as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9cd as uint32,
        last: 0x9cd as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9ce as uint32,
        last: 0x9ce as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9d7 as uint32,
        last: 0x9d7 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9dc as uint32,
        last: 0x9dd as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9df as uint32,
        last: 0x9e1 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9e2 as uint32,
        last: 0x9e3 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9e6 as uint32,
        last: 0x9ef as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9f0 as uint32,
        last: 0x9f1 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9f2 as uint32,
        last: 0x9f3 as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9f4 as uint32,
        last: 0x9f9 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9fa as uint32,
        last: 0x9fa as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9fb as uint32,
        last: 0x9fb as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9fc as uint32,
        last: 0x9fc as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9fd as uint32,
        last: 0x9fd as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x9fe as uint32,
        last: 0x9fe as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa01 as uint32,
        last: 0xa02 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa03 as uint32,
        last: 0xa03 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa05 as uint32,
        last: 0xa0a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa0f as uint32,
        last: 0xa10 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa13 as uint32,
        last: 0xa28 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa2a as uint32,
        last: 0xa30 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa32 as uint32,
        last: 0xa33 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa35 as uint32,
        last: 0xa36 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa38 as uint32,
        last: 0xa39 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa3c as uint32,
        last: 0xa3c as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa3e as uint32,
        last: 0xa40 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa41 as uint32,
        last: 0xa42 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa47 as uint32,
        last: 0xa48 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa4b as uint32,
        last: 0xa4d as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa51 as uint32,
        last: 0xa51 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa59 as uint32,
        last: 0xa5c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa5e as uint32,
        last: 0xa5e as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa66 as uint32,
        last: 0xa6f as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa70 as uint32,
        last: 0xa71 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa72 as uint32,
        last: 0xa74 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa75 as uint32,
        last: 0xa75 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa76 as uint32,
        last: 0xa76 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa81 as uint32,
        last: 0xa82 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa83 as uint32,
        last: 0xa83 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa85 as uint32,
        last: 0xa8d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa8f as uint32,
        last: 0xa91 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa93 as uint32,
        last: 0xaa8 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaaa as uint32,
        last: 0xab0 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xab2 as uint32,
        last: 0xab3 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xab5 as uint32,
        last: 0xab9 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xabc as uint32,
        last: 0xabc as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xabd as uint32,
        last: 0xabd as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xabe as uint32,
        last: 0xac0 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xac1 as uint32,
        last: 0xac5 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xac7 as uint32,
        last: 0xac8 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xac9 as uint32,
        last: 0xac9 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xacb as uint32,
        last: 0xacc as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xacd as uint32,
        last: 0xacd as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xad0 as uint32,
        last: 0xad0 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xae0 as uint32,
        last: 0xae1 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xae2 as uint32,
        last: 0xae3 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xae6 as uint32,
        last: 0xaef as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaf0 as uint32,
        last: 0xaf0 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaf1 as uint32,
        last: 0xaf1 as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaf9 as uint32,
        last: 0xaf9 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xafa as uint32,
        last: 0xaff as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb01 as uint32,
        last: 0xb01 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb02 as uint32,
        last: 0xb03 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb05 as uint32,
        last: 0xb0c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb0f as uint32,
        last: 0xb10 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb13 as uint32,
        last: 0xb28 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb2a as uint32,
        last: 0xb30 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb32 as uint32,
        last: 0xb33 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb35 as uint32,
        last: 0xb39 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb3c as uint32,
        last: 0xb3c as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb3d as uint32,
        last: 0xb3d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb3e as uint32,
        last: 0xb3e as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb3f as uint32,
        last: 0xb3f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb40 as uint32,
        last: 0xb40 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb41 as uint32,
        last: 0xb44 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb47 as uint32,
        last: 0xb48 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb4b as uint32,
        last: 0xb4c as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb4d as uint32,
        last: 0xb4d as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb55 as uint32,
        last: 0xb56 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb57 as uint32,
        last: 0xb57 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb5c as uint32,
        last: 0xb5d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb5f as uint32,
        last: 0xb61 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb62 as uint32,
        last: 0xb63 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb66 as uint32,
        last: 0xb6f as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb70 as uint32,
        last: 0xb70 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb71 as uint32,
        last: 0xb71 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb72 as uint32,
        last: 0xb77 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb82 as uint32,
        last: 0xb82 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb83 as uint32,
        last: 0xb83 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb85 as uint32,
        last: 0xb8a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb8e as uint32,
        last: 0xb90 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb92 as uint32,
        last: 0xb95 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb99 as uint32,
        last: 0xb9a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb9c as uint32,
        last: 0xb9c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xb9e as uint32,
        last: 0xb9f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xba3 as uint32,
        last: 0xba4 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xba8 as uint32,
        last: 0xbaa as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xbae as uint32,
        last: 0xbb9 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xbbe as uint32,
        last: 0xbbf as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xbc0 as uint32,
        last: 0xbc0 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xbc1 as uint32,
        last: 0xbc2 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xbc6 as uint32,
        last: 0xbc8 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xbca as uint32,
        last: 0xbcc as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xbcd as uint32,
        last: 0xbcd as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xbd0 as uint32,
        last: 0xbd0 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xbd7 as uint32,
        last: 0xbd7 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xbe6 as uint32,
        last: 0xbef as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xbf0 as uint32,
        last: 0xbf2 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xbf3 as uint32,
        last: 0xbf8 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xbf9 as uint32,
        last: 0xbf9 as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xbfa as uint32,
        last: 0xbfa as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc00 as uint32,
        last: 0xc00 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc01 as uint32,
        last: 0xc03 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc04 as uint32,
        last: 0xc04 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc05 as uint32,
        last: 0xc0c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc0e as uint32,
        last: 0xc10 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc12 as uint32,
        last: 0xc28 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc2a as uint32,
        last: 0xc39 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc3c as uint32,
        last: 0xc3c as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc3d as uint32,
        last: 0xc3d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc3e as uint32,
        last: 0xc40 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc41 as uint32,
        last: 0xc44 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc46 as uint32,
        last: 0xc48 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc4a as uint32,
        last: 0xc4d as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc55 as uint32,
        last: 0xc56 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc58 as uint32,
        last: 0xc5a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc5d as uint32,
        last: 0xc5d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc60 as uint32,
        last: 0xc61 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc62 as uint32,
        last: 0xc63 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc66 as uint32,
        last: 0xc6f as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc77 as uint32,
        last: 0xc77 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc78 as uint32,
        last: 0xc7e as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc7f as uint32,
        last: 0xc7f as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc80 as uint32,
        last: 0xc80 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc81 as uint32,
        last: 0xc81 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc82 as uint32,
        last: 0xc83 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc84 as uint32,
        last: 0xc84 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc85 as uint32,
        last: 0xc8c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc8e as uint32,
        last: 0xc90 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xc92 as uint32,
        last: 0xca8 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xcaa as uint32,
        last: 0xcb3 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xcb5 as uint32,
        last: 0xcb9 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xcbc as uint32,
        last: 0xcbc as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xcbd as uint32,
        last: 0xcbd as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xcbe as uint32,
        last: 0xcbe as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xcbf as uint32,
        last: 0xcbf as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xcc0 as uint32,
        last: 0xcc4 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xcc6 as uint32,
        last: 0xcc6 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xcc7 as uint32,
        last: 0xcc8 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xcca as uint32,
        last: 0xccb as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xccc as uint32,
        last: 0xccd as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xcd5 as uint32,
        last: 0xcd6 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xcdd as uint32,
        last: 0xcde as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xce0 as uint32,
        last: 0xce1 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xce2 as uint32,
        last: 0xce3 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xce6 as uint32,
        last: 0xcef as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xcf1 as uint32,
        last: 0xcf2 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xcf3 as uint32,
        last: 0xcf3 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd00 as uint32,
        last: 0xd01 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd02 as uint32,
        last: 0xd03 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd04 as uint32,
        last: 0xd0c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd0e as uint32,
        last: 0xd10 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd12 as uint32,
        last: 0xd3a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd3b as uint32,
        last: 0xd3c as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd3d as uint32,
        last: 0xd3d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd3e as uint32,
        last: 0xd40 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd41 as uint32,
        last: 0xd44 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd46 as uint32,
        last: 0xd48 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd4a as uint32,
        last: 0xd4c as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd4d as uint32,
        last: 0xd4d as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd4e as uint32,
        last: 0xd4e as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd4f as uint32,
        last: 0xd4f as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd54 as uint32,
        last: 0xd56 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd57 as uint32,
        last: 0xd57 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd58 as uint32,
        last: 0xd5e as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd5f as uint32,
        last: 0xd61 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd62 as uint32,
        last: 0xd63 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd66 as uint32,
        last: 0xd6f as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd70 as uint32,
        last: 0xd78 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd79 as uint32,
        last: 0xd79 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd7a as uint32,
        last: 0xd7f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd81 as uint32,
        last: 0xd81 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd82 as uint32,
        last: 0xd83 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd85 as uint32,
        last: 0xd96 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd9a as uint32,
        last: 0xdb1 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xdb3 as uint32,
        last: 0xdbb as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xdbd as uint32,
        last: 0xdbd as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xdc0 as uint32,
        last: 0xdc6 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xdca as uint32,
        last: 0xdca as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xdcf as uint32,
        last: 0xdd1 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xdd2 as uint32,
        last: 0xdd4 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xdd6 as uint32,
        last: 0xdd6 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xdd8 as uint32,
        last: 0xddf as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xde6 as uint32,
        last: 0xdef as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xdf2 as uint32,
        last: 0xdf3 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xdf4 as uint32,
        last: 0xdf4 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xe01 as uint32,
        last: 0xe30 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xe31 as uint32,
        last: 0xe31 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xe32 as uint32,
        last: 0xe33 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xe34 as uint32,
        last: 0xe3a as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xe3f as uint32,
        last: 0xe3f as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xe40 as uint32,
        last: 0xe45 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xe46 as uint32,
        last: 0xe46 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xe47 as uint32,
        last: 0xe4e as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xe4f as uint32,
        last: 0xe4f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xe50 as uint32,
        last: 0xe59 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xe5a as uint32,
        last: 0xe5b as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xe81 as uint32,
        last: 0xe82 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xe84 as uint32,
        last: 0xe84 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xe86 as uint32,
        last: 0xe8a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xe8c as uint32,
        last: 0xea3 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xea5 as uint32,
        last: 0xea5 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xea7 as uint32,
        last: 0xeb0 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xeb1 as uint32,
        last: 0xeb1 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xeb2 as uint32,
        last: 0xeb3 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xeb4 as uint32,
        last: 0xebc as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xebd as uint32,
        last: 0xebd as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xec0 as uint32,
        last: 0xec4 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xec6 as uint32,
        last: 0xec6 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xec8 as uint32,
        last: 0xece as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xed0 as uint32,
        last: 0xed9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xedc as uint32,
        last: 0xedf as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf00 as uint32,
        last: 0xf00 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf01 as uint32,
        last: 0xf03 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf04 as uint32,
        last: 0xf12 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf13 as uint32,
        last: 0xf13 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf14 as uint32,
        last: 0xf14 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf15 as uint32,
        last: 0xf17 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf18 as uint32,
        last: 0xf19 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf1a as uint32,
        last: 0xf1f as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf20 as uint32,
        last: 0xf29 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf2a as uint32,
        last: 0xf33 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf34 as uint32,
        last: 0xf34 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf35 as uint32,
        last: 0xf35 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf36 as uint32,
        last: 0xf36 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf37 as uint32,
        last: 0xf37 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf38 as uint32,
        last: 0xf38 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf39 as uint32,
        last: 0xf39 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf3a as uint32,
        last: 0xf3a as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf3b as uint32,
        last: 0xf3b as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf3c as uint32,
        last: 0xf3c as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf3d as uint32,
        last: 0xf3d as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf3e as uint32,
        last: 0xf3f as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf40 as uint32,
        last: 0xf47 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf49 as uint32,
        last: 0xf6c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf71 as uint32,
        last: 0xf7e as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf7f as uint32,
        last: 0xf7f as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf80 as uint32,
        last: 0xf84 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf85 as uint32,
        last: 0xf85 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf86 as uint32,
        last: 0xf87 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf88 as uint32,
        last: 0xf8c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf8d as uint32,
        last: 0xf97 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf99 as uint32,
        last: 0xfbc as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfbe as uint32,
        last: 0xfc5 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfc6 as uint32,
        last: 0xfc6 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfc7 as uint32,
        last: 0xfcc as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfce as uint32,
        last: 0xfcf as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfd0 as uint32,
        last: 0xfd4 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfd5 as uint32,
        last: 0xfd8 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfd9 as uint32,
        last: 0xfda as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1000 as uint32,
        last: 0x102a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x102b as uint32,
        last: 0x102c as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x102d as uint32,
        last: 0x1030 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1031 as uint32,
        last: 0x1031 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1032 as uint32,
        last: 0x1037 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1038 as uint32,
        last: 0x1038 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1039 as uint32,
        last: 0x103a as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x103b as uint32,
        last: 0x103c as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x103d as uint32,
        last: 0x103e as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x103f as uint32,
        last: 0x103f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1040 as uint32,
        last: 0x1049 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x104a as uint32,
        last: 0x104f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1050 as uint32,
        last: 0x1055 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1056 as uint32,
        last: 0x1057 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1058 as uint32,
        last: 0x1059 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x105a as uint32,
        last: 0x105d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x105e as uint32,
        last: 0x1060 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1061 as uint32,
        last: 0x1061 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1062 as uint32,
        last: 0x1064 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1065 as uint32,
        last: 0x1066 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1067 as uint32,
        last: 0x106d as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x106e as uint32,
        last: 0x1070 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1071 as uint32,
        last: 0x1074 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1075 as uint32,
        last: 0x1081 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1082 as uint32,
        last: 0x1082 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1083 as uint32,
        last: 0x1084 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1085 as uint32,
        last: 0x1086 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1087 as uint32,
        last: 0x108c as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x108d as uint32,
        last: 0x108d as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x108e as uint32,
        last: 0x108e as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x108f as uint32,
        last: 0x108f as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1090 as uint32,
        last: 0x1099 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x109a as uint32,
        last: 0x109c as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x109d as uint32,
        last: 0x109d as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x109e as uint32,
        last: 0x109f as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10a0 as uint32,
        last: 0x10c5 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10c7 as uint32,
        last: 0x10c7 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10cd as uint32,
        last: 0x10cd as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10d0 as uint32,
        last: 0x10fa as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10fb as uint32,
        last: 0x10fb as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10fc as uint32,
        last: 0x10fc as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10fd as uint32,
        last: 0x10ff as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1100 as uint32,
        last: 0x1248 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x124a as uint32,
        last: 0x124d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1250 as uint32,
        last: 0x1256 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1258 as uint32,
        last: 0x1258 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x125a as uint32,
        last: 0x125d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1260 as uint32,
        last: 0x1288 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x128a as uint32,
        last: 0x128d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1290 as uint32,
        last: 0x12b0 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x12b2 as uint32,
        last: 0x12b5 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x12b8 as uint32,
        last: 0x12be as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x12c0 as uint32,
        last: 0x12c0 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x12c2 as uint32,
        last: 0x12c5 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x12c8 as uint32,
        last: 0x12d6 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x12d8 as uint32,
        last: 0x1310 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1312 as uint32,
        last: 0x1315 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1318 as uint32,
        last: 0x135a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x135d as uint32,
        last: 0x135f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1360 as uint32,
        last: 0x1368 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1369 as uint32,
        last: 0x137c as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1380 as uint32,
        last: 0x138f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1390 as uint32,
        last: 0x1399 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x13a0 as uint32,
        last: 0x13f5 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x13f8 as uint32,
        last: 0x13fd as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1400 as uint32,
        last: 0x1400 as uint32,
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1401 as uint32,
        last: 0x166c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x166d as uint32,
        last: 0x166d as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x166e as uint32,
        last: 0x166e as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x166f as uint32,
        last: 0x167f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1680 as uint32,
        last: 0x1680 as uint32,
        category: PG_U_SPACE_SEPARATOR as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1681 as uint32,
        last: 0x169a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x169b as uint32,
        last: 0x169b as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x169c as uint32,
        last: 0x169c as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16a0 as uint32,
        last: 0x16ea as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16eb as uint32,
        last: 0x16ed as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16ee as uint32,
        last: 0x16f0 as uint32,
        category: PG_U_LETTER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16f1 as uint32,
        last: 0x16f8 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1700 as uint32,
        last: 0x1711 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1712 as uint32,
        last: 0x1714 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1715 as uint32,
        last: 0x1715 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x171f as uint32,
        last: 0x1731 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1732 as uint32,
        last: 0x1733 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1734 as uint32,
        last: 0x1734 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1735 as uint32,
        last: 0x1736 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1740 as uint32,
        last: 0x1751 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1752 as uint32,
        last: 0x1753 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1760 as uint32,
        last: 0x176c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x176e as uint32,
        last: 0x1770 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1772 as uint32,
        last: 0x1773 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1780 as uint32,
        last: 0x17b3 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17b4 as uint32,
        last: 0x17b5 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17b6 as uint32,
        last: 0x17b6 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17b7 as uint32,
        last: 0x17bd as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17be as uint32,
        last: 0x17c5 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17c6 as uint32,
        last: 0x17c6 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17c7 as uint32,
        last: 0x17c8 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17c9 as uint32,
        last: 0x17d3 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17d4 as uint32,
        last: 0x17d6 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17d7 as uint32,
        last: 0x17d7 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17d8 as uint32,
        last: 0x17da as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17db as uint32,
        last: 0x17db as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17dc as uint32,
        last: 0x17dc as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17dd as uint32,
        last: 0x17dd as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17e0 as uint32,
        last: 0x17e9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17f0 as uint32,
        last: 0x17f9 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1800 as uint32,
        last: 0x1805 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1806 as uint32,
        last: 0x1806 as uint32,
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1807 as uint32,
        last: 0x180a as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x180b as uint32,
        last: 0x180d as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x180e as uint32,
        last: 0x180e as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x180f as uint32,
        last: 0x180f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1810 as uint32,
        last: 0x1819 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1820 as uint32,
        last: 0x1842 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1843 as uint32,
        last: 0x1843 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1844 as uint32,
        last: 0x1878 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1880 as uint32,
        last: 0x1884 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1885 as uint32,
        last: 0x1886 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1887 as uint32,
        last: 0x18a8 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x18a9 as uint32,
        last: 0x18a9 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x18aa as uint32,
        last: 0x18aa as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x18b0 as uint32,
        last: 0x18f5 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1900 as uint32,
        last: 0x191e as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1920 as uint32,
        last: 0x1922 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1923 as uint32,
        last: 0x1926 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1927 as uint32,
        last: 0x1928 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1929 as uint32,
        last: 0x192b as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1930 as uint32,
        last: 0x1931 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1932 as uint32,
        last: 0x1932 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1933 as uint32,
        last: 0x1938 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1939 as uint32,
        last: 0x193b as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1940 as uint32,
        last: 0x1940 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1944 as uint32,
        last: 0x1945 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1946 as uint32,
        last: 0x194f as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1950 as uint32,
        last: 0x196d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1970 as uint32,
        last: 0x1974 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1980 as uint32,
        last: 0x19ab as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x19b0 as uint32,
        last: 0x19c9 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x19d0 as uint32,
        last: 0x19d9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x19da as uint32,
        last: 0x19da as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x19de as uint32,
        last: 0x19ff as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a00 as uint32,
        last: 0x1a16 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a17 as uint32,
        last: 0x1a18 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a19 as uint32,
        last: 0x1a1a as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a1b as uint32,
        last: 0x1a1b as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a1e as uint32,
        last: 0x1a1f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a20 as uint32,
        last: 0x1a54 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a55 as uint32,
        last: 0x1a55 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a56 as uint32,
        last: 0x1a56 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a57 as uint32,
        last: 0x1a57 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a58 as uint32,
        last: 0x1a5e as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a60 as uint32,
        last: 0x1a60 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a61 as uint32,
        last: 0x1a61 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a62 as uint32,
        last: 0x1a62 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a63 as uint32,
        last: 0x1a64 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a65 as uint32,
        last: 0x1a6c as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a6d as uint32,
        last: 0x1a72 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a73 as uint32,
        last: 0x1a7c as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a7f as uint32,
        last: 0x1a7f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a80 as uint32,
        last: 0x1a89 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1a90 as uint32,
        last: 0x1a99 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1aa0 as uint32,
        last: 0x1aa6 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1aa7 as uint32,
        last: 0x1aa7 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1aa8 as uint32,
        last: 0x1aad as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ab0 as uint32,
        last: 0x1abd as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1abe as uint32,
        last: 0x1abe as uint32,
        category: PG_U_ENCLOSING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1abf as uint32,
        last: 0x1ace as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b00 as uint32,
        last: 0x1b03 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b04 as uint32,
        last: 0x1b04 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b05 as uint32,
        last: 0x1b33 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b34 as uint32,
        last: 0x1b34 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b35 as uint32,
        last: 0x1b35 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b36 as uint32,
        last: 0x1b3a as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b3b as uint32,
        last: 0x1b3b as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b3c as uint32,
        last: 0x1b3c as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b3d as uint32,
        last: 0x1b41 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b42 as uint32,
        last: 0x1b42 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b43 as uint32,
        last: 0x1b44 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b45 as uint32,
        last: 0x1b4c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b4e as uint32,
        last: 0x1b4f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b50 as uint32,
        last: 0x1b59 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b5a as uint32,
        last: 0x1b60 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b61 as uint32,
        last: 0x1b6a as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b6b as uint32,
        last: 0x1b73 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b74 as uint32,
        last: 0x1b7c as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b7d as uint32,
        last: 0x1b7f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b80 as uint32,
        last: 0x1b81 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b82 as uint32,
        last: 0x1b82 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b83 as uint32,
        last: 0x1ba0 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ba1 as uint32,
        last: 0x1ba1 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ba2 as uint32,
        last: 0x1ba5 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ba6 as uint32,
        last: 0x1ba7 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ba8 as uint32,
        last: 0x1ba9 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1baa as uint32,
        last: 0x1baa as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bab as uint32,
        last: 0x1bad as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bae as uint32,
        last: 0x1baf as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bb0 as uint32,
        last: 0x1bb9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bba as uint32,
        last: 0x1be5 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1be6 as uint32,
        last: 0x1be6 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1be7 as uint32,
        last: 0x1be7 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1be8 as uint32,
        last: 0x1be9 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bea as uint32,
        last: 0x1bec as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bed as uint32,
        last: 0x1bed as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bee as uint32,
        last: 0x1bee as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bef as uint32,
        last: 0x1bf1 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bf2 as uint32,
        last: 0x1bf3 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bfc as uint32,
        last: 0x1bff as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c00 as uint32,
        last: 0x1c23 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c24 as uint32,
        last: 0x1c2b as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c2c as uint32,
        last: 0x1c33 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c34 as uint32,
        last: 0x1c35 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c36 as uint32,
        last: 0x1c37 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c3b as uint32,
        last: 0x1c3f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c40 as uint32,
        last: 0x1c49 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c4d as uint32,
        last: 0x1c4f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c50 as uint32,
        last: 0x1c59 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c5a as uint32,
        last: 0x1c77 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c78 as uint32,
        last: 0x1c7d as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c7e as uint32,
        last: 0x1c7f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c80 as uint32,
        last: 0x1c88 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c89 as uint32,
        last: 0x1c89 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c8a as uint32,
        last: 0x1c8a as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1c90 as uint32,
        last: 0x1cba as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1cbd as uint32,
        last: 0x1cbf as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1cc0 as uint32,
        last: 0x1cc7 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1cd0 as uint32,
        last: 0x1cd2 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1cd3 as uint32,
        last: 0x1cd3 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1cd4 as uint32,
        last: 0x1ce0 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ce1 as uint32,
        last: 0x1ce1 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ce2 as uint32,
        last: 0x1ce8 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ce9 as uint32,
        last: 0x1cec as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ced as uint32,
        last: 0x1ced as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1cee as uint32,
        last: 0x1cf3 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1cf4 as uint32,
        last: 0x1cf4 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1cf5 as uint32,
        last: 0x1cf6 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1cf7 as uint32,
        last: 0x1cf7 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1cf8 as uint32,
        last: 0x1cf9 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1cfa as uint32,
        last: 0x1cfa as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d00 as uint32,
        last: 0x1d2b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d2c as uint32,
        last: 0x1d6a as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d6b as uint32,
        last: 0x1d77 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d78 as uint32,
        last: 0x1d78 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d79 as uint32,
        last: 0x1d9a as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d9b as uint32,
        last: 0x1dbf as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1dc0 as uint32,
        last: 0x1dff as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e00 as uint32,
        last: 0x1e00 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e01 as uint32,
        last: 0x1e01 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e02 as uint32,
        last: 0x1e02 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e03 as uint32,
        last: 0x1e03 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e04 as uint32,
        last: 0x1e04 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e05 as uint32,
        last: 0x1e05 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e06 as uint32,
        last: 0x1e06 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e07 as uint32,
        last: 0x1e07 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e08 as uint32,
        last: 0x1e08 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e09 as uint32,
        last: 0x1e09 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e0a as uint32,
        last: 0x1e0a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e0b as uint32,
        last: 0x1e0b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e0c as uint32,
        last: 0x1e0c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e0d as uint32,
        last: 0x1e0d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e0e as uint32,
        last: 0x1e0e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e0f as uint32,
        last: 0x1e0f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e10 as uint32,
        last: 0x1e10 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e11 as uint32,
        last: 0x1e11 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e12 as uint32,
        last: 0x1e12 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e13 as uint32,
        last: 0x1e13 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e14 as uint32,
        last: 0x1e14 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e15 as uint32,
        last: 0x1e15 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e16 as uint32,
        last: 0x1e16 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e17 as uint32,
        last: 0x1e17 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e18 as uint32,
        last: 0x1e18 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e19 as uint32,
        last: 0x1e19 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e1a as uint32,
        last: 0x1e1a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e1b as uint32,
        last: 0x1e1b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e1c as uint32,
        last: 0x1e1c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e1d as uint32,
        last: 0x1e1d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e1e as uint32,
        last: 0x1e1e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e1f as uint32,
        last: 0x1e1f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e20 as uint32,
        last: 0x1e20 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e21 as uint32,
        last: 0x1e21 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e22 as uint32,
        last: 0x1e22 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e23 as uint32,
        last: 0x1e23 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e24 as uint32,
        last: 0x1e24 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e25 as uint32,
        last: 0x1e25 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e26 as uint32,
        last: 0x1e26 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e27 as uint32,
        last: 0x1e27 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e28 as uint32,
        last: 0x1e28 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e29 as uint32,
        last: 0x1e29 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e2a as uint32,
        last: 0x1e2a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e2b as uint32,
        last: 0x1e2b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e2c as uint32,
        last: 0x1e2c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e2d as uint32,
        last: 0x1e2d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e2e as uint32,
        last: 0x1e2e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e2f as uint32,
        last: 0x1e2f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e30 as uint32,
        last: 0x1e30 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e31 as uint32,
        last: 0x1e31 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e32 as uint32,
        last: 0x1e32 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e33 as uint32,
        last: 0x1e33 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e34 as uint32,
        last: 0x1e34 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e35 as uint32,
        last: 0x1e35 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e36 as uint32,
        last: 0x1e36 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e37 as uint32,
        last: 0x1e37 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e38 as uint32,
        last: 0x1e38 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e39 as uint32,
        last: 0x1e39 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e3a as uint32,
        last: 0x1e3a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e3b as uint32,
        last: 0x1e3b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e3c as uint32,
        last: 0x1e3c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e3d as uint32,
        last: 0x1e3d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e3e as uint32,
        last: 0x1e3e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e3f as uint32,
        last: 0x1e3f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e40 as uint32,
        last: 0x1e40 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e41 as uint32,
        last: 0x1e41 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e42 as uint32,
        last: 0x1e42 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e43 as uint32,
        last: 0x1e43 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e44 as uint32,
        last: 0x1e44 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e45 as uint32,
        last: 0x1e45 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e46 as uint32,
        last: 0x1e46 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e47 as uint32,
        last: 0x1e47 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e48 as uint32,
        last: 0x1e48 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e49 as uint32,
        last: 0x1e49 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e4a as uint32,
        last: 0x1e4a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e4b as uint32,
        last: 0x1e4b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e4c as uint32,
        last: 0x1e4c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e4d as uint32,
        last: 0x1e4d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e4e as uint32,
        last: 0x1e4e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e4f as uint32,
        last: 0x1e4f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e50 as uint32,
        last: 0x1e50 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e51 as uint32,
        last: 0x1e51 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e52 as uint32,
        last: 0x1e52 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e53 as uint32,
        last: 0x1e53 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e54 as uint32,
        last: 0x1e54 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e55 as uint32,
        last: 0x1e55 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e56 as uint32,
        last: 0x1e56 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e57 as uint32,
        last: 0x1e57 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e58 as uint32,
        last: 0x1e58 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e59 as uint32,
        last: 0x1e59 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e5a as uint32,
        last: 0x1e5a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e5b as uint32,
        last: 0x1e5b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e5c as uint32,
        last: 0x1e5c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e5d as uint32,
        last: 0x1e5d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e5e as uint32,
        last: 0x1e5e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e5f as uint32,
        last: 0x1e5f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e60 as uint32,
        last: 0x1e60 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e61 as uint32,
        last: 0x1e61 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e62 as uint32,
        last: 0x1e62 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e63 as uint32,
        last: 0x1e63 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e64 as uint32,
        last: 0x1e64 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e65 as uint32,
        last: 0x1e65 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e66 as uint32,
        last: 0x1e66 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e67 as uint32,
        last: 0x1e67 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e68 as uint32,
        last: 0x1e68 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e69 as uint32,
        last: 0x1e69 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e6a as uint32,
        last: 0x1e6a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e6b as uint32,
        last: 0x1e6b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e6c as uint32,
        last: 0x1e6c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e6d as uint32,
        last: 0x1e6d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e6e as uint32,
        last: 0x1e6e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e6f as uint32,
        last: 0x1e6f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e70 as uint32,
        last: 0x1e70 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e71 as uint32,
        last: 0x1e71 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e72 as uint32,
        last: 0x1e72 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e73 as uint32,
        last: 0x1e73 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e74 as uint32,
        last: 0x1e74 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e75 as uint32,
        last: 0x1e75 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e76 as uint32,
        last: 0x1e76 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e77 as uint32,
        last: 0x1e77 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e78 as uint32,
        last: 0x1e78 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e79 as uint32,
        last: 0x1e79 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e7a as uint32,
        last: 0x1e7a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e7b as uint32,
        last: 0x1e7b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e7c as uint32,
        last: 0x1e7c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e7d as uint32,
        last: 0x1e7d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e7e as uint32,
        last: 0x1e7e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e7f as uint32,
        last: 0x1e7f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e80 as uint32,
        last: 0x1e80 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e81 as uint32,
        last: 0x1e81 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e82 as uint32,
        last: 0x1e82 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e83 as uint32,
        last: 0x1e83 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e84 as uint32,
        last: 0x1e84 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e85 as uint32,
        last: 0x1e85 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e86 as uint32,
        last: 0x1e86 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e87 as uint32,
        last: 0x1e87 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e88 as uint32,
        last: 0x1e88 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e89 as uint32,
        last: 0x1e89 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e8a as uint32,
        last: 0x1e8a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e8b as uint32,
        last: 0x1e8b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e8c as uint32,
        last: 0x1e8c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e8d as uint32,
        last: 0x1e8d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e8e as uint32,
        last: 0x1e8e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e8f as uint32,
        last: 0x1e8f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e90 as uint32,
        last: 0x1e90 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e91 as uint32,
        last: 0x1e91 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e92 as uint32,
        last: 0x1e92 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e93 as uint32,
        last: 0x1e93 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e94 as uint32,
        last: 0x1e94 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e95 as uint32,
        last: 0x1e9d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e9e as uint32,
        last: 0x1e9e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e9f as uint32,
        last: 0x1e9f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ea0 as uint32,
        last: 0x1ea0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ea1 as uint32,
        last: 0x1ea1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ea2 as uint32,
        last: 0x1ea2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ea3 as uint32,
        last: 0x1ea3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ea4 as uint32,
        last: 0x1ea4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ea5 as uint32,
        last: 0x1ea5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ea6 as uint32,
        last: 0x1ea6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ea7 as uint32,
        last: 0x1ea7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ea8 as uint32,
        last: 0x1ea8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ea9 as uint32,
        last: 0x1ea9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eaa as uint32,
        last: 0x1eaa as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eab as uint32,
        last: 0x1eab as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eac as uint32,
        last: 0x1eac as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ead as uint32,
        last: 0x1ead as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eae as uint32,
        last: 0x1eae as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eaf as uint32,
        last: 0x1eaf as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eb0 as uint32,
        last: 0x1eb0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eb1 as uint32,
        last: 0x1eb1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eb2 as uint32,
        last: 0x1eb2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eb3 as uint32,
        last: 0x1eb3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eb4 as uint32,
        last: 0x1eb4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eb5 as uint32,
        last: 0x1eb5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eb6 as uint32,
        last: 0x1eb6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eb7 as uint32,
        last: 0x1eb7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eb8 as uint32,
        last: 0x1eb8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eb9 as uint32,
        last: 0x1eb9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eba as uint32,
        last: 0x1eba as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ebb as uint32,
        last: 0x1ebb as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ebc as uint32,
        last: 0x1ebc as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ebd as uint32,
        last: 0x1ebd as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ebe as uint32,
        last: 0x1ebe as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ebf as uint32,
        last: 0x1ebf as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ec0 as uint32,
        last: 0x1ec0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ec1 as uint32,
        last: 0x1ec1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ec2 as uint32,
        last: 0x1ec2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ec3 as uint32,
        last: 0x1ec3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ec4 as uint32,
        last: 0x1ec4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ec5 as uint32,
        last: 0x1ec5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ec6 as uint32,
        last: 0x1ec6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ec7 as uint32,
        last: 0x1ec7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ec8 as uint32,
        last: 0x1ec8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ec9 as uint32,
        last: 0x1ec9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eca as uint32,
        last: 0x1eca as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ecb as uint32,
        last: 0x1ecb as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ecc as uint32,
        last: 0x1ecc as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ecd as uint32,
        last: 0x1ecd as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ece as uint32,
        last: 0x1ece as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ecf as uint32,
        last: 0x1ecf as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ed0 as uint32,
        last: 0x1ed0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ed1 as uint32,
        last: 0x1ed1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ed2 as uint32,
        last: 0x1ed2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ed3 as uint32,
        last: 0x1ed3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ed4 as uint32,
        last: 0x1ed4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ed5 as uint32,
        last: 0x1ed5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ed6 as uint32,
        last: 0x1ed6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ed7 as uint32,
        last: 0x1ed7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ed8 as uint32,
        last: 0x1ed8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ed9 as uint32,
        last: 0x1ed9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eda as uint32,
        last: 0x1eda as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1edb as uint32,
        last: 0x1edb as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1edc as uint32,
        last: 0x1edc as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1edd as uint32,
        last: 0x1edd as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ede as uint32,
        last: 0x1ede as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1edf as uint32,
        last: 0x1edf as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee0 as uint32,
        last: 0x1ee0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee1 as uint32,
        last: 0x1ee1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee2 as uint32,
        last: 0x1ee2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee3 as uint32,
        last: 0x1ee3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee4 as uint32,
        last: 0x1ee4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee5 as uint32,
        last: 0x1ee5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee6 as uint32,
        last: 0x1ee6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee7 as uint32,
        last: 0x1ee7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee8 as uint32,
        last: 0x1ee8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee9 as uint32,
        last: 0x1ee9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eea as uint32,
        last: 0x1eea as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eeb as uint32,
        last: 0x1eeb as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eec as uint32,
        last: 0x1eec as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eed as uint32,
        last: 0x1eed as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eee as uint32,
        last: 0x1eee as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eef as uint32,
        last: 0x1eef as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ef0 as uint32,
        last: 0x1ef0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ef1 as uint32,
        last: 0x1ef1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ef2 as uint32,
        last: 0x1ef2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ef3 as uint32,
        last: 0x1ef3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ef4 as uint32,
        last: 0x1ef4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ef5 as uint32,
        last: 0x1ef5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ef6 as uint32,
        last: 0x1ef6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ef7 as uint32,
        last: 0x1ef7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ef8 as uint32,
        last: 0x1ef8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ef9 as uint32,
        last: 0x1ef9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1efa as uint32,
        last: 0x1efa as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1efb as uint32,
        last: 0x1efb as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1efc as uint32,
        last: 0x1efc as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1efd as uint32,
        last: 0x1efd as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1efe as uint32,
        last: 0x1efe as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eff as uint32,
        last: 0x1f07 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f08 as uint32,
        last: 0x1f0f as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f10 as uint32,
        last: 0x1f15 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f18 as uint32,
        last: 0x1f1d as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f20 as uint32,
        last: 0x1f27 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f28 as uint32,
        last: 0x1f2f as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f30 as uint32,
        last: 0x1f37 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f38 as uint32,
        last: 0x1f3f as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f40 as uint32,
        last: 0x1f45 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f48 as uint32,
        last: 0x1f4d as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f50 as uint32,
        last: 0x1f57 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f59 as uint32,
        last: 0x1f59 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f5b as uint32,
        last: 0x1f5b as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f5d as uint32,
        last: 0x1f5d as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f5f as uint32,
        last: 0x1f5f as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f60 as uint32,
        last: 0x1f67 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f68 as uint32,
        last: 0x1f6f as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f70 as uint32,
        last: 0x1f7d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f80 as uint32,
        last: 0x1f87 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f88 as uint32,
        last: 0x1f8f as uint32,
        category: PG_U_TITLECASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f90 as uint32,
        last: 0x1f97 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f98 as uint32,
        last: 0x1f9f as uint32,
        category: PG_U_TITLECASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fa0 as uint32,
        last: 0x1fa7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fa8 as uint32,
        last: 0x1faf as uint32,
        category: PG_U_TITLECASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fb0 as uint32,
        last: 0x1fb4 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fb6 as uint32,
        last: 0x1fb7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fb8 as uint32,
        last: 0x1fbb as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fbc as uint32,
        last: 0x1fbc as uint32,
        category: PG_U_TITLECASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fbd as uint32,
        last: 0x1fbd as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fbe as uint32,
        last: 0x1fbe as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fbf as uint32,
        last: 0x1fc1 as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fc2 as uint32,
        last: 0x1fc4 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fc6 as uint32,
        last: 0x1fc7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fc8 as uint32,
        last: 0x1fcb as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fcc as uint32,
        last: 0x1fcc as uint32,
        category: PG_U_TITLECASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fcd as uint32,
        last: 0x1fcf as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fd0 as uint32,
        last: 0x1fd3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fd6 as uint32,
        last: 0x1fd7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fd8 as uint32,
        last: 0x1fdb as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fdd as uint32,
        last: 0x1fdf as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fe0 as uint32,
        last: 0x1fe7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fe8 as uint32,
        last: 0x1fec as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fed as uint32,
        last: 0x1fef as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ff2 as uint32,
        last: 0x1ff4 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ff6 as uint32,
        last: 0x1ff7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ff8 as uint32,
        last: 0x1ffb as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ffc as uint32,
        last: 0x1ffc as uint32,
        category: PG_U_TITLECASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ffd as uint32,
        last: 0x1ffe as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2000 as uint32,
        last: 0x200a as uint32,
        category: PG_U_SPACE_SEPARATOR as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x200b as uint32,
        last: 0x200f as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2010 as uint32,
        last: 0x2015 as uint32,
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2016 as uint32,
        last: 0x2017 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2018 as uint32,
        last: 0x2018 as uint32,
        category: PG_U_INITIAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2019 as uint32,
        last: 0x2019 as uint32,
        category: PG_U_FINAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x201a as uint32,
        last: 0x201a as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x201b as uint32,
        last: 0x201c as uint32,
        category: PG_U_INITIAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x201d as uint32,
        last: 0x201d as uint32,
        category: PG_U_FINAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x201e as uint32,
        last: 0x201e as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x201f as uint32,
        last: 0x201f as uint32,
        category: PG_U_INITIAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2020 as uint32,
        last: 0x2027 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2028 as uint32,
        last: 0x2028 as uint32,
        category: PG_U_LINE_SEPARATOR as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2029 as uint32,
        last: 0x2029 as uint32,
        category: PG_U_PARAGRAPH_SEPARATOR as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x202a as uint32,
        last: 0x202e as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x202f as uint32,
        last: 0x202f as uint32,
        category: PG_U_SPACE_SEPARATOR as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2030 as uint32,
        last: 0x2038 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2039 as uint32,
        last: 0x2039 as uint32,
        category: PG_U_INITIAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x203a as uint32,
        last: 0x203a as uint32,
        category: PG_U_FINAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x203b as uint32,
        last: 0x203e as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x203f as uint32,
        last: 0x2040 as uint32,
        category: PG_U_CONNECTOR_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2041 as uint32,
        last: 0x2043 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2044 as uint32,
        last: 0x2044 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2045 as uint32,
        last: 0x2045 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2046 as uint32,
        last: 0x2046 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2047 as uint32,
        last: 0x2051 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2052 as uint32,
        last: 0x2052 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2053 as uint32,
        last: 0x2053 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2054 as uint32,
        last: 0x2054 as uint32,
        category: PG_U_CONNECTOR_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2055 as uint32,
        last: 0x205e as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x205f as uint32,
        last: 0x205f as uint32,
        category: PG_U_SPACE_SEPARATOR as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2060 as uint32,
        last: 0x2064 as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2066 as uint32,
        last: 0x206f as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2070 as uint32,
        last: 0x2070 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2071 as uint32,
        last: 0x2071 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2074 as uint32,
        last: 0x2079 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x207a as uint32,
        last: 0x207c as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x207d as uint32,
        last: 0x207d as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x207e as uint32,
        last: 0x207e as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x207f as uint32,
        last: 0x207f as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2080 as uint32,
        last: 0x2089 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x208a as uint32,
        last: 0x208c as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x208d as uint32,
        last: 0x208d as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x208e as uint32,
        last: 0x208e as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2090 as uint32,
        last: 0x209c as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x20a0 as uint32,
        last: 0x20c0 as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x20d0 as uint32,
        last: 0x20dc as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x20dd as uint32,
        last: 0x20e0 as uint32,
        category: PG_U_ENCLOSING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x20e1 as uint32,
        last: 0x20e1 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x20e2 as uint32,
        last: 0x20e4 as uint32,
        category: PG_U_ENCLOSING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x20e5 as uint32,
        last: 0x20f0 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2100 as uint32,
        last: 0x2101 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2102 as uint32,
        last: 0x2102 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2103 as uint32,
        last: 0x2106 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2107 as uint32,
        last: 0x2107 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2108 as uint32,
        last: 0x2109 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x210a as uint32,
        last: 0x210a as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x210b as uint32,
        last: 0x210d as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x210e as uint32,
        last: 0x210f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2110 as uint32,
        last: 0x2112 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2113 as uint32,
        last: 0x2113 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2114 as uint32,
        last: 0x2114 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2115 as uint32,
        last: 0x2115 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2116 as uint32,
        last: 0x2117 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2118 as uint32,
        last: 0x2118 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2119 as uint32,
        last: 0x211d as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x211e as uint32,
        last: 0x2123 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2124 as uint32,
        last: 0x2124 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2125 as uint32,
        last: 0x2125 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2126 as uint32,
        last: 0x2126 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2127 as uint32,
        last: 0x2127 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2128 as uint32,
        last: 0x2128 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2129 as uint32,
        last: 0x2129 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x212a as uint32,
        last: 0x212d as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x212e as uint32,
        last: 0x212e as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x212f as uint32,
        last: 0x212f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2130 as uint32,
        last: 0x2133 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2134 as uint32,
        last: 0x2134 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2135 as uint32,
        last: 0x2138 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2139 as uint32,
        last: 0x2139 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x213a as uint32,
        last: 0x213b as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x213c as uint32,
        last: 0x213d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x213e as uint32,
        last: 0x213f as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2140 as uint32,
        last: 0x2144 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2145 as uint32,
        last: 0x2145 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2146 as uint32,
        last: 0x2149 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x214a as uint32,
        last: 0x214a as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x214b as uint32,
        last: 0x214b as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x214c as uint32,
        last: 0x214d as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x214e as uint32,
        last: 0x214e as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x214f as uint32,
        last: 0x214f as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2150 as uint32,
        last: 0x215f as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2160 as uint32,
        last: 0x2182 as uint32,
        category: PG_U_LETTER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2183 as uint32,
        last: 0x2183 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2184 as uint32,
        last: 0x2184 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2185 as uint32,
        last: 0x2188 as uint32,
        category: PG_U_LETTER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2189 as uint32,
        last: 0x2189 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x218a as uint32,
        last: 0x218b as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2190 as uint32,
        last: 0x2194 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2195 as uint32,
        last: 0x2199 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x219a as uint32,
        last: 0x219b as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x219c as uint32,
        last: 0x219f as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21a0 as uint32,
        last: 0x21a0 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21a1 as uint32,
        last: 0x21a2 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21a3 as uint32,
        last: 0x21a3 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21a4 as uint32,
        last: 0x21a5 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21a6 as uint32,
        last: 0x21a6 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21a7 as uint32,
        last: 0x21ad as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21ae as uint32,
        last: 0x21ae as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21af as uint32,
        last: 0x21cd as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21ce as uint32,
        last: 0x21cf as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21d0 as uint32,
        last: 0x21d1 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21d2 as uint32,
        last: 0x21d2 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21d3 as uint32,
        last: 0x21d3 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21d4 as uint32,
        last: 0x21d4 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21d5 as uint32,
        last: 0x21f3 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x21f4 as uint32,
        last: 0x22ff as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2300 as uint32,
        last: 0x2307 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2308 as uint32,
        last: 0x2308 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2309 as uint32,
        last: 0x2309 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x230a as uint32,
        last: 0x230a as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x230b as uint32,
        last: 0x230b as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x230c as uint32,
        last: 0x231f as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2320 as uint32,
        last: 0x2321 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2322 as uint32,
        last: 0x2328 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2329 as uint32,
        last: 0x2329 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x232a as uint32,
        last: 0x232a as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x232b as uint32,
        last: 0x237b as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x237c as uint32,
        last: 0x237c as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x237d as uint32,
        last: 0x239a as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x239b as uint32,
        last: 0x23b3 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x23b4 as uint32,
        last: 0x23db as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x23dc as uint32,
        last: 0x23e1 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x23e2 as uint32,
        last: 0x2429 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2440 as uint32,
        last: 0x244a as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2460 as uint32,
        last: 0x249b as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x249c as uint32,
        last: 0x24e9 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x24ea as uint32,
        last: 0x24ff as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2500 as uint32,
        last: 0x25b6 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x25b7 as uint32,
        last: 0x25b7 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x25b8 as uint32,
        last: 0x25c0 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x25c1 as uint32,
        last: 0x25c1 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x25c2 as uint32,
        last: 0x25f7 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x25f8 as uint32,
        last: 0x25ff as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2600 as uint32,
        last: 0x266e as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x266f as uint32,
        last: 0x266f as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2670 as uint32,
        last: 0x2767 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2768 as uint32,
        last: 0x2768 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2769 as uint32,
        last: 0x2769 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x276a as uint32,
        last: 0x276a as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x276b as uint32,
        last: 0x276b as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x276c as uint32,
        last: 0x276c as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x276d as uint32,
        last: 0x276d as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x276e as uint32,
        last: 0x276e as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x276f as uint32,
        last: 0x276f as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2770 as uint32,
        last: 0x2770 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2771 as uint32,
        last: 0x2771 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2772 as uint32,
        last: 0x2772 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2773 as uint32,
        last: 0x2773 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2774 as uint32,
        last: 0x2774 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2775 as uint32,
        last: 0x2775 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2776 as uint32,
        last: 0x2793 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2794 as uint32,
        last: 0x27bf as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x27c0 as uint32,
        last: 0x27c4 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x27c5 as uint32,
        last: 0x27c5 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x27c6 as uint32,
        last: 0x27c6 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x27c7 as uint32,
        last: 0x27e5 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x27e6 as uint32,
        last: 0x27e6 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x27e7 as uint32,
        last: 0x27e7 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x27e8 as uint32,
        last: 0x27e8 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x27e9 as uint32,
        last: 0x27e9 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x27ea as uint32,
        last: 0x27ea as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x27eb as uint32,
        last: 0x27eb as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x27ec as uint32,
        last: 0x27ec as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x27ed as uint32,
        last: 0x27ed as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x27ee as uint32,
        last: 0x27ee as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x27ef as uint32,
        last: 0x27ef as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x27f0 as uint32,
        last: 0x27ff as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2800 as uint32,
        last: 0x28ff as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2900 as uint32,
        last: 0x2982 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2983 as uint32,
        last: 0x2983 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2984 as uint32,
        last: 0x2984 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2985 as uint32,
        last: 0x2985 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2986 as uint32,
        last: 0x2986 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2987 as uint32,
        last: 0x2987 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2988 as uint32,
        last: 0x2988 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2989 as uint32,
        last: 0x2989 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x298a as uint32,
        last: 0x298a as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x298b as uint32,
        last: 0x298b as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x298c as uint32,
        last: 0x298c as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x298d as uint32,
        last: 0x298d as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x298e as uint32,
        last: 0x298e as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x298f as uint32,
        last: 0x298f as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2990 as uint32,
        last: 0x2990 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2991 as uint32,
        last: 0x2991 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2992 as uint32,
        last: 0x2992 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2993 as uint32,
        last: 0x2993 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2994 as uint32,
        last: 0x2994 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2995 as uint32,
        last: 0x2995 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2996 as uint32,
        last: 0x2996 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2997 as uint32,
        last: 0x2997 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2998 as uint32,
        last: 0x2998 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2999 as uint32,
        last: 0x29d7 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x29d8 as uint32,
        last: 0x29d8 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x29d9 as uint32,
        last: 0x29d9 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x29da as uint32,
        last: 0x29da as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x29db as uint32,
        last: 0x29db as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x29dc as uint32,
        last: 0x29fb as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x29fc as uint32,
        last: 0x29fc as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x29fd as uint32,
        last: 0x29fd as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x29fe as uint32,
        last: 0x2aff as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2b00 as uint32,
        last: 0x2b2f as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2b30 as uint32,
        last: 0x2b44 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2b45 as uint32,
        last: 0x2b46 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2b47 as uint32,
        last: 0x2b4c as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2b4d as uint32,
        last: 0x2b73 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2b76 as uint32,
        last: 0x2b95 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2b97 as uint32,
        last: 0x2bff as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c00 as uint32,
        last: 0x2c2f as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c30 as uint32,
        last: 0x2c5f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c60 as uint32,
        last: 0x2c60 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c61 as uint32,
        last: 0x2c61 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c62 as uint32,
        last: 0x2c64 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c65 as uint32,
        last: 0x2c66 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c67 as uint32,
        last: 0x2c67 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c68 as uint32,
        last: 0x2c68 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c69 as uint32,
        last: 0x2c69 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c6a as uint32,
        last: 0x2c6a as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c6b as uint32,
        last: 0x2c6b as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c6c as uint32,
        last: 0x2c6c as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c6d as uint32,
        last: 0x2c70 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c71 as uint32,
        last: 0x2c71 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c72 as uint32,
        last: 0x2c72 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c73 as uint32,
        last: 0x2c74 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c75 as uint32,
        last: 0x2c75 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c76 as uint32,
        last: 0x2c7b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c7c as uint32,
        last: 0x2c7d as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c7e as uint32,
        last: 0x2c80 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c81 as uint32,
        last: 0x2c81 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c82 as uint32,
        last: 0x2c82 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c83 as uint32,
        last: 0x2c83 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c84 as uint32,
        last: 0x2c84 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c85 as uint32,
        last: 0x2c85 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c86 as uint32,
        last: 0x2c86 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c87 as uint32,
        last: 0x2c87 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c88 as uint32,
        last: 0x2c88 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c89 as uint32,
        last: 0x2c89 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c8a as uint32,
        last: 0x2c8a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c8b as uint32,
        last: 0x2c8b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c8c as uint32,
        last: 0x2c8c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c8d as uint32,
        last: 0x2c8d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c8e as uint32,
        last: 0x2c8e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c8f as uint32,
        last: 0x2c8f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c90 as uint32,
        last: 0x2c90 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c91 as uint32,
        last: 0x2c91 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c92 as uint32,
        last: 0x2c92 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c93 as uint32,
        last: 0x2c93 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c94 as uint32,
        last: 0x2c94 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c95 as uint32,
        last: 0x2c95 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c96 as uint32,
        last: 0x2c96 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c97 as uint32,
        last: 0x2c97 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c98 as uint32,
        last: 0x2c98 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c99 as uint32,
        last: 0x2c99 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c9a as uint32,
        last: 0x2c9a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c9b as uint32,
        last: 0x2c9b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c9c as uint32,
        last: 0x2c9c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c9d as uint32,
        last: 0x2c9d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c9e as uint32,
        last: 0x2c9e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2c9f as uint32,
        last: 0x2c9f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ca0 as uint32,
        last: 0x2ca0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ca1 as uint32,
        last: 0x2ca1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ca2 as uint32,
        last: 0x2ca2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ca3 as uint32,
        last: 0x2ca3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ca4 as uint32,
        last: 0x2ca4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ca5 as uint32,
        last: 0x2ca5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ca6 as uint32,
        last: 0x2ca6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ca7 as uint32,
        last: 0x2ca7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ca8 as uint32,
        last: 0x2ca8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ca9 as uint32,
        last: 0x2ca9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2caa as uint32,
        last: 0x2caa as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cab as uint32,
        last: 0x2cab as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cac as uint32,
        last: 0x2cac as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cad as uint32,
        last: 0x2cad as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cae as uint32,
        last: 0x2cae as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2caf as uint32,
        last: 0x2caf as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cb0 as uint32,
        last: 0x2cb0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cb1 as uint32,
        last: 0x2cb1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cb2 as uint32,
        last: 0x2cb2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cb3 as uint32,
        last: 0x2cb3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cb4 as uint32,
        last: 0x2cb4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cb5 as uint32,
        last: 0x2cb5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cb6 as uint32,
        last: 0x2cb6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cb7 as uint32,
        last: 0x2cb7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cb8 as uint32,
        last: 0x2cb8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cb9 as uint32,
        last: 0x2cb9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cba as uint32,
        last: 0x2cba as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cbb as uint32,
        last: 0x2cbb as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cbc as uint32,
        last: 0x2cbc as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cbd as uint32,
        last: 0x2cbd as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cbe as uint32,
        last: 0x2cbe as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cbf as uint32,
        last: 0x2cbf as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cc0 as uint32,
        last: 0x2cc0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cc1 as uint32,
        last: 0x2cc1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cc2 as uint32,
        last: 0x2cc2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cc3 as uint32,
        last: 0x2cc3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cc4 as uint32,
        last: 0x2cc4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cc5 as uint32,
        last: 0x2cc5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cc6 as uint32,
        last: 0x2cc6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cc7 as uint32,
        last: 0x2cc7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cc8 as uint32,
        last: 0x2cc8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cc9 as uint32,
        last: 0x2cc9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cca as uint32,
        last: 0x2cca as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ccb as uint32,
        last: 0x2ccb as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ccc as uint32,
        last: 0x2ccc as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ccd as uint32,
        last: 0x2ccd as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cce as uint32,
        last: 0x2cce as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ccf as uint32,
        last: 0x2ccf as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cd0 as uint32,
        last: 0x2cd0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cd1 as uint32,
        last: 0x2cd1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cd2 as uint32,
        last: 0x2cd2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cd3 as uint32,
        last: 0x2cd3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cd4 as uint32,
        last: 0x2cd4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cd5 as uint32,
        last: 0x2cd5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cd6 as uint32,
        last: 0x2cd6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cd7 as uint32,
        last: 0x2cd7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cd8 as uint32,
        last: 0x2cd8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cd9 as uint32,
        last: 0x2cd9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cda as uint32,
        last: 0x2cda as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cdb as uint32,
        last: 0x2cdb as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cdc as uint32,
        last: 0x2cdc as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cdd as uint32,
        last: 0x2cdd as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cde as uint32,
        last: 0x2cde as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cdf as uint32,
        last: 0x2cdf as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ce0 as uint32,
        last: 0x2ce0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ce1 as uint32,
        last: 0x2ce1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ce2 as uint32,
        last: 0x2ce2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ce3 as uint32,
        last: 0x2ce4 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ce5 as uint32,
        last: 0x2cea as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ceb as uint32,
        last: 0x2ceb as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cec as uint32,
        last: 0x2cec as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ced as uint32,
        last: 0x2ced as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cee as uint32,
        last: 0x2cee as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cef as uint32,
        last: 0x2cf1 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cf2 as uint32,
        last: 0x2cf2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cf3 as uint32,
        last: 0x2cf3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cf9 as uint32,
        last: 0x2cfc as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cfd as uint32,
        last: 0x2cfd as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2cfe as uint32,
        last: 0x2cff as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2d00 as uint32,
        last: 0x2d25 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2d27 as uint32,
        last: 0x2d27 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2d2d as uint32,
        last: 0x2d2d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2d30 as uint32,
        last: 0x2d67 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2d6f as uint32,
        last: 0x2d6f as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2d70 as uint32,
        last: 0x2d70 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2d7f as uint32,
        last: 0x2d7f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2d80 as uint32,
        last: 0x2d96 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2da0 as uint32,
        last: 0x2da6 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2da8 as uint32,
        last: 0x2dae as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2db0 as uint32,
        last: 0x2db6 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2db8 as uint32,
        last: 0x2dbe as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2dc0 as uint32,
        last: 0x2dc6 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2dc8 as uint32,
        last: 0x2dce as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2dd0 as uint32,
        last: 0x2dd6 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2dd8 as uint32,
        last: 0x2dde as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2de0 as uint32,
        last: 0x2dff as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e00 as uint32,
        last: 0x2e01 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e02 as uint32,
        last: 0x2e02 as uint32,
        category: PG_U_INITIAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e03 as uint32,
        last: 0x2e03 as uint32,
        category: PG_U_FINAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e04 as uint32,
        last: 0x2e04 as uint32,
        category: PG_U_INITIAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e05 as uint32,
        last: 0x2e05 as uint32,
        category: PG_U_FINAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e06 as uint32,
        last: 0x2e08 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e09 as uint32,
        last: 0x2e09 as uint32,
        category: PG_U_INITIAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e0a as uint32,
        last: 0x2e0a as uint32,
        category: PG_U_FINAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e0b as uint32,
        last: 0x2e0b as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e0c as uint32,
        last: 0x2e0c as uint32,
        category: PG_U_INITIAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e0d as uint32,
        last: 0x2e0d as uint32,
        category: PG_U_FINAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e0e as uint32,
        last: 0x2e16 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e17 as uint32,
        last: 0x2e17 as uint32,
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e18 as uint32,
        last: 0x2e19 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e1a as uint32,
        last: 0x2e1a as uint32,
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e1b as uint32,
        last: 0x2e1b as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e1c as uint32,
        last: 0x2e1c as uint32,
        category: PG_U_INITIAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e1d as uint32,
        last: 0x2e1d as uint32,
        category: PG_U_FINAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e1e as uint32,
        last: 0x2e1f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e20 as uint32,
        last: 0x2e20 as uint32,
        category: PG_U_INITIAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e21 as uint32,
        last: 0x2e21 as uint32,
        category: PG_U_FINAL_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e22 as uint32,
        last: 0x2e22 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e23 as uint32,
        last: 0x2e23 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e24 as uint32,
        last: 0x2e24 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e25 as uint32,
        last: 0x2e25 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e26 as uint32,
        last: 0x2e26 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e27 as uint32,
        last: 0x2e27 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e28 as uint32,
        last: 0x2e28 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e29 as uint32,
        last: 0x2e29 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e2a as uint32,
        last: 0x2e2e as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e2f as uint32,
        last: 0x2e2f as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e30 as uint32,
        last: 0x2e39 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e3a as uint32,
        last: 0x2e3b as uint32,
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e3c as uint32,
        last: 0x2e3f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e40 as uint32,
        last: 0x2e40 as uint32,
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e41 as uint32,
        last: 0x2e41 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e42 as uint32,
        last: 0x2e42 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e43 as uint32,
        last: 0x2e4f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e50 as uint32,
        last: 0x2e51 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e52 as uint32,
        last: 0x2e54 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e55 as uint32,
        last: 0x2e55 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e56 as uint32,
        last: 0x2e56 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e57 as uint32,
        last: 0x2e57 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e58 as uint32,
        last: 0x2e58 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e59 as uint32,
        last: 0x2e59 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e5a as uint32,
        last: 0x2e5a as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e5b as uint32,
        last: 0x2e5b as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e5c as uint32,
        last: 0x2e5c as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e5d as uint32,
        last: 0x2e5d as uint32,
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e80 as uint32,
        last: 0x2e99 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2e9b as uint32,
        last: 0x2ef3 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2f00 as uint32,
        last: 0x2fd5 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ff0 as uint32,
        last: 0x2fff as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3000 as uint32,
        last: 0x3000 as uint32,
        category: PG_U_SPACE_SEPARATOR as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3001 as uint32,
        last: 0x3003 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3004 as uint32,
        last: 0x3004 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3005 as uint32,
        last: 0x3005 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3006 as uint32,
        last: 0x3006 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3007 as uint32,
        last: 0x3007 as uint32,
        category: PG_U_LETTER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3008 as uint32,
        last: 0x3008 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3009 as uint32,
        last: 0x3009 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x300a as uint32,
        last: 0x300a as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x300b as uint32,
        last: 0x300b as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x300c as uint32,
        last: 0x300c as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x300d as uint32,
        last: 0x300d as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x300e as uint32,
        last: 0x300e as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x300f as uint32,
        last: 0x300f as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3010 as uint32,
        last: 0x3010 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3011 as uint32,
        last: 0x3011 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3012 as uint32,
        last: 0x3013 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3014 as uint32,
        last: 0x3014 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3015 as uint32,
        last: 0x3015 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3016 as uint32,
        last: 0x3016 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3017 as uint32,
        last: 0x3017 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3018 as uint32,
        last: 0x3018 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3019 as uint32,
        last: 0x3019 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x301a as uint32,
        last: 0x301a as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x301b as uint32,
        last: 0x301b as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x301c as uint32,
        last: 0x301c as uint32,
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x301d as uint32,
        last: 0x301d as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x301e as uint32,
        last: 0x301f as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3020 as uint32,
        last: 0x3020 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3021 as uint32,
        last: 0x3029 as uint32,
        category: PG_U_LETTER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x302a as uint32,
        last: 0x302d as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x302e as uint32,
        last: 0x302f as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3030 as uint32,
        last: 0x3030 as uint32,
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3031 as uint32,
        last: 0x3035 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3036 as uint32,
        last: 0x3037 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3038 as uint32,
        last: 0x303a as uint32,
        category: PG_U_LETTER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x303b as uint32,
        last: 0x303b as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x303c as uint32,
        last: 0x303c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x303d as uint32,
        last: 0x303d as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x303e as uint32,
        last: 0x303f as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3041 as uint32,
        last: 0x3096 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3099 as uint32,
        last: 0x309a as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x309b as uint32,
        last: 0x309c as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x309d as uint32,
        last: 0x309e as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x309f as uint32,
        last: 0x309f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x30a0 as uint32,
        last: 0x30a0 as uint32,
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x30a1 as uint32,
        last: 0x30fa as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x30fb as uint32,
        last: 0x30fb as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x30fc as uint32,
        last: 0x30fe as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x30ff as uint32,
        last: 0x30ff as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3105 as uint32,
        last: 0x312f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3131 as uint32,
        last: 0x318e as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3190 as uint32,
        last: 0x3191 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3192 as uint32,
        last: 0x3195 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3196 as uint32,
        last: 0x319f as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x31a0 as uint32,
        last: 0x31bf as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x31c0 as uint32,
        last: 0x31e5 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x31ef as uint32,
        last: 0x31ef as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x31f0 as uint32,
        last: 0x31ff as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3200 as uint32,
        last: 0x321e as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3220 as uint32,
        last: 0x3229 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x322a as uint32,
        last: 0x3247 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3248 as uint32,
        last: 0x324f as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3250 as uint32,
        last: 0x3250 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3251 as uint32,
        last: 0x325f as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3260 as uint32,
        last: 0x327f as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3280 as uint32,
        last: 0x3289 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x328a as uint32,
        last: 0x32b0 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x32b1 as uint32,
        last: 0x32bf as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x32c0 as uint32,
        last: 0x33ff as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x3400 as uint32,
        last: 0x4dbf as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4dc0 as uint32,
        last: 0x4dff as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x4e00 as uint32,
        last: 0xa014 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa015 as uint32,
        last: 0xa015 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa016 as uint32,
        last: 0xa48c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa490 as uint32,
        last: 0xa4c6 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa4d0 as uint32,
        last: 0xa4f7 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa4f8 as uint32,
        last: 0xa4fd as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa4fe as uint32,
        last: 0xa4ff as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa500 as uint32,
        last: 0xa60b as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa60c as uint32,
        last: 0xa60c as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa60d as uint32,
        last: 0xa60f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa610 as uint32,
        last: 0xa61f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa620 as uint32,
        last: 0xa629 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa62a as uint32,
        last: 0xa62b as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa640 as uint32,
        last: 0xa640 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa641 as uint32,
        last: 0xa641 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa642 as uint32,
        last: 0xa642 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa643 as uint32,
        last: 0xa643 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa644 as uint32,
        last: 0xa644 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa645 as uint32,
        last: 0xa645 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa646 as uint32,
        last: 0xa646 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa647 as uint32,
        last: 0xa647 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa648 as uint32,
        last: 0xa648 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa649 as uint32,
        last: 0xa649 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa64a as uint32,
        last: 0xa64a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa64b as uint32,
        last: 0xa64b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa64c as uint32,
        last: 0xa64c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa64d as uint32,
        last: 0xa64d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa64e as uint32,
        last: 0xa64e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa64f as uint32,
        last: 0xa64f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa650 as uint32,
        last: 0xa650 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa651 as uint32,
        last: 0xa651 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa652 as uint32,
        last: 0xa652 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa653 as uint32,
        last: 0xa653 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa654 as uint32,
        last: 0xa654 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa655 as uint32,
        last: 0xa655 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa656 as uint32,
        last: 0xa656 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa657 as uint32,
        last: 0xa657 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa658 as uint32,
        last: 0xa658 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa659 as uint32,
        last: 0xa659 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa65a as uint32,
        last: 0xa65a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa65b as uint32,
        last: 0xa65b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa65c as uint32,
        last: 0xa65c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa65d as uint32,
        last: 0xa65d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa65e as uint32,
        last: 0xa65e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa65f as uint32,
        last: 0xa65f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa660 as uint32,
        last: 0xa660 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa661 as uint32,
        last: 0xa661 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa662 as uint32,
        last: 0xa662 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa663 as uint32,
        last: 0xa663 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa664 as uint32,
        last: 0xa664 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa665 as uint32,
        last: 0xa665 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa666 as uint32,
        last: 0xa666 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa667 as uint32,
        last: 0xa667 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa668 as uint32,
        last: 0xa668 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa669 as uint32,
        last: 0xa669 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa66a as uint32,
        last: 0xa66a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa66b as uint32,
        last: 0xa66b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa66c as uint32,
        last: 0xa66c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa66d as uint32,
        last: 0xa66d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa66e as uint32,
        last: 0xa66e as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa66f as uint32,
        last: 0xa66f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa670 as uint32,
        last: 0xa672 as uint32,
        category: PG_U_ENCLOSING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa673 as uint32,
        last: 0xa673 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa674 as uint32,
        last: 0xa67d as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa67e as uint32,
        last: 0xa67e as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa67f as uint32,
        last: 0xa67f as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa680 as uint32,
        last: 0xa680 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa681 as uint32,
        last: 0xa681 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa682 as uint32,
        last: 0xa682 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa683 as uint32,
        last: 0xa683 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa684 as uint32,
        last: 0xa684 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa685 as uint32,
        last: 0xa685 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa686 as uint32,
        last: 0xa686 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa687 as uint32,
        last: 0xa687 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa688 as uint32,
        last: 0xa688 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa689 as uint32,
        last: 0xa689 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa68a as uint32,
        last: 0xa68a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa68b as uint32,
        last: 0xa68b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa68c as uint32,
        last: 0xa68c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa68d as uint32,
        last: 0xa68d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa68e as uint32,
        last: 0xa68e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa68f as uint32,
        last: 0xa68f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa690 as uint32,
        last: 0xa690 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa691 as uint32,
        last: 0xa691 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa692 as uint32,
        last: 0xa692 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa693 as uint32,
        last: 0xa693 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa694 as uint32,
        last: 0xa694 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa695 as uint32,
        last: 0xa695 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa696 as uint32,
        last: 0xa696 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa697 as uint32,
        last: 0xa697 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa698 as uint32,
        last: 0xa698 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa699 as uint32,
        last: 0xa699 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa69a as uint32,
        last: 0xa69a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa69b as uint32,
        last: 0xa69b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa69c as uint32,
        last: 0xa69d as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa69e as uint32,
        last: 0xa69f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa6a0 as uint32,
        last: 0xa6e5 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa6e6 as uint32,
        last: 0xa6ef as uint32,
        category: PG_U_LETTER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa6f0 as uint32,
        last: 0xa6f1 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa6f2 as uint32,
        last: 0xa6f7 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa700 as uint32,
        last: 0xa716 as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa717 as uint32,
        last: 0xa71f as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa720 as uint32,
        last: 0xa721 as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa722 as uint32,
        last: 0xa722 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa723 as uint32,
        last: 0xa723 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa724 as uint32,
        last: 0xa724 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa725 as uint32,
        last: 0xa725 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa726 as uint32,
        last: 0xa726 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa727 as uint32,
        last: 0xa727 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa728 as uint32,
        last: 0xa728 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa729 as uint32,
        last: 0xa729 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa72a as uint32,
        last: 0xa72a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa72b as uint32,
        last: 0xa72b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa72c as uint32,
        last: 0xa72c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa72d as uint32,
        last: 0xa72d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa72e as uint32,
        last: 0xa72e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa72f as uint32,
        last: 0xa731 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa732 as uint32,
        last: 0xa732 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa733 as uint32,
        last: 0xa733 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa734 as uint32,
        last: 0xa734 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa735 as uint32,
        last: 0xa735 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa736 as uint32,
        last: 0xa736 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa737 as uint32,
        last: 0xa737 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa738 as uint32,
        last: 0xa738 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa739 as uint32,
        last: 0xa739 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa73a as uint32,
        last: 0xa73a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa73b as uint32,
        last: 0xa73b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa73c as uint32,
        last: 0xa73c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa73d as uint32,
        last: 0xa73d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa73e as uint32,
        last: 0xa73e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa73f as uint32,
        last: 0xa73f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa740 as uint32,
        last: 0xa740 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa741 as uint32,
        last: 0xa741 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa742 as uint32,
        last: 0xa742 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa743 as uint32,
        last: 0xa743 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa744 as uint32,
        last: 0xa744 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa745 as uint32,
        last: 0xa745 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa746 as uint32,
        last: 0xa746 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa747 as uint32,
        last: 0xa747 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa748 as uint32,
        last: 0xa748 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa749 as uint32,
        last: 0xa749 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa74a as uint32,
        last: 0xa74a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa74b as uint32,
        last: 0xa74b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa74c as uint32,
        last: 0xa74c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa74d as uint32,
        last: 0xa74d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa74e as uint32,
        last: 0xa74e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa74f as uint32,
        last: 0xa74f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa750 as uint32,
        last: 0xa750 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa751 as uint32,
        last: 0xa751 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa752 as uint32,
        last: 0xa752 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa753 as uint32,
        last: 0xa753 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa754 as uint32,
        last: 0xa754 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa755 as uint32,
        last: 0xa755 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa756 as uint32,
        last: 0xa756 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa757 as uint32,
        last: 0xa757 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa758 as uint32,
        last: 0xa758 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa759 as uint32,
        last: 0xa759 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa75a as uint32,
        last: 0xa75a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa75b as uint32,
        last: 0xa75b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa75c as uint32,
        last: 0xa75c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa75d as uint32,
        last: 0xa75d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa75e as uint32,
        last: 0xa75e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa75f as uint32,
        last: 0xa75f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa760 as uint32,
        last: 0xa760 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa761 as uint32,
        last: 0xa761 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa762 as uint32,
        last: 0xa762 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa763 as uint32,
        last: 0xa763 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa764 as uint32,
        last: 0xa764 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa765 as uint32,
        last: 0xa765 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa766 as uint32,
        last: 0xa766 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa767 as uint32,
        last: 0xa767 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa768 as uint32,
        last: 0xa768 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa769 as uint32,
        last: 0xa769 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa76a as uint32,
        last: 0xa76a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa76b as uint32,
        last: 0xa76b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa76c as uint32,
        last: 0xa76c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa76d as uint32,
        last: 0xa76d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa76e as uint32,
        last: 0xa76e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa76f as uint32,
        last: 0xa76f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa770 as uint32,
        last: 0xa770 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa771 as uint32,
        last: 0xa778 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa779 as uint32,
        last: 0xa779 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa77a as uint32,
        last: 0xa77a as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa77b as uint32,
        last: 0xa77b as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa77c as uint32,
        last: 0xa77c as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa77d as uint32,
        last: 0xa77e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa77f as uint32,
        last: 0xa77f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa780 as uint32,
        last: 0xa780 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa781 as uint32,
        last: 0xa781 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa782 as uint32,
        last: 0xa782 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa783 as uint32,
        last: 0xa783 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa784 as uint32,
        last: 0xa784 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa785 as uint32,
        last: 0xa785 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa786 as uint32,
        last: 0xa786 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa787 as uint32,
        last: 0xa787 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa788 as uint32,
        last: 0xa788 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa789 as uint32,
        last: 0xa78a as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa78b as uint32,
        last: 0xa78b as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa78c as uint32,
        last: 0xa78c as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa78d as uint32,
        last: 0xa78d as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa78e as uint32,
        last: 0xa78e as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa78f as uint32,
        last: 0xa78f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa790 as uint32,
        last: 0xa790 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa791 as uint32,
        last: 0xa791 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa792 as uint32,
        last: 0xa792 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa793 as uint32,
        last: 0xa795 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa796 as uint32,
        last: 0xa796 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa797 as uint32,
        last: 0xa797 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa798 as uint32,
        last: 0xa798 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa799 as uint32,
        last: 0xa799 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa79a as uint32,
        last: 0xa79a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa79b as uint32,
        last: 0xa79b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa79c as uint32,
        last: 0xa79c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa79d as uint32,
        last: 0xa79d as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa79e as uint32,
        last: 0xa79e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa79f as uint32,
        last: 0xa79f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7a0 as uint32,
        last: 0xa7a0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7a1 as uint32,
        last: 0xa7a1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7a2 as uint32,
        last: 0xa7a2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7a3 as uint32,
        last: 0xa7a3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7a4 as uint32,
        last: 0xa7a4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7a5 as uint32,
        last: 0xa7a5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7a6 as uint32,
        last: 0xa7a6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7a7 as uint32,
        last: 0xa7a7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7a8 as uint32,
        last: 0xa7a8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7a9 as uint32,
        last: 0xa7a9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7aa as uint32,
        last: 0xa7ae as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7af as uint32,
        last: 0xa7af as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7b0 as uint32,
        last: 0xa7b4 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7b5 as uint32,
        last: 0xa7b5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7b6 as uint32,
        last: 0xa7b6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7b7 as uint32,
        last: 0xa7b7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7b8 as uint32,
        last: 0xa7b8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7b9 as uint32,
        last: 0xa7b9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7ba as uint32,
        last: 0xa7ba as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7bb as uint32,
        last: 0xa7bb as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7bc as uint32,
        last: 0xa7bc as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7bd as uint32,
        last: 0xa7bd as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7be as uint32,
        last: 0xa7be as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7bf as uint32,
        last: 0xa7bf as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7c0 as uint32,
        last: 0xa7c0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7c1 as uint32,
        last: 0xa7c1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7c2 as uint32,
        last: 0xa7c2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7c3 as uint32,
        last: 0xa7c3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7c4 as uint32,
        last: 0xa7c7 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7c8 as uint32,
        last: 0xa7c8 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7c9 as uint32,
        last: 0xa7c9 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7ca as uint32,
        last: 0xa7ca as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7cb as uint32,
        last: 0xa7cc as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7cd as uint32,
        last: 0xa7cd as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7d0 as uint32,
        last: 0xa7d0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7d1 as uint32,
        last: 0xa7d1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7d3 as uint32,
        last: 0xa7d3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7d5 as uint32,
        last: 0xa7d5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7d6 as uint32,
        last: 0xa7d6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7d7 as uint32,
        last: 0xa7d7 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7d8 as uint32,
        last: 0xa7d8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7d9 as uint32,
        last: 0xa7d9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7da as uint32,
        last: 0xa7da as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7db as uint32,
        last: 0xa7db as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7dc as uint32,
        last: 0xa7dc as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7f2 as uint32,
        last: 0xa7f4 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7f5 as uint32,
        last: 0xa7f5 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7f6 as uint32,
        last: 0xa7f6 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7f7 as uint32,
        last: 0xa7f7 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7f8 as uint32,
        last: 0xa7f9 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7fa as uint32,
        last: 0xa7fa as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa7fb as uint32,
        last: 0xa801 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa802 as uint32,
        last: 0xa802 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa803 as uint32,
        last: 0xa805 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa806 as uint32,
        last: 0xa806 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa807 as uint32,
        last: 0xa80a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa80b as uint32,
        last: 0xa80b as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa80c as uint32,
        last: 0xa822 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa823 as uint32,
        last: 0xa824 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa825 as uint32,
        last: 0xa826 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa827 as uint32,
        last: 0xa827 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa828 as uint32,
        last: 0xa82b as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa82c as uint32,
        last: 0xa82c as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa830 as uint32,
        last: 0xa835 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa836 as uint32,
        last: 0xa837 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa838 as uint32,
        last: 0xa838 as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa839 as uint32,
        last: 0xa839 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa840 as uint32,
        last: 0xa873 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa874 as uint32,
        last: 0xa877 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa880 as uint32,
        last: 0xa881 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa882 as uint32,
        last: 0xa8b3 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa8b4 as uint32,
        last: 0xa8c3 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa8c4 as uint32,
        last: 0xa8c5 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa8ce as uint32,
        last: 0xa8cf as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa8d0 as uint32,
        last: 0xa8d9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa8e0 as uint32,
        last: 0xa8f1 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa8f2 as uint32,
        last: 0xa8f7 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa8f8 as uint32,
        last: 0xa8fa as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa8fb as uint32,
        last: 0xa8fb as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa8fc as uint32,
        last: 0xa8fc as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa8fd as uint32,
        last: 0xa8fe as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa8ff as uint32,
        last: 0xa8ff as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa900 as uint32,
        last: 0xa909 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa90a as uint32,
        last: 0xa925 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa926 as uint32,
        last: 0xa92d as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa92e as uint32,
        last: 0xa92f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa930 as uint32,
        last: 0xa946 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa947 as uint32,
        last: 0xa951 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa952 as uint32,
        last: 0xa953 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa95f as uint32,
        last: 0xa95f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa960 as uint32,
        last: 0xa97c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa980 as uint32,
        last: 0xa982 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa983 as uint32,
        last: 0xa983 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa984 as uint32,
        last: 0xa9b2 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa9b3 as uint32,
        last: 0xa9b3 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa9b4 as uint32,
        last: 0xa9b5 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa9b6 as uint32,
        last: 0xa9b9 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa9ba as uint32,
        last: 0xa9bb as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa9bc as uint32,
        last: 0xa9bd as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa9be as uint32,
        last: 0xa9c0 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa9c1 as uint32,
        last: 0xa9cd as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa9cf as uint32,
        last: 0xa9cf as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa9d0 as uint32,
        last: 0xa9d9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa9de as uint32,
        last: 0xa9df as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa9e0 as uint32,
        last: 0xa9e4 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa9e5 as uint32,
        last: 0xa9e5 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa9e6 as uint32,
        last: 0xa9e6 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa9e7 as uint32,
        last: 0xa9ef as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa9f0 as uint32,
        last: 0xa9f9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xa9fa as uint32,
        last: 0xa9fe as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa00 as uint32,
        last: 0xaa28 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa29 as uint32,
        last: 0xaa2e as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa2f as uint32,
        last: 0xaa30 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa31 as uint32,
        last: 0xaa32 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa33 as uint32,
        last: 0xaa34 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa35 as uint32,
        last: 0xaa36 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa40 as uint32,
        last: 0xaa42 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa43 as uint32,
        last: 0xaa43 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa44 as uint32,
        last: 0xaa4b as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa4c as uint32,
        last: 0xaa4c as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa4d as uint32,
        last: 0xaa4d as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa50 as uint32,
        last: 0xaa59 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa5c as uint32,
        last: 0xaa5f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa60 as uint32,
        last: 0xaa6f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa70 as uint32,
        last: 0xaa70 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa71 as uint32,
        last: 0xaa76 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa77 as uint32,
        last: 0xaa79 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa7a as uint32,
        last: 0xaa7a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa7b as uint32,
        last: 0xaa7b as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa7c as uint32,
        last: 0xaa7c as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa7d as uint32,
        last: 0xaa7d as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaa7e as uint32,
        last: 0xaaaf as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaab0 as uint32,
        last: 0xaab0 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaab1 as uint32,
        last: 0xaab1 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaab2 as uint32,
        last: 0xaab4 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaab5 as uint32,
        last: 0xaab6 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaab7 as uint32,
        last: 0xaab8 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaab9 as uint32,
        last: 0xaabd as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaabe as uint32,
        last: 0xaabf as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaac0 as uint32,
        last: 0xaac0 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaac1 as uint32,
        last: 0xaac1 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaac2 as uint32,
        last: 0xaac2 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaadb as uint32,
        last: 0xaadc as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaadd as uint32,
        last: 0xaadd as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaade as uint32,
        last: 0xaadf as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaae0 as uint32,
        last: 0xaaea as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaaeb as uint32,
        last: 0xaaeb as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaaec as uint32,
        last: 0xaaed as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaaee as uint32,
        last: 0xaaef as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaaf0 as uint32,
        last: 0xaaf1 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaaf2 as uint32,
        last: 0xaaf2 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaaf3 as uint32,
        last: 0xaaf4 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaaf5 as uint32,
        last: 0xaaf5 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xaaf6 as uint32,
        last: 0xaaf6 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xab01 as uint32,
        last: 0xab06 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xab09 as uint32,
        last: 0xab0e as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xab11 as uint32,
        last: 0xab16 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xab20 as uint32,
        last: 0xab26 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xab28 as uint32,
        last: 0xab2e as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xab30 as uint32,
        last: 0xab5a as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xab5b as uint32,
        last: 0xab5b as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xab5c as uint32,
        last: 0xab5f as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xab60 as uint32,
        last: 0xab68 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xab69 as uint32,
        last: 0xab69 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xab6a as uint32,
        last: 0xab6b as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xab70 as uint32,
        last: 0xabbf as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xabc0 as uint32,
        last: 0xabe2 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xabe3 as uint32,
        last: 0xabe4 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xabe5 as uint32,
        last: 0xabe5 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xabe6 as uint32,
        last: 0xabe7 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xabe8 as uint32,
        last: 0xabe8 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xabe9 as uint32,
        last: 0xabea as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xabeb as uint32,
        last: 0xabeb as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xabec as uint32,
        last: 0xabec as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xabed as uint32,
        last: 0xabed as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xabf0 as uint32,
        last: 0xabf9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xac00 as uint32,
        last: 0xd7a3 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd7b0 as uint32,
        last: 0xd7c6 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd7cb as uint32,
        last: 0xd7fb as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xd800 as uint32,
        last: 0xdfff as uint32,
        category: PG_U_SURROGATE as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xe000 as uint32,
        last: 0xf8ff as uint32,
        category: PG_U_PRIVATE_USE as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf900 as uint32,
        last: 0xfa6d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfa70 as uint32,
        last: 0xfad9 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfb00 as uint32,
        last: 0xfb06 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfb13 as uint32,
        last: 0xfb17 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfb1d as uint32,
        last: 0xfb1d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfb1e as uint32,
        last: 0xfb1e as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfb1f as uint32,
        last: 0xfb28 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfb29 as uint32,
        last: 0xfb29 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfb2a as uint32,
        last: 0xfb36 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfb38 as uint32,
        last: 0xfb3c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfb3e as uint32,
        last: 0xfb3e as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfb40 as uint32,
        last: 0xfb41 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfb43 as uint32,
        last: 0xfb44 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfb46 as uint32,
        last: 0xfbb1 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfbb2 as uint32,
        last: 0xfbc2 as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfbd3 as uint32,
        last: 0xfd3d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfd3e as uint32,
        last: 0xfd3e as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfd3f as uint32,
        last: 0xfd3f as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfd40 as uint32,
        last: 0xfd4f as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfd50 as uint32,
        last: 0xfd8f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfd92 as uint32,
        last: 0xfdc7 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfdcf as uint32,
        last: 0xfdcf as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfdf0 as uint32,
        last: 0xfdfb as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfdfc as uint32,
        last: 0xfdfc as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfdfd as uint32,
        last: 0xfdff as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe00 as uint32,
        last: 0xfe0f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe10 as uint32,
        last: 0xfe16 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe17 as uint32,
        last: 0xfe17 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe18 as uint32,
        last: 0xfe18 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe19 as uint32,
        last: 0xfe19 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe20 as uint32,
        last: 0xfe2f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe30 as uint32,
        last: 0xfe30 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe31 as uint32,
        last: 0xfe32 as uint32,
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe33 as uint32,
        last: 0xfe34 as uint32,
        category: PG_U_CONNECTOR_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe35 as uint32,
        last: 0xfe35 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe36 as uint32,
        last: 0xfe36 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe37 as uint32,
        last: 0xfe37 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe38 as uint32,
        last: 0xfe38 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe39 as uint32,
        last: 0xfe39 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe3a as uint32,
        last: 0xfe3a as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe3b as uint32,
        last: 0xfe3b as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe3c as uint32,
        last: 0xfe3c as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe3d as uint32,
        last: 0xfe3d as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe3e as uint32,
        last: 0xfe3e as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe3f as uint32,
        last: 0xfe3f as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe40 as uint32,
        last: 0xfe40 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe41 as uint32,
        last: 0xfe41 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe42 as uint32,
        last: 0xfe42 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe43 as uint32,
        last: 0xfe43 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe44 as uint32,
        last: 0xfe44 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe45 as uint32,
        last: 0xfe46 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe47 as uint32,
        last: 0xfe47 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe48 as uint32,
        last: 0xfe48 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe49 as uint32,
        last: 0xfe4c as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe4d as uint32,
        last: 0xfe4f as uint32,
        category: PG_U_CONNECTOR_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe50 as uint32,
        last: 0xfe52 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe54 as uint32,
        last: 0xfe57 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe58 as uint32,
        last: 0xfe58 as uint32,
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe59 as uint32,
        last: 0xfe59 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe5a as uint32,
        last: 0xfe5a as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe5b as uint32,
        last: 0xfe5b as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe5c as uint32,
        last: 0xfe5c as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe5d as uint32,
        last: 0xfe5d as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe5e as uint32,
        last: 0xfe5e as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe5f as uint32,
        last: 0xfe61 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe62 as uint32,
        last: 0xfe62 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe63 as uint32,
        last: 0xfe63 as uint32,
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe64 as uint32,
        last: 0xfe66 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe68 as uint32,
        last: 0xfe68 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe69 as uint32,
        last: 0xfe69 as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe6a as uint32,
        last: 0xfe6b as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe70 as uint32,
        last: 0xfe74 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfe76 as uint32,
        last: 0xfefc as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfeff as uint32,
        last: 0xfeff as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff01 as uint32,
        last: 0xff03 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff04 as uint32,
        last: 0xff04 as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff05 as uint32,
        last: 0xff07 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff08 as uint32,
        last: 0xff08 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff09 as uint32,
        last: 0xff09 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff0a as uint32,
        last: 0xff0a as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff0b as uint32,
        last: 0xff0b as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff0c as uint32,
        last: 0xff0c as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff0d as uint32,
        last: 0xff0d as uint32,
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff0e as uint32,
        last: 0xff0f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff10 as uint32,
        last: 0xff19 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff1a as uint32,
        last: 0xff1b as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff1c as uint32,
        last: 0xff1e as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff1f as uint32,
        last: 0xff20 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff21 as uint32,
        last: 0xff3a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff3b as uint32,
        last: 0xff3b as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff3c as uint32,
        last: 0xff3c as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff3d as uint32,
        last: 0xff3d as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff3e as uint32,
        last: 0xff3e as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff3f as uint32,
        last: 0xff3f as uint32,
        category: PG_U_CONNECTOR_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff40 as uint32,
        last: 0xff40 as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff41 as uint32,
        last: 0xff5a as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff5b as uint32,
        last: 0xff5b as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff5c as uint32,
        last: 0xff5c as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff5d as uint32,
        last: 0xff5d as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff5e as uint32,
        last: 0xff5e as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff5f as uint32,
        last: 0xff5f as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff60 as uint32,
        last: 0xff60 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff61 as uint32,
        last: 0xff61 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff62 as uint32,
        last: 0xff62 as uint32,
        category: PG_U_OPEN_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff63 as uint32,
        last: 0xff63 as uint32,
        category: PG_U_CLOSE_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff64 as uint32,
        last: 0xff65 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff66 as uint32,
        last: 0xff6f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff70 as uint32,
        last: 0xff70 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff71 as uint32,
        last: 0xff9d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xff9e as uint32,
        last: 0xff9f as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xffa0 as uint32,
        last: 0xffbe as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xffc2 as uint32,
        last: 0xffc7 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xffca as uint32,
        last: 0xffcf as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xffd2 as uint32,
        last: 0xffd7 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xffda as uint32,
        last: 0xffdc as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xffe0 as uint32,
        last: 0xffe1 as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xffe2 as uint32,
        last: 0xffe2 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xffe3 as uint32,
        last: 0xffe3 as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xffe4 as uint32,
        last: 0xffe4 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xffe5 as uint32,
        last: 0xffe6 as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xffe8 as uint32,
        last: 0xffe8 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xffe9 as uint32,
        last: 0xffec as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xffed as uint32,
        last: 0xffee as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfff9 as uint32,
        last: 0xfffb as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xfffc as uint32,
        last: 0xfffd as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10000 as uint32,
        last: 0x1000b as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1000d as uint32,
        last: 0x10026 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10028 as uint32,
        last: 0x1003a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1003c as uint32,
        last: 0x1003d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1003f as uint32,
        last: 0x1004d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10050 as uint32,
        last: 0x1005d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10080 as uint32,
        last: 0x100fa as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10100 as uint32,
        last: 0x10102 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10107 as uint32,
        last: 0x10133 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10137 as uint32,
        last: 0x1013f as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10140 as uint32,
        last: 0x10174 as uint32,
        category: PG_U_LETTER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10175 as uint32,
        last: 0x10178 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10179 as uint32,
        last: 0x10189 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1018a as uint32,
        last: 0x1018b as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1018c as uint32,
        last: 0x1018e as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10190 as uint32,
        last: 0x1019c as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x101a0 as uint32,
        last: 0x101a0 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x101d0 as uint32,
        last: 0x101fc as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x101fd as uint32,
        last: 0x101fd as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10280 as uint32,
        last: 0x1029c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x102a0 as uint32,
        last: 0x102d0 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x102e0 as uint32,
        last: 0x102e0 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x102e1 as uint32,
        last: 0x102fb as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10300 as uint32,
        last: 0x1031f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10320 as uint32,
        last: 0x10323 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1032d as uint32,
        last: 0x10340 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10341 as uint32,
        last: 0x10341 as uint32,
        category: PG_U_LETTER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10342 as uint32,
        last: 0x10349 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1034a as uint32,
        last: 0x1034a as uint32,
        category: PG_U_LETTER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10350 as uint32,
        last: 0x10375 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10376 as uint32,
        last: 0x1037a as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10380 as uint32,
        last: 0x1039d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1039f as uint32,
        last: 0x1039f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x103a0 as uint32,
        last: 0x103c3 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x103c8 as uint32,
        last: 0x103cf as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x103d0 as uint32,
        last: 0x103d0 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x103d1 as uint32,
        last: 0x103d5 as uint32,
        category: PG_U_LETTER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10400 as uint32,
        last: 0x10427 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10428 as uint32,
        last: 0x1044f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10450 as uint32,
        last: 0x1049d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x104a0 as uint32,
        last: 0x104a9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x104b0 as uint32,
        last: 0x104d3 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x104d8 as uint32,
        last: 0x104fb as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10500 as uint32,
        last: 0x10527 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10530 as uint32,
        last: 0x10563 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1056f as uint32,
        last: 0x1056f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10570 as uint32,
        last: 0x1057a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1057c as uint32,
        last: 0x1058a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1058c as uint32,
        last: 0x10592 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10594 as uint32,
        last: 0x10595 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10597 as uint32,
        last: 0x105a1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x105a3 as uint32,
        last: 0x105b1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x105b3 as uint32,
        last: 0x105b9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x105bb as uint32,
        last: 0x105bc as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x105c0 as uint32,
        last: 0x105f3 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10600 as uint32,
        last: 0x10736 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10740 as uint32,
        last: 0x10755 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10760 as uint32,
        last: 0x10767 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10780 as uint32,
        last: 0x10785 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10787 as uint32,
        last: 0x107b0 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x107b2 as uint32,
        last: 0x107ba as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10800 as uint32,
        last: 0x10805 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10808 as uint32,
        last: 0x10808 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1080a as uint32,
        last: 0x10835 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10837 as uint32,
        last: 0x10838 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1083c as uint32,
        last: 0x1083c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1083f as uint32,
        last: 0x10855 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10857 as uint32,
        last: 0x10857 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10858 as uint32,
        last: 0x1085f as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10860 as uint32,
        last: 0x10876 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10877 as uint32,
        last: 0x10878 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10879 as uint32,
        last: 0x1087f as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10880 as uint32,
        last: 0x1089e as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x108a7 as uint32,
        last: 0x108af as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x108e0 as uint32,
        last: 0x108f2 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x108f4 as uint32,
        last: 0x108f5 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x108fb as uint32,
        last: 0x108ff as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10900 as uint32,
        last: 0x10915 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10916 as uint32,
        last: 0x1091b as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1091f as uint32,
        last: 0x1091f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10920 as uint32,
        last: 0x10939 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1093f as uint32,
        last: 0x1093f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10980 as uint32,
        last: 0x109b7 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x109bc as uint32,
        last: 0x109bd as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x109be as uint32,
        last: 0x109bf as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x109c0 as uint32,
        last: 0x109cf as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x109d2 as uint32,
        last: 0x109ff as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10a00 as uint32,
        last: 0x10a00 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10a01 as uint32,
        last: 0x10a03 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10a05 as uint32,
        last: 0x10a06 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10a0c as uint32,
        last: 0x10a0f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10a10 as uint32,
        last: 0x10a13 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10a15 as uint32,
        last: 0x10a17 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10a19 as uint32,
        last: 0x10a35 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10a38 as uint32,
        last: 0x10a3a as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10a3f as uint32,
        last: 0x10a3f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10a40 as uint32,
        last: 0x10a48 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10a50 as uint32,
        last: 0x10a58 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10a60 as uint32,
        last: 0x10a7c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10a7d as uint32,
        last: 0x10a7e as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10a7f as uint32,
        last: 0x10a7f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10a80 as uint32,
        last: 0x10a9c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10a9d as uint32,
        last: 0x10a9f as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10ac0 as uint32,
        last: 0x10ac7 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10ac8 as uint32,
        last: 0x10ac8 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10ac9 as uint32,
        last: 0x10ae4 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10ae5 as uint32,
        last: 0x10ae6 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10aeb as uint32,
        last: 0x10aef as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10af0 as uint32,
        last: 0x10af6 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10b00 as uint32,
        last: 0x10b35 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10b39 as uint32,
        last: 0x10b3f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10b40 as uint32,
        last: 0x10b55 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10b58 as uint32,
        last: 0x10b5f as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10b60 as uint32,
        last: 0x10b72 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10b78 as uint32,
        last: 0x10b7f as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10b80 as uint32,
        last: 0x10b91 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10b99 as uint32,
        last: 0x10b9c as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10ba9 as uint32,
        last: 0x10baf as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10c00 as uint32,
        last: 0x10c48 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10c80 as uint32,
        last: 0x10cb2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10cc0 as uint32,
        last: 0x10cf2 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10cfa as uint32,
        last: 0x10cff as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10d00 as uint32,
        last: 0x10d23 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10d24 as uint32,
        last: 0x10d27 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10d30 as uint32,
        last: 0x10d39 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10d40 as uint32,
        last: 0x10d49 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10d4a as uint32,
        last: 0x10d4d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10d4e as uint32,
        last: 0x10d4e as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10d4f as uint32,
        last: 0x10d4f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10d50 as uint32,
        last: 0x10d65 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10d69 as uint32,
        last: 0x10d6d as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10d6e as uint32,
        last: 0x10d6e as uint32,
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10d6f as uint32,
        last: 0x10d6f as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10d70 as uint32,
        last: 0x10d85 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10d8e as uint32,
        last: 0x10d8f as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10e60 as uint32,
        last: 0x10e7e as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10e80 as uint32,
        last: 0x10ea9 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10eab as uint32,
        last: 0x10eac as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10ead as uint32,
        last: 0x10ead as uint32,
        category: PG_U_DASH_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10eb0 as uint32,
        last: 0x10eb1 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10ec2 as uint32,
        last: 0x10ec4 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10efc as uint32,
        last: 0x10eff as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10f00 as uint32,
        last: 0x10f1c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10f1d as uint32,
        last: 0x10f26 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10f27 as uint32,
        last: 0x10f27 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10f30 as uint32,
        last: 0x10f45 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10f46 as uint32,
        last: 0x10f50 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10f51 as uint32,
        last: 0x10f54 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10f55 as uint32,
        last: 0x10f59 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10f70 as uint32,
        last: 0x10f81 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10f82 as uint32,
        last: 0x10f85 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10f86 as uint32,
        last: 0x10f89 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10fb0 as uint32,
        last: 0x10fc4 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10fc5 as uint32,
        last: 0x10fcb as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x10fe0 as uint32,
        last: 0x10ff6 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11000 as uint32,
        last: 0x11000 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11001 as uint32,
        last: 0x11001 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11002 as uint32,
        last: 0x11002 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11003 as uint32,
        last: 0x11037 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11038 as uint32,
        last: 0x11046 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11047 as uint32,
        last: 0x1104d as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11052 as uint32,
        last: 0x11065 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11066 as uint32,
        last: 0x1106f as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11070 as uint32,
        last: 0x11070 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11071 as uint32,
        last: 0x11072 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11073 as uint32,
        last: 0x11074 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11075 as uint32,
        last: 0x11075 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1107f as uint32,
        last: 0x11081 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11082 as uint32,
        last: 0x11082 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11083 as uint32,
        last: 0x110af as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x110b0 as uint32,
        last: 0x110b2 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x110b3 as uint32,
        last: 0x110b6 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x110b7 as uint32,
        last: 0x110b8 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x110b9 as uint32,
        last: 0x110ba as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x110bb as uint32,
        last: 0x110bc as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x110bd as uint32,
        last: 0x110bd as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x110be as uint32,
        last: 0x110c1 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x110c2 as uint32,
        last: 0x110c2 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x110cd as uint32,
        last: 0x110cd as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x110d0 as uint32,
        last: 0x110e8 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x110f0 as uint32,
        last: 0x110f9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11100 as uint32,
        last: 0x11102 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11103 as uint32,
        last: 0x11126 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11127 as uint32,
        last: 0x1112b as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1112c as uint32,
        last: 0x1112c as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1112d as uint32,
        last: 0x11134 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11136 as uint32,
        last: 0x1113f as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11140 as uint32,
        last: 0x11143 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11144 as uint32,
        last: 0x11144 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11145 as uint32,
        last: 0x11146 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11147 as uint32,
        last: 0x11147 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11150 as uint32,
        last: 0x11172 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11173 as uint32,
        last: 0x11173 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11174 as uint32,
        last: 0x11175 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11176 as uint32,
        last: 0x11176 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11180 as uint32,
        last: 0x11181 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11182 as uint32,
        last: 0x11182 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11183 as uint32,
        last: 0x111b2 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x111b3 as uint32,
        last: 0x111b5 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x111b6 as uint32,
        last: 0x111be as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x111bf as uint32,
        last: 0x111c0 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x111c1 as uint32,
        last: 0x111c4 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x111c5 as uint32,
        last: 0x111c8 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x111c9 as uint32,
        last: 0x111cc as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x111cd as uint32,
        last: 0x111cd as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x111ce as uint32,
        last: 0x111ce as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x111cf as uint32,
        last: 0x111cf as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x111d0 as uint32,
        last: 0x111d9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x111da as uint32,
        last: 0x111da as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x111db as uint32,
        last: 0x111db as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x111dc as uint32,
        last: 0x111dc as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x111dd as uint32,
        last: 0x111df as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x111e1 as uint32,
        last: 0x111f4 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11200 as uint32,
        last: 0x11211 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11213 as uint32,
        last: 0x1122b as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1122c as uint32,
        last: 0x1122e as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1122f as uint32,
        last: 0x11231 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11232 as uint32,
        last: 0x11233 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11234 as uint32,
        last: 0x11234 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11235 as uint32,
        last: 0x11235 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11236 as uint32,
        last: 0x11237 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11238 as uint32,
        last: 0x1123d as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1123e as uint32,
        last: 0x1123e as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1123f as uint32,
        last: 0x11240 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11241 as uint32,
        last: 0x11241 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11280 as uint32,
        last: 0x11286 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11288 as uint32,
        last: 0x11288 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1128a as uint32,
        last: 0x1128d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1128f as uint32,
        last: 0x1129d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1129f as uint32,
        last: 0x112a8 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x112a9 as uint32,
        last: 0x112a9 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x112b0 as uint32,
        last: 0x112de as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x112df as uint32,
        last: 0x112df as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x112e0 as uint32,
        last: 0x112e2 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x112e3 as uint32,
        last: 0x112ea as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x112f0 as uint32,
        last: 0x112f9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11300 as uint32,
        last: 0x11301 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11302 as uint32,
        last: 0x11303 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11305 as uint32,
        last: 0x1130c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1130f as uint32,
        last: 0x11310 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11313 as uint32,
        last: 0x11328 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1132a as uint32,
        last: 0x11330 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11332 as uint32,
        last: 0x11333 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11335 as uint32,
        last: 0x11339 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1133b as uint32,
        last: 0x1133c as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1133d as uint32,
        last: 0x1133d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1133e as uint32,
        last: 0x1133f as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11340 as uint32,
        last: 0x11340 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11341 as uint32,
        last: 0x11344 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11347 as uint32,
        last: 0x11348 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1134b as uint32,
        last: 0x1134d as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11350 as uint32,
        last: 0x11350 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11357 as uint32,
        last: 0x11357 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1135d as uint32,
        last: 0x11361 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11362 as uint32,
        last: 0x11363 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11366 as uint32,
        last: 0x1136c as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11370 as uint32,
        last: 0x11374 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11380 as uint32,
        last: 0x11389 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1138b as uint32,
        last: 0x1138b as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1138e as uint32,
        last: 0x1138e as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11390 as uint32,
        last: 0x113b5 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x113b7 as uint32,
        last: 0x113b7 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x113b8 as uint32,
        last: 0x113ba as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x113bb as uint32,
        last: 0x113c0 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x113c2 as uint32,
        last: 0x113c2 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x113c5 as uint32,
        last: 0x113c5 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x113c7 as uint32,
        last: 0x113ca as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x113cc as uint32,
        last: 0x113cd as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x113ce as uint32,
        last: 0x113ce as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x113cf as uint32,
        last: 0x113cf as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x113d0 as uint32,
        last: 0x113d0 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x113d1 as uint32,
        last: 0x113d1 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x113d2 as uint32,
        last: 0x113d2 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x113d3 as uint32,
        last: 0x113d3 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x113d4 as uint32,
        last: 0x113d5 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x113d7 as uint32,
        last: 0x113d8 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x113e1 as uint32,
        last: 0x113e2 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11400 as uint32,
        last: 0x11434 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11435 as uint32,
        last: 0x11437 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11438 as uint32,
        last: 0x1143f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11440 as uint32,
        last: 0x11441 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11442 as uint32,
        last: 0x11444 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11445 as uint32,
        last: 0x11445 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11446 as uint32,
        last: 0x11446 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11447 as uint32,
        last: 0x1144a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1144b as uint32,
        last: 0x1144f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11450 as uint32,
        last: 0x11459 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1145a as uint32,
        last: 0x1145b as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1145d as uint32,
        last: 0x1145d as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1145e as uint32,
        last: 0x1145e as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1145f as uint32,
        last: 0x11461 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11480 as uint32,
        last: 0x114af as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x114b0 as uint32,
        last: 0x114b2 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x114b3 as uint32,
        last: 0x114b8 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x114b9 as uint32,
        last: 0x114b9 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x114ba as uint32,
        last: 0x114ba as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x114bb as uint32,
        last: 0x114be as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x114bf as uint32,
        last: 0x114c0 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x114c1 as uint32,
        last: 0x114c1 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x114c2 as uint32,
        last: 0x114c3 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x114c4 as uint32,
        last: 0x114c5 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x114c6 as uint32,
        last: 0x114c6 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x114c7 as uint32,
        last: 0x114c7 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x114d0 as uint32,
        last: 0x114d9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11580 as uint32,
        last: 0x115ae as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x115af as uint32,
        last: 0x115b1 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x115b2 as uint32,
        last: 0x115b5 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x115b8 as uint32,
        last: 0x115bb as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x115bc as uint32,
        last: 0x115bd as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x115be as uint32,
        last: 0x115be as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x115bf as uint32,
        last: 0x115c0 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x115c1 as uint32,
        last: 0x115d7 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x115d8 as uint32,
        last: 0x115db as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x115dc as uint32,
        last: 0x115dd as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11600 as uint32,
        last: 0x1162f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11630 as uint32,
        last: 0x11632 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11633 as uint32,
        last: 0x1163a as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1163b as uint32,
        last: 0x1163c as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1163d as uint32,
        last: 0x1163d as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1163e as uint32,
        last: 0x1163e as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1163f as uint32,
        last: 0x11640 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11641 as uint32,
        last: 0x11643 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11644 as uint32,
        last: 0x11644 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11650 as uint32,
        last: 0x11659 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11660 as uint32,
        last: 0x1166c as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11680 as uint32,
        last: 0x116aa as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x116ab as uint32,
        last: 0x116ab as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x116ac as uint32,
        last: 0x116ac as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x116ad as uint32,
        last: 0x116ad as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x116ae as uint32,
        last: 0x116af as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x116b0 as uint32,
        last: 0x116b5 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x116b6 as uint32,
        last: 0x116b6 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x116b7 as uint32,
        last: 0x116b7 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x116b8 as uint32,
        last: 0x116b8 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x116b9 as uint32,
        last: 0x116b9 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x116c0 as uint32,
        last: 0x116c9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x116d0 as uint32,
        last: 0x116e3 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11700 as uint32,
        last: 0x1171a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1171d as uint32,
        last: 0x1171d as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1171e as uint32,
        last: 0x1171e as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1171f as uint32,
        last: 0x1171f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11720 as uint32,
        last: 0x11721 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11722 as uint32,
        last: 0x11725 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11726 as uint32,
        last: 0x11726 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11727 as uint32,
        last: 0x1172b as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11730 as uint32,
        last: 0x11739 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1173a as uint32,
        last: 0x1173b as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1173c as uint32,
        last: 0x1173e as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1173f as uint32,
        last: 0x1173f as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11740 as uint32,
        last: 0x11746 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11800 as uint32,
        last: 0x1182b as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1182c as uint32,
        last: 0x1182e as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1182f as uint32,
        last: 0x11837 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11838 as uint32,
        last: 0x11838 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11839 as uint32,
        last: 0x1183a as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1183b as uint32,
        last: 0x1183b as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x118a0 as uint32,
        last: 0x118bf as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x118c0 as uint32,
        last: 0x118df as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x118e0 as uint32,
        last: 0x118e9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x118ea as uint32,
        last: 0x118f2 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x118ff as uint32,
        last: 0x11906 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11909 as uint32,
        last: 0x11909 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1190c as uint32,
        last: 0x11913 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11915 as uint32,
        last: 0x11916 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11918 as uint32,
        last: 0x1192f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11930 as uint32,
        last: 0x11935 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11937 as uint32,
        last: 0x11938 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1193b as uint32,
        last: 0x1193c as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1193d as uint32,
        last: 0x1193d as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1193e as uint32,
        last: 0x1193e as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1193f as uint32,
        last: 0x1193f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11940 as uint32,
        last: 0x11940 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11941 as uint32,
        last: 0x11941 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11942 as uint32,
        last: 0x11942 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11943 as uint32,
        last: 0x11943 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11944 as uint32,
        last: 0x11946 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11950 as uint32,
        last: 0x11959 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x119a0 as uint32,
        last: 0x119a7 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x119aa as uint32,
        last: 0x119d0 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x119d1 as uint32,
        last: 0x119d3 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x119d4 as uint32,
        last: 0x119d7 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x119da as uint32,
        last: 0x119db as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x119dc as uint32,
        last: 0x119df as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x119e0 as uint32,
        last: 0x119e0 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x119e1 as uint32,
        last: 0x119e1 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x119e2 as uint32,
        last: 0x119e2 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x119e3 as uint32,
        last: 0x119e3 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x119e4 as uint32,
        last: 0x119e4 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a00 as uint32,
        last: 0x11a00 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a01 as uint32,
        last: 0x11a0a as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a0b as uint32,
        last: 0x11a32 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a33 as uint32,
        last: 0x11a38 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a39 as uint32,
        last: 0x11a39 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a3a as uint32,
        last: 0x11a3a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a3b as uint32,
        last: 0x11a3e as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a3f as uint32,
        last: 0x11a46 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a47 as uint32,
        last: 0x11a47 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a50 as uint32,
        last: 0x11a50 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a51 as uint32,
        last: 0x11a56 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a57 as uint32,
        last: 0x11a58 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a59 as uint32,
        last: 0x11a5b as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a5c as uint32,
        last: 0x11a89 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a8a as uint32,
        last: 0x11a96 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a97 as uint32,
        last: 0x11a97 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a98 as uint32,
        last: 0x11a99 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a9a as uint32,
        last: 0x11a9c as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a9d as uint32,
        last: 0x11a9d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11a9e as uint32,
        last: 0x11aa2 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11ab0 as uint32,
        last: 0x11af8 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11b00 as uint32,
        last: 0x11b09 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11bc0 as uint32,
        last: 0x11be0 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11be1 as uint32,
        last: 0x11be1 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11bf0 as uint32,
        last: 0x11bf9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11c00 as uint32,
        last: 0x11c08 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11c0a as uint32,
        last: 0x11c2e as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11c2f as uint32,
        last: 0x11c2f as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11c30 as uint32,
        last: 0x11c36 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11c38 as uint32,
        last: 0x11c3d as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11c3e as uint32,
        last: 0x11c3e as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11c3f as uint32,
        last: 0x11c3f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11c40 as uint32,
        last: 0x11c40 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11c41 as uint32,
        last: 0x11c45 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11c50 as uint32,
        last: 0x11c59 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11c5a as uint32,
        last: 0x11c6c as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11c70 as uint32,
        last: 0x11c71 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11c72 as uint32,
        last: 0x11c8f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11c92 as uint32,
        last: 0x11ca7 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11ca9 as uint32,
        last: 0x11ca9 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11caa as uint32,
        last: 0x11cb0 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11cb1 as uint32,
        last: 0x11cb1 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11cb2 as uint32,
        last: 0x11cb3 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11cb4 as uint32,
        last: 0x11cb4 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11cb5 as uint32,
        last: 0x11cb6 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d00 as uint32,
        last: 0x11d06 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d08 as uint32,
        last: 0x11d09 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d0b as uint32,
        last: 0x11d30 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d31 as uint32,
        last: 0x11d36 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d3a as uint32,
        last: 0x11d3a as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d3c as uint32,
        last: 0x11d3d as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d3f as uint32,
        last: 0x11d45 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d46 as uint32,
        last: 0x11d46 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d47 as uint32,
        last: 0x11d47 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d50 as uint32,
        last: 0x11d59 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d60 as uint32,
        last: 0x11d65 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d67 as uint32,
        last: 0x11d68 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d6a as uint32,
        last: 0x11d89 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d8a as uint32,
        last: 0x11d8e as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d90 as uint32,
        last: 0x11d91 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d93 as uint32,
        last: 0x11d94 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d95 as uint32,
        last: 0x11d95 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d96 as uint32,
        last: 0x11d96 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d97 as uint32,
        last: 0x11d97 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11d98 as uint32,
        last: 0x11d98 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11da0 as uint32,
        last: 0x11da9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11ee0 as uint32,
        last: 0x11ef2 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11ef3 as uint32,
        last: 0x11ef4 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11ef5 as uint32,
        last: 0x11ef6 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11ef7 as uint32,
        last: 0x11ef8 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11f00 as uint32,
        last: 0x11f01 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11f02 as uint32,
        last: 0x11f02 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11f03 as uint32,
        last: 0x11f03 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11f04 as uint32,
        last: 0x11f10 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11f12 as uint32,
        last: 0x11f33 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11f34 as uint32,
        last: 0x11f35 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11f36 as uint32,
        last: 0x11f3a as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11f3e as uint32,
        last: 0x11f3f as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11f40 as uint32,
        last: 0x11f40 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11f41 as uint32,
        last: 0x11f41 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11f42 as uint32,
        last: 0x11f42 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11f43 as uint32,
        last: 0x11f4f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11f50 as uint32,
        last: 0x11f59 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11f5a as uint32,
        last: 0x11f5a as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11fb0 as uint32,
        last: 0x11fb0 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11fc0 as uint32,
        last: 0x11fd4 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11fd5 as uint32,
        last: 0x11fdc as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11fdd as uint32,
        last: 0x11fe0 as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11fe1 as uint32,
        last: 0x11ff1 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x11fff as uint32,
        last: 0x11fff as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x12000 as uint32,
        last: 0x12399 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x12400 as uint32,
        last: 0x1246e as uint32,
        category: PG_U_LETTER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x12470 as uint32,
        last: 0x12474 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x12480 as uint32,
        last: 0x12543 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x12f90 as uint32,
        last: 0x12ff0 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x12ff1 as uint32,
        last: 0x12ff2 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x13000 as uint32,
        last: 0x1342f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x13430 as uint32,
        last: 0x1343f as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x13440 as uint32,
        last: 0x13440 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x13441 as uint32,
        last: 0x13446 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x13447 as uint32,
        last: 0x13455 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x13460 as uint32,
        last: 0x143fa as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x14400 as uint32,
        last: 0x14646 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16100 as uint32,
        last: 0x1611d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1611e as uint32,
        last: 0x16129 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1612a as uint32,
        last: 0x1612c as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1612d as uint32,
        last: 0x1612f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16130 as uint32,
        last: 0x16139 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16800 as uint32,
        last: 0x16a38 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16a40 as uint32,
        last: 0x16a5e as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16a60 as uint32,
        last: 0x16a69 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16a6e as uint32,
        last: 0x16a6f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16a70 as uint32,
        last: 0x16abe as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16ac0 as uint32,
        last: 0x16ac9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16ad0 as uint32,
        last: 0x16aed as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16af0 as uint32,
        last: 0x16af4 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16af5 as uint32,
        last: 0x16af5 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16b00 as uint32,
        last: 0x16b2f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16b30 as uint32,
        last: 0x16b36 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16b37 as uint32,
        last: 0x16b3b as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16b3c as uint32,
        last: 0x16b3f as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16b40 as uint32,
        last: 0x16b43 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16b44 as uint32,
        last: 0x16b44 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16b45 as uint32,
        last: 0x16b45 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16b50 as uint32,
        last: 0x16b59 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16b5b as uint32,
        last: 0x16b61 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16b63 as uint32,
        last: 0x16b77 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16b7d as uint32,
        last: 0x16b8f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16d40 as uint32,
        last: 0x16d42 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16d43 as uint32,
        last: 0x16d6a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16d6b as uint32,
        last: 0x16d6c as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16d6d as uint32,
        last: 0x16d6f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16d70 as uint32,
        last: 0x16d79 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16e40 as uint32,
        last: 0x16e5f as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16e60 as uint32,
        last: 0x16e7f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16e80 as uint32,
        last: 0x16e96 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16e97 as uint32,
        last: 0x16e9a as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16f00 as uint32,
        last: 0x16f4a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16f4f as uint32,
        last: 0x16f4f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16f50 as uint32,
        last: 0x16f50 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16f51 as uint32,
        last: 0x16f87 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16f8f as uint32,
        last: 0x16f92 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16f93 as uint32,
        last: 0x16f9f as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16fe0 as uint32,
        last: 0x16fe1 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16fe2 as uint32,
        last: 0x16fe2 as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16fe3 as uint32,
        last: 0x16fe3 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16fe4 as uint32,
        last: 0x16fe4 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x16ff0 as uint32,
        last: 0x16ff1 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x17000 as uint32,
        last: 0x187f7 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x18800 as uint32,
        last: 0x18cd5 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x18cff as uint32,
        last: 0x18d08 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1aff0 as uint32,
        last: 0x1aff3 as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1aff5 as uint32,
        last: 0x1affb as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1affd as uint32,
        last: 0x1affe as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b000 as uint32,
        last: 0x1b122 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b132 as uint32,
        last: 0x1b132 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b150 as uint32,
        last: 0x1b152 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b155 as uint32,
        last: 0x1b155 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b164 as uint32,
        last: 0x1b167 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1b170 as uint32,
        last: 0x1b2fb as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bc00 as uint32,
        last: 0x1bc6a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bc70 as uint32,
        last: 0x1bc7c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bc80 as uint32,
        last: 0x1bc88 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bc90 as uint32,
        last: 0x1bc99 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bc9c as uint32,
        last: 0x1bc9c as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bc9d as uint32,
        last: 0x1bc9e as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bc9f as uint32,
        last: 0x1bc9f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1bca0 as uint32,
        last: 0x1bca3 as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1cc00 as uint32,
        last: 0x1ccef as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ccf0 as uint32,
        last: 0x1ccf9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1cd00 as uint32,
        last: 0x1ceb3 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1cf00 as uint32,
        last: 0x1cf2d as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1cf30 as uint32,
        last: 0x1cf46 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1cf50 as uint32,
        last: 0x1cfc3 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d000 as uint32,
        last: 0x1d0f5 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d100 as uint32,
        last: 0x1d126 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d129 as uint32,
        last: 0x1d164 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d165 as uint32,
        last: 0x1d166 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d167 as uint32,
        last: 0x1d169 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d16a as uint32,
        last: 0x1d16c as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d16d as uint32,
        last: 0x1d172 as uint32,
        category: PG_U_SPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d173 as uint32,
        last: 0x1d17a as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d17b as uint32,
        last: 0x1d182 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d183 as uint32,
        last: 0x1d184 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d185 as uint32,
        last: 0x1d18b as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d18c as uint32,
        last: 0x1d1a9 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d1aa as uint32,
        last: 0x1d1ad as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d1ae as uint32,
        last: 0x1d1ea as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d200 as uint32,
        last: 0x1d241 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d242 as uint32,
        last: 0x1d244 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d245 as uint32,
        last: 0x1d245 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d2c0 as uint32,
        last: 0x1d2d3 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d2e0 as uint32,
        last: 0x1d2f3 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d300 as uint32,
        last: 0x1d356 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d360 as uint32,
        last: 0x1d378 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d400 as uint32,
        last: 0x1d419 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d41a as uint32,
        last: 0x1d433 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d434 as uint32,
        last: 0x1d44d as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d44e as uint32,
        last: 0x1d454 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d456 as uint32,
        last: 0x1d467 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d468 as uint32,
        last: 0x1d481 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d482 as uint32,
        last: 0x1d49b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d49c as uint32,
        last: 0x1d49c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d49e as uint32,
        last: 0x1d49f as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d4a2 as uint32,
        last: 0x1d4a2 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d4a5 as uint32,
        last: 0x1d4a6 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d4a9 as uint32,
        last: 0x1d4ac as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d4ae as uint32,
        last: 0x1d4b5 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d4b6 as uint32,
        last: 0x1d4b9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d4bb as uint32,
        last: 0x1d4bb as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d4bd as uint32,
        last: 0x1d4c3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d4c5 as uint32,
        last: 0x1d4cf as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d4d0 as uint32,
        last: 0x1d4e9 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d4ea as uint32,
        last: 0x1d503 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d504 as uint32,
        last: 0x1d505 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d507 as uint32,
        last: 0x1d50a as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d50d as uint32,
        last: 0x1d514 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d516 as uint32,
        last: 0x1d51c as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d51e as uint32,
        last: 0x1d537 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d538 as uint32,
        last: 0x1d539 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d53b as uint32,
        last: 0x1d53e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d540 as uint32,
        last: 0x1d544 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d546 as uint32,
        last: 0x1d546 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d54a as uint32,
        last: 0x1d550 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d552 as uint32,
        last: 0x1d56b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d56c as uint32,
        last: 0x1d585 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d586 as uint32,
        last: 0x1d59f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d5a0 as uint32,
        last: 0x1d5b9 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d5ba as uint32,
        last: 0x1d5d3 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d5d4 as uint32,
        last: 0x1d5ed as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d5ee as uint32,
        last: 0x1d607 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d608 as uint32,
        last: 0x1d621 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d622 as uint32,
        last: 0x1d63b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d63c as uint32,
        last: 0x1d655 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d656 as uint32,
        last: 0x1d66f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d670 as uint32,
        last: 0x1d689 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d68a as uint32,
        last: 0x1d6a5 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d6a8 as uint32,
        last: 0x1d6c0 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d6c1 as uint32,
        last: 0x1d6c1 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d6c2 as uint32,
        last: 0x1d6da as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d6db as uint32,
        last: 0x1d6db as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d6dc as uint32,
        last: 0x1d6e1 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d6e2 as uint32,
        last: 0x1d6fa as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d6fb as uint32,
        last: 0x1d6fb as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d6fc as uint32,
        last: 0x1d714 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d715 as uint32,
        last: 0x1d715 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d716 as uint32,
        last: 0x1d71b as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d71c as uint32,
        last: 0x1d734 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d735 as uint32,
        last: 0x1d735 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d736 as uint32,
        last: 0x1d74e as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d74f as uint32,
        last: 0x1d74f as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d750 as uint32,
        last: 0x1d755 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d756 as uint32,
        last: 0x1d76e as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d76f as uint32,
        last: 0x1d76f as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d770 as uint32,
        last: 0x1d788 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d789 as uint32,
        last: 0x1d789 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d78a as uint32,
        last: 0x1d78f as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d790 as uint32,
        last: 0x1d7a8 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d7a9 as uint32,
        last: 0x1d7a9 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d7aa as uint32,
        last: 0x1d7c2 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d7c3 as uint32,
        last: 0x1d7c3 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d7c4 as uint32,
        last: 0x1d7c9 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d7ca as uint32,
        last: 0x1d7ca as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d7cb as uint32,
        last: 0x1d7cb as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d7ce as uint32,
        last: 0x1d7ff as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1d800 as uint32,
        last: 0x1d9ff as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1da00 as uint32,
        last: 0x1da36 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1da37 as uint32,
        last: 0x1da3a as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1da3b as uint32,
        last: 0x1da6c as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1da6d as uint32,
        last: 0x1da74 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1da75 as uint32,
        last: 0x1da75 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1da76 as uint32,
        last: 0x1da83 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1da84 as uint32,
        last: 0x1da84 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1da85 as uint32,
        last: 0x1da86 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1da87 as uint32,
        last: 0x1da8b as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1da9b as uint32,
        last: 0x1da9f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1daa1 as uint32,
        last: 0x1daaf as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1df00 as uint32,
        last: 0x1df09 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1df0a as uint32,
        last: 0x1df0a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1df0b as uint32,
        last: 0x1df1e as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1df25 as uint32,
        last: 0x1df2a as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e000 as uint32,
        last: 0x1e006 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e008 as uint32,
        last: 0x1e018 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e01b as uint32,
        last: 0x1e021 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e023 as uint32,
        last: 0x1e024 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e026 as uint32,
        last: 0x1e02a as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e030 as uint32,
        last: 0x1e06d as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e08f as uint32,
        last: 0x1e08f as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e100 as uint32,
        last: 0x1e12c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e130 as uint32,
        last: 0x1e136 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e137 as uint32,
        last: 0x1e13d as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e140 as uint32,
        last: 0x1e149 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e14e as uint32,
        last: 0x1e14e as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e14f as uint32,
        last: 0x1e14f as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e290 as uint32,
        last: 0x1e2ad as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e2ae as uint32,
        last: 0x1e2ae as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e2c0 as uint32,
        last: 0x1e2eb as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e2ec as uint32,
        last: 0x1e2ef as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e2f0 as uint32,
        last: 0x1e2f9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e2ff as uint32,
        last: 0x1e2ff as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e4d0 as uint32,
        last: 0x1e4ea as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e4eb as uint32,
        last: 0x1e4eb as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e4ec as uint32,
        last: 0x1e4ef as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e4f0 as uint32,
        last: 0x1e4f9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e5d0 as uint32,
        last: 0x1e5ed as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e5ee as uint32,
        last: 0x1e5ef as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e5f0 as uint32,
        last: 0x1e5f0 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e5f1 as uint32,
        last: 0x1e5fa as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e5ff as uint32,
        last: 0x1e5ff as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e7e0 as uint32,
        last: 0x1e7e6 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e7e8 as uint32,
        last: 0x1e7eb as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e7ed as uint32,
        last: 0x1e7ee as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e7f0 as uint32,
        last: 0x1e7fe as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e800 as uint32,
        last: 0x1e8c4 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e8c7 as uint32,
        last: 0x1e8cf as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e8d0 as uint32,
        last: 0x1e8d6 as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e900 as uint32,
        last: 0x1e921 as uint32,
        category: PG_U_UPPERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e922 as uint32,
        last: 0x1e943 as uint32,
        category: PG_U_LOWERCASE_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e944 as uint32,
        last: 0x1e94a as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e94b as uint32,
        last: 0x1e94b as uint32,
        category: PG_U_MODIFIER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e950 as uint32,
        last: 0x1e959 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1e95e as uint32,
        last: 0x1e95f as uint32,
        category: PG_U_OTHER_PUNCTUATION as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ec71 as uint32,
        last: 0x1ecab as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ecac as uint32,
        last: 0x1ecac as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ecad as uint32,
        last: 0x1ecaf as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ecb0 as uint32,
        last: 0x1ecb0 as uint32,
        category: PG_U_CURRENCY_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ecb1 as uint32,
        last: 0x1ecb4 as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ed01 as uint32,
        last: 0x1ed2d as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ed2e as uint32,
        last: 0x1ed2e as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ed2f as uint32,
        last: 0x1ed3d as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee00 as uint32,
        last: 0x1ee03 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee05 as uint32,
        last: 0x1ee1f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee21 as uint32,
        last: 0x1ee22 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee24 as uint32,
        last: 0x1ee24 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee27 as uint32,
        last: 0x1ee27 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee29 as uint32,
        last: 0x1ee32 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee34 as uint32,
        last: 0x1ee37 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee39 as uint32,
        last: 0x1ee39 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee3b as uint32,
        last: 0x1ee3b as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee42 as uint32,
        last: 0x1ee42 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee47 as uint32,
        last: 0x1ee47 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee49 as uint32,
        last: 0x1ee49 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee4b as uint32,
        last: 0x1ee4b as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee4d as uint32,
        last: 0x1ee4f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee51 as uint32,
        last: 0x1ee52 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee54 as uint32,
        last: 0x1ee54 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee57 as uint32,
        last: 0x1ee57 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee59 as uint32,
        last: 0x1ee59 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee5b as uint32,
        last: 0x1ee5b as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee5d as uint32,
        last: 0x1ee5d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee5f as uint32,
        last: 0x1ee5f as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee61 as uint32,
        last: 0x1ee62 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee64 as uint32,
        last: 0x1ee64 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee67 as uint32,
        last: 0x1ee6a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee6c as uint32,
        last: 0x1ee72 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee74 as uint32,
        last: 0x1ee77 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee79 as uint32,
        last: 0x1ee7c as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee7e as uint32,
        last: 0x1ee7e as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee80 as uint32,
        last: 0x1ee89 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1ee8b as uint32,
        last: 0x1ee9b as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eea1 as uint32,
        last: 0x1eea3 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eea5 as uint32,
        last: 0x1eea9 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eeab as uint32,
        last: 0x1eebb as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1eef0 as uint32,
        last: 0x1eef1 as uint32,
        category: PG_U_MATH_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f000 as uint32,
        last: 0x1f02b as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f030 as uint32,
        last: 0x1f093 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f0a0 as uint32,
        last: 0x1f0ae as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f0b1 as uint32,
        last: 0x1f0bf as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f0c1 as uint32,
        last: 0x1f0cf as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f0d1 as uint32,
        last: 0x1f0f5 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f100 as uint32,
        last: 0x1f10c as uint32,
        category: PG_U_OTHER_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f10d as uint32,
        last: 0x1f1ad as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f1e6 as uint32,
        last: 0x1f202 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f210 as uint32,
        last: 0x1f23b as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f240 as uint32,
        last: 0x1f248 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f250 as uint32,
        last: 0x1f251 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f260 as uint32,
        last: 0x1f265 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f300 as uint32,
        last: 0x1f3fa as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f3fb as uint32,
        last: 0x1f3ff as uint32,
        category: PG_U_MODIFIER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f400 as uint32,
        last: 0x1f6d7 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f6dc as uint32,
        last: 0x1f6ec as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f6f0 as uint32,
        last: 0x1f6fc as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f700 as uint32,
        last: 0x1f776 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f77b as uint32,
        last: 0x1f7d9 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f7e0 as uint32,
        last: 0x1f7eb as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f7f0 as uint32,
        last: 0x1f7f0 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f800 as uint32,
        last: 0x1f80b as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f810 as uint32,
        last: 0x1f847 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f850 as uint32,
        last: 0x1f859 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f860 as uint32,
        last: 0x1f887 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f890 as uint32,
        last: 0x1f8ad as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f8b0 as uint32,
        last: 0x1f8bb as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f8c0 as uint32,
        last: 0x1f8c1 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1f900 as uint32,
        last: 0x1fa53 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fa60 as uint32,
        last: 0x1fa6d as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fa70 as uint32,
        last: 0x1fa7c as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fa80 as uint32,
        last: 0x1fa89 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fa8f as uint32,
        last: 0x1fac6 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1face as uint32,
        last: 0x1fadc as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fadf as uint32,
        last: 0x1fae9 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1faf0 as uint32,
        last: 0x1faf8 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fb00 as uint32,
        last: 0x1fb92 as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fb94 as uint32,
        last: 0x1fbef as uint32,
        category: PG_U_OTHER_SYMBOL as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x1fbf0 as uint32,
        last: 0x1fbf9 as uint32,
        category: PG_U_DECIMAL_NUMBER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x20000 as uint32,
        last: 0x2a6df as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2a700 as uint32,
        last: 0x2b739 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2b740 as uint32,
        last: 0x2b81d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2b820 as uint32,
        last: 0x2cea1 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ceb0 as uint32,
        last: 0x2ebe0 as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2ebf0 as uint32,
        last: 0x2ee5d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x2f800 as uint32,
        last: 0x2fa1d as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x30000 as uint32,
        last: 0x3134a as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x31350 as uint32,
        last: 0x323af as uint32,
        category: PG_U_OTHER_LETTER as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xe0001 as uint32,
        last: 0xe0001 as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xe0020 as uint32,
        last: 0xe007f as uint32,
        category: PG_U_FORMAT as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xe0100 as uint32,
        last: 0xe01ef as uint32,
        category: PG_U_NONSPACING_MARK as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0xf0000 as uint32,
        last: 0xffffd as uint32,
        category: PG_U_PRIVATE_USE as ::core::ffi::c_int as uint8,
    },
    pg_category_range {
        first: 0x100000 as uint32,
        last: 0x10fffd as uint32,
        category: PG_U_PRIVATE_USE as ::core::ffi::c_int as uint8,
    },
];
pub(crate) static unicode_alphabetic: [pg_unicode_range; 1179] = [
    pg_unicode_range {
        first: 0x41 as uint32,
        last: 0x5a as uint32,
    },
    pg_unicode_range {
        first: 0x61 as uint32,
        last: 0x7a as uint32,
    },
    pg_unicode_range {
        first: 0xaa as uint32,
        last: 0xaa as uint32,
    },
    pg_unicode_range {
        first: 0xb5 as uint32,
        last: 0xb5 as uint32,
    },
    pg_unicode_range {
        first: 0xba as uint32,
        last: 0xba as uint32,
    },
    pg_unicode_range {
        first: 0xc0 as uint32,
        last: 0xd6 as uint32,
    },
    pg_unicode_range {
        first: 0xd8 as uint32,
        last: 0xf6 as uint32,
    },
    pg_unicode_range {
        first: 0xf8 as uint32,
        last: 0x1ba as uint32,
    },
    pg_unicode_range {
        first: 0x1bb as uint32,
        last: 0x1bb as uint32,
    },
    pg_unicode_range {
        first: 0x1bc as uint32,
        last: 0x1bf as uint32,
    },
    pg_unicode_range {
        first: 0x1c0 as uint32,
        last: 0x1c3 as uint32,
    },
    pg_unicode_range {
        first: 0x1c4 as uint32,
        last: 0x293 as uint32,
    },
    pg_unicode_range {
        first: 0x294 as uint32,
        last: 0x294 as uint32,
    },
    pg_unicode_range {
        first: 0x295 as uint32,
        last: 0x2af as uint32,
    },
    pg_unicode_range {
        first: 0x2b0 as uint32,
        last: 0x2c1 as uint32,
    },
    pg_unicode_range {
        first: 0x2c6 as uint32,
        last: 0x2d1 as uint32,
    },
    pg_unicode_range {
        first: 0x2e0 as uint32,
        last: 0x2e4 as uint32,
    },
    pg_unicode_range {
        first: 0x2ec as uint32,
        last: 0x2ec as uint32,
    },
    pg_unicode_range {
        first: 0x2ee as uint32,
        last: 0x2ee as uint32,
    },
    pg_unicode_range {
        first: 0x345 as uint32,
        last: 0x345 as uint32,
    },
    pg_unicode_range {
        first: 0x363 as uint32,
        last: 0x36f as uint32,
    },
    pg_unicode_range {
        first: 0x370 as uint32,
        last: 0x373 as uint32,
    },
    pg_unicode_range {
        first: 0x374 as uint32,
        last: 0x374 as uint32,
    },
    pg_unicode_range {
        first: 0x376 as uint32,
        last: 0x377 as uint32,
    },
    pg_unicode_range {
        first: 0x37a as uint32,
        last: 0x37a as uint32,
    },
    pg_unicode_range {
        first: 0x37b as uint32,
        last: 0x37d as uint32,
    },
    pg_unicode_range {
        first: 0x37f as uint32,
        last: 0x37f as uint32,
    },
    pg_unicode_range {
        first: 0x386 as uint32,
        last: 0x386 as uint32,
    },
    pg_unicode_range {
        first: 0x388 as uint32,
        last: 0x38a as uint32,
    },
    pg_unicode_range {
        first: 0x38c as uint32,
        last: 0x38c as uint32,
    },
    pg_unicode_range {
        first: 0x38e as uint32,
        last: 0x3a1 as uint32,
    },
    pg_unicode_range {
        first: 0x3a3 as uint32,
        last: 0x3f5 as uint32,
    },
    pg_unicode_range {
        first: 0x3f7 as uint32,
        last: 0x481 as uint32,
    },
    pg_unicode_range {
        first: 0x48a as uint32,
        last: 0x52f as uint32,
    },
    pg_unicode_range {
        first: 0x531 as uint32,
        last: 0x556 as uint32,
    },
    pg_unicode_range {
        first: 0x559 as uint32,
        last: 0x559 as uint32,
    },
    pg_unicode_range {
        first: 0x560 as uint32,
        last: 0x588 as uint32,
    },
    pg_unicode_range {
        first: 0x5b0 as uint32,
        last: 0x5bd as uint32,
    },
    pg_unicode_range {
        first: 0x5bf as uint32,
        last: 0x5bf as uint32,
    },
    pg_unicode_range {
        first: 0x5c1 as uint32,
        last: 0x5c2 as uint32,
    },
    pg_unicode_range {
        first: 0x5c4 as uint32,
        last: 0x5c5 as uint32,
    },
    pg_unicode_range {
        first: 0x5c7 as uint32,
        last: 0x5c7 as uint32,
    },
    pg_unicode_range {
        first: 0x5d0 as uint32,
        last: 0x5ea as uint32,
    },
    pg_unicode_range {
        first: 0x5ef as uint32,
        last: 0x5f2 as uint32,
    },
    pg_unicode_range {
        first: 0x610 as uint32,
        last: 0x61a as uint32,
    },
    pg_unicode_range {
        first: 0x620 as uint32,
        last: 0x63f as uint32,
    },
    pg_unicode_range {
        first: 0x640 as uint32,
        last: 0x640 as uint32,
    },
    pg_unicode_range {
        first: 0x641 as uint32,
        last: 0x64a as uint32,
    },
    pg_unicode_range {
        first: 0x64b as uint32,
        last: 0x657 as uint32,
    },
    pg_unicode_range {
        first: 0x659 as uint32,
        last: 0x65f as uint32,
    },
    pg_unicode_range {
        first: 0x66e as uint32,
        last: 0x66f as uint32,
    },
    pg_unicode_range {
        first: 0x670 as uint32,
        last: 0x670 as uint32,
    },
    pg_unicode_range {
        first: 0x671 as uint32,
        last: 0x6d3 as uint32,
    },
    pg_unicode_range {
        first: 0x6d5 as uint32,
        last: 0x6d5 as uint32,
    },
    pg_unicode_range {
        first: 0x6d6 as uint32,
        last: 0x6dc as uint32,
    },
    pg_unicode_range {
        first: 0x6e1 as uint32,
        last: 0x6e4 as uint32,
    },
    pg_unicode_range {
        first: 0x6e5 as uint32,
        last: 0x6e6 as uint32,
    },
    pg_unicode_range {
        first: 0x6e7 as uint32,
        last: 0x6e8 as uint32,
    },
    pg_unicode_range {
        first: 0x6ed as uint32,
        last: 0x6ed as uint32,
    },
    pg_unicode_range {
        first: 0x6ee as uint32,
        last: 0x6ef as uint32,
    },
    pg_unicode_range {
        first: 0x6fa as uint32,
        last: 0x6fc as uint32,
    },
    pg_unicode_range {
        first: 0x6ff as uint32,
        last: 0x6ff as uint32,
    },
    pg_unicode_range {
        first: 0x710 as uint32,
        last: 0x710 as uint32,
    },
    pg_unicode_range {
        first: 0x711 as uint32,
        last: 0x711 as uint32,
    },
    pg_unicode_range {
        first: 0x712 as uint32,
        last: 0x72f as uint32,
    },
    pg_unicode_range {
        first: 0x730 as uint32,
        last: 0x73f as uint32,
    },
    pg_unicode_range {
        first: 0x74d as uint32,
        last: 0x7a5 as uint32,
    },
    pg_unicode_range {
        first: 0x7a6 as uint32,
        last: 0x7b0 as uint32,
    },
    pg_unicode_range {
        first: 0x7b1 as uint32,
        last: 0x7b1 as uint32,
    },
    pg_unicode_range {
        first: 0x7ca as uint32,
        last: 0x7ea as uint32,
    },
    pg_unicode_range {
        first: 0x7f4 as uint32,
        last: 0x7f5 as uint32,
    },
    pg_unicode_range {
        first: 0x7fa as uint32,
        last: 0x7fa as uint32,
    },
    pg_unicode_range {
        first: 0x800 as uint32,
        last: 0x815 as uint32,
    },
    pg_unicode_range {
        first: 0x816 as uint32,
        last: 0x817 as uint32,
    },
    pg_unicode_range {
        first: 0x81a as uint32,
        last: 0x81a as uint32,
    },
    pg_unicode_range {
        first: 0x81b as uint32,
        last: 0x823 as uint32,
    },
    pg_unicode_range {
        first: 0x824 as uint32,
        last: 0x824 as uint32,
    },
    pg_unicode_range {
        first: 0x825 as uint32,
        last: 0x827 as uint32,
    },
    pg_unicode_range {
        first: 0x828 as uint32,
        last: 0x828 as uint32,
    },
    pg_unicode_range {
        first: 0x829 as uint32,
        last: 0x82c as uint32,
    },
    pg_unicode_range {
        first: 0x840 as uint32,
        last: 0x858 as uint32,
    },
    pg_unicode_range {
        first: 0x860 as uint32,
        last: 0x86a as uint32,
    },
    pg_unicode_range {
        first: 0x870 as uint32,
        last: 0x887 as uint32,
    },
    pg_unicode_range {
        first: 0x889 as uint32,
        last: 0x88e as uint32,
    },
    pg_unicode_range {
        first: 0x897 as uint32,
        last: 0x897 as uint32,
    },
    pg_unicode_range {
        first: 0x8a0 as uint32,
        last: 0x8c8 as uint32,
    },
    pg_unicode_range {
        first: 0x8c9 as uint32,
        last: 0x8c9 as uint32,
    },
    pg_unicode_range {
        first: 0x8d4 as uint32,
        last: 0x8df as uint32,
    },
    pg_unicode_range {
        first: 0x8e3 as uint32,
        last: 0x8e9 as uint32,
    },
    pg_unicode_range {
        first: 0x8f0 as uint32,
        last: 0x902 as uint32,
    },
    pg_unicode_range {
        first: 0x903 as uint32,
        last: 0x903 as uint32,
    },
    pg_unicode_range {
        first: 0x904 as uint32,
        last: 0x939 as uint32,
    },
    pg_unicode_range {
        first: 0x93a as uint32,
        last: 0x93a as uint32,
    },
    pg_unicode_range {
        first: 0x93b as uint32,
        last: 0x93b as uint32,
    },
    pg_unicode_range {
        first: 0x93d as uint32,
        last: 0x93d as uint32,
    },
    pg_unicode_range {
        first: 0x93e as uint32,
        last: 0x940 as uint32,
    },
    pg_unicode_range {
        first: 0x941 as uint32,
        last: 0x948 as uint32,
    },
    pg_unicode_range {
        first: 0x949 as uint32,
        last: 0x94c as uint32,
    },
    pg_unicode_range {
        first: 0x94e as uint32,
        last: 0x94f as uint32,
    },
    pg_unicode_range {
        first: 0x950 as uint32,
        last: 0x950 as uint32,
    },
    pg_unicode_range {
        first: 0x955 as uint32,
        last: 0x957 as uint32,
    },
    pg_unicode_range {
        first: 0x958 as uint32,
        last: 0x961 as uint32,
    },
    pg_unicode_range {
        first: 0x962 as uint32,
        last: 0x963 as uint32,
    },
    pg_unicode_range {
        first: 0x971 as uint32,
        last: 0x971 as uint32,
    },
    pg_unicode_range {
        first: 0x972 as uint32,
        last: 0x980 as uint32,
    },
    pg_unicode_range {
        first: 0x981 as uint32,
        last: 0x981 as uint32,
    },
    pg_unicode_range {
        first: 0x982 as uint32,
        last: 0x983 as uint32,
    },
    pg_unicode_range {
        first: 0x985 as uint32,
        last: 0x98c as uint32,
    },
    pg_unicode_range {
        first: 0x98f as uint32,
        last: 0x990 as uint32,
    },
    pg_unicode_range {
        first: 0x993 as uint32,
        last: 0x9a8 as uint32,
    },
    pg_unicode_range {
        first: 0x9aa as uint32,
        last: 0x9b0 as uint32,
    },
    pg_unicode_range {
        first: 0x9b2 as uint32,
        last: 0x9b2 as uint32,
    },
    pg_unicode_range {
        first: 0x9b6 as uint32,
        last: 0x9b9 as uint32,
    },
    pg_unicode_range {
        first: 0x9bd as uint32,
        last: 0x9bd as uint32,
    },
    pg_unicode_range {
        first: 0x9be as uint32,
        last: 0x9c0 as uint32,
    },
    pg_unicode_range {
        first: 0x9c1 as uint32,
        last: 0x9c4 as uint32,
    },
    pg_unicode_range {
        first: 0x9c7 as uint32,
        last: 0x9c8 as uint32,
    },
    pg_unicode_range {
        first: 0x9cb as uint32,
        last: 0x9cc as uint32,
    },
    pg_unicode_range {
        first: 0x9ce as uint32,
        last: 0x9ce as uint32,
    },
    pg_unicode_range {
        first: 0x9d7 as uint32,
        last: 0x9d7 as uint32,
    },
    pg_unicode_range {
        first: 0x9dc as uint32,
        last: 0x9dd as uint32,
    },
    pg_unicode_range {
        first: 0x9df as uint32,
        last: 0x9e1 as uint32,
    },
    pg_unicode_range {
        first: 0x9e2 as uint32,
        last: 0x9e3 as uint32,
    },
    pg_unicode_range {
        first: 0x9f0 as uint32,
        last: 0x9f1 as uint32,
    },
    pg_unicode_range {
        first: 0x9fc as uint32,
        last: 0x9fc as uint32,
    },
    pg_unicode_range {
        first: 0xa01 as uint32,
        last: 0xa02 as uint32,
    },
    pg_unicode_range {
        first: 0xa03 as uint32,
        last: 0xa03 as uint32,
    },
    pg_unicode_range {
        first: 0xa05 as uint32,
        last: 0xa0a as uint32,
    },
    pg_unicode_range {
        first: 0xa0f as uint32,
        last: 0xa10 as uint32,
    },
    pg_unicode_range {
        first: 0xa13 as uint32,
        last: 0xa28 as uint32,
    },
    pg_unicode_range {
        first: 0xa2a as uint32,
        last: 0xa30 as uint32,
    },
    pg_unicode_range {
        first: 0xa32 as uint32,
        last: 0xa33 as uint32,
    },
    pg_unicode_range {
        first: 0xa35 as uint32,
        last: 0xa36 as uint32,
    },
    pg_unicode_range {
        first: 0xa38 as uint32,
        last: 0xa39 as uint32,
    },
    pg_unicode_range {
        first: 0xa3e as uint32,
        last: 0xa40 as uint32,
    },
    pg_unicode_range {
        first: 0xa41 as uint32,
        last: 0xa42 as uint32,
    },
    pg_unicode_range {
        first: 0xa47 as uint32,
        last: 0xa48 as uint32,
    },
    pg_unicode_range {
        first: 0xa4b as uint32,
        last: 0xa4c as uint32,
    },
    pg_unicode_range {
        first: 0xa51 as uint32,
        last: 0xa51 as uint32,
    },
    pg_unicode_range {
        first: 0xa59 as uint32,
        last: 0xa5c as uint32,
    },
    pg_unicode_range {
        first: 0xa5e as uint32,
        last: 0xa5e as uint32,
    },
    pg_unicode_range {
        first: 0xa70 as uint32,
        last: 0xa71 as uint32,
    },
    pg_unicode_range {
        first: 0xa72 as uint32,
        last: 0xa74 as uint32,
    },
    pg_unicode_range {
        first: 0xa75 as uint32,
        last: 0xa75 as uint32,
    },
    pg_unicode_range {
        first: 0xa81 as uint32,
        last: 0xa82 as uint32,
    },
    pg_unicode_range {
        first: 0xa83 as uint32,
        last: 0xa83 as uint32,
    },
    pg_unicode_range {
        first: 0xa85 as uint32,
        last: 0xa8d as uint32,
    },
    pg_unicode_range {
        first: 0xa8f as uint32,
        last: 0xa91 as uint32,
    },
    pg_unicode_range {
        first: 0xa93 as uint32,
        last: 0xaa8 as uint32,
    },
    pg_unicode_range {
        first: 0xaaa as uint32,
        last: 0xab0 as uint32,
    },
    pg_unicode_range {
        first: 0xab2 as uint32,
        last: 0xab3 as uint32,
    },
    pg_unicode_range {
        first: 0xab5 as uint32,
        last: 0xab9 as uint32,
    },
    pg_unicode_range {
        first: 0xabd as uint32,
        last: 0xabd as uint32,
    },
    pg_unicode_range {
        first: 0xabe as uint32,
        last: 0xac0 as uint32,
    },
    pg_unicode_range {
        first: 0xac1 as uint32,
        last: 0xac5 as uint32,
    },
    pg_unicode_range {
        first: 0xac7 as uint32,
        last: 0xac8 as uint32,
    },
    pg_unicode_range {
        first: 0xac9 as uint32,
        last: 0xac9 as uint32,
    },
    pg_unicode_range {
        first: 0xacb as uint32,
        last: 0xacc as uint32,
    },
    pg_unicode_range {
        first: 0xad0 as uint32,
        last: 0xad0 as uint32,
    },
    pg_unicode_range {
        first: 0xae0 as uint32,
        last: 0xae1 as uint32,
    },
    pg_unicode_range {
        first: 0xae2 as uint32,
        last: 0xae3 as uint32,
    },
    pg_unicode_range {
        first: 0xaf9 as uint32,
        last: 0xaf9 as uint32,
    },
    pg_unicode_range {
        first: 0xafa as uint32,
        last: 0xafc as uint32,
    },
    pg_unicode_range {
        first: 0xb01 as uint32,
        last: 0xb01 as uint32,
    },
    pg_unicode_range {
        first: 0xb02 as uint32,
        last: 0xb03 as uint32,
    },
    pg_unicode_range {
        first: 0xb05 as uint32,
        last: 0xb0c as uint32,
    },
    pg_unicode_range {
        first: 0xb0f as uint32,
        last: 0xb10 as uint32,
    },
    pg_unicode_range {
        first: 0xb13 as uint32,
        last: 0xb28 as uint32,
    },
    pg_unicode_range {
        first: 0xb2a as uint32,
        last: 0xb30 as uint32,
    },
    pg_unicode_range {
        first: 0xb32 as uint32,
        last: 0xb33 as uint32,
    },
    pg_unicode_range {
        first: 0xb35 as uint32,
        last: 0xb39 as uint32,
    },
    pg_unicode_range {
        first: 0xb3d as uint32,
        last: 0xb3d as uint32,
    },
    pg_unicode_range {
        first: 0xb3e as uint32,
        last: 0xb3e as uint32,
    },
    pg_unicode_range {
        first: 0xb3f as uint32,
        last: 0xb3f as uint32,
    },
    pg_unicode_range {
        first: 0xb40 as uint32,
        last: 0xb40 as uint32,
    },
    pg_unicode_range {
        first: 0xb41 as uint32,
        last: 0xb44 as uint32,
    },
    pg_unicode_range {
        first: 0xb47 as uint32,
        last: 0xb48 as uint32,
    },
    pg_unicode_range {
        first: 0xb4b as uint32,
        last: 0xb4c as uint32,
    },
    pg_unicode_range {
        first: 0xb56 as uint32,
        last: 0xb56 as uint32,
    },
    pg_unicode_range {
        first: 0xb57 as uint32,
        last: 0xb57 as uint32,
    },
    pg_unicode_range {
        first: 0xb5c as uint32,
        last: 0xb5d as uint32,
    },
    pg_unicode_range {
        first: 0xb5f as uint32,
        last: 0xb61 as uint32,
    },
    pg_unicode_range {
        first: 0xb62 as uint32,
        last: 0xb63 as uint32,
    },
    pg_unicode_range {
        first: 0xb71 as uint32,
        last: 0xb71 as uint32,
    },
    pg_unicode_range {
        first: 0xb82 as uint32,
        last: 0xb82 as uint32,
    },
    pg_unicode_range {
        first: 0xb83 as uint32,
        last: 0xb83 as uint32,
    },
    pg_unicode_range {
        first: 0xb85 as uint32,
        last: 0xb8a as uint32,
    },
    pg_unicode_range {
        first: 0xb8e as uint32,
        last: 0xb90 as uint32,
    },
    pg_unicode_range {
        first: 0xb92 as uint32,
        last: 0xb95 as uint32,
    },
    pg_unicode_range {
        first: 0xb99 as uint32,
        last: 0xb9a as uint32,
    },
    pg_unicode_range {
        first: 0xb9c as uint32,
        last: 0xb9c as uint32,
    },
    pg_unicode_range {
        first: 0xb9e as uint32,
        last: 0xb9f as uint32,
    },
    pg_unicode_range {
        first: 0xba3 as uint32,
        last: 0xba4 as uint32,
    },
    pg_unicode_range {
        first: 0xba8 as uint32,
        last: 0xbaa as uint32,
    },
    pg_unicode_range {
        first: 0xbae as uint32,
        last: 0xbb9 as uint32,
    },
    pg_unicode_range {
        first: 0xbbe as uint32,
        last: 0xbbf as uint32,
    },
    pg_unicode_range {
        first: 0xbc0 as uint32,
        last: 0xbc0 as uint32,
    },
    pg_unicode_range {
        first: 0xbc1 as uint32,
        last: 0xbc2 as uint32,
    },
    pg_unicode_range {
        first: 0xbc6 as uint32,
        last: 0xbc8 as uint32,
    },
    pg_unicode_range {
        first: 0xbca as uint32,
        last: 0xbcc as uint32,
    },
    pg_unicode_range {
        first: 0xbd0 as uint32,
        last: 0xbd0 as uint32,
    },
    pg_unicode_range {
        first: 0xbd7 as uint32,
        last: 0xbd7 as uint32,
    },
    pg_unicode_range {
        first: 0xc00 as uint32,
        last: 0xc00 as uint32,
    },
    pg_unicode_range {
        first: 0xc01 as uint32,
        last: 0xc03 as uint32,
    },
    pg_unicode_range {
        first: 0xc04 as uint32,
        last: 0xc04 as uint32,
    },
    pg_unicode_range {
        first: 0xc05 as uint32,
        last: 0xc0c as uint32,
    },
    pg_unicode_range {
        first: 0xc0e as uint32,
        last: 0xc10 as uint32,
    },
    pg_unicode_range {
        first: 0xc12 as uint32,
        last: 0xc28 as uint32,
    },
    pg_unicode_range {
        first: 0xc2a as uint32,
        last: 0xc39 as uint32,
    },
    pg_unicode_range {
        first: 0xc3d as uint32,
        last: 0xc3d as uint32,
    },
    pg_unicode_range {
        first: 0xc3e as uint32,
        last: 0xc40 as uint32,
    },
    pg_unicode_range {
        first: 0xc41 as uint32,
        last: 0xc44 as uint32,
    },
    pg_unicode_range {
        first: 0xc46 as uint32,
        last: 0xc48 as uint32,
    },
    pg_unicode_range {
        first: 0xc4a as uint32,
        last: 0xc4c as uint32,
    },
    pg_unicode_range {
        first: 0xc55 as uint32,
        last: 0xc56 as uint32,
    },
    pg_unicode_range {
        first: 0xc58 as uint32,
        last: 0xc5a as uint32,
    },
    pg_unicode_range {
        first: 0xc5d as uint32,
        last: 0xc5d as uint32,
    },
    pg_unicode_range {
        first: 0xc60 as uint32,
        last: 0xc61 as uint32,
    },
    pg_unicode_range {
        first: 0xc62 as uint32,
        last: 0xc63 as uint32,
    },
    pg_unicode_range {
        first: 0xc80 as uint32,
        last: 0xc80 as uint32,
    },
    pg_unicode_range {
        first: 0xc81 as uint32,
        last: 0xc81 as uint32,
    },
    pg_unicode_range {
        first: 0xc82 as uint32,
        last: 0xc83 as uint32,
    },
    pg_unicode_range {
        first: 0xc85 as uint32,
        last: 0xc8c as uint32,
    },
    pg_unicode_range {
        first: 0xc8e as uint32,
        last: 0xc90 as uint32,
    },
    pg_unicode_range {
        first: 0xc92 as uint32,
        last: 0xca8 as uint32,
    },
    pg_unicode_range {
        first: 0xcaa as uint32,
        last: 0xcb3 as uint32,
    },
    pg_unicode_range {
        first: 0xcb5 as uint32,
        last: 0xcb9 as uint32,
    },
    pg_unicode_range {
        first: 0xcbd as uint32,
        last: 0xcbd as uint32,
    },
    pg_unicode_range {
        first: 0xcbe as uint32,
        last: 0xcbe as uint32,
    },
    pg_unicode_range {
        first: 0xcbf as uint32,
        last: 0xcbf as uint32,
    },
    pg_unicode_range {
        first: 0xcc0 as uint32,
        last: 0xcc4 as uint32,
    },
    pg_unicode_range {
        first: 0xcc6 as uint32,
        last: 0xcc6 as uint32,
    },
    pg_unicode_range {
        first: 0xcc7 as uint32,
        last: 0xcc8 as uint32,
    },
    pg_unicode_range {
        first: 0xcca as uint32,
        last: 0xccb as uint32,
    },
    pg_unicode_range {
        first: 0xccc as uint32,
        last: 0xccc as uint32,
    },
    pg_unicode_range {
        first: 0xcd5 as uint32,
        last: 0xcd6 as uint32,
    },
    pg_unicode_range {
        first: 0xcdd as uint32,
        last: 0xcde as uint32,
    },
    pg_unicode_range {
        first: 0xce0 as uint32,
        last: 0xce1 as uint32,
    },
    pg_unicode_range {
        first: 0xce2 as uint32,
        last: 0xce3 as uint32,
    },
    pg_unicode_range {
        first: 0xcf1 as uint32,
        last: 0xcf2 as uint32,
    },
    pg_unicode_range {
        first: 0xcf3 as uint32,
        last: 0xcf3 as uint32,
    },
    pg_unicode_range {
        first: 0xd00 as uint32,
        last: 0xd01 as uint32,
    },
    pg_unicode_range {
        first: 0xd02 as uint32,
        last: 0xd03 as uint32,
    },
    pg_unicode_range {
        first: 0xd04 as uint32,
        last: 0xd0c as uint32,
    },
    pg_unicode_range {
        first: 0xd0e as uint32,
        last: 0xd10 as uint32,
    },
    pg_unicode_range {
        first: 0xd12 as uint32,
        last: 0xd3a as uint32,
    },
    pg_unicode_range {
        first: 0xd3d as uint32,
        last: 0xd3d as uint32,
    },
    pg_unicode_range {
        first: 0xd3e as uint32,
        last: 0xd40 as uint32,
    },
    pg_unicode_range {
        first: 0xd41 as uint32,
        last: 0xd44 as uint32,
    },
    pg_unicode_range {
        first: 0xd46 as uint32,
        last: 0xd48 as uint32,
    },
    pg_unicode_range {
        first: 0xd4a as uint32,
        last: 0xd4c as uint32,
    },
    pg_unicode_range {
        first: 0xd4e as uint32,
        last: 0xd4e as uint32,
    },
    pg_unicode_range {
        first: 0xd54 as uint32,
        last: 0xd56 as uint32,
    },
    pg_unicode_range {
        first: 0xd57 as uint32,
        last: 0xd57 as uint32,
    },
    pg_unicode_range {
        first: 0xd5f as uint32,
        last: 0xd61 as uint32,
    },
    pg_unicode_range {
        first: 0xd62 as uint32,
        last: 0xd63 as uint32,
    },
    pg_unicode_range {
        first: 0xd7a as uint32,
        last: 0xd7f as uint32,
    },
    pg_unicode_range {
        first: 0xd81 as uint32,
        last: 0xd81 as uint32,
    },
    pg_unicode_range {
        first: 0xd82 as uint32,
        last: 0xd83 as uint32,
    },
    pg_unicode_range {
        first: 0xd85 as uint32,
        last: 0xd96 as uint32,
    },
    pg_unicode_range {
        first: 0xd9a as uint32,
        last: 0xdb1 as uint32,
    },
    pg_unicode_range {
        first: 0xdb3 as uint32,
        last: 0xdbb as uint32,
    },
    pg_unicode_range {
        first: 0xdbd as uint32,
        last: 0xdbd as uint32,
    },
    pg_unicode_range {
        first: 0xdc0 as uint32,
        last: 0xdc6 as uint32,
    },
    pg_unicode_range {
        first: 0xdcf as uint32,
        last: 0xdd1 as uint32,
    },
    pg_unicode_range {
        first: 0xdd2 as uint32,
        last: 0xdd4 as uint32,
    },
    pg_unicode_range {
        first: 0xdd6 as uint32,
        last: 0xdd6 as uint32,
    },
    pg_unicode_range {
        first: 0xdd8 as uint32,
        last: 0xddf as uint32,
    },
    pg_unicode_range {
        first: 0xdf2 as uint32,
        last: 0xdf3 as uint32,
    },
    pg_unicode_range {
        first: 0xe01 as uint32,
        last: 0xe30 as uint32,
    },
    pg_unicode_range {
        first: 0xe31 as uint32,
        last: 0xe31 as uint32,
    },
    pg_unicode_range {
        first: 0xe32 as uint32,
        last: 0xe33 as uint32,
    },
    pg_unicode_range {
        first: 0xe34 as uint32,
        last: 0xe3a as uint32,
    },
    pg_unicode_range {
        first: 0xe40 as uint32,
        last: 0xe45 as uint32,
    },
    pg_unicode_range {
        first: 0xe46 as uint32,
        last: 0xe46 as uint32,
    },
    pg_unicode_range {
        first: 0xe4d as uint32,
        last: 0xe4d as uint32,
    },
    pg_unicode_range {
        first: 0xe81 as uint32,
        last: 0xe82 as uint32,
    },
    pg_unicode_range {
        first: 0xe84 as uint32,
        last: 0xe84 as uint32,
    },
    pg_unicode_range {
        first: 0xe86 as uint32,
        last: 0xe8a as uint32,
    },
    pg_unicode_range {
        first: 0xe8c as uint32,
        last: 0xea3 as uint32,
    },
    pg_unicode_range {
        first: 0xea5 as uint32,
        last: 0xea5 as uint32,
    },
    pg_unicode_range {
        first: 0xea7 as uint32,
        last: 0xeb0 as uint32,
    },
    pg_unicode_range {
        first: 0xeb1 as uint32,
        last: 0xeb1 as uint32,
    },
    pg_unicode_range {
        first: 0xeb2 as uint32,
        last: 0xeb3 as uint32,
    },
    pg_unicode_range {
        first: 0xeb4 as uint32,
        last: 0xeb9 as uint32,
    },
    pg_unicode_range {
        first: 0xebb as uint32,
        last: 0xebc as uint32,
    },
    pg_unicode_range {
        first: 0xebd as uint32,
        last: 0xebd as uint32,
    },
    pg_unicode_range {
        first: 0xec0 as uint32,
        last: 0xec4 as uint32,
    },
    pg_unicode_range {
        first: 0xec6 as uint32,
        last: 0xec6 as uint32,
    },
    pg_unicode_range {
        first: 0xecd as uint32,
        last: 0xecd as uint32,
    },
    pg_unicode_range {
        first: 0xedc as uint32,
        last: 0xedf as uint32,
    },
    pg_unicode_range {
        first: 0xf00 as uint32,
        last: 0xf00 as uint32,
    },
    pg_unicode_range {
        first: 0xf40 as uint32,
        last: 0xf47 as uint32,
    },
    pg_unicode_range {
        first: 0xf49 as uint32,
        last: 0xf6c as uint32,
    },
    pg_unicode_range {
        first: 0xf71 as uint32,
        last: 0xf7e as uint32,
    },
    pg_unicode_range {
        first: 0xf7f as uint32,
        last: 0xf7f as uint32,
    },
    pg_unicode_range {
        first: 0xf80 as uint32,
        last: 0xf83 as uint32,
    },
    pg_unicode_range {
        first: 0xf88 as uint32,
        last: 0xf8c as uint32,
    },
    pg_unicode_range {
        first: 0xf8d as uint32,
        last: 0xf97 as uint32,
    },
    pg_unicode_range {
        first: 0xf99 as uint32,
        last: 0xfbc as uint32,
    },
    pg_unicode_range {
        first: 0x1000 as uint32,
        last: 0x102a as uint32,
    },
    pg_unicode_range {
        first: 0x102b as uint32,
        last: 0x102c as uint32,
    },
    pg_unicode_range {
        first: 0x102d as uint32,
        last: 0x1030 as uint32,
    },
    pg_unicode_range {
        first: 0x1031 as uint32,
        last: 0x1031 as uint32,
    },
    pg_unicode_range {
        first: 0x1032 as uint32,
        last: 0x1036 as uint32,
    },
    pg_unicode_range {
        first: 0x1038 as uint32,
        last: 0x1038 as uint32,
    },
    pg_unicode_range {
        first: 0x103b as uint32,
        last: 0x103c as uint32,
    },
    pg_unicode_range {
        first: 0x103d as uint32,
        last: 0x103e as uint32,
    },
    pg_unicode_range {
        first: 0x103f as uint32,
        last: 0x103f as uint32,
    },
    pg_unicode_range {
        first: 0x1050 as uint32,
        last: 0x1055 as uint32,
    },
    pg_unicode_range {
        first: 0x1056 as uint32,
        last: 0x1057 as uint32,
    },
    pg_unicode_range {
        first: 0x1058 as uint32,
        last: 0x1059 as uint32,
    },
    pg_unicode_range {
        first: 0x105a as uint32,
        last: 0x105d as uint32,
    },
    pg_unicode_range {
        first: 0x105e as uint32,
        last: 0x1060 as uint32,
    },
    pg_unicode_range {
        first: 0x1061 as uint32,
        last: 0x1061 as uint32,
    },
    pg_unicode_range {
        first: 0x1062 as uint32,
        last: 0x1064 as uint32,
    },
    pg_unicode_range {
        first: 0x1065 as uint32,
        last: 0x1066 as uint32,
    },
    pg_unicode_range {
        first: 0x1067 as uint32,
        last: 0x106d as uint32,
    },
    pg_unicode_range {
        first: 0x106e as uint32,
        last: 0x1070 as uint32,
    },
    pg_unicode_range {
        first: 0x1071 as uint32,
        last: 0x1074 as uint32,
    },
    pg_unicode_range {
        first: 0x1075 as uint32,
        last: 0x1081 as uint32,
    },
    pg_unicode_range {
        first: 0x1082 as uint32,
        last: 0x1082 as uint32,
    },
    pg_unicode_range {
        first: 0x1083 as uint32,
        last: 0x1084 as uint32,
    },
    pg_unicode_range {
        first: 0x1085 as uint32,
        last: 0x1086 as uint32,
    },
    pg_unicode_range {
        first: 0x1087 as uint32,
        last: 0x108c as uint32,
    },
    pg_unicode_range {
        first: 0x108d as uint32,
        last: 0x108d as uint32,
    },
    pg_unicode_range {
        first: 0x108e as uint32,
        last: 0x108e as uint32,
    },
    pg_unicode_range {
        first: 0x108f as uint32,
        last: 0x108f as uint32,
    },
    pg_unicode_range {
        first: 0x109a as uint32,
        last: 0x109c as uint32,
    },
    pg_unicode_range {
        first: 0x109d as uint32,
        last: 0x109d as uint32,
    },
    pg_unicode_range {
        first: 0x10a0 as uint32,
        last: 0x10c5 as uint32,
    },
    pg_unicode_range {
        first: 0x10c7 as uint32,
        last: 0x10c7 as uint32,
    },
    pg_unicode_range {
        first: 0x10cd as uint32,
        last: 0x10cd as uint32,
    },
    pg_unicode_range {
        first: 0x10d0 as uint32,
        last: 0x10fa as uint32,
    },
    pg_unicode_range {
        first: 0x10fc as uint32,
        last: 0x10fc as uint32,
    },
    pg_unicode_range {
        first: 0x10fd as uint32,
        last: 0x10ff as uint32,
    },
    pg_unicode_range {
        first: 0x1100 as uint32,
        last: 0x1248 as uint32,
    },
    pg_unicode_range {
        first: 0x124a as uint32,
        last: 0x124d as uint32,
    },
    pg_unicode_range {
        first: 0x1250 as uint32,
        last: 0x1256 as uint32,
    },
    pg_unicode_range {
        first: 0x1258 as uint32,
        last: 0x1258 as uint32,
    },
    pg_unicode_range {
        first: 0x125a as uint32,
        last: 0x125d as uint32,
    },
    pg_unicode_range {
        first: 0x1260 as uint32,
        last: 0x1288 as uint32,
    },
    pg_unicode_range {
        first: 0x128a as uint32,
        last: 0x128d as uint32,
    },
    pg_unicode_range {
        first: 0x1290 as uint32,
        last: 0x12b0 as uint32,
    },
    pg_unicode_range {
        first: 0x12b2 as uint32,
        last: 0x12b5 as uint32,
    },
    pg_unicode_range {
        first: 0x12b8 as uint32,
        last: 0x12be as uint32,
    },
    pg_unicode_range {
        first: 0x12c0 as uint32,
        last: 0x12c0 as uint32,
    },
    pg_unicode_range {
        first: 0x12c2 as uint32,
        last: 0x12c5 as uint32,
    },
    pg_unicode_range {
        first: 0x12c8 as uint32,
        last: 0x12d6 as uint32,
    },
    pg_unicode_range {
        first: 0x12d8 as uint32,
        last: 0x1310 as uint32,
    },
    pg_unicode_range {
        first: 0x1312 as uint32,
        last: 0x1315 as uint32,
    },
    pg_unicode_range {
        first: 0x1318 as uint32,
        last: 0x135a as uint32,
    },
    pg_unicode_range {
        first: 0x1380 as uint32,
        last: 0x138f as uint32,
    },
    pg_unicode_range {
        first: 0x13a0 as uint32,
        last: 0x13f5 as uint32,
    },
    pg_unicode_range {
        first: 0x13f8 as uint32,
        last: 0x13fd as uint32,
    },
    pg_unicode_range {
        first: 0x1401 as uint32,
        last: 0x166c as uint32,
    },
    pg_unicode_range {
        first: 0x166f as uint32,
        last: 0x167f as uint32,
    },
    pg_unicode_range {
        first: 0x1681 as uint32,
        last: 0x169a as uint32,
    },
    pg_unicode_range {
        first: 0x16a0 as uint32,
        last: 0x16ea as uint32,
    },
    pg_unicode_range {
        first: 0x16ee as uint32,
        last: 0x16f0 as uint32,
    },
    pg_unicode_range {
        first: 0x16f1 as uint32,
        last: 0x16f8 as uint32,
    },
    pg_unicode_range {
        first: 0x1700 as uint32,
        last: 0x1711 as uint32,
    },
    pg_unicode_range {
        first: 0x1712 as uint32,
        last: 0x1713 as uint32,
    },
    pg_unicode_range {
        first: 0x171f as uint32,
        last: 0x1731 as uint32,
    },
    pg_unicode_range {
        first: 0x1732 as uint32,
        last: 0x1733 as uint32,
    },
    pg_unicode_range {
        first: 0x1740 as uint32,
        last: 0x1751 as uint32,
    },
    pg_unicode_range {
        first: 0x1752 as uint32,
        last: 0x1753 as uint32,
    },
    pg_unicode_range {
        first: 0x1760 as uint32,
        last: 0x176c as uint32,
    },
    pg_unicode_range {
        first: 0x176e as uint32,
        last: 0x1770 as uint32,
    },
    pg_unicode_range {
        first: 0x1772 as uint32,
        last: 0x1773 as uint32,
    },
    pg_unicode_range {
        first: 0x1780 as uint32,
        last: 0x17b3 as uint32,
    },
    pg_unicode_range {
        first: 0x17b6 as uint32,
        last: 0x17b6 as uint32,
    },
    pg_unicode_range {
        first: 0x17b7 as uint32,
        last: 0x17bd as uint32,
    },
    pg_unicode_range {
        first: 0x17be as uint32,
        last: 0x17c5 as uint32,
    },
    pg_unicode_range {
        first: 0x17c6 as uint32,
        last: 0x17c6 as uint32,
    },
    pg_unicode_range {
        first: 0x17c7 as uint32,
        last: 0x17c8 as uint32,
    },
    pg_unicode_range {
        first: 0x17d7 as uint32,
        last: 0x17d7 as uint32,
    },
    pg_unicode_range {
        first: 0x17dc as uint32,
        last: 0x17dc as uint32,
    },
    pg_unicode_range {
        first: 0x1820 as uint32,
        last: 0x1842 as uint32,
    },
    pg_unicode_range {
        first: 0x1843 as uint32,
        last: 0x1843 as uint32,
    },
    pg_unicode_range {
        first: 0x1844 as uint32,
        last: 0x1878 as uint32,
    },
    pg_unicode_range {
        first: 0x1880 as uint32,
        last: 0x1884 as uint32,
    },
    pg_unicode_range {
        first: 0x1885 as uint32,
        last: 0x1886 as uint32,
    },
    pg_unicode_range {
        first: 0x1887 as uint32,
        last: 0x18a8 as uint32,
    },
    pg_unicode_range {
        first: 0x18a9 as uint32,
        last: 0x18a9 as uint32,
    },
    pg_unicode_range {
        first: 0x18aa as uint32,
        last: 0x18aa as uint32,
    },
    pg_unicode_range {
        first: 0x18b0 as uint32,
        last: 0x18f5 as uint32,
    },
    pg_unicode_range {
        first: 0x1900 as uint32,
        last: 0x191e as uint32,
    },
    pg_unicode_range {
        first: 0x1920 as uint32,
        last: 0x1922 as uint32,
    },
    pg_unicode_range {
        first: 0x1923 as uint32,
        last: 0x1926 as uint32,
    },
    pg_unicode_range {
        first: 0x1927 as uint32,
        last: 0x1928 as uint32,
    },
    pg_unicode_range {
        first: 0x1929 as uint32,
        last: 0x192b as uint32,
    },
    pg_unicode_range {
        first: 0x1930 as uint32,
        last: 0x1931 as uint32,
    },
    pg_unicode_range {
        first: 0x1932 as uint32,
        last: 0x1932 as uint32,
    },
    pg_unicode_range {
        first: 0x1933 as uint32,
        last: 0x1938 as uint32,
    },
    pg_unicode_range {
        first: 0x1950 as uint32,
        last: 0x196d as uint32,
    },
    pg_unicode_range {
        first: 0x1970 as uint32,
        last: 0x1974 as uint32,
    },
    pg_unicode_range {
        first: 0x1980 as uint32,
        last: 0x19ab as uint32,
    },
    pg_unicode_range {
        first: 0x19b0 as uint32,
        last: 0x19c9 as uint32,
    },
    pg_unicode_range {
        first: 0x1a00 as uint32,
        last: 0x1a16 as uint32,
    },
    pg_unicode_range {
        first: 0x1a17 as uint32,
        last: 0x1a18 as uint32,
    },
    pg_unicode_range {
        first: 0x1a19 as uint32,
        last: 0x1a1a as uint32,
    },
    pg_unicode_range {
        first: 0x1a1b as uint32,
        last: 0x1a1b as uint32,
    },
    pg_unicode_range {
        first: 0x1a20 as uint32,
        last: 0x1a54 as uint32,
    },
    pg_unicode_range {
        first: 0x1a55 as uint32,
        last: 0x1a55 as uint32,
    },
    pg_unicode_range {
        first: 0x1a56 as uint32,
        last: 0x1a56 as uint32,
    },
    pg_unicode_range {
        first: 0x1a57 as uint32,
        last: 0x1a57 as uint32,
    },
    pg_unicode_range {
        first: 0x1a58 as uint32,
        last: 0x1a5e as uint32,
    },
    pg_unicode_range {
        first: 0x1a61 as uint32,
        last: 0x1a61 as uint32,
    },
    pg_unicode_range {
        first: 0x1a62 as uint32,
        last: 0x1a62 as uint32,
    },
    pg_unicode_range {
        first: 0x1a63 as uint32,
        last: 0x1a64 as uint32,
    },
    pg_unicode_range {
        first: 0x1a65 as uint32,
        last: 0x1a6c as uint32,
    },
    pg_unicode_range {
        first: 0x1a6d as uint32,
        last: 0x1a72 as uint32,
    },
    pg_unicode_range {
        first: 0x1a73 as uint32,
        last: 0x1a74 as uint32,
    },
    pg_unicode_range {
        first: 0x1aa7 as uint32,
        last: 0x1aa7 as uint32,
    },
    pg_unicode_range {
        first: 0x1abf as uint32,
        last: 0x1ac0 as uint32,
    },
    pg_unicode_range {
        first: 0x1acc as uint32,
        last: 0x1ace as uint32,
    },
    pg_unicode_range {
        first: 0x1b00 as uint32,
        last: 0x1b03 as uint32,
    },
    pg_unicode_range {
        first: 0x1b04 as uint32,
        last: 0x1b04 as uint32,
    },
    pg_unicode_range {
        first: 0x1b05 as uint32,
        last: 0x1b33 as uint32,
    },
    pg_unicode_range {
        first: 0x1b35 as uint32,
        last: 0x1b35 as uint32,
    },
    pg_unicode_range {
        first: 0x1b36 as uint32,
        last: 0x1b3a as uint32,
    },
    pg_unicode_range {
        first: 0x1b3b as uint32,
        last: 0x1b3b as uint32,
    },
    pg_unicode_range {
        first: 0x1b3c as uint32,
        last: 0x1b3c as uint32,
    },
    pg_unicode_range {
        first: 0x1b3d as uint32,
        last: 0x1b41 as uint32,
    },
    pg_unicode_range {
        first: 0x1b42 as uint32,
        last: 0x1b42 as uint32,
    },
    pg_unicode_range {
        first: 0x1b43 as uint32,
        last: 0x1b43 as uint32,
    },
    pg_unicode_range {
        first: 0x1b45 as uint32,
        last: 0x1b4c as uint32,
    },
    pg_unicode_range {
        first: 0x1b80 as uint32,
        last: 0x1b81 as uint32,
    },
    pg_unicode_range {
        first: 0x1b82 as uint32,
        last: 0x1b82 as uint32,
    },
    pg_unicode_range {
        first: 0x1b83 as uint32,
        last: 0x1ba0 as uint32,
    },
    pg_unicode_range {
        first: 0x1ba1 as uint32,
        last: 0x1ba1 as uint32,
    },
    pg_unicode_range {
        first: 0x1ba2 as uint32,
        last: 0x1ba5 as uint32,
    },
    pg_unicode_range {
        first: 0x1ba6 as uint32,
        last: 0x1ba7 as uint32,
    },
    pg_unicode_range {
        first: 0x1ba8 as uint32,
        last: 0x1ba9 as uint32,
    },
    pg_unicode_range {
        first: 0x1bac as uint32,
        last: 0x1bad as uint32,
    },
    pg_unicode_range {
        first: 0x1bae as uint32,
        last: 0x1baf as uint32,
    },
    pg_unicode_range {
        first: 0x1bba as uint32,
        last: 0x1be5 as uint32,
    },
    pg_unicode_range {
        first: 0x1be7 as uint32,
        last: 0x1be7 as uint32,
    },
    pg_unicode_range {
        first: 0x1be8 as uint32,
        last: 0x1be9 as uint32,
    },
    pg_unicode_range {
        first: 0x1bea as uint32,
        last: 0x1bec as uint32,
    },
    pg_unicode_range {
        first: 0x1bed as uint32,
        last: 0x1bed as uint32,
    },
    pg_unicode_range {
        first: 0x1bee as uint32,
        last: 0x1bee as uint32,
    },
    pg_unicode_range {
        first: 0x1bef as uint32,
        last: 0x1bf1 as uint32,
    },
    pg_unicode_range {
        first: 0x1c00 as uint32,
        last: 0x1c23 as uint32,
    },
    pg_unicode_range {
        first: 0x1c24 as uint32,
        last: 0x1c2b as uint32,
    },
    pg_unicode_range {
        first: 0x1c2c as uint32,
        last: 0x1c33 as uint32,
    },
    pg_unicode_range {
        first: 0x1c34 as uint32,
        last: 0x1c35 as uint32,
    },
    pg_unicode_range {
        first: 0x1c36 as uint32,
        last: 0x1c36 as uint32,
    },
    pg_unicode_range {
        first: 0x1c4d as uint32,
        last: 0x1c4f as uint32,
    },
    pg_unicode_range {
        first: 0x1c5a as uint32,
        last: 0x1c77 as uint32,
    },
    pg_unicode_range {
        first: 0x1c78 as uint32,
        last: 0x1c7d as uint32,
    },
    pg_unicode_range {
        first: 0x1c80 as uint32,
        last: 0x1c8a as uint32,
    },
    pg_unicode_range {
        first: 0x1c90 as uint32,
        last: 0x1cba as uint32,
    },
    pg_unicode_range {
        first: 0x1cbd as uint32,
        last: 0x1cbf as uint32,
    },
    pg_unicode_range {
        first: 0x1ce9 as uint32,
        last: 0x1cec as uint32,
    },
    pg_unicode_range {
        first: 0x1cee as uint32,
        last: 0x1cf3 as uint32,
    },
    pg_unicode_range {
        first: 0x1cf5 as uint32,
        last: 0x1cf6 as uint32,
    },
    pg_unicode_range {
        first: 0x1cfa as uint32,
        last: 0x1cfa as uint32,
    },
    pg_unicode_range {
        first: 0x1d00 as uint32,
        last: 0x1d2b as uint32,
    },
    pg_unicode_range {
        first: 0x1d2c as uint32,
        last: 0x1d6a as uint32,
    },
    pg_unicode_range {
        first: 0x1d6b as uint32,
        last: 0x1d77 as uint32,
    },
    pg_unicode_range {
        first: 0x1d78 as uint32,
        last: 0x1d78 as uint32,
    },
    pg_unicode_range {
        first: 0x1d79 as uint32,
        last: 0x1d9a as uint32,
    },
    pg_unicode_range {
        first: 0x1d9b as uint32,
        last: 0x1dbf as uint32,
    },
    pg_unicode_range {
        first: 0x1dd3 as uint32,
        last: 0x1df4 as uint32,
    },
    pg_unicode_range {
        first: 0x1e00 as uint32,
        last: 0x1f15 as uint32,
    },
    pg_unicode_range {
        first: 0x1f18 as uint32,
        last: 0x1f1d as uint32,
    },
    pg_unicode_range {
        first: 0x1f20 as uint32,
        last: 0x1f45 as uint32,
    },
    pg_unicode_range {
        first: 0x1f48 as uint32,
        last: 0x1f4d as uint32,
    },
    pg_unicode_range {
        first: 0x1f50 as uint32,
        last: 0x1f57 as uint32,
    },
    pg_unicode_range {
        first: 0x1f59 as uint32,
        last: 0x1f59 as uint32,
    },
    pg_unicode_range {
        first: 0x1f5b as uint32,
        last: 0x1f5b as uint32,
    },
    pg_unicode_range {
        first: 0x1f5d as uint32,
        last: 0x1f5d as uint32,
    },
    pg_unicode_range {
        first: 0x1f5f as uint32,
        last: 0x1f7d as uint32,
    },
    pg_unicode_range {
        first: 0x1f80 as uint32,
        last: 0x1fb4 as uint32,
    },
    pg_unicode_range {
        first: 0x1fb6 as uint32,
        last: 0x1fbc as uint32,
    },
    pg_unicode_range {
        first: 0x1fbe as uint32,
        last: 0x1fbe as uint32,
    },
    pg_unicode_range {
        first: 0x1fc2 as uint32,
        last: 0x1fc4 as uint32,
    },
    pg_unicode_range {
        first: 0x1fc6 as uint32,
        last: 0x1fcc as uint32,
    },
    pg_unicode_range {
        first: 0x1fd0 as uint32,
        last: 0x1fd3 as uint32,
    },
    pg_unicode_range {
        first: 0x1fd6 as uint32,
        last: 0x1fdb as uint32,
    },
    pg_unicode_range {
        first: 0x1fe0 as uint32,
        last: 0x1fec as uint32,
    },
    pg_unicode_range {
        first: 0x1ff2 as uint32,
        last: 0x1ff4 as uint32,
    },
    pg_unicode_range {
        first: 0x1ff6 as uint32,
        last: 0x1ffc as uint32,
    },
    pg_unicode_range {
        first: 0x2071 as uint32,
        last: 0x2071 as uint32,
    },
    pg_unicode_range {
        first: 0x207f as uint32,
        last: 0x207f as uint32,
    },
    pg_unicode_range {
        first: 0x2090 as uint32,
        last: 0x209c as uint32,
    },
    pg_unicode_range {
        first: 0x2102 as uint32,
        last: 0x2102 as uint32,
    },
    pg_unicode_range {
        first: 0x2107 as uint32,
        last: 0x2107 as uint32,
    },
    pg_unicode_range {
        first: 0x210a as uint32,
        last: 0x2113 as uint32,
    },
    pg_unicode_range {
        first: 0x2115 as uint32,
        last: 0x2115 as uint32,
    },
    pg_unicode_range {
        first: 0x2119 as uint32,
        last: 0x211d as uint32,
    },
    pg_unicode_range {
        first: 0x2124 as uint32,
        last: 0x2124 as uint32,
    },
    pg_unicode_range {
        first: 0x2126 as uint32,
        last: 0x2126 as uint32,
    },
    pg_unicode_range {
        first: 0x2128 as uint32,
        last: 0x2128 as uint32,
    },
    pg_unicode_range {
        first: 0x212a as uint32,
        last: 0x212d as uint32,
    },
    pg_unicode_range {
        first: 0x212f as uint32,
        last: 0x2134 as uint32,
    },
    pg_unicode_range {
        first: 0x2135 as uint32,
        last: 0x2138 as uint32,
    },
    pg_unicode_range {
        first: 0x2139 as uint32,
        last: 0x2139 as uint32,
    },
    pg_unicode_range {
        first: 0x213c as uint32,
        last: 0x213f as uint32,
    },
    pg_unicode_range {
        first: 0x2145 as uint32,
        last: 0x2149 as uint32,
    },
    pg_unicode_range {
        first: 0x214e as uint32,
        last: 0x214e as uint32,
    },
    pg_unicode_range {
        first: 0x2160 as uint32,
        last: 0x2182 as uint32,
    },
    pg_unicode_range {
        first: 0x2183 as uint32,
        last: 0x2184 as uint32,
    },
    pg_unicode_range {
        first: 0x2185 as uint32,
        last: 0x2188 as uint32,
    },
    pg_unicode_range {
        first: 0x24b6 as uint32,
        last: 0x24e9 as uint32,
    },
    pg_unicode_range {
        first: 0x2c00 as uint32,
        last: 0x2c7b as uint32,
    },
    pg_unicode_range {
        first: 0x2c7c as uint32,
        last: 0x2c7d as uint32,
    },
    pg_unicode_range {
        first: 0x2c7e as uint32,
        last: 0x2ce4 as uint32,
    },
    pg_unicode_range {
        first: 0x2ceb as uint32,
        last: 0x2cee as uint32,
    },
    pg_unicode_range {
        first: 0x2cf2 as uint32,
        last: 0x2cf3 as uint32,
    },
    pg_unicode_range {
        first: 0x2d00 as uint32,
        last: 0x2d25 as uint32,
    },
    pg_unicode_range {
        first: 0x2d27 as uint32,
        last: 0x2d27 as uint32,
    },
    pg_unicode_range {
        first: 0x2d2d as uint32,
        last: 0x2d2d as uint32,
    },
    pg_unicode_range {
        first: 0x2d30 as uint32,
        last: 0x2d67 as uint32,
    },
    pg_unicode_range {
        first: 0x2d6f as uint32,
        last: 0x2d6f as uint32,
    },
    pg_unicode_range {
        first: 0x2d80 as uint32,
        last: 0x2d96 as uint32,
    },
    pg_unicode_range {
        first: 0x2da0 as uint32,
        last: 0x2da6 as uint32,
    },
    pg_unicode_range {
        first: 0x2da8 as uint32,
        last: 0x2dae as uint32,
    },
    pg_unicode_range {
        first: 0x2db0 as uint32,
        last: 0x2db6 as uint32,
    },
    pg_unicode_range {
        first: 0x2db8 as uint32,
        last: 0x2dbe as uint32,
    },
    pg_unicode_range {
        first: 0x2dc0 as uint32,
        last: 0x2dc6 as uint32,
    },
    pg_unicode_range {
        first: 0x2dc8 as uint32,
        last: 0x2dce as uint32,
    },
    pg_unicode_range {
        first: 0x2dd0 as uint32,
        last: 0x2dd6 as uint32,
    },
    pg_unicode_range {
        first: 0x2dd8 as uint32,
        last: 0x2dde as uint32,
    },
    pg_unicode_range {
        first: 0x2de0 as uint32,
        last: 0x2dff as uint32,
    },
    pg_unicode_range {
        first: 0x2e2f as uint32,
        last: 0x2e2f as uint32,
    },
    pg_unicode_range {
        first: 0x3005 as uint32,
        last: 0x3005 as uint32,
    },
    pg_unicode_range {
        first: 0x3006 as uint32,
        last: 0x3006 as uint32,
    },
    pg_unicode_range {
        first: 0x3007 as uint32,
        last: 0x3007 as uint32,
    },
    pg_unicode_range {
        first: 0x3021 as uint32,
        last: 0x3029 as uint32,
    },
    pg_unicode_range {
        first: 0x3031 as uint32,
        last: 0x3035 as uint32,
    },
    pg_unicode_range {
        first: 0x3038 as uint32,
        last: 0x303a as uint32,
    },
    pg_unicode_range {
        first: 0x303b as uint32,
        last: 0x303b as uint32,
    },
    pg_unicode_range {
        first: 0x303c as uint32,
        last: 0x303c as uint32,
    },
    pg_unicode_range {
        first: 0x3041 as uint32,
        last: 0x3096 as uint32,
    },
    pg_unicode_range {
        first: 0x309d as uint32,
        last: 0x309e as uint32,
    },
    pg_unicode_range {
        first: 0x309f as uint32,
        last: 0x309f as uint32,
    },
    pg_unicode_range {
        first: 0x30a1 as uint32,
        last: 0x30fa as uint32,
    },
    pg_unicode_range {
        first: 0x30fc as uint32,
        last: 0x30fe as uint32,
    },
    pg_unicode_range {
        first: 0x30ff as uint32,
        last: 0x30ff as uint32,
    },
    pg_unicode_range {
        first: 0x3105 as uint32,
        last: 0x312f as uint32,
    },
    pg_unicode_range {
        first: 0x3131 as uint32,
        last: 0x318e as uint32,
    },
    pg_unicode_range {
        first: 0x31a0 as uint32,
        last: 0x31bf as uint32,
    },
    pg_unicode_range {
        first: 0x31f0 as uint32,
        last: 0x31ff as uint32,
    },
    pg_unicode_range {
        first: 0x3400 as uint32,
        last: 0x4dbf as uint32,
    },
    pg_unicode_range {
        first: 0x4e00 as uint32,
        last: 0xa014 as uint32,
    },
    pg_unicode_range {
        first: 0xa015 as uint32,
        last: 0xa015 as uint32,
    },
    pg_unicode_range {
        first: 0xa016 as uint32,
        last: 0xa48c as uint32,
    },
    pg_unicode_range {
        first: 0xa4d0 as uint32,
        last: 0xa4f7 as uint32,
    },
    pg_unicode_range {
        first: 0xa4f8 as uint32,
        last: 0xa4fd as uint32,
    },
    pg_unicode_range {
        first: 0xa500 as uint32,
        last: 0xa60b as uint32,
    },
    pg_unicode_range {
        first: 0xa60c as uint32,
        last: 0xa60c as uint32,
    },
    pg_unicode_range {
        first: 0xa610 as uint32,
        last: 0xa61f as uint32,
    },
    pg_unicode_range {
        first: 0xa62a as uint32,
        last: 0xa62b as uint32,
    },
    pg_unicode_range {
        first: 0xa640 as uint32,
        last: 0xa66d as uint32,
    },
    pg_unicode_range {
        first: 0xa66e as uint32,
        last: 0xa66e as uint32,
    },
    pg_unicode_range {
        first: 0xa674 as uint32,
        last: 0xa67b as uint32,
    },
    pg_unicode_range {
        first: 0xa67f as uint32,
        last: 0xa67f as uint32,
    },
    pg_unicode_range {
        first: 0xa680 as uint32,
        last: 0xa69b as uint32,
    },
    pg_unicode_range {
        first: 0xa69c as uint32,
        last: 0xa69d as uint32,
    },
    pg_unicode_range {
        first: 0xa69e as uint32,
        last: 0xa69f as uint32,
    },
    pg_unicode_range {
        first: 0xa6a0 as uint32,
        last: 0xa6e5 as uint32,
    },
    pg_unicode_range {
        first: 0xa6e6 as uint32,
        last: 0xa6ef as uint32,
    },
    pg_unicode_range {
        first: 0xa717 as uint32,
        last: 0xa71f as uint32,
    },
    pg_unicode_range {
        first: 0xa722 as uint32,
        last: 0xa76f as uint32,
    },
    pg_unicode_range {
        first: 0xa770 as uint32,
        last: 0xa770 as uint32,
    },
    pg_unicode_range {
        first: 0xa771 as uint32,
        last: 0xa787 as uint32,
    },
    pg_unicode_range {
        first: 0xa788 as uint32,
        last: 0xa788 as uint32,
    },
    pg_unicode_range {
        first: 0xa78b as uint32,
        last: 0xa78e as uint32,
    },
    pg_unicode_range {
        first: 0xa78f as uint32,
        last: 0xa78f as uint32,
    },
    pg_unicode_range {
        first: 0xa790 as uint32,
        last: 0xa7cd as uint32,
    },
    pg_unicode_range {
        first: 0xa7d0 as uint32,
        last: 0xa7d1 as uint32,
    },
    pg_unicode_range {
        first: 0xa7d3 as uint32,
        last: 0xa7d3 as uint32,
    },
    pg_unicode_range {
        first: 0xa7d5 as uint32,
        last: 0xa7dc as uint32,
    },
    pg_unicode_range {
        first: 0xa7f2 as uint32,
        last: 0xa7f4 as uint32,
    },
    pg_unicode_range {
        first: 0xa7f5 as uint32,
        last: 0xa7f6 as uint32,
    },
    pg_unicode_range {
        first: 0xa7f7 as uint32,
        last: 0xa7f7 as uint32,
    },
    pg_unicode_range {
        first: 0xa7f8 as uint32,
        last: 0xa7f9 as uint32,
    },
    pg_unicode_range {
        first: 0xa7fa as uint32,
        last: 0xa7fa as uint32,
    },
    pg_unicode_range {
        first: 0xa7fb as uint32,
        last: 0xa801 as uint32,
    },
    pg_unicode_range {
        first: 0xa802 as uint32,
        last: 0xa802 as uint32,
    },
    pg_unicode_range {
        first: 0xa803 as uint32,
        last: 0xa805 as uint32,
    },
    pg_unicode_range {
        first: 0xa807 as uint32,
        last: 0xa80a as uint32,
    },
    pg_unicode_range {
        first: 0xa80b as uint32,
        last: 0xa80b as uint32,
    },
    pg_unicode_range {
        first: 0xa80c as uint32,
        last: 0xa822 as uint32,
    },
    pg_unicode_range {
        first: 0xa823 as uint32,
        last: 0xa824 as uint32,
    },
    pg_unicode_range {
        first: 0xa825 as uint32,
        last: 0xa826 as uint32,
    },
    pg_unicode_range {
        first: 0xa827 as uint32,
        last: 0xa827 as uint32,
    },
    pg_unicode_range {
        first: 0xa840 as uint32,
        last: 0xa873 as uint32,
    },
    pg_unicode_range {
        first: 0xa880 as uint32,
        last: 0xa881 as uint32,
    },
    pg_unicode_range {
        first: 0xa882 as uint32,
        last: 0xa8b3 as uint32,
    },
    pg_unicode_range {
        first: 0xa8b4 as uint32,
        last: 0xa8c3 as uint32,
    },
    pg_unicode_range {
        first: 0xa8c5 as uint32,
        last: 0xa8c5 as uint32,
    },
    pg_unicode_range {
        first: 0xa8f2 as uint32,
        last: 0xa8f7 as uint32,
    },
    pg_unicode_range {
        first: 0xa8fb as uint32,
        last: 0xa8fb as uint32,
    },
    pg_unicode_range {
        first: 0xa8fd as uint32,
        last: 0xa8fe as uint32,
    },
    pg_unicode_range {
        first: 0xa8ff as uint32,
        last: 0xa8ff as uint32,
    },
    pg_unicode_range {
        first: 0xa90a as uint32,
        last: 0xa925 as uint32,
    },
    pg_unicode_range {
        first: 0xa926 as uint32,
        last: 0xa92a as uint32,
    },
    pg_unicode_range {
        first: 0xa930 as uint32,
        last: 0xa946 as uint32,
    },
    pg_unicode_range {
        first: 0xa947 as uint32,
        last: 0xa951 as uint32,
    },
    pg_unicode_range {
        first: 0xa952 as uint32,
        last: 0xa952 as uint32,
    },
    pg_unicode_range {
        first: 0xa960 as uint32,
        last: 0xa97c as uint32,
    },
    pg_unicode_range {
        first: 0xa980 as uint32,
        last: 0xa982 as uint32,
    },
    pg_unicode_range {
        first: 0xa983 as uint32,
        last: 0xa983 as uint32,
    },
    pg_unicode_range {
        first: 0xa984 as uint32,
        last: 0xa9b2 as uint32,
    },
    pg_unicode_range {
        first: 0xa9b4 as uint32,
        last: 0xa9b5 as uint32,
    },
    pg_unicode_range {
        first: 0xa9b6 as uint32,
        last: 0xa9b9 as uint32,
    },
    pg_unicode_range {
        first: 0xa9ba as uint32,
        last: 0xa9bb as uint32,
    },
    pg_unicode_range {
        first: 0xa9bc as uint32,
        last: 0xa9bd as uint32,
    },
    pg_unicode_range {
        first: 0xa9be as uint32,
        last: 0xa9bf as uint32,
    },
    pg_unicode_range {
        first: 0xa9cf as uint32,
        last: 0xa9cf as uint32,
    },
    pg_unicode_range {
        first: 0xa9e0 as uint32,
        last: 0xa9e4 as uint32,
    },
    pg_unicode_range {
        first: 0xa9e5 as uint32,
        last: 0xa9e5 as uint32,
    },
    pg_unicode_range {
        first: 0xa9e6 as uint32,
        last: 0xa9e6 as uint32,
    },
    pg_unicode_range {
        first: 0xa9e7 as uint32,
        last: 0xa9ef as uint32,
    },
    pg_unicode_range {
        first: 0xa9fa as uint32,
        last: 0xa9fe as uint32,
    },
    pg_unicode_range {
        first: 0xaa00 as uint32,
        last: 0xaa28 as uint32,
    },
    pg_unicode_range {
        first: 0xaa29 as uint32,
        last: 0xaa2e as uint32,
    },
    pg_unicode_range {
        first: 0xaa2f as uint32,
        last: 0xaa30 as uint32,
    },
    pg_unicode_range {
        first: 0xaa31 as uint32,
        last: 0xaa32 as uint32,
    },
    pg_unicode_range {
        first: 0xaa33 as uint32,
        last: 0xaa34 as uint32,
    },
    pg_unicode_range {
        first: 0xaa35 as uint32,
        last: 0xaa36 as uint32,
    },
    pg_unicode_range {
        first: 0xaa40 as uint32,
        last: 0xaa42 as uint32,
    },
    pg_unicode_range {
        first: 0xaa43 as uint32,
        last: 0xaa43 as uint32,
    },
    pg_unicode_range {
        first: 0xaa44 as uint32,
        last: 0xaa4b as uint32,
    },
    pg_unicode_range {
        first: 0xaa4c as uint32,
        last: 0xaa4c as uint32,
    },
    pg_unicode_range {
        first: 0xaa4d as uint32,
        last: 0xaa4d as uint32,
    },
    pg_unicode_range {
        first: 0xaa60 as uint32,
        last: 0xaa6f as uint32,
    },
    pg_unicode_range {
        first: 0xaa70 as uint32,
        last: 0xaa70 as uint32,
    },
    pg_unicode_range {
        first: 0xaa71 as uint32,
        last: 0xaa76 as uint32,
    },
    pg_unicode_range {
        first: 0xaa7a as uint32,
        last: 0xaa7a as uint32,
    },
    pg_unicode_range {
        first: 0xaa7b as uint32,
        last: 0xaa7b as uint32,
    },
    pg_unicode_range {
        first: 0xaa7c as uint32,
        last: 0xaa7c as uint32,
    },
    pg_unicode_range {
        first: 0xaa7d as uint32,
        last: 0xaa7d as uint32,
    },
    pg_unicode_range {
        first: 0xaa7e as uint32,
        last: 0xaaaf as uint32,
    },
    pg_unicode_range {
        first: 0xaab0 as uint32,
        last: 0xaab0 as uint32,
    },
    pg_unicode_range {
        first: 0xaab1 as uint32,
        last: 0xaab1 as uint32,
    },
    pg_unicode_range {
        first: 0xaab2 as uint32,
        last: 0xaab4 as uint32,
    },
    pg_unicode_range {
        first: 0xaab5 as uint32,
        last: 0xaab6 as uint32,
    },
    pg_unicode_range {
        first: 0xaab7 as uint32,
        last: 0xaab8 as uint32,
    },
    pg_unicode_range {
        first: 0xaab9 as uint32,
        last: 0xaabd as uint32,
    },
    pg_unicode_range {
        first: 0xaabe as uint32,
        last: 0xaabe as uint32,
    },
    pg_unicode_range {
        first: 0xaac0 as uint32,
        last: 0xaac0 as uint32,
    },
    pg_unicode_range {
        first: 0xaac2 as uint32,
        last: 0xaac2 as uint32,
    },
    pg_unicode_range {
        first: 0xaadb as uint32,
        last: 0xaadc as uint32,
    },
    pg_unicode_range {
        first: 0xaadd as uint32,
        last: 0xaadd as uint32,
    },
    pg_unicode_range {
        first: 0xaae0 as uint32,
        last: 0xaaea as uint32,
    },
    pg_unicode_range {
        first: 0xaaeb as uint32,
        last: 0xaaeb as uint32,
    },
    pg_unicode_range {
        first: 0xaaec as uint32,
        last: 0xaaed as uint32,
    },
    pg_unicode_range {
        first: 0xaaee as uint32,
        last: 0xaaef as uint32,
    },
    pg_unicode_range {
        first: 0xaaf2 as uint32,
        last: 0xaaf2 as uint32,
    },
    pg_unicode_range {
        first: 0xaaf3 as uint32,
        last: 0xaaf4 as uint32,
    },
    pg_unicode_range {
        first: 0xaaf5 as uint32,
        last: 0xaaf5 as uint32,
    },
    pg_unicode_range {
        first: 0xab01 as uint32,
        last: 0xab06 as uint32,
    },
    pg_unicode_range {
        first: 0xab09 as uint32,
        last: 0xab0e as uint32,
    },
    pg_unicode_range {
        first: 0xab11 as uint32,
        last: 0xab16 as uint32,
    },
    pg_unicode_range {
        first: 0xab20 as uint32,
        last: 0xab26 as uint32,
    },
    pg_unicode_range {
        first: 0xab28 as uint32,
        last: 0xab2e as uint32,
    },
    pg_unicode_range {
        first: 0xab30 as uint32,
        last: 0xab5a as uint32,
    },
    pg_unicode_range {
        first: 0xab5c as uint32,
        last: 0xab5f as uint32,
    },
    pg_unicode_range {
        first: 0xab60 as uint32,
        last: 0xab68 as uint32,
    },
    pg_unicode_range {
        first: 0xab69 as uint32,
        last: 0xab69 as uint32,
    },
    pg_unicode_range {
        first: 0xab70 as uint32,
        last: 0xabbf as uint32,
    },
    pg_unicode_range {
        first: 0xabc0 as uint32,
        last: 0xabe2 as uint32,
    },
    pg_unicode_range {
        first: 0xabe3 as uint32,
        last: 0xabe4 as uint32,
    },
    pg_unicode_range {
        first: 0xabe5 as uint32,
        last: 0xabe5 as uint32,
    },
    pg_unicode_range {
        first: 0xabe6 as uint32,
        last: 0xabe7 as uint32,
    },
    pg_unicode_range {
        first: 0xabe8 as uint32,
        last: 0xabe8 as uint32,
    },
    pg_unicode_range {
        first: 0xabe9 as uint32,
        last: 0xabea as uint32,
    },
    pg_unicode_range {
        first: 0xac00 as uint32,
        last: 0xd7a3 as uint32,
    },
    pg_unicode_range {
        first: 0xd7b0 as uint32,
        last: 0xd7c6 as uint32,
    },
    pg_unicode_range {
        first: 0xd7cb as uint32,
        last: 0xd7fb as uint32,
    },
    pg_unicode_range {
        first: 0xf900 as uint32,
        last: 0xfa6d as uint32,
    },
    pg_unicode_range {
        first: 0xfa70 as uint32,
        last: 0xfad9 as uint32,
    },
    pg_unicode_range {
        first: 0xfb00 as uint32,
        last: 0xfb06 as uint32,
    },
    pg_unicode_range {
        first: 0xfb13 as uint32,
        last: 0xfb17 as uint32,
    },
    pg_unicode_range {
        first: 0xfb1d as uint32,
        last: 0xfb1d as uint32,
    },
    pg_unicode_range {
        first: 0xfb1e as uint32,
        last: 0xfb1e as uint32,
    },
    pg_unicode_range {
        first: 0xfb1f as uint32,
        last: 0xfb28 as uint32,
    },
    pg_unicode_range {
        first: 0xfb2a as uint32,
        last: 0xfb36 as uint32,
    },
    pg_unicode_range {
        first: 0xfb38 as uint32,
        last: 0xfb3c as uint32,
    },
    pg_unicode_range {
        first: 0xfb3e as uint32,
        last: 0xfb3e as uint32,
    },
    pg_unicode_range {
        first: 0xfb40 as uint32,
        last: 0xfb41 as uint32,
    },
    pg_unicode_range {
        first: 0xfb43 as uint32,
        last: 0xfb44 as uint32,
    },
    pg_unicode_range {
        first: 0xfb46 as uint32,
        last: 0xfbb1 as uint32,
    },
    pg_unicode_range {
        first: 0xfbd3 as uint32,
        last: 0xfd3d as uint32,
    },
    pg_unicode_range {
        first: 0xfd50 as uint32,
        last: 0xfd8f as uint32,
    },
    pg_unicode_range {
        first: 0xfd92 as uint32,
        last: 0xfdc7 as uint32,
    },
    pg_unicode_range {
        first: 0xfdf0 as uint32,
        last: 0xfdfb as uint32,
    },
    pg_unicode_range {
        first: 0xfe70 as uint32,
        last: 0xfe74 as uint32,
    },
    pg_unicode_range {
        first: 0xfe76 as uint32,
        last: 0xfefc as uint32,
    },
    pg_unicode_range {
        first: 0xff21 as uint32,
        last: 0xff3a as uint32,
    },
    pg_unicode_range {
        first: 0xff41 as uint32,
        last: 0xff5a as uint32,
    },
    pg_unicode_range {
        first: 0xff66 as uint32,
        last: 0xff6f as uint32,
    },
    pg_unicode_range {
        first: 0xff70 as uint32,
        last: 0xff70 as uint32,
    },
    pg_unicode_range {
        first: 0xff71 as uint32,
        last: 0xff9d as uint32,
    },
    pg_unicode_range {
        first: 0xff9e as uint32,
        last: 0xff9f as uint32,
    },
    pg_unicode_range {
        first: 0xffa0 as uint32,
        last: 0xffbe as uint32,
    },
    pg_unicode_range {
        first: 0xffc2 as uint32,
        last: 0xffc7 as uint32,
    },
    pg_unicode_range {
        first: 0xffca as uint32,
        last: 0xffcf as uint32,
    },
    pg_unicode_range {
        first: 0xffd2 as uint32,
        last: 0xffd7 as uint32,
    },
    pg_unicode_range {
        first: 0xffda as uint32,
        last: 0xffdc as uint32,
    },
    pg_unicode_range {
        first: 0x10000 as uint32,
        last: 0x1000b as uint32,
    },
    pg_unicode_range {
        first: 0x1000d as uint32,
        last: 0x10026 as uint32,
    },
    pg_unicode_range {
        first: 0x10028 as uint32,
        last: 0x1003a as uint32,
    },
    pg_unicode_range {
        first: 0x1003c as uint32,
        last: 0x1003d as uint32,
    },
    pg_unicode_range {
        first: 0x1003f as uint32,
        last: 0x1004d as uint32,
    },
    pg_unicode_range {
        first: 0x10050 as uint32,
        last: 0x1005d as uint32,
    },
    pg_unicode_range {
        first: 0x10080 as uint32,
        last: 0x100fa as uint32,
    },
    pg_unicode_range {
        first: 0x10140 as uint32,
        last: 0x10174 as uint32,
    },
    pg_unicode_range {
        first: 0x10280 as uint32,
        last: 0x1029c as uint32,
    },
    pg_unicode_range {
        first: 0x102a0 as uint32,
        last: 0x102d0 as uint32,
    },
    pg_unicode_range {
        first: 0x10300 as uint32,
        last: 0x1031f as uint32,
    },
    pg_unicode_range {
        first: 0x1032d as uint32,
        last: 0x10340 as uint32,
    },
    pg_unicode_range {
        first: 0x10341 as uint32,
        last: 0x10341 as uint32,
    },
    pg_unicode_range {
        first: 0x10342 as uint32,
        last: 0x10349 as uint32,
    },
    pg_unicode_range {
        first: 0x1034a as uint32,
        last: 0x1034a as uint32,
    },
    pg_unicode_range {
        first: 0x10350 as uint32,
        last: 0x10375 as uint32,
    },
    pg_unicode_range {
        first: 0x10376 as uint32,
        last: 0x1037a as uint32,
    },
    pg_unicode_range {
        first: 0x10380 as uint32,
        last: 0x1039d as uint32,
    },
    pg_unicode_range {
        first: 0x103a0 as uint32,
        last: 0x103c3 as uint32,
    },
    pg_unicode_range {
        first: 0x103c8 as uint32,
        last: 0x103cf as uint32,
    },
    pg_unicode_range {
        first: 0x103d1 as uint32,
        last: 0x103d5 as uint32,
    },
    pg_unicode_range {
        first: 0x10400 as uint32,
        last: 0x1044f as uint32,
    },
    pg_unicode_range {
        first: 0x10450 as uint32,
        last: 0x1049d as uint32,
    },
    pg_unicode_range {
        first: 0x104b0 as uint32,
        last: 0x104d3 as uint32,
    },
    pg_unicode_range {
        first: 0x104d8 as uint32,
        last: 0x104fb as uint32,
    },
    pg_unicode_range {
        first: 0x10500 as uint32,
        last: 0x10527 as uint32,
    },
    pg_unicode_range {
        first: 0x10530 as uint32,
        last: 0x10563 as uint32,
    },
    pg_unicode_range {
        first: 0x10570 as uint32,
        last: 0x1057a as uint32,
    },
    pg_unicode_range {
        first: 0x1057c as uint32,
        last: 0x1058a as uint32,
    },
    pg_unicode_range {
        first: 0x1058c as uint32,
        last: 0x10592 as uint32,
    },
    pg_unicode_range {
        first: 0x10594 as uint32,
        last: 0x10595 as uint32,
    },
    pg_unicode_range {
        first: 0x10597 as uint32,
        last: 0x105a1 as uint32,
    },
    pg_unicode_range {
        first: 0x105a3 as uint32,
        last: 0x105b1 as uint32,
    },
    pg_unicode_range {
        first: 0x105b3 as uint32,
        last: 0x105b9 as uint32,
    },
    pg_unicode_range {
        first: 0x105bb as uint32,
        last: 0x105bc as uint32,
    },
    pg_unicode_range {
        first: 0x105c0 as uint32,
        last: 0x105f3 as uint32,
    },
    pg_unicode_range {
        first: 0x10600 as uint32,
        last: 0x10736 as uint32,
    },
    pg_unicode_range {
        first: 0x10740 as uint32,
        last: 0x10755 as uint32,
    },
    pg_unicode_range {
        first: 0x10760 as uint32,
        last: 0x10767 as uint32,
    },
    pg_unicode_range {
        first: 0x10780 as uint32,
        last: 0x10785 as uint32,
    },
    pg_unicode_range {
        first: 0x10787 as uint32,
        last: 0x107b0 as uint32,
    },
    pg_unicode_range {
        first: 0x107b2 as uint32,
        last: 0x107ba as uint32,
    },
    pg_unicode_range {
        first: 0x10800 as uint32,
        last: 0x10805 as uint32,
    },
    pg_unicode_range {
        first: 0x10808 as uint32,
        last: 0x10808 as uint32,
    },
    pg_unicode_range {
        first: 0x1080a as uint32,
        last: 0x10835 as uint32,
    },
    pg_unicode_range {
        first: 0x10837 as uint32,
        last: 0x10838 as uint32,
    },
    pg_unicode_range {
        first: 0x1083c as uint32,
        last: 0x1083c as uint32,
    },
    pg_unicode_range {
        first: 0x1083f as uint32,
        last: 0x10855 as uint32,
    },
    pg_unicode_range {
        first: 0x10860 as uint32,
        last: 0x10876 as uint32,
    },
    pg_unicode_range {
        first: 0x10880 as uint32,
        last: 0x1089e as uint32,
    },
    pg_unicode_range {
        first: 0x108e0 as uint32,
        last: 0x108f2 as uint32,
    },
    pg_unicode_range {
        first: 0x108f4 as uint32,
        last: 0x108f5 as uint32,
    },
    pg_unicode_range {
        first: 0x10900 as uint32,
        last: 0x10915 as uint32,
    },
    pg_unicode_range {
        first: 0x10920 as uint32,
        last: 0x10939 as uint32,
    },
    pg_unicode_range {
        first: 0x10980 as uint32,
        last: 0x109b7 as uint32,
    },
    pg_unicode_range {
        first: 0x109be as uint32,
        last: 0x109bf as uint32,
    },
    pg_unicode_range {
        first: 0x10a00 as uint32,
        last: 0x10a00 as uint32,
    },
    pg_unicode_range {
        first: 0x10a01 as uint32,
        last: 0x10a03 as uint32,
    },
    pg_unicode_range {
        first: 0x10a05 as uint32,
        last: 0x10a06 as uint32,
    },
    pg_unicode_range {
        first: 0x10a0c as uint32,
        last: 0x10a0f as uint32,
    },
    pg_unicode_range {
        first: 0x10a10 as uint32,
        last: 0x10a13 as uint32,
    },
    pg_unicode_range {
        first: 0x10a15 as uint32,
        last: 0x10a17 as uint32,
    },
    pg_unicode_range {
        first: 0x10a19 as uint32,
        last: 0x10a35 as uint32,
    },
    pg_unicode_range {
        first: 0x10a60 as uint32,
        last: 0x10a7c as uint32,
    },
    pg_unicode_range {
        first: 0x10a80 as uint32,
        last: 0x10a9c as uint32,
    },
    pg_unicode_range {
        first: 0x10ac0 as uint32,
        last: 0x10ac7 as uint32,
    },
    pg_unicode_range {
        first: 0x10ac9 as uint32,
        last: 0x10ae4 as uint32,
    },
    pg_unicode_range {
        first: 0x10b00 as uint32,
        last: 0x10b35 as uint32,
    },
    pg_unicode_range {
        first: 0x10b40 as uint32,
        last: 0x10b55 as uint32,
    },
    pg_unicode_range {
        first: 0x10b60 as uint32,
        last: 0x10b72 as uint32,
    },
    pg_unicode_range {
        first: 0x10b80 as uint32,
        last: 0x10b91 as uint32,
    },
    pg_unicode_range {
        first: 0x10c00 as uint32,
        last: 0x10c48 as uint32,
    },
    pg_unicode_range {
        first: 0x10c80 as uint32,
        last: 0x10cb2 as uint32,
    },
    pg_unicode_range {
        first: 0x10cc0 as uint32,
        last: 0x10cf2 as uint32,
    },
    pg_unicode_range {
        first: 0x10d00 as uint32,
        last: 0x10d23 as uint32,
    },
    pg_unicode_range {
        first: 0x10d24 as uint32,
        last: 0x10d27 as uint32,
    },
    pg_unicode_range {
        first: 0x10d4a as uint32,
        last: 0x10d4d as uint32,
    },
    pg_unicode_range {
        first: 0x10d4e as uint32,
        last: 0x10d4e as uint32,
    },
    pg_unicode_range {
        first: 0x10d4f as uint32,
        last: 0x10d4f as uint32,
    },
    pg_unicode_range {
        first: 0x10d50 as uint32,
        last: 0x10d65 as uint32,
    },
    pg_unicode_range {
        first: 0x10d69 as uint32,
        last: 0x10d69 as uint32,
    },
    pg_unicode_range {
        first: 0x10d6f as uint32,
        last: 0x10d6f as uint32,
    },
    pg_unicode_range {
        first: 0x10d70 as uint32,
        last: 0x10d85 as uint32,
    },
    pg_unicode_range {
        first: 0x10e80 as uint32,
        last: 0x10ea9 as uint32,
    },
    pg_unicode_range {
        first: 0x10eab as uint32,
        last: 0x10eac as uint32,
    },
    pg_unicode_range {
        first: 0x10eb0 as uint32,
        last: 0x10eb1 as uint32,
    },
    pg_unicode_range {
        first: 0x10ec2 as uint32,
        last: 0x10ec4 as uint32,
    },
    pg_unicode_range {
        first: 0x10efc as uint32,
        last: 0x10efc as uint32,
    },
    pg_unicode_range {
        first: 0x10f00 as uint32,
        last: 0x10f1c as uint32,
    },
    pg_unicode_range {
        first: 0x10f27 as uint32,
        last: 0x10f27 as uint32,
    },
    pg_unicode_range {
        first: 0x10f30 as uint32,
        last: 0x10f45 as uint32,
    },
    pg_unicode_range {
        first: 0x10f70 as uint32,
        last: 0x10f81 as uint32,
    },
    pg_unicode_range {
        first: 0x10fb0 as uint32,
        last: 0x10fc4 as uint32,
    },
    pg_unicode_range {
        first: 0x10fe0 as uint32,
        last: 0x10ff6 as uint32,
    },
    pg_unicode_range {
        first: 0x11000 as uint32,
        last: 0x11000 as uint32,
    },
    pg_unicode_range {
        first: 0x11001 as uint32,
        last: 0x11001 as uint32,
    },
    pg_unicode_range {
        first: 0x11002 as uint32,
        last: 0x11002 as uint32,
    },
    pg_unicode_range {
        first: 0x11003 as uint32,
        last: 0x11037 as uint32,
    },
    pg_unicode_range {
        first: 0x11038 as uint32,
        last: 0x11045 as uint32,
    },
    pg_unicode_range {
        first: 0x11071 as uint32,
        last: 0x11072 as uint32,
    },
    pg_unicode_range {
        first: 0x11073 as uint32,
        last: 0x11074 as uint32,
    },
    pg_unicode_range {
        first: 0x11075 as uint32,
        last: 0x11075 as uint32,
    },
    pg_unicode_range {
        first: 0x11080 as uint32,
        last: 0x11081 as uint32,
    },
    pg_unicode_range {
        first: 0x11082 as uint32,
        last: 0x11082 as uint32,
    },
    pg_unicode_range {
        first: 0x11083 as uint32,
        last: 0x110af as uint32,
    },
    pg_unicode_range {
        first: 0x110b0 as uint32,
        last: 0x110b2 as uint32,
    },
    pg_unicode_range {
        first: 0x110b3 as uint32,
        last: 0x110b6 as uint32,
    },
    pg_unicode_range {
        first: 0x110b7 as uint32,
        last: 0x110b8 as uint32,
    },
    pg_unicode_range {
        first: 0x110c2 as uint32,
        last: 0x110c2 as uint32,
    },
    pg_unicode_range {
        first: 0x110d0 as uint32,
        last: 0x110e8 as uint32,
    },
    pg_unicode_range {
        first: 0x11100 as uint32,
        last: 0x11102 as uint32,
    },
    pg_unicode_range {
        first: 0x11103 as uint32,
        last: 0x11126 as uint32,
    },
    pg_unicode_range {
        first: 0x11127 as uint32,
        last: 0x1112b as uint32,
    },
    pg_unicode_range {
        first: 0x1112c as uint32,
        last: 0x1112c as uint32,
    },
    pg_unicode_range {
        first: 0x1112d as uint32,
        last: 0x11132 as uint32,
    },
    pg_unicode_range {
        first: 0x11144 as uint32,
        last: 0x11144 as uint32,
    },
    pg_unicode_range {
        first: 0x11145 as uint32,
        last: 0x11146 as uint32,
    },
    pg_unicode_range {
        first: 0x11147 as uint32,
        last: 0x11147 as uint32,
    },
    pg_unicode_range {
        first: 0x11150 as uint32,
        last: 0x11172 as uint32,
    },
    pg_unicode_range {
        first: 0x11176 as uint32,
        last: 0x11176 as uint32,
    },
    pg_unicode_range {
        first: 0x11180 as uint32,
        last: 0x11181 as uint32,
    },
    pg_unicode_range {
        first: 0x11182 as uint32,
        last: 0x11182 as uint32,
    },
    pg_unicode_range {
        first: 0x11183 as uint32,
        last: 0x111b2 as uint32,
    },
    pg_unicode_range {
        first: 0x111b3 as uint32,
        last: 0x111b5 as uint32,
    },
    pg_unicode_range {
        first: 0x111b6 as uint32,
        last: 0x111be as uint32,
    },
    pg_unicode_range {
        first: 0x111bf as uint32,
        last: 0x111bf as uint32,
    },
    pg_unicode_range {
        first: 0x111c1 as uint32,
        last: 0x111c4 as uint32,
    },
    pg_unicode_range {
        first: 0x111ce as uint32,
        last: 0x111ce as uint32,
    },
    pg_unicode_range {
        first: 0x111cf as uint32,
        last: 0x111cf as uint32,
    },
    pg_unicode_range {
        first: 0x111da as uint32,
        last: 0x111da as uint32,
    },
    pg_unicode_range {
        first: 0x111dc as uint32,
        last: 0x111dc as uint32,
    },
    pg_unicode_range {
        first: 0x11200 as uint32,
        last: 0x11211 as uint32,
    },
    pg_unicode_range {
        first: 0x11213 as uint32,
        last: 0x1122b as uint32,
    },
    pg_unicode_range {
        first: 0x1122c as uint32,
        last: 0x1122e as uint32,
    },
    pg_unicode_range {
        first: 0x1122f as uint32,
        last: 0x11231 as uint32,
    },
    pg_unicode_range {
        first: 0x11232 as uint32,
        last: 0x11233 as uint32,
    },
    pg_unicode_range {
        first: 0x11234 as uint32,
        last: 0x11234 as uint32,
    },
    pg_unicode_range {
        first: 0x11237 as uint32,
        last: 0x11237 as uint32,
    },
    pg_unicode_range {
        first: 0x1123e as uint32,
        last: 0x1123e as uint32,
    },
    pg_unicode_range {
        first: 0x1123f as uint32,
        last: 0x11240 as uint32,
    },
    pg_unicode_range {
        first: 0x11241 as uint32,
        last: 0x11241 as uint32,
    },
    pg_unicode_range {
        first: 0x11280 as uint32,
        last: 0x11286 as uint32,
    },
    pg_unicode_range {
        first: 0x11288 as uint32,
        last: 0x11288 as uint32,
    },
    pg_unicode_range {
        first: 0x1128a as uint32,
        last: 0x1128d as uint32,
    },
    pg_unicode_range {
        first: 0x1128f as uint32,
        last: 0x1129d as uint32,
    },
    pg_unicode_range {
        first: 0x1129f as uint32,
        last: 0x112a8 as uint32,
    },
    pg_unicode_range {
        first: 0x112b0 as uint32,
        last: 0x112de as uint32,
    },
    pg_unicode_range {
        first: 0x112df as uint32,
        last: 0x112df as uint32,
    },
    pg_unicode_range {
        first: 0x112e0 as uint32,
        last: 0x112e2 as uint32,
    },
    pg_unicode_range {
        first: 0x112e3 as uint32,
        last: 0x112e8 as uint32,
    },
    pg_unicode_range {
        first: 0x11300 as uint32,
        last: 0x11301 as uint32,
    },
    pg_unicode_range {
        first: 0x11302 as uint32,
        last: 0x11303 as uint32,
    },
    pg_unicode_range {
        first: 0x11305 as uint32,
        last: 0x1130c as uint32,
    },
    pg_unicode_range {
        first: 0x1130f as uint32,
        last: 0x11310 as uint32,
    },
    pg_unicode_range {
        first: 0x11313 as uint32,
        last: 0x11328 as uint32,
    },
    pg_unicode_range {
        first: 0x1132a as uint32,
        last: 0x11330 as uint32,
    },
    pg_unicode_range {
        first: 0x11332 as uint32,
        last: 0x11333 as uint32,
    },
    pg_unicode_range {
        first: 0x11335 as uint32,
        last: 0x11339 as uint32,
    },
    pg_unicode_range {
        first: 0x1133d as uint32,
        last: 0x1133d as uint32,
    },
    pg_unicode_range {
        first: 0x1133e as uint32,
        last: 0x1133f as uint32,
    },
    pg_unicode_range {
        first: 0x11340 as uint32,
        last: 0x11340 as uint32,
    },
    pg_unicode_range {
        first: 0x11341 as uint32,
        last: 0x11344 as uint32,
    },
    pg_unicode_range {
        first: 0x11347 as uint32,
        last: 0x11348 as uint32,
    },
    pg_unicode_range {
        first: 0x1134b as uint32,
        last: 0x1134c as uint32,
    },
    pg_unicode_range {
        first: 0x11350 as uint32,
        last: 0x11350 as uint32,
    },
    pg_unicode_range {
        first: 0x11357 as uint32,
        last: 0x11357 as uint32,
    },
    pg_unicode_range {
        first: 0x1135d as uint32,
        last: 0x11361 as uint32,
    },
    pg_unicode_range {
        first: 0x11362 as uint32,
        last: 0x11363 as uint32,
    },
    pg_unicode_range {
        first: 0x11380 as uint32,
        last: 0x11389 as uint32,
    },
    pg_unicode_range {
        first: 0x1138b as uint32,
        last: 0x1138b as uint32,
    },
    pg_unicode_range {
        first: 0x1138e as uint32,
        last: 0x1138e as uint32,
    },
    pg_unicode_range {
        first: 0x11390 as uint32,
        last: 0x113b5 as uint32,
    },
    pg_unicode_range {
        first: 0x113b7 as uint32,
        last: 0x113b7 as uint32,
    },
    pg_unicode_range {
        first: 0x113b8 as uint32,
        last: 0x113ba as uint32,
    },
    pg_unicode_range {
        first: 0x113bb as uint32,
        last: 0x113c0 as uint32,
    },
    pg_unicode_range {
        first: 0x113c2 as uint32,
        last: 0x113c2 as uint32,
    },
    pg_unicode_range {
        first: 0x113c5 as uint32,
        last: 0x113c5 as uint32,
    },
    pg_unicode_range {
        first: 0x113c7 as uint32,
        last: 0x113ca as uint32,
    },
    pg_unicode_range {
        first: 0x113cc as uint32,
        last: 0x113cd as uint32,
    },
    pg_unicode_range {
        first: 0x113d1 as uint32,
        last: 0x113d1 as uint32,
    },
    pg_unicode_range {
        first: 0x113d3 as uint32,
        last: 0x113d3 as uint32,
    },
    pg_unicode_range {
        first: 0x11400 as uint32,
        last: 0x11434 as uint32,
    },
    pg_unicode_range {
        first: 0x11435 as uint32,
        last: 0x11437 as uint32,
    },
    pg_unicode_range {
        first: 0x11438 as uint32,
        last: 0x1143f as uint32,
    },
    pg_unicode_range {
        first: 0x11440 as uint32,
        last: 0x11441 as uint32,
    },
    pg_unicode_range {
        first: 0x11443 as uint32,
        last: 0x11444 as uint32,
    },
    pg_unicode_range {
        first: 0x11445 as uint32,
        last: 0x11445 as uint32,
    },
    pg_unicode_range {
        first: 0x11447 as uint32,
        last: 0x1144a as uint32,
    },
    pg_unicode_range {
        first: 0x1145f as uint32,
        last: 0x11461 as uint32,
    },
    pg_unicode_range {
        first: 0x11480 as uint32,
        last: 0x114af as uint32,
    },
    pg_unicode_range {
        first: 0x114b0 as uint32,
        last: 0x114b2 as uint32,
    },
    pg_unicode_range {
        first: 0x114b3 as uint32,
        last: 0x114b8 as uint32,
    },
    pg_unicode_range {
        first: 0x114b9 as uint32,
        last: 0x114b9 as uint32,
    },
    pg_unicode_range {
        first: 0x114ba as uint32,
        last: 0x114ba as uint32,
    },
    pg_unicode_range {
        first: 0x114bb as uint32,
        last: 0x114be as uint32,
    },
    pg_unicode_range {
        first: 0x114bf as uint32,
        last: 0x114c0 as uint32,
    },
    pg_unicode_range {
        first: 0x114c1 as uint32,
        last: 0x114c1 as uint32,
    },
    pg_unicode_range {
        first: 0x114c4 as uint32,
        last: 0x114c5 as uint32,
    },
    pg_unicode_range {
        first: 0x114c7 as uint32,
        last: 0x114c7 as uint32,
    },
    pg_unicode_range {
        first: 0x11580 as uint32,
        last: 0x115ae as uint32,
    },
    pg_unicode_range {
        first: 0x115af as uint32,
        last: 0x115b1 as uint32,
    },
    pg_unicode_range {
        first: 0x115b2 as uint32,
        last: 0x115b5 as uint32,
    },
    pg_unicode_range {
        first: 0x115b8 as uint32,
        last: 0x115bb as uint32,
    },
    pg_unicode_range {
        first: 0x115bc as uint32,
        last: 0x115bd as uint32,
    },
    pg_unicode_range {
        first: 0x115be as uint32,
        last: 0x115be as uint32,
    },
    pg_unicode_range {
        first: 0x115d8 as uint32,
        last: 0x115db as uint32,
    },
    pg_unicode_range {
        first: 0x115dc as uint32,
        last: 0x115dd as uint32,
    },
    pg_unicode_range {
        first: 0x11600 as uint32,
        last: 0x1162f as uint32,
    },
    pg_unicode_range {
        first: 0x11630 as uint32,
        last: 0x11632 as uint32,
    },
    pg_unicode_range {
        first: 0x11633 as uint32,
        last: 0x1163a as uint32,
    },
    pg_unicode_range {
        first: 0x1163b as uint32,
        last: 0x1163c as uint32,
    },
    pg_unicode_range {
        first: 0x1163d as uint32,
        last: 0x1163d as uint32,
    },
    pg_unicode_range {
        first: 0x1163e as uint32,
        last: 0x1163e as uint32,
    },
    pg_unicode_range {
        first: 0x11640 as uint32,
        last: 0x11640 as uint32,
    },
    pg_unicode_range {
        first: 0x11644 as uint32,
        last: 0x11644 as uint32,
    },
    pg_unicode_range {
        first: 0x11680 as uint32,
        last: 0x116aa as uint32,
    },
    pg_unicode_range {
        first: 0x116ab as uint32,
        last: 0x116ab as uint32,
    },
    pg_unicode_range {
        first: 0x116ac as uint32,
        last: 0x116ac as uint32,
    },
    pg_unicode_range {
        first: 0x116ad as uint32,
        last: 0x116ad as uint32,
    },
    pg_unicode_range {
        first: 0x116ae as uint32,
        last: 0x116af as uint32,
    },
    pg_unicode_range {
        first: 0x116b0 as uint32,
        last: 0x116b5 as uint32,
    },
    pg_unicode_range {
        first: 0x116b8 as uint32,
        last: 0x116b8 as uint32,
    },
    pg_unicode_range {
        first: 0x11700 as uint32,
        last: 0x1171a as uint32,
    },
    pg_unicode_range {
        first: 0x1171d as uint32,
        last: 0x1171d as uint32,
    },
    pg_unicode_range {
        first: 0x1171e as uint32,
        last: 0x1171e as uint32,
    },
    pg_unicode_range {
        first: 0x1171f as uint32,
        last: 0x1171f as uint32,
    },
    pg_unicode_range {
        first: 0x11720 as uint32,
        last: 0x11721 as uint32,
    },
    pg_unicode_range {
        first: 0x11722 as uint32,
        last: 0x11725 as uint32,
    },
    pg_unicode_range {
        first: 0x11726 as uint32,
        last: 0x11726 as uint32,
    },
    pg_unicode_range {
        first: 0x11727 as uint32,
        last: 0x1172a as uint32,
    },
    pg_unicode_range {
        first: 0x11740 as uint32,
        last: 0x11746 as uint32,
    },
    pg_unicode_range {
        first: 0x11800 as uint32,
        last: 0x1182b as uint32,
    },
    pg_unicode_range {
        first: 0x1182c as uint32,
        last: 0x1182e as uint32,
    },
    pg_unicode_range {
        first: 0x1182f as uint32,
        last: 0x11837 as uint32,
    },
    pg_unicode_range {
        first: 0x11838 as uint32,
        last: 0x11838 as uint32,
    },
    pg_unicode_range {
        first: 0x118a0 as uint32,
        last: 0x118df as uint32,
    },
    pg_unicode_range {
        first: 0x118ff as uint32,
        last: 0x11906 as uint32,
    },
    pg_unicode_range {
        first: 0x11909 as uint32,
        last: 0x11909 as uint32,
    },
    pg_unicode_range {
        first: 0x1190c as uint32,
        last: 0x11913 as uint32,
    },
    pg_unicode_range {
        first: 0x11915 as uint32,
        last: 0x11916 as uint32,
    },
    pg_unicode_range {
        first: 0x11918 as uint32,
        last: 0x1192f as uint32,
    },
    pg_unicode_range {
        first: 0x11930 as uint32,
        last: 0x11935 as uint32,
    },
    pg_unicode_range {
        first: 0x11937 as uint32,
        last: 0x11938 as uint32,
    },
    pg_unicode_range {
        first: 0x1193b as uint32,
        last: 0x1193c as uint32,
    },
    pg_unicode_range {
        first: 0x1193f as uint32,
        last: 0x1193f as uint32,
    },
    pg_unicode_range {
        first: 0x11940 as uint32,
        last: 0x11940 as uint32,
    },
    pg_unicode_range {
        first: 0x11941 as uint32,
        last: 0x11941 as uint32,
    },
    pg_unicode_range {
        first: 0x11942 as uint32,
        last: 0x11942 as uint32,
    },
    pg_unicode_range {
        first: 0x119a0 as uint32,
        last: 0x119a7 as uint32,
    },
    pg_unicode_range {
        first: 0x119aa as uint32,
        last: 0x119d0 as uint32,
    },
    pg_unicode_range {
        first: 0x119d1 as uint32,
        last: 0x119d3 as uint32,
    },
    pg_unicode_range {
        first: 0x119d4 as uint32,
        last: 0x119d7 as uint32,
    },
    pg_unicode_range {
        first: 0x119da as uint32,
        last: 0x119db as uint32,
    },
    pg_unicode_range {
        first: 0x119dc as uint32,
        last: 0x119df as uint32,
    },
    pg_unicode_range {
        first: 0x119e1 as uint32,
        last: 0x119e1 as uint32,
    },
    pg_unicode_range {
        first: 0x119e3 as uint32,
        last: 0x119e3 as uint32,
    },
    pg_unicode_range {
        first: 0x119e4 as uint32,
        last: 0x119e4 as uint32,
    },
    pg_unicode_range {
        first: 0x11a00 as uint32,
        last: 0x11a00 as uint32,
    },
    pg_unicode_range {
        first: 0x11a01 as uint32,
        last: 0x11a0a as uint32,
    },
    pg_unicode_range {
        first: 0x11a0b as uint32,
        last: 0x11a32 as uint32,
    },
    pg_unicode_range {
        first: 0x11a35 as uint32,
        last: 0x11a38 as uint32,
    },
    pg_unicode_range {
        first: 0x11a39 as uint32,
        last: 0x11a39 as uint32,
    },
    pg_unicode_range {
        first: 0x11a3a as uint32,
        last: 0x11a3a as uint32,
    },
    pg_unicode_range {
        first: 0x11a3b as uint32,
        last: 0x11a3e as uint32,
    },
    pg_unicode_range {
        first: 0x11a50 as uint32,
        last: 0x11a50 as uint32,
    },
    pg_unicode_range {
        first: 0x11a51 as uint32,
        last: 0x11a56 as uint32,
    },
    pg_unicode_range {
        first: 0x11a57 as uint32,
        last: 0x11a58 as uint32,
    },
    pg_unicode_range {
        first: 0x11a59 as uint32,
        last: 0x11a5b as uint32,
    },
    pg_unicode_range {
        first: 0x11a5c as uint32,
        last: 0x11a89 as uint32,
    },
    pg_unicode_range {
        first: 0x11a8a as uint32,
        last: 0x11a96 as uint32,
    },
    pg_unicode_range {
        first: 0x11a97 as uint32,
        last: 0x11a97 as uint32,
    },
    pg_unicode_range {
        first: 0x11a9d as uint32,
        last: 0x11a9d as uint32,
    },
    pg_unicode_range {
        first: 0x11ab0 as uint32,
        last: 0x11af8 as uint32,
    },
    pg_unicode_range {
        first: 0x11bc0 as uint32,
        last: 0x11be0 as uint32,
    },
    pg_unicode_range {
        first: 0x11c00 as uint32,
        last: 0x11c08 as uint32,
    },
    pg_unicode_range {
        first: 0x11c0a as uint32,
        last: 0x11c2e as uint32,
    },
    pg_unicode_range {
        first: 0x11c2f as uint32,
        last: 0x11c2f as uint32,
    },
    pg_unicode_range {
        first: 0x11c30 as uint32,
        last: 0x11c36 as uint32,
    },
    pg_unicode_range {
        first: 0x11c38 as uint32,
        last: 0x11c3d as uint32,
    },
    pg_unicode_range {
        first: 0x11c3e as uint32,
        last: 0x11c3e as uint32,
    },
    pg_unicode_range {
        first: 0x11c40 as uint32,
        last: 0x11c40 as uint32,
    },
    pg_unicode_range {
        first: 0x11c72 as uint32,
        last: 0x11c8f as uint32,
    },
    pg_unicode_range {
        first: 0x11c92 as uint32,
        last: 0x11ca7 as uint32,
    },
    pg_unicode_range {
        first: 0x11ca9 as uint32,
        last: 0x11ca9 as uint32,
    },
    pg_unicode_range {
        first: 0x11caa as uint32,
        last: 0x11cb0 as uint32,
    },
    pg_unicode_range {
        first: 0x11cb1 as uint32,
        last: 0x11cb1 as uint32,
    },
    pg_unicode_range {
        first: 0x11cb2 as uint32,
        last: 0x11cb3 as uint32,
    },
    pg_unicode_range {
        first: 0x11cb4 as uint32,
        last: 0x11cb4 as uint32,
    },
    pg_unicode_range {
        first: 0x11cb5 as uint32,
        last: 0x11cb6 as uint32,
    },
    pg_unicode_range {
        first: 0x11d00 as uint32,
        last: 0x11d06 as uint32,
    },
    pg_unicode_range {
        first: 0x11d08 as uint32,
        last: 0x11d09 as uint32,
    },
    pg_unicode_range {
        first: 0x11d0b as uint32,
        last: 0x11d30 as uint32,
    },
    pg_unicode_range {
        first: 0x11d31 as uint32,
        last: 0x11d36 as uint32,
    },
    pg_unicode_range {
        first: 0x11d3a as uint32,
        last: 0x11d3a as uint32,
    },
    pg_unicode_range {
        first: 0x11d3c as uint32,
        last: 0x11d3d as uint32,
    },
    pg_unicode_range {
        first: 0x11d3f as uint32,
        last: 0x11d41 as uint32,
    },
    pg_unicode_range {
        first: 0x11d43 as uint32,
        last: 0x11d43 as uint32,
    },
    pg_unicode_range {
        first: 0x11d46 as uint32,
        last: 0x11d46 as uint32,
    },
    pg_unicode_range {
        first: 0x11d47 as uint32,
        last: 0x11d47 as uint32,
    },
    pg_unicode_range {
        first: 0x11d60 as uint32,
        last: 0x11d65 as uint32,
    },
    pg_unicode_range {
        first: 0x11d67 as uint32,
        last: 0x11d68 as uint32,
    },
    pg_unicode_range {
        first: 0x11d6a as uint32,
        last: 0x11d89 as uint32,
    },
    pg_unicode_range {
        first: 0x11d8a as uint32,
        last: 0x11d8e as uint32,
    },
    pg_unicode_range {
        first: 0x11d90 as uint32,
        last: 0x11d91 as uint32,
    },
    pg_unicode_range {
        first: 0x11d93 as uint32,
        last: 0x11d94 as uint32,
    },
    pg_unicode_range {
        first: 0x11d95 as uint32,
        last: 0x11d95 as uint32,
    },
    pg_unicode_range {
        first: 0x11d96 as uint32,
        last: 0x11d96 as uint32,
    },
    pg_unicode_range {
        first: 0x11d98 as uint32,
        last: 0x11d98 as uint32,
    },
    pg_unicode_range {
        first: 0x11ee0 as uint32,
        last: 0x11ef2 as uint32,
    },
    pg_unicode_range {
        first: 0x11ef3 as uint32,
        last: 0x11ef4 as uint32,
    },
    pg_unicode_range {
        first: 0x11ef5 as uint32,
        last: 0x11ef6 as uint32,
    },
    pg_unicode_range {
        first: 0x11f00 as uint32,
        last: 0x11f01 as uint32,
    },
    pg_unicode_range {
        first: 0x11f02 as uint32,
        last: 0x11f02 as uint32,
    },
    pg_unicode_range {
        first: 0x11f03 as uint32,
        last: 0x11f03 as uint32,
    },
    pg_unicode_range {
        first: 0x11f04 as uint32,
        last: 0x11f10 as uint32,
    },
    pg_unicode_range {
        first: 0x11f12 as uint32,
        last: 0x11f33 as uint32,
    },
    pg_unicode_range {
        first: 0x11f34 as uint32,
        last: 0x11f35 as uint32,
    },
    pg_unicode_range {
        first: 0x11f36 as uint32,
        last: 0x11f3a as uint32,
    },
    pg_unicode_range {
        first: 0x11f3e as uint32,
        last: 0x11f3f as uint32,
    },
    pg_unicode_range {
        first: 0x11f40 as uint32,
        last: 0x11f40 as uint32,
    },
    pg_unicode_range {
        first: 0x11fb0 as uint32,
        last: 0x11fb0 as uint32,
    },
    pg_unicode_range {
        first: 0x12000 as uint32,
        last: 0x12399 as uint32,
    },
    pg_unicode_range {
        first: 0x12400 as uint32,
        last: 0x1246e as uint32,
    },
    pg_unicode_range {
        first: 0x12480 as uint32,
        last: 0x12543 as uint32,
    },
    pg_unicode_range {
        first: 0x12f90 as uint32,
        last: 0x12ff0 as uint32,
    },
    pg_unicode_range {
        first: 0x13000 as uint32,
        last: 0x1342f as uint32,
    },
    pg_unicode_range {
        first: 0x13441 as uint32,
        last: 0x13446 as uint32,
    },
    pg_unicode_range {
        first: 0x13460 as uint32,
        last: 0x143fa as uint32,
    },
    pg_unicode_range {
        first: 0x14400 as uint32,
        last: 0x14646 as uint32,
    },
    pg_unicode_range {
        first: 0x16100 as uint32,
        last: 0x1611d as uint32,
    },
    pg_unicode_range {
        first: 0x1611e as uint32,
        last: 0x16129 as uint32,
    },
    pg_unicode_range {
        first: 0x1612a as uint32,
        last: 0x1612c as uint32,
    },
    pg_unicode_range {
        first: 0x1612d as uint32,
        last: 0x1612e as uint32,
    },
    pg_unicode_range {
        first: 0x16800 as uint32,
        last: 0x16a38 as uint32,
    },
    pg_unicode_range {
        first: 0x16a40 as uint32,
        last: 0x16a5e as uint32,
    },
    pg_unicode_range {
        first: 0x16a70 as uint32,
        last: 0x16abe as uint32,
    },
    pg_unicode_range {
        first: 0x16ad0 as uint32,
        last: 0x16aed as uint32,
    },
    pg_unicode_range {
        first: 0x16b00 as uint32,
        last: 0x16b2f as uint32,
    },
    pg_unicode_range {
        first: 0x16b40 as uint32,
        last: 0x16b43 as uint32,
    },
    pg_unicode_range {
        first: 0x16b63 as uint32,
        last: 0x16b77 as uint32,
    },
    pg_unicode_range {
        first: 0x16b7d as uint32,
        last: 0x16b8f as uint32,
    },
    pg_unicode_range {
        first: 0x16d40 as uint32,
        last: 0x16d42 as uint32,
    },
    pg_unicode_range {
        first: 0x16d43 as uint32,
        last: 0x16d6a as uint32,
    },
    pg_unicode_range {
        first: 0x16d6b as uint32,
        last: 0x16d6c as uint32,
    },
    pg_unicode_range {
        first: 0x16e40 as uint32,
        last: 0x16e7f as uint32,
    },
    pg_unicode_range {
        first: 0x16f00 as uint32,
        last: 0x16f4a as uint32,
    },
    pg_unicode_range {
        first: 0x16f4f as uint32,
        last: 0x16f4f as uint32,
    },
    pg_unicode_range {
        first: 0x16f50 as uint32,
        last: 0x16f50 as uint32,
    },
    pg_unicode_range {
        first: 0x16f51 as uint32,
        last: 0x16f87 as uint32,
    },
    pg_unicode_range {
        first: 0x16f8f as uint32,
        last: 0x16f92 as uint32,
    },
    pg_unicode_range {
        first: 0x16f93 as uint32,
        last: 0x16f9f as uint32,
    },
    pg_unicode_range {
        first: 0x16fe0 as uint32,
        last: 0x16fe1 as uint32,
    },
    pg_unicode_range {
        first: 0x16fe3 as uint32,
        last: 0x16fe3 as uint32,
    },
    pg_unicode_range {
        first: 0x16ff0 as uint32,
        last: 0x16ff1 as uint32,
    },
    pg_unicode_range {
        first: 0x17000 as uint32,
        last: 0x187f7 as uint32,
    },
    pg_unicode_range {
        first: 0x18800 as uint32,
        last: 0x18cd5 as uint32,
    },
    pg_unicode_range {
        first: 0x18cff as uint32,
        last: 0x18d08 as uint32,
    },
    pg_unicode_range {
        first: 0x1aff0 as uint32,
        last: 0x1aff3 as uint32,
    },
    pg_unicode_range {
        first: 0x1aff5 as uint32,
        last: 0x1affb as uint32,
    },
    pg_unicode_range {
        first: 0x1affd as uint32,
        last: 0x1affe as uint32,
    },
    pg_unicode_range {
        first: 0x1b000 as uint32,
        last: 0x1b122 as uint32,
    },
    pg_unicode_range {
        first: 0x1b132 as uint32,
        last: 0x1b132 as uint32,
    },
    pg_unicode_range {
        first: 0x1b150 as uint32,
        last: 0x1b152 as uint32,
    },
    pg_unicode_range {
        first: 0x1b155 as uint32,
        last: 0x1b155 as uint32,
    },
    pg_unicode_range {
        first: 0x1b164 as uint32,
        last: 0x1b167 as uint32,
    },
    pg_unicode_range {
        first: 0x1b170 as uint32,
        last: 0x1b2fb as uint32,
    },
    pg_unicode_range {
        first: 0x1bc00 as uint32,
        last: 0x1bc6a as uint32,
    },
    pg_unicode_range {
        first: 0x1bc70 as uint32,
        last: 0x1bc7c as uint32,
    },
    pg_unicode_range {
        first: 0x1bc80 as uint32,
        last: 0x1bc88 as uint32,
    },
    pg_unicode_range {
        first: 0x1bc90 as uint32,
        last: 0x1bc99 as uint32,
    },
    pg_unicode_range {
        first: 0x1bc9e as uint32,
        last: 0x1bc9e as uint32,
    },
    pg_unicode_range {
        first: 0x1d400 as uint32,
        last: 0x1d454 as uint32,
    },
    pg_unicode_range {
        first: 0x1d456 as uint32,
        last: 0x1d49c as uint32,
    },
    pg_unicode_range {
        first: 0x1d49e as uint32,
        last: 0x1d49f as uint32,
    },
    pg_unicode_range {
        first: 0x1d4a2 as uint32,
        last: 0x1d4a2 as uint32,
    },
    pg_unicode_range {
        first: 0x1d4a5 as uint32,
        last: 0x1d4a6 as uint32,
    },
    pg_unicode_range {
        first: 0x1d4a9 as uint32,
        last: 0x1d4ac as uint32,
    },
    pg_unicode_range {
        first: 0x1d4ae as uint32,
        last: 0x1d4b9 as uint32,
    },
    pg_unicode_range {
        first: 0x1d4bb as uint32,
        last: 0x1d4bb as uint32,
    },
    pg_unicode_range {
        first: 0x1d4bd as uint32,
        last: 0x1d4c3 as uint32,
    },
    pg_unicode_range {
        first: 0x1d4c5 as uint32,
        last: 0x1d505 as uint32,
    },
    pg_unicode_range {
        first: 0x1d507 as uint32,
        last: 0x1d50a as uint32,
    },
    pg_unicode_range {
        first: 0x1d50d as uint32,
        last: 0x1d514 as uint32,
    },
    pg_unicode_range {
        first: 0x1d516 as uint32,
        last: 0x1d51c as uint32,
    },
    pg_unicode_range {
        first: 0x1d51e as uint32,
        last: 0x1d539 as uint32,
    },
    pg_unicode_range {
        first: 0x1d53b as uint32,
        last: 0x1d53e as uint32,
    },
    pg_unicode_range {
        first: 0x1d540 as uint32,
        last: 0x1d544 as uint32,
    },
    pg_unicode_range {
        first: 0x1d546 as uint32,
        last: 0x1d546 as uint32,
    },
    pg_unicode_range {
        first: 0x1d54a as uint32,
        last: 0x1d550 as uint32,
    },
    pg_unicode_range {
        first: 0x1d552 as uint32,
        last: 0x1d6a5 as uint32,
    },
    pg_unicode_range {
        first: 0x1d6a8 as uint32,
        last: 0x1d6c0 as uint32,
    },
    pg_unicode_range {
        first: 0x1d6c2 as uint32,
        last: 0x1d6da as uint32,
    },
    pg_unicode_range {
        first: 0x1d6dc as uint32,
        last: 0x1d6fa as uint32,
    },
    pg_unicode_range {
        first: 0x1d6fc as uint32,
        last: 0x1d714 as uint32,
    },
    pg_unicode_range {
        first: 0x1d716 as uint32,
        last: 0x1d734 as uint32,
    },
    pg_unicode_range {
        first: 0x1d736 as uint32,
        last: 0x1d74e as uint32,
    },
    pg_unicode_range {
        first: 0x1d750 as uint32,
        last: 0x1d76e as uint32,
    },
    pg_unicode_range {
        first: 0x1d770 as uint32,
        last: 0x1d788 as uint32,
    },
    pg_unicode_range {
        first: 0x1d78a as uint32,
        last: 0x1d7a8 as uint32,
    },
    pg_unicode_range {
        first: 0x1d7aa as uint32,
        last: 0x1d7c2 as uint32,
    },
    pg_unicode_range {
        first: 0x1d7c4 as uint32,
        last: 0x1d7cb as uint32,
    },
    pg_unicode_range {
        first: 0x1df00 as uint32,
        last: 0x1df09 as uint32,
    },
    pg_unicode_range {
        first: 0x1df0a as uint32,
        last: 0x1df0a as uint32,
    },
    pg_unicode_range {
        first: 0x1df0b as uint32,
        last: 0x1df1e as uint32,
    },
    pg_unicode_range {
        first: 0x1df25 as uint32,
        last: 0x1df2a as uint32,
    },
    pg_unicode_range {
        first: 0x1e000 as uint32,
        last: 0x1e006 as uint32,
    },
    pg_unicode_range {
        first: 0x1e008 as uint32,
        last: 0x1e018 as uint32,
    },
    pg_unicode_range {
        first: 0x1e01b as uint32,
        last: 0x1e021 as uint32,
    },
    pg_unicode_range {
        first: 0x1e023 as uint32,
        last: 0x1e024 as uint32,
    },
    pg_unicode_range {
        first: 0x1e026 as uint32,
        last: 0x1e02a as uint32,
    },
    pg_unicode_range {
        first: 0x1e030 as uint32,
        last: 0x1e06d as uint32,
    },
    pg_unicode_range {
        first: 0x1e08f as uint32,
        last: 0x1e08f as uint32,
    },
    pg_unicode_range {
        first: 0x1e100 as uint32,
        last: 0x1e12c as uint32,
    },
    pg_unicode_range {
        first: 0x1e137 as uint32,
        last: 0x1e13d as uint32,
    },
    pg_unicode_range {
        first: 0x1e14e as uint32,
        last: 0x1e14e as uint32,
    },
    pg_unicode_range {
        first: 0x1e290 as uint32,
        last: 0x1e2ad as uint32,
    },
    pg_unicode_range {
        first: 0x1e2c0 as uint32,
        last: 0x1e2eb as uint32,
    },
    pg_unicode_range {
        first: 0x1e4d0 as uint32,
        last: 0x1e4ea as uint32,
    },
    pg_unicode_range {
        first: 0x1e4eb as uint32,
        last: 0x1e4eb as uint32,
    },
    pg_unicode_range {
        first: 0x1e5d0 as uint32,
        last: 0x1e5ed as uint32,
    },
    pg_unicode_range {
        first: 0x1e5f0 as uint32,
        last: 0x1e5f0 as uint32,
    },
    pg_unicode_range {
        first: 0x1e7e0 as uint32,
        last: 0x1e7e6 as uint32,
    },
    pg_unicode_range {
        first: 0x1e7e8 as uint32,
        last: 0x1e7eb as uint32,
    },
    pg_unicode_range {
        first: 0x1e7ed as uint32,
        last: 0x1e7ee as uint32,
    },
    pg_unicode_range {
        first: 0x1e7f0 as uint32,
        last: 0x1e7fe as uint32,
    },
    pg_unicode_range {
        first: 0x1e800 as uint32,
        last: 0x1e8c4 as uint32,
    },
    pg_unicode_range {
        first: 0x1e900 as uint32,
        last: 0x1e943 as uint32,
    },
    pg_unicode_range {
        first: 0x1e947 as uint32,
        last: 0x1e947 as uint32,
    },
    pg_unicode_range {
        first: 0x1e94b as uint32,
        last: 0x1e94b as uint32,
    },
    pg_unicode_range {
        first: 0x1ee00 as uint32,
        last: 0x1ee03 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee05 as uint32,
        last: 0x1ee1f as uint32,
    },
    pg_unicode_range {
        first: 0x1ee21 as uint32,
        last: 0x1ee22 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee24 as uint32,
        last: 0x1ee24 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee27 as uint32,
        last: 0x1ee27 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee29 as uint32,
        last: 0x1ee32 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee34 as uint32,
        last: 0x1ee37 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee39 as uint32,
        last: 0x1ee39 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee3b as uint32,
        last: 0x1ee3b as uint32,
    },
    pg_unicode_range {
        first: 0x1ee42 as uint32,
        last: 0x1ee42 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee47 as uint32,
        last: 0x1ee47 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee49 as uint32,
        last: 0x1ee49 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee4b as uint32,
        last: 0x1ee4b as uint32,
    },
    pg_unicode_range {
        first: 0x1ee4d as uint32,
        last: 0x1ee4f as uint32,
    },
    pg_unicode_range {
        first: 0x1ee51 as uint32,
        last: 0x1ee52 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee54 as uint32,
        last: 0x1ee54 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee57 as uint32,
        last: 0x1ee57 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee59 as uint32,
        last: 0x1ee59 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee5b as uint32,
        last: 0x1ee5b as uint32,
    },
    pg_unicode_range {
        first: 0x1ee5d as uint32,
        last: 0x1ee5d as uint32,
    },
    pg_unicode_range {
        first: 0x1ee5f as uint32,
        last: 0x1ee5f as uint32,
    },
    pg_unicode_range {
        first: 0x1ee61 as uint32,
        last: 0x1ee62 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee64 as uint32,
        last: 0x1ee64 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee67 as uint32,
        last: 0x1ee6a as uint32,
    },
    pg_unicode_range {
        first: 0x1ee6c as uint32,
        last: 0x1ee72 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee74 as uint32,
        last: 0x1ee77 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee79 as uint32,
        last: 0x1ee7c as uint32,
    },
    pg_unicode_range {
        first: 0x1ee7e as uint32,
        last: 0x1ee7e as uint32,
    },
    pg_unicode_range {
        first: 0x1ee80 as uint32,
        last: 0x1ee89 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee8b as uint32,
        last: 0x1ee9b as uint32,
    },
    pg_unicode_range {
        first: 0x1eea1 as uint32,
        last: 0x1eea3 as uint32,
    },
    pg_unicode_range {
        first: 0x1eea5 as uint32,
        last: 0x1eea9 as uint32,
    },
    pg_unicode_range {
        first: 0x1eeab as uint32,
        last: 0x1eebb as uint32,
    },
    pg_unicode_range {
        first: 0x1f130 as uint32,
        last: 0x1f149 as uint32,
    },
    pg_unicode_range {
        first: 0x1f150 as uint32,
        last: 0x1f169 as uint32,
    },
    pg_unicode_range {
        first: 0x1f170 as uint32,
        last: 0x1f189 as uint32,
    },
    pg_unicode_range {
        first: 0x20000 as uint32,
        last: 0x2a6df as uint32,
    },
    pg_unicode_range {
        first: 0x2a700 as uint32,
        last: 0x2b739 as uint32,
    },
    pg_unicode_range {
        first: 0x2b740 as uint32,
        last: 0x2b81d as uint32,
    },
    pg_unicode_range {
        first: 0x2b820 as uint32,
        last: 0x2cea1 as uint32,
    },
    pg_unicode_range {
        first: 0x2ceb0 as uint32,
        last: 0x2ebe0 as uint32,
    },
    pg_unicode_range {
        first: 0x2ebf0 as uint32,
        last: 0x2ee5d as uint32,
    },
    pg_unicode_range {
        first: 0x2f800 as uint32,
        last: 0x2fa1d as uint32,
    },
    pg_unicode_range {
        first: 0x30000 as uint32,
        last: 0x3134a as uint32,
    },
    pg_unicode_range {
        first: 0x31350 as uint32,
        last: 0x323af as uint32,
    },
];
pub(crate) static unicode_lowercase: [pg_unicode_range; 690] = [
    pg_unicode_range {
        first: 0x61 as uint32,
        last: 0x7a as uint32,
    },
    pg_unicode_range {
        first: 0xaa as uint32,
        last: 0xaa as uint32,
    },
    pg_unicode_range {
        first: 0xb5 as uint32,
        last: 0xb5 as uint32,
    },
    pg_unicode_range {
        first: 0xba as uint32,
        last: 0xba as uint32,
    },
    pg_unicode_range {
        first: 0xdf as uint32,
        last: 0xf6 as uint32,
    },
    pg_unicode_range {
        first: 0xf8 as uint32,
        last: 0xff as uint32,
    },
    pg_unicode_range {
        first: 0x101 as uint32,
        last: 0x101 as uint32,
    },
    pg_unicode_range {
        first: 0x103 as uint32,
        last: 0x103 as uint32,
    },
    pg_unicode_range {
        first: 0x105 as uint32,
        last: 0x105 as uint32,
    },
    pg_unicode_range {
        first: 0x107 as uint32,
        last: 0x107 as uint32,
    },
    pg_unicode_range {
        first: 0x109 as uint32,
        last: 0x109 as uint32,
    },
    pg_unicode_range {
        first: 0x10b as uint32,
        last: 0x10b as uint32,
    },
    pg_unicode_range {
        first: 0x10d as uint32,
        last: 0x10d as uint32,
    },
    pg_unicode_range {
        first: 0x10f as uint32,
        last: 0x10f as uint32,
    },
    pg_unicode_range {
        first: 0x111 as uint32,
        last: 0x111 as uint32,
    },
    pg_unicode_range {
        first: 0x113 as uint32,
        last: 0x113 as uint32,
    },
    pg_unicode_range {
        first: 0x115 as uint32,
        last: 0x115 as uint32,
    },
    pg_unicode_range {
        first: 0x117 as uint32,
        last: 0x117 as uint32,
    },
    pg_unicode_range {
        first: 0x119 as uint32,
        last: 0x119 as uint32,
    },
    pg_unicode_range {
        first: 0x11b as uint32,
        last: 0x11b as uint32,
    },
    pg_unicode_range {
        first: 0x11d as uint32,
        last: 0x11d as uint32,
    },
    pg_unicode_range {
        first: 0x11f as uint32,
        last: 0x11f as uint32,
    },
    pg_unicode_range {
        first: 0x121 as uint32,
        last: 0x121 as uint32,
    },
    pg_unicode_range {
        first: 0x123 as uint32,
        last: 0x123 as uint32,
    },
    pg_unicode_range {
        first: 0x125 as uint32,
        last: 0x125 as uint32,
    },
    pg_unicode_range {
        first: 0x127 as uint32,
        last: 0x127 as uint32,
    },
    pg_unicode_range {
        first: 0x129 as uint32,
        last: 0x129 as uint32,
    },
    pg_unicode_range {
        first: 0x12b as uint32,
        last: 0x12b as uint32,
    },
    pg_unicode_range {
        first: 0x12d as uint32,
        last: 0x12d as uint32,
    },
    pg_unicode_range {
        first: 0x12f as uint32,
        last: 0x12f as uint32,
    },
    pg_unicode_range {
        first: 0x131 as uint32,
        last: 0x131 as uint32,
    },
    pg_unicode_range {
        first: 0x133 as uint32,
        last: 0x133 as uint32,
    },
    pg_unicode_range {
        first: 0x135 as uint32,
        last: 0x135 as uint32,
    },
    pg_unicode_range {
        first: 0x137 as uint32,
        last: 0x138 as uint32,
    },
    pg_unicode_range {
        first: 0x13a as uint32,
        last: 0x13a as uint32,
    },
    pg_unicode_range {
        first: 0x13c as uint32,
        last: 0x13c as uint32,
    },
    pg_unicode_range {
        first: 0x13e as uint32,
        last: 0x13e as uint32,
    },
    pg_unicode_range {
        first: 0x140 as uint32,
        last: 0x140 as uint32,
    },
    pg_unicode_range {
        first: 0x142 as uint32,
        last: 0x142 as uint32,
    },
    pg_unicode_range {
        first: 0x144 as uint32,
        last: 0x144 as uint32,
    },
    pg_unicode_range {
        first: 0x146 as uint32,
        last: 0x146 as uint32,
    },
    pg_unicode_range {
        first: 0x148 as uint32,
        last: 0x149 as uint32,
    },
    pg_unicode_range {
        first: 0x14b as uint32,
        last: 0x14b as uint32,
    },
    pg_unicode_range {
        first: 0x14d as uint32,
        last: 0x14d as uint32,
    },
    pg_unicode_range {
        first: 0x14f as uint32,
        last: 0x14f as uint32,
    },
    pg_unicode_range {
        first: 0x151 as uint32,
        last: 0x151 as uint32,
    },
    pg_unicode_range {
        first: 0x153 as uint32,
        last: 0x153 as uint32,
    },
    pg_unicode_range {
        first: 0x155 as uint32,
        last: 0x155 as uint32,
    },
    pg_unicode_range {
        first: 0x157 as uint32,
        last: 0x157 as uint32,
    },
    pg_unicode_range {
        first: 0x159 as uint32,
        last: 0x159 as uint32,
    },
    pg_unicode_range {
        first: 0x15b as uint32,
        last: 0x15b as uint32,
    },
    pg_unicode_range {
        first: 0x15d as uint32,
        last: 0x15d as uint32,
    },
    pg_unicode_range {
        first: 0x15f as uint32,
        last: 0x15f as uint32,
    },
    pg_unicode_range {
        first: 0x161 as uint32,
        last: 0x161 as uint32,
    },
    pg_unicode_range {
        first: 0x163 as uint32,
        last: 0x163 as uint32,
    },
    pg_unicode_range {
        first: 0x165 as uint32,
        last: 0x165 as uint32,
    },
    pg_unicode_range {
        first: 0x167 as uint32,
        last: 0x167 as uint32,
    },
    pg_unicode_range {
        first: 0x169 as uint32,
        last: 0x169 as uint32,
    },
    pg_unicode_range {
        first: 0x16b as uint32,
        last: 0x16b as uint32,
    },
    pg_unicode_range {
        first: 0x16d as uint32,
        last: 0x16d as uint32,
    },
    pg_unicode_range {
        first: 0x16f as uint32,
        last: 0x16f as uint32,
    },
    pg_unicode_range {
        first: 0x171 as uint32,
        last: 0x171 as uint32,
    },
    pg_unicode_range {
        first: 0x173 as uint32,
        last: 0x173 as uint32,
    },
    pg_unicode_range {
        first: 0x175 as uint32,
        last: 0x175 as uint32,
    },
    pg_unicode_range {
        first: 0x177 as uint32,
        last: 0x177 as uint32,
    },
    pg_unicode_range {
        first: 0x17a as uint32,
        last: 0x17a as uint32,
    },
    pg_unicode_range {
        first: 0x17c as uint32,
        last: 0x17c as uint32,
    },
    pg_unicode_range {
        first: 0x17e as uint32,
        last: 0x180 as uint32,
    },
    pg_unicode_range {
        first: 0x183 as uint32,
        last: 0x183 as uint32,
    },
    pg_unicode_range {
        first: 0x185 as uint32,
        last: 0x185 as uint32,
    },
    pg_unicode_range {
        first: 0x188 as uint32,
        last: 0x188 as uint32,
    },
    pg_unicode_range {
        first: 0x18c as uint32,
        last: 0x18d as uint32,
    },
    pg_unicode_range {
        first: 0x192 as uint32,
        last: 0x192 as uint32,
    },
    pg_unicode_range {
        first: 0x195 as uint32,
        last: 0x195 as uint32,
    },
    pg_unicode_range {
        first: 0x199 as uint32,
        last: 0x19b as uint32,
    },
    pg_unicode_range {
        first: 0x19e as uint32,
        last: 0x19e as uint32,
    },
    pg_unicode_range {
        first: 0x1a1 as uint32,
        last: 0x1a1 as uint32,
    },
    pg_unicode_range {
        first: 0x1a3 as uint32,
        last: 0x1a3 as uint32,
    },
    pg_unicode_range {
        first: 0x1a5 as uint32,
        last: 0x1a5 as uint32,
    },
    pg_unicode_range {
        first: 0x1a8 as uint32,
        last: 0x1a8 as uint32,
    },
    pg_unicode_range {
        first: 0x1aa as uint32,
        last: 0x1ab as uint32,
    },
    pg_unicode_range {
        first: 0x1ad as uint32,
        last: 0x1ad as uint32,
    },
    pg_unicode_range {
        first: 0x1b0 as uint32,
        last: 0x1b0 as uint32,
    },
    pg_unicode_range {
        first: 0x1b4 as uint32,
        last: 0x1b4 as uint32,
    },
    pg_unicode_range {
        first: 0x1b6 as uint32,
        last: 0x1b6 as uint32,
    },
    pg_unicode_range {
        first: 0x1b9 as uint32,
        last: 0x1ba as uint32,
    },
    pg_unicode_range {
        first: 0x1bd as uint32,
        last: 0x1bf as uint32,
    },
    pg_unicode_range {
        first: 0x1c6 as uint32,
        last: 0x1c6 as uint32,
    },
    pg_unicode_range {
        first: 0x1c9 as uint32,
        last: 0x1c9 as uint32,
    },
    pg_unicode_range {
        first: 0x1cc as uint32,
        last: 0x1cc as uint32,
    },
    pg_unicode_range {
        first: 0x1ce as uint32,
        last: 0x1ce as uint32,
    },
    pg_unicode_range {
        first: 0x1d0 as uint32,
        last: 0x1d0 as uint32,
    },
    pg_unicode_range {
        first: 0x1d2 as uint32,
        last: 0x1d2 as uint32,
    },
    pg_unicode_range {
        first: 0x1d4 as uint32,
        last: 0x1d4 as uint32,
    },
    pg_unicode_range {
        first: 0x1d6 as uint32,
        last: 0x1d6 as uint32,
    },
    pg_unicode_range {
        first: 0x1d8 as uint32,
        last: 0x1d8 as uint32,
    },
    pg_unicode_range {
        first: 0x1da as uint32,
        last: 0x1da as uint32,
    },
    pg_unicode_range {
        first: 0x1dc as uint32,
        last: 0x1dd as uint32,
    },
    pg_unicode_range {
        first: 0x1df as uint32,
        last: 0x1df as uint32,
    },
    pg_unicode_range {
        first: 0x1e1 as uint32,
        last: 0x1e1 as uint32,
    },
    pg_unicode_range {
        first: 0x1e3 as uint32,
        last: 0x1e3 as uint32,
    },
    pg_unicode_range {
        first: 0x1e5 as uint32,
        last: 0x1e5 as uint32,
    },
    pg_unicode_range {
        first: 0x1e7 as uint32,
        last: 0x1e7 as uint32,
    },
    pg_unicode_range {
        first: 0x1e9 as uint32,
        last: 0x1e9 as uint32,
    },
    pg_unicode_range {
        first: 0x1eb as uint32,
        last: 0x1eb as uint32,
    },
    pg_unicode_range {
        first: 0x1ed as uint32,
        last: 0x1ed as uint32,
    },
    pg_unicode_range {
        first: 0x1ef as uint32,
        last: 0x1f0 as uint32,
    },
    pg_unicode_range {
        first: 0x1f3 as uint32,
        last: 0x1f3 as uint32,
    },
    pg_unicode_range {
        first: 0x1f5 as uint32,
        last: 0x1f5 as uint32,
    },
    pg_unicode_range {
        first: 0x1f9 as uint32,
        last: 0x1f9 as uint32,
    },
    pg_unicode_range {
        first: 0x1fb as uint32,
        last: 0x1fb as uint32,
    },
    pg_unicode_range {
        first: 0x1fd as uint32,
        last: 0x1fd as uint32,
    },
    pg_unicode_range {
        first: 0x1ff as uint32,
        last: 0x1ff as uint32,
    },
    pg_unicode_range {
        first: 0x201 as uint32,
        last: 0x201 as uint32,
    },
    pg_unicode_range {
        first: 0x203 as uint32,
        last: 0x203 as uint32,
    },
    pg_unicode_range {
        first: 0x205 as uint32,
        last: 0x205 as uint32,
    },
    pg_unicode_range {
        first: 0x207 as uint32,
        last: 0x207 as uint32,
    },
    pg_unicode_range {
        first: 0x209 as uint32,
        last: 0x209 as uint32,
    },
    pg_unicode_range {
        first: 0x20b as uint32,
        last: 0x20b as uint32,
    },
    pg_unicode_range {
        first: 0x20d as uint32,
        last: 0x20d as uint32,
    },
    pg_unicode_range {
        first: 0x20f as uint32,
        last: 0x20f as uint32,
    },
    pg_unicode_range {
        first: 0x211 as uint32,
        last: 0x211 as uint32,
    },
    pg_unicode_range {
        first: 0x213 as uint32,
        last: 0x213 as uint32,
    },
    pg_unicode_range {
        first: 0x215 as uint32,
        last: 0x215 as uint32,
    },
    pg_unicode_range {
        first: 0x217 as uint32,
        last: 0x217 as uint32,
    },
    pg_unicode_range {
        first: 0x219 as uint32,
        last: 0x219 as uint32,
    },
    pg_unicode_range {
        first: 0x21b as uint32,
        last: 0x21b as uint32,
    },
    pg_unicode_range {
        first: 0x21d as uint32,
        last: 0x21d as uint32,
    },
    pg_unicode_range {
        first: 0x21f as uint32,
        last: 0x21f as uint32,
    },
    pg_unicode_range {
        first: 0x221 as uint32,
        last: 0x221 as uint32,
    },
    pg_unicode_range {
        first: 0x223 as uint32,
        last: 0x223 as uint32,
    },
    pg_unicode_range {
        first: 0x225 as uint32,
        last: 0x225 as uint32,
    },
    pg_unicode_range {
        first: 0x227 as uint32,
        last: 0x227 as uint32,
    },
    pg_unicode_range {
        first: 0x229 as uint32,
        last: 0x229 as uint32,
    },
    pg_unicode_range {
        first: 0x22b as uint32,
        last: 0x22b as uint32,
    },
    pg_unicode_range {
        first: 0x22d as uint32,
        last: 0x22d as uint32,
    },
    pg_unicode_range {
        first: 0x22f as uint32,
        last: 0x22f as uint32,
    },
    pg_unicode_range {
        first: 0x231 as uint32,
        last: 0x231 as uint32,
    },
    pg_unicode_range {
        first: 0x233 as uint32,
        last: 0x239 as uint32,
    },
    pg_unicode_range {
        first: 0x23c as uint32,
        last: 0x23c as uint32,
    },
    pg_unicode_range {
        first: 0x23f as uint32,
        last: 0x240 as uint32,
    },
    pg_unicode_range {
        first: 0x242 as uint32,
        last: 0x242 as uint32,
    },
    pg_unicode_range {
        first: 0x247 as uint32,
        last: 0x247 as uint32,
    },
    pg_unicode_range {
        first: 0x249 as uint32,
        last: 0x249 as uint32,
    },
    pg_unicode_range {
        first: 0x24b as uint32,
        last: 0x24b as uint32,
    },
    pg_unicode_range {
        first: 0x24d as uint32,
        last: 0x24d as uint32,
    },
    pg_unicode_range {
        first: 0x24f as uint32,
        last: 0x293 as uint32,
    },
    pg_unicode_range {
        first: 0x295 as uint32,
        last: 0x2af as uint32,
    },
    pg_unicode_range {
        first: 0x2b0 as uint32,
        last: 0x2b8 as uint32,
    },
    pg_unicode_range {
        first: 0x2c0 as uint32,
        last: 0x2c1 as uint32,
    },
    pg_unicode_range {
        first: 0x2e0 as uint32,
        last: 0x2e4 as uint32,
    },
    pg_unicode_range {
        first: 0x345 as uint32,
        last: 0x345 as uint32,
    },
    pg_unicode_range {
        first: 0x371 as uint32,
        last: 0x371 as uint32,
    },
    pg_unicode_range {
        first: 0x373 as uint32,
        last: 0x373 as uint32,
    },
    pg_unicode_range {
        first: 0x377 as uint32,
        last: 0x377 as uint32,
    },
    pg_unicode_range {
        first: 0x37a as uint32,
        last: 0x37a as uint32,
    },
    pg_unicode_range {
        first: 0x37b as uint32,
        last: 0x37d as uint32,
    },
    pg_unicode_range {
        first: 0x390 as uint32,
        last: 0x390 as uint32,
    },
    pg_unicode_range {
        first: 0x3ac as uint32,
        last: 0x3ce as uint32,
    },
    pg_unicode_range {
        first: 0x3d0 as uint32,
        last: 0x3d1 as uint32,
    },
    pg_unicode_range {
        first: 0x3d5 as uint32,
        last: 0x3d7 as uint32,
    },
    pg_unicode_range {
        first: 0x3d9 as uint32,
        last: 0x3d9 as uint32,
    },
    pg_unicode_range {
        first: 0x3db as uint32,
        last: 0x3db as uint32,
    },
    pg_unicode_range {
        first: 0x3dd as uint32,
        last: 0x3dd as uint32,
    },
    pg_unicode_range {
        first: 0x3df as uint32,
        last: 0x3df as uint32,
    },
    pg_unicode_range {
        first: 0x3e1 as uint32,
        last: 0x3e1 as uint32,
    },
    pg_unicode_range {
        first: 0x3e3 as uint32,
        last: 0x3e3 as uint32,
    },
    pg_unicode_range {
        first: 0x3e5 as uint32,
        last: 0x3e5 as uint32,
    },
    pg_unicode_range {
        first: 0x3e7 as uint32,
        last: 0x3e7 as uint32,
    },
    pg_unicode_range {
        first: 0x3e9 as uint32,
        last: 0x3e9 as uint32,
    },
    pg_unicode_range {
        first: 0x3eb as uint32,
        last: 0x3eb as uint32,
    },
    pg_unicode_range {
        first: 0x3ed as uint32,
        last: 0x3ed as uint32,
    },
    pg_unicode_range {
        first: 0x3ef as uint32,
        last: 0x3f3 as uint32,
    },
    pg_unicode_range {
        first: 0x3f5 as uint32,
        last: 0x3f5 as uint32,
    },
    pg_unicode_range {
        first: 0x3f8 as uint32,
        last: 0x3f8 as uint32,
    },
    pg_unicode_range {
        first: 0x3fb as uint32,
        last: 0x3fc as uint32,
    },
    pg_unicode_range {
        first: 0x430 as uint32,
        last: 0x45f as uint32,
    },
    pg_unicode_range {
        first: 0x461 as uint32,
        last: 0x461 as uint32,
    },
    pg_unicode_range {
        first: 0x463 as uint32,
        last: 0x463 as uint32,
    },
    pg_unicode_range {
        first: 0x465 as uint32,
        last: 0x465 as uint32,
    },
    pg_unicode_range {
        first: 0x467 as uint32,
        last: 0x467 as uint32,
    },
    pg_unicode_range {
        first: 0x469 as uint32,
        last: 0x469 as uint32,
    },
    pg_unicode_range {
        first: 0x46b as uint32,
        last: 0x46b as uint32,
    },
    pg_unicode_range {
        first: 0x46d as uint32,
        last: 0x46d as uint32,
    },
    pg_unicode_range {
        first: 0x46f as uint32,
        last: 0x46f as uint32,
    },
    pg_unicode_range {
        first: 0x471 as uint32,
        last: 0x471 as uint32,
    },
    pg_unicode_range {
        first: 0x473 as uint32,
        last: 0x473 as uint32,
    },
    pg_unicode_range {
        first: 0x475 as uint32,
        last: 0x475 as uint32,
    },
    pg_unicode_range {
        first: 0x477 as uint32,
        last: 0x477 as uint32,
    },
    pg_unicode_range {
        first: 0x479 as uint32,
        last: 0x479 as uint32,
    },
    pg_unicode_range {
        first: 0x47b as uint32,
        last: 0x47b as uint32,
    },
    pg_unicode_range {
        first: 0x47d as uint32,
        last: 0x47d as uint32,
    },
    pg_unicode_range {
        first: 0x47f as uint32,
        last: 0x47f as uint32,
    },
    pg_unicode_range {
        first: 0x481 as uint32,
        last: 0x481 as uint32,
    },
    pg_unicode_range {
        first: 0x48b as uint32,
        last: 0x48b as uint32,
    },
    pg_unicode_range {
        first: 0x48d as uint32,
        last: 0x48d as uint32,
    },
    pg_unicode_range {
        first: 0x48f as uint32,
        last: 0x48f as uint32,
    },
    pg_unicode_range {
        first: 0x491 as uint32,
        last: 0x491 as uint32,
    },
    pg_unicode_range {
        first: 0x493 as uint32,
        last: 0x493 as uint32,
    },
    pg_unicode_range {
        first: 0x495 as uint32,
        last: 0x495 as uint32,
    },
    pg_unicode_range {
        first: 0x497 as uint32,
        last: 0x497 as uint32,
    },
    pg_unicode_range {
        first: 0x499 as uint32,
        last: 0x499 as uint32,
    },
    pg_unicode_range {
        first: 0x49b as uint32,
        last: 0x49b as uint32,
    },
    pg_unicode_range {
        first: 0x49d as uint32,
        last: 0x49d as uint32,
    },
    pg_unicode_range {
        first: 0x49f as uint32,
        last: 0x49f as uint32,
    },
    pg_unicode_range {
        first: 0x4a1 as uint32,
        last: 0x4a1 as uint32,
    },
    pg_unicode_range {
        first: 0x4a3 as uint32,
        last: 0x4a3 as uint32,
    },
    pg_unicode_range {
        first: 0x4a5 as uint32,
        last: 0x4a5 as uint32,
    },
    pg_unicode_range {
        first: 0x4a7 as uint32,
        last: 0x4a7 as uint32,
    },
    pg_unicode_range {
        first: 0x4a9 as uint32,
        last: 0x4a9 as uint32,
    },
    pg_unicode_range {
        first: 0x4ab as uint32,
        last: 0x4ab as uint32,
    },
    pg_unicode_range {
        first: 0x4ad as uint32,
        last: 0x4ad as uint32,
    },
    pg_unicode_range {
        first: 0x4af as uint32,
        last: 0x4af as uint32,
    },
    pg_unicode_range {
        first: 0x4b1 as uint32,
        last: 0x4b1 as uint32,
    },
    pg_unicode_range {
        first: 0x4b3 as uint32,
        last: 0x4b3 as uint32,
    },
    pg_unicode_range {
        first: 0x4b5 as uint32,
        last: 0x4b5 as uint32,
    },
    pg_unicode_range {
        first: 0x4b7 as uint32,
        last: 0x4b7 as uint32,
    },
    pg_unicode_range {
        first: 0x4b9 as uint32,
        last: 0x4b9 as uint32,
    },
    pg_unicode_range {
        first: 0x4bb as uint32,
        last: 0x4bb as uint32,
    },
    pg_unicode_range {
        first: 0x4bd as uint32,
        last: 0x4bd as uint32,
    },
    pg_unicode_range {
        first: 0x4bf as uint32,
        last: 0x4bf as uint32,
    },
    pg_unicode_range {
        first: 0x4c2 as uint32,
        last: 0x4c2 as uint32,
    },
    pg_unicode_range {
        first: 0x4c4 as uint32,
        last: 0x4c4 as uint32,
    },
    pg_unicode_range {
        first: 0x4c6 as uint32,
        last: 0x4c6 as uint32,
    },
    pg_unicode_range {
        first: 0x4c8 as uint32,
        last: 0x4c8 as uint32,
    },
    pg_unicode_range {
        first: 0x4ca as uint32,
        last: 0x4ca as uint32,
    },
    pg_unicode_range {
        first: 0x4cc as uint32,
        last: 0x4cc as uint32,
    },
    pg_unicode_range {
        first: 0x4ce as uint32,
        last: 0x4cf as uint32,
    },
    pg_unicode_range {
        first: 0x4d1 as uint32,
        last: 0x4d1 as uint32,
    },
    pg_unicode_range {
        first: 0x4d3 as uint32,
        last: 0x4d3 as uint32,
    },
    pg_unicode_range {
        first: 0x4d5 as uint32,
        last: 0x4d5 as uint32,
    },
    pg_unicode_range {
        first: 0x4d7 as uint32,
        last: 0x4d7 as uint32,
    },
    pg_unicode_range {
        first: 0x4d9 as uint32,
        last: 0x4d9 as uint32,
    },
    pg_unicode_range {
        first: 0x4db as uint32,
        last: 0x4db as uint32,
    },
    pg_unicode_range {
        first: 0x4dd as uint32,
        last: 0x4dd as uint32,
    },
    pg_unicode_range {
        first: 0x4df as uint32,
        last: 0x4df as uint32,
    },
    pg_unicode_range {
        first: 0x4e1 as uint32,
        last: 0x4e1 as uint32,
    },
    pg_unicode_range {
        first: 0x4e3 as uint32,
        last: 0x4e3 as uint32,
    },
    pg_unicode_range {
        first: 0x4e5 as uint32,
        last: 0x4e5 as uint32,
    },
    pg_unicode_range {
        first: 0x4e7 as uint32,
        last: 0x4e7 as uint32,
    },
    pg_unicode_range {
        first: 0x4e9 as uint32,
        last: 0x4e9 as uint32,
    },
    pg_unicode_range {
        first: 0x4eb as uint32,
        last: 0x4eb as uint32,
    },
    pg_unicode_range {
        first: 0x4ed as uint32,
        last: 0x4ed as uint32,
    },
    pg_unicode_range {
        first: 0x4ef as uint32,
        last: 0x4ef as uint32,
    },
    pg_unicode_range {
        first: 0x4f1 as uint32,
        last: 0x4f1 as uint32,
    },
    pg_unicode_range {
        first: 0x4f3 as uint32,
        last: 0x4f3 as uint32,
    },
    pg_unicode_range {
        first: 0x4f5 as uint32,
        last: 0x4f5 as uint32,
    },
    pg_unicode_range {
        first: 0x4f7 as uint32,
        last: 0x4f7 as uint32,
    },
    pg_unicode_range {
        first: 0x4f9 as uint32,
        last: 0x4f9 as uint32,
    },
    pg_unicode_range {
        first: 0x4fb as uint32,
        last: 0x4fb as uint32,
    },
    pg_unicode_range {
        first: 0x4fd as uint32,
        last: 0x4fd as uint32,
    },
    pg_unicode_range {
        first: 0x4ff as uint32,
        last: 0x4ff as uint32,
    },
    pg_unicode_range {
        first: 0x501 as uint32,
        last: 0x501 as uint32,
    },
    pg_unicode_range {
        first: 0x503 as uint32,
        last: 0x503 as uint32,
    },
    pg_unicode_range {
        first: 0x505 as uint32,
        last: 0x505 as uint32,
    },
    pg_unicode_range {
        first: 0x507 as uint32,
        last: 0x507 as uint32,
    },
    pg_unicode_range {
        first: 0x509 as uint32,
        last: 0x509 as uint32,
    },
    pg_unicode_range {
        first: 0x50b as uint32,
        last: 0x50b as uint32,
    },
    pg_unicode_range {
        first: 0x50d as uint32,
        last: 0x50d as uint32,
    },
    pg_unicode_range {
        first: 0x50f as uint32,
        last: 0x50f as uint32,
    },
    pg_unicode_range {
        first: 0x511 as uint32,
        last: 0x511 as uint32,
    },
    pg_unicode_range {
        first: 0x513 as uint32,
        last: 0x513 as uint32,
    },
    pg_unicode_range {
        first: 0x515 as uint32,
        last: 0x515 as uint32,
    },
    pg_unicode_range {
        first: 0x517 as uint32,
        last: 0x517 as uint32,
    },
    pg_unicode_range {
        first: 0x519 as uint32,
        last: 0x519 as uint32,
    },
    pg_unicode_range {
        first: 0x51b as uint32,
        last: 0x51b as uint32,
    },
    pg_unicode_range {
        first: 0x51d as uint32,
        last: 0x51d as uint32,
    },
    pg_unicode_range {
        first: 0x51f as uint32,
        last: 0x51f as uint32,
    },
    pg_unicode_range {
        first: 0x521 as uint32,
        last: 0x521 as uint32,
    },
    pg_unicode_range {
        first: 0x523 as uint32,
        last: 0x523 as uint32,
    },
    pg_unicode_range {
        first: 0x525 as uint32,
        last: 0x525 as uint32,
    },
    pg_unicode_range {
        first: 0x527 as uint32,
        last: 0x527 as uint32,
    },
    pg_unicode_range {
        first: 0x529 as uint32,
        last: 0x529 as uint32,
    },
    pg_unicode_range {
        first: 0x52b as uint32,
        last: 0x52b as uint32,
    },
    pg_unicode_range {
        first: 0x52d as uint32,
        last: 0x52d as uint32,
    },
    pg_unicode_range {
        first: 0x52f as uint32,
        last: 0x52f as uint32,
    },
    pg_unicode_range {
        first: 0x560 as uint32,
        last: 0x588 as uint32,
    },
    pg_unicode_range {
        first: 0x10d0 as uint32,
        last: 0x10fa as uint32,
    },
    pg_unicode_range {
        first: 0x10fc as uint32,
        last: 0x10fc as uint32,
    },
    pg_unicode_range {
        first: 0x10fd as uint32,
        last: 0x10ff as uint32,
    },
    pg_unicode_range {
        first: 0x13f8 as uint32,
        last: 0x13fd as uint32,
    },
    pg_unicode_range {
        first: 0x1c80 as uint32,
        last: 0x1c88 as uint32,
    },
    pg_unicode_range {
        first: 0x1c8a as uint32,
        last: 0x1c8a as uint32,
    },
    pg_unicode_range {
        first: 0x1d00 as uint32,
        last: 0x1d2b as uint32,
    },
    pg_unicode_range {
        first: 0x1d2c as uint32,
        last: 0x1d6a as uint32,
    },
    pg_unicode_range {
        first: 0x1d6b as uint32,
        last: 0x1d77 as uint32,
    },
    pg_unicode_range {
        first: 0x1d78 as uint32,
        last: 0x1d78 as uint32,
    },
    pg_unicode_range {
        first: 0x1d79 as uint32,
        last: 0x1d9a as uint32,
    },
    pg_unicode_range {
        first: 0x1d9b as uint32,
        last: 0x1dbf as uint32,
    },
    pg_unicode_range {
        first: 0x1e01 as uint32,
        last: 0x1e01 as uint32,
    },
    pg_unicode_range {
        first: 0x1e03 as uint32,
        last: 0x1e03 as uint32,
    },
    pg_unicode_range {
        first: 0x1e05 as uint32,
        last: 0x1e05 as uint32,
    },
    pg_unicode_range {
        first: 0x1e07 as uint32,
        last: 0x1e07 as uint32,
    },
    pg_unicode_range {
        first: 0x1e09 as uint32,
        last: 0x1e09 as uint32,
    },
    pg_unicode_range {
        first: 0x1e0b as uint32,
        last: 0x1e0b as uint32,
    },
    pg_unicode_range {
        first: 0x1e0d as uint32,
        last: 0x1e0d as uint32,
    },
    pg_unicode_range {
        first: 0x1e0f as uint32,
        last: 0x1e0f as uint32,
    },
    pg_unicode_range {
        first: 0x1e11 as uint32,
        last: 0x1e11 as uint32,
    },
    pg_unicode_range {
        first: 0x1e13 as uint32,
        last: 0x1e13 as uint32,
    },
    pg_unicode_range {
        first: 0x1e15 as uint32,
        last: 0x1e15 as uint32,
    },
    pg_unicode_range {
        first: 0x1e17 as uint32,
        last: 0x1e17 as uint32,
    },
    pg_unicode_range {
        first: 0x1e19 as uint32,
        last: 0x1e19 as uint32,
    },
    pg_unicode_range {
        first: 0x1e1b as uint32,
        last: 0x1e1b as uint32,
    },
    pg_unicode_range {
        first: 0x1e1d as uint32,
        last: 0x1e1d as uint32,
    },
    pg_unicode_range {
        first: 0x1e1f as uint32,
        last: 0x1e1f as uint32,
    },
    pg_unicode_range {
        first: 0x1e21 as uint32,
        last: 0x1e21 as uint32,
    },
    pg_unicode_range {
        first: 0x1e23 as uint32,
        last: 0x1e23 as uint32,
    },
    pg_unicode_range {
        first: 0x1e25 as uint32,
        last: 0x1e25 as uint32,
    },
    pg_unicode_range {
        first: 0x1e27 as uint32,
        last: 0x1e27 as uint32,
    },
    pg_unicode_range {
        first: 0x1e29 as uint32,
        last: 0x1e29 as uint32,
    },
    pg_unicode_range {
        first: 0x1e2b as uint32,
        last: 0x1e2b as uint32,
    },
    pg_unicode_range {
        first: 0x1e2d as uint32,
        last: 0x1e2d as uint32,
    },
    pg_unicode_range {
        first: 0x1e2f as uint32,
        last: 0x1e2f as uint32,
    },
    pg_unicode_range {
        first: 0x1e31 as uint32,
        last: 0x1e31 as uint32,
    },
    pg_unicode_range {
        first: 0x1e33 as uint32,
        last: 0x1e33 as uint32,
    },
    pg_unicode_range {
        first: 0x1e35 as uint32,
        last: 0x1e35 as uint32,
    },
    pg_unicode_range {
        first: 0x1e37 as uint32,
        last: 0x1e37 as uint32,
    },
    pg_unicode_range {
        first: 0x1e39 as uint32,
        last: 0x1e39 as uint32,
    },
    pg_unicode_range {
        first: 0x1e3b as uint32,
        last: 0x1e3b as uint32,
    },
    pg_unicode_range {
        first: 0x1e3d as uint32,
        last: 0x1e3d as uint32,
    },
    pg_unicode_range {
        first: 0x1e3f as uint32,
        last: 0x1e3f as uint32,
    },
    pg_unicode_range {
        first: 0x1e41 as uint32,
        last: 0x1e41 as uint32,
    },
    pg_unicode_range {
        first: 0x1e43 as uint32,
        last: 0x1e43 as uint32,
    },
    pg_unicode_range {
        first: 0x1e45 as uint32,
        last: 0x1e45 as uint32,
    },
    pg_unicode_range {
        first: 0x1e47 as uint32,
        last: 0x1e47 as uint32,
    },
    pg_unicode_range {
        first: 0x1e49 as uint32,
        last: 0x1e49 as uint32,
    },
    pg_unicode_range {
        first: 0x1e4b as uint32,
        last: 0x1e4b as uint32,
    },
    pg_unicode_range {
        first: 0x1e4d as uint32,
        last: 0x1e4d as uint32,
    },
    pg_unicode_range {
        first: 0x1e4f as uint32,
        last: 0x1e4f as uint32,
    },
    pg_unicode_range {
        first: 0x1e51 as uint32,
        last: 0x1e51 as uint32,
    },
    pg_unicode_range {
        first: 0x1e53 as uint32,
        last: 0x1e53 as uint32,
    },
    pg_unicode_range {
        first: 0x1e55 as uint32,
        last: 0x1e55 as uint32,
    },
    pg_unicode_range {
        first: 0x1e57 as uint32,
        last: 0x1e57 as uint32,
    },
    pg_unicode_range {
        first: 0x1e59 as uint32,
        last: 0x1e59 as uint32,
    },
    pg_unicode_range {
        first: 0x1e5b as uint32,
        last: 0x1e5b as uint32,
    },
    pg_unicode_range {
        first: 0x1e5d as uint32,
        last: 0x1e5d as uint32,
    },
    pg_unicode_range {
        first: 0x1e5f as uint32,
        last: 0x1e5f as uint32,
    },
    pg_unicode_range {
        first: 0x1e61 as uint32,
        last: 0x1e61 as uint32,
    },
    pg_unicode_range {
        first: 0x1e63 as uint32,
        last: 0x1e63 as uint32,
    },
    pg_unicode_range {
        first: 0x1e65 as uint32,
        last: 0x1e65 as uint32,
    },
    pg_unicode_range {
        first: 0x1e67 as uint32,
        last: 0x1e67 as uint32,
    },
    pg_unicode_range {
        first: 0x1e69 as uint32,
        last: 0x1e69 as uint32,
    },
    pg_unicode_range {
        first: 0x1e6b as uint32,
        last: 0x1e6b as uint32,
    },
    pg_unicode_range {
        first: 0x1e6d as uint32,
        last: 0x1e6d as uint32,
    },
    pg_unicode_range {
        first: 0x1e6f as uint32,
        last: 0x1e6f as uint32,
    },
    pg_unicode_range {
        first: 0x1e71 as uint32,
        last: 0x1e71 as uint32,
    },
    pg_unicode_range {
        first: 0x1e73 as uint32,
        last: 0x1e73 as uint32,
    },
    pg_unicode_range {
        first: 0x1e75 as uint32,
        last: 0x1e75 as uint32,
    },
    pg_unicode_range {
        first: 0x1e77 as uint32,
        last: 0x1e77 as uint32,
    },
    pg_unicode_range {
        first: 0x1e79 as uint32,
        last: 0x1e79 as uint32,
    },
    pg_unicode_range {
        first: 0x1e7b as uint32,
        last: 0x1e7b as uint32,
    },
    pg_unicode_range {
        first: 0x1e7d as uint32,
        last: 0x1e7d as uint32,
    },
    pg_unicode_range {
        first: 0x1e7f as uint32,
        last: 0x1e7f as uint32,
    },
    pg_unicode_range {
        first: 0x1e81 as uint32,
        last: 0x1e81 as uint32,
    },
    pg_unicode_range {
        first: 0x1e83 as uint32,
        last: 0x1e83 as uint32,
    },
    pg_unicode_range {
        first: 0x1e85 as uint32,
        last: 0x1e85 as uint32,
    },
    pg_unicode_range {
        first: 0x1e87 as uint32,
        last: 0x1e87 as uint32,
    },
    pg_unicode_range {
        first: 0x1e89 as uint32,
        last: 0x1e89 as uint32,
    },
    pg_unicode_range {
        first: 0x1e8b as uint32,
        last: 0x1e8b as uint32,
    },
    pg_unicode_range {
        first: 0x1e8d as uint32,
        last: 0x1e8d as uint32,
    },
    pg_unicode_range {
        first: 0x1e8f as uint32,
        last: 0x1e8f as uint32,
    },
    pg_unicode_range {
        first: 0x1e91 as uint32,
        last: 0x1e91 as uint32,
    },
    pg_unicode_range {
        first: 0x1e93 as uint32,
        last: 0x1e93 as uint32,
    },
    pg_unicode_range {
        first: 0x1e95 as uint32,
        last: 0x1e9d as uint32,
    },
    pg_unicode_range {
        first: 0x1e9f as uint32,
        last: 0x1e9f as uint32,
    },
    pg_unicode_range {
        first: 0x1ea1 as uint32,
        last: 0x1ea1 as uint32,
    },
    pg_unicode_range {
        first: 0x1ea3 as uint32,
        last: 0x1ea3 as uint32,
    },
    pg_unicode_range {
        first: 0x1ea5 as uint32,
        last: 0x1ea5 as uint32,
    },
    pg_unicode_range {
        first: 0x1ea7 as uint32,
        last: 0x1ea7 as uint32,
    },
    pg_unicode_range {
        first: 0x1ea9 as uint32,
        last: 0x1ea9 as uint32,
    },
    pg_unicode_range {
        first: 0x1eab as uint32,
        last: 0x1eab as uint32,
    },
    pg_unicode_range {
        first: 0x1ead as uint32,
        last: 0x1ead as uint32,
    },
    pg_unicode_range {
        first: 0x1eaf as uint32,
        last: 0x1eaf as uint32,
    },
    pg_unicode_range {
        first: 0x1eb1 as uint32,
        last: 0x1eb1 as uint32,
    },
    pg_unicode_range {
        first: 0x1eb3 as uint32,
        last: 0x1eb3 as uint32,
    },
    pg_unicode_range {
        first: 0x1eb5 as uint32,
        last: 0x1eb5 as uint32,
    },
    pg_unicode_range {
        first: 0x1eb7 as uint32,
        last: 0x1eb7 as uint32,
    },
    pg_unicode_range {
        first: 0x1eb9 as uint32,
        last: 0x1eb9 as uint32,
    },
    pg_unicode_range {
        first: 0x1ebb as uint32,
        last: 0x1ebb as uint32,
    },
    pg_unicode_range {
        first: 0x1ebd as uint32,
        last: 0x1ebd as uint32,
    },
    pg_unicode_range {
        first: 0x1ebf as uint32,
        last: 0x1ebf as uint32,
    },
    pg_unicode_range {
        first: 0x1ec1 as uint32,
        last: 0x1ec1 as uint32,
    },
    pg_unicode_range {
        first: 0x1ec3 as uint32,
        last: 0x1ec3 as uint32,
    },
    pg_unicode_range {
        first: 0x1ec5 as uint32,
        last: 0x1ec5 as uint32,
    },
    pg_unicode_range {
        first: 0x1ec7 as uint32,
        last: 0x1ec7 as uint32,
    },
    pg_unicode_range {
        first: 0x1ec9 as uint32,
        last: 0x1ec9 as uint32,
    },
    pg_unicode_range {
        first: 0x1ecb as uint32,
        last: 0x1ecb as uint32,
    },
    pg_unicode_range {
        first: 0x1ecd as uint32,
        last: 0x1ecd as uint32,
    },
    pg_unicode_range {
        first: 0x1ecf as uint32,
        last: 0x1ecf as uint32,
    },
    pg_unicode_range {
        first: 0x1ed1 as uint32,
        last: 0x1ed1 as uint32,
    },
    pg_unicode_range {
        first: 0x1ed3 as uint32,
        last: 0x1ed3 as uint32,
    },
    pg_unicode_range {
        first: 0x1ed5 as uint32,
        last: 0x1ed5 as uint32,
    },
    pg_unicode_range {
        first: 0x1ed7 as uint32,
        last: 0x1ed7 as uint32,
    },
    pg_unicode_range {
        first: 0x1ed9 as uint32,
        last: 0x1ed9 as uint32,
    },
    pg_unicode_range {
        first: 0x1edb as uint32,
        last: 0x1edb as uint32,
    },
    pg_unicode_range {
        first: 0x1edd as uint32,
        last: 0x1edd as uint32,
    },
    pg_unicode_range {
        first: 0x1edf as uint32,
        last: 0x1edf as uint32,
    },
    pg_unicode_range {
        first: 0x1ee1 as uint32,
        last: 0x1ee1 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee3 as uint32,
        last: 0x1ee3 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee5 as uint32,
        last: 0x1ee5 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee7 as uint32,
        last: 0x1ee7 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee9 as uint32,
        last: 0x1ee9 as uint32,
    },
    pg_unicode_range {
        first: 0x1eeb as uint32,
        last: 0x1eeb as uint32,
    },
    pg_unicode_range {
        first: 0x1eed as uint32,
        last: 0x1eed as uint32,
    },
    pg_unicode_range {
        first: 0x1eef as uint32,
        last: 0x1eef as uint32,
    },
    pg_unicode_range {
        first: 0x1ef1 as uint32,
        last: 0x1ef1 as uint32,
    },
    pg_unicode_range {
        first: 0x1ef3 as uint32,
        last: 0x1ef3 as uint32,
    },
    pg_unicode_range {
        first: 0x1ef5 as uint32,
        last: 0x1ef5 as uint32,
    },
    pg_unicode_range {
        first: 0x1ef7 as uint32,
        last: 0x1ef7 as uint32,
    },
    pg_unicode_range {
        first: 0x1ef9 as uint32,
        last: 0x1ef9 as uint32,
    },
    pg_unicode_range {
        first: 0x1efb as uint32,
        last: 0x1efb as uint32,
    },
    pg_unicode_range {
        first: 0x1efd as uint32,
        last: 0x1efd as uint32,
    },
    pg_unicode_range {
        first: 0x1eff as uint32,
        last: 0x1f07 as uint32,
    },
    pg_unicode_range {
        first: 0x1f10 as uint32,
        last: 0x1f15 as uint32,
    },
    pg_unicode_range {
        first: 0x1f20 as uint32,
        last: 0x1f27 as uint32,
    },
    pg_unicode_range {
        first: 0x1f30 as uint32,
        last: 0x1f37 as uint32,
    },
    pg_unicode_range {
        first: 0x1f40 as uint32,
        last: 0x1f45 as uint32,
    },
    pg_unicode_range {
        first: 0x1f50 as uint32,
        last: 0x1f57 as uint32,
    },
    pg_unicode_range {
        first: 0x1f60 as uint32,
        last: 0x1f67 as uint32,
    },
    pg_unicode_range {
        first: 0x1f70 as uint32,
        last: 0x1f7d as uint32,
    },
    pg_unicode_range {
        first: 0x1f80 as uint32,
        last: 0x1f87 as uint32,
    },
    pg_unicode_range {
        first: 0x1f90 as uint32,
        last: 0x1f97 as uint32,
    },
    pg_unicode_range {
        first: 0x1fa0 as uint32,
        last: 0x1fa7 as uint32,
    },
    pg_unicode_range {
        first: 0x1fb0 as uint32,
        last: 0x1fb4 as uint32,
    },
    pg_unicode_range {
        first: 0x1fb6 as uint32,
        last: 0x1fb7 as uint32,
    },
    pg_unicode_range {
        first: 0x1fbe as uint32,
        last: 0x1fbe as uint32,
    },
    pg_unicode_range {
        first: 0x1fc2 as uint32,
        last: 0x1fc4 as uint32,
    },
    pg_unicode_range {
        first: 0x1fc6 as uint32,
        last: 0x1fc7 as uint32,
    },
    pg_unicode_range {
        first: 0x1fd0 as uint32,
        last: 0x1fd3 as uint32,
    },
    pg_unicode_range {
        first: 0x1fd6 as uint32,
        last: 0x1fd7 as uint32,
    },
    pg_unicode_range {
        first: 0x1fe0 as uint32,
        last: 0x1fe7 as uint32,
    },
    pg_unicode_range {
        first: 0x1ff2 as uint32,
        last: 0x1ff4 as uint32,
    },
    pg_unicode_range {
        first: 0x1ff6 as uint32,
        last: 0x1ff7 as uint32,
    },
    pg_unicode_range {
        first: 0x2071 as uint32,
        last: 0x2071 as uint32,
    },
    pg_unicode_range {
        first: 0x207f as uint32,
        last: 0x207f as uint32,
    },
    pg_unicode_range {
        first: 0x2090 as uint32,
        last: 0x209c as uint32,
    },
    pg_unicode_range {
        first: 0x210a as uint32,
        last: 0x210a as uint32,
    },
    pg_unicode_range {
        first: 0x210e as uint32,
        last: 0x210f as uint32,
    },
    pg_unicode_range {
        first: 0x2113 as uint32,
        last: 0x2113 as uint32,
    },
    pg_unicode_range {
        first: 0x212f as uint32,
        last: 0x212f as uint32,
    },
    pg_unicode_range {
        first: 0x2134 as uint32,
        last: 0x2134 as uint32,
    },
    pg_unicode_range {
        first: 0x2139 as uint32,
        last: 0x2139 as uint32,
    },
    pg_unicode_range {
        first: 0x213c as uint32,
        last: 0x213d as uint32,
    },
    pg_unicode_range {
        first: 0x2146 as uint32,
        last: 0x2149 as uint32,
    },
    pg_unicode_range {
        first: 0x214e as uint32,
        last: 0x214e as uint32,
    },
    pg_unicode_range {
        first: 0x2170 as uint32,
        last: 0x217f as uint32,
    },
    pg_unicode_range {
        first: 0x2184 as uint32,
        last: 0x2184 as uint32,
    },
    pg_unicode_range {
        first: 0x24d0 as uint32,
        last: 0x24e9 as uint32,
    },
    pg_unicode_range {
        first: 0x2c30 as uint32,
        last: 0x2c5f as uint32,
    },
    pg_unicode_range {
        first: 0x2c61 as uint32,
        last: 0x2c61 as uint32,
    },
    pg_unicode_range {
        first: 0x2c65 as uint32,
        last: 0x2c66 as uint32,
    },
    pg_unicode_range {
        first: 0x2c68 as uint32,
        last: 0x2c68 as uint32,
    },
    pg_unicode_range {
        first: 0x2c6a as uint32,
        last: 0x2c6a as uint32,
    },
    pg_unicode_range {
        first: 0x2c6c as uint32,
        last: 0x2c6c as uint32,
    },
    pg_unicode_range {
        first: 0x2c71 as uint32,
        last: 0x2c71 as uint32,
    },
    pg_unicode_range {
        first: 0x2c73 as uint32,
        last: 0x2c74 as uint32,
    },
    pg_unicode_range {
        first: 0x2c76 as uint32,
        last: 0x2c7b as uint32,
    },
    pg_unicode_range {
        first: 0x2c7c as uint32,
        last: 0x2c7d as uint32,
    },
    pg_unicode_range {
        first: 0x2c81 as uint32,
        last: 0x2c81 as uint32,
    },
    pg_unicode_range {
        first: 0x2c83 as uint32,
        last: 0x2c83 as uint32,
    },
    pg_unicode_range {
        first: 0x2c85 as uint32,
        last: 0x2c85 as uint32,
    },
    pg_unicode_range {
        first: 0x2c87 as uint32,
        last: 0x2c87 as uint32,
    },
    pg_unicode_range {
        first: 0x2c89 as uint32,
        last: 0x2c89 as uint32,
    },
    pg_unicode_range {
        first: 0x2c8b as uint32,
        last: 0x2c8b as uint32,
    },
    pg_unicode_range {
        first: 0x2c8d as uint32,
        last: 0x2c8d as uint32,
    },
    pg_unicode_range {
        first: 0x2c8f as uint32,
        last: 0x2c8f as uint32,
    },
    pg_unicode_range {
        first: 0x2c91 as uint32,
        last: 0x2c91 as uint32,
    },
    pg_unicode_range {
        first: 0x2c93 as uint32,
        last: 0x2c93 as uint32,
    },
    pg_unicode_range {
        first: 0x2c95 as uint32,
        last: 0x2c95 as uint32,
    },
    pg_unicode_range {
        first: 0x2c97 as uint32,
        last: 0x2c97 as uint32,
    },
    pg_unicode_range {
        first: 0x2c99 as uint32,
        last: 0x2c99 as uint32,
    },
    pg_unicode_range {
        first: 0x2c9b as uint32,
        last: 0x2c9b as uint32,
    },
    pg_unicode_range {
        first: 0x2c9d as uint32,
        last: 0x2c9d as uint32,
    },
    pg_unicode_range {
        first: 0x2c9f as uint32,
        last: 0x2c9f as uint32,
    },
    pg_unicode_range {
        first: 0x2ca1 as uint32,
        last: 0x2ca1 as uint32,
    },
    pg_unicode_range {
        first: 0x2ca3 as uint32,
        last: 0x2ca3 as uint32,
    },
    pg_unicode_range {
        first: 0x2ca5 as uint32,
        last: 0x2ca5 as uint32,
    },
    pg_unicode_range {
        first: 0x2ca7 as uint32,
        last: 0x2ca7 as uint32,
    },
    pg_unicode_range {
        first: 0x2ca9 as uint32,
        last: 0x2ca9 as uint32,
    },
    pg_unicode_range {
        first: 0x2cab as uint32,
        last: 0x2cab as uint32,
    },
    pg_unicode_range {
        first: 0x2cad as uint32,
        last: 0x2cad as uint32,
    },
    pg_unicode_range {
        first: 0x2caf as uint32,
        last: 0x2caf as uint32,
    },
    pg_unicode_range {
        first: 0x2cb1 as uint32,
        last: 0x2cb1 as uint32,
    },
    pg_unicode_range {
        first: 0x2cb3 as uint32,
        last: 0x2cb3 as uint32,
    },
    pg_unicode_range {
        first: 0x2cb5 as uint32,
        last: 0x2cb5 as uint32,
    },
    pg_unicode_range {
        first: 0x2cb7 as uint32,
        last: 0x2cb7 as uint32,
    },
    pg_unicode_range {
        first: 0x2cb9 as uint32,
        last: 0x2cb9 as uint32,
    },
    pg_unicode_range {
        first: 0x2cbb as uint32,
        last: 0x2cbb as uint32,
    },
    pg_unicode_range {
        first: 0x2cbd as uint32,
        last: 0x2cbd as uint32,
    },
    pg_unicode_range {
        first: 0x2cbf as uint32,
        last: 0x2cbf as uint32,
    },
    pg_unicode_range {
        first: 0x2cc1 as uint32,
        last: 0x2cc1 as uint32,
    },
    pg_unicode_range {
        first: 0x2cc3 as uint32,
        last: 0x2cc3 as uint32,
    },
    pg_unicode_range {
        first: 0x2cc5 as uint32,
        last: 0x2cc5 as uint32,
    },
    pg_unicode_range {
        first: 0x2cc7 as uint32,
        last: 0x2cc7 as uint32,
    },
    pg_unicode_range {
        first: 0x2cc9 as uint32,
        last: 0x2cc9 as uint32,
    },
    pg_unicode_range {
        first: 0x2ccb as uint32,
        last: 0x2ccb as uint32,
    },
    pg_unicode_range {
        first: 0x2ccd as uint32,
        last: 0x2ccd as uint32,
    },
    pg_unicode_range {
        first: 0x2ccf as uint32,
        last: 0x2ccf as uint32,
    },
    pg_unicode_range {
        first: 0x2cd1 as uint32,
        last: 0x2cd1 as uint32,
    },
    pg_unicode_range {
        first: 0x2cd3 as uint32,
        last: 0x2cd3 as uint32,
    },
    pg_unicode_range {
        first: 0x2cd5 as uint32,
        last: 0x2cd5 as uint32,
    },
    pg_unicode_range {
        first: 0x2cd7 as uint32,
        last: 0x2cd7 as uint32,
    },
    pg_unicode_range {
        first: 0x2cd9 as uint32,
        last: 0x2cd9 as uint32,
    },
    pg_unicode_range {
        first: 0x2cdb as uint32,
        last: 0x2cdb as uint32,
    },
    pg_unicode_range {
        first: 0x2cdd as uint32,
        last: 0x2cdd as uint32,
    },
    pg_unicode_range {
        first: 0x2cdf as uint32,
        last: 0x2cdf as uint32,
    },
    pg_unicode_range {
        first: 0x2ce1 as uint32,
        last: 0x2ce1 as uint32,
    },
    pg_unicode_range {
        first: 0x2ce3 as uint32,
        last: 0x2ce4 as uint32,
    },
    pg_unicode_range {
        first: 0x2cec as uint32,
        last: 0x2cec as uint32,
    },
    pg_unicode_range {
        first: 0x2cee as uint32,
        last: 0x2cee as uint32,
    },
    pg_unicode_range {
        first: 0x2cf3 as uint32,
        last: 0x2cf3 as uint32,
    },
    pg_unicode_range {
        first: 0x2d00 as uint32,
        last: 0x2d25 as uint32,
    },
    pg_unicode_range {
        first: 0x2d27 as uint32,
        last: 0x2d27 as uint32,
    },
    pg_unicode_range {
        first: 0x2d2d as uint32,
        last: 0x2d2d as uint32,
    },
    pg_unicode_range {
        first: 0xa641 as uint32,
        last: 0xa641 as uint32,
    },
    pg_unicode_range {
        first: 0xa643 as uint32,
        last: 0xa643 as uint32,
    },
    pg_unicode_range {
        first: 0xa645 as uint32,
        last: 0xa645 as uint32,
    },
    pg_unicode_range {
        first: 0xa647 as uint32,
        last: 0xa647 as uint32,
    },
    pg_unicode_range {
        first: 0xa649 as uint32,
        last: 0xa649 as uint32,
    },
    pg_unicode_range {
        first: 0xa64b as uint32,
        last: 0xa64b as uint32,
    },
    pg_unicode_range {
        first: 0xa64d as uint32,
        last: 0xa64d as uint32,
    },
    pg_unicode_range {
        first: 0xa64f as uint32,
        last: 0xa64f as uint32,
    },
    pg_unicode_range {
        first: 0xa651 as uint32,
        last: 0xa651 as uint32,
    },
    pg_unicode_range {
        first: 0xa653 as uint32,
        last: 0xa653 as uint32,
    },
    pg_unicode_range {
        first: 0xa655 as uint32,
        last: 0xa655 as uint32,
    },
    pg_unicode_range {
        first: 0xa657 as uint32,
        last: 0xa657 as uint32,
    },
    pg_unicode_range {
        first: 0xa659 as uint32,
        last: 0xa659 as uint32,
    },
    pg_unicode_range {
        first: 0xa65b as uint32,
        last: 0xa65b as uint32,
    },
    pg_unicode_range {
        first: 0xa65d as uint32,
        last: 0xa65d as uint32,
    },
    pg_unicode_range {
        first: 0xa65f as uint32,
        last: 0xa65f as uint32,
    },
    pg_unicode_range {
        first: 0xa661 as uint32,
        last: 0xa661 as uint32,
    },
    pg_unicode_range {
        first: 0xa663 as uint32,
        last: 0xa663 as uint32,
    },
    pg_unicode_range {
        first: 0xa665 as uint32,
        last: 0xa665 as uint32,
    },
    pg_unicode_range {
        first: 0xa667 as uint32,
        last: 0xa667 as uint32,
    },
    pg_unicode_range {
        first: 0xa669 as uint32,
        last: 0xa669 as uint32,
    },
    pg_unicode_range {
        first: 0xa66b as uint32,
        last: 0xa66b as uint32,
    },
    pg_unicode_range {
        first: 0xa66d as uint32,
        last: 0xa66d as uint32,
    },
    pg_unicode_range {
        first: 0xa681 as uint32,
        last: 0xa681 as uint32,
    },
    pg_unicode_range {
        first: 0xa683 as uint32,
        last: 0xa683 as uint32,
    },
    pg_unicode_range {
        first: 0xa685 as uint32,
        last: 0xa685 as uint32,
    },
    pg_unicode_range {
        first: 0xa687 as uint32,
        last: 0xa687 as uint32,
    },
    pg_unicode_range {
        first: 0xa689 as uint32,
        last: 0xa689 as uint32,
    },
    pg_unicode_range {
        first: 0xa68b as uint32,
        last: 0xa68b as uint32,
    },
    pg_unicode_range {
        first: 0xa68d as uint32,
        last: 0xa68d as uint32,
    },
    pg_unicode_range {
        first: 0xa68f as uint32,
        last: 0xa68f as uint32,
    },
    pg_unicode_range {
        first: 0xa691 as uint32,
        last: 0xa691 as uint32,
    },
    pg_unicode_range {
        first: 0xa693 as uint32,
        last: 0xa693 as uint32,
    },
    pg_unicode_range {
        first: 0xa695 as uint32,
        last: 0xa695 as uint32,
    },
    pg_unicode_range {
        first: 0xa697 as uint32,
        last: 0xa697 as uint32,
    },
    pg_unicode_range {
        first: 0xa699 as uint32,
        last: 0xa699 as uint32,
    },
    pg_unicode_range {
        first: 0xa69b as uint32,
        last: 0xa69b as uint32,
    },
    pg_unicode_range {
        first: 0xa69c as uint32,
        last: 0xa69d as uint32,
    },
    pg_unicode_range {
        first: 0xa723 as uint32,
        last: 0xa723 as uint32,
    },
    pg_unicode_range {
        first: 0xa725 as uint32,
        last: 0xa725 as uint32,
    },
    pg_unicode_range {
        first: 0xa727 as uint32,
        last: 0xa727 as uint32,
    },
    pg_unicode_range {
        first: 0xa729 as uint32,
        last: 0xa729 as uint32,
    },
    pg_unicode_range {
        first: 0xa72b as uint32,
        last: 0xa72b as uint32,
    },
    pg_unicode_range {
        first: 0xa72d as uint32,
        last: 0xa72d as uint32,
    },
    pg_unicode_range {
        first: 0xa72f as uint32,
        last: 0xa731 as uint32,
    },
    pg_unicode_range {
        first: 0xa733 as uint32,
        last: 0xa733 as uint32,
    },
    pg_unicode_range {
        first: 0xa735 as uint32,
        last: 0xa735 as uint32,
    },
    pg_unicode_range {
        first: 0xa737 as uint32,
        last: 0xa737 as uint32,
    },
    pg_unicode_range {
        first: 0xa739 as uint32,
        last: 0xa739 as uint32,
    },
    pg_unicode_range {
        first: 0xa73b as uint32,
        last: 0xa73b as uint32,
    },
    pg_unicode_range {
        first: 0xa73d as uint32,
        last: 0xa73d as uint32,
    },
    pg_unicode_range {
        first: 0xa73f as uint32,
        last: 0xa73f as uint32,
    },
    pg_unicode_range {
        first: 0xa741 as uint32,
        last: 0xa741 as uint32,
    },
    pg_unicode_range {
        first: 0xa743 as uint32,
        last: 0xa743 as uint32,
    },
    pg_unicode_range {
        first: 0xa745 as uint32,
        last: 0xa745 as uint32,
    },
    pg_unicode_range {
        first: 0xa747 as uint32,
        last: 0xa747 as uint32,
    },
    pg_unicode_range {
        first: 0xa749 as uint32,
        last: 0xa749 as uint32,
    },
    pg_unicode_range {
        first: 0xa74b as uint32,
        last: 0xa74b as uint32,
    },
    pg_unicode_range {
        first: 0xa74d as uint32,
        last: 0xa74d as uint32,
    },
    pg_unicode_range {
        first: 0xa74f as uint32,
        last: 0xa74f as uint32,
    },
    pg_unicode_range {
        first: 0xa751 as uint32,
        last: 0xa751 as uint32,
    },
    pg_unicode_range {
        first: 0xa753 as uint32,
        last: 0xa753 as uint32,
    },
    pg_unicode_range {
        first: 0xa755 as uint32,
        last: 0xa755 as uint32,
    },
    pg_unicode_range {
        first: 0xa757 as uint32,
        last: 0xa757 as uint32,
    },
    pg_unicode_range {
        first: 0xa759 as uint32,
        last: 0xa759 as uint32,
    },
    pg_unicode_range {
        first: 0xa75b as uint32,
        last: 0xa75b as uint32,
    },
    pg_unicode_range {
        first: 0xa75d as uint32,
        last: 0xa75d as uint32,
    },
    pg_unicode_range {
        first: 0xa75f as uint32,
        last: 0xa75f as uint32,
    },
    pg_unicode_range {
        first: 0xa761 as uint32,
        last: 0xa761 as uint32,
    },
    pg_unicode_range {
        first: 0xa763 as uint32,
        last: 0xa763 as uint32,
    },
    pg_unicode_range {
        first: 0xa765 as uint32,
        last: 0xa765 as uint32,
    },
    pg_unicode_range {
        first: 0xa767 as uint32,
        last: 0xa767 as uint32,
    },
    pg_unicode_range {
        first: 0xa769 as uint32,
        last: 0xa769 as uint32,
    },
    pg_unicode_range {
        first: 0xa76b as uint32,
        last: 0xa76b as uint32,
    },
    pg_unicode_range {
        first: 0xa76d as uint32,
        last: 0xa76d as uint32,
    },
    pg_unicode_range {
        first: 0xa76f as uint32,
        last: 0xa76f as uint32,
    },
    pg_unicode_range {
        first: 0xa770 as uint32,
        last: 0xa770 as uint32,
    },
    pg_unicode_range {
        first: 0xa771 as uint32,
        last: 0xa778 as uint32,
    },
    pg_unicode_range {
        first: 0xa77a as uint32,
        last: 0xa77a as uint32,
    },
    pg_unicode_range {
        first: 0xa77c as uint32,
        last: 0xa77c as uint32,
    },
    pg_unicode_range {
        first: 0xa77f as uint32,
        last: 0xa77f as uint32,
    },
    pg_unicode_range {
        first: 0xa781 as uint32,
        last: 0xa781 as uint32,
    },
    pg_unicode_range {
        first: 0xa783 as uint32,
        last: 0xa783 as uint32,
    },
    pg_unicode_range {
        first: 0xa785 as uint32,
        last: 0xa785 as uint32,
    },
    pg_unicode_range {
        first: 0xa787 as uint32,
        last: 0xa787 as uint32,
    },
    pg_unicode_range {
        first: 0xa78c as uint32,
        last: 0xa78c as uint32,
    },
    pg_unicode_range {
        first: 0xa78e as uint32,
        last: 0xa78e as uint32,
    },
    pg_unicode_range {
        first: 0xa791 as uint32,
        last: 0xa791 as uint32,
    },
    pg_unicode_range {
        first: 0xa793 as uint32,
        last: 0xa795 as uint32,
    },
    pg_unicode_range {
        first: 0xa797 as uint32,
        last: 0xa797 as uint32,
    },
    pg_unicode_range {
        first: 0xa799 as uint32,
        last: 0xa799 as uint32,
    },
    pg_unicode_range {
        first: 0xa79b as uint32,
        last: 0xa79b as uint32,
    },
    pg_unicode_range {
        first: 0xa79d as uint32,
        last: 0xa79d as uint32,
    },
    pg_unicode_range {
        first: 0xa79f as uint32,
        last: 0xa79f as uint32,
    },
    pg_unicode_range {
        first: 0xa7a1 as uint32,
        last: 0xa7a1 as uint32,
    },
    pg_unicode_range {
        first: 0xa7a3 as uint32,
        last: 0xa7a3 as uint32,
    },
    pg_unicode_range {
        first: 0xa7a5 as uint32,
        last: 0xa7a5 as uint32,
    },
    pg_unicode_range {
        first: 0xa7a7 as uint32,
        last: 0xa7a7 as uint32,
    },
    pg_unicode_range {
        first: 0xa7a9 as uint32,
        last: 0xa7a9 as uint32,
    },
    pg_unicode_range {
        first: 0xa7af as uint32,
        last: 0xa7af as uint32,
    },
    pg_unicode_range {
        first: 0xa7b5 as uint32,
        last: 0xa7b5 as uint32,
    },
    pg_unicode_range {
        first: 0xa7b7 as uint32,
        last: 0xa7b7 as uint32,
    },
    pg_unicode_range {
        first: 0xa7b9 as uint32,
        last: 0xa7b9 as uint32,
    },
    pg_unicode_range {
        first: 0xa7bb as uint32,
        last: 0xa7bb as uint32,
    },
    pg_unicode_range {
        first: 0xa7bd as uint32,
        last: 0xa7bd as uint32,
    },
    pg_unicode_range {
        first: 0xa7bf as uint32,
        last: 0xa7bf as uint32,
    },
    pg_unicode_range {
        first: 0xa7c1 as uint32,
        last: 0xa7c1 as uint32,
    },
    pg_unicode_range {
        first: 0xa7c3 as uint32,
        last: 0xa7c3 as uint32,
    },
    pg_unicode_range {
        first: 0xa7c8 as uint32,
        last: 0xa7c8 as uint32,
    },
    pg_unicode_range {
        first: 0xa7ca as uint32,
        last: 0xa7ca as uint32,
    },
    pg_unicode_range {
        first: 0xa7cd as uint32,
        last: 0xa7cd as uint32,
    },
    pg_unicode_range {
        first: 0xa7d1 as uint32,
        last: 0xa7d1 as uint32,
    },
    pg_unicode_range {
        first: 0xa7d3 as uint32,
        last: 0xa7d3 as uint32,
    },
    pg_unicode_range {
        first: 0xa7d5 as uint32,
        last: 0xa7d5 as uint32,
    },
    pg_unicode_range {
        first: 0xa7d7 as uint32,
        last: 0xa7d7 as uint32,
    },
    pg_unicode_range {
        first: 0xa7d9 as uint32,
        last: 0xa7d9 as uint32,
    },
    pg_unicode_range {
        first: 0xa7db as uint32,
        last: 0xa7db as uint32,
    },
    pg_unicode_range {
        first: 0xa7f2 as uint32,
        last: 0xa7f4 as uint32,
    },
    pg_unicode_range {
        first: 0xa7f6 as uint32,
        last: 0xa7f6 as uint32,
    },
    pg_unicode_range {
        first: 0xa7f8 as uint32,
        last: 0xa7f9 as uint32,
    },
    pg_unicode_range {
        first: 0xa7fa as uint32,
        last: 0xa7fa as uint32,
    },
    pg_unicode_range {
        first: 0xab30 as uint32,
        last: 0xab5a as uint32,
    },
    pg_unicode_range {
        first: 0xab5c as uint32,
        last: 0xab5f as uint32,
    },
    pg_unicode_range {
        first: 0xab60 as uint32,
        last: 0xab68 as uint32,
    },
    pg_unicode_range {
        first: 0xab69 as uint32,
        last: 0xab69 as uint32,
    },
    pg_unicode_range {
        first: 0xab70 as uint32,
        last: 0xabbf as uint32,
    },
    pg_unicode_range {
        first: 0xfb00 as uint32,
        last: 0xfb06 as uint32,
    },
    pg_unicode_range {
        first: 0xfb13 as uint32,
        last: 0xfb17 as uint32,
    },
    pg_unicode_range {
        first: 0xff41 as uint32,
        last: 0xff5a as uint32,
    },
    pg_unicode_range {
        first: 0x10428 as uint32,
        last: 0x1044f as uint32,
    },
    pg_unicode_range {
        first: 0x104d8 as uint32,
        last: 0x104fb as uint32,
    },
    pg_unicode_range {
        first: 0x10597 as uint32,
        last: 0x105a1 as uint32,
    },
    pg_unicode_range {
        first: 0x105a3 as uint32,
        last: 0x105b1 as uint32,
    },
    pg_unicode_range {
        first: 0x105b3 as uint32,
        last: 0x105b9 as uint32,
    },
    pg_unicode_range {
        first: 0x105bb as uint32,
        last: 0x105bc as uint32,
    },
    pg_unicode_range {
        first: 0x10780 as uint32,
        last: 0x10780 as uint32,
    },
    pg_unicode_range {
        first: 0x10783 as uint32,
        last: 0x10785 as uint32,
    },
    pg_unicode_range {
        first: 0x10787 as uint32,
        last: 0x107b0 as uint32,
    },
    pg_unicode_range {
        first: 0x107b2 as uint32,
        last: 0x107ba as uint32,
    },
    pg_unicode_range {
        first: 0x10cc0 as uint32,
        last: 0x10cf2 as uint32,
    },
    pg_unicode_range {
        first: 0x10d70 as uint32,
        last: 0x10d85 as uint32,
    },
    pg_unicode_range {
        first: 0x118c0 as uint32,
        last: 0x118df as uint32,
    },
    pg_unicode_range {
        first: 0x16e60 as uint32,
        last: 0x16e7f as uint32,
    },
    pg_unicode_range {
        first: 0x1d41a as uint32,
        last: 0x1d433 as uint32,
    },
    pg_unicode_range {
        first: 0x1d44e as uint32,
        last: 0x1d454 as uint32,
    },
    pg_unicode_range {
        first: 0x1d456 as uint32,
        last: 0x1d467 as uint32,
    },
    pg_unicode_range {
        first: 0x1d482 as uint32,
        last: 0x1d49b as uint32,
    },
    pg_unicode_range {
        first: 0x1d4b6 as uint32,
        last: 0x1d4b9 as uint32,
    },
    pg_unicode_range {
        first: 0x1d4bb as uint32,
        last: 0x1d4bb as uint32,
    },
    pg_unicode_range {
        first: 0x1d4bd as uint32,
        last: 0x1d4c3 as uint32,
    },
    pg_unicode_range {
        first: 0x1d4c5 as uint32,
        last: 0x1d4cf as uint32,
    },
    pg_unicode_range {
        first: 0x1d4ea as uint32,
        last: 0x1d503 as uint32,
    },
    pg_unicode_range {
        first: 0x1d51e as uint32,
        last: 0x1d537 as uint32,
    },
    pg_unicode_range {
        first: 0x1d552 as uint32,
        last: 0x1d56b as uint32,
    },
    pg_unicode_range {
        first: 0x1d586 as uint32,
        last: 0x1d59f as uint32,
    },
    pg_unicode_range {
        first: 0x1d5ba as uint32,
        last: 0x1d5d3 as uint32,
    },
    pg_unicode_range {
        first: 0x1d5ee as uint32,
        last: 0x1d607 as uint32,
    },
    pg_unicode_range {
        first: 0x1d622 as uint32,
        last: 0x1d63b as uint32,
    },
    pg_unicode_range {
        first: 0x1d656 as uint32,
        last: 0x1d66f as uint32,
    },
    pg_unicode_range {
        first: 0x1d68a as uint32,
        last: 0x1d6a5 as uint32,
    },
    pg_unicode_range {
        first: 0x1d6c2 as uint32,
        last: 0x1d6da as uint32,
    },
    pg_unicode_range {
        first: 0x1d6dc as uint32,
        last: 0x1d6e1 as uint32,
    },
    pg_unicode_range {
        first: 0x1d6fc as uint32,
        last: 0x1d714 as uint32,
    },
    pg_unicode_range {
        first: 0x1d716 as uint32,
        last: 0x1d71b as uint32,
    },
    pg_unicode_range {
        first: 0x1d736 as uint32,
        last: 0x1d74e as uint32,
    },
    pg_unicode_range {
        first: 0x1d750 as uint32,
        last: 0x1d755 as uint32,
    },
    pg_unicode_range {
        first: 0x1d770 as uint32,
        last: 0x1d788 as uint32,
    },
    pg_unicode_range {
        first: 0x1d78a as uint32,
        last: 0x1d78f as uint32,
    },
    pg_unicode_range {
        first: 0x1d7aa as uint32,
        last: 0x1d7c2 as uint32,
    },
    pg_unicode_range {
        first: 0x1d7c4 as uint32,
        last: 0x1d7c9 as uint32,
    },
    pg_unicode_range {
        first: 0x1d7cb as uint32,
        last: 0x1d7cb as uint32,
    },
    pg_unicode_range {
        first: 0x1df00 as uint32,
        last: 0x1df09 as uint32,
    },
    pg_unicode_range {
        first: 0x1df0b as uint32,
        last: 0x1df1e as uint32,
    },
    pg_unicode_range {
        first: 0x1df25 as uint32,
        last: 0x1df2a as uint32,
    },
    pg_unicode_range {
        first: 0x1e030 as uint32,
        last: 0x1e06d as uint32,
    },
    pg_unicode_range {
        first: 0x1e922 as uint32,
        last: 0x1e943 as uint32,
    },
];
pub(crate) static unicode_uppercase: [pg_unicode_range; 656] = [
    pg_unicode_range {
        first: 0x41 as uint32,
        last: 0x5a as uint32,
    },
    pg_unicode_range {
        first: 0xc0 as uint32,
        last: 0xd6 as uint32,
    },
    pg_unicode_range {
        first: 0xd8 as uint32,
        last: 0xde as uint32,
    },
    pg_unicode_range {
        first: 0x100 as uint32,
        last: 0x100 as uint32,
    },
    pg_unicode_range {
        first: 0x102 as uint32,
        last: 0x102 as uint32,
    },
    pg_unicode_range {
        first: 0x104 as uint32,
        last: 0x104 as uint32,
    },
    pg_unicode_range {
        first: 0x106 as uint32,
        last: 0x106 as uint32,
    },
    pg_unicode_range {
        first: 0x108 as uint32,
        last: 0x108 as uint32,
    },
    pg_unicode_range {
        first: 0x10a as uint32,
        last: 0x10a as uint32,
    },
    pg_unicode_range {
        first: 0x10c as uint32,
        last: 0x10c as uint32,
    },
    pg_unicode_range {
        first: 0x10e as uint32,
        last: 0x10e as uint32,
    },
    pg_unicode_range {
        first: 0x110 as uint32,
        last: 0x110 as uint32,
    },
    pg_unicode_range {
        first: 0x112 as uint32,
        last: 0x112 as uint32,
    },
    pg_unicode_range {
        first: 0x114 as uint32,
        last: 0x114 as uint32,
    },
    pg_unicode_range {
        first: 0x116 as uint32,
        last: 0x116 as uint32,
    },
    pg_unicode_range {
        first: 0x118 as uint32,
        last: 0x118 as uint32,
    },
    pg_unicode_range {
        first: 0x11a as uint32,
        last: 0x11a as uint32,
    },
    pg_unicode_range {
        first: 0x11c as uint32,
        last: 0x11c as uint32,
    },
    pg_unicode_range {
        first: 0x11e as uint32,
        last: 0x11e as uint32,
    },
    pg_unicode_range {
        first: 0x120 as uint32,
        last: 0x120 as uint32,
    },
    pg_unicode_range {
        first: 0x122 as uint32,
        last: 0x122 as uint32,
    },
    pg_unicode_range {
        first: 0x124 as uint32,
        last: 0x124 as uint32,
    },
    pg_unicode_range {
        first: 0x126 as uint32,
        last: 0x126 as uint32,
    },
    pg_unicode_range {
        first: 0x128 as uint32,
        last: 0x128 as uint32,
    },
    pg_unicode_range {
        first: 0x12a as uint32,
        last: 0x12a as uint32,
    },
    pg_unicode_range {
        first: 0x12c as uint32,
        last: 0x12c as uint32,
    },
    pg_unicode_range {
        first: 0x12e as uint32,
        last: 0x12e as uint32,
    },
    pg_unicode_range {
        first: 0x130 as uint32,
        last: 0x130 as uint32,
    },
    pg_unicode_range {
        first: 0x132 as uint32,
        last: 0x132 as uint32,
    },
    pg_unicode_range {
        first: 0x134 as uint32,
        last: 0x134 as uint32,
    },
    pg_unicode_range {
        first: 0x136 as uint32,
        last: 0x136 as uint32,
    },
    pg_unicode_range {
        first: 0x139 as uint32,
        last: 0x139 as uint32,
    },
    pg_unicode_range {
        first: 0x13b as uint32,
        last: 0x13b as uint32,
    },
    pg_unicode_range {
        first: 0x13d as uint32,
        last: 0x13d as uint32,
    },
    pg_unicode_range {
        first: 0x13f as uint32,
        last: 0x13f as uint32,
    },
    pg_unicode_range {
        first: 0x141 as uint32,
        last: 0x141 as uint32,
    },
    pg_unicode_range {
        first: 0x143 as uint32,
        last: 0x143 as uint32,
    },
    pg_unicode_range {
        first: 0x145 as uint32,
        last: 0x145 as uint32,
    },
    pg_unicode_range {
        first: 0x147 as uint32,
        last: 0x147 as uint32,
    },
    pg_unicode_range {
        first: 0x14a as uint32,
        last: 0x14a as uint32,
    },
    pg_unicode_range {
        first: 0x14c as uint32,
        last: 0x14c as uint32,
    },
    pg_unicode_range {
        first: 0x14e as uint32,
        last: 0x14e as uint32,
    },
    pg_unicode_range {
        first: 0x150 as uint32,
        last: 0x150 as uint32,
    },
    pg_unicode_range {
        first: 0x152 as uint32,
        last: 0x152 as uint32,
    },
    pg_unicode_range {
        first: 0x154 as uint32,
        last: 0x154 as uint32,
    },
    pg_unicode_range {
        first: 0x156 as uint32,
        last: 0x156 as uint32,
    },
    pg_unicode_range {
        first: 0x158 as uint32,
        last: 0x158 as uint32,
    },
    pg_unicode_range {
        first: 0x15a as uint32,
        last: 0x15a as uint32,
    },
    pg_unicode_range {
        first: 0x15c as uint32,
        last: 0x15c as uint32,
    },
    pg_unicode_range {
        first: 0x15e as uint32,
        last: 0x15e as uint32,
    },
    pg_unicode_range {
        first: 0x160 as uint32,
        last: 0x160 as uint32,
    },
    pg_unicode_range {
        first: 0x162 as uint32,
        last: 0x162 as uint32,
    },
    pg_unicode_range {
        first: 0x164 as uint32,
        last: 0x164 as uint32,
    },
    pg_unicode_range {
        first: 0x166 as uint32,
        last: 0x166 as uint32,
    },
    pg_unicode_range {
        first: 0x168 as uint32,
        last: 0x168 as uint32,
    },
    pg_unicode_range {
        first: 0x16a as uint32,
        last: 0x16a as uint32,
    },
    pg_unicode_range {
        first: 0x16c as uint32,
        last: 0x16c as uint32,
    },
    pg_unicode_range {
        first: 0x16e as uint32,
        last: 0x16e as uint32,
    },
    pg_unicode_range {
        first: 0x170 as uint32,
        last: 0x170 as uint32,
    },
    pg_unicode_range {
        first: 0x172 as uint32,
        last: 0x172 as uint32,
    },
    pg_unicode_range {
        first: 0x174 as uint32,
        last: 0x174 as uint32,
    },
    pg_unicode_range {
        first: 0x176 as uint32,
        last: 0x176 as uint32,
    },
    pg_unicode_range {
        first: 0x178 as uint32,
        last: 0x179 as uint32,
    },
    pg_unicode_range {
        first: 0x17b as uint32,
        last: 0x17b as uint32,
    },
    pg_unicode_range {
        first: 0x17d as uint32,
        last: 0x17d as uint32,
    },
    pg_unicode_range {
        first: 0x181 as uint32,
        last: 0x182 as uint32,
    },
    pg_unicode_range {
        first: 0x184 as uint32,
        last: 0x184 as uint32,
    },
    pg_unicode_range {
        first: 0x186 as uint32,
        last: 0x187 as uint32,
    },
    pg_unicode_range {
        first: 0x189 as uint32,
        last: 0x18b as uint32,
    },
    pg_unicode_range {
        first: 0x18e as uint32,
        last: 0x191 as uint32,
    },
    pg_unicode_range {
        first: 0x193 as uint32,
        last: 0x194 as uint32,
    },
    pg_unicode_range {
        first: 0x196 as uint32,
        last: 0x198 as uint32,
    },
    pg_unicode_range {
        first: 0x19c as uint32,
        last: 0x19d as uint32,
    },
    pg_unicode_range {
        first: 0x19f as uint32,
        last: 0x1a0 as uint32,
    },
    pg_unicode_range {
        first: 0x1a2 as uint32,
        last: 0x1a2 as uint32,
    },
    pg_unicode_range {
        first: 0x1a4 as uint32,
        last: 0x1a4 as uint32,
    },
    pg_unicode_range {
        first: 0x1a6 as uint32,
        last: 0x1a7 as uint32,
    },
    pg_unicode_range {
        first: 0x1a9 as uint32,
        last: 0x1a9 as uint32,
    },
    pg_unicode_range {
        first: 0x1ac as uint32,
        last: 0x1ac as uint32,
    },
    pg_unicode_range {
        first: 0x1ae as uint32,
        last: 0x1af as uint32,
    },
    pg_unicode_range {
        first: 0x1b1 as uint32,
        last: 0x1b3 as uint32,
    },
    pg_unicode_range {
        first: 0x1b5 as uint32,
        last: 0x1b5 as uint32,
    },
    pg_unicode_range {
        first: 0x1b7 as uint32,
        last: 0x1b8 as uint32,
    },
    pg_unicode_range {
        first: 0x1bc as uint32,
        last: 0x1bc as uint32,
    },
    pg_unicode_range {
        first: 0x1c4 as uint32,
        last: 0x1c4 as uint32,
    },
    pg_unicode_range {
        first: 0x1c7 as uint32,
        last: 0x1c7 as uint32,
    },
    pg_unicode_range {
        first: 0x1ca as uint32,
        last: 0x1ca as uint32,
    },
    pg_unicode_range {
        first: 0x1cd as uint32,
        last: 0x1cd as uint32,
    },
    pg_unicode_range {
        first: 0x1cf as uint32,
        last: 0x1cf as uint32,
    },
    pg_unicode_range {
        first: 0x1d1 as uint32,
        last: 0x1d1 as uint32,
    },
    pg_unicode_range {
        first: 0x1d3 as uint32,
        last: 0x1d3 as uint32,
    },
    pg_unicode_range {
        first: 0x1d5 as uint32,
        last: 0x1d5 as uint32,
    },
    pg_unicode_range {
        first: 0x1d7 as uint32,
        last: 0x1d7 as uint32,
    },
    pg_unicode_range {
        first: 0x1d9 as uint32,
        last: 0x1d9 as uint32,
    },
    pg_unicode_range {
        first: 0x1db as uint32,
        last: 0x1db as uint32,
    },
    pg_unicode_range {
        first: 0x1de as uint32,
        last: 0x1de as uint32,
    },
    pg_unicode_range {
        first: 0x1e0 as uint32,
        last: 0x1e0 as uint32,
    },
    pg_unicode_range {
        first: 0x1e2 as uint32,
        last: 0x1e2 as uint32,
    },
    pg_unicode_range {
        first: 0x1e4 as uint32,
        last: 0x1e4 as uint32,
    },
    pg_unicode_range {
        first: 0x1e6 as uint32,
        last: 0x1e6 as uint32,
    },
    pg_unicode_range {
        first: 0x1e8 as uint32,
        last: 0x1e8 as uint32,
    },
    pg_unicode_range {
        first: 0x1ea as uint32,
        last: 0x1ea as uint32,
    },
    pg_unicode_range {
        first: 0x1ec as uint32,
        last: 0x1ec as uint32,
    },
    pg_unicode_range {
        first: 0x1ee as uint32,
        last: 0x1ee as uint32,
    },
    pg_unicode_range {
        first: 0x1f1 as uint32,
        last: 0x1f1 as uint32,
    },
    pg_unicode_range {
        first: 0x1f4 as uint32,
        last: 0x1f4 as uint32,
    },
    pg_unicode_range {
        first: 0x1f6 as uint32,
        last: 0x1f8 as uint32,
    },
    pg_unicode_range {
        first: 0x1fa as uint32,
        last: 0x1fa as uint32,
    },
    pg_unicode_range {
        first: 0x1fc as uint32,
        last: 0x1fc as uint32,
    },
    pg_unicode_range {
        first: 0x1fe as uint32,
        last: 0x1fe as uint32,
    },
    pg_unicode_range {
        first: 0x200 as uint32,
        last: 0x200 as uint32,
    },
    pg_unicode_range {
        first: 0x202 as uint32,
        last: 0x202 as uint32,
    },
    pg_unicode_range {
        first: 0x204 as uint32,
        last: 0x204 as uint32,
    },
    pg_unicode_range {
        first: 0x206 as uint32,
        last: 0x206 as uint32,
    },
    pg_unicode_range {
        first: 0x208 as uint32,
        last: 0x208 as uint32,
    },
    pg_unicode_range {
        first: 0x20a as uint32,
        last: 0x20a as uint32,
    },
    pg_unicode_range {
        first: 0x20c as uint32,
        last: 0x20c as uint32,
    },
    pg_unicode_range {
        first: 0x20e as uint32,
        last: 0x20e as uint32,
    },
    pg_unicode_range {
        first: 0x210 as uint32,
        last: 0x210 as uint32,
    },
    pg_unicode_range {
        first: 0x212 as uint32,
        last: 0x212 as uint32,
    },
    pg_unicode_range {
        first: 0x214 as uint32,
        last: 0x214 as uint32,
    },
    pg_unicode_range {
        first: 0x216 as uint32,
        last: 0x216 as uint32,
    },
    pg_unicode_range {
        first: 0x218 as uint32,
        last: 0x218 as uint32,
    },
    pg_unicode_range {
        first: 0x21a as uint32,
        last: 0x21a as uint32,
    },
    pg_unicode_range {
        first: 0x21c as uint32,
        last: 0x21c as uint32,
    },
    pg_unicode_range {
        first: 0x21e as uint32,
        last: 0x21e as uint32,
    },
    pg_unicode_range {
        first: 0x220 as uint32,
        last: 0x220 as uint32,
    },
    pg_unicode_range {
        first: 0x222 as uint32,
        last: 0x222 as uint32,
    },
    pg_unicode_range {
        first: 0x224 as uint32,
        last: 0x224 as uint32,
    },
    pg_unicode_range {
        first: 0x226 as uint32,
        last: 0x226 as uint32,
    },
    pg_unicode_range {
        first: 0x228 as uint32,
        last: 0x228 as uint32,
    },
    pg_unicode_range {
        first: 0x22a as uint32,
        last: 0x22a as uint32,
    },
    pg_unicode_range {
        first: 0x22c as uint32,
        last: 0x22c as uint32,
    },
    pg_unicode_range {
        first: 0x22e as uint32,
        last: 0x22e as uint32,
    },
    pg_unicode_range {
        first: 0x230 as uint32,
        last: 0x230 as uint32,
    },
    pg_unicode_range {
        first: 0x232 as uint32,
        last: 0x232 as uint32,
    },
    pg_unicode_range {
        first: 0x23a as uint32,
        last: 0x23b as uint32,
    },
    pg_unicode_range {
        first: 0x23d as uint32,
        last: 0x23e as uint32,
    },
    pg_unicode_range {
        first: 0x241 as uint32,
        last: 0x241 as uint32,
    },
    pg_unicode_range {
        first: 0x243 as uint32,
        last: 0x246 as uint32,
    },
    pg_unicode_range {
        first: 0x248 as uint32,
        last: 0x248 as uint32,
    },
    pg_unicode_range {
        first: 0x24a as uint32,
        last: 0x24a as uint32,
    },
    pg_unicode_range {
        first: 0x24c as uint32,
        last: 0x24c as uint32,
    },
    pg_unicode_range {
        first: 0x24e as uint32,
        last: 0x24e as uint32,
    },
    pg_unicode_range {
        first: 0x370 as uint32,
        last: 0x370 as uint32,
    },
    pg_unicode_range {
        first: 0x372 as uint32,
        last: 0x372 as uint32,
    },
    pg_unicode_range {
        first: 0x376 as uint32,
        last: 0x376 as uint32,
    },
    pg_unicode_range {
        first: 0x37f as uint32,
        last: 0x37f as uint32,
    },
    pg_unicode_range {
        first: 0x386 as uint32,
        last: 0x386 as uint32,
    },
    pg_unicode_range {
        first: 0x388 as uint32,
        last: 0x38a as uint32,
    },
    pg_unicode_range {
        first: 0x38c as uint32,
        last: 0x38c as uint32,
    },
    pg_unicode_range {
        first: 0x38e as uint32,
        last: 0x38f as uint32,
    },
    pg_unicode_range {
        first: 0x391 as uint32,
        last: 0x3a1 as uint32,
    },
    pg_unicode_range {
        first: 0x3a3 as uint32,
        last: 0x3ab as uint32,
    },
    pg_unicode_range {
        first: 0x3cf as uint32,
        last: 0x3cf as uint32,
    },
    pg_unicode_range {
        first: 0x3d2 as uint32,
        last: 0x3d4 as uint32,
    },
    pg_unicode_range {
        first: 0x3d8 as uint32,
        last: 0x3d8 as uint32,
    },
    pg_unicode_range {
        first: 0x3da as uint32,
        last: 0x3da as uint32,
    },
    pg_unicode_range {
        first: 0x3dc as uint32,
        last: 0x3dc as uint32,
    },
    pg_unicode_range {
        first: 0x3de as uint32,
        last: 0x3de as uint32,
    },
    pg_unicode_range {
        first: 0x3e0 as uint32,
        last: 0x3e0 as uint32,
    },
    pg_unicode_range {
        first: 0x3e2 as uint32,
        last: 0x3e2 as uint32,
    },
    pg_unicode_range {
        first: 0x3e4 as uint32,
        last: 0x3e4 as uint32,
    },
    pg_unicode_range {
        first: 0x3e6 as uint32,
        last: 0x3e6 as uint32,
    },
    pg_unicode_range {
        first: 0x3e8 as uint32,
        last: 0x3e8 as uint32,
    },
    pg_unicode_range {
        first: 0x3ea as uint32,
        last: 0x3ea as uint32,
    },
    pg_unicode_range {
        first: 0x3ec as uint32,
        last: 0x3ec as uint32,
    },
    pg_unicode_range {
        first: 0x3ee as uint32,
        last: 0x3ee as uint32,
    },
    pg_unicode_range {
        first: 0x3f4 as uint32,
        last: 0x3f4 as uint32,
    },
    pg_unicode_range {
        first: 0x3f7 as uint32,
        last: 0x3f7 as uint32,
    },
    pg_unicode_range {
        first: 0x3f9 as uint32,
        last: 0x3fa as uint32,
    },
    pg_unicode_range {
        first: 0x3fd as uint32,
        last: 0x42f as uint32,
    },
    pg_unicode_range {
        first: 0x460 as uint32,
        last: 0x460 as uint32,
    },
    pg_unicode_range {
        first: 0x462 as uint32,
        last: 0x462 as uint32,
    },
    pg_unicode_range {
        first: 0x464 as uint32,
        last: 0x464 as uint32,
    },
    pg_unicode_range {
        first: 0x466 as uint32,
        last: 0x466 as uint32,
    },
    pg_unicode_range {
        first: 0x468 as uint32,
        last: 0x468 as uint32,
    },
    pg_unicode_range {
        first: 0x46a as uint32,
        last: 0x46a as uint32,
    },
    pg_unicode_range {
        first: 0x46c as uint32,
        last: 0x46c as uint32,
    },
    pg_unicode_range {
        first: 0x46e as uint32,
        last: 0x46e as uint32,
    },
    pg_unicode_range {
        first: 0x470 as uint32,
        last: 0x470 as uint32,
    },
    pg_unicode_range {
        first: 0x472 as uint32,
        last: 0x472 as uint32,
    },
    pg_unicode_range {
        first: 0x474 as uint32,
        last: 0x474 as uint32,
    },
    pg_unicode_range {
        first: 0x476 as uint32,
        last: 0x476 as uint32,
    },
    pg_unicode_range {
        first: 0x478 as uint32,
        last: 0x478 as uint32,
    },
    pg_unicode_range {
        first: 0x47a as uint32,
        last: 0x47a as uint32,
    },
    pg_unicode_range {
        first: 0x47c as uint32,
        last: 0x47c as uint32,
    },
    pg_unicode_range {
        first: 0x47e as uint32,
        last: 0x47e as uint32,
    },
    pg_unicode_range {
        first: 0x480 as uint32,
        last: 0x480 as uint32,
    },
    pg_unicode_range {
        first: 0x48a as uint32,
        last: 0x48a as uint32,
    },
    pg_unicode_range {
        first: 0x48c as uint32,
        last: 0x48c as uint32,
    },
    pg_unicode_range {
        first: 0x48e as uint32,
        last: 0x48e as uint32,
    },
    pg_unicode_range {
        first: 0x490 as uint32,
        last: 0x490 as uint32,
    },
    pg_unicode_range {
        first: 0x492 as uint32,
        last: 0x492 as uint32,
    },
    pg_unicode_range {
        first: 0x494 as uint32,
        last: 0x494 as uint32,
    },
    pg_unicode_range {
        first: 0x496 as uint32,
        last: 0x496 as uint32,
    },
    pg_unicode_range {
        first: 0x498 as uint32,
        last: 0x498 as uint32,
    },
    pg_unicode_range {
        first: 0x49a as uint32,
        last: 0x49a as uint32,
    },
    pg_unicode_range {
        first: 0x49c as uint32,
        last: 0x49c as uint32,
    },
    pg_unicode_range {
        first: 0x49e as uint32,
        last: 0x49e as uint32,
    },
    pg_unicode_range {
        first: 0x4a0 as uint32,
        last: 0x4a0 as uint32,
    },
    pg_unicode_range {
        first: 0x4a2 as uint32,
        last: 0x4a2 as uint32,
    },
    pg_unicode_range {
        first: 0x4a4 as uint32,
        last: 0x4a4 as uint32,
    },
    pg_unicode_range {
        first: 0x4a6 as uint32,
        last: 0x4a6 as uint32,
    },
    pg_unicode_range {
        first: 0x4a8 as uint32,
        last: 0x4a8 as uint32,
    },
    pg_unicode_range {
        first: 0x4aa as uint32,
        last: 0x4aa as uint32,
    },
    pg_unicode_range {
        first: 0x4ac as uint32,
        last: 0x4ac as uint32,
    },
    pg_unicode_range {
        first: 0x4ae as uint32,
        last: 0x4ae as uint32,
    },
    pg_unicode_range {
        first: 0x4b0 as uint32,
        last: 0x4b0 as uint32,
    },
    pg_unicode_range {
        first: 0x4b2 as uint32,
        last: 0x4b2 as uint32,
    },
    pg_unicode_range {
        first: 0x4b4 as uint32,
        last: 0x4b4 as uint32,
    },
    pg_unicode_range {
        first: 0x4b6 as uint32,
        last: 0x4b6 as uint32,
    },
    pg_unicode_range {
        first: 0x4b8 as uint32,
        last: 0x4b8 as uint32,
    },
    pg_unicode_range {
        first: 0x4ba as uint32,
        last: 0x4ba as uint32,
    },
    pg_unicode_range {
        first: 0x4bc as uint32,
        last: 0x4bc as uint32,
    },
    pg_unicode_range {
        first: 0x4be as uint32,
        last: 0x4be as uint32,
    },
    pg_unicode_range {
        first: 0x4c0 as uint32,
        last: 0x4c1 as uint32,
    },
    pg_unicode_range {
        first: 0x4c3 as uint32,
        last: 0x4c3 as uint32,
    },
    pg_unicode_range {
        first: 0x4c5 as uint32,
        last: 0x4c5 as uint32,
    },
    pg_unicode_range {
        first: 0x4c7 as uint32,
        last: 0x4c7 as uint32,
    },
    pg_unicode_range {
        first: 0x4c9 as uint32,
        last: 0x4c9 as uint32,
    },
    pg_unicode_range {
        first: 0x4cb as uint32,
        last: 0x4cb as uint32,
    },
    pg_unicode_range {
        first: 0x4cd as uint32,
        last: 0x4cd as uint32,
    },
    pg_unicode_range {
        first: 0x4d0 as uint32,
        last: 0x4d0 as uint32,
    },
    pg_unicode_range {
        first: 0x4d2 as uint32,
        last: 0x4d2 as uint32,
    },
    pg_unicode_range {
        first: 0x4d4 as uint32,
        last: 0x4d4 as uint32,
    },
    pg_unicode_range {
        first: 0x4d6 as uint32,
        last: 0x4d6 as uint32,
    },
    pg_unicode_range {
        first: 0x4d8 as uint32,
        last: 0x4d8 as uint32,
    },
    pg_unicode_range {
        first: 0x4da as uint32,
        last: 0x4da as uint32,
    },
    pg_unicode_range {
        first: 0x4dc as uint32,
        last: 0x4dc as uint32,
    },
    pg_unicode_range {
        first: 0x4de as uint32,
        last: 0x4de as uint32,
    },
    pg_unicode_range {
        first: 0x4e0 as uint32,
        last: 0x4e0 as uint32,
    },
    pg_unicode_range {
        first: 0x4e2 as uint32,
        last: 0x4e2 as uint32,
    },
    pg_unicode_range {
        first: 0x4e4 as uint32,
        last: 0x4e4 as uint32,
    },
    pg_unicode_range {
        first: 0x4e6 as uint32,
        last: 0x4e6 as uint32,
    },
    pg_unicode_range {
        first: 0x4e8 as uint32,
        last: 0x4e8 as uint32,
    },
    pg_unicode_range {
        first: 0x4ea as uint32,
        last: 0x4ea as uint32,
    },
    pg_unicode_range {
        first: 0x4ec as uint32,
        last: 0x4ec as uint32,
    },
    pg_unicode_range {
        first: 0x4ee as uint32,
        last: 0x4ee as uint32,
    },
    pg_unicode_range {
        first: 0x4f0 as uint32,
        last: 0x4f0 as uint32,
    },
    pg_unicode_range {
        first: 0x4f2 as uint32,
        last: 0x4f2 as uint32,
    },
    pg_unicode_range {
        first: 0x4f4 as uint32,
        last: 0x4f4 as uint32,
    },
    pg_unicode_range {
        first: 0x4f6 as uint32,
        last: 0x4f6 as uint32,
    },
    pg_unicode_range {
        first: 0x4f8 as uint32,
        last: 0x4f8 as uint32,
    },
    pg_unicode_range {
        first: 0x4fa as uint32,
        last: 0x4fa as uint32,
    },
    pg_unicode_range {
        first: 0x4fc as uint32,
        last: 0x4fc as uint32,
    },
    pg_unicode_range {
        first: 0x4fe as uint32,
        last: 0x4fe as uint32,
    },
    pg_unicode_range {
        first: 0x500 as uint32,
        last: 0x500 as uint32,
    },
    pg_unicode_range {
        first: 0x502 as uint32,
        last: 0x502 as uint32,
    },
    pg_unicode_range {
        first: 0x504 as uint32,
        last: 0x504 as uint32,
    },
    pg_unicode_range {
        first: 0x506 as uint32,
        last: 0x506 as uint32,
    },
    pg_unicode_range {
        first: 0x508 as uint32,
        last: 0x508 as uint32,
    },
    pg_unicode_range {
        first: 0x50a as uint32,
        last: 0x50a as uint32,
    },
    pg_unicode_range {
        first: 0x50c as uint32,
        last: 0x50c as uint32,
    },
    pg_unicode_range {
        first: 0x50e as uint32,
        last: 0x50e as uint32,
    },
    pg_unicode_range {
        first: 0x510 as uint32,
        last: 0x510 as uint32,
    },
    pg_unicode_range {
        first: 0x512 as uint32,
        last: 0x512 as uint32,
    },
    pg_unicode_range {
        first: 0x514 as uint32,
        last: 0x514 as uint32,
    },
    pg_unicode_range {
        first: 0x516 as uint32,
        last: 0x516 as uint32,
    },
    pg_unicode_range {
        first: 0x518 as uint32,
        last: 0x518 as uint32,
    },
    pg_unicode_range {
        first: 0x51a as uint32,
        last: 0x51a as uint32,
    },
    pg_unicode_range {
        first: 0x51c as uint32,
        last: 0x51c as uint32,
    },
    pg_unicode_range {
        first: 0x51e as uint32,
        last: 0x51e as uint32,
    },
    pg_unicode_range {
        first: 0x520 as uint32,
        last: 0x520 as uint32,
    },
    pg_unicode_range {
        first: 0x522 as uint32,
        last: 0x522 as uint32,
    },
    pg_unicode_range {
        first: 0x524 as uint32,
        last: 0x524 as uint32,
    },
    pg_unicode_range {
        first: 0x526 as uint32,
        last: 0x526 as uint32,
    },
    pg_unicode_range {
        first: 0x528 as uint32,
        last: 0x528 as uint32,
    },
    pg_unicode_range {
        first: 0x52a as uint32,
        last: 0x52a as uint32,
    },
    pg_unicode_range {
        first: 0x52c as uint32,
        last: 0x52c as uint32,
    },
    pg_unicode_range {
        first: 0x52e as uint32,
        last: 0x52e as uint32,
    },
    pg_unicode_range {
        first: 0x531 as uint32,
        last: 0x556 as uint32,
    },
    pg_unicode_range {
        first: 0x10a0 as uint32,
        last: 0x10c5 as uint32,
    },
    pg_unicode_range {
        first: 0x10c7 as uint32,
        last: 0x10c7 as uint32,
    },
    pg_unicode_range {
        first: 0x10cd as uint32,
        last: 0x10cd as uint32,
    },
    pg_unicode_range {
        first: 0x13a0 as uint32,
        last: 0x13f5 as uint32,
    },
    pg_unicode_range {
        first: 0x1c89 as uint32,
        last: 0x1c89 as uint32,
    },
    pg_unicode_range {
        first: 0x1c90 as uint32,
        last: 0x1cba as uint32,
    },
    pg_unicode_range {
        first: 0x1cbd as uint32,
        last: 0x1cbf as uint32,
    },
    pg_unicode_range {
        first: 0x1e00 as uint32,
        last: 0x1e00 as uint32,
    },
    pg_unicode_range {
        first: 0x1e02 as uint32,
        last: 0x1e02 as uint32,
    },
    pg_unicode_range {
        first: 0x1e04 as uint32,
        last: 0x1e04 as uint32,
    },
    pg_unicode_range {
        first: 0x1e06 as uint32,
        last: 0x1e06 as uint32,
    },
    pg_unicode_range {
        first: 0x1e08 as uint32,
        last: 0x1e08 as uint32,
    },
    pg_unicode_range {
        first: 0x1e0a as uint32,
        last: 0x1e0a as uint32,
    },
    pg_unicode_range {
        first: 0x1e0c as uint32,
        last: 0x1e0c as uint32,
    },
    pg_unicode_range {
        first: 0x1e0e as uint32,
        last: 0x1e0e as uint32,
    },
    pg_unicode_range {
        first: 0x1e10 as uint32,
        last: 0x1e10 as uint32,
    },
    pg_unicode_range {
        first: 0x1e12 as uint32,
        last: 0x1e12 as uint32,
    },
    pg_unicode_range {
        first: 0x1e14 as uint32,
        last: 0x1e14 as uint32,
    },
    pg_unicode_range {
        first: 0x1e16 as uint32,
        last: 0x1e16 as uint32,
    },
    pg_unicode_range {
        first: 0x1e18 as uint32,
        last: 0x1e18 as uint32,
    },
    pg_unicode_range {
        first: 0x1e1a as uint32,
        last: 0x1e1a as uint32,
    },
    pg_unicode_range {
        first: 0x1e1c as uint32,
        last: 0x1e1c as uint32,
    },
    pg_unicode_range {
        first: 0x1e1e as uint32,
        last: 0x1e1e as uint32,
    },
    pg_unicode_range {
        first: 0x1e20 as uint32,
        last: 0x1e20 as uint32,
    },
    pg_unicode_range {
        first: 0x1e22 as uint32,
        last: 0x1e22 as uint32,
    },
    pg_unicode_range {
        first: 0x1e24 as uint32,
        last: 0x1e24 as uint32,
    },
    pg_unicode_range {
        first: 0x1e26 as uint32,
        last: 0x1e26 as uint32,
    },
    pg_unicode_range {
        first: 0x1e28 as uint32,
        last: 0x1e28 as uint32,
    },
    pg_unicode_range {
        first: 0x1e2a as uint32,
        last: 0x1e2a as uint32,
    },
    pg_unicode_range {
        first: 0x1e2c as uint32,
        last: 0x1e2c as uint32,
    },
    pg_unicode_range {
        first: 0x1e2e as uint32,
        last: 0x1e2e as uint32,
    },
    pg_unicode_range {
        first: 0x1e30 as uint32,
        last: 0x1e30 as uint32,
    },
    pg_unicode_range {
        first: 0x1e32 as uint32,
        last: 0x1e32 as uint32,
    },
    pg_unicode_range {
        first: 0x1e34 as uint32,
        last: 0x1e34 as uint32,
    },
    pg_unicode_range {
        first: 0x1e36 as uint32,
        last: 0x1e36 as uint32,
    },
    pg_unicode_range {
        first: 0x1e38 as uint32,
        last: 0x1e38 as uint32,
    },
    pg_unicode_range {
        first: 0x1e3a as uint32,
        last: 0x1e3a as uint32,
    },
    pg_unicode_range {
        first: 0x1e3c as uint32,
        last: 0x1e3c as uint32,
    },
    pg_unicode_range {
        first: 0x1e3e as uint32,
        last: 0x1e3e as uint32,
    },
    pg_unicode_range {
        first: 0x1e40 as uint32,
        last: 0x1e40 as uint32,
    },
    pg_unicode_range {
        first: 0x1e42 as uint32,
        last: 0x1e42 as uint32,
    },
    pg_unicode_range {
        first: 0x1e44 as uint32,
        last: 0x1e44 as uint32,
    },
    pg_unicode_range {
        first: 0x1e46 as uint32,
        last: 0x1e46 as uint32,
    },
    pg_unicode_range {
        first: 0x1e48 as uint32,
        last: 0x1e48 as uint32,
    },
    pg_unicode_range {
        first: 0x1e4a as uint32,
        last: 0x1e4a as uint32,
    },
    pg_unicode_range {
        first: 0x1e4c as uint32,
        last: 0x1e4c as uint32,
    },
    pg_unicode_range {
        first: 0x1e4e as uint32,
        last: 0x1e4e as uint32,
    },
    pg_unicode_range {
        first: 0x1e50 as uint32,
        last: 0x1e50 as uint32,
    },
    pg_unicode_range {
        first: 0x1e52 as uint32,
        last: 0x1e52 as uint32,
    },
    pg_unicode_range {
        first: 0x1e54 as uint32,
        last: 0x1e54 as uint32,
    },
    pg_unicode_range {
        first: 0x1e56 as uint32,
        last: 0x1e56 as uint32,
    },
    pg_unicode_range {
        first: 0x1e58 as uint32,
        last: 0x1e58 as uint32,
    },
    pg_unicode_range {
        first: 0x1e5a as uint32,
        last: 0x1e5a as uint32,
    },
    pg_unicode_range {
        first: 0x1e5c as uint32,
        last: 0x1e5c as uint32,
    },
    pg_unicode_range {
        first: 0x1e5e as uint32,
        last: 0x1e5e as uint32,
    },
    pg_unicode_range {
        first: 0x1e60 as uint32,
        last: 0x1e60 as uint32,
    },
    pg_unicode_range {
        first: 0x1e62 as uint32,
        last: 0x1e62 as uint32,
    },
    pg_unicode_range {
        first: 0x1e64 as uint32,
        last: 0x1e64 as uint32,
    },
    pg_unicode_range {
        first: 0x1e66 as uint32,
        last: 0x1e66 as uint32,
    },
    pg_unicode_range {
        first: 0x1e68 as uint32,
        last: 0x1e68 as uint32,
    },
    pg_unicode_range {
        first: 0x1e6a as uint32,
        last: 0x1e6a as uint32,
    },
    pg_unicode_range {
        first: 0x1e6c as uint32,
        last: 0x1e6c as uint32,
    },
    pg_unicode_range {
        first: 0x1e6e as uint32,
        last: 0x1e6e as uint32,
    },
    pg_unicode_range {
        first: 0x1e70 as uint32,
        last: 0x1e70 as uint32,
    },
    pg_unicode_range {
        first: 0x1e72 as uint32,
        last: 0x1e72 as uint32,
    },
    pg_unicode_range {
        first: 0x1e74 as uint32,
        last: 0x1e74 as uint32,
    },
    pg_unicode_range {
        first: 0x1e76 as uint32,
        last: 0x1e76 as uint32,
    },
    pg_unicode_range {
        first: 0x1e78 as uint32,
        last: 0x1e78 as uint32,
    },
    pg_unicode_range {
        first: 0x1e7a as uint32,
        last: 0x1e7a as uint32,
    },
    pg_unicode_range {
        first: 0x1e7c as uint32,
        last: 0x1e7c as uint32,
    },
    pg_unicode_range {
        first: 0x1e7e as uint32,
        last: 0x1e7e as uint32,
    },
    pg_unicode_range {
        first: 0x1e80 as uint32,
        last: 0x1e80 as uint32,
    },
    pg_unicode_range {
        first: 0x1e82 as uint32,
        last: 0x1e82 as uint32,
    },
    pg_unicode_range {
        first: 0x1e84 as uint32,
        last: 0x1e84 as uint32,
    },
    pg_unicode_range {
        first: 0x1e86 as uint32,
        last: 0x1e86 as uint32,
    },
    pg_unicode_range {
        first: 0x1e88 as uint32,
        last: 0x1e88 as uint32,
    },
    pg_unicode_range {
        first: 0x1e8a as uint32,
        last: 0x1e8a as uint32,
    },
    pg_unicode_range {
        first: 0x1e8c as uint32,
        last: 0x1e8c as uint32,
    },
    pg_unicode_range {
        first: 0x1e8e as uint32,
        last: 0x1e8e as uint32,
    },
    pg_unicode_range {
        first: 0x1e90 as uint32,
        last: 0x1e90 as uint32,
    },
    pg_unicode_range {
        first: 0x1e92 as uint32,
        last: 0x1e92 as uint32,
    },
    pg_unicode_range {
        first: 0x1e94 as uint32,
        last: 0x1e94 as uint32,
    },
    pg_unicode_range {
        first: 0x1e9e as uint32,
        last: 0x1e9e as uint32,
    },
    pg_unicode_range {
        first: 0x1ea0 as uint32,
        last: 0x1ea0 as uint32,
    },
    pg_unicode_range {
        first: 0x1ea2 as uint32,
        last: 0x1ea2 as uint32,
    },
    pg_unicode_range {
        first: 0x1ea4 as uint32,
        last: 0x1ea4 as uint32,
    },
    pg_unicode_range {
        first: 0x1ea6 as uint32,
        last: 0x1ea6 as uint32,
    },
    pg_unicode_range {
        first: 0x1ea8 as uint32,
        last: 0x1ea8 as uint32,
    },
    pg_unicode_range {
        first: 0x1eaa as uint32,
        last: 0x1eaa as uint32,
    },
    pg_unicode_range {
        first: 0x1eac as uint32,
        last: 0x1eac as uint32,
    },
    pg_unicode_range {
        first: 0x1eae as uint32,
        last: 0x1eae as uint32,
    },
    pg_unicode_range {
        first: 0x1eb0 as uint32,
        last: 0x1eb0 as uint32,
    },
    pg_unicode_range {
        first: 0x1eb2 as uint32,
        last: 0x1eb2 as uint32,
    },
    pg_unicode_range {
        first: 0x1eb4 as uint32,
        last: 0x1eb4 as uint32,
    },
    pg_unicode_range {
        first: 0x1eb6 as uint32,
        last: 0x1eb6 as uint32,
    },
    pg_unicode_range {
        first: 0x1eb8 as uint32,
        last: 0x1eb8 as uint32,
    },
    pg_unicode_range {
        first: 0x1eba as uint32,
        last: 0x1eba as uint32,
    },
    pg_unicode_range {
        first: 0x1ebc as uint32,
        last: 0x1ebc as uint32,
    },
    pg_unicode_range {
        first: 0x1ebe as uint32,
        last: 0x1ebe as uint32,
    },
    pg_unicode_range {
        first: 0x1ec0 as uint32,
        last: 0x1ec0 as uint32,
    },
    pg_unicode_range {
        first: 0x1ec2 as uint32,
        last: 0x1ec2 as uint32,
    },
    pg_unicode_range {
        first: 0x1ec4 as uint32,
        last: 0x1ec4 as uint32,
    },
    pg_unicode_range {
        first: 0x1ec6 as uint32,
        last: 0x1ec6 as uint32,
    },
    pg_unicode_range {
        first: 0x1ec8 as uint32,
        last: 0x1ec8 as uint32,
    },
    pg_unicode_range {
        first: 0x1eca as uint32,
        last: 0x1eca as uint32,
    },
    pg_unicode_range {
        first: 0x1ecc as uint32,
        last: 0x1ecc as uint32,
    },
    pg_unicode_range {
        first: 0x1ece as uint32,
        last: 0x1ece as uint32,
    },
    pg_unicode_range {
        first: 0x1ed0 as uint32,
        last: 0x1ed0 as uint32,
    },
    pg_unicode_range {
        first: 0x1ed2 as uint32,
        last: 0x1ed2 as uint32,
    },
    pg_unicode_range {
        first: 0x1ed4 as uint32,
        last: 0x1ed4 as uint32,
    },
    pg_unicode_range {
        first: 0x1ed6 as uint32,
        last: 0x1ed6 as uint32,
    },
    pg_unicode_range {
        first: 0x1ed8 as uint32,
        last: 0x1ed8 as uint32,
    },
    pg_unicode_range {
        first: 0x1eda as uint32,
        last: 0x1eda as uint32,
    },
    pg_unicode_range {
        first: 0x1edc as uint32,
        last: 0x1edc as uint32,
    },
    pg_unicode_range {
        first: 0x1ede as uint32,
        last: 0x1ede as uint32,
    },
    pg_unicode_range {
        first: 0x1ee0 as uint32,
        last: 0x1ee0 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee2 as uint32,
        last: 0x1ee2 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee4 as uint32,
        last: 0x1ee4 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee6 as uint32,
        last: 0x1ee6 as uint32,
    },
    pg_unicode_range {
        first: 0x1ee8 as uint32,
        last: 0x1ee8 as uint32,
    },
    pg_unicode_range {
        first: 0x1eea as uint32,
        last: 0x1eea as uint32,
    },
    pg_unicode_range {
        first: 0x1eec as uint32,
        last: 0x1eec as uint32,
    },
    pg_unicode_range {
        first: 0x1eee as uint32,
        last: 0x1eee as uint32,
    },
    pg_unicode_range {
        first: 0x1ef0 as uint32,
        last: 0x1ef0 as uint32,
    },
    pg_unicode_range {
        first: 0x1ef2 as uint32,
        last: 0x1ef2 as uint32,
    },
    pg_unicode_range {
        first: 0x1ef4 as uint32,
        last: 0x1ef4 as uint32,
    },
    pg_unicode_range {
        first: 0x1ef6 as uint32,
        last: 0x1ef6 as uint32,
    },
    pg_unicode_range {
        first: 0x1ef8 as uint32,
        last: 0x1ef8 as uint32,
    },
    pg_unicode_range {
        first: 0x1efa as uint32,
        last: 0x1efa as uint32,
    },
    pg_unicode_range {
        first: 0x1efc as uint32,
        last: 0x1efc as uint32,
    },
    pg_unicode_range {
        first: 0x1efe as uint32,
        last: 0x1efe as uint32,
    },
    pg_unicode_range {
        first: 0x1f08 as uint32,
        last: 0x1f0f as uint32,
    },
    pg_unicode_range {
        first: 0x1f18 as uint32,
        last: 0x1f1d as uint32,
    },
    pg_unicode_range {
        first: 0x1f28 as uint32,
        last: 0x1f2f as uint32,
    },
    pg_unicode_range {
        first: 0x1f38 as uint32,
        last: 0x1f3f as uint32,
    },
    pg_unicode_range {
        first: 0x1f48 as uint32,
        last: 0x1f4d as uint32,
    },
    pg_unicode_range {
        first: 0x1f59 as uint32,
        last: 0x1f59 as uint32,
    },
    pg_unicode_range {
        first: 0x1f5b as uint32,
        last: 0x1f5b as uint32,
    },
    pg_unicode_range {
        first: 0x1f5d as uint32,
        last: 0x1f5d as uint32,
    },
    pg_unicode_range {
        first: 0x1f5f as uint32,
        last: 0x1f5f as uint32,
    },
    pg_unicode_range {
        first: 0x1f68 as uint32,
        last: 0x1f6f as uint32,
    },
    pg_unicode_range {
        first: 0x1fb8 as uint32,
        last: 0x1fbb as uint32,
    },
    pg_unicode_range {
        first: 0x1fc8 as uint32,
        last: 0x1fcb as uint32,
    },
    pg_unicode_range {
        first: 0x1fd8 as uint32,
        last: 0x1fdb as uint32,
    },
    pg_unicode_range {
        first: 0x1fe8 as uint32,
        last: 0x1fec as uint32,
    },
    pg_unicode_range {
        first: 0x1ff8 as uint32,
        last: 0x1ffb as uint32,
    },
    pg_unicode_range {
        first: 0x2102 as uint32,
        last: 0x2102 as uint32,
    },
    pg_unicode_range {
        first: 0x2107 as uint32,
        last: 0x2107 as uint32,
    },
    pg_unicode_range {
        first: 0x210b as uint32,
        last: 0x210d as uint32,
    },
    pg_unicode_range {
        first: 0x2110 as uint32,
        last: 0x2112 as uint32,
    },
    pg_unicode_range {
        first: 0x2115 as uint32,
        last: 0x2115 as uint32,
    },
    pg_unicode_range {
        first: 0x2119 as uint32,
        last: 0x211d as uint32,
    },
    pg_unicode_range {
        first: 0x2124 as uint32,
        last: 0x2124 as uint32,
    },
    pg_unicode_range {
        first: 0x2126 as uint32,
        last: 0x2126 as uint32,
    },
    pg_unicode_range {
        first: 0x2128 as uint32,
        last: 0x2128 as uint32,
    },
    pg_unicode_range {
        first: 0x212a as uint32,
        last: 0x212d as uint32,
    },
    pg_unicode_range {
        first: 0x2130 as uint32,
        last: 0x2133 as uint32,
    },
    pg_unicode_range {
        first: 0x213e as uint32,
        last: 0x213f as uint32,
    },
    pg_unicode_range {
        first: 0x2145 as uint32,
        last: 0x2145 as uint32,
    },
    pg_unicode_range {
        first: 0x2160 as uint32,
        last: 0x216f as uint32,
    },
    pg_unicode_range {
        first: 0x2183 as uint32,
        last: 0x2183 as uint32,
    },
    pg_unicode_range {
        first: 0x24b6 as uint32,
        last: 0x24cf as uint32,
    },
    pg_unicode_range {
        first: 0x2c00 as uint32,
        last: 0x2c2f as uint32,
    },
    pg_unicode_range {
        first: 0x2c60 as uint32,
        last: 0x2c60 as uint32,
    },
    pg_unicode_range {
        first: 0x2c62 as uint32,
        last: 0x2c64 as uint32,
    },
    pg_unicode_range {
        first: 0x2c67 as uint32,
        last: 0x2c67 as uint32,
    },
    pg_unicode_range {
        first: 0x2c69 as uint32,
        last: 0x2c69 as uint32,
    },
    pg_unicode_range {
        first: 0x2c6b as uint32,
        last: 0x2c6b as uint32,
    },
    pg_unicode_range {
        first: 0x2c6d as uint32,
        last: 0x2c70 as uint32,
    },
    pg_unicode_range {
        first: 0x2c72 as uint32,
        last: 0x2c72 as uint32,
    },
    pg_unicode_range {
        first: 0x2c75 as uint32,
        last: 0x2c75 as uint32,
    },
    pg_unicode_range {
        first: 0x2c7e as uint32,
        last: 0x2c80 as uint32,
    },
    pg_unicode_range {
        first: 0x2c82 as uint32,
        last: 0x2c82 as uint32,
    },
    pg_unicode_range {
        first: 0x2c84 as uint32,
        last: 0x2c84 as uint32,
    },
    pg_unicode_range {
        first: 0x2c86 as uint32,
        last: 0x2c86 as uint32,
    },
    pg_unicode_range {
        first: 0x2c88 as uint32,
        last: 0x2c88 as uint32,
    },
    pg_unicode_range {
        first: 0x2c8a as uint32,
        last: 0x2c8a as uint32,
    },
    pg_unicode_range {
        first: 0x2c8c as uint32,
        last: 0x2c8c as uint32,
    },
    pg_unicode_range {
        first: 0x2c8e as uint32,
        last: 0x2c8e as uint32,
    },
    pg_unicode_range {
        first: 0x2c90 as uint32,
        last: 0x2c90 as uint32,
    },
    pg_unicode_range {
        first: 0x2c92 as uint32,
        last: 0x2c92 as uint32,
    },
    pg_unicode_range {
        first: 0x2c94 as uint32,
        last: 0x2c94 as uint32,
    },
    pg_unicode_range {
        first: 0x2c96 as uint32,
        last: 0x2c96 as uint32,
    },
    pg_unicode_range {
        first: 0x2c98 as uint32,
        last: 0x2c98 as uint32,
    },
    pg_unicode_range {
        first: 0x2c9a as uint32,
        last: 0x2c9a as uint32,
    },
    pg_unicode_range {
        first: 0x2c9c as uint32,
        last: 0x2c9c as uint32,
    },
    pg_unicode_range {
        first: 0x2c9e as uint32,
        last: 0x2c9e as uint32,
    },
    pg_unicode_range {
        first: 0x2ca0 as uint32,
        last: 0x2ca0 as uint32,
    },
    pg_unicode_range {
        first: 0x2ca2 as uint32,
        last: 0x2ca2 as uint32,
    },
    pg_unicode_range {
        first: 0x2ca4 as uint32,
        last: 0x2ca4 as uint32,
    },
    pg_unicode_range {
        first: 0x2ca6 as uint32,
        last: 0x2ca6 as uint32,
    },
    pg_unicode_range {
        first: 0x2ca8 as uint32,
        last: 0x2ca8 as uint32,
    },
    pg_unicode_range {
        first: 0x2caa as uint32,
        last: 0x2caa as uint32,
    },
    pg_unicode_range {
        first: 0x2cac as uint32,
        last: 0x2cac as uint32,
    },
    pg_unicode_range {
        first: 0x2cae as uint32,
        last: 0x2cae as uint32,
    },
    pg_unicode_range {
        first: 0x2cb0 as uint32,
        last: 0x2cb0 as uint32,
    },
    pg_unicode_range {
        first: 0x2cb2 as uint32,
        last: 0x2cb2 as uint32,
    },
    pg_unicode_range {
        first: 0x2cb4 as uint32,
        last: 0x2cb4 as uint32,
    },
    pg_unicode_range {
        first: 0x2cb6 as uint32,
        last: 0x2cb6 as uint32,
    },
    pg_unicode_range {
        first: 0x2cb8 as uint32,
        last: 0x2cb8 as uint32,
    },
    pg_unicode_range {
        first: 0x2cba as uint32,
        last: 0x2cba as uint32,
    },
    pg_unicode_range {
        first: 0x2cbc as uint32,
        last: 0x2cbc as uint32,
    },
    pg_unicode_range {
        first: 0x2cbe as uint32,
        last: 0x2cbe as uint32,
    },
    pg_unicode_range {
        first: 0x2cc0 as uint32,
        last: 0x2cc0 as uint32,
    },
    pg_unicode_range {
        first: 0x2cc2 as uint32,
        last: 0x2cc2 as uint32,
    },
    pg_unicode_range {
        first: 0x2cc4 as uint32,
        last: 0x2cc4 as uint32,
    },
    pg_unicode_range {
        first: 0x2cc6 as uint32,
        last: 0x2cc6 as uint32,
    },
    pg_unicode_range {
        first: 0x2cc8 as uint32,
        last: 0x2cc8 as uint32,
    },
    pg_unicode_range {
        first: 0x2cca as uint32,
        last: 0x2cca as uint32,
    },
    pg_unicode_range {
        first: 0x2ccc as uint32,
        last: 0x2ccc as uint32,
    },
    pg_unicode_range {
        first: 0x2cce as uint32,
        last: 0x2cce as uint32,
    },
    pg_unicode_range {
        first: 0x2cd0 as uint32,
        last: 0x2cd0 as uint32,
    },
    pg_unicode_range {
        first: 0x2cd2 as uint32,
        last: 0x2cd2 as uint32,
    },
    pg_unicode_range {
        first: 0x2cd4 as uint32,
        last: 0x2cd4 as uint32,
    },
    pg_unicode_range {
        first: 0x2cd6 as uint32,
        last: 0x2cd6 as uint32,
    },
    pg_unicode_range {
        first: 0x2cd8 as uint32,
        last: 0x2cd8 as uint32,
    },
    pg_unicode_range {
        first: 0x2cda as uint32,
        last: 0x2cda as uint32,
    },
    pg_unicode_range {
        first: 0x2cdc as uint32,
        last: 0x2cdc as uint32,
    },
    pg_unicode_range {
        first: 0x2cde as uint32,
        last: 0x2cde as uint32,
    },
    pg_unicode_range {
        first: 0x2ce0 as uint32,
        last: 0x2ce0 as uint32,
    },
    pg_unicode_range {
        first: 0x2ce2 as uint32,
        last: 0x2ce2 as uint32,
    },
    pg_unicode_range {
        first: 0x2ceb as uint32,
        last: 0x2ceb as uint32,
    },
    pg_unicode_range {
        first: 0x2ced as uint32,
        last: 0x2ced as uint32,
    },
    pg_unicode_range {
        first: 0x2cf2 as uint32,
        last: 0x2cf2 as uint32,
    },
    pg_unicode_range {
        first: 0xa640 as uint32,
        last: 0xa640 as uint32,
    },
    pg_unicode_range {
        first: 0xa642 as uint32,
        last: 0xa642 as uint32,
    },
    pg_unicode_range {
        first: 0xa644 as uint32,
        last: 0xa644 as uint32,
    },
    pg_unicode_range {
        first: 0xa646 as uint32,
        last: 0xa646 as uint32,
    },
    pg_unicode_range {
        first: 0xa648 as uint32,
        last: 0xa648 as uint32,
    },
    pg_unicode_range {
        first: 0xa64a as uint32,
        last: 0xa64a as uint32,
    },
    pg_unicode_range {
        first: 0xa64c as uint32,
        last: 0xa64c as uint32,
    },
    pg_unicode_range {
        first: 0xa64e as uint32,
        last: 0xa64e as uint32,
    },
    pg_unicode_range {
        first: 0xa650 as uint32,
        last: 0xa650 as uint32,
    },
    pg_unicode_range {
        first: 0xa652 as uint32,
        last: 0xa652 as uint32,
    },
    pg_unicode_range {
        first: 0xa654 as uint32,
        last: 0xa654 as uint32,
    },
    pg_unicode_range {
        first: 0xa656 as uint32,
        last: 0xa656 as uint32,
    },
    pg_unicode_range {
        first: 0xa658 as uint32,
        last: 0xa658 as uint32,
    },
    pg_unicode_range {
        first: 0xa65a as uint32,
        last: 0xa65a as uint32,
    },
    pg_unicode_range {
        first: 0xa65c as uint32,
        last: 0xa65c as uint32,
    },
    pg_unicode_range {
        first: 0xa65e as uint32,
        last: 0xa65e as uint32,
    },
    pg_unicode_range {
        first: 0xa660 as uint32,
        last: 0xa660 as uint32,
    },
    pg_unicode_range {
        first: 0xa662 as uint32,
        last: 0xa662 as uint32,
    },
    pg_unicode_range {
        first: 0xa664 as uint32,
        last: 0xa664 as uint32,
    },
    pg_unicode_range {
        first: 0xa666 as uint32,
        last: 0xa666 as uint32,
    },
    pg_unicode_range {
        first: 0xa668 as uint32,
        last: 0xa668 as uint32,
    },
    pg_unicode_range {
        first: 0xa66a as uint32,
        last: 0xa66a as uint32,
    },
    pg_unicode_range {
        first: 0xa66c as uint32,
        last: 0xa66c as uint32,
    },
    pg_unicode_range {
        first: 0xa680 as uint32,
        last: 0xa680 as uint32,
    },
    pg_unicode_range {
        first: 0xa682 as uint32,
        last: 0xa682 as uint32,
    },
    pg_unicode_range {
        first: 0xa684 as uint32,
        last: 0xa684 as uint32,
    },
    pg_unicode_range {
        first: 0xa686 as uint32,
        last: 0xa686 as uint32,
    },
    pg_unicode_range {
        first: 0xa688 as uint32,
        last: 0xa688 as uint32,
    },
    pg_unicode_range {
        first: 0xa68a as uint32,
        last: 0xa68a as uint32,
    },
    pg_unicode_range {
        first: 0xa68c as uint32,
        last: 0xa68c as uint32,
    },
    pg_unicode_range {
        first: 0xa68e as uint32,
        last: 0xa68e as uint32,
    },
    pg_unicode_range {
        first: 0xa690 as uint32,
        last: 0xa690 as uint32,
    },
    pg_unicode_range {
        first: 0xa692 as uint32,
        last: 0xa692 as uint32,
    },
    pg_unicode_range {
        first: 0xa694 as uint32,
        last: 0xa694 as uint32,
    },
    pg_unicode_range {
        first: 0xa696 as uint32,
        last: 0xa696 as uint32,
    },
    pg_unicode_range {
        first: 0xa698 as uint32,
        last: 0xa698 as uint32,
    },
    pg_unicode_range {
        first: 0xa69a as uint32,
        last: 0xa69a as uint32,
    },
    pg_unicode_range {
        first: 0xa722 as uint32,
        last: 0xa722 as uint32,
    },
    pg_unicode_range {
        first: 0xa724 as uint32,
        last: 0xa724 as uint32,
    },
    pg_unicode_range {
        first: 0xa726 as uint32,
        last: 0xa726 as uint32,
    },
    pg_unicode_range {
        first: 0xa728 as uint32,
        last: 0xa728 as uint32,
    },
    pg_unicode_range {
        first: 0xa72a as uint32,
        last: 0xa72a as uint32,
    },
    pg_unicode_range {
        first: 0xa72c as uint32,
        last: 0xa72c as uint32,
    },
    pg_unicode_range {
        first: 0xa72e as uint32,
        last: 0xa72e as uint32,
    },
    pg_unicode_range {
        first: 0xa732 as uint32,
        last: 0xa732 as uint32,
    },
    pg_unicode_range {
        first: 0xa734 as uint32,
        last: 0xa734 as uint32,
    },
    pg_unicode_range {
        first: 0xa736 as uint32,
        last: 0xa736 as uint32,
    },
    pg_unicode_range {
        first: 0xa738 as uint32,
        last: 0xa738 as uint32,
    },
    pg_unicode_range {
        first: 0xa73a as uint32,
        last: 0xa73a as uint32,
    },
    pg_unicode_range {
        first: 0xa73c as uint32,
        last: 0xa73c as uint32,
    },
    pg_unicode_range {
        first: 0xa73e as uint32,
        last: 0xa73e as uint32,
    },
    pg_unicode_range {
        first: 0xa740 as uint32,
        last: 0xa740 as uint32,
    },
    pg_unicode_range {
        first: 0xa742 as uint32,
        last: 0xa742 as uint32,
    },
    pg_unicode_range {
        first: 0xa744 as uint32,
        last: 0xa744 as uint32,
    },
    pg_unicode_range {
        first: 0xa746 as uint32,
        last: 0xa746 as uint32,
    },
    pg_unicode_range {
        first: 0xa748 as uint32,
        last: 0xa748 as uint32,
    },
    pg_unicode_range {
        first: 0xa74a as uint32,
        last: 0xa74a as uint32,
    },
    pg_unicode_range {
        first: 0xa74c as uint32,
        last: 0xa74c as uint32,
    },
    pg_unicode_range {
        first: 0xa74e as uint32,
        last: 0xa74e as uint32,
    },
    pg_unicode_range {
        first: 0xa750 as uint32,
        last: 0xa750 as uint32,
    },
    pg_unicode_range {
        first: 0xa752 as uint32,
        last: 0xa752 as uint32,
    },
    pg_unicode_range {
        first: 0xa754 as uint32,
        last: 0xa754 as uint32,
    },
    pg_unicode_range {
        first: 0xa756 as uint32,
        last: 0xa756 as uint32,
    },
    pg_unicode_range {
        first: 0xa758 as uint32,
        last: 0xa758 as uint32,
    },
    pg_unicode_range {
        first: 0xa75a as uint32,
        last: 0xa75a as uint32,
    },
    pg_unicode_range {
        first: 0xa75c as uint32,
        last: 0xa75c as uint32,
    },
    pg_unicode_range {
        first: 0xa75e as uint32,
        last: 0xa75e as uint32,
    },
    pg_unicode_range {
        first: 0xa760 as uint32,
        last: 0xa760 as uint32,
    },
    pg_unicode_range {
        first: 0xa762 as uint32,
        last: 0xa762 as uint32,
    },
    pg_unicode_range {
        first: 0xa764 as uint32,
        last: 0xa764 as uint32,
    },
    pg_unicode_range {
        first: 0xa766 as uint32,
        last: 0xa766 as uint32,
    },
    pg_unicode_range {
        first: 0xa768 as uint32,
        last: 0xa768 as uint32,
    },
    pg_unicode_range {
        first: 0xa76a as uint32,
        last: 0xa76a as uint32,
    },
    pg_unicode_range {
        first: 0xa76c as uint32,
        last: 0xa76c as uint32,
    },
    pg_unicode_range {
        first: 0xa76e as uint32,
        last: 0xa76e as uint32,
    },
    pg_unicode_range {
        first: 0xa779 as uint32,
        last: 0xa779 as uint32,
    },
    pg_unicode_range {
        first: 0xa77b as uint32,
        last: 0xa77b as uint32,
    },
    pg_unicode_range {
        first: 0xa77d as uint32,
        last: 0xa77e as uint32,
    },
    pg_unicode_range {
        first: 0xa780 as uint32,
        last: 0xa780 as uint32,
    },
    pg_unicode_range {
        first: 0xa782 as uint32,
        last: 0xa782 as uint32,
    },
    pg_unicode_range {
        first: 0xa784 as uint32,
        last: 0xa784 as uint32,
    },
    pg_unicode_range {
        first: 0xa786 as uint32,
        last: 0xa786 as uint32,
    },
    pg_unicode_range {
        first: 0xa78b as uint32,
        last: 0xa78b as uint32,
    },
    pg_unicode_range {
        first: 0xa78d as uint32,
        last: 0xa78d as uint32,
    },
    pg_unicode_range {
        first: 0xa790 as uint32,
        last: 0xa790 as uint32,
    },
    pg_unicode_range {
        first: 0xa792 as uint32,
        last: 0xa792 as uint32,
    },
    pg_unicode_range {
        first: 0xa796 as uint32,
        last: 0xa796 as uint32,
    },
    pg_unicode_range {
        first: 0xa798 as uint32,
        last: 0xa798 as uint32,
    },
    pg_unicode_range {
        first: 0xa79a as uint32,
        last: 0xa79a as uint32,
    },
    pg_unicode_range {
        first: 0xa79c as uint32,
        last: 0xa79c as uint32,
    },
    pg_unicode_range {
        first: 0xa79e as uint32,
        last: 0xa79e as uint32,
    },
    pg_unicode_range {
        first: 0xa7a0 as uint32,
        last: 0xa7a0 as uint32,
    },
    pg_unicode_range {
        first: 0xa7a2 as uint32,
        last: 0xa7a2 as uint32,
    },
    pg_unicode_range {
        first: 0xa7a4 as uint32,
        last: 0xa7a4 as uint32,
    },
    pg_unicode_range {
        first: 0xa7a6 as uint32,
        last: 0xa7a6 as uint32,
    },
    pg_unicode_range {
        first: 0xa7a8 as uint32,
        last: 0xa7a8 as uint32,
    },
    pg_unicode_range {
        first: 0xa7aa as uint32,
        last: 0xa7ae as uint32,
    },
    pg_unicode_range {
        first: 0xa7b0 as uint32,
        last: 0xa7b4 as uint32,
    },
    pg_unicode_range {
        first: 0xa7b6 as uint32,
        last: 0xa7b6 as uint32,
    },
    pg_unicode_range {
        first: 0xa7b8 as uint32,
        last: 0xa7b8 as uint32,
    },
    pg_unicode_range {
        first: 0xa7ba as uint32,
        last: 0xa7ba as uint32,
    },
    pg_unicode_range {
        first: 0xa7bc as uint32,
        last: 0xa7bc as uint32,
    },
    pg_unicode_range {
        first: 0xa7be as uint32,
        last: 0xa7be as uint32,
    },
    pg_unicode_range {
        first: 0xa7c0 as uint32,
        last: 0xa7c0 as uint32,
    },
    pg_unicode_range {
        first: 0xa7c2 as uint32,
        last: 0xa7c2 as uint32,
    },
    pg_unicode_range {
        first: 0xa7c4 as uint32,
        last: 0xa7c7 as uint32,
    },
    pg_unicode_range {
        first: 0xa7c9 as uint32,
        last: 0xa7c9 as uint32,
    },
    pg_unicode_range {
        first: 0xa7cb as uint32,
        last: 0xa7cc as uint32,
    },
    pg_unicode_range {
        first: 0xa7d0 as uint32,
        last: 0xa7d0 as uint32,
    },
    pg_unicode_range {
        first: 0xa7d6 as uint32,
        last: 0xa7d6 as uint32,
    },
    pg_unicode_range {
        first: 0xa7d8 as uint32,
        last: 0xa7d8 as uint32,
    },
    pg_unicode_range {
        first: 0xa7da as uint32,
        last: 0xa7da as uint32,
    },
    pg_unicode_range {
        first: 0xa7dc as uint32,
        last: 0xa7dc as uint32,
    },
    pg_unicode_range {
        first: 0xa7f5 as uint32,
        last: 0xa7f5 as uint32,
    },
    pg_unicode_range {
        first: 0xff21 as uint32,
        last: 0xff3a as uint32,
    },
    pg_unicode_range {
        first: 0x10400 as uint32,
        last: 0x10427 as uint32,
    },
    pg_unicode_range {
        first: 0x104b0 as uint32,
        last: 0x104d3 as uint32,
    },
    pg_unicode_range {
        first: 0x10570 as uint32,
        last: 0x1057a as uint32,
    },
    pg_unicode_range {
        first: 0x1057c as uint32,
        last: 0x1058a as uint32,
    },
    pg_unicode_range {
        first: 0x1058c as uint32,
        last: 0x10592 as uint32,
    },
    pg_unicode_range {
        first: 0x10594 as uint32,
        last: 0x10595 as uint32,
    },
    pg_unicode_range {
        first: 0x10c80 as uint32,
        last: 0x10cb2 as uint32,
    },
    pg_unicode_range {
        first: 0x10d50 as uint32,
        last: 0x10d65 as uint32,
    },
    pg_unicode_range {
        first: 0x118a0 as uint32,
        last: 0x118bf as uint32,
    },
    pg_unicode_range {
        first: 0x16e40 as uint32,
        last: 0x16e5f as uint32,
    },
    pg_unicode_range {
        first: 0x1d400 as uint32,
        last: 0x1d419 as uint32,
    },
    pg_unicode_range {
        first: 0x1d434 as uint32,
        last: 0x1d44d as uint32,
    },
    pg_unicode_range {
        first: 0x1d468 as uint32,
        last: 0x1d481 as uint32,
    },
    pg_unicode_range {
        first: 0x1d49c as uint32,
        last: 0x1d49c as uint32,
    },
    pg_unicode_range {
        first: 0x1d49e as uint32,
        last: 0x1d49f as uint32,
    },
    pg_unicode_range {
        first: 0x1d4a2 as uint32,
        last: 0x1d4a2 as uint32,
    },
    pg_unicode_range {
        first: 0x1d4a5 as uint32,
        last: 0x1d4a6 as uint32,
    },
    pg_unicode_range {
        first: 0x1d4a9 as uint32,
        last: 0x1d4ac as uint32,
    },
    pg_unicode_range {
        first: 0x1d4ae as uint32,
        last: 0x1d4b5 as uint32,
    },
    pg_unicode_range {
        first: 0x1d4d0 as uint32,
        last: 0x1d4e9 as uint32,
    },
    pg_unicode_range {
        first: 0x1d504 as uint32,
        last: 0x1d505 as uint32,
    },
    pg_unicode_range {
        first: 0x1d507 as uint32,
        last: 0x1d50a as uint32,
    },
    pg_unicode_range {
        first: 0x1d50d as uint32,
        last: 0x1d514 as uint32,
    },
    pg_unicode_range {
        first: 0x1d516 as uint32,
        last: 0x1d51c as uint32,
    },
    pg_unicode_range {
        first: 0x1d538 as uint32,
        last: 0x1d539 as uint32,
    },
    pg_unicode_range {
        first: 0x1d53b as uint32,
        last: 0x1d53e as uint32,
    },
    pg_unicode_range {
        first: 0x1d540 as uint32,
        last: 0x1d544 as uint32,
    },
    pg_unicode_range {
        first: 0x1d546 as uint32,
        last: 0x1d546 as uint32,
    },
    pg_unicode_range {
        first: 0x1d54a as uint32,
        last: 0x1d550 as uint32,
    },
    pg_unicode_range {
        first: 0x1d56c as uint32,
        last: 0x1d585 as uint32,
    },
    pg_unicode_range {
        first: 0x1d5a0 as uint32,
        last: 0x1d5b9 as uint32,
    },
    pg_unicode_range {
        first: 0x1d5d4 as uint32,
        last: 0x1d5ed as uint32,
    },
    pg_unicode_range {
        first: 0x1d608 as uint32,
        last: 0x1d621 as uint32,
    },
    pg_unicode_range {
        first: 0x1d63c as uint32,
        last: 0x1d655 as uint32,
    },
    pg_unicode_range {
        first: 0x1d670 as uint32,
        last: 0x1d689 as uint32,
    },
    pg_unicode_range {
        first: 0x1d6a8 as uint32,
        last: 0x1d6c0 as uint32,
    },
    pg_unicode_range {
        first: 0x1d6e2 as uint32,
        last: 0x1d6fa as uint32,
    },
    pg_unicode_range {
        first: 0x1d71c as uint32,
        last: 0x1d734 as uint32,
    },
    pg_unicode_range {
        first: 0x1d756 as uint32,
        last: 0x1d76e as uint32,
    },
    pg_unicode_range {
        first: 0x1d790 as uint32,
        last: 0x1d7a8 as uint32,
    },
    pg_unicode_range {
        first: 0x1d7ca as uint32,
        last: 0x1d7ca as uint32,
    },
    pg_unicode_range {
        first: 0x1e900 as uint32,
        last: 0x1e921 as uint32,
    },
    pg_unicode_range {
        first: 0x1f130 as uint32,
        last: 0x1f149 as uint32,
    },
    pg_unicode_range {
        first: 0x1f150 as uint32,
        last: 0x1f169 as uint32,
    },
    pg_unicode_range {
        first: 0x1f170 as uint32,
        last: 0x1f189 as uint32,
    },
];
pub(crate) static unicode_case_ignorable: [pg_unicode_range; 506] = [
    pg_unicode_range {
        first: 0x27 as uint32,
        last: 0x27 as uint32,
    },
    pg_unicode_range {
        first: 0x2e as uint32,
        last: 0x2e as uint32,
    },
    pg_unicode_range {
        first: 0x3a as uint32,
        last: 0x3a as uint32,
    },
    pg_unicode_range {
        first: 0x5e as uint32,
        last: 0x5e as uint32,
    },
    pg_unicode_range {
        first: 0x60 as uint32,
        last: 0x60 as uint32,
    },
    pg_unicode_range {
        first: 0xa8 as uint32,
        last: 0xa8 as uint32,
    },
    pg_unicode_range {
        first: 0xad as uint32,
        last: 0xad as uint32,
    },
    pg_unicode_range {
        first: 0xaf as uint32,
        last: 0xaf as uint32,
    },
    pg_unicode_range {
        first: 0xb4 as uint32,
        last: 0xb4 as uint32,
    },
    pg_unicode_range {
        first: 0xb7 as uint32,
        last: 0xb7 as uint32,
    },
    pg_unicode_range {
        first: 0xb8 as uint32,
        last: 0xb8 as uint32,
    },
    pg_unicode_range {
        first: 0x2b0 as uint32,
        last: 0x2c1 as uint32,
    },
    pg_unicode_range {
        first: 0x2c2 as uint32,
        last: 0x2c5 as uint32,
    },
    pg_unicode_range {
        first: 0x2c6 as uint32,
        last: 0x2d1 as uint32,
    },
    pg_unicode_range {
        first: 0x2d2 as uint32,
        last: 0x2df as uint32,
    },
    pg_unicode_range {
        first: 0x2e0 as uint32,
        last: 0x2e4 as uint32,
    },
    pg_unicode_range {
        first: 0x2e5 as uint32,
        last: 0x2eb as uint32,
    },
    pg_unicode_range {
        first: 0x2ec as uint32,
        last: 0x2ec as uint32,
    },
    pg_unicode_range {
        first: 0x2ed as uint32,
        last: 0x2ed as uint32,
    },
    pg_unicode_range {
        first: 0x2ee as uint32,
        last: 0x2ee as uint32,
    },
    pg_unicode_range {
        first: 0x2ef as uint32,
        last: 0x2ff as uint32,
    },
    pg_unicode_range {
        first: 0x300 as uint32,
        last: 0x36f as uint32,
    },
    pg_unicode_range {
        first: 0x374 as uint32,
        last: 0x374 as uint32,
    },
    pg_unicode_range {
        first: 0x375 as uint32,
        last: 0x375 as uint32,
    },
    pg_unicode_range {
        first: 0x37a as uint32,
        last: 0x37a as uint32,
    },
    pg_unicode_range {
        first: 0x384 as uint32,
        last: 0x385 as uint32,
    },
    pg_unicode_range {
        first: 0x387 as uint32,
        last: 0x387 as uint32,
    },
    pg_unicode_range {
        first: 0x483 as uint32,
        last: 0x487 as uint32,
    },
    pg_unicode_range {
        first: 0x488 as uint32,
        last: 0x489 as uint32,
    },
    pg_unicode_range {
        first: 0x559 as uint32,
        last: 0x559 as uint32,
    },
    pg_unicode_range {
        first: 0x55f as uint32,
        last: 0x55f as uint32,
    },
    pg_unicode_range {
        first: 0x591 as uint32,
        last: 0x5bd as uint32,
    },
    pg_unicode_range {
        first: 0x5bf as uint32,
        last: 0x5bf as uint32,
    },
    pg_unicode_range {
        first: 0x5c1 as uint32,
        last: 0x5c2 as uint32,
    },
    pg_unicode_range {
        first: 0x5c4 as uint32,
        last: 0x5c5 as uint32,
    },
    pg_unicode_range {
        first: 0x5c7 as uint32,
        last: 0x5c7 as uint32,
    },
    pg_unicode_range {
        first: 0x5f4 as uint32,
        last: 0x5f4 as uint32,
    },
    pg_unicode_range {
        first: 0x600 as uint32,
        last: 0x605 as uint32,
    },
    pg_unicode_range {
        first: 0x610 as uint32,
        last: 0x61a as uint32,
    },
    pg_unicode_range {
        first: 0x61c as uint32,
        last: 0x61c as uint32,
    },
    pg_unicode_range {
        first: 0x640 as uint32,
        last: 0x640 as uint32,
    },
    pg_unicode_range {
        first: 0x64b as uint32,
        last: 0x65f as uint32,
    },
    pg_unicode_range {
        first: 0x670 as uint32,
        last: 0x670 as uint32,
    },
    pg_unicode_range {
        first: 0x6d6 as uint32,
        last: 0x6dc as uint32,
    },
    pg_unicode_range {
        first: 0x6dd as uint32,
        last: 0x6dd as uint32,
    },
    pg_unicode_range {
        first: 0x6df as uint32,
        last: 0x6e4 as uint32,
    },
    pg_unicode_range {
        first: 0x6e5 as uint32,
        last: 0x6e6 as uint32,
    },
    pg_unicode_range {
        first: 0x6e7 as uint32,
        last: 0x6e8 as uint32,
    },
    pg_unicode_range {
        first: 0x6ea as uint32,
        last: 0x6ed as uint32,
    },
    pg_unicode_range {
        first: 0x70f as uint32,
        last: 0x70f as uint32,
    },
    pg_unicode_range {
        first: 0x711 as uint32,
        last: 0x711 as uint32,
    },
    pg_unicode_range {
        first: 0x730 as uint32,
        last: 0x74a as uint32,
    },
    pg_unicode_range {
        first: 0x7a6 as uint32,
        last: 0x7b0 as uint32,
    },
    pg_unicode_range {
        first: 0x7eb as uint32,
        last: 0x7f3 as uint32,
    },
    pg_unicode_range {
        first: 0x7f4 as uint32,
        last: 0x7f5 as uint32,
    },
    pg_unicode_range {
        first: 0x7fa as uint32,
        last: 0x7fa as uint32,
    },
    pg_unicode_range {
        first: 0x7fd as uint32,
        last: 0x7fd as uint32,
    },
    pg_unicode_range {
        first: 0x816 as uint32,
        last: 0x819 as uint32,
    },
    pg_unicode_range {
        first: 0x81a as uint32,
        last: 0x81a as uint32,
    },
    pg_unicode_range {
        first: 0x81b as uint32,
        last: 0x823 as uint32,
    },
    pg_unicode_range {
        first: 0x824 as uint32,
        last: 0x824 as uint32,
    },
    pg_unicode_range {
        first: 0x825 as uint32,
        last: 0x827 as uint32,
    },
    pg_unicode_range {
        first: 0x828 as uint32,
        last: 0x828 as uint32,
    },
    pg_unicode_range {
        first: 0x829 as uint32,
        last: 0x82d as uint32,
    },
    pg_unicode_range {
        first: 0x859 as uint32,
        last: 0x85b as uint32,
    },
    pg_unicode_range {
        first: 0x888 as uint32,
        last: 0x888 as uint32,
    },
    pg_unicode_range {
        first: 0x890 as uint32,
        last: 0x891 as uint32,
    },
    pg_unicode_range {
        first: 0x897 as uint32,
        last: 0x89f as uint32,
    },
    pg_unicode_range {
        first: 0x8c9 as uint32,
        last: 0x8c9 as uint32,
    },
    pg_unicode_range {
        first: 0x8ca as uint32,
        last: 0x8e1 as uint32,
    },
    pg_unicode_range {
        first: 0x8e2 as uint32,
        last: 0x8e2 as uint32,
    },
    pg_unicode_range {
        first: 0x8e3 as uint32,
        last: 0x902 as uint32,
    },
    pg_unicode_range {
        first: 0x93a as uint32,
        last: 0x93a as uint32,
    },
    pg_unicode_range {
        first: 0x93c as uint32,
        last: 0x93c as uint32,
    },
    pg_unicode_range {
        first: 0x941 as uint32,
        last: 0x948 as uint32,
    },
    pg_unicode_range {
        first: 0x94d as uint32,
        last: 0x94d as uint32,
    },
    pg_unicode_range {
        first: 0x951 as uint32,
        last: 0x957 as uint32,
    },
    pg_unicode_range {
        first: 0x962 as uint32,
        last: 0x963 as uint32,
    },
    pg_unicode_range {
        first: 0x971 as uint32,
        last: 0x971 as uint32,
    },
    pg_unicode_range {
        first: 0x981 as uint32,
        last: 0x981 as uint32,
    },
    pg_unicode_range {
        first: 0x9bc as uint32,
        last: 0x9bc as uint32,
    },
    pg_unicode_range {
        first: 0x9c1 as uint32,
        last: 0x9c4 as uint32,
    },
    pg_unicode_range {
        first: 0x9cd as uint32,
        last: 0x9cd as uint32,
    },
    pg_unicode_range {
        first: 0x9e2 as uint32,
        last: 0x9e3 as uint32,
    },
    pg_unicode_range {
        first: 0x9fe as uint32,
        last: 0x9fe as uint32,
    },
    pg_unicode_range {
        first: 0xa01 as uint32,
        last: 0xa02 as uint32,
    },
    pg_unicode_range {
        first: 0xa3c as uint32,
        last: 0xa3c as uint32,
    },
    pg_unicode_range {
        first: 0xa41 as uint32,
        last: 0xa42 as uint32,
    },
    pg_unicode_range {
        first: 0xa47 as uint32,
        last: 0xa48 as uint32,
    },
    pg_unicode_range {
        first: 0xa4b as uint32,
        last: 0xa4d as uint32,
    },
    pg_unicode_range {
        first: 0xa51 as uint32,
        last: 0xa51 as uint32,
    },
    pg_unicode_range {
        first: 0xa70 as uint32,
        last: 0xa71 as uint32,
    },
    pg_unicode_range {
        first: 0xa75 as uint32,
        last: 0xa75 as uint32,
    },
    pg_unicode_range {
        first: 0xa81 as uint32,
        last: 0xa82 as uint32,
    },
    pg_unicode_range {
        first: 0xabc as uint32,
        last: 0xabc as uint32,
    },
    pg_unicode_range {
        first: 0xac1 as uint32,
        last: 0xac5 as uint32,
    },
    pg_unicode_range {
        first: 0xac7 as uint32,
        last: 0xac8 as uint32,
    },
    pg_unicode_range {
        first: 0xacd as uint32,
        last: 0xacd as uint32,
    },
    pg_unicode_range {
        first: 0xae2 as uint32,
        last: 0xae3 as uint32,
    },
    pg_unicode_range {
        first: 0xafa as uint32,
        last: 0xaff as uint32,
    },
    pg_unicode_range {
        first: 0xb01 as uint32,
        last: 0xb01 as uint32,
    },
    pg_unicode_range {
        first: 0xb3c as uint32,
        last: 0xb3c as uint32,
    },
    pg_unicode_range {
        first: 0xb3f as uint32,
        last: 0xb3f as uint32,
    },
    pg_unicode_range {
        first: 0xb41 as uint32,
        last: 0xb44 as uint32,
    },
    pg_unicode_range {
        first: 0xb4d as uint32,
        last: 0xb4d as uint32,
    },
    pg_unicode_range {
        first: 0xb55 as uint32,
        last: 0xb56 as uint32,
    },
    pg_unicode_range {
        first: 0xb62 as uint32,
        last: 0xb63 as uint32,
    },
    pg_unicode_range {
        first: 0xb82 as uint32,
        last: 0xb82 as uint32,
    },
    pg_unicode_range {
        first: 0xbc0 as uint32,
        last: 0xbc0 as uint32,
    },
    pg_unicode_range {
        first: 0xbcd as uint32,
        last: 0xbcd as uint32,
    },
    pg_unicode_range {
        first: 0xc00 as uint32,
        last: 0xc00 as uint32,
    },
    pg_unicode_range {
        first: 0xc04 as uint32,
        last: 0xc04 as uint32,
    },
    pg_unicode_range {
        first: 0xc3c as uint32,
        last: 0xc3c as uint32,
    },
    pg_unicode_range {
        first: 0xc3e as uint32,
        last: 0xc40 as uint32,
    },
    pg_unicode_range {
        first: 0xc46 as uint32,
        last: 0xc48 as uint32,
    },
    pg_unicode_range {
        first: 0xc4a as uint32,
        last: 0xc4d as uint32,
    },
    pg_unicode_range {
        first: 0xc55 as uint32,
        last: 0xc56 as uint32,
    },
    pg_unicode_range {
        first: 0xc62 as uint32,
        last: 0xc63 as uint32,
    },
    pg_unicode_range {
        first: 0xc81 as uint32,
        last: 0xc81 as uint32,
    },
    pg_unicode_range {
        first: 0xcbc as uint32,
        last: 0xcbc as uint32,
    },
    pg_unicode_range {
        first: 0xcbf as uint32,
        last: 0xcbf as uint32,
    },
    pg_unicode_range {
        first: 0xcc6 as uint32,
        last: 0xcc6 as uint32,
    },
    pg_unicode_range {
        first: 0xccc as uint32,
        last: 0xccd as uint32,
    },
    pg_unicode_range {
        first: 0xce2 as uint32,
        last: 0xce3 as uint32,
    },
    pg_unicode_range {
        first: 0xd00 as uint32,
        last: 0xd01 as uint32,
    },
    pg_unicode_range {
        first: 0xd3b as uint32,
        last: 0xd3c as uint32,
    },
    pg_unicode_range {
        first: 0xd41 as uint32,
        last: 0xd44 as uint32,
    },
    pg_unicode_range {
        first: 0xd4d as uint32,
        last: 0xd4d as uint32,
    },
    pg_unicode_range {
        first: 0xd62 as uint32,
        last: 0xd63 as uint32,
    },
    pg_unicode_range {
        first: 0xd81 as uint32,
        last: 0xd81 as uint32,
    },
    pg_unicode_range {
        first: 0xdca as uint32,
        last: 0xdca as uint32,
    },
    pg_unicode_range {
        first: 0xdd2 as uint32,
        last: 0xdd4 as uint32,
    },
    pg_unicode_range {
        first: 0xdd6 as uint32,
        last: 0xdd6 as uint32,
    },
    pg_unicode_range {
        first: 0xe31 as uint32,
        last: 0xe31 as uint32,
    },
    pg_unicode_range {
        first: 0xe34 as uint32,
        last: 0xe3a as uint32,
    },
    pg_unicode_range {
        first: 0xe46 as uint32,
        last: 0xe46 as uint32,
    },
    pg_unicode_range {
        first: 0xe47 as uint32,
        last: 0xe4e as uint32,
    },
    pg_unicode_range {
        first: 0xeb1 as uint32,
        last: 0xeb1 as uint32,
    },
    pg_unicode_range {
        first: 0xeb4 as uint32,
        last: 0xebc as uint32,
    },
    pg_unicode_range {
        first: 0xec6 as uint32,
        last: 0xec6 as uint32,
    },
    pg_unicode_range {
        first: 0xec8 as uint32,
        last: 0xece as uint32,
    },
    pg_unicode_range {
        first: 0xf18 as uint32,
        last: 0xf19 as uint32,
    },
    pg_unicode_range {
        first: 0xf35 as uint32,
        last: 0xf35 as uint32,
    },
    pg_unicode_range {
        first: 0xf37 as uint32,
        last: 0xf37 as uint32,
    },
    pg_unicode_range {
        first: 0xf39 as uint32,
        last: 0xf39 as uint32,
    },
    pg_unicode_range {
        first: 0xf71 as uint32,
        last: 0xf7e as uint32,
    },
    pg_unicode_range {
        first: 0xf80 as uint32,
        last: 0xf84 as uint32,
    },
    pg_unicode_range {
        first: 0xf86 as uint32,
        last: 0xf87 as uint32,
    },
    pg_unicode_range {
        first: 0xf8d as uint32,
        last: 0xf97 as uint32,
    },
    pg_unicode_range {
        first: 0xf99 as uint32,
        last: 0xfbc as uint32,
    },
    pg_unicode_range {
        first: 0xfc6 as uint32,
        last: 0xfc6 as uint32,
    },
    pg_unicode_range {
        first: 0x102d as uint32,
        last: 0x1030 as uint32,
    },
    pg_unicode_range {
        first: 0x1032 as uint32,
        last: 0x1037 as uint32,
    },
    pg_unicode_range {
        first: 0x1039 as uint32,
        last: 0x103a as uint32,
    },
    pg_unicode_range {
        first: 0x103d as uint32,
        last: 0x103e as uint32,
    },
    pg_unicode_range {
        first: 0x1058 as uint32,
        last: 0x1059 as uint32,
    },
    pg_unicode_range {
        first: 0x105e as uint32,
        last: 0x1060 as uint32,
    },
    pg_unicode_range {
        first: 0x1071 as uint32,
        last: 0x1074 as uint32,
    },
    pg_unicode_range {
        first: 0x1082 as uint32,
        last: 0x1082 as uint32,
    },
    pg_unicode_range {
        first: 0x1085 as uint32,
        last: 0x1086 as uint32,
    },
    pg_unicode_range {
        first: 0x108d as uint32,
        last: 0x108d as uint32,
    },
    pg_unicode_range {
        first: 0x109d as uint32,
        last: 0x109d as uint32,
    },
    pg_unicode_range {
        first: 0x10fc as uint32,
        last: 0x10fc as uint32,
    },
    pg_unicode_range {
        first: 0x135d as uint32,
        last: 0x135f as uint32,
    },
    pg_unicode_range {
        first: 0x1712 as uint32,
        last: 0x1714 as uint32,
    },
    pg_unicode_range {
        first: 0x1732 as uint32,
        last: 0x1733 as uint32,
    },
    pg_unicode_range {
        first: 0x1752 as uint32,
        last: 0x1753 as uint32,
    },
    pg_unicode_range {
        first: 0x1772 as uint32,
        last: 0x1773 as uint32,
    },
    pg_unicode_range {
        first: 0x17b4 as uint32,
        last: 0x17b5 as uint32,
    },
    pg_unicode_range {
        first: 0x17b7 as uint32,
        last: 0x17bd as uint32,
    },
    pg_unicode_range {
        first: 0x17c6 as uint32,
        last: 0x17c6 as uint32,
    },
    pg_unicode_range {
        first: 0x17c9 as uint32,
        last: 0x17d3 as uint32,
    },
    pg_unicode_range {
        first: 0x17d7 as uint32,
        last: 0x17d7 as uint32,
    },
    pg_unicode_range {
        first: 0x17dd as uint32,
        last: 0x17dd as uint32,
    },
    pg_unicode_range {
        first: 0x180b as uint32,
        last: 0x180d as uint32,
    },
    pg_unicode_range {
        first: 0x180e as uint32,
        last: 0x180e as uint32,
    },
    pg_unicode_range {
        first: 0x180f as uint32,
        last: 0x180f as uint32,
    },
    pg_unicode_range {
        first: 0x1843 as uint32,
        last: 0x1843 as uint32,
    },
    pg_unicode_range {
        first: 0x1885 as uint32,
        last: 0x1886 as uint32,
    },
    pg_unicode_range {
        first: 0x18a9 as uint32,
        last: 0x18a9 as uint32,
    },
    pg_unicode_range {
        first: 0x1920 as uint32,
        last: 0x1922 as uint32,
    },
    pg_unicode_range {
        first: 0x1927 as uint32,
        last: 0x1928 as uint32,
    },
    pg_unicode_range {
        first: 0x1932 as uint32,
        last: 0x1932 as uint32,
    },
    pg_unicode_range {
        first: 0x1939 as uint32,
        last: 0x193b as uint32,
    },
    pg_unicode_range {
        first: 0x1a17 as uint32,
        last: 0x1a18 as uint32,
    },
    pg_unicode_range {
        first: 0x1a1b as uint32,
        last: 0x1a1b as uint32,
    },
    pg_unicode_range {
        first: 0x1a56 as uint32,
        last: 0x1a56 as uint32,
    },
    pg_unicode_range {
        first: 0x1a58 as uint32,
        last: 0x1a5e as uint32,
    },
    pg_unicode_range {
        first: 0x1a60 as uint32,
        last: 0x1a60 as uint32,
    },
    pg_unicode_range {
        first: 0x1a62 as uint32,
        last: 0x1a62 as uint32,
    },
    pg_unicode_range {
        first: 0x1a65 as uint32,
        last: 0x1a6c as uint32,
    },
    pg_unicode_range {
        first: 0x1a73 as uint32,
        last: 0x1a7c as uint32,
    },
    pg_unicode_range {
        first: 0x1a7f as uint32,
        last: 0x1a7f as uint32,
    },
    pg_unicode_range {
        first: 0x1aa7 as uint32,
        last: 0x1aa7 as uint32,
    },
    pg_unicode_range {
        first: 0x1ab0 as uint32,
        last: 0x1abd as uint32,
    },
    pg_unicode_range {
        first: 0x1abe as uint32,
        last: 0x1abe as uint32,
    },
    pg_unicode_range {
        first: 0x1abf as uint32,
        last: 0x1ace as uint32,
    },
    pg_unicode_range {
        first: 0x1b00 as uint32,
        last: 0x1b03 as uint32,
    },
    pg_unicode_range {
        first: 0x1b34 as uint32,
        last: 0x1b34 as uint32,
    },
    pg_unicode_range {
        first: 0x1b36 as uint32,
        last: 0x1b3a as uint32,
    },
    pg_unicode_range {
        first: 0x1b3c as uint32,
        last: 0x1b3c as uint32,
    },
    pg_unicode_range {
        first: 0x1b42 as uint32,
        last: 0x1b42 as uint32,
    },
    pg_unicode_range {
        first: 0x1b6b as uint32,
        last: 0x1b73 as uint32,
    },
    pg_unicode_range {
        first: 0x1b80 as uint32,
        last: 0x1b81 as uint32,
    },
    pg_unicode_range {
        first: 0x1ba2 as uint32,
        last: 0x1ba5 as uint32,
    },
    pg_unicode_range {
        first: 0x1ba8 as uint32,
        last: 0x1ba9 as uint32,
    },
    pg_unicode_range {
        first: 0x1bab as uint32,
        last: 0x1bad as uint32,
    },
    pg_unicode_range {
        first: 0x1be6 as uint32,
        last: 0x1be6 as uint32,
    },
    pg_unicode_range {
        first: 0x1be8 as uint32,
        last: 0x1be9 as uint32,
    },
    pg_unicode_range {
        first: 0x1bed as uint32,
        last: 0x1bed as uint32,
    },
    pg_unicode_range {
        first: 0x1bef as uint32,
        last: 0x1bf1 as uint32,
    },
    pg_unicode_range {
        first: 0x1c2c as uint32,
        last: 0x1c33 as uint32,
    },
    pg_unicode_range {
        first: 0x1c36 as uint32,
        last: 0x1c37 as uint32,
    },
    pg_unicode_range {
        first: 0x1c78 as uint32,
        last: 0x1c7d as uint32,
    },
    pg_unicode_range {
        first: 0x1cd0 as uint32,
        last: 0x1cd2 as uint32,
    },
    pg_unicode_range {
        first: 0x1cd4 as uint32,
        last: 0x1ce0 as uint32,
    },
    pg_unicode_range {
        first: 0x1ce2 as uint32,
        last: 0x1ce8 as uint32,
    },
    pg_unicode_range {
        first: 0x1ced as uint32,
        last: 0x1ced as uint32,
    },
    pg_unicode_range {
        first: 0x1cf4 as uint32,
        last: 0x1cf4 as uint32,
    },
    pg_unicode_range {
        first: 0x1cf8 as uint32,
        last: 0x1cf9 as uint32,
    },
    pg_unicode_range {
        first: 0x1d2c as uint32,
        last: 0x1d6a as uint32,
    },
    pg_unicode_range {
        first: 0x1d78 as uint32,
        last: 0x1d78 as uint32,
    },
    pg_unicode_range {
        first: 0x1d9b as uint32,
        last: 0x1dbf as uint32,
    },
    pg_unicode_range {
        first: 0x1dc0 as uint32,
        last: 0x1dff as uint32,
    },
    pg_unicode_range {
        first: 0x1fbd as uint32,
        last: 0x1fbd as uint32,
    },
    pg_unicode_range {
        first: 0x1fbf as uint32,
        last: 0x1fc1 as uint32,
    },
    pg_unicode_range {
        first: 0x1fcd as uint32,
        last: 0x1fcf as uint32,
    },
    pg_unicode_range {
        first: 0x1fdd as uint32,
        last: 0x1fdf as uint32,
    },
    pg_unicode_range {
        first: 0x1fed as uint32,
        last: 0x1fef as uint32,
    },
    pg_unicode_range {
        first: 0x1ffd as uint32,
        last: 0x1ffe as uint32,
    },
    pg_unicode_range {
        first: 0x200b as uint32,
        last: 0x200f as uint32,
    },
    pg_unicode_range {
        first: 0x2018 as uint32,
        last: 0x2018 as uint32,
    },
    pg_unicode_range {
        first: 0x2019 as uint32,
        last: 0x2019 as uint32,
    },
    pg_unicode_range {
        first: 0x2024 as uint32,
        last: 0x2024 as uint32,
    },
    pg_unicode_range {
        first: 0x2027 as uint32,
        last: 0x2027 as uint32,
    },
    pg_unicode_range {
        first: 0x202a as uint32,
        last: 0x202e as uint32,
    },
    pg_unicode_range {
        first: 0x2060 as uint32,
        last: 0x2064 as uint32,
    },
    pg_unicode_range {
        first: 0x2066 as uint32,
        last: 0x206f as uint32,
    },
    pg_unicode_range {
        first: 0x2071 as uint32,
        last: 0x2071 as uint32,
    },
    pg_unicode_range {
        first: 0x207f as uint32,
        last: 0x207f as uint32,
    },
    pg_unicode_range {
        first: 0x2090 as uint32,
        last: 0x209c as uint32,
    },
    pg_unicode_range {
        first: 0x20d0 as uint32,
        last: 0x20dc as uint32,
    },
    pg_unicode_range {
        first: 0x20dd as uint32,
        last: 0x20e0 as uint32,
    },
    pg_unicode_range {
        first: 0x20e1 as uint32,
        last: 0x20e1 as uint32,
    },
    pg_unicode_range {
        first: 0x20e2 as uint32,
        last: 0x20e4 as uint32,
    },
    pg_unicode_range {
        first: 0x20e5 as uint32,
        last: 0x20f0 as uint32,
    },
    pg_unicode_range {
        first: 0x2c7c as uint32,
        last: 0x2c7d as uint32,
    },
    pg_unicode_range {
        first: 0x2cef as uint32,
        last: 0x2cf1 as uint32,
    },
    pg_unicode_range {
        first: 0x2d6f as uint32,
        last: 0x2d6f as uint32,
    },
    pg_unicode_range {
        first: 0x2d7f as uint32,
        last: 0x2d7f as uint32,
    },
    pg_unicode_range {
        first: 0x2de0 as uint32,
        last: 0x2dff as uint32,
    },
    pg_unicode_range {
        first: 0x2e2f as uint32,
        last: 0x2e2f as uint32,
    },
    pg_unicode_range {
        first: 0x3005 as uint32,
        last: 0x3005 as uint32,
    },
    pg_unicode_range {
        first: 0x302a as uint32,
        last: 0x302d as uint32,
    },
    pg_unicode_range {
        first: 0x3031 as uint32,
        last: 0x3035 as uint32,
    },
    pg_unicode_range {
        first: 0x303b as uint32,
        last: 0x303b as uint32,
    },
    pg_unicode_range {
        first: 0x3099 as uint32,
        last: 0x309a as uint32,
    },
    pg_unicode_range {
        first: 0x309b as uint32,
        last: 0x309c as uint32,
    },
    pg_unicode_range {
        first: 0x309d as uint32,
        last: 0x309e as uint32,
    },
    pg_unicode_range {
        first: 0x30fc as uint32,
        last: 0x30fe as uint32,
    },
    pg_unicode_range {
        first: 0xa015 as uint32,
        last: 0xa015 as uint32,
    },
    pg_unicode_range {
        first: 0xa4f8 as uint32,
        last: 0xa4fd as uint32,
    },
    pg_unicode_range {
        first: 0xa60c as uint32,
        last: 0xa60c as uint32,
    },
    pg_unicode_range {
        first: 0xa66f as uint32,
        last: 0xa66f as uint32,
    },
    pg_unicode_range {
        first: 0xa670 as uint32,
        last: 0xa672 as uint32,
    },
    pg_unicode_range {
        first: 0xa674 as uint32,
        last: 0xa67d as uint32,
    },
    pg_unicode_range {
        first: 0xa67f as uint32,
        last: 0xa67f as uint32,
    },
    pg_unicode_range {
        first: 0xa69c as uint32,
        last: 0xa69d as uint32,
    },
    pg_unicode_range {
        first: 0xa69e as uint32,
        last: 0xa69f as uint32,
    },
    pg_unicode_range {
        first: 0xa6f0 as uint32,
        last: 0xa6f1 as uint32,
    },
    pg_unicode_range {
        first: 0xa700 as uint32,
        last: 0xa716 as uint32,
    },
    pg_unicode_range {
        first: 0xa717 as uint32,
        last: 0xa71f as uint32,
    },
    pg_unicode_range {
        first: 0xa720 as uint32,
        last: 0xa721 as uint32,
    },
    pg_unicode_range {
        first: 0xa770 as uint32,
        last: 0xa770 as uint32,
    },
    pg_unicode_range {
        first: 0xa788 as uint32,
        last: 0xa788 as uint32,
    },
    pg_unicode_range {
        first: 0xa789 as uint32,
        last: 0xa78a as uint32,
    },
    pg_unicode_range {
        first: 0xa7f2 as uint32,
        last: 0xa7f4 as uint32,
    },
    pg_unicode_range {
        first: 0xa7f8 as uint32,
        last: 0xa7f9 as uint32,
    },
    pg_unicode_range {
        first: 0xa802 as uint32,
        last: 0xa802 as uint32,
    },
    pg_unicode_range {
        first: 0xa806 as uint32,
        last: 0xa806 as uint32,
    },
    pg_unicode_range {
        first: 0xa80b as uint32,
        last: 0xa80b as uint32,
    },
    pg_unicode_range {
        first: 0xa825 as uint32,
        last: 0xa826 as uint32,
    },
    pg_unicode_range {
        first: 0xa82c as uint32,
        last: 0xa82c as uint32,
    },
    pg_unicode_range {
        first: 0xa8c4 as uint32,
        last: 0xa8c5 as uint32,
    },
    pg_unicode_range {
        first: 0xa8e0 as uint32,
        last: 0xa8f1 as uint32,
    },
    pg_unicode_range {
        first: 0xa8ff as uint32,
        last: 0xa8ff as uint32,
    },
    pg_unicode_range {
        first: 0xa926 as uint32,
        last: 0xa92d as uint32,
    },
    pg_unicode_range {
        first: 0xa947 as uint32,
        last: 0xa951 as uint32,
    },
    pg_unicode_range {
        first: 0xa980 as uint32,
        last: 0xa982 as uint32,
    },
    pg_unicode_range {
        first: 0xa9b3 as uint32,
        last: 0xa9b3 as uint32,
    },
    pg_unicode_range {
        first: 0xa9b6 as uint32,
        last: 0xa9b9 as uint32,
    },
    pg_unicode_range {
        first: 0xa9bc as uint32,
        last: 0xa9bd as uint32,
    },
    pg_unicode_range {
        first: 0xa9cf as uint32,
        last: 0xa9cf as uint32,
    },
    pg_unicode_range {
        first: 0xa9e5 as uint32,
        last: 0xa9e5 as uint32,
    },
    pg_unicode_range {
        first: 0xa9e6 as uint32,
        last: 0xa9e6 as uint32,
    },
    pg_unicode_range {
        first: 0xaa29 as uint32,
        last: 0xaa2e as uint32,
    },
    pg_unicode_range {
        first: 0xaa31 as uint32,
        last: 0xaa32 as uint32,
    },
    pg_unicode_range {
        first: 0xaa35 as uint32,
        last: 0xaa36 as uint32,
    },
    pg_unicode_range {
        first: 0xaa43 as uint32,
        last: 0xaa43 as uint32,
    },
    pg_unicode_range {
        first: 0xaa4c as uint32,
        last: 0xaa4c as uint32,
    },
    pg_unicode_range {
        first: 0xaa70 as uint32,
        last: 0xaa70 as uint32,
    },
    pg_unicode_range {
        first: 0xaa7c as uint32,
        last: 0xaa7c as uint32,
    },
    pg_unicode_range {
        first: 0xaab0 as uint32,
        last: 0xaab0 as uint32,
    },
    pg_unicode_range {
        first: 0xaab2 as uint32,
        last: 0xaab4 as uint32,
    },
    pg_unicode_range {
        first: 0xaab7 as uint32,
        last: 0xaab8 as uint32,
    },
    pg_unicode_range {
        first: 0xaabe as uint32,
        last: 0xaabf as uint32,
    },
    pg_unicode_range {
        first: 0xaac1 as uint32,
        last: 0xaac1 as uint32,
    },
    pg_unicode_range {
        first: 0xaadd as uint32,
        last: 0xaadd as uint32,
    },
    pg_unicode_range {
        first: 0xaaec as uint32,
        last: 0xaaed as uint32,
    },
    pg_unicode_range {
        first: 0xaaf3 as uint32,
        last: 0xaaf4 as uint32,
    },
    pg_unicode_range {
        first: 0xaaf6 as uint32,
        last: 0xaaf6 as uint32,
    },
    pg_unicode_range {
        first: 0xab5b as uint32,
        last: 0xab5b as uint32,
    },
    pg_unicode_range {
        first: 0xab5c as uint32,
        last: 0xab5f as uint32,
    },
    pg_unicode_range {
        first: 0xab69 as uint32,
        last: 0xab69 as uint32,
    },
    pg_unicode_range {
        first: 0xab6a as uint32,
        last: 0xab6b as uint32,
    },
    pg_unicode_range {
        first: 0xabe5 as uint32,
        last: 0xabe5 as uint32,
    },
    pg_unicode_range {
        first: 0xabe8 as uint32,
        last: 0xabe8 as uint32,
    },
    pg_unicode_range {
        first: 0xabed as uint32,
        last: 0xabed as uint32,
    },
    pg_unicode_range {
        first: 0xfb1e as uint32,
        last: 0xfb1e as uint32,
    },
    pg_unicode_range {
        first: 0xfbb2 as uint32,
        last: 0xfbc2 as uint32,
    },
    pg_unicode_range {
        first: 0xfe00 as uint32,
        last: 0xfe0f as uint32,
    },
    pg_unicode_range {
        first: 0xfe13 as uint32,
        last: 0xfe13 as uint32,
    },
    pg_unicode_range {
        first: 0xfe20 as uint32,
        last: 0xfe2f as uint32,
    },
    pg_unicode_range {
        first: 0xfe52 as uint32,
        last: 0xfe52 as uint32,
    },
    pg_unicode_range {
        first: 0xfe55 as uint32,
        last: 0xfe55 as uint32,
    },
    pg_unicode_range {
        first: 0xfeff as uint32,
        last: 0xfeff as uint32,
    },
    pg_unicode_range {
        first: 0xff07 as uint32,
        last: 0xff07 as uint32,
    },
    pg_unicode_range {
        first: 0xff0e as uint32,
        last: 0xff0e as uint32,
    },
    pg_unicode_range {
        first: 0xff1a as uint32,
        last: 0xff1a as uint32,
    },
    pg_unicode_range {
        first: 0xff3e as uint32,
        last: 0xff3e as uint32,
    },
    pg_unicode_range {
        first: 0xff40 as uint32,
        last: 0xff40 as uint32,
    },
    pg_unicode_range {
        first: 0xff70 as uint32,
        last: 0xff70 as uint32,
    },
    pg_unicode_range {
        first: 0xff9e as uint32,
        last: 0xff9f as uint32,
    },
    pg_unicode_range {
        first: 0xffe3 as uint32,
        last: 0xffe3 as uint32,
    },
    pg_unicode_range {
        first: 0xfff9 as uint32,
        last: 0xfffb as uint32,
    },
    pg_unicode_range {
        first: 0x101fd as uint32,
        last: 0x101fd as uint32,
    },
    pg_unicode_range {
        first: 0x102e0 as uint32,
        last: 0x102e0 as uint32,
    },
    pg_unicode_range {
        first: 0x10376 as uint32,
        last: 0x1037a as uint32,
    },
    pg_unicode_range {
        first: 0x10780 as uint32,
        last: 0x10785 as uint32,
    },
    pg_unicode_range {
        first: 0x10787 as uint32,
        last: 0x107b0 as uint32,
    },
    pg_unicode_range {
        first: 0x107b2 as uint32,
        last: 0x107ba as uint32,
    },
    pg_unicode_range {
        first: 0x10a01 as uint32,
        last: 0x10a03 as uint32,
    },
    pg_unicode_range {
        first: 0x10a05 as uint32,
        last: 0x10a06 as uint32,
    },
    pg_unicode_range {
        first: 0x10a0c as uint32,
        last: 0x10a0f as uint32,
    },
    pg_unicode_range {
        first: 0x10a38 as uint32,
        last: 0x10a3a as uint32,
    },
    pg_unicode_range {
        first: 0x10a3f as uint32,
        last: 0x10a3f as uint32,
    },
    pg_unicode_range {
        first: 0x10ae5 as uint32,
        last: 0x10ae6 as uint32,
    },
    pg_unicode_range {
        first: 0x10d24 as uint32,
        last: 0x10d27 as uint32,
    },
    pg_unicode_range {
        first: 0x10d4e as uint32,
        last: 0x10d4e as uint32,
    },
    pg_unicode_range {
        first: 0x10d69 as uint32,
        last: 0x10d6d as uint32,
    },
    pg_unicode_range {
        first: 0x10d6f as uint32,
        last: 0x10d6f as uint32,
    },
    pg_unicode_range {
        first: 0x10eab as uint32,
        last: 0x10eac as uint32,
    },
    pg_unicode_range {
        first: 0x10efc as uint32,
        last: 0x10eff as uint32,
    },
    pg_unicode_range {
        first: 0x10f46 as uint32,
        last: 0x10f50 as uint32,
    },
    pg_unicode_range {
        first: 0x10f82 as uint32,
        last: 0x10f85 as uint32,
    },
    pg_unicode_range {
        first: 0x11001 as uint32,
        last: 0x11001 as uint32,
    },
    pg_unicode_range {
        first: 0x11038 as uint32,
        last: 0x11046 as uint32,
    },
    pg_unicode_range {
        first: 0x11070 as uint32,
        last: 0x11070 as uint32,
    },
    pg_unicode_range {
        first: 0x11073 as uint32,
        last: 0x11074 as uint32,
    },
    pg_unicode_range {
        first: 0x1107f as uint32,
        last: 0x11081 as uint32,
    },
    pg_unicode_range {
        first: 0x110b3 as uint32,
        last: 0x110b6 as uint32,
    },
    pg_unicode_range {
        first: 0x110b9 as uint32,
        last: 0x110ba as uint32,
    },
    pg_unicode_range {
        first: 0x110bd as uint32,
        last: 0x110bd as uint32,
    },
    pg_unicode_range {
        first: 0x110c2 as uint32,
        last: 0x110c2 as uint32,
    },
    pg_unicode_range {
        first: 0x110cd as uint32,
        last: 0x110cd as uint32,
    },
    pg_unicode_range {
        first: 0x11100 as uint32,
        last: 0x11102 as uint32,
    },
    pg_unicode_range {
        first: 0x11127 as uint32,
        last: 0x1112b as uint32,
    },
    pg_unicode_range {
        first: 0x1112d as uint32,
        last: 0x11134 as uint32,
    },
    pg_unicode_range {
        first: 0x11173 as uint32,
        last: 0x11173 as uint32,
    },
    pg_unicode_range {
        first: 0x11180 as uint32,
        last: 0x11181 as uint32,
    },
    pg_unicode_range {
        first: 0x111b6 as uint32,
        last: 0x111be as uint32,
    },
    pg_unicode_range {
        first: 0x111c9 as uint32,
        last: 0x111cc as uint32,
    },
    pg_unicode_range {
        first: 0x111cf as uint32,
        last: 0x111cf as uint32,
    },
    pg_unicode_range {
        first: 0x1122f as uint32,
        last: 0x11231 as uint32,
    },
    pg_unicode_range {
        first: 0x11234 as uint32,
        last: 0x11234 as uint32,
    },
    pg_unicode_range {
        first: 0x11236 as uint32,
        last: 0x11237 as uint32,
    },
    pg_unicode_range {
        first: 0x1123e as uint32,
        last: 0x1123e as uint32,
    },
    pg_unicode_range {
        first: 0x11241 as uint32,
        last: 0x11241 as uint32,
    },
    pg_unicode_range {
        first: 0x112df as uint32,
        last: 0x112df as uint32,
    },
    pg_unicode_range {
        first: 0x112e3 as uint32,
        last: 0x112ea as uint32,
    },
    pg_unicode_range {
        first: 0x11300 as uint32,
        last: 0x11301 as uint32,
    },
    pg_unicode_range {
        first: 0x1133b as uint32,
        last: 0x1133c as uint32,
    },
    pg_unicode_range {
        first: 0x11340 as uint32,
        last: 0x11340 as uint32,
    },
    pg_unicode_range {
        first: 0x11366 as uint32,
        last: 0x1136c as uint32,
    },
    pg_unicode_range {
        first: 0x11370 as uint32,
        last: 0x11374 as uint32,
    },
    pg_unicode_range {
        first: 0x113bb as uint32,
        last: 0x113c0 as uint32,
    },
    pg_unicode_range {
        first: 0x113ce as uint32,
        last: 0x113ce as uint32,
    },
    pg_unicode_range {
        first: 0x113d0 as uint32,
        last: 0x113d0 as uint32,
    },
    pg_unicode_range {
        first: 0x113d2 as uint32,
        last: 0x113d2 as uint32,
    },
    pg_unicode_range {
        first: 0x113e1 as uint32,
        last: 0x113e2 as uint32,
    },
    pg_unicode_range {
        first: 0x11438 as uint32,
        last: 0x1143f as uint32,
    },
    pg_unicode_range {
        first: 0x11442 as uint32,
        last: 0x11444 as uint32,
    },
    pg_unicode_range {
        first: 0x11446 as uint32,
        last: 0x11446 as uint32,
    },
    pg_unicode_range {
        first: 0x1145e as uint32,
        last: 0x1145e as uint32,
    },
    pg_unicode_range {
        first: 0x114b3 as uint32,
        last: 0x114b8 as uint32,
    },
    pg_unicode_range {
        first: 0x114ba as uint32,
        last: 0x114ba as uint32,
    },
    pg_unicode_range {
        first: 0x114bf as uint32,
        last: 0x114c0 as uint32,
    },
    pg_unicode_range {
        first: 0x114c2 as uint32,
        last: 0x114c3 as uint32,
    },
    pg_unicode_range {
        first: 0x115b2 as uint32,
        last: 0x115b5 as uint32,
    },
    pg_unicode_range {
        first: 0x115bc as uint32,
        last: 0x115bd as uint32,
    },
    pg_unicode_range {
        first: 0x115bf as uint32,
        last: 0x115c0 as uint32,
    },
    pg_unicode_range {
        first: 0x115dc as uint32,
        last: 0x115dd as uint32,
    },
    pg_unicode_range {
        first: 0x11633 as uint32,
        last: 0x1163a as uint32,
    },
    pg_unicode_range {
        first: 0x1163d as uint32,
        last: 0x1163d as uint32,
    },
    pg_unicode_range {
        first: 0x1163f as uint32,
        last: 0x11640 as uint32,
    },
    pg_unicode_range {
        first: 0x116ab as uint32,
        last: 0x116ab as uint32,
    },
    pg_unicode_range {
        first: 0x116ad as uint32,
        last: 0x116ad as uint32,
    },
    pg_unicode_range {
        first: 0x116b0 as uint32,
        last: 0x116b5 as uint32,
    },
    pg_unicode_range {
        first: 0x116b7 as uint32,
        last: 0x116b7 as uint32,
    },
    pg_unicode_range {
        first: 0x1171d as uint32,
        last: 0x1171d as uint32,
    },
    pg_unicode_range {
        first: 0x1171f as uint32,
        last: 0x1171f as uint32,
    },
    pg_unicode_range {
        first: 0x11722 as uint32,
        last: 0x11725 as uint32,
    },
    pg_unicode_range {
        first: 0x11727 as uint32,
        last: 0x1172b as uint32,
    },
    pg_unicode_range {
        first: 0x1182f as uint32,
        last: 0x11837 as uint32,
    },
    pg_unicode_range {
        first: 0x11839 as uint32,
        last: 0x1183a as uint32,
    },
    pg_unicode_range {
        first: 0x1193b as uint32,
        last: 0x1193c as uint32,
    },
    pg_unicode_range {
        first: 0x1193e as uint32,
        last: 0x1193e as uint32,
    },
    pg_unicode_range {
        first: 0x11943 as uint32,
        last: 0x11943 as uint32,
    },
    pg_unicode_range {
        first: 0x119d4 as uint32,
        last: 0x119d7 as uint32,
    },
    pg_unicode_range {
        first: 0x119da as uint32,
        last: 0x119db as uint32,
    },
    pg_unicode_range {
        first: 0x119e0 as uint32,
        last: 0x119e0 as uint32,
    },
    pg_unicode_range {
        first: 0x11a01 as uint32,
        last: 0x11a0a as uint32,
    },
    pg_unicode_range {
        first: 0x11a33 as uint32,
        last: 0x11a38 as uint32,
    },
    pg_unicode_range {
        first: 0x11a3b as uint32,
        last: 0x11a3e as uint32,
    },
    pg_unicode_range {
        first: 0x11a47 as uint32,
        last: 0x11a47 as uint32,
    },
    pg_unicode_range {
        first: 0x11a51 as uint32,
        last: 0x11a56 as uint32,
    },
    pg_unicode_range {
        first: 0x11a59 as uint32,
        last: 0x11a5b as uint32,
    },
    pg_unicode_range {
        first: 0x11a8a as uint32,
        last: 0x11a96 as uint32,
    },
    pg_unicode_range {
        first: 0x11a98 as uint32,
        last: 0x11a99 as uint32,
    },
    pg_unicode_range {
        first: 0x11c30 as uint32,
        last: 0x11c36 as uint32,
    },
    pg_unicode_range {
        first: 0x11c38 as uint32,
        last: 0x11c3d as uint32,
    },
    pg_unicode_range {
        first: 0x11c3f as uint32,
        last: 0x11c3f as uint32,
    },
    pg_unicode_range {
        first: 0x11c92 as uint32,
        last: 0x11ca7 as uint32,
    },
    pg_unicode_range {
        first: 0x11caa as uint32,
        last: 0x11cb0 as uint32,
    },
    pg_unicode_range {
        first: 0x11cb2 as uint32,
        last: 0x11cb3 as uint32,
    },
    pg_unicode_range {
        first: 0x11cb5 as uint32,
        last: 0x11cb6 as uint32,
    },
    pg_unicode_range {
        first: 0x11d31 as uint32,
        last: 0x11d36 as uint32,
    },
    pg_unicode_range {
        first: 0x11d3a as uint32,
        last: 0x11d3a as uint32,
    },
    pg_unicode_range {
        first: 0x11d3c as uint32,
        last: 0x11d3d as uint32,
    },
    pg_unicode_range {
        first: 0x11d3f as uint32,
        last: 0x11d45 as uint32,
    },
    pg_unicode_range {
        first: 0x11d47 as uint32,
        last: 0x11d47 as uint32,
    },
    pg_unicode_range {
        first: 0x11d90 as uint32,
        last: 0x11d91 as uint32,
    },
    pg_unicode_range {
        first: 0x11d95 as uint32,
        last: 0x11d95 as uint32,
    },
    pg_unicode_range {
        first: 0x11d97 as uint32,
        last: 0x11d97 as uint32,
    },
    pg_unicode_range {
        first: 0x11ef3 as uint32,
        last: 0x11ef4 as uint32,
    },
    pg_unicode_range {
        first: 0x11f00 as uint32,
        last: 0x11f01 as uint32,
    },
    pg_unicode_range {
        first: 0x11f36 as uint32,
        last: 0x11f3a as uint32,
    },
    pg_unicode_range {
        first: 0x11f40 as uint32,
        last: 0x11f40 as uint32,
    },
    pg_unicode_range {
        first: 0x11f42 as uint32,
        last: 0x11f42 as uint32,
    },
    pg_unicode_range {
        first: 0x11f5a as uint32,
        last: 0x11f5a as uint32,
    },
    pg_unicode_range {
        first: 0x13430 as uint32,
        last: 0x1343f as uint32,
    },
    pg_unicode_range {
        first: 0x13440 as uint32,
        last: 0x13440 as uint32,
    },
    pg_unicode_range {
        first: 0x13447 as uint32,
        last: 0x13455 as uint32,
    },
    pg_unicode_range {
        first: 0x1611e as uint32,
        last: 0x16129 as uint32,
    },
    pg_unicode_range {
        first: 0x1612d as uint32,
        last: 0x1612f as uint32,
    },
    pg_unicode_range {
        first: 0x16af0 as uint32,
        last: 0x16af4 as uint32,
    },
    pg_unicode_range {
        first: 0x16b30 as uint32,
        last: 0x16b36 as uint32,
    },
    pg_unicode_range {
        first: 0x16b40 as uint32,
        last: 0x16b43 as uint32,
    },
    pg_unicode_range {
        first: 0x16d40 as uint32,
        last: 0x16d42 as uint32,
    },
    pg_unicode_range {
        first: 0x16d6b as uint32,
        last: 0x16d6c as uint32,
    },
    pg_unicode_range {
        first: 0x16f4f as uint32,
        last: 0x16f4f as uint32,
    },
    pg_unicode_range {
        first: 0x16f8f as uint32,
        last: 0x16f92 as uint32,
    },
    pg_unicode_range {
        first: 0x16f93 as uint32,
        last: 0x16f9f as uint32,
    },
    pg_unicode_range {
        first: 0x16fe0 as uint32,
        last: 0x16fe1 as uint32,
    },
    pg_unicode_range {
        first: 0x16fe3 as uint32,
        last: 0x16fe3 as uint32,
    },
    pg_unicode_range {
        first: 0x16fe4 as uint32,
        last: 0x16fe4 as uint32,
    },
    pg_unicode_range {
        first: 0x1aff0 as uint32,
        last: 0x1aff3 as uint32,
    },
    pg_unicode_range {
        first: 0x1aff5 as uint32,
        last: 0x1affb as uint32,
    },
    pg_unicode_range {
        first: 0x1affd as uint32,
        last: 0x1affe as uint32,
    },
    pg_unicode_range {
        first: 0x1bc9d as uint32,
        last: 0x1bc9e as uint32,
    },
    pg_unicode_range {
        first: 0x1bca0 as uint32,
        last: 0x1bca3 as uint32,
    },
    pg_unicode_range {
        first: 0x1cf00 as uint32,
        last: 0x1cf2d as uint32,
    },
    pg_unicode_range {
        first: 0x1cf30 as uint32,
        last: 0x1cf46 as uint32,
    },
    pg_unicode_range {
        first: 0x1d167 as uint32,
        last: 0x1d169 as uint32,
    },
    pg_unicode_range {
        first: 0x1d173 as uint32,
        last: 0x1d17a as uint32,
    },
    pg_unicode_range {
        first: 0x1d17b as uint32,
        last: 0x1d182 as uint32,
    },
    pg_unicode_range {
        first: 0x1d185 as uint32,
        last: 0x1d18b as uint32,
    },
    pg_unicode_range {
        first: 0x1d1aa as uint32,
        last: 0x1d1ad as uint32,
    },
    pg_unicode_range {
        first: 0x1d242 as uint32,
        last: 0x1d244 as uint32,
    },
    pg_unicode_range {
        first: 0x1da00 as uint32,
        last: 0x1da36 as uint32,
    },
    pg_unicode_range {
        first: 0x1da3b as uint32,
        last: 0x1da6c as uint32,
    },
    pg_unicode_range {
        first: 0x1da75 as uint32,
        last: 0x1da75 as uint32,
    },
    pg_unicode_range {
        first: 0x1da84 as uint32,
        last: 0x1da84 as uint32,
    },
    pg_unicode_range {
        first: 0x1da9b as uint32,
        last: 0x1da9f as uint32,
    },
    pg_unicode_range {
        first: 0x1daa1 as uint32,
        last: 0x1daaf as uint32,
    },
    pg_unicode_range {
        first: 0x1e000 as uint32,
        last: 0x1e006 as uint32,
    },
    pg_unicode_range {
        first: 0x1e008 as uint32,
        last: 0x1e018 as uint32,
    },
    pg_unicode_range {
        first: 0x1e01b as uint32,
        last: 0x1e021 as uint32,
    },
    pg_unicode_range {
        first: 0x1e023 as uint32,
        last: 0x1e024 as uint32,
    },
    pg_unicode_range {
        first: 0x1e026 as uint32,
        last: 0x1e02a as uint32,
    },
    pg_unicode_range {
        first: 0x1e030 as uint32,
        last: 0x1e06d as uint32,
    },
    pg_unicode_range {
        first: 0x1e08f as uint32,
        last: 0x1e08f as uint32,
    },
    pg_unicode_range {
        first: 0x1e130 as uint32,
        last: 0x1e136 as uint32,
    },
    pg_unicode_range {
        first: 0x1e137 as uint32,
        last: 0x1e13d as uint32,
    },
    pg_unicode_range {
        first: 0x1e2ae as uint32,
        last: 0x1e2ae as uint32,
    },
    pg_unicode_range {
        first: 0x1e2ec as uint32,
        last: 0x1e2ef as uint32,
    },
    pg_unicode_range {
        first: 0x1e4eb as uint32,
        last: 0x1e4eb as uint32,
    },
    pg_unicode_range {
        first: 0x1e4ec as uint32,
        last: 0x1e4ef as uint32,
    },
    pg_unicode_range {
        first: 0x1e5ee as uint32,
        last: 0x1e5ef as uint32,
    },
    pg_unicode_range {
        first: 0x1e8d0 as uint32,
        last: 0x1e8d6 as uint32,
    },
    pg_unicode_range {
        first: 0x1e944 as uint32,
        last: 0x1e94a as uint32,
    },
    pg_unicode_range {
        first: 0x1e94b as uint32,
        last: 0x1e94b as uint32,
    },
    pg_unicode_range {
        first: 0x1f3fb as uint32,
        last: 0x1f3ff as uint32,
    },
    pg_unicode_range {
        first: 0xe0001 as uint32,
        last: 0xe0001 as uint32,
    },
    pg_unicode_range {
        first: 0xe0020 as uint32,
        last: 0xe007f as uint32,
    },
    pg_unicode_range {
        first: 0xe0100 as uint32,
        last: 0xe01ef as uint32,
    },
];
pub(crate) static unicode_white_space: [pg_unicode_range; 11] = [
    pg_unicode_range {
        first: 0x9 as uint32,
        last: 0xd as uint32,
    },
    pg_unicode_range {
        first: 0x20 as uint32,
        last: 0x20 as uint32,
    },
    pg_unicode_range {
        first: 0x85 as uint32,
        last: 0x85 as uint32,
    },
    pg_unicode_range {
        first: 0xa0 as uint32,
        last: 0xa0 as uint32,
    },
    pg_unicode_range {
        first: 0x1680 as uint32,
        last: 0x1680 as uint32,
    },
    pg_unicode_range {
        first: 0x2000 as uint32,
        last: 0x200a as uint32,
    },
    pg_unicode_range {
        first: 0x2028 as uint32,
        last: 0x2028 as uint32,
    },
    pg_unicode_range {
        first: 0x2029 as uint32,
        last: 0x2029 as uint32,
    },
    pg_unicode_range {
        first: 0x202f as uint32,
        last: 0x202f as uint32,
    },
    pg_unicode_range {
        first: 0x205f as uint32,
        last: 0x205f as uint32,
    },
    pg_unicode_range {
        first: 0x3000 as uint32,
        last: 0x3000 as uint32,
    },
];
pub(crate) static unicode_hex_digit: [pg_unicode_range; 6] = [
    pg_unicode_range {
        first: 0x30 as uint32,
        last: 0x39 as uint32,
    },
    pg_unicode_range {
        first: 0x41 as uint32,
        last: 0x46 as uint32,
    },
    pg_unicode_range {
        first: 0x61 as uint32,
        last: 0x66 as uint32,
    },
    pg_unicode_range {
        first: 0xff10 as uint32,
        last: 0xff19 as uint32,
    },
    pg_unicode_range {
        first: 0xff21 as uint32,
        last: 0xff26 as uint32,
    },
    pg_unicode_range {
        first: 0xff41 as uint32,
        last: 0xff46 as uint32,
    },
];
pub(crate) static unicode_join_control: [pg_unicode_range; 1] = [pg_unicode_range {
    first: 0x200c as uint32,
    last: 0x200d as uint32,
}];
