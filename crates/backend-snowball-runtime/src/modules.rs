use crate::types::SN_env;

/// One supported stemmer module (mirrors `dict_snowball.c`'s
/// `stemmer_module`: name, PG encoding, and the create/close/stem fns).
#[derive(Copy, Clone)]
pub struct StemmerModule {
    pub name: &'static str,
    pub enc: i32,
    pub create: unsafe fn() -> *mut SN_env,
    pub close: unsafe fn(*mut SN_env),
    pub stem: unsafe fn(*mut SN_env) -> core::ffi::c_int,
}

// PostgreSQL `pg_enc` codes used by the stemmer table (mb/pg_wchar.h).
pub const PG_SQL_ASCII: i32 = 0;
pub const PG_UTF8: i32 = 6;
pub const PG_LATIN1: i32 = 8;
pub const PG_LATIN2: i32 = 9;
pub const PG_KOI8R: i32 = 22;

/// The supported Snowball stemmer modules, in `dict_snowball.c` order.
pub static STEMMER_MODULES: [StemmerModule; 49] = [
    StemmerModule { name: "basque", enc: PG_LATIN1, create: crate::stemmers::stem_ISO_8859_1_basque::basque_ISO_8859_1_create_env, close: crate::stemmers::stem_ISO_8859_1_basque::basque_ISO_8859_1_close_env, stem: crate::stemmers::stem_ISO_8859_1_basque::basque_ISO_8859_1_stem },
    StemmerModule { name: "catalan", enc: PG_LATIN1, create: crate::stemmers::stem_ISO_8859_1_catalan::catalan_ISO_8859_1_create_env, close: crate::stemmers::stem_ISO_8859_1_catalan::catalan_ISO_8859_1_close_env, stem: crate::stemmers::stem_ISO_8859_1_catalan::catalan_ISO_8859_1_stem },
    StemmerModule { name: "danish", enc: PG_LATIN1, create: crate::stemmers::stem_ISO_8859_1_danish::danish_ISO_8859_1_create_env, close: crate::stemmers::stem_ISO_8859_1_danish::danish_ISO_8859_1_close_env, stem: crate::stemmers::stem_ISO_8859_1_danish::danish_ISO_8859_1_stem },
    StemmerModule { name: "dutch", enc: PG_LATIN1, create: crate::stemmers::stem_ISO_8859_1_dutch::dutch_ISO_8859_1_create_env, close: crate::stemmers::stem_ISO_8859_1_dutch::dutch_ISO_8859_1_close_env, stem: crate::stemmers::stem_ISO_8859_1_dutch::dutch_ISO_8859_1_stem },
    StemmerModule { name: "english", enc: PG_LATIN1, create: crate::stemmers::stem_ISO_8859_1_english::english_ISO_8859_1_create_env, close: crate::stemmers::stem_ISO_8859_1_english::english_ISO_8859_1_close_env, stem: crate::stemmers::stem_ISO_8859_1_english::english_ISO_8859_1_stem },
    StemmerModule { name: "finnish", enc: PG_LATIN1, create: crate::stemmers::stem_ISO_8859_1_finnish::finnish_ISO_8859_1_create_env, close: crate::stemmers::stem_ISO_8859_1_finnish::finnish_ISO_8859_1_close_env, stem: crate::stemmers::stem_ISO_8859_1_finnish::finnish_ISO_8859_1_stem },
    StemmerModule { name: "french", enc: PG_LATIN1, create: crate::stemmers::stem_ISO_8859_1_french::french_ISO_8859_1_create_env, close: crate::stemmers::stem_ISO_8859_1_french::french_ISO_8859_1_close_env, stem: crate::stemmers::stem_ISO_8859_1_french::french_ISO_8859_1_stem },
    StemmerModule { name: "german", enc: PG_LATIN1, create: crate::stemmers::stem_ISO_8859_1_german::german_ISO_8859_1_create_env, close: crate::stemmers::stem_ISO_8859_1_german::german_ISO_8859_1_close_env, stem: crate::stemmers::stem_ISO_8859_1_german::german_ISO_8859_1_stem },
    StemmerModule { name: "indonesian", enc: PG_LATIN1, create: crate::stemmers::stem_ISO_8859_1_indonesian::indonesian_ISO_8859_1_create_env, close: crate::stemmers::stem_ISO_8859_1_indonesian::indonesian_ISO_8859_1_close_env, stem: crate::stemmers::stem_ISO_8859_1_indonesian::indonesian_ISO_8859_1_stem },
    StemmerModule { name: "irish", enc: PG_LATIN1, create: crate::stemmers::stem_ISO_8859_1_irish::irish_ISO_8859_1_create_env, close: crate::stemmers::stem_ISO_8859_1_irish::irish_ISO_8859_1_close_env, stem: crate::stemmers::stem_ISO_8859_1_irish::irish_ISO_8859_1_stem },
    StemmerModule { name: "italian", enc: PG_LATIN1, create: crate::stemmers::stem_ISO_8859_1_italian::italian_ISO_8859_1_create_env, close: crate::stemmers::stem_ISO_8859_1_italian::italian_ISO_8859_1_close_env, stem: crate::stemmers::stem_ISO_8859_1_italian::italian_ISO_8859_1_stem },
    StemmerModule { name: "norwegian", enc: PG_LATIN1, create: crate::stemmers::stem_ISO_8859_1_norwegian::norwegian_ISO_8859_1_create_env, close: crate::stemmers::stem_ISO_8859_1_norwegian::norwegian_ISO_8859_1_close_env, stem: crate::stemmers::stem_ISO_8859_1_norwegian::norwegian_ISO_8859_1_stem },
    StemmerModule { name: "porter", enc: PG_LATIN1, create: crate::stemmers::stem_ISO_8859_1_porter::porter_ISO_8859_1_create_env, close: crate::stemmers::stem_ISO_8859_1_porter::porter_ISO_8859_1_close_env, stem: crate::stemmers::stem_ISO_8859_1_porter::porter_ISO_8859_1_stem },
    StemmerModule { name: "portuguese", enc: PG_LATIN1, create: crate::stemmers::stem_ISO_8859_1_portuguese::portuguese_ISO_8859_1_create_env, close: crate::stemmers::stem_ISO_8859_1_portuguese::portuguese_ISO_8859_1_close_env, stem: crate::stemmers::stem_ISO_8859_1_portuguese::portuguese_ISO_8859_1_stem },
    StemmerModule { name: "spanish", enc: PG_LATIN1, create: crate::stemmers::stem_ISO_8859_1_spanish::spanish_ISO_8859_1_create_env, close: crate::stemmers::stem_ISO_8859_1_spanish::spanish_ISO_8859_1_close_env, stem: crate::stemmers::stem_ISO_8859_1_spanish::spanish_ISO_8859_1_stem },
    StemmerModule { name: "swedish", enc: PG_LATIN1, create: crate::stemmers::stem_ISO_8859_1_swedish::swedish_ISO_8859_1_create_env, close: crate::stemmers::stem_ISO_8859_1_swedish::swedish_ISO_8859_1_close_env, stem: crate::stemmers::stem_ISO_8859_1_swedish::swedish_ISO_8859_1_stem },
    StemmerModule { name: "hungarian", enc: PG_LATIN2, create: crate::stemmers::stem_ISO_8859_2_hungarian::hungarian_ISO_8859_2_create_env, close: crate::stemmers::stem_ISO_8859_2_hungarian::hungarian_ISO_8859_2_close_env, stem: crate::stemmers::stem_ISO_8859_2_hungarian::hungarian_ISO_8859_2_stem },
    StemmerModule { name: "russian", enc: PG_KOI8R, create: crate::stemmers::stem_KOI8_R_russian::russian_KOI8_R_create_env, close: crate::stemmers::stem_KOI8_R_russian::russian_KOI8_R_close_env, stem: crate::stemmers::stem_KOI8_R_russian::russian_KOI8_R_stem },
    StemmerModule { name: "arabic", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_arabic::arabic_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_arabic::arabic_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_arabic::arabic_UTF_8_stem },
    StemmerModule { name: "armenian", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_armenian::armenian_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_armenian::armenian_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_armenian::armenian_UTF_8_stem },
    StemmerModule { name: "basque", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_basque::basque_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_basque::basque_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_basque::basque_UTF_8_stem },
    StemmerModule { name: "catalan", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_catalan::catalan_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_catalan::catalan_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_catalan::catalan_UTF_8_stem },
    StemmerModule { name: "danish", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_danish::danish_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_danish::danish_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_danish::danish_UTF_8_stem },
    StemmerModule { name: "dutch", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_dutch::dutch_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_dutch::dutch_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_dutch::dutch_UTF_8_stem },
    StemmerModule { name: "english", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_english::english_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_english::english_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_english::english_UTF_8_stem },
    StemmerModule { name: "estonian", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_estonian::estonian_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_estonian::estonian_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_estonian::estonian_UTF_8_stem },
    StemmerModule { name: "finnish", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_finnish::finnish_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_finnish::finnish_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_finnish::finnish_UTF_8_stem },
    StemmerModule { name: "french", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_french::french_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_french::french_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_french::french_UTF_8_stem },
    StemmerModule { name: "german", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_german::german_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_german::german_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_german::german_UTF_8_stem },
    StemmerModule { name: "greek", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_greek::greek_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_greek::greek_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_greek::greek_UTF_8_stem },
    StemmerModule { name: "hindi", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_hindi::hindi_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_hindi::hindi_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_hindi::hindi_UTF_8_stem },
    StemmerModule { name: "hungarian", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_hungarian::hungarian_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_hungarian::hungarian_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_hungarian::hungarian_UTF_8_stem },
    StemmerModule { name: "indonesian", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_indonesian::indonesian_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_indonesian::indonesian_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_indonesian::indonesian_UTF_8_stem },
    StemmerModule { name: "irish", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_irish::irish_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_irish::irish_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_irish::irish_UTF_8_stem },
    StemmerModule { name: "italian", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_italian::italian_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_italian::italian_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_italian::italian_UTF_8_stem },
    StemmerModule { name: "lithuanian", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_lithuanian::lithuanian_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_lithuanian::lithuanian_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_lithuanian::lithuanian_UTF_8_stem },
    StemmerModule { name: "nepali", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_nepali::nepali_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_nepali::nepali_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_nepali::nepali_UTF_8_stem },
    StemmerModule { name: "norwegian", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_norwegian::norwegian_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_norwegian::norwegian_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_norwegian::norwegian_UTF_8_stem },
    StemmerModule { name: "porter", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_porter::porter_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_porter::porter_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_porter::porter_UTF_8_stem },
    StemmerModule { name: "portuguese", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_portuguese::portuguese_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_portuguese::portuguese_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_portuguese::portuguese_UTF_8_stem },
    StemmerModule { name: "romanian", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_romanian::romanian_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_romanian::romanian_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_romanian::romanian_UTF_8_stem },
    StemmerModule { name: "russian", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_russian::russian_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_russian::russian_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_russian::russian_UTF_8_stem },
    StemmerModule { name: "serbian", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_serbian::serbian_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_serbian::serbian_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_serbian::serbian_UTF_8_stem },
    StemmerModule { name: "spanish", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_spanish::spanish_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_spanish::spanish_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_spanish::spanish_UTF_8_stem },
    StemmerModule { name: "swedish", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_swedish::swedish_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_swedish::swedish_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_swedish::swedish_UTF_8_stem },
    StemmerModule { name: "tamil", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_tamil::tamil_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_tamil::tamil_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_tamil::tamil_UTF_8_stem },
    StemmerModule { name: "turkish", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_turkish::turkish_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_turkish::turkish_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_turkish::turkish_UTF_8_stem },
    StemmerModule { name: "yiddish", enc: PG_UTF8, create: crate::stemmers::stem_UTF_8_yiddish::yiddish_UTF_8_create_env, close: crate::stemmers::stem_UTF_8_yiddish::yiddish_UTF_8_close_env, stem: crate::stemmers::stem_UTF_8_yiddish::yiddish_UTF_8_stem },
    StemmerModule { name: "english", enc: PG_SQL_ASCII, create: crate::stemmers::stem_ISO_8859_1_english::english_ISO_8859_1_create_env, close: crate::stemmers::stem_ISO_8859_1_english::english_ISO_8859_1_close_env, stem: crate::stemmers::stem_ISO_8859_1_english::english_ISO_8859_1_stem },
];
