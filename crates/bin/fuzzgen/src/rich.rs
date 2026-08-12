//! T1 rich-type productions: formatting functions (to_char/to_date/
//! to_timestamp/to_number with GENERATED format pictures), json/jsonb
//! operators and jsonpath, arrays, bytea, interval/time arithmetic,
//! EXTRACT/date_part, regexp functions, and full-text search surfaces.
//!
//! Everything here is expression-level and hooks into `expr::gen_typed` /
//! `expr::gen_bool` option lists (the F2b smallest-diff recipe). The
//! statement-level set-returning shapes (unnest in FROM etc.) live in
//! crate::types_stmt.
//!
//! Determinism discipline:
//! - format pictures / jsonpath / regex patterns are generated from
//!   integer PRNG draws only — random token strings, valid-ish by
//!   construction; invalid ones are fine (both sides must reject
//!   identically);
//! - no nondeterministic functions ever (gen_random_uuid, now, random);
//! - `to_jsonb` arguments avoid engine-computed floats (float text inside
//!   jsonb would compare exact, outside the ulp path — same discipline as
//!   DML float writes);
//! - tsearch config names draw from the full shipped-config intersection
//!   (30 configs, enumerated identical on both engines 2026-08-11: simple
//!   plus 29 snowball languages including estonian); document text draws
//!   from per-language word pools (T2) so each language's stemmer suffix
//!   rules actually fire — diacritics, cyrillic, arabic/hebrew script,
//!   devanagari and tamil are all in-pool, deterministic and quote-free
//!   by construction.

use crate::catalog::SqlType;
use crate::expr::Expr;
use crate::scope::Scope;
use crate::stmt::Gen;

// ---------------------------------------------------------------------
// T2 language pools: one entry per shipped text-search configuration
// (the pg_ts_config intersection of both engines — 30 rows, verified
// identical). `stem` is the config's snowball dictionary name (used by
// the tsdl module's ALTER MAPPING arm). Words are inflected forms chosen
// to drive each stemmer's suffix rules; all are quote/backslash-free.
// ---------------------------------------------------------------------

pub struct TsLang {
    pub cfg: &'static str,
    pub stem: &'static str,
    pub words: &'static [&'static str],
}

pub const TS_LANGS: &[TsLang] = &[
    TsLang { cfg: "simple", stem: "simple", words: &["alpha", "beta", "gamma", "delta", "omega", "zero", "seven", "matrix"] },
    TsLang { cfg: "english", stem: "english_stem", words: &["running", "jumped", "happiness", "nationalization", "cats", "studies", "beautiful", "conditional"] },
    TsLang { cfg: "arabic", stem: "arabic_stem", words: &["الكلاب", "تجري", "بسرعة", "الحديقة", "الكبيرة", "والجميلة", "المدرسة", "يكتبون"] },
    TsLang { cfg: "armenian", stem: "armenian_stem", words: &["երեխաները", "ուրախությամբ", "խաղում", "այգիներում", "գրքերը", "կարդում", "էին", "մեծ"] },
    TsLang { cfg: "basque", stem: "basque_stem", words: &["etxeak", "mendietan", "handiak", "zuhaitzak", "ibaiaren", "gainean", "lorategietako", "umeak"] },
    TsLang { cfg: "catalan", stem: "catalan_stem", words: &["els", "nens", "jugaven", "alegrement", "jardins", "cançons", "estudiants", "treballaven"] },
    TsLang { cfg: "danish", stem: "danish_stem", words: &["børnene", "legede", "glade", "store", "haver", "løbende", "hunde", "læste"] },
    TsLang { cfg: "dutch", stem: "dutch_stem", words: &["kinderen", "speelden", "vrolijk", "grote", "tuinen", "lopende", "honden", "gelukkig"] },
    TsLang { cfg: "estonian", stem: "estonian_stem", words: &["lapsed", "mängisid", "rõõmsalt", "suurtes", "aedades", "õunapuude", "jooksvad", "koerad"] },
    TsLang { cfg: "finnish", stem: "finnish_stem", words: &["lapset", "leikkivät", "iloisesti", "suurissa", "puutarhoissa", "juoksevat", "koirat", "kirjoja"] },
    TsLang { cfg: "french", stem: "french_stem", words: &["étudiantes", "travaillaient", "sérieusement", "châteaux", "première", "nationales", "heureusement", "mangeaient"] },
    TsLang { cfg: "german", stem: "german_stem", words: &["schönsten", "Häuser", "größer", "Straßen", "Kinder", "spielten", "fröhlich", "Gärten"] },
    TsLang { cfg: "greek", stem: "greek_stem", words: &["παιδιά", "έπαιζαν", "χαρούμενα", "μεγάλους", "κήπους", "τρέχοντας", "σκυλιά", "βιβλία"] },
    TsLang { cfg: "hindi", stem: "hindi_stem", words: &["बच्चे", "बगीचे", "खुशी", "खेलते", "किताबें", "पढ़ते", "लड़के", "दौड़ते"] },
    TsLang { cfg: "hungarian", stem: "hungarian_stem", words: &["gyerekek", "boldogan", "játszottak", "kertekben", "szaladgáló", "kutyák", "könyveket", "olvastak"] },
    TsLang { cfg: "indonesian", stem: "indonesian_stem", words: &["anak-anak", "bermain", "gembira", "taman", "besar", "berlarian", "membaca", "keindahan"] },
    TsLang { cfg: "irish", stem: "irish_stem", words: &["leanaí", "súgradh", "sona", "gairdíní", "móra", "madraí", "leabhair", "rithim"] },
    TsLang { cfg: "italian", stem: "italian_stem", words: &["bambini", "giocavano", "allegramente", "giardini", "grandissimi", "correvano", "cani", "leggevano"] },
    TsLang { cfg: "lithuanian", stem: "lithuanian_stem", words: &["vaikai", "linksmai", "žaidė", "dideliuose", "soduose", "bėgiojantys", "šunys", "knygas"] },
    TsLang { cfg: "nepali", stem: "nepali_stem", words: &["बालबालिकाहरू", "बगैंचामा", "रमाईलो", "खेलिरहेका", "किताबहरू", "पढ्दै", "ठूला", "रूखहरू"] },
    TsLang { cfg: "norwegian", stem: "norwegian_stem", words: &["barna", "lekte", "glade", "store", "hagene", "løpende", "hunder", "leste"] },
    TsLang { cfg: "portuguese", stem: "portuguese_stem", words: &["crianças", "brincavam", "alegremente", "jardins", "grandes", "correndo", "cães", "felizes"] },
    TsLang { cfg: "romanian", stem: "romanian_stem", words: &["copiii", "jucau", "veseli", "grădinile", "mari", "alergând", "câini", "citeau"] },
    TsLang { cfg: "russian", stem: "russian_stem", words: &["бегущие", "собаки", "быстро", "прыгали", "высокие", "заборы", "читали", "книги"] },
    TsLang { cfg: "serbian", stem: "serbian_stem", words: &["деца", "играла", "великим", "баштама", "срећно", "трчали", "књиге", "читали"] },
    TsLang { cfg: "spanish", stem: "spanish_stem", words: &["niños", "corrían", "rápidamente", "añoranza", "construcción", "jugaban", "felices", "jardines"] },
    TsLang { cfg: "swedish", stem: "swedish_stem", words: &["barnen", "lekte", "glatt", "stora", "trädgårdarna", "springande", "hundar", "läste"] },
    TsLang { cfg: "tamil", stem: "tamil_stem", words: &["குழந்தைகள்", "தோட்டத்தில்", "மகிழ்ச்சியாக", "விளையாடினர்", "ஓடும்", "நாய்கள்", "புத்தகங்கள்", "படித்தனர்"] },
    TsLang { cfg: "turkish", stem: "turkish_stem", words: &["çocuklar", "bahçelerde", "mutlulukla", "oynuyorlardı", "ağaçların", "koşan", "köpekler", "kitapları"] },
    TsLang { cfg: "yiddish", stem: "yiddish_stem", words: &["קינדער", "שפילן", "פריילעך", "גרויסע", "גערטנער", "לויפנדיקע", "הינט", "ביכער"] },
];

/// Languages with a shipped stopword file on BOTH engines (the
/// $SHAREDIR/tsearch_data/*.stop intersection, verified 2026-08-11) —
/// the only names `STOPWORDS =` may draw (a missing file is a load
/// error, not a both-side grammar error).
pub const TS_STOPWORD_LANGS: &[&str] = &[
    "danish", "dutch", "english", "finnish", "french", "german", "hungarian", "italian",
    "nepali", "norwegian", "portuguese", "russian", "spanish", "swedish", "turkish",
];

/// Non-word parser-token fuel spliced into documents at low weight:
/// numbers, sfloat, version, url, email, host, file, hyphenated words,
/// numword/numhword shapes and tags — lights the non-stemmer half of the
/// default parser's token table. All quote/backslash-free.
pub const TS_SPECIAL_TOKENS: &[&str] = &[
    "42", "3.14", "-7", "2e7", "v1.2.3", "http://ex.example.com/path?q=1",
    "mot@example.fr", "example.org", "/usr/local/lib", "châteaux-forts", "abc123",
    "12abc", "<b>", "&amp;", "pre-1990s",
];

// ---------------------------------------------------------------------
// Generated format pictures.
// ---------------------------------------------------------------------

/// DCH (datetime) format tokens; separators picked independently.
const DCH_TOKENS: &[&str] = &[
    "YYYY", "YYY", "YY", "Y", "IYYY", "IY", "MM", "MON", "Mon", "mon", "MONTH", "Month",
    "month", "DD", "DDD", "D", "ID", "DAY", "Day", "day", "DY", "Dy", "dy", "HH24", "HH12",
    "HH", "MI", "SS", "MS", "US", "FF1", "FF3", "FF6", "SSSS", "AM", "PM", "am", "pm",
    "A.M.", "P.M.", "Q", "WW", "IW", "W", "CC", "J", "RM", "rm", "TZ", "tz", "OF", "BC",
    "AD", "b.c.", "a.d.", "TZH", "TZM", "FF2", "FF4", "FF5",
];

const DCH_SEPS: &[&str] = &["", " ", "-", "/", ":", ".", "  "];

/// NUM (numeric) format fragments. L/D/G are locale-driven (C locale on
/// both servers by charter; the null diff catches any residue).
const NUM_HEADS: &[&str] = &["", "FM", "S", "FMS", "L", "B"];
const NUM_BODIES: &[&str] = &[
    "9999", "0000", "99999999", "999G999", "0999", "9990", "999", "99V99", "9G999G999",
];
const NUM_DECIMALS: &[&str] = &["", ".99", ".999", ".00", "D99", ".9999999999"];
const NUM_TAILS: &[&str] = &["", "PR", "MI", "S", "PL", "SG", "TH", "th", "EEEE"];

impl Gen<'_> {
    /// Random DCH picture: 1-4 tokens with random separators, optional FM
    /// prefix and occasional TH suffix. Contains no quotes/parens.
    pub fn gen_dch_picture(&mut self) -> String {
        let mut pic = String::new();
        if self.rng.chance(1, 4) {
            pic.push_str("FM");
        }
        let n = 1 + self.rng.below(4);
        for i in 0..n {
            if i > 0 {
                pic.push_str(self.rng.pick(DCH_SEPS));
            }
            pic.push_str(self.rng.pick(DCH_TOKENS));
            if self.rng.chance(1, 8) {
                pic.push_str(if self.rng.chance(1, 2) { "TH" } else { "th" });
            }
        }
        pic
    }

    /// Roundtrip-friendly DCH picture: date-component tokens only, always
    /// separated — what `to_date(to_char(d, pic), pic)` can survive.
    pub fn gen_dch_picture_date(&mut self) -> String {
        let toks: &[&str] = &["YYYY", "MM", "DD", "YYYY", "MM", "DD", "DDD", "J"];
        let n = 1 + self.rng.below(3);
        let mut pic = String::new();
        for i in 0..n {
            if i > 0 {
                pic.push('-');
            }
            pic.push_str(toks[self.rng.below_usize(toks.len())]);
        }
        pic
    }

    /// Random NUM picture. Invalid combinations are fine (identical
    /// two-sided rejection is the assertion).
    pub fn gen_num_picture(&mut self) -> String {
        let mut pic = String::new();
        pic.push_str(self.rng.pick(NUM_HEADS));
        pic.push_str(self.rng.pick(NUM_BODIES));
        pic.push_str(self.rng.pick(NUM_DECIMALS));
        pic.push_str(self.rng.pick(NUM_TAILS));
        if self.rng.chance(1, 10) {
            pic = "RN".to_string(); // roman numerals, whole-picture form
        }
        pic
    }

    /// Small jsonpath expression: root steps, wildcards, filters,
    /// item methods. Paren-balanced, quote-free by construction.
    pub fn gen_jsonpath(&mut self) -> String {
        let mut p = String::new();
        match self.rng.below(4) {
            0 => p.push_str("lax "),
            1 => p.push_str("strict "),
            _ => {}
        }
        p.push('$');
        let steps = self.rng.below(3);
        for _ in 0..=steps {
            match self.rng.below(8) {
                0 => p.push_str(".a"),
                1 => p.push_str(".b"),
                2 => p.push_str(".k"),
                3 => p.push_str(".*"),
                4 => p.push_str("[*]"),
                5 => p.push_str("[0]"),
                6 => p.push_str("[last]"),
                _ => p.push_str("[0 to 2]"),
            }
        }
        match self.rng.below(6) {
            0 => p.push_str(" ? (@ > 1)"),
            1 => p.push_str(" ? (@ == 1)"),
            2 => p.push_str(" ? (@.a < 2)"),
            3 => p.push_str(".type()"),
            4 => p.push_str(".size()"),
            _ => {}
        }
        p
    }

    /// Small regex pattern from a closed grammar: literals, classes,
    /// escapes, quantifiers, groups, alternation, anchors. Balanced
    /// parens/brackets by construction; no quotes.
    pub fn gen_regex_pattern(&mut self) -> String {
        fn atom(g: &mut Gen) -> String {
            match g.rng.below(10) {
                0 => "a".to_string(),
                1 => "b".to_string(),
                2 => ".".to_string(),
                3 => "[abc]".to_string(),
                4 => "[^a]".to_string(),
                5 => "[a-z]".to_string(),
                6 => "\\d".to_string(),
                7 => "\\w".to_string(),
                8 => "\\s".to_string(),
                _ => "x".to_string(),
            }
        }
        fn piece(g: &mut Gen) -> String {
            let mut s = atom(g);
            match g.rng.below(6) {
                0 => s.push('*'),
                1 => s.push('+'),
                2 => s.push('?'),
                3 => s.push_str("{1,3}"),
                _ => {}
            }
            s
        }
        let mut pat = String::new();
        if self.rng.chance(1, 6) {
            pat.push('^');
        }
        let n = 1 + self.rng.below(3);
        for _ in 0..n {
            if self.rng.chance(1, 5) {
                // Group, possibly alternated.
                let a = piece(self);
                let b = piece(self);
                pat.push('(');
                pat.push_str(&a);
                if self.rng.chance(1, 2) {
                    pat.push('|');
                    pat.push_str(&b);
                }
                pat.push(')');
            } else {
                pat.push_str(&piece(self));
            }
        }
        if self.rng.chance(1, 6) {
            pat.push('$');
        }
        pat
    }

    /// Text-search language draw: 1/3 english/simple (the query-shape
    /// workhorses), else uniform over all 30 shipped configs — every
    /// snowball stemmer gets real airtime (T2; T1 ran a 1/3-weight tail).
    pub fn gen_ts_lang(&mut self) -> &'static TsLang {
        if self.rng.chance(1, 3) {
            &TS_LANGS[self.rng.below_usize(2)]
        } else {
            &TS_LANGS[self.rng.below_usize(TS_LANGS.len())]
        }
    }

    /// Config-name form of `gen_ts_lang` (ddl-side callers want the name).
    pub fn gen_ts_config(&mut self) -> &'static str {
        self.gen_ts_lang().cfg
    }

    /// Space-joined document over one language's word pool: `lo..=hi`
    /// words, with low-weight foreign-word and special-token splices
    /// (numbers/urls/emails/hyphenations — the non-stemmer parser paths)
    /// and occasional repeats (position lists / rank weights). Quote-free
    /// by construction.
    pub fn gen_ts_words(&mut self, lang: &TsLang, lo: u64, hi: u64) -> String {
        let n = lo + self.rng.below(hi - lo + 1);
        let mut doc = String::new();
        let mut last: &str = "";
        for i in 0..n {
            if i > 0 {
                doc.push(' ');
            }
            let w: &str = if self.rng.chance(1, 8) {
                self.rng.pick(TS_SPECIAL_TOKENS)
            } else if self.rng.chance(1, 10) {
                let other = &TS_LANGS[self.rng.below_usize(TS_LANGS.len())];
                other.words[self.rng.below_usize(other.words.len())]
            } else if !last.is_empty() && self.rng.chance(1, 10) {
                last // repeat: multi-position lexemes
            } else {
                lang.words[self.rng.below_usize(lang.words.len())]
            };
            doc.push_str(w);
            last = w;
        }
        doc
    }

    /// Document expression for a ts function: usually a pool literal in
    /// the config's language (drives the stemmer), sometimes the generic
    /// text grammar (column refs / formatting output — the T1 surface).
    fn gen_ts_doc(&mut self, scope: &Scope, lang: &TsLang, depth: u32) -> Expr {
        if self.rng.chance(4, 5) {
            let doc = self.gen_ts_words(lang, 2, 7);
            Expr::Lit { sql: format!("'{}'", doc) }
        } else {
            self.gen_typed(scope, SqlType::Text, depth.saturating_sub(1))
        }
    }

    fn lit(&mut self, sql: String) -> Expr {
        Expr::Lit { sql }
    }

    // -----------------------------------------------------------------
    // Text-typed productions.
    // -----------------------------------------------------------------

    /// to_char(numeric-family, NUM picture) -> text.
    pub fn gen_to_char_num(&mut self, scope: &Scope, depth: u32) -> Expr {
        self.fire("fmt:to_char_num");
        let src = *self
            .rng
            .pick(&[SqlType::Numeric, SqlType::Int4, SqlType::Int8, SqlType::Float8]);
        let arg = self.gen_typed(scope, src, depth.saturating_sub(1));
        let pic = self.gen_num_picture();
        let picl = self.lit(format!("'{}'", pic));
        Expr::Func { name: "to_char", args: vec![arg, picl] }
    }

    /// to_char(date/timestamp/interval, DCH picture) -> text.
    pub fn gen_to_char_dt(&mut self, scope: &Scope, depth: u32) -> Expr {
        self.fire("fmt:to_char_dt");
        let src = *self.rng.pick(&[
            SqlType::Timestamp,
            SqlType::Date,
            SqlType::Timestamp,
            SqlType::Interval,
        ]);
        let arg = self.gen_typed(scope, src, depth.saturating_sub(1));
        let pic = self.gen_dch_picture();
        let picl = self.lit(format!("'{}'", pic));
        Expr::Func { name: "to_char", args: vec![arg, picl] }
    }

    /// (jsonb ->> accessor) -> text; accessor is a small key or index.
    pub fn gen_jsonb_arrow_text(&mut self, scope: &Scope, depth: u32) -> Expr {
        self.fire("jsonb:arrow_text");
        let src_ty = if self.rng.chance(1, 4) { SqlType::Json } else { SqlType::Jsonb };
        let lhs = self.gen_typed(scope, src_ty, depth.saturating_sub(1));
        let rhs = self.gen_json_accessor();
        Expr::Binary { op: "->>", lhs: Box::new(lhs), rhs: Box::new(rhs) }
    }

    fn gen_json_accessor(&mut self) -> Expr {
        let sql = match self.rng.below(6) {
            0 => "'a'".to_string(),
            1 => "'b'".to_string(),
            2 => "'k'".to_string(),
            3 => "0".to_string(),
            4 => "(-1)".to_string(),
            _ => format!("{}", self.rng.below(4)),
        };
        Expr::Lit { sql }
    }

    /// encode(bytea, fmt) -> text.
    pub fn gen_encode(&mut self, scope: &Scope, depth: u32) -> Expr {
        self.fire("func:encode");
        let arg = self.gen_typed(scope, SqlType::Bytea, depth.saturating_sub(1));
        let fmt = *self.rng.pick(&["hex", "base64", "escape"]);
        let fmtl = self.lit(format!("'{}'", fmt));
        Expr::Func { name: "encode", args: vec![arg, fmtl] }
    }

    /// (to_tsvector('cfg', doc))::text over the config's language pool —
    /// the stemmer's suffix rules fire on in-language inflections;
    /// tsvector output is deterministic and compares exact. Low-weight
    /// setweight/strip wrappers ride the same surface.
    pub fn gen_tsvector_text(&mut self, scope: &Scope, depth: u32) -> Expr {
        self.fire("ts:vector_text");
        let lang = self.gen_ts_lang();
        let cfgl = self.lit(format!("'{}'", lang.cfg));
        let arg = self.gen_ts_doc(scope, lang, depth);
        let mut vec = Expr::Func { name: "to_tsvector", args: vec![cfgl, arg] };
        if self.rng.chance(1, 5) {
            let w = *self.rng.pick(&["A", "B", "C", "D"]);
            vec = Expr::Func {
                name: "setweight",
                args: vec![vec, Expr::Lit { sql: format!("'{}'", w) }],
            };
        } else if self.rng.chance(1, 6) {
            vec = Expr::Func { name: "strip", args: vec![vec] };
        }
        Expr::Cast { arg: Box::new(vec), to: SqlType::Text }
    }

    /// (Xto_tsquery('cfg', words))::text; occasionally two queries joined
    /// with a tsquery operator, or a querytree() probe.
    pub fn gen_tsquery_text(&mut self, scope: &Scope, depth: u32) -> Expr {
        self.fire("ts:query_text");
        let lang = self.gen_ts_lang();
        let q = self.gen_ts_query(scope, lang, depth);
        if self.rng.chance(1, 6) {
            return Expr::Func { name: "querytree", args: vec![q] };
        }
        Expr::Cast { arg: Box::new(q), to: SqlType::Text }
    }

    /// One tsquery-typed expression over a language pool; low-weight
    /// `&& || !! <->` combination of two.
    fn gen_ts_query(&mut self, scope: &Scope, lang: &TsLang, depth: u32) -> Expr {
        let name = *self.rng.pick(&[
            "plainto_tsquery",
            "phraseto_tsquery",
            "websearch_to_tsquery",
        ]);
        let cfgl = self.lit(format!("'{}'", lang.cfg));
        let words = if name == "websearch_to_tsquery" && self.rng.chance(1, 3) {
            let a = lang.words[self.rng.below_usize(lang.words.len())];
            let b = lang.words[self.rng.below_usize(lang.words.len())];
            let joiner = *self.rng.pick(&[" OR ", " -", " \"", " "]);
            let tail = if joiner == " \"" { format!("{}\"", b) } else { b.to_string() };
            format!("{}{}{}", a, joiner, tail)
        } else {
            self.gen_ts_words(lang, 1, 3)
        };
        let arg = if self.rng.chance(4, 5) {
            Expr::Lit { sql: format!("'{}'", words) }
        } else {
            self.gen_typed(scope, SqlType::Text, depth.saturating_sub(1))
        };
        let q = Expr::Func { name, args: vec![cfgl.clone(), arg] };
        if self.rng.chance(1, 5) {
            let op = *self.rng.pick(&["&&", "||", "<->"]);
            let w2 = self.gen_ts_words(lang, 1, 2);
            let q2 = Expr::Func {
                name: "plainto_tsquery",
                args: vec![cfgl, Expr::Lit { sql: format!("'{}'", w2) }],
            };
            return Expr::Binary { op, lhs: Box::new(q), rhs: Box::new(q2) };
        }
        if self.rng.chance(1, 8) {
            return Expr::Unary { op: "!!", arg: Box::new(q) };
        }
        q
    }

    /// ts_headline('cfg', doc, query [, options]) -> text: the headline
    /// generator over in-language documents (hlCover/mark machinery per
    /// language). Option strings are quote-free literals.
    pub fn gen_ts_headline(&mut self, scope: &Scope, depth: u32) -> Expr {
        self.fire("ts:headline");
        let lang = self.gen_ts_lang();
        let cfgl = self.lit(format!("'{}'", lang.cfg));
        let doc = self.gen_ts_doc(scope, lang, depth);
        let qw = self.gen_ts_words(lang, 1, 2);
        let query = Expr::Func {
            name: "plainto_tsquery",
            args: vec![cfgl.clone(), Expr::Lit { sql: format!("'{}'", qw) }],
        };
        let mut args = vec![cfgl, doc, query];
        if self.rng.chance(1, 2) {
            let opts = *self.rng.pick(&[
                "StartSel=<<, StopSel=>>",
                "MaxWords=7, MinWords=2",
                "ShortWord=2",
                "HighlightAll=true",
                "MaxFragments=2, FragmentDelimiter= ... ",
                "StartSel=**, StopSel=**, MaxWords=10, MinWords=3",
            ]);
            args.push(Expr::Lit { sql: format!("'{}'", opts) });
        }
        Expr::Func { name: "ts_headline", args }
    }

    /// regexp_replace(text, pattern, replacement [, flags]) -> text.
    pub fn gen_regexp_replace(&mut self, scope: &Scope, depth: u32) -> Expr {
        self.fire("func:regexp_replace");
        let arg = self.gen_typed(scope, SqlType::Text, depth.saturating_sub(1));
        let pat = self.gen_regex_pattern();
        let patl = self.lit(format!("'{}'", pat));
        let repl = *self.rng.pick(&["", "X", "<&>", "\\1", "y\\1z"]);
        let repll = self.lit(format!("'{}'", repl));
        let mut args = vec![arg, patl, repll];
        if self.rng.chance(1, 3) {
            let flags = *self.rng.pick(&["g", "i", "gi", "n"]);
            args.push(Expr::Lit { sql: format!("'{}'", flags) });
        }
        Expr::Func { name: "regexp_replace", args }
    }

    // -----------------------------------------------------------------
    // Bool-typed productions.
    // -----------------------------------------------------------------

    /// to_tsvector(cfg, doc) @@ Xto_tsquery(cfg, words) -> bool — doc and
    /// query draw from the SAME language pool, so matches actually fire
    /// (an English query against a Russian doc only ever tests the false
    /// path).
    pub fn gen_ts_match(&mut self, scope: &Scope, depth: u32) -> Expr {
        self.fire("ts:match");
        let lang = self.gen_ts_lang();
        let cfgl = self.lit(format!("'{}'", lang.cfg));
        let doc = self.gen_ts_doc(scope, lang, depth);
        let vec = Expr::Func { name: "to_tsvector", args: vec![cfgl, doc] };
        let query = self.gen_ts_query(scope, lang, 0);
        Expr::Binary { op: "@@", lhs: Box::new(vec), rhs: Box::new(query) }
    }

    /// jsonb containment / key-exists / path-exists -> bool.
    pub fn gen_jsonb_bool(&mut self, scope: &Scope, depth: u32) -> Expr {
        let d = depth.saturating_sub(1);
        match self.rng.below(4) {
            0 => {
                self.fire("jsonb:contains");
                let op = if self.rng.chance(1, 2) { "@>" } else { "<@" };
                let lhs = self.gen_typed(scope, SqlType::Jsonb, d);
                let rhs = self.gen_typed(scope, SqlType::Jsonb, 0);
                Expr::Binary { op, lhs: Box::new(lhs), rhs: Box::new(rhs) }
            }
            1 => {
                self.fire("jsonb:exists_key");
                let op = *self.rng.pick(&["?", "?|", "?&"]);
                let lhs = self.gen_typed(scope, SqlType::Jsonb, d);
                let rhs = if op == "?" {
                    let key = ["a", "b", "k", ""][self.rng.below_usize(4)];
                    self.lit(format!("'{}'", key))
                } else {
                    Expr::Lit { sql: "(ARRAY['a'::text])".to_string() }
                };
                Expr::Binary { op, lhs: Box::new(lhs), rhs: Box::new(rhs) }
            }
            _ => {
                self.fire("jsonb:path_exists");
                let arg = self.gen_typed(scope, SqlType::Jsonb, d);
                let path = self.gen_jsonpath();
                let pathl = self.lit(format!("('{}')::jsonpath", path));
                let name = if self.rng.chance(1, 3) {
                    "jsonb_path_match"
                } else {
                    "jsonb_path_exists"
                };
                Expr::Func { name, args: vec![arg, pathl] }
            }
        }
    }

    // -----------------------------------------------------------------
    // Jsonb-typed productions.
    // -----------------------------------------------------------------

    pub fn gen_jsonb_composite(&mut self, scope: &Scope, depth: u32) -> Expr {
        let d = depth.saturating_sub(1);
        match self.rng.below(7) {
            6 => {
                // jsonb subscripting (jsonbsubs.c fetch arm): 1-2 subscript
                // levels over a jsonb source; keys from the fixture-key
                // pool, occasional integer index. Rendered `(expr)['k']` —
                // parenthesized so any source expression subscripts
                // legally. Hand-verified byte-identical on both engines
                // (G2 deck 1, 2026-08-11) including missing keys and NULL
                // sources.
                self.fire("jsonb:subs");
                let arg = self.gen_typed(scope, SqlType::Jsonb, d);
                let mut post = format!(")[{}]", self.gen_jsonb_subscript());
                if self.rng.chance(1, 3) {
                    post.push_str(&format!("[{}]", self.gen_jsonb_subscript()));
                }
                Expr::Wrap { pre: "(".to_string(), arg: Box::new(arg), post }
            }
            0 => {
                self.fire("jsonb:arrow");
                let lhs = self.gen_typed(scope, SqlType::Jsonb, d);
                let rhs = self.gen_json_accessor();
                Expr::Binary { op: "->", lhs: Box::new(lhs), rhs: Box::new(rhs) }
            }
            1 => {
                self.fire("jsonb:concat");
                let lhs = self.gen_typed(scope, SqlType::Jsonb, d);
                let rhs = self.gen_typed(scope, SqlType::Jsonb, d);
                Expr::Binary { op: "||", lhs: Box::new(lhs), rhs: Box::new(rhs) }
            }
            2 => {
                self.fire("jsonb:minus");
                let lhs = self.gen_typed(scope, SqlType::Jsonb, d);
                let rhs = self.gen_json_accessor();
                Expr::Binary { op: "-", lhs: Box::new(lhs), rhs: Box::new(rhs) }
            }
            3 => {
                self.fire("jsonb:build");
                if self.rng.chance(1, 2) {
                    let k = *self.rng.pick(&["a", "b", "k"]);
                    let v = self.gen_to_jsonb_arg(scope);
                    Expr::Func {
                        name: "jsonb_build_object",
                        args: vec![Expr::Lit { sql: format!("'{}'", k) }, v],
                    }
                } else {
                    let a = self.gen_to_jsonb_arg(scope);
                    let b = self.gen_to_jsonb_arg(scope);
                    Expr::Func { name: "jsonb_build_array", args: vec![a, b] }
                }
            }
            4 => {
                self.fire("jsonb:to_jsonb");
                let v = self.gen_to_jsonb_arg(scope);
                Expr::Func { name: "to_jsonb", args: vec![v] }
            }
            _ => {
                self.fire("jsonb:path_query");
                let arg = self.gen_typed(scope, SqlType::Jsonb, d);
                let path = self.gen_jsonpath();
                let pathl = self.lit(format!("('{}')::jsonpath", path));
                let name = if self.rng.chance(1, 2) {
                    "jsonb_path_query_first"
                } else {
                    "jsonb_path_query_array"
                };
                Expr::Func { name, args: vec![arg, pathl] }
            }
        }
    }

    /// One jsonb subscript: a small key from the fixture-key pool or an
    /// integer index (negative indexes count from the end on arrays).
    pub(crate) fn gen_jsonb_subscript(&mut self) -> String {
        match self.rng.below(8) {
            0 => "'a'".to_string(),
            1 => "'b'".to_string(),
            2 => "'k'".to_string(),
            3 => "'nested'".to_string(),
            4 => "0".to_string(),
            5 => "1".to_string(),
            6 => "-1".to_string(),
            _ => format!("{}", self.rng.below(4)),
        }
    }

    /// to_jsonb / jsonb_build argument: non-float leaf (float text inside
    /// jsonb compares exact — keep values literal-derived).
    fn gen_to_jsonb_arg(&mut self, scope: &Scope) -> Expr {
        let ty = *self.rng.pick(&[
            SqlType::Int4,
            SqlType::Int8,
            SqlType::Numeric,
            SqlType::Text,
            SqlType::Bool,
            SqlType::Date,
        ]);
        self.gen_typed(scope, ty, 0)
    }

    // -----------------------------------------------------------------
    // Bytea / interval / time productions.
    // -----------------------------------------------------------------

    pub fn gen_bytea_composite(&mut self, scope: &Scope, depth: u32) -> Expr {
        let d = depth.saturating_sub(1);
        match self.rng.below(3) {
            0 => {
                self.fire("bytea:concat");
                let lhs = self.gen_typed(scope, SqlType::Bytea, d);
                let rhs = self.gen_typed(scope, SqlType::Bytea, d);
                Expr::Binary { op: "||", lhs: Box::new(lhs), rhs: Box::new(rhs) }
            }
            1 => {
                self.fire("bytea:substr");
                let arg = self.gen_typed(scope, SqlType::Bytea, d);
                let start_n = self.rng_i64(-2, 6);
                let len_n = self.rng_i64(0, 6);
                let start = self.lit(format!("{}", start_n));
                let len = self.lit(format!("{}", len_n));
                Expr::Func { name: "substr", args: vec![arg, start, len] }
            }
            _ => {
                self.fire("bytea:decode_rt");
                let arg = self.gen_typed(scope, SqlType::Bytea, d);
                let hex = Expr::Lit { sql: "'hex'".to_string() };
                let enc = Expr::Func { name: "encode", args: vec![arg, hex.clone()] };
                Expr::Func { name: "decode", args: vec![enc, hex] }
            }
        }
    }

    pub(crate) fn rng_i64(&mut self, lo: i64, hi: i64) -> i64 {
        self.rng.range_i64(lo, hi)
    }

    pub fn gen_interval_composite(&mut self, scope: &Scope, depth: u32) -> Expr {
        let d = depth.saturating_sub(1);
        match self.rng.below(5) {
            0 => {
                self.fire("interval:add");
                let op = if self.rng.chance(1, 2) { "+" } else { "-" };
                let lhs = self.gen_typed(scope, SqlType::Interval, d);
                let rhs = self.gen_typed(scope, SqlType::Interval, d);
                Expr::Binary { op, lhs: Box::new(lhs), rhs: Box::new(rhs) }
            }
            1 => {
                self.fire("interval:neg");
                let arg = self.gen_typed(scope, SqlType::Interval, d);
                Expr::Unary { op: "-", arg: Box::new(arg) }
            }
            2 => {
                self.fire("interval:ts_diff");
                let lhs = self.gen_typed(scope, SqlType::Timestamp, d);
                let rhs = self.gen_typed(scope, SqlType::Timestamp, d);
                Expr::Binary { op: "-", lhs: Box::new(lhs), rhs: Box::new(rhs) }
            }
            3 => {
                self.fire("interval:mul");
                let arg = self.gen_typed(scope, SqlType::Interval, d);
                let k_n = self.rng_i64(-3, 3);
                let k = self.lit(format!("{}", k_n));
                Expr::Binary { op: "*", lhs: Box::new(arg), rhs: Box::new(k) }
            }
            _ => {
                self.fire("interval:justify");
                let name = *self
                    .rng
                    .pick(&["justify_hours", "justify_days", "justify_interval"]);
                let arg = self.gen_typed(scope, SqlType::Interval, d);
                Expr::Func { name, args: vec![arg] }
            }
        }
    }

    /// timestamp + interval (or - interval) -> timestamp.
    pub fn gen_ts_plus_interval(&mut self, scope: &Scope, depth: u32) -> Expr {
        self.fire("binop:ts+interval");
        let d = depth.saturating_sub(1);
        let op = if self.rng.chance(1, 2) { "+" } else { "-" };
        let lhs = self.gen_typed(scope, SqlType::Timestamp, d);
        let rhs = self.gen_typed(scope, SqlType::Interval, d);
        Expr::Binary { op, lhs: Box::new(lhs), rhs: Box::new(rhs) }
    }

    /// time + interval -> time (wraps around midnight).
    pub fn gen_time_plus_interval(&mut self, scope: &Scope, depth: u32) -> Expr {
        self.fire("binop:time+interval");
        let d = depth.saturating_sub(1);
        let op = if self.rng.chance(1, 2) { "+" } else { "-" };
        let lhs = self.gen_typed(scope, SqlType::Time, d);
        let rhs = self.gen_typed(scope, SqlType::Interval, d);
        Expr::Binary { op, lhs: Box::new(lhs), rhs: Box::new(rhs) }
    }

    /// to_timestamp/to_date round trip through a generated picture (same
    /// picture literal on both sides); occasionally fixture text instead —
    /// both-sides-error fuel.
    pub fn gen_to_timestamp_rt(&mut self, scope: &Scope, depth: u32) -> Expr {
        self.fire("fmt:to_timestamp_rt");
        let pic = self.gen_dch_picture();
        let picl = Expr::Lit { sql: format!("'{}'", pic) };
        let text = if self.rng.chance(3, 4) {
            let src = self.gen_typed(scope, SqlType::Timestamp, depth.saturating_sub(1));
            Expr::Func { name: "to_char", args: vec![src, picl.clone()] }
        } else {
            self.gen_typed(scope, SqlType::Text, 0)
        };
        Expr::Cast {
            arg: Box::new(Expr::Func { name: "to_timestamp", args: vec![text, picl] }),
            to: SqlType::Timestamp,
        }
    }

    pub fn gen_to_date_rt(&mut self, scope: &Scope, depth: u32) -> Expr {
        self.fire("fmt:to_date_rt");
        let pic = self.gen_dch_picture_date();
        let picl = Expr::Lit { sql: format!("'{}'", pic) };
        let text = if self.rng.chance(3, 4) {
            let src = self.gen_typed(scope, SqlType::Date, depth.saturating_sub(1));
            Expr::Func { name: "to_char", args: vec![src, picl.clone()] }
        } else {
            self.gen_typed(scope, SqlType::Text, 0)
        };
        Expr::Func { name: "to_date", args: vec![text, picl] }
    }

    /// to_number(to_char(n, pic), pic) -> numeric (low weight; the parse
    /// half of the NUM engine).
    pub fn gen_to_number_rt(&mut self, scope: &Scope, depth: u32) -> Expr {
        self.fire("fmt:to_number_rt");
        let pic = self.gen_num_picture();
        let picl = Expr::Lit { sql: format!("'{}'", pic) };
        let src = self.gen_typed(scope, SqlType::Numeric, depth.saturating_sub(1));
        let text = Expr::Func { name: "to_char", args: vec![src, picl.clone()] };
        Expr::Func { name: "to_number", args: vec![text, picl] }
    }

    /// EXTRACT(field FROM datetime/interval) -> numeric.
    pub fn gen_extract(&mut self, scope: &Scope, depth: u32) -> Expr {
        self.fire("fmt:extract");
        let field = *self.rng.pick(&[
            "epoch", "year", "month", "day", "hour", "minute", "second", "dow", "doy",
            "quarter", "week", "century", "millennium", "microseconds", "milliseconds",
            "decade", "isodow", "isoyear", "julian",
        ]);
        let src = *self.rng.pick(&[
            SqlType::Timestamp,
            SqlType::Date,
            SqlType::Interval,
            SqlType::Time,
        ]);
        let arg = self.gen_typed(scope, src, depth.saturating_sub(1));
        // Lowercase `from`: the statement-level convention is that " FROM "
        // (uppercase, space-delimited) introduces a relation source —
        // textual test oracles and reducers lean on it. EXTRACT's argument
        // separator must not masquerade as one.
        Expr::Wrap {
            pre: format!("EXTRACT({} from ", field),
            arg: Box::new(arg),
            post: ")".to_string(),
        }
    }

    /// date_part('field', datetime) -> float8.
    pub fn gen_date_part(&mut self, scope: &Scope, depth: u32) -> Expr {
        self.fire("func:date_part");
        let field = *self.rng.pick(&[
            "epoch", "year", "month", "day", "hour", "minute", "second", "dow", "doy",
        ]);
        let src = *self.rng.pick(&[SqlType::Timestamp, SqlType::Date, SqlType::Interval]);
        let arg = self.gen_typed(scope, src, depth.saturating_sub(1));
        Expr::Func {
            name: "date_part",
            args: vec![Expr::Lit { sql: format!("'{}'", field) }, arg],
        }
    }

    /// ts_rank[_cd](to_tsvector(cfg, doc), query [, normalization]) ->
    /// float4 (rides the existing ulp compare path via the wire float4
    /// oid); same-language doc/query so nonzero ranks occur, plus the
    /// optional weight array and normalization-mask arms.
    pub fn gen_ts_rank(&mut self, scope: &Scope, depth: u32) -> Expr {
        self.fire("ts:rank");
        let lang = self.gen_ts_lang();
        let cfgl = Expr::Lit { sql: format!("'{}'", lang.cfg) };
        let doc = self.gen_ts_doc(scope, lang, depth);
        let mut vec = Expr::Func { name: "to_tsvector", args: vec![cfgl.clone(), doc] };
        if self.rng.chance(1, 5) {
            let w = *self.rng.pick(&["A", "B", "C", "D"]);
            vec = Expr::Func {
                name: "setweight",
                args: vec![vec, Expr::Lit { sql: format!("'{}'", w) }],
            };
        }
        let qw = self.gen_ts_words(lang, 1, 2);
        let query = Expr::Func {
            name: "plainto_tsquery",
            args: vec![cfgl, Expr::Lit { sql: format!("'{}'", qw) }],
        };
        let name = if self.rng.chance(1, 4) { "ts_rank_cd" } else { "ts_rank" };
        let mut args = Vec::new();
        if self.rng.chance(1, 5) {
            args.push(Expr::Lit { sql: "(ARRAY[0.1, 0.2, 0.4, 1.0])::float4[]".to_string() });
        }
        args.push(vec);
        args.push(query);
        if self.rng.chance(1, 4) {
            let norm = *self.rng.pick(&["0", "1", "2", "4", "8", "16", "32"]);
            args.push(Expr::Lit { sql: norm.to_string() });
        }
        Expr::Func { name, args }
    }

    // -----------------------------------------------------------------
    // Array productions.
    // -----------------------------------------------------------------

    /// int4-typed draws over arrays: length/cardinality or subscript.
    pub fn gen_array_int(&mut self, scope: &Scope, depth: u32) -> Expr {
        let d = depth.saturating_sub(1);
        let arr_ty = if self.rng.chance(1, 2) { SqlType::Int4Arr } else { SqlType::TextArr };
        if self.rng.chance(1, 2) {
            self.fire("arr:length");
            let arg = self.gen_typed(scope, arr_ty, d);
            if self.rng.chance(1, 2) {
                Expr::Func { name: "cardinality", args: vec![arg] }
            } else {
                let dim = Expr::Lit { sql: "1".to_string() };
                Expr::Func { name: "array_length", args: vec![arg, dim] }
            }
        } else {
            self.fire("arr:sub");
            let arg = self.gen_typed(scope, SqlType::Int4Arr, d);
            let idx = self.rng_i64(-1, 4);
            Expr::Wrap {
                pre: "(".to_string(),
                arg: Box::new(arg),
                post: format!(")[{}]", idx),
            }
        }
    }

    /// One slice spec for an array subscript: closed `lo:hi` (reversed and
    /// negative bounds included — empty results, matched semantics),
    /// open-ended `:hi` / `lo:`, or the full-slice `:`. Hand-verified
    /// byte-identical on both engines (G2 deck 2, 2026-08-11).
    fn gen_array_slice_spec(&mut self) -> String {
        match self.rng.below(6) {
            0 | 1 => {
                let lo = self.rng_i64(-1, 3);
                let hi = self.rng_i64(0, 5);
                format!("{}:{}", lo, hi)
            }
            2 => format!(":{}", self.rng_i64(0, 4)),
            3 => format!("{}:", self.rng_i64(-1, 3)),
            4 => ":".to_string(),
            // Reversed bounds: a deliberate empty-slice draw.
            _ => {
                let lo = self.rng_i64(2, 4);
                let hi = self.rng_i64(-1, 1);
                format!("{}:{}", lo, hi)
            }
        }
    }

    pub fn gen_textarr_composite(&mut self, scope: &Scope, depth: u32) -> Expr {
        let d = depth.saturating_sub(1);
        match self.rng.below(6) {
            5 => {
                // Slice over a text array — array_get_slice / multi-dim
                // double subscripting on the text-array side.
                self.fire("arr:slice");
                let arr = self.gen_typed(scope, SqlType::TextArr, d);
                let mut post = format!(")[{}]", self.gen_array_slice_spec());
                if self.rng.chance(1, 4) {
                    // Second slice level: 1-D fixture arrays yield NULL /
                    // empty (matched multi-dim semantics on both sides).
                    post.push_str(&format!("[{}]", self.gen_array_slice_spec()));
                }
                Expr::Wrap { pre: "(".to_string(), arg: Box::new(arr), post }
            }
            0 => {
                self.fire("arr:concat");
                let lhs = self.gen_typed(scope, SqlType::TextArr, d);
                let rhs = self.gen_typed(scope, SqlType::TextArr, d);
                Expr::Binary { op: "||", lhs: Box::new(lhs), rhs: Box::new(rhs) }
            }
            1 => {
                self.fire("arr:append");
                let arr = self.gen_typed(scope, SqlType::TextArr, d);
                let el = self.gen_typed(scope, SqlType::Text, 0);
                Expr::Func { name: "array_append", args: vec![arr, el] }
            }
            2 => {
                self.fire("func:string_to_array");
                let t = self.gen_typed(scope, SqlType::Text, d);
                let delim = *self.rng.pick(&["','", "''", "' '", "NULL"]);
                Expr::Func {
                    name: "string_to_array",
                    args: vec![t, Expr::Lit { sql: delim.to_string() }],
                }
            }
            3 => {
                self.fire("func:regexp_split_arr");
                let t = self.gen_typed(scope, SqlType::Text, d);
                let pat = self.gen_regex_pattern();
                Expr::Func {
                    name: "regexp_split_to_array",
                    args: vec![t, Expr::Lit { sql: format!("'{}'", pat) }],
                }
            }
            _ => {
                self.fire("func:regexp_match");
                let t = self.gen_typed(scope, SqlType::Text, d);
                let pat = self.gen_regex_pattern();
                Expr::Func {
                    name: "regexp_match",
                    args: vec![t, Expr::Lit { sql: format!("'{}'", pat) }],
                }
            }
        }
    }

    pub fn gen_intarr_composite(&mut self, scope: &Scope, depth: u32) -> Expr {
        let d = depth.saturating_sub(1);
        match self.rng.below(4) {
            0 => {
                self.fire("arr:concat");
                let lhs = self.gen_typed(scope, SqlType::Int4Arr, d);
                let rhs = self.gen_typed(scope, SqlType::Int4Arr, d);
                Expr::Binary { op: "||", lhs: Box::new(lhs), rhs: Box::new(rhs) }
            }
            1 => {
                self.fire("arr:append");
                let arr = self.gen_typed(scope, SqlType::Int4Arr, d);
                let el = self.gen_typed(scope, SqlType::Int4, 0);
                let name = if self.rng.chance(1, 2) { "array_append" } else { "array_remove" };
                Expr::Func { name, args: vec![arr, el] }
            }
            2 => {
                self.fire("arr:slice");
                let arr = self.gen_typed(scope, SqlType::Int4Arr, d);
                let post = format!(")[{}]", self.gen_array_slice_spec());
                Expr::Wrap { pre: "(".to_string(), arg: Box::new(arr), post }
            }
            _ => {
                self.fire("arr:prepend");
                let el = self.gen_typed(scope, SqlType::Int4, 0);
                let arr = self.gen_typed(scope, SqlType::Int4Arr, d);
                Expr::Func { name: "array_prepend", args: vec![el, arr] }
            }
        }
    }

    /// ANY/ALL over an array: `elem cmp ANY(array)` -> bool.
    pub fn gen_any_all(&mut self, scope: &Scope, depth: u32) -> Expr {
        self.fire("arr:any_all");
        let d = depth.saturating_sub(1);
        let (el_ty, arr_ty) = if self.rng.chance(1, 2) {
            (SqlType::Int4, SqlType::Int4Arr)
        } else {
            (SqlType::Text, SqlType::TextArr)
        };
        let lhs = self.gen_typed(scope, el_ty, d);
        let op = self.pick_cmp_op();
        let quant = if self.rng.chance(2, 3) { "ANY" } else { "ALL" };
        let arr = self.gen_typed(scope, arr_ty, d);
        // The argument must parse as the ARRAY form of ANY/ALL, never the
        // subquery form: a bare scalar subquery in that position would be
        // reinterpreted as `op ANY (SELECT ...)` over rows of the ARRAY
        // type (a guaranteed operator-lookup error). A self-cast pins the
        // array reading without changing the value.
        Expr::Binary {
            op,
            lhs: Box::new(lhs),
            rhs: Box::new(Expr::Wrap {
                pre: format!("{}(", quant),
                arg: Box::new(Expr::Cast { arg: Box::new(arr), to: arr_ty }),
                post: ")".to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::scope::ScopeRel;
    use crate::weights::WeightTable;

    fn with_gen<R>(seed: u64, f: impl FnOnce(&mut Gen, &Scope) -> R) -> R {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let rich = cat.tables.iter().position(|t| t.name == "fz_rich").unwrap();
        let rels = vec![ScopeRel::from_table(&cat.tables[rich], "t0".to_string())];
        let w = WeightTable::defaults();
        let mut rng = Rng::new(seed);
        let mut prods = Vec::new();
        let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
        let scope = Scope { rels: &rels, outer: None };
        f(&mut g, &scope)
    }

    #[test]
    fn pictures_are_deterministic_and_quote_free() {
        let run = |seed| {
            with_gen(seed, |g, _| {
                (0..50)
                    .map(|_| (g.gen_dch_picture(), g.gen_num_picture(), g.gen_jsonpath()))
                    .collect::<Vec<_>>()
            })
        };
        assert_eq!(run(5), run(5));
        assert_ne!(run(5), run(6));
        for (dch, num, path) in run(9) {
            for s in [&dch, &num] {
                assert!(!s.contains('\''), "quote in picture {s}");
                assert!(!s.contains('(') && !s.contains(')'), "paren in picture {s}");
            }
            assert_eq!(
                path.matches('(').count(),
                path.matches(')').count(),
                "unbalanced jsonpath {path}"
            );
            assert!(!path.contains('\''), "quote in jsonpath {path}");
        }
    }

    #[test]
    fn regex_patterns_are_balanced() {
        with_gen(11, |g, _| {
            for _ in 0..200 {
                let p = g.gen_regex_pattern();
                assert_eq!(p.matches('(').count(), p.matches(')').count(), "{p}");
                assert_eq!(p.matches('[').count(), p.matches(']').count(), "{p}");
                assert!(!p.contains('\''), "quote in pattern {p}");
            }
        });
    }

    #[test]
    fn rich_productions_render_balanced_sql() {
        with_gen(13, |g, scope| {
            for i in 0..300 {
                let e = match i % 14 {
                    0 => g.gen_to_char_num(scope, 2),
                    1 => g.gen_to_char_dt(scope, 2),
                    2 => g.gen_jsonb_composite(scope, 2),
                    3 => g.gen_jsonb_bool(scope, 2),
                    4 => g.gen_ts_match(scope, 2),
                    5 => g.gen_bytea_composite(scope, 2),
                    6 => g.gen_interval_composite(scope, 2),
                    7 => g.gen_extract(scope, 2),
                    8 => g.gen_ts_rank(scope, 2),
                    9 => g.gen_textarr_composite(scope, 2),
                    10 => g.gen_intarr_composite(scope, 2),
                    11 => g.gen_ts_headline(scope, 2),
                    12 => g.gen_tsquery_text(scope, 2),
                    _ => g.gen_any_all(scope, 2),
                };
                let sql = e.to_sql();
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced: {sql}"
                );
                assert!(!sql.contains('\n'));
            }
        });
    }

    #[test]
    fn ts_config_tail_fires() {
        let cfgs = with_gen(17, |g, _| {
            (0..300).map(|_| g.gen_ts_config()).collect::<Vec<_>>()
        });
        assert!(cfgs.contains(&"english"));
        assert!(
            cfgs.iter().any(|c| !matches!(*c, "english" | "simple")),
            "snowball tail never fired"
        );
    }

    #[test]
    fn ts_pools_are_quote_free_and_complete() {
        // 30 shipped configs (both-engine pg_ts_config intersection).
        assert_eq!(TS_LANGS.len(), 30);
        let mut names: Vec<&str> = TS_LANGS.iter().map(|l| l.cfg).collect();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), 30, "duplicate config name in TS_LANGS");
        for lang in TS_LANGS {
            assert!(!lang.words.is_empty());
            for w in lang.words {
                assert!(!w.contains('\'') && !w.contains('\\'), "bad pool word {w}");
                assert!(std::str::from_utf8(w.as_bytes()).is_ok());
            }
        }
        for t in TS_SPECIAL_TOKENS {
            assert!(!t.contains('\'') && !t.contains('\\'), "bad special token {t}");
        }
        for s in TS_STOPWORD_LANGS {
            assert!(TS_LANGS.iter().any(|l| l.cfg == *s), "stopword lang {s} not a config");
        }
    }

    #[test]
    fn ts_docs_are_deterministic_and_all_langs_reachable() {
        let run = |seed| {
            with_gen(seed, |g, _| {
                (0..200)
                    .map(|_| {
                        let lang = g.gen_ts_lang();
                        (lang.cfg, g.gen_ts_words(lang, 2, 7))
                    })
                    .collect::<Vec<_>>()
            })
        };
        assert_eq!(run(23), run(23));
        assert_ne!(run(23), run(24));
        let langs: Vec<&str> = run(23).iter().map(|(c, _)| *c).collect();
        // Uniform draw over 30 configs x 200 pulls: expect a broad spread.
        let mut distinct = langs.clone();
        distinct.sort();
        distinct.dedup();
        assert!(distinct.len() > 15, "only {} languages drawn", distinct.len());
        for (_, doc) in run(23) {
            assert!(!doc.contains('\''), "quote in doc {doc}");
        }
    }
}
