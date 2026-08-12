//! T2 text-search DDL statement module: CREATE/ALTER/DROP TEXT SEARCH
//! CONFIGURATION and DICTIONARY, mapping surgery, COMMENT, RENAME, and
//! per-language probe statements (to_tsvector / ts_debug / ts_lexize) over
//! both the shipped configs and the session-created ones.
//!
//! `TsState` (session-persistent, swapped in and out of `Gen` exactly like
//! `DdlState`) tracks every text-search object this module creates so later
//! statements are valid by construction — the same zero-42xxx discipline as
//! the ddl module:
//!
//!   - names come from monotonic counters (`fz_tscfg{n}` / `fz_tsdict{n}`)
//!     and are never reused, so CREATE can't collide and RENAME targets
//!     are always fresh;
//!   - each created config models its mapped-token-type set. All shipped
//!     configs carry the same 19-type mapping (verified identical on both
//!     engines), so a `COPY = <shipped>` starts from `BASE_MAPPED`.
//!     ADD MAPPING only ever targets unmapped types (a duplicate ADD is a
//!     23505), ALTER MAPPING upserts (verified: it creates missing
//!     mappings) so any type is legal and joins the model set, and plain
//!     DROP MAPPING only targets mapped types (IF EXISTS may target any);
//!   - dictionaries draw the `simple` template plus the file-backed
//!     templates (snowball/ispell/thesaurus/synonym) over the SAMPLE data
//!     files shipped in `$SHAREDIR/tsearch_data` — `ispell_sample`,
//!     `hunspell_sample_long`, `hunspell_sample_num`, `thesaurus_sample`,
//!     `synonym_sample` — all verified loadable and byte-identical on both
//!     engines (2026-08-11). These are the only file names used: an
//!     unshipped name is a load error, and the pgrust side resolves the
//!     share directory through `PGRUST_PGSHAREDIR`, which every rig script
//!     points at the C install's share dir (the ruled initdb-via-C path),
//!     so both engines read the same files. `STOPWORDS =` likewise draws
//!     only from `rich::TS_STOPWORD_LANGS` (the both-engine *.stop
//!     intersection);
//!   - a dictionary referenced by any mapping is marked `used` and never
//!     dropped (dropping it would 2BP01 on the dependent config);
//!   - configs are dropped freely (mappings die with them); probes only
//!     ever reference live objects.
//!
//! Population caps keep the object count bounded over long streams; at the
//! cap, create picks convert into probe picks.

use crate::rich::{TS_LANGS, TS_STOPWORD_LANGS};
use crate::stem_data::STEM_SUFFIXES;
use crate::stem_reach::STEM_REACH;
use crate::stmt::{Gen, StmtKind};

/// Token types every shipped configuration maps (the pg_ts_config_map
/// set shared by all 30 configs, verified identical on both engines).
pub const BASE_MAPPED: &[&str] = &[
    "asciihword", "asciiword", "email", "file", "float", "host", "hword",
    "hword_asciipart", "hword_numpart", "hword_part", "int", "numhword", "numword",
    "sfloat", "uint", "url", "url_path", "version", "word",
];

/// Sample ispell/hunspell dictionaries shipped in `$SHAREDIR/tsearch_data`
/// with BOTH a `.dict` and an `.affix` file (hunspell_sample has only an
/// affix file and is therefore never used).
pub const SAMPLE_ISPELL: &[&str] =
    &["ispell_sample", "hunspell_sample_long", "hunspell_sample_num"];

/// Default-parser token types NOT mapped by shipped configs (ADD MAPPING
/// fuel; `blank` is legal to map even though shipped configs never do).
pub const EXTRA_TOKENS: &[&str] = &["blank", "tag", "protocol", "entity"];

const MAX_LIVE_CFGS: usize = 5;
const MAX_LIVE_DICTS: usize = 5;

#[derive(Clone, Debug)]
pub struct TsCfg {
    pub name: String,
    /// Token types currently mapped (generation-time model).
    pub mapped: Vec<&'static str>,
}

#[derive(Clone, Debug)]
pub struct TsDict {
    pub name: String,
    /// Referenced by some mapping at some point: never dropped (2BP01).
    pub used: bool,
    /// Built from the `simple` template: only these accept the
    /// STOPWORDS/ACCEPT option surgery `ALTER ... DICTIONARY` emits (an
    /// ispell/thesaurus/synonym dictionary rejects unknown options).
    pub simple: bool,
}

#[derive(Clone, Debug, Default)]
pub struct TsState {
    pub cfgs: Vec<TsCfg>,
    pub dicts: Vec<TsDict>,
    cfg_n: u32,
    dict_n: u32,
    /// tsdl:stemdrain sweep state (LD3): index into
    /// `stem_data::STEM_SUFFIXES` and the cursor within that language's
    /// suffix table. The sweep walks every language's every among-table
    /// string exactly once per full cycle, so a long-enough stream is an
    /// exhaustive suffix-arm drain by construction, not by luck.
    stem_lang: usize,
    stem_pos: usize,
    /// tsdl:stemvocab sweep state (LD10): index into `STEM_VOCAB` and the
    /// cursor within that language's vocabulary pool — same exhaustive-
    /// by-construction contract as the suffix sweep.
    vocab_lang: usize,
    vocab_pos: usize,
    /// Created `TEMPLATE = snowball, Language = 'porter'` dictionary
    /// (the one stemmer with no shipped `<lang>_stem` name), created on
    /// first porter drain in the session.
    porter_dict: Option<String>,
}

impl TsState {
    pub fn new() -> TsState {
        TsState::default()
    }

    fn next_cfg_name(&mut self) -> String {
        let n = self.cfg_n;
        self.cfg_n += 1;
        format!("fz_tscfg{}", n)
    }

    fn next_dict_name(&mut self) -> String {
        let n = self.dict_n;
        self.dict_n += 1;
        format!("fz_tsdict{}", n)
    }
}

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_tsdl_module(g: &mut Gen) -> Vec<StmtKind> {
    let mut options: Vec<&'static str> =
        vec!["tsdl:probe", "tsdl:debug", "tsdl:lexize", "tsdl:stemdrain", "tsdl:stemvocab", "tsdl:tsfx"];
    if g.ts.cfgs.len() < MAX_LIVE_CFGS {
        options.push("tsdl:create_cfg");
    }
    if g.ts.dicts.len() < MAX_LIVE_DICTS {
        options.push("tsdl:create_dict");
    }
    if !g.ts.cfgs.is_empty() {
        options.push("tsdl:alter_mapping");
        options.push("tsdl:comment");
        options.push("tsdl:drop");
        options.push("tsdl:rename");
    }
    if !g.ts.dicts.is_empty() {
        options.push("tsdl:alter_dict");
    }
    let pick = g.weights.pick(g.rng, &options);
    let stmts = match pick {
        "tsdl:create_cfg" => create_cfg(g),
        "tsdl:create_dict" => create_dict(g),
        "tsdl:alter_mapping" => alter_mapping(g),
        "tsdl:alter_dict" => alter_dict(g),
        "tsdl:comment" => comment(g),
        "tsdl:rename" => rename(g),
        "tsdl:drop" => drop_obj(g),
        "tsdl:debug" => vec![debug_probe(g)],
        "tsdl:lexize" => vec![lexize_probe(g)],
        "tsdl:stemdrain" => stemdrain(g),
        "tsdl:stemvocab" => stemvocab(g),
        "tsdl:tsfx" => gen_tsfx_stmts(g),
        _ => vec![vector_probe(g)],
    };
    stmts.into_iter().map(StmtKind::Raw).collect()
}

/// Config name for a probe: a live created config when one exists (and
/// the coin lands), else a shipped one.
fn probe_cfg(g: &mut Gen) -> String {
    if !g.ts.cfgs.is_empty() && g.rng.chance(1, 2) {
        let i = g.rng.below_usize(g.ts.cfgs.len());
        g.ts.cfgs[i].name.clone()
    } else {
        g.gen_ts_config().to_string()
    }
}

/// Multilingual document: usually one language's pool, the special-token
/// splices ride inside `gen_ts_words`.
fn probe_doc(g: &mut Gen) -> String {
    let lang = g.gen_ts_lang();
    g.gen_ts_words(lang, 2, 8)
}

fn vector_probe(g: &mut Gen) -> String {
    g.fire("tsdl:probe");
    let cfg = probe_cfg(g);
    let doc = probe_doc(g);
    format!("SELECT (to_tsvector('{}', '{}'))::text;", cfg, doc)
}

fn debug_probe(g: &mut Gen) -> String {
    g.fire("tsdl:debug");
    let cfg = probe_cfg(g);
    let doc = probe_doc(g);
    format!("SELECT ts_debug('{}', '{}')::text;", cfg, doc)
}

fn lexize_probe(g: &mut Gen) -> String {
    g.fire("tsdl:lexize");
    let dict = if !g.ts.dicts.is_empty() && g.rng.chance(1, 2) {
        let i = g.rng.below_usize(g.ts.dicts.len());
        g.ts.dicts[i].name.clone()
    } else {
        g.gen_ts_lang().stem.to_string()
    };
    let lang = g.gen_ts_lang();
    let word = lang.words[g.rng.below_usize(lang.words.len())];
    format!("SELECT ts_lexize('{}', '{}');", dict, word)
}

/// Words per stemdrain statement (16 keeps the ARRAY literal well under
/// any length limit while sweeping serbian's 2218 strings in ~140
/// statements).
const STEMDRAIN_WORDS: usize = 16;

/// Latin-script serbian bases: the serbian stemmer transliterates
/// cyrillic to latin in its prelude and every Step_1/2/3 suffix table is
/// latin, so latin bases reach the suffix arms directly (the cyrillic
/// TS_LANGS pool covers the cyr_to_lat arms).
const SERBIAN_LATIN_BASES: &[&str] = &[
    "postojan", "razmatra", "najjeftin", "devojc", "ucitelj", "zelen", "trcal", "knjig",
];

const STEM_FALLBACK_BASES: &[&str] = &["stemdrainbase"];

/// Real-vocabulary supplements for rule classes a suffix-append sweep
/// cannot reach: PREFIX rules (indonesian me-/pe-/ber-/ter-, arabic
/// prelude), exception-word tables (english r_exception1), morpheme
/// chains gated on vowel harmony (turkish -ki), and conjugation shapes
/// whose left context must be a real stem (romance -issement/-amiento,
/// yiddish ge- participles). Two of these ride along in every stemdrain
/// statement for a language that has a pool.
const STEM_EXTRA_WORDS: &[(&str, &[&str])] = &[
    ("turkish", &[
        "evdekiler", "kitaplardakiler", "onlarınki", "seninkiler", "bendeki",
        "okuldakilerden", "yarınki", "sabahki", "buradakilerin", "ağaçtakiler",
        "gördüklerimden", "yapabileceklerimiz", "gelemeyecekmişsiniz", "koşuyorlardı",
        "arkadaşlarımızdan", "çiçeklerimizin", "üzüntülerinden", "düşüncelerindeki",
    ]),
    ("indonesian", &[
        "mengambil", "penulis", "berlari", "terbaik", "membaca", "pembangunan",
        "kemerdekaan", "perjuangan", "mempermainkan", "keadilan", "berkelanjutan",
        "menyanyikan", "pelajaran", "diambil", "sebaiknya", "memperjuangkan",
    ]),
    ("english", &[
        "skis", "skies", "dying", "lying", "tying", "idly", "gently", "ugly",
        "early", "only", "singly", "sky", "news", "howe", "atlas", "cosmos",
        "bias", "andes", "inning", "outing", "canning", "herring", "earring",
        "proceed", "exceed", "succeed",
    ]),
    ("yiddish", &[
        "געגאנגען", "געשריבן", "אויפגעשטאנען", "פארשטאנען", "אנגעקומען",
        "צוגעהערט", "געלערנטע", "אריבערגעטראגן", "אונטערגענומען", "באקומען",
        "דערציילונגען", "אפגעלאזן", "מיטגעברענגט", "געזונטערהייט",
    ]),
    ("french", &[
        "établissement", "consciencieusement", "amicalement", "administratrice",
        "vieillissement", "attendrissement", "épanouissement", "applaudissements",
        "investissements", "reconnaissance", "assainissement", "affectueusement",
        "chargement", "remerciements", "rajeunissement", "éblouissante",
    ]),
    ("italian", &[
        "avvicinamento", "invecchiamento", "comportamenti", "allontanandosi",
        "ringraziandovi", "trasferimento", "divertimento", "spaventosamente",
        "indipendentemente", "organizzazione", "internazionalizzazione",
        "responsabilità", "impossibilità", "ammodernamento",
    ]),
    ("spanish", &[
        "acercamiento", "envejecimiento", "comportamientos", "alejándose",
        "agradeciéndoselo", "entretenimiento", "espantosamente", "organización",
        "internacionalización", "responsabilidades", "imposibilidad",
        "devolvérmelo", "tráigannoslos", "levantándose",
    ]),
    ("portuguese", &[
        "envelhecimento", "comportamentos", "afastando", "agradecendo",
        "entretenimento", "organização", "internacionalização",
        "responsabilidades", "impossibilidade", "reconhecimento",
        "desenvolvimento", "aproximadamente", "estabelecimentos",
    ]),
    ("greek", &[
        "χρησιμοποιούνται", "αναπτύσσονται", "δημιουργήθηκαν", "εργαζόμενους",
        "περιβάλλοντος", "αποτελέσματα", "χαρακτηριστικά", "πληροφορίες",
        "εκπαιδευτικός", "παρακολούθηση", "συγκεκριμένα", "διαφορετικές",
    ]),
    ("arabic", &[
        "والمكتبات", "فسيكتبونها", "بالمدرسة", "والطالبات", "استخراج",
        "المعلومات", "التطبيقات", "مستشفيات", "وسيذهبون", "بالتعاون",
        "الاستقلال", "المسؤولية",
    ]),
];

/// LD10 residual-vocabulary pools: the line-gap-report-002 snowball
/// residue is context-conditioned morphology — arms whose guard needs a
/// REAL stem to the left of the suffix (grouping predicates, vowel
/// harmony, R1/R2/RV placement, mutation prefixes), which the synthetic
/// suffix-append sweep cannot satisfy. One curated inflected-form pool
/// per residual language, sized to the report's per-language unhit mass
/// (yiddish 126, french 102, turkish 192 across three functions, dutch
/// 45, italian 44, greek 93, german 32, spanish 55, portuguese 50,
/// indonesian 28, irish 24, finnish 18, serbian 45, arabic 41, tamil 16,
/// porter/english flow arms 50). Swept exhaustively by the
/// `tsdl:stemvocab` cursor (16 words/statement).
const STEM_VOCAB: &[(&str, &[&str])] = &[
    ("yiddish", &[
        // ge- participles over the full consonant-cluster prelude classes,
        // plural/diminutive chains (־לעך ,־עלעך), abstract-noun suffixes
        // (־קייט ,־שאפט ,־הייט ,־ניש), agentives and comparatives.
        "געגאנגען", "געקומען", "געזאגט", "געמאכט", "געטראכט", "געפונען",
        "געשטאנען", "געזעסן", "געלעגן", "געהאלטן", "געגעבן", "גענומען",
        "אויסגעצייכנט", "איבערגעזעצט", "אונטערגעשריבן", "אפגערופן",
        "אנגעהויבן", "ארויסגעגאנגען", "אריינגעקומען", "צוזאמענגעשטעלט",
        "מיידעלעך", "קינדערלעך", "ביכעלעך", "שטעטעלעך", "פייגעלעך",
        "הייזעלעך", "בלימעלעך", "שיינקייט", "גרויסקייט", "קליינקייט",
        "איידלקייט", "פריינדשאפט", "לאנדשאפט", "געזעלשאפט", "קינדהייט",
        "פרייהייט", "געזונטהייט", "בענקעניש", "געדעכעניש", "פארשטענדעניש",
        "לערערין", "שרייבערין", "זינגערין", "ארבעטערס", "לערערס",
        "שענסטער", "גרעסטער", "קלענסטער", "יינגסטער", "עלטסטער",
        "געוואקסענע", "צעבראכענע", "פארלוירענע", "אנגעקומענע",
        "דערציילונג", "באדייטונג", "פארבינדונג", "אנטוויקלונג",
        "שרייבנדיק", "לויפנדיק", "זינגענדיק", "טאנצנדיק",
    ]),
    ("french", &[
        // -eaux/-aux plurals, -euse/-euses agentives, -issement/-issante
        // verbal nouns, -icité/-abilité/-ivité abstracta, -atrice, -logie,
        // -usion/-ution, -ences, adverbial -emment/-amment/-ûment.
        "châteaux", "bateaux", "cadeaux", "chapeaux", "tableaux", "journaux",
        "chevaux", "travaux", "généraux", "principaux", "vendeuses",
        "chanteuses", "menteuses", "travailleuses", "danseuses",
        "vieillissements", "agrandissement", "refroidissement",
        "affaiblissement", "élargissement", "appauvrissement",
        "éclaircissement", "avertissements", "divertissements",
        "électricité", "authenticité", "élasticité", "publicité",
        "capacités", "possibilités", "probabilités", "responsabilités",
        "stabilité", "flexibilité", "productivité", "créativité",
        "objectivité", "relativité", "collaboratrice", "organisatrices",
        "éducatrices", "administratrices", "biologies", "technologies",
        "psychologie", "méthodologies", "confusions", "conclusions",
        "solutions", "évolutions", "contributions", "distributions",
        "différences", "conséquences", "influences", "préférences",
        "évidemment", "prudemment", "constamment", "élégamment",
        "brillamment", "assidûment", "goulûment", "continûment",
        "heureusement", "sérieusement", "curieusement", "précieusement",
        "logiquement", "pratiquement", "politiquement", "économiquement",
        "nationaux", "régionaux", "originaux", "amicaux",
    ]),
    ("turkish", &[
        // Possessive+case chains across both harmony classes, -ki chains
        // (bare, doubled, after genitive/locative), aorist/participle
        // verb chains, soft-consonant stems (ğ/b/c/d mutation guards).
        "kitaplarımızdan", "evlerimizden", "arkadaşlarımdan", "çocuklarının",
        "öğretmenlerimizin", "gözlerindeki", "sokaklardaki", "şehirlerdeki",
        "denizlerdeki", "ağaçlardaki", "yüreğimdeki", "aklımdakiler",
        "elimdekiler", "cebimdekilerden", "seninkilerden", "bizimkilerin",
        "onlarınkiler", "dünkü", "bugünkü", "akşamki", "geceki",
        "sabahkinden", "öncekilerden", "sonrakiler", "karşıdakiler",
        "yukarıdakilerden", "aşağıdakiler", "içindekiler", "üstündekiler",
        "altındakilerden", "gelecekteki", "geçmişteki", "gördüklerini",
        "yaptıklarımızdan", "söylediklerinin", "düşündüklerimiz",
        "yazdıklarından", "okuduklarımı", "bildiklerimizden",
        "sevdiklerimize", "gidebileceğimiz", "yapabileceklerini",
        "gelemeyeceğini", "olamayacaklarını", "anlayamadıklarımız",
        "koşuyorlardı", "geliyormuşsunuz", "yapacakmışız", "gitmeliydik",
        "okumalıymışsınız", "kitabını", "ağacını", "rengini", "kalbini",
        "oğlunu", "burnunu", "gönlünü", "şehrini", "vaktini", "resmini",
    ]),
    ("dutch", &[
        // -heden plurals, ge—t/ge—d participles, -ingen, -baar/-lijk,
        // vowel-undoubling stems, -tje diminutives, e-insertion guards.
        "mogelijkheden", "moeilijkheden", "eigenschappen", "aanbiedingen",
        "ontwikkelingen", "verzamelingen", "herinneringen", "opleidingen",
        "gebeurtenissen", "verantwoordelijkheden", "snelheden",
        "gelegenheden", "werkzaamheden", "bezienswaardigheden",
        "gemaakt", "gewerkt", "gehoord", "gespeeld", "geleefd", "gebouwd",
        "verteld", "beloofd", "ontdekt", "herhaald", "huizen", "muren",
        "boten", "wegen", "dieren", "uren", "verhalen", "gebaren",
        "aanvaardbaar", "bereikbaar", "betrouwbaar", "onvermijdelijk",
        "vriendelijk", "gevaarlijk", "wetenschappelijk", "maatschappelijk",
        "kinderen", "eieren", "liederen", "volkeren", "goederen",
        "grootste", "kleinste", "mooiste", "belangrijkste", "sterkste",
    ]),
    ("italian", &[
        // -zione/-sione families, clitic chains (-andosi, -arglielo),
        // -abilità/-ibilità, -issimo superlatives, -mente adverbs.
        "organizzazione", "informazioni", "comunicazioni", "applicazioni",
        "dimostrazione", "considerazioni", "amministrazione",
        "trasformazioni", "conclusioni", "decisioni", "discussioni",
        "impressioni", "professioni", "svegliandosi", "lavandosi",
        "vestendosi", "avvicinandosi", "allontanandosi", "ricordandosene",
        "andandosene", "parlargliene", "spiegarglielo", "portarglieli",
        "dirglielo", "mandargliela", "responsabilità", "disponibilità",
        "possibilità", "sensibilità", "affidabilità", "sostenibilità",
        "bellissimo", "grandissima", "importantissimi", "interessantissime",
        "gentilissimo", "velocissima", "rapidamente", "lentamente",
        "felicemente", "difficilmente", "particolarmente", "sicuramente",
        "probabilmente", "generalmente", "miglioramento", "cambiamento",
        "insegnamento", "funzionamento", "comportamenti", "ragionamenti",
    ]),
    ("greek", &[
        // Verb chains for step5/s6 (aorist passives -ηθήκαμε/-θηκαν,
        // imperfect -ούσαν/-ιόταν, -όμαστε), neuter -ματα, comparatives
        // -ότερος/-ύτερος, diminutives -άκια/-ούλα, -ισμός/-ίστας.
        "χρησιμοποιήθηκαν", "δημιουργήθηκε", "πραγματοποιήθηκαν",
        "παρουσιάστηκαν", "εμφανίστηκαν", "αναπτύχθηκαν", "εξετάστηκαν",
        "ολοκληρώθηκαν", "βρεθήκαμε", "χαθήκατε", "αγαπηθήκαμε",
        "εργαζόμασταν", "ερχόμασταν", "καθόμασταν", "σκεφτόμουν",
        "μιλούσαν", "περπατούσαμε", "τραγουδούσατε", "αγαπούσες",
        "κοιτούσε", "ρωτιόταν", "ετοιμαζόταν", "βρισκόταν", "φαινόταν",
        "αποτελέσματα", "προβλήματα", "συστήματα", "μαθήματα",
        "γράμματα", "χρώματα", "κύματα", "σώματα", "πράγματα",
        "μεγαλύτερος", "καλύτερη", "μικρότερα", "ψηλότεροι",
        "γρηγορότερα", "ομορφότερη", "παιδάκια", "σπιτάκια",
        "τραπεζάκια", "μικρούλα", "κοπελίτσα", "ανθρωπισμός",
        "τουρισμός", "πολιτισμός", "οργανισμοί", "μηχανισμούς",
        "ποδοσφαιριστές", "καλλιτέχνες", "τραγουδίστριες",
    ]),
    ("german", &[
        // -ungen/-keiten/-heiten, -isch(e), -ern/-em adjective endings,
        // ge—t participles, -nis(se), s-linked compounds.
        "entwicklungen", "erfahrungen", "bedingungen", "beziehungen",
        "veränderungen", "entscheidungen", "untersuchungen", "bemühungen",
        "möglichkeiten", "schwierigkeiten", "fähigkeiten",
        "geschwindigkeiten", "persönlichkeiten", "gelegenheiten",
        "krankheiten", "schönheiten", "wahrheiten", "praktisch",
        "theoretische", "politischen", "wissenschaftlichem",
        "wirtschaftlicher", "künstlerischen", "gemacht", "gearbeitet",
        "gesprochen", "geschrieben", "verstanden", "erklärt",
        "ereignisse", "erlebnisse", "verhältnisses", "kenntnissen",
        "größeren", "kleinerem", "besseren", "schnellerem", "längerer",
        "stärksten", "wichtigsten", "häufigsten", "neuesten",
    ]),
    ("spanish", &[
        // Clitic chains (-ándoselo, -érselas), -amiento/-imiento,
        // -ización, -ísimo, subjunctive/conditional verb tails.
        "acercamientos", "descubrimiento", "establecimientos",
        "fortalecimiento", "envejecimiento", "arrepentimiento",
        "levantándose", "acordándose", "despidiéndose", "durmiéndose",
        "explicándoselo", "devolviéndosela", "entregárselos",
        "comprándonoslas", "dándomelo", "diciéndotelo", "organizaciones",
        "globalización", "modernización", "caracterización",
        "generalizaciones", "importantísimo", "grandísima", "riquísimos",
        "facilísimas", "rapidísimo", "hablaríamos", "comeríais",
        "viviríamos", "estudiaríais", "trabajásemos", "hubiéramos",
        "quisiéramos", "pudiésemos", "tuviéramos", "cantaremos",
        "entenderemos", "escribiremos", "felizmente", "rápidamente",
        "cuidadosamente", "tranquilamente", "profundamente",
    ]),
    ("portuguese", &[
        // -amento/-imento, -ização, -íssimo, -ariam/-êssemos verb tails,
        // nasal plurals (-ções, -ães, -ões), -eiro/-eira agentives.
        "desenvolvimentos", "estabelecimento", "envelhecimento",
        "fortalecimento", "conhecimentos", "acontecimentos",
        "comportamento", "relacionamentos", "informações", "organizações",
        "comunicações", "aplicações", "tradições", "multidões",
        "capitães", "alemães", "organização", "globalização",
        "caracterização", "modernização", "importantíssimo",
        "grandíssima", "riquíssimos", "facílimas", "trabalhariam",
        "comeriam", "escreveríamos", "estudaríeis", "falássemos",
        "tivéssemos", "pudéssemos", "quiséssemos", "cantaremos",
        "entenderemos", "felizmente", "rapidamente", "cuidadosamente",
        "profundamente", "brasileiros", "estrangeiras", "verdadeiros",
        "primeiras", "terceiros",
    ]),
    ("indonesian", &[
        // Full prefix matrix: meng-/meny-/mem-/men-/me-, peng-/peny-/pem-,
        // ber-/bel-, ter-, di-, ke—an circumfix, -kan/-i/-nya suffixes.
        "mengambil", "mengerti", "menghitung", "menggambar", "mengukur",
        "menyanyi", "menyapu", "menyimpan", "menyusun", "membaca",
        "membangun", "memberi", "membuat", "menulis", "mendengar",
        "mendapat", "menjadi", "melihat", "merasa", "melempar",
        "pengambilan", "pengertian", "penghitungan", "penyanyi",
        "penyimpanan", "pembacaan", "pembangunan", "pemberian",
        "penulisan", "pendengaran", "berjalan", "berbicara", "bekerja",
        "belajar", "berenang", "bermain", "terambil", "terbesar",
        "terkenal", "tertulis", "diambil", "dibaca", "ditulis",
        "keadilan", "kebersihan", "kemerdekaan", "keindahan",
        "memperjuangkan", "mempermainkan", "menyampaikan", "mengajari",
    ]),
    ("irish", &[
        // Eclipsis (mb/gc/nd/bhf/ng/bp/dt) and lenition initial-morph
        // arms, -acha/-anna plurals, -óir agentives, verbal nouns.
        "mbord", "gcarr", "ndoras", "bhfear", "ngairdín", "bpáirc",
        "dtír", "mbaile", "gceist", "ndeireadh", "bhfuinneog", "ngrian",
        "bplean", "dteach", "n-athair", "n-oíche", "t-uisce", "t-am",
        "hoibre", "haimsire", "cheist", "bhean", "fhear", "mhúinteoir",
        "shamhradh", "thimpeall", "chathair", "gheata", "dhoras",
        "ceisteanna", "scoileanna", "bóithre", "cathracha", "oibreacha",
        "múinteoirí", "feirmeoirí", "ceoltóirí", "scríbhneoirí",
        "foghlaimeoirí", "leabharlanna", "seachtainí", "imeachtaí",
    ]),
    ("finnish", &[
        // Possessive suffixes (-ni/-si/-nsa/-mme/-nne) stacked on case
        // forms, partitive/illative/inessive plurals, -minen nouns.
        "talossani", "autossasi", "kirjassansa", "maassamme", "työssänne",
        "ystävänsä", "perheeni", "koulussamme", "kaupungissanne",
        "elämässään", "ajatuksissaan", "sydämessäni", "käsissäsi",
        "silmissänsä", "taloissa", "autoissa", "kirjoissa", "kaupungeissa",
        "kouluihin", "taloihin", "maihin", "kaupunkeihin", "ihmisiin",
        "opiskeleminen", "lukeminen", "kirjoittaminen", "ajatteleminen",
        "ymmärtäminen", "kehittäminen", "suurempia", "pienempiä",
        "parempien", "vanhimpien", "nuorimmat", "kauneimmat",
    ]),
    ("english", &[
        // Porter2 flow arms: -ization/-ational/-fulness chains, -logi/
        // -izer/-ator, short-word R1 boundaries, -eed/-eedly, -ingly,
        // consonant doubling undone, y-to-i after consonant.
        "generalizations", "internationalization", "characterization",
        "rationalizations", "organizational", "computational",
        "sensational", "operational", "conditionally", "traditionally",
        "exceptionally", "professionally", "hopefulness", "carefulness",
        "usefulness", "thankfulness", "gratefulness", "biologically",
        "technological", "psychologically", "methodological",
        "fertilizer", "organizer", "synthesizer", "generator",
        "administrator", "communicator", "agreed", "agreedly", "freed",
        "guaranteed", "proceeded", "succeeding", "exceedingly",
        "amazingly", "surprisingly", "increasingly", "willingly",
        "knowingly", "hopping", "hopped", "referred", "controlled",
        "beginning", "forgetting", "happier", "happiest", "prettier",
        "loveliest", "worthiness", "dizziness", "emptiness", "laziness",
        "cries", "tries", "flies", "denied", "replied", "studied",
        "conflated", "vindicated", "consolidated", "differentiate",
    ]),
    ("porter", &[
        // Legacy porter flow arms (porter_UTF_8_stem 32): step-2/3/4
        // suffix chains and the m>1 measure guards.
        "relational", "conditional", "rational", "valenci", "hesitanci",
        "digitizer", "conformabli", "radicalli", "differentli", "vileli",
        "analogousli", "vietnamization", "predication", "operator",
        "feudalism", "decisiveness", "hopefulness", "callousness",
        "formaliti", "sensitiviti", "sensibiliti", "triplicate",
        "formative", "formalize", "electriciti", "electrical", "hopeful",
        "goodness", "revival", "allowance", "inference", "airliner",
        "gyroscopic", "adjustable", "defensible", "irritant", "replacement",
        "adjustment", "dependent", "adoption", "homologou", "communism",
        "activate", "angulariti", "homologous", "effective", "bowdlerize",
        "probate", "rate", "cease", "controll", "roll",
    ]),
    ("serbian", &[
        // Latin-script inflected forms for the residual Step_1/2 classes:
        // comparatives, verbal nouns -enje/-anje, adjective case chains,
        // -ovima/-evima plurals, -ijim/-ijima comparatives.
        "najlepšima", "najboljima", "najvećima", "najmanjima",
        "gradovima", "sinovima", "vukovima", "putevima", "krajevima",
        "muzejima", "prijateljima", "učiteljima", "razmišljanje",
        "putovanja", "istraživanjima", "obrazovanje", "poštovanje",
        "verovanja", "očekivanjima", "zadovoljstvo", "prijateljstva",
        "društvima", "srećnijim", "pametnijima", "starijima", "novijim",
        "lakšima", "težima", "jednostavnijima", "zanimljivijima",
        "najzanimljivijima", "devojkama", "knjigama", "školama",
        "pesmama", "zemljama", "stvarima", "rečima", "noćima",
        "radostima", "mogućnostima", "odgovornostima",
    ]),
    ("arabic", &[
        // Conjunction+preposition+article prefix stacks, verb prefix/
        // suffix chains, broken plural + pronoun-suffix tails.
        "وبالمدرسة", "فبالكتاب", "وللطلاب", "فللمعلمين", "وبالتعاون",
        "كالشمس", "فكالقمر", "وكالبحر", "بالمكتبات", "للجامعات",
        "سيكتبون", "ستدرسين", "سنعملها", "ليكتبوها", "فسيقرؤونها",
        "يدرسونها", "تكتبينها", "نفهمها", "أعرفهم", "يعلمونهم",
        "مدارسهم", "كتبهن", "أقلامكم", "بيوتنا", "أصدقائي",
        "معلماتها", "طالباتكن", "مكتباتنا", "استخدامات", "استقبال",
        "انتظارهم", "اجتماعاتنا", "تطبيقاتها", "مؤسساتكم",
    ]),
    ("tamil", &[
        // Case-suffix (vetrumai urupukal) chains: -ஐ/-ஆல்/-க்கு/-இல்/
        // -இன்/-உடன்/-இடம் over noun classes, verb participles.
        "புத்தகத்தை", "மரத்தால்", "வீட்டுக்கு", "பள்ளியில்",
        "மனிதனின்", "நண்பருடன்", "ஆசிரியரிடம்", "குழந்தைகளை",
        "மாணவர்களுக்கு", "நகரங்களில்", "நாடுகளின்", "பறவைகளால்",
        "கண்களிலிருந்து", "வீடுகளிலேயே", "அவனுடைய", "அவளிடமிருந்து",
        "படித்துக்கொண்டு", "எழுதியிருக்கிறான்", "வந்துவிட்டார்கள்",
        "சென்றுகொண்டிருந்தாள்", "பேசிக்கொண்டிருக்கிறார்கள்",
        "செய்யப்பட்டது", "காணப்படுகிறது", "நடைபெற்றது",
    ]),
    // --- W4-STEM lane additions: arm-reaching vocabulary authored against
    // the still-unhit suffix-rule arms of the REL_18_3 snowball C (read-C ->
    // author reaching input; see docs/fuzzing/findings-w4stem.md).  New
    // tuples rather than edits so the LD10 pools stay byte-identical; the
    // sweep visits every tuple.
    ("arabic", &[
        "أأنتم", "أؤمن", "أآمن", "أإذا", "ببساطة", "ببطء", "سأكتب", "سأذهب", "كتابك", "كتابه",
        "كتابي", "كتابكما", "كتابهما", "مدرستكما", "كتبتموه", "رأيتموها", "أخذتموها", "علمتموهم",
        "لأأكل", "وأؤمن", "فأآمن", "بأإذا", "أأكلتم", "أؤكد", "أآخذ", "أإنك", "وببساطة", "فببطء",
        "لسأذهب", "وسأكتب", "فسأقرأ", "كتابكِ", "كتابكَ", "لكتابك", "والكتابه", "مدرستكم",
        "مدرستهن", "مدرستنا", "مدرستها", "قرأتموه", "سمعتموها", "فهمتموهم", "أكلتموهن",
        "ضربتموهما", "أاحمد", "أاخوك", "أاكتب", "أانت", "أإسلام", "أإنسان", "أإكرام", "أإبل",
        "مكتبتك", "مكتبته", "مكتبتي", "مدرستهم", "بيوتكما", "بيوتهما", "سيارتكن", "مدرساتهم",
        "سياراتهن", "مكتباتكم", "جامعاتنا", "معلمتاهما",
    ]),
    ("danish", &[
        "venligst", "hjælpeløst", "kærligst", "farligst", "dejligst", "hurtigst", "hjælpsomst",
        "morsomst", "langsomst", "voldsomst", "ensomst", "alvorsomst", "frygtsomst", "agtsomst",
    ]),
    ("dutch", &[
        "waarheid", "vrijheid", "schoonheid", "gezondheid", "zichtbaar", "eetbaar",
        "dankbaarheid", "gedachten", "krachten", "vluchten", "lichamelijk", "gemeenschappelijk",
        "smeuïg", "ruïne", "beïnvloed", "geïnd", "geëerd", "geëindigd", "gecreëerd", "geërfd",
        "geëvalueerd", "coördinatie", "reëel", "ideeën", "zeeën", "tweeën", "knieën",
        "categorieën", "theorieën", "industrieën", "politiek", "fabriek", "muziek", "techniek",
        "kritiek", "publiek", "uniek", "antiek", "logisch", "tragisch", "typisch", "fysisch",
        "komisch",
    ]),
    ("english", &[
        "'twas", "o'clock", "don't", "cats'", "'ello", "authoritative", "imaginative",
        "generative", "communicative", "cry", "by", "say", "enormously", "effortlessly",
        "proceeding", "exceeding", "communing", "enthralling", "extolling", "vying", "hying",
        "guying", "buying", "commune", "communed", "atlases", "cosmoses", "biases", "gases",
        "gasses",
    ]),
    ("estonian", &[
        "raamatugi", "lapski", "majagi", "seegi", "kappide", "lippude", "kottide", "seppade",
        "majade", "raamatute", "laste", "poiste", "autode", "majadesse", "raamatutesse",
        "lastele", "poistele", "koolidesse", "inimestki", "lehmgi", "tulebki", "läkski", "ongi",
        "polegi", "sepp", "kepp", "lipp", "kott", "pott", "sekk", "pikk", "rikk", "seppa",
        "keppi", "lippu", "kotti", "küll", "kell", "tall", "pall", "kass", "mass", "juss", "kukk",
        "sukk", "tikk", "pukk", "nukk",
    ]),
    ("finnish", &[
        "lyhyet", "kauniit", "tiet", "työt", "yöt", "suot", "maat", "talot", "kirjaani",
        "taloaan", "ystävääni", "kotiinsa", "vapaassa", "maassaan", "taloonsa", "kauneimmassa",
        "tärkeintä", "useimmiten", "kaikkein", "tyttäret", "miehet", "naiset", "lapset", "vuodet",
        "kädet", "veneet", "huoneet", "perheet", "lampaat", "hampaat", "rikkaat", "oppaat",
        "ihanat", "kanat", "sanat", "tavat", "kalat", "talvet", "järvet", "lahdet", "tähdet",
        "kannet", "onnet", "immet", "kymmenet", "askeleet", "kirjeet", "kappaleet", "ihmiset",
        "hampaani", "lampaani", "oppaansa", "rikkaansa", "taloni", "kirjani", "autoni", "maani",
        "puuni", "tieni", "työni", "yöni", "suoni", "hampaissa", "lampaissa", "rikkaissa",
        "oppaissa", "vieraissa", "oikeissa", "suurissa", "pienissä", "nuorissa", "kokouksensa",
        "tyttäreksensä", "kirjaksensa", "hyväksensä", "huonetta", "perhettä", "kirjettä",
        "venettä", "ainetta", "konetta", "talohon", "radiohon", "autohon", "maahan", "päähän",
        "työhön", "tiehen", "suohon", "yöhön", "puuhun", "teehen", "syyhyn", "maiden", "teiden",
        "öiden", "soiden", "puiden", "töiden", "maisiin", "kalliisiin", "vapaisiin", "tehtaisiin",
        "maita", "töitä", "öitä", "puita", "teitä", "soita",
    ]),
    ("french", &[
        "noël", "naïve", "maïs", "canoë", "aïeul", "ambiguë", "haïr", "héroïne", "égoïste",
        "archaïque", "justificative", "qualificatives", "explicatifs", "significatifs",
        "communicative", "multiplicatifs", "applicatives", "indicatifs", "l'homme", "d'accord",
        "qu'elle", "jusqu'à", "s'il", "n'est", "c'était", "presqu'île", "m'appelle", "t'aime",
        "aujourd'hui", "quelqu'un", "inhumaine", "déshumaniser", "bonhomie", "silhouette",
        "souhait", "véhicule", "appréhension", "cohérent", "véhément", "menhir", "dahlia",
        "cahier", "trahir", "envahir", "ébahi", "cohue", "héroïsme", "égoïsme", "archaïsme",
        "judaïsme", "athéisme", "prosaïque", "mosaïque", "stoïque", "naïveté", "aïeux", "païen",
        "haïssable", "ouïe", "inouï", "ambiguïté", "exiguïté", "contiguïté", "ciguë", "aiguë",
        "exiguë", "significance", "significances", "publicance", "fabricances", "multiplicatrice",
        "significatrices", "communicatrices", "essayer", "employer", "nettoyer", "balayer",
        "ennuyer", "appuyer", "essuyer", "tutoyer", "vouvoyer", "l'", "d'", "qu'", "s'", "n'",
        "j'", "m'", "t'", "c'",
    ]),
    ("german", &[
        "beleidigungen", "entschuldigung", "reinigung", "heiligung", "fröhlichkeit", "möglich",
        "herrlich", "täglich", "physik", "musik", "kritik", "politik", "technik", "straße",
        "größe", "füße", "weiß", "heißen", "grüßen", "mißverständnis", "ätzend", "äußerst",
        "öffentlich", "übermäßig", "gemütlichkeit", "tatsächlich", "grundsätzlich", "zusätzlich",
        "ausschließlich", "schließlich", "gründlich", "mündlich", "natürlich", "persönlich",
        "gewöhnlich", "verhältnismäßig", "regelmäßig", "gleichmäßig", "gierig", "gierige",
        "neugierig", "schwierig", "niedrig", "niedrige", "würdig", "würdige", "lebendig",
        "lebendige", "vollständig", "vollständige", "selbständig", "beständige",
    ]),
    ("hungarian", &[
        "házért", "könyvért", "emberként", "házanként", "fáért", "munkáért", "egyenként",
        "percenként", "almát", "körtét", "könyvvé", "vízzé", "házzá", "emberré", "kővé", "fává",
        "naponta", "hetente", "havonta", "évente", "percente", "óránta", "pénzzé", "mézzé",
        "gazzá", "vazzá", "jéggé", "éggé", "lisztté", "tésztává",
    ]),
    ("indonesian", &[
        "mengecat", "mengebom", "memukul", "menari", "penari", "pemukul", "pengecatan",
        "perbuatan", "kebaikan", "memukuli", "menandai", "mengunjungi", "mengevaluasi",
        "mengekspor",
    ]),
    ("italian", &[
        "logica", "pratica", "famosa", "turismo", "amabile", "possibile", "incredibile",
        "attività", "elettricità", "creativa", "informativo", "educative", "abilità", "utilità",
        "velocità", "qualificativa", "significativi", "indicativa", "comunicativo", "divano",
        "divani", "usciere", "uscieri", "comprarglielo", "vendendoglieli", "parlandoci",
        "scrivendovi", "leggendola", "portandoteli", "lucciole", "gucciardini", "sciogliendogli",
        "raccogliendole", "togliendoci", "accogliendovi", "distribuendoglieli",
        "attribuendogliela", "costruendoglielo", "riducendoglieli", "producendogliele",
        "traducendoglielo", "bevendoglielo", "dicendoglielo", "facendoglielo", "traendone",
        "ponendovi", "componendoci", "proponendoglielo",
    ]),
    ("nepali", &[
        "छन्", "गर्छन्", "भएका", "गरेका", "हुन्छन्", "थिएनन्", "गरिन्छ", "भनिन्छ", "गर्नेछन्",
        "जानेछन्", "आएका", "गएका", "खान्छिन्", "जान्छिन्", "गर्छिन्", "हुन्छिन्", "खान्थे",
        "जान्थे", "गर्थे", "हुन्थे", "खाएछ", "गएछ", "भएछ", "आएछ", "खानेछु", "जानेछु", "गर्नेछु",
        "हुनेछु", "गर्नुहुन्छ", "जानुहुन्छ", "खानुहुन्छ", "भन्नुहुन्छ", "गर्नुभयो", "जानुभयो",
        "आउनुभयो", "भन्नुभयो", "गरिएको", "भनिएको", "लेखिएको", "पढिएको",
    ]),
    ("norwegian", &[
        "hetens", "husets", "barnets", "landets", "kjærlighetens", "regjeringens", "sannhetens",
        "mulighetenes", "virksomhetens", "nyhetenes", "sikkerhetens", "hemmelighetens",
        "myndighetenes", "mulighetens", "sannhetenes", "skjønnhetens", "sikkerhetenes",
        "frihetens", "enighetenes",
    ]),
    ("porter", &[
        "troubled", "sized", "tanned", "falling", "hissing", "fizzed", "failing", "filing",
        "crying", "string", "feed", "motoring", "sing", "happy", "poniard", "abed", "shed",
        "bled", "sled", "breed", "treed", "king", "ring", "thing", "bring", "spring", "fly",
        "dry", "shy", "why", "cry", "try", "ply", "sly", "enjoy", "employ", "destroy", "annoy",
        "convey", "survey", "obey", "prey", "say", "day", "they", "buy", "guy", "ripped",
        "matted", "meetings", "feelings", "sufferings", "happenings", "occasional",
        "professional", "irrational", "emotional", "educational", "generational", "international",
        "abilities", "abilitiy", "enjoyed", "employed", "destroyed", "surveyed", "conveyed",
        "obeyed", "preyed", "stayed", "played", "prayed", "swayed",
    ]),
    ("portuguese", &[
        "amabilidade", "possibilidades", "eletricidade", "atividade", "lógica", "prática",
        "famosa", "turismo", "amável", "incrível", "tratamento", "movimento", "dá-lo", "fazê-lo",
        "vendê-las", "parti-lo", "dando-se", "vendo-a", "comprá-los", "dizê-lo", "ouvi-la",
        "pô-lo", "cheguei", "paguei", "seguiu", "consegue", "água", "línguas", "averigúe",
        "argúi", "mão", "cão", "põe", "irmã", "alemã", "órgão", "decorativa", "educativas",
        "informativos", "negativa", "comunicativa", "distribuição", "atribuições", "contribuição",
        "retribuições", "constituição", "substituições", "instituição", "restituição",
        "diminuição", "evolução", "revolução", "solução", "resolução", "execução", "tradução",
        "produção", "redução", "introdução", "construção", "destruição", "seguindo-a",
        "conseguindo-o", "distinguindo-as", "perseguindo-os", "arguindo-se", "delinquindo",
        "significância", "insignificâncias", "elegância", "fragância", "arqueologias",
        "mineralogia", "geologias", "reconhece", "aparece", "cresce", "desce", "padece", "merece",
    ]),
    ("romanian", &[
        "națiunile", "stațiunea", "porțiunilor", "abilitățile", "posibilitățile", "activității",
        "acțiunea", "elasticității", "acțiune", "condiție", "funcții", "noțiuni", "generație",
        "operațiile", "tradiții", "pozițiilor", "ambiție", "ambiguë", "copiii", "fiii",
        "geamgiii", "împărții", "călătorii", "citii", "vorbii", "dormii", "fugii", "veniși",
        "făcuși", "avuși", "dăduși", "stătuși", "băuși",
    ]),
    ("russian", &[
        "вернувшись", "улыбнувшись", "прочитав", "сказавши", "встретившись", "прочитанная",
        "сделанного", "говорящими", "улыбающейся", "написанным", "новейший", "сильнейшая",
        "добрейшее", "величайший", "синии", "армии", "линии", "гении", "далее", "ранее", "более",
        "менее", "быстрее", "сильнее", "красивее", "интереснее", "занимавшись", "поднявшись",
        "собравшись", "оставшись", "добившись", "влюбившись", "задумавшись", "пробившись",
        "евшая", "певшего", "жившими", "бывшей", "плывшим", "нывшее", "знавшая", "державшего",
        "слышавшими", "видевшей", "прочитаемы", "читаемы", "делаемы", "любимы", "гонимы",
        "хранимы", "влекомы", "ведомы", "несомы", "знакомы",
    ]),
    ("spanish", &[
        "leyéndolo", "construyéndola", "oyéndolas", "huyéndonos", "cayéndose", "trayéndomelo",
        "yéndose", "creyéndole", "famosa", "modernista", "esperanza", "amable", "imposible",
        "científico", "turismo", "perezoso", "tratamientos", "sufrimiento", "lógica", "práctica",
        "elegantemente", "amablemente", "posiblemente", "evidentemente", "increíblemente",
        "amabilidad", "electricidad", "actividades", "visibilidad", "sensibilidades",
        "informativa", "educativos", "creativas", "negativo", "decorativas", "busquemos",
        "lleguen", "paguéis", "toquen", "juguemos", "llegué", "pagué", "apagué", "entregué",
        "saqué", "yendo", "oyendo", "cayendo", "leyendo", "huyendo", "trayendo", "creyendo",
        "construyendo", "destruyendo", "atribuyendo", "concluyendo", "influyendo", "sustituyendo",
        "distribuyéndolas", "atribuyéndoselo", "guemos", "lleguemos", "paguemos", "apaguemos",
        "neguemos", "entreguemos", "carguemos", "descarguemos", "llegue", "pague", "apague",
        "juegue", "niegue", "entregue", "cargue", "descargue", "agregue", "daránselo",
        "haránselas", "dénselo", "comprénselo", "hacérselo", "ponérselas", "construírselo",
        "huírse", "oírse", "reírse", "freírlo", "sonreírles", "significancia", "significancias",
        "publicancia", "elegancia", "fragancia", "vagancia",
    ]),
    ("tamil", &[
        "வோகம்", "வொப்பு", "வோட்டு", "வொலி", "ஏதாவது", "ஓரிடம்", "எங்கே", "யாரோ", "அக்காலம்",
        "இப்போது", "உத்தரவு", "அவ்வூர்", "இச்செயல்", "எக்காலம்", "மரங்கள்", "வீடுகள்",
        "புத்தகங்களை", "அவர்களது", "படித்தேன்", "வருவேன்", "சாப்பிட்டோம்", "அவனது", "இவளது",
        "உங்கள்", "வோட்டம்", "வொற்றி", "வோகன்", "வொழுக்கம்", "வீடு", "விளக்கு", "வெற்றி", "வேலை",
        "எவ்வளவோ", "ஏதேனும்", "ஓடினான்", "ஊரெங்கும்", "அத்தனை", "இத்தனை", "எத்தனை", "அந்நாள்",
        "இந்நாள்", "எந்நாள்", "அஃது", "இஃது", "எஃது", "மரத்திலிருந்து", "வீட்டிலிருந்து",
        "அவர்களிடமிருந்து", "புத்தகங்களிலிருந்து", "படிக்கவில்லை", "வரவில்லை", "செய்யவில்லை",
        "போகவில்லை", "படித்துக்கொண்டிருக்கிறேன்", "எழுதிக்கொண்டிருந்தான்", "எவ்வோட்டம்",
        "அவ்வோட்டம்", "இவ்வொழுக்கம்", "அவ்வொலி", "எவ்வொப்பு", "அவ்விளக்கு", "இவ்வீடு",
        "எவ்வெற்றி", "அவ்வேலை", "இவ்வேளை", "எவ்வீதி", "அவ்வீடு", "உவ்வோடு", "எவ்வுலகு",
        "மரங்களுக்காக", "வீடுகளுக்காக", "அவைகளிடமிருந்து", "புத்தகங்களினுடைய", "மனிதர்களிடத்தில்",
        "குழந்தைகளினால்", "பள்ளிகளிலேயே", "நகரங்களிலுள்ள",
    ]),
    ("turkish", &[
        "annelerininki", "babasınınki", "evdekininki", "evdekilerininki", "kapısındaki",
        "odalarındaki", "evdekindeki", "evdekilerindeki", "arabalarınınki", "kitaplarındakiler",
        "çocuklarınınkiler", "bahçedekilerinki", "arkadaşlarınınki", "öğrencilerininki",
        "okulundakilerinki", "masasındakilerden", "elindekininki", "yolundakilerinki",
        "şehirdekilerininki", "denizindekiler", "gözlerindekini", "kalbindekini", "aklındakinden",
        "cebindekinin", "davamınki", "paranınki", "seninkisi", "benimkisi", "bizimkisi",
        "kedininki", "komşununki", "müdürünki", "gülünki", "kapınınki", "suyunki", "köyünki",
        "evdeyken", "okuldayken", "çocukken", "gençken", "öğrenciyken", "hastayken", "hastaydım",
        "hastaydın", "hastaydık", "hastaydınız", "hastaydılar", "zenginsem", "zenginsen",
        "zenginsek", "zenginseniz", "zenginseler", "güzelmişim", "güzelmişsin", "güzelmişiz",
        "güzelmişsiniz", "güzelmişler", "öğretmenim", "öğretmensin", "öğretmeniz",
        "öğretmensiniz", "çocuğum", "çocuksun", "çocuğuz", "çocuksunuz", "iyidir", "iyidirler",
        "kötüdür", "büyüktür", "küçüktürler", "delicesine", "aptalcasına", "çılgıncasına",
        "kahramancasına", "evdeymişsiniz", "okuldaymışız", "hastaymışlar", "buradaymışsınız",
        "hastaymıştı", "zenginmişse", "güzeldiyseniz", "iyiymişsinizdir", "evdekilerimizinki",
        "okuldakilerimizinki", "arabadakilerinizinki", "okuldakininki", "okuldakindeki",
        "yurttakindeki", "köydekindeki", "bahçedekindeki", "sokaktakininki", "gemidekindeki",
        "sonrakindeki", "öncekindeki", "evdekinizinki", "okuldakimizinki", "katalog", "diyalog",
        "psikolog", "jeolog", "monolog", "katalogu", "psikologlar", "ad", "soyad", "adı",
        "soyadı", "adın", "soyadım", "Türkiye'de", "Ankara'nın", "İstanbul'dan", "Ali'nin",
        "İzmir'e", "Ahmet'le", "sözcüklerini", "gözlüğünü", "köylülerden", "önsözünde",
        "gözünüzü", "köyümüzü", "gönlümüzü", "ölümsüz", "gözlerimizdekilerdenmiş", "hastayiz",
        "zenginyız", "okulyüz", "gördünüzse", "öldünüzmü", "çözdükçe", "süründüler", "büyüdünüz",
        "yürüdüğümüz", "hastaymışsınızcasına", "hastaymışımcasına", "hastaymışsıncasına",
        "hastaymışızcasına", "deliymişlercesine", "zenginmişsinizcesine", "hastaymışımdır",
        "hastaymışsındır", "hastaymışızdır", "hastaymışsınızdır", "güzelmişsinizdir", "radyum",
        "alüminyum", "potasyum", "kalsiyum", "stadyum", "akvaryum",
    ]),
    ("yiddish", &[
        "געמיטן", "געביטן", "געביסן", "געליטן", "געריסן", "אלטענעם", "אלטענער", "אלטענע",
        "אלטענס", "גוטענעם", "שיינענער", "קליינענע", "קאמוניסט", "סאציאליסט", "ארטיסט",
        "זשורנאליסט", "געגאנגענער", "גענומענע", "געשריבענעם", "פארשטאנענער", "צוגענומענער",
        "אנגעשריבענע", "וווינען", "ווייסן", "ייִדיש", "בלוילעך", "גרינלעך", "רויטלעך",
        "שוואַרצלעך", "זיסלעך", "טובֿות", "מצוות", "שבתות", "חבֿרטע", "רביצין", "מיטענע",
        "ביטענע", "ביסענע", "וויזענע", "טריבענע", "ליטענע", "קליבענע", "ריבענע", "ריסענע",
        "שוויגענע", "שמיסענע", "שניטענע", "בונדענע", "זונגענע", "טרונקענע", "צווונגענע",
        "שלונגענע", "בויגענע", "הויבענע", "שוווירענע", "נומענע", "גאנגענע", "שריבענע", "שטאנענע",
        "מיטענער", "ביטענעם", "וויזענער", "טרונקענעם", "בויגענער", "לערנען", "קוקן", "זאגן",
        "מאכן", "ברענגען", "בראכטע", "געבראכטע", "יידיש", "ווו", "בלוילעכע", "גרינלעכער",
        "זיסלעכן", "קינדערלעכס", "מיידעלעס", "פייגעלעס", "טישל", "בענקל", "שטעטל", "ביכל",
        "הענטל", "פיסל", "קעפל", "שיפל", "וווּ", "וווּנדער", "באַוווּסט", "צוּם", "בּוּך",
        "יִנגל", "יִד", "ייִנגל", "געלט", "געלטן", "געבן", "געבנדיק", "צוגנעמען", "צוקטן",
        "צוקנען", "בארגיק", "גליק", "בליק", "אויסבליק", "אומגליק", "גליקן", "בליקן", "צוגן",
        "צוקט",
    ]),
];

/// Words per stemvocab statement.
const STEMVOCAB_WORDS: usize = 16;

/// LD10 vocabulary drain: cursor-walk the residual-language pools in
/// `STEM_VOCAB` through the language's stemmer, 16 real inflected forms
/// per statement (porter through the created snowball dictionary, like
/// stemdrain). Exhaustive per full cycle by construction.
fn stemvocab(g: &mut Gen) -> Vec<String> {
    g.fire("tsdl:stemvocab");
    // The sweep covers the curated STEM_VOCAB tuples then the generated
    // STEM_REACH exception-stem products (W4-STEM lane).
    let n_pools = STEM_VOCAB.len() + STEM_REACH.len();
    let li = g.ts.vocab_lang % n_pools;
    let (lang, pool) = if li < STEM_VOCAB.len() {
        STEM_VOCAB[li]
    } else {
        STEM_REACH[li - STEM_VOCAB.len()]
    };
    let mut stmts = Vec::new();
    let dict = if lang == "porter" {
        match g.ts.porter_dict.clone() {
            Some(d) => d,
            None => {
                let name = "fz_stem_porter".to_string();
                stmts.push(format!(
                    "CREATE TEXT SEARCH DICTIONARY {name} (TEMPLATE = snowball, Language = 'porter');"
                ));
                g.ts.porter_dict = Some(name.clone());
                name
            }
        }
    } else {
        format!("{lang}_stem")
    };
    let mut words = Vec::new();
    for _ in 0..STEMVOCAB_WORDS {
        let w = pool[g.ts.vocab_pos];
        // 1/6 case-mangled to uppercase-prefix: the lowercase prelude of
        // several stemmers is its own arm class.
        let w = if g.rng.chance(1, 6) {
            let mut cs = w.chars();
            match cs.next() {
                Some(c) => c.to_uppercase().collect::<String>() + cs.as_str(),
                None => String::new(),
            }
        } else {
            w.to_string()
        };
        words.push(w.replace('\'', "''"));
        g.ts.vocab_pos += 1;
        if g.ts.vocab_pos >= pool.len() {
            g.ts.vocab_pos = 0;
            g.ts.vocab_lang = (li + 1) % n_pools;
            break;
        }
    }
    let arr: Vec<String> = words.iter().map(|w| format!("'{w}'")).collect();
    stmts.push(format!(
        "SELECT o, w, ts_lexize('{}', w)::text FROM unnest(ARRAY[{}]) WITH ORDINALITY t(w, o) ORDER BY o;",
        dict,
        arr.join(",")
    ));
    stmts
}

/// Base word a drain suffix is appended to: usually a real word from the
/// language's pool (R1/R2 nonempty), 1/8 clipped to 3 chars and 1/8
/// empty (the short-word / R1-empty rejection arms are lines too).
fn stem_base(g: &mut Gen, lang: &str) -> String {
    if g.rng.chance(1, 8) {
        return String::new();
    }
    let pool_lang = if lang == "porter" { "english" } else { lang };
    let words: &[&str] = if pool_lang == "serbian" && g.rng.chance(1, 2) {
        SERBIAN_LATIN_BASES
    } else {
        TS_LANGS
            .iter()
            .find(|l| l.cfg == pool_lang)
            .map(|l| l.words)
            .unwrap_or(STEM_FALLBACK_BASES)
    };
    let w = words[g.rng.below_usize(words.len())];
    if g.rng.chance(1, 8) {
        w.chars().take(3).collect()
    } else {
        w.to_string()
    }
}

/// LD3 stemmer drain: walk the language's among-table strings via the
/// session cursor, appending each to a base word, and lexize the batch
/// through the shipped `<lang>_stem` dictionary (porter via a created
/// snowball dictionary). One statement per call; the cursor hands the
/// next call the next slice (rolling into the next language at the end
/// of a table), so a stream of N calls is a deterministic exhaustive
/// sweep, not a coupon-collector hope.
fn stemdrain(g: &mut Gen) -> Vec<String> {
    g.fire("tsdl:stemdrain");
    let li = g.ts.stem_lang % STEM_SUFFIXES.len();
    let entry = &STEM_SUFFIXES[li];
    let mut stmts = Vec::new();
    let dict = if entry.lang == "porter" {
        match g.ts.porter_dict.clone() {
            Some(d) => d,
            None => {
                let name = "fz_stem_porter".to_string();
                stmts.push(format!(
                    "CREATE TEXT SEARCH DICTIONARY {name} (TEMPLATE = snowball, Language = 'porter');"
                ));
                g.ts.porter_dict = Some(name.clone());
                name
            }
        }
    } else {
        format!("{}_stem", entry.lang)
    };
    let mut words = Vec::new();
    for _ in 0..STEMDRAIN_WORDS {
        let suf = entry.suffixes[g.ts.stem_pos];
        let mut w = stem_base(g, entry.lang);
        // 1/4 context pair: another table string BEFORE the cursor suffix
        // — many suffix arms are conditioned on what precedes them (the
        // french -icité / turkish chained-morpheme / spanish -amiento
        // style rules), and the condition strings are themselves among-
        // table entries. The sweep guarantees the tail; the random left
        // context accumulates pair coverage across cycles.
        if g.rng.chance(1, 4) {
            w.push_str(g.rng.pick(entry.suffixes));
        }
        w.push_str(suf);
        // Occasional double suffix AFTER as well: chained-rule arms
        // (turkish stem_suffix_chain_before_ki and friends).
        if g.rng.chance(1, 8) {
            w.push_str(g.rng.pick(entry.suffixes));
        }
        words.push(w.replace('\'', "''"));
        g.ts.stem_pos += 1;
        if g.ts.stem_pos >= entry.suffixes.len() {
            g.ts.stem_pos = 0;
            g.ts.stem_lang = (li + 1) % STEM_SUFFIXES.len();
            break; // next call starts the next language
        }
    }
    // Real-vocabulary riders (prefix rules, exceptions, harmony chains) —
    // drawn from the LD10 residual pools when the language has one, else
    // the original LD3 extras. Four per statement (LD10: the residue is
    // exactly the context-conditioned class the riders exist for).
    let rider_pool = STEM_VOCAB
        .iter()
        .find(|(l, _)| *l == entry.lang)
        .or_else(|| STEM_EXTRA_WORDS.iter().find(|(l, _)| *l == entry.lang));
    if let Some((_, pool)) = rider_pool {
        for _ in 0..4 {
            words.push(pool[g.rng.below_usize(pool.len())].replace('\'', "''"));
        }
    }
    let arr: Vec<String> = words.iter().map(|w| format!("'{w}'")).collect();
    stmts.push(format!(
        "SELECT o, w, ts_lexize('{}', w)::text FROM unnest(ARRAY[{}]) WITH ORDINALITY t(w, o) ORDER BY o;",
        dict,
        arr.join(",")
    ));
    stmts
}

/// CREATE TEXT SEARCH CONFIGURATION (COPY = shipped) + immediate probe.
fn create_cfg(g: &mut Gen) -> Vec<String> {
    g.fire("tsdl:create_cfg");
    let name = g.ts.next_cfg_name();
    let src = g.gen_ts_config();
    let create = format!(
        "CREATE TEXT SEARCH CONFIGURATION {} (COPY = {});",
        name, src
    );
    g.ts.cfgs.push(TsCfg { name: name.clone(), mapped: BASE_MAPPED.to_vec() });
    let doc = probe_doc(g);
    let probe = format!("SELECT (to_tsvector('{}', '{}'))::text;", name, doc);
    vec![create, probe]
}

/// CREATE TEXT SEARCH DICTIONARY + ts_lexize probe. Half the picks are
/// the `simple` template (optional STOPWORDS/ACCEPT), the rest are the
/// file-backed templates over the shipped sample data (the ispell/
/// hunspell affix machinery, the thesaurus phrase matcher, the synonym
/// map and the legacy porter stemmer — none of which any shipped
/// configuration reaches).
fn create_dict(g: &mut Gen) -> Vec<String> {
    g.fire("tsdl:create_dict");
    // Template kind cycles over the counter rather than the PRNG: the
    // live-dictionary cap keeps creates rare (single digits per thousand
    // statements), and a uniform draw left ispell/thesaurus unreached in
    // whole runs — measured, covloop 2026-08-11. The cycle guarantees
    // every template appears within five creates; the PRNG still picks
    // each one's options.
    let kind = g.ts.dict_n % 5;
    let name = g.ts.next_dict_name();
    let simple = kind == 0;
    let opts = if simple {
        let mut o = "TEMPLATE = simple".to_string();
        if g.rng.chance(1, 2) {
            let sw = *g.rng.pick(TS_STOPWORD_LANGS);
            o.push_str(&format!(", STOPWORDS = {}", sw));
        }
        if g.rng.chance(1, 3) {
            let accept = if g.rng.chance(1, 2) { "true" } else { "false" };
            o.push_str(&format!(", ACCEPT = {}", accept));
        }
        o
    } else {
        g.fire("tsdl:dict_filebacked");
        match kind - 1 {
            // Legacy porter stemmer: the one snowball language no shipped
            // configuration selects.
            0 => {
                let lang = if g.rng.chance(1, 3) {
                    g.gen_ts_lang().cfg
                } else {
                    "porter"
                };
                let mut o = format!("TEMPLATE = snowball, Language = '{}'", lang);
                if g.rng.chance(1, 3) {
                    let sw = *g.rng.pick(TS_STOPWORD_LANGS);
                    o.push_str(&format!(", StopWords = {}", sw));
                }
                o
            }
            1 => {
                let f = *g.rng.pick(SAMPLE_ISPELL);
                format!("TEMPLATE = ispell, DictFile = {f}, AffFile = {f}")
            }
            2 => format!(
                "TEMPLATE = thesaurus, DictFile = thesaurus_sample, Dictionary = {}",
                g.gen_ts_lang().stem
            ),
            _ => "TEMPLATE = synonym, SYNONYMS = synonym_sample".to_string(),
        }
    };
    let create = format!("CREATE TEXT SEARCH DICTIONARY {} ({});", name, opts);
    g.ts.dicts.push(TsDict { name: name.clone(), used: false, simple });
    let lang = g.gen_ts_lang();
    let word = lang.words[g.rng.below_usize(lang.words.len())];
    let probe = format!("SELECT ts_lexize('{}', '{}');", name, word);
    vec![create, probe]
}

/// Dictionary name for a mapping's WITH list: a live created dictionary
/// (marked used) or a shipped stemmer/simple.
fn mapping_dict(g: &mut Gen) -> String {
    if !g.ts.dicts.is_empty() && g.rng.chance(1, 3) {
        let i = g.rng.below_usize(g.ts.dicts.len());
        g.ts.dicts[i].used = true;
        g.ts.dicts[i].name.clone()
    } else {
        g.gen_ts_lang().stem.to_string()
    }
}

/// ADD / ALTER / DROP MAPPING on a live config + ts_debug probe. The
/// model's mapped set tracks every change so plain forms stay valid.
fn alter_mapping(g: &mut Gen) -> Vec<String> {
    let ci = g.rng.below_usize(g.ts.cfgs.len());
    let cfg = g.ts.cfgs[ci].name.clone();
    let kind = *g.rng.pick(&["add", "alter", "drop", "drop_if_exists"]);
    let stmt = match kind {
        "add" => {
            let unmapped: Vec<&'static str> = EXTRA_TOKENS
                .iter()
                .chain(BASE_MAPPED.iter())
                .copied()
                .filter(|t| !g.ts.cfgs[ci].mapped.contains(t))
                .collect();
            if unmapped.is_empty() {
                // Everything mapped: fall through to an upsert instead.
                return alter_mapping_upsert(g, ci);
            }
            g.fire("tsdl:map_add");
            let tok = *g.rng.pick(&unmapped);
            let dict = mapping_dict(g);
            g.ts.cfgs[ci].mapped.push(tok);
            format!(
                "ALTER TEXT SEARCH CONFIGURATION {} ADD MAPPING FOR {} WITH {};",
                cfg, tok, dict
            )
        }
        "alter" => return alter_mapping_upsert(g, ci),
        "drop" => {
            if g.ts.cfgs[ci].mapped.is_empty() {
                return alter_mapping_upsert(g, ci);
            }
            g.fire("tsdl:map_drop");
            let wi = g.rng.below_usize(g.ts.cfgs[ci].mapped.len());
            let tok = g.ts.cfgs[ci].mapped.remove(wi);
            format!(
                "ALTER TEXT SEARCH CONFIGURATION {} DROP MAPPING FOR {};",
                cfg, tok
            )
        }
        _ => {
            g.fire("tsdl:map_drop");
            // IF EXISTS: any token type is fair game, mapped or not.
            let all: Vec<&'static str> =
                BASE_MAPPED.iter().chain(EXTRA_TOKENS.iter()).copied().collect();
            let tok = *g.rng.pick(&all);
            g.ts.cfgs[ci].mapped.retain(|t| *t != tok);
            format!(
                "ALTER TEXT SEARCH CONFIGURATION {} DROP MAPPING IF EXISTS FOR {};",
                cfg, tok
            )
        }
    };
    let doc = probe_doc(g);
    let probe = format!("SELECT ts_debug('{}', '{}')::text;", cfg, doc);
    vec![stmt, probe]
}

/// ALTER MAPPING FOR ... WITH — an upsert on both engines (verified: it
/// creates missing mappings), so any 1-2 token types are legal.
fn alter_mapping_upsert(g: &mut Gen, ci: usize) -> Vec<String> {
    g.fire("tsdl:map_alter");
    let cfg = g.ts.cfgs[ci].name.clone();
    let all: Vec<&'static str> =
        BASE_MAPPED.iter().chain(EXTRA_TOKENS.iter()).copied().collect();
    let mut toks = vec![*g.rng.pick(&all)];
    if g.rng.chance(1, 3) {
        let t2 = *g.rng.pick(&all);
        if t2 != toks[0] {
            toks.push(t2);
        }
    }
    let dict = mapping_dict(g);
    for t in &toks {
        if !g.ts.cfgs[ci].mapped.contains(t) {
            g.ts.cfgs[ci].mapped.push(t);
        }
    }
    let stmt = format!(
        "ALTER TEXT SEARCH CONFIGURATION {} ALTER MAPPING FOR {} WITH {};",
        cfg,
        toks.join(", "),
        dict
    );
    let doc = probe_doc(g);
    let probe = format!("SELECT (to_tsvector('{}', '{}'))::text;", cfg, doc);
    vec![stmt, probe]
}

/// ALTER TEXT SEARCH DICTIONARY (option surgery on a live simple dict —
/// the file-backed templates reject STOPWORDS/ACCEPT, so they are only
/// ever re-probed, never altered).
fn alter_dict(g: &mut Gen) -> Vec<String> {
    g.fire("tsdl:alter_dict");
    let simple_idx: Vec<usize> = g
        .ts
        .dicts
        .iter()
        .enumerate()
        .filter(|(_, d)| d.simple)
        .map(|(i, _)| i)
        .collect();
    if simple_idx.is_empty() {
        return vec![lexize_probe(g)];
    }
    let i = simple_idx[g.rng.below_usize(simple_idx.len())];
    let name = g.ts.dicts[i].name.clone();
    let opt = if g.rng.chance(1, 2) {
        let sw = *g.rng.pick(TS_STOPWORD_LANGS);
        format!("STOPWORDS = {}", sw)
    } else {
        let accept = if g.rng.chance(1, 2) { "true" } else { "false" };
        format!("ACCEPT = {}", accept)
    };
    let stmt = format!("ALTER TEXT SEARCH DICTIONARY {} ({});", name, opt);
    let lang = g.gen_ts_lang();
    let word = lang.words[g.rng.below_usize(lang.words.len())];
    let probe = format!("SELECT ts_lexize('{}', '{}');", name, word);
    vec![stmt, probe]
}

/// COMMENT ON a live config or dict (the object-address path for TS
/// object classes).
fn comment(g: &mut Gen) -> Vec<String> {
    g.fire("tsdl:comment");
    let on_dict = !g.ts.dicts.is_empty() && g.rng.chance(1, 2);
    let (kind, name) = if on_dict {
        let i = g.rng.below_usize(g.ts.dicts.len());
        ("DICTIONARY", g.ts.dicts[i].name.clone())
    } else {
        let i = g.rng.below_usize(g.ts.cfgs.len());
        ("CONFIGURATION", g.ts.cfgs[i].name.clone())
    };
    let text = if g.rng.chance(1, 4) { "NULL".to_string() } else { "'ts probe object'".to_string() };
    vec![format!("COMMENT ON TEXT SEARCH {} {} IS {};", kind, name, text)]
}

/// RENAME a live config or dict to a fresh counter name (generic-rename
/// path for TS object classes; mappings track oids, so renaming a used
/// dictionary is safe).
fn rename(g: &mut Gen) -> Vec<String> {
    g.fire("tsdl:rename");
    let on_dict = !g.ts.dicts.is_empty() && g.rng.chance(1, 2);
    if on_dict {
        let i = g.rng.below_usize(g.ts.dicts.len());
        let old = g.ts.dicts[i].name.clone();
        let new = g.ts.next_dict_name();
        g.ts.dicts[i].name = new.clone();
        vec![format!("ALTER TEXT SEARCH DICTIONARY {} RENAME TO {};", old, new)]
    } else {
        let i = g.rng.below_usize(g.ts.cfgs.len());
        let old = g.ts.cfgs[i].name.clone();
        let new = g.ts.next_cfg_name();
        g.ts.cfgs[i].name = new.clone();
        vec![format!("ALTER TEXT SEARCH CONFIGURATION {} RENAME TO {};", old, new)]
    }
}

/// DROP a live config (mappings die with it), an unused dict, or an
/// IF EXISTS no-op on a never-created name.
fn drop_obj(g: &mut Gen) -> Vec<String> {
    g.fire("tsdl:drop");
    if g.rng.chance(1, 6) {
        // IF EXISTS over a name the counters will never mint again.
        let kind = if g.rng.chance(1, 2) { "CONFIGURATION" } else { "DICTIONARY" };
        return vec![format!("DROP TEXT SEARCH {} IF EXISTS fz_ts_never;", kind)];
    }
    let droppable: Vec<usize> = g
        .ts
        .dicts
        .iter()
        .enumerate()
        .filter(|(_, d)| !d.used)
        .map(|(i, _)| i)
        .collect();
    // At the dictionary cap, prefer dropping one: creates are the only
    // way new templates enter the stream, and a full dictionary table
    // would freeze the template cycle.
    let at_cap = g.ts.dicts.len() >= MAX_LIVE_DICTS;
    if !droppable.is_empty() && (at_cap || g.rng.chance(1, 3)) {
        let i = droppable[g.rng.below_usize(droppable.len())];
        let name = g.ts.dicts.remove(i).name;
        return vec![format!("DROP TEXT SEARCH DICTIONARY {};", name)];
    }
    let i = g.rng.below_usize(g.ts.cfgs.len());
    let name = g.ts.cfgs.remove(i).name;
    vec![format!("DROP TEXT SEARCH CONFIGURATION {};", name)]
}

// ===================================================================
// Q7 tsearch-funcs raw families (sql-reachable-queue GEN-GAP-SIBLING
// chunk, 90 remaining fns after gap-010): tsvector/tsquery operator and
// function breadth over FIXED literals — the tsdl DDL machinery above
// owns configs/dictionaries; this section owns the value-level surface
// (tsvector_op.c, tsquery_op.c, tsquery_rewrite.c, tsquery_util.c,
// to_tsany.c, wparser.c, ts_stat, tsvector_update_trigger, the
// like_support prosupport family, and CREATE TEXT SEARCH PARSER over the
// core prsd_* functions). Hand-verified deck: docs/fuzzing/
// deck-q7-tsearch.sql (both engines, see findings-q7.md).
//
// Compare-safety: every probe is a fixed-literal expression cast ::text
// (tsvector/tsquery output ordering is lexeme-sorted and deterministic),
// SRF probes (unnest/ts_parse/ts_token_type) emit rows in defined
// document/lexeme order, and ts_stat always takes an outer ORDER BY
// word COLLATE "C", ndoc, nentry. ts_rank/ts_rank_cd are float4 sums
// over per-row fixed inputs in fixed order — no plan-dependent
// accumulation — and ride ::text. Deliberate error probes (tsfx:errlit)
// are matched-error findings fuel, never one-sided by construction.
// ===================================================================

const TSFX_SHAPES: &[&str] = &[
    "tsfx:cast",
    "tsfx:vecops",
    "tsfx:qryops",
    "tsfx:match",
    "tsfx:rewrite",
    "tsfx:headline",
    "tsfx:parse",
    "tsfx:stat",
    "tsfx:trigger",
    "tsfx:parserddl",
    "tsfx:analyze",
    "tsfx:likesup",
    "tsfx:errlit",
];

/// Registry entry point for the tsearch-funcs families (dispatched from
/// gen_tsdl_module under the tsdl:tsfx arm).
pub fn gen_tsfx_stmts(g: &mut Gen) -> Vec<String> {
    g.fire("tsdl:tsfx");
    let shape = g.weights.pick(g.rng, TSFX_SHAPES);
    g.fire(shape);
    match shape {
        "tsfx:cast" => tsfx_cast(g),
        "tsfx:vecops" => tsfx_vecops(g),
        "tsfx:qryops" => tsfx_qryops(g),
        "tsfx:match" => tsfx_match(g),
        "tsfx:rewrite" => tsfx_rewrite(g),
        "tsfx:headline" => tsfx_headline(g),
        "tsfx:parse" => tsfx_parse(g),
        "tsfx:stat" => tsfx_stat(g),
        "tsfx:trigger" => tsfx_trigger(g),
        "tsfx:parserddl" => tsfx_parserddl(g),
        "tsfx:analyze" => tsfx_analyze(g),
        "tsfx:likesup" => tsfx_likesup(g),
        "tsfx:errlit" => tsfx_errlit(g),
        other => unreachable!("unknown tsfx shape {other}"),
    }
}

/// Small fixed word pools (ASCII + one multibyte pool). All C-locale /
/// UTF8-safe; no quotes, parens, or backslashes (module test invariants).
const TSFX_WORDS: &[&str] = &["cat", "cats", "dog", "run", "running", "fat", "rat", "jump"];
const TSFX_MB: &[&str] = &["h\u{e9}llo", "w\u{f6}rld", "na\u{ef}ve", "\u{fc}ber"];

fn tpick<'x>(g: &mut Gen, opts: &[&'x str]) -> &'x str {
    opts[g.rng.below_usize(opts.len())]
}

fn tsfx_word(g: &mut Gen) -> &'static str {
    tpick(g, TSFX_WORDS)
}

/// A fixed tsvector literal: 2-4 lexemes, some with multi-position lists
/// and weight labels (compareWordEntryPos + weight parsing).
fn tsfx_vec_lit(g: &mut Gen) -> String {
    let n = 2 + g.rng.below_usize(3);
    let mut parts = Vec::with_capacity(n);
    for k in 0..n {
        let w = TSFX_WORDS[(g.rng.below_usize(TSFX_WORDS.len()) + k) % TSFX_WORDS.len()];
        match g.rng.below(4) {
            0 => parts.push(format!("{w}:{}", 1 + g.rng.below(20))),
            1 => parts.push(format!(
                "{w}:{},{}A,{}B",
                1 + g.rng.below(9),
                10 + g.rng.below(9),
                20 + g.rng.below(9)
            )),
            2 => parts.push(format!("{w}:{}C", 1 + g.rng.below(20))),
            _ => parts.push(w.to_string()),
        }
    }
    format!("'{}'::tsvector", parts.join(" "))
}

/// A fixed tsquery literal over the same pool: &, |, !, <-> and <N>.
fn tsfx_qry_lit(g: &mut Gen) -> String {
    let a = tsfx_word(g);
    let b = tsfx_word(g);
    let q = match g.rng.below(6) {
        0 => format!("{a} & {b}"),
        1 => format!("{a} | !{b}"),
        2 => format!("{a} <-> {b}"),
        3 => format!("{a} <{}> {b}", 1 + g.rng.below(4)),
        4 => format!("{a}:A & {b}:BC"),
        _ => format!("{a} & {b} | {a}"),
    };
    format!("'{q}'::tsquery")
}

/// to_tsvector / to_tsquery family: explicit-config 2-arg forms, the
/// single-arg forms under a pinned default_text_search_config
/// (get_current_ts_config / getTSCurrentConfig), and direct casts.
fn tsfx_cast(g: &mut Gen) -> Vec<String> {
    let cfg = tpick(g, &["english", "simple"]);
    let doc = format!("{} {} and the {}", tsfx_word(g), tsfx_word(g), tsfx_word(g));
    let w = tsfx_word(g);
    match g.rng.below(6) {
        0 => vec![format!(
            "SELECT to_tsvector('{cfg}', '{doc}')::text, to_tsquery('{cfg}', '{w}')::text;"
        )],
        1 => vec![format!(
            "SELECT plainto_tsquery('{cfg}', '{doc}')::text, phraseto_tsquery('{cfg}', '{doc}')::text;"
        )],
        2 => vec![format!(
            "SELECT websearch_to_tsquery('{cfg}', '{} or {} -{}')::text;",
            tsfx_word(g),
            tsfx_word(g),
            tsfx_word(g)
        )],
        3 => vec![
            format!("SET default_text_search_config = 'pg_catalog.{cfg}';"),
            format!(
                "SELECT to_tsvector('{doc}')::text, to_tsquery('{w}')::text, plainto_tsquery('{doc}')::text;"
            ),
            format!(
                "SELECT phraseto_tsquery('{doc}')::text, websearch_to_tsquery('{doc}')::text;"
            ),
            "RESET default_text_search_config;".to_string(),
        ],
        4 => vec![format!(
            "SELECT '{} {}'::tsvector::text, ('{} & {}')::tsquery::text;",
            tsfx_word(g),
            tsfx_word(g),
            tsfx_word(g),
            tsfx_word(g)
        )],
        _ => vec![format!(
            "SELECT to_tsvector('{cfg}', '{} v1.2.3 {} 12.5 a4b'::text)::text;",
            tsfx_word(g),
            tsfx_word(g)
        )],
    }
}

/// tsvector value ops breadth (tsvector_op.c): || comparisons setweight
/// ts_delete ts_filter strip length tsvector_to_array array_to_tsvector
/// unnest (DatumGetTSVectorCopy riders).
fn tsfx_vecops(g: &mut Gen) -> Vec<String> {
    let v = tsfx_vec_lit(g);
    let v2 = tsfx_vec_lit(g);
    let w = tsfx_word(g);
    match g.rng.below(9) {
        0 => vec![format!("SELECT ({v} || {v2})::text, length({v}), length({v2});")],
        1 => vec![format!(
            "SELECT {v} < {v2}, {v} <= {v2}, {v} = {v2}, {v} <> {v2}, {v} >= {v2}, {v} > {v2};"
        )],
        2 => vec![format!(
            "SELECT setweight({v}, 'A')::text, setweight({v2}, 'D')::text, strip({v})::text;"
        )],
        3 => vec![format!(
            "SELECT setweight({v}, 'B', ARRAY['{w}', 'cats'])::text;"
        )],
        4 => vec![format!(
            "SELECT ts_delete({v}, '{w}')::text, ts_delete({v2}, ARRAY['{w}', 'dog'])::text;"
        )],
        5 => vec![format!(
            "SELECT ts_filter(setweight({v}, 'A', ARRAY['{w}']), ARRAY['A']::\"char\"[])::text;"
        )],
        6 => vec![format!(
            "SELECT tsvector_to_array({v}), array_to_tsvector(ARRAY['{w}', 'dog', 'ant'])::text;"
        )],
        7 => vec![format!(
            "SELECT v::text FROM (VALUES ({v}), ({v2}), ('ant'::tsvector)) t(v) ORDER BY v;"
        )],
        _ => vec![format!(
            "SELECT lexeme, positions::text, weights::text FROM unnest({v});"
        )],
    }
}

/// tsquery value ops (tsquery_op.c / tsquery_util.c): full comparison
/// row (CompareTSQ), <@ / @> containment (tsq_mcontained/mcontains),
/// numnode, && || !! <-> composition, tsquery_phrase.
fn tsfx_qryops(g: &mut Gen) -> Vec<String> {
    let q = tsfx_qry_lit(g);
    let q2 = tsfx_qry_lit(g);
    match g.rng.below(6) {
        0 => vec![format!(
            "SELECT {q} < {q2}, {q} <= {q2}, {q} = {q2}, {q} <> {q2}, {q} >= {q2}, {q} > {q2};"
        )],
        1 => vec![format!("SELECT {q} <@ {q2}, {q} @> {q2}, numnode({q}), numnode({q2});")],
        2 => vec![format!("SELECT ({q} && {q2})::text, ({q} || {q2})::text, (!!{q})::text;")],
        3 => vec![format!(
            "SELECT ({q} <-> {q2})::text, tsquery_phrase({q}, {q2}, {})::text;",
            1 + g.rng.below(5)
        )],
        4 => vec![format!(
            "SELECT q::text FROM (VALUES ({q}), ({q2}), ('ant'::tsquery)) t(q) ORDER BY q;"
        )],
        _ => vec![format!(
            "SELECT numnode('{} & !{}'::tsquery), numnode('{}'::tsquery);",
            tsfx_word(g),
            tsfx_word(g),
            tsfx_word(g)
        )],
    }
}

/// Match-operator variants (ts_match_qv/tq/tt + vq) and ts_rank /
/// ts_rank_cd (fixed inputs, fixed order; float4 riders ::text).
fn tsfx_match(g: &mut Gen) -> Vec<String> {
    let v = tsfx_vec_lit(g);
    let q = tsfx_qry_lit(g);
    let a = tsfx_word(g);
    let b = tsfx_word(g);
    match g.rng.below(5) {
        0 => vec![format!("SELECT {v} @@ {q}, {q} @@ {v};")],
        1 => vec![format!(
            "SELECT '{a} and the {b}'::text @@ '{b}'::tsquery, '{a} {b} run'::text @@ '{a} & run'::text;"
        )],
        2 => vec![format!("SELECT ts_rank({v}, {q})::text, ts_rank_cd({v}, {q})::text;")],
        3 => vec![format!(
            "SELECT ts_rank(ARRAY[0.2, 0.3, 0.5, 0.9]::float4[], {v}, {q})::text, ts_rank({v}, {q}, {})::text;",
            tpick(g, &["0", "1", "2", "4", "8", "16", "32"])
        )],
        _ => vec![format!(
            "SELECT ts_rank_cd(ARRAY[0.1, 0.2, 0.4, 1.0]::float4[], {v}, {q}, {})::text;",
            tpick(g, &["0", "1", "2", "4", "8", "14"])
        )],
    }
}

/// ts_rewrite forms (tsquery_rewrite.c + the QTN tree internals): the
/// 3-argument form and the SELECT-string form (SPI_gettypeid).
fn tsfx_rewrite(g: &mut Gen) -> Vec<String> {
    let a = tsfx_word(g);
    let b = tsfx_word(g);
    let c = tsfx_word(g);
    match g.rng.below(3) {
        0 => vec![format!(
            "SELECT ts_rewrite('{a} & {b}'::tsquery, '{a}'::tsquery, '{c} | {a}'::tsquery)::text;"
        )],
        1 => vec![format!(
            "SELECT ts_rewrite('{a} <-> {b}'::tsquery, '{b}'::tsquery, '{c}'::tsquery)::text;"
        )],
        _ => vec![
            "DROP TABLE IF EXISTS fz_q7tsal CASCADE;".to_string(),
            "CREATE TABLE fz_q7tsal (t tsquery, s tsquery);".to_string(),
            format!(
                "INSERT INTO fz_q7tsal VALUES ('{a}'::tsquery, '{c} | {b}'::tsquery), ('{b} & {c}'::tsquery, '{a}'::tsquery);"
            ),
            format!(
                "SELECT ts_rewrite('{a} & {b} & {c}'::tsquery, 'SELECT t, s FROM fz_q7tsal')::text;"
            ),
            "DROP TABLE fz_q7tsal CASCADE;".to_string(),
        ],
    }
}

/// ts_headline breadth (wparser.c + wparser_def.c headline machinery):
/// default and option-string forms, multibyte documents (pg_wchar2mb /
/// pg_wchar_strlen), HTML-tag text (TPS_InTag), version tokens
/// (TPS_InSVerVersion).
fn tsfx_headline(g: &mut Gen) -> Vec<String> {
    let cfg = tpick(g, &["english", "simple"]);
    let a = tsfx_word(g);
    let b = tsfx_word(g);
    match g.rng.below(7) {
        0 => vec![format!(
            "SELECT ts_headline('{cfg}', 'the {a} sat on the {b} mat', '{a}'::tsquery);"
        )],
        6 => vec![
            format!("SET default_text_search_config = 'pg_catalog.{cfg}';"),
            format!(
                "SELECT ts_headline('the {a} sat near the {b}', '{a}'::tsquery), ts_headline('the {a} and the {b}', '{b}'::tsquery, 'MaxWords=5, MinWords=1');"
            ),
            "RESET default_text_search_config;".to_string(),
        ],
        1 => vec![format!(
            "SELECT ts_headline('{cfg}', 'the {a} and the {b} ran far away today', '{a} & {b}'::tsquery, 'StartSel=<<, StopSel=>>, MaxWords=6, MinWords=2');"
        )],
        2 => vec![format!(
            "SELECT ts_headline('{cfg}', 'one {a} two {b} three {a} four {b} five', '{a}'::tsquery, 'MaxFragments=2, FragmentDelimiter=+, ShortWord=2');"
        )],
        3 => vec![format!(
            "SELECT ts_headline('simple', '{} {} {} {}', '{}'::tsquery, 'HighlightAll=true');",
            TSFX_MB[0], TSFX_MB[1], TSFX_MB[2], TSFX_MB[3], TSFX_MB[0]
        )],
        4 => vec![format!(
            "SELECT ts_headline('{cfg}', '<a href=x.png>{a} inside a tag</a> and {b} outside', '{a} | {b}'::tsquery);"
        )],
        _ => vec![format!(
            "SELECT ts_headline('{cfg}', 'release v1.2.3 of {a} and 2.0.1rc1 of {b}', '{a}'::tsquery);"
        )],
    }
}

/// ts_parse / ts_token_type by-name and by-oid (wparser.c) plus a
/// multibyte parse (SRF rows in document order — deterministic).
fn tsfx_parse(g: &mut Gen) -> Vec<String> {
    let a = tsfx_word(g);
    match g.rng.below(4) {
        0 => vec![format!(
            "SELECT tokid, token FROM ts_parse('default', 'the {a} v1.2 ran to x.org fast');"
        )],
        1 => vec![
            "SELECT tokid, alias, description FROM ts_token_type('default');".to_string(),
        ],
        2 => vec![format!(
            "SELECT tokid, token FROM ts_parse('default', '{} 12 {} 3.14 a@b.com');",
            TSFX_MB[0], TSFX_MB[3]
        )],
        _ => vec![format!(
            "SELECT tokid, token FROM ts_parse(3722, 'oid form {a} works too');"
        )],
    }
}

/// ts_stat over a small fixed tsvector table (ts_stat1/ts_stat2 +
/// the stat-entry tree walkers), always under a total ORDER BY.
fn tsfx_stat(g: &mut Gen) -> Vec<String> {
    let weights = tpick(g, &["", "ab", "d"]);
    let stat = if weights.is_empty() {
        "SELECT word, ndoc, nentry FROM ts_stat('SELECT v FROM fz_q7tst') ORDER BY word COLLATE \"C\", ndoc, nentry;".to_string()
    } else {
        format!(
            "SELECT word, ndoc, nentry FROM ts_stat('SELECT v FROM fz_q7tst', '{weights}') ORDER BY word COLLATE \"C\", ndoc, nentry;"
        )
    };
    vec![
        "DROP TABLE IF EXISTS fz_q7tst CASCADE;".to_string(),
        "CREATE TABLE fz_q7tst (v tsvector);".to_string(),
        format!(
            "INSERT INTO fz_q7tst SELECT setweight(to_tsvector('simple', 'w' || (n % 7)::text || ' cat shared'), (CASE WHEN n % 3 = 0 THEN 'A' ELSE 'D' END)::\"char\") FROM generate_series(1, {}) n;",
            20 + g.rng.below(20)
        ),
        stat,
        "DROP TABLE fz_q7tst CASCADE;".to_string(),
    ]
}

/// tsvector_update_trigger / _column brackets: BEFORE INSERT OR UPDATE
/// trigger maintaining a tsvector column, fired by INSERT + UPDATE.
fn tsfx_trigger(g: &mut Gen) -> Vec<String> {
    let bycol = g.rng.below(2) == 0;
    let a = tsfx_word(g);
    let b = tsfx_word(g);
    let mut v = vec![
        "DROP TABLE IF EXISTS fz_q7tsg CASCADE;".to_string(),
        "CREATE TABLE fz_q7tsg (id int, title text, body text, cfg regconfig, v tsvector);"
            .to_string(),
    ];
    if bycol {
        v.push(
            "CREATE TRIGGER fz_q7tsg_upd BEFORE INSERT OR UPDATE ON fz_q7tsg FOR EACH ROW EXECUTE FUNCTION tsvector_update_trigger_column(v, cfg, title, body);"
                .to_string(),
        );
        v.push(format!(
            "INSERT INTO fz_q7tsg VALUES (1, 'the {a}', 'a {b} body', 'pg_catalog.english', NULL);"
        ));
    } else {
        v.push(
            "CREATE TRIGGER fz_q7tsg_upd BEFORE INSERT OR UPDATE ON fz_q7tsg FOR EACH ROW EXECUTE FUNCTION tsvector_update_trigger(v, 'pg_catalog.english', title, body);"
                .to_string(),
        );
        v.push(format!(
            "INSERT INTO fz_q7tsg VALUES (1, 'the {a}', 'a {b} body', NULL, NULL);"
        ));
    }
    v.push(format!("UPDATE fz_q7tsg SET body = 'now a {a} tale' WHERE id = 1;"));
    v.push("SELECT id, v::text FROM fz_q7tsg ORDER BY id;".to_string());
    v.push("DROP TABLE fz_q7tsg CASCADE;".to_string());
    v
}

/// CREATE TEXT SEARCH PARSER over the core prsd_* functions
/// (DefineTSParser / get_ts_parser_func / makeParserDependencies), a
/// probe through it, a config bound to it, and the cascade drop.
fn tsfx_parserddl(g: &mut Gen) -> Vec<String> {
    let a = tsfx_word(g);
    let with_headline = g.rng.below(2) == 0;
    let hl = if with_headline { ", HEADLINE = prsd_headline" } else { "" };
    vec![
        "DROP TEXT SEARCH PARSER IF EXISTS fz_q7prs CASCADE;".to_string(),
        format!(
            "CREATE TEXT SEARCH PARSER fz_q7prs (START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype{hl});"
        ),
        format!("SELECT tokid, token FROM ts_parse('fz_q7prs', 'the {a} reparses fine');"),
        "CREATE TEXT SEARCH CONFIGURATION fz_q7prscfg (PARSER = fz_q7prs);".to_string(),
        "ALTER TEXT SEARCH CONFIGURATION fz_q7prscfg ADD MAPPING FOR asciiword WITH simple;"
            .to_string(),
        format!("SELECT to_tsvector('fz_q7prscfg', 'the {a} maps words')::text;"),
        "DROP TEXT SEARCH PARSER fz_q7prs CASCADE;".to_string(),
    ]
}

/// ANALYZE over a tsvector column with enough distinct lexemes to sort
/// MCELEM candidates (ts_typanalyze / trackitem_compare_frequencies_desc).
/// Statistics content is never compared — the ANALYZE itself is the
/// coverage; the probe afterwards is a plain deterministic count.
fn tsfx_analyze(g: &mut Gen) -> Vec<String> {
    let rows = 320 + g.rng.below(120);
    vec![
        "DROP TABLE IF EXISTS fz_q7tsan CASCADE;".to_string(),
        "CREATE TABLE fz_q7tsan (v tsvector);".to_string(),
        format!(
            "INSERT INTO fz_q7tsan SELECT to_tsvector('simple', 'common w' || (n % 23)::text || ' x' || (n % 11)::text || ' y' || n::text) FROM generate_series(1, {rows}) n;"
        ),
        "ANALYZE fz_q7tsan;".to_string(),
        "SELECT count(*) FROM fz_q7tsan;".to_string(),
        "DROP TABLE fz_q7tsan CASCADE;".to_string(),
    ]
}

/// LIKE / regex / starts_with in an indexable WHERE over a btree
/// text_pattern_ops index (the like_support prosupport index-clause
/// family: text_starts_with_support, texticlike_support,
/// texticregexeq_support, textregexeq_support).
fn tsfx_likesup(g: &mut Gen) -> Vec<String> {
    let pfx = tpick(g, &["ca", "do", "ru"]);
    let probe = match g.rng.below(5) {
        0 => format!("SELECT count(*) FROM fz_q7tsl WHERE t LIKE '{pfx}%';"),
        1 => format!("SELECT count(*) FROM fz_q7tsl WHERE t ~ '^{pfx}';"),
        2 => format!("SELECT count(*) FROM fz_q7tsl WHERE starts_with(t, '{pfx}');"),
        3 => format!("SELECT count(*) FROM fz_q7tsl WHERE t ILIKE '{pfx}%';"),
        _ => format!("SELECT count(*) FROM fz_q7tsl WHERE t ~* '^{pfx}';"),
    };
    vec![
        "DROP TABLE IF EXISTS fz_q7tsl CASCADE;".to_string(),
        "CREATE TABLE fz_q7tsl (t text);".to_string(),
        "INSERT INTO fz_q7tsl SELECT w || n::text FROM generate_series(1, 30) n, unnest(ARRAY['cat', 'dog', 'run', 'car']) w;"
            .to_string(),
        "CREATE INDEX fz_q7tsl_i ON fz_q7tsl (t text_pattern_ops);".to_string(),
        "SET enable_seqscan = off;".to_string(),
        probe,
        "RESET enable_seqscan;".to_string(),
        "DROP TABLE fz_q7tsl CASCADE;".to_string(),
    ]
}

/// Malformed tsvector/tsquery literal error paths (prssyntaxerror and
/// the tsquery parser error family) — deliberate matched errors.
fn tsfx_errlit(g: &mut Gen) -> Vec<String> {
    let bad = tpick(
        g,
        &[
            "SELECT 'cat:'::tsvector;",
            "SELECT 'cat:0'::tsvector;",
            "SELECT 'cat:1X'::tsvector;",
            // NOT sprayed: SELECT ''::tsquery — a NOTICE, not an error, and
            // a KNOWN divergence (B omits the LINE-cursor context on the
            // notice; findings-q7.md F1). Kept in the hand deck only.
            "SELECT 'cat &'::tsquery;",
            "SELECT '& cat'::tsquery;",
            "SELECT 'cat <x> dog'::tsquery;",
            "SELECT 'cat:1,'::tsvector;",
        ],
    );
    vec![bad.to_string()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Replays the session-loop contract: one Gen per group, TsState
    /// swapped across groups.
    fn run_groups(seed: u64, groups: usize) -> Vec<String> {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(seed);
        let mut ts = TsState::new();
        let mut out = Vec::new();
        for _ in 0..groups {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            std::mem::swap(&mut g.ts, &mut ts);
            let stmts = gen_tsdl_module(&mut g);
            std::mem::swap(&mut g.ts, &mut ts);
            for s in stmts {
                out.push(s.to_sql());
            }
        }
        out
    }

    #[test]
    fn tsdl_stream_is_deterministic_and_wellformed() {
        let a = run_groups(41, 400);
        let b = run_groups(41, 400);
        assert_eq!(a, b);
        assert_ne!(a, run_groups(42, 400));
        for sql in &a {
            assert!(sql.ends_with(';'), "{sql}");
            assert!(!sql.contains('\n'), "{sql}");
            assert_eq!(sql.matches('(').count(), sql.matches(')').count(), "{sql}");
            // Even number of single quotes: every literal closed.
            assert_eq!(sql.matches('\'').count() % 2, 0, "{sql}");
        }
        // The full production family fires across 400 groups.
        let all = a.join("\n");
        for frag in [
            "CREATE TEXT SEARCH CONFIGURATION",
            "CREATE TEXT SEARCH DICTIONARY",
            "ADD MAPPING FOR",
            "ALTER MAPPING FOR",
            "DROP MAPPING",
            "COMMENT ON TEXT SEARCH",
            "RENAME TO",
            "DROP TEXT SEARCH",
            "ts_debug(",
            "ts_lexize(",
            "to_tsvector(",
        ] {
            assert!(all.contains(frag), "{frag} never generated");
        }
    }

    /// Every dictionary template must appear — the coverage this module
    /// exists for (ispell/thesaurus reach 9k otherwise-dead regions), and
    /// the property a uniform PRNG draw silently failed to deliver.
    #[test]
    fn tsdl_generates_every_dictionary_template() {
        let all = run_groups(53, 400).join("\n");
        for tmpl in ["= simple", "= snowball", "= ispell", "= thesaurus", "= synonym"] {
            assert!(all.contains(&format!("TEMPLATE {}", tmpl)), "{tmpl} never generated");
        }
        assert!(all.contains("Language = 'porter'"), "porter stemmer never generated");
        // File names are only ever the shipped samples.
        for line in all.lines().filter(|l| l.contains("DictFile")) {
            assert!(
                SAMPLE_ISPELL.iter().any(|f| line.contains(*f))
                    || line.contains("thesaurus_sample"),
                "unshipped dictionary file: {line}"
            );
        }
    }

    /// Every tsfx family fires under a boosted tsdl:tsfx weight, and the
    /// value-level surface the chunk exists for all appears in-stream.
    #[test]
    fn tsfx_generates_every_family() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let mut spec = String::from("tsdl:tsfx=100");
        for s in TSFX_SHAPES {
            spec.push_str(&format!(",{s}=5"));
        }
        let w = WeightTable::parse(&spec).unwrap();
        let mut rng = Rng::new(61);
        let mut ts = TsState::new();
        let mut fired: Vec<String> = Vec::new();
        let mut all = String::new();
        for _ in 0..800 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            std::mem::swap(&mut g.ts, &mut ts);
            let stmts = gen_tsdl_module(&mut g);
            std::mem::swap(&mut g.ts, &mut ts);
            fired.extend(prods);
            for s in stmts {
                all.push_str(&s.to_sql());
                all.push('\n');
            }
        }
        for shape in TSFX_SHAPES {
            assert!(
                fired.iter().any(|p| p == shape),
                "tsfx family {shape} never fired"
            );
        }
        for frag in [
            "plainto_tsquery(",
            "phraseto_tsquery(",
            "websearch_to_tsquery(",
            "SET default_text_search_config",
            "setweight(",
            "ts_delete(",
            "ts_filter(",
            "tsvector_to_array(",
            "array_to_tsvector(",
            "unnest(",
            "numnode(",
            "tsquery_phrase(",
            " <@ ",
            " @@ ",
            "ts_rank(",
            "ts_rank_cd(",
            "ts_rewrite(",
            "ts_headline(",
            "ts_parse(",
            "ts_token_type(",
            "ts_stat(",
            "tsvector_update_trigger(",
            "tsvector_update_trigger_column(",
            "CREATE TEXT SEARCH PARSER",
            "ANALYZE fz_q7tsan",
            "text_pattern_ops",
            "starts_with(",
        ] {
            assert!(all.contains(frag), "tsfx surface {frag:?} never generated");
        }
    }

    #[test]
    fn tsdl_never_references_dropped_objects() {
        // Model check: replay the stream against a name-liveness model.
        let stmts = run_groups(43, 600);
        let mut live: Vec<String> = Vec::new();
        for sql in &stmts {
            // tsfx brackets are self-contained fixed-name fixtures
            // (fz_q7...) outside the counter-name liveness model.
            if sql.contains("fz_q7") {
                continue;
            }
            if let Some(rest) = sql
                .strip_prefix("CREATE TEXT SEARCH CONFIGURATION ")
                .or_else(|| sql.strip_prefix("CREATE TEXT SEARCH DICTIONARY "))
            {
                let name = rest.split([' ', '(']).next().unwrap().to_string();
                assert!(!live.contains(&name), "name reuse: {name}");
                live.push(name);
            } else if sql.starts_with("DROP TEXT SEARCH") && !sql.contains("IF EXISTS") {
                let name = sql.trim_end_matches(';').rsplit(' ').next().unwrap().to_string();
                let pos = live.iter().position(|n| *n == name);
                assert!(pos.is_some(), "drop of unknown object: {sql}");
                live.remove(pos.unwrap());
            } else if sql.contains("RENAME TO") {
                let words: Vec<&str> = sql.trim_end_matches(';').split(' ').collect();
                let old = words[words.len() - 4].to_string();
                let new = words[words.len() - 1].to_string();
                let pos = live.iter().position(|n| *n == old);
                assert!(pos.is_some(), "rename of unknown object: {sql}");
                live[pos.unwrap()] = new;
            } else {
                // Probes / ALTERs referencing created names must be live.
                for n in sql.split(['\'', ' ', '(', ')', ',', ';']) {
                    if n.starts_with("fz_tscfg") || n.starts_with("fz_tsdict") {
                        assert!(
                            live.contains(&n.to_string()),
                            "reference to non-live {n}: {sql}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn tsdl_add_mapping_never_duplicates() {
        // ADD MAPPING targets must be unmapped at that point (a duplicate
        // is a 23505 on both engines — modeled, not fuzzed).
        let stmts = run_groups(47, 600);
        use std::collections::{BTreeMap, BTreeSet};
        let mut mapped: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for sql in &stmts {
            // tsfx parser-DDL brackets create fresh-parser configs whose
            // mapping set is empty (no COPY semantics) — outside this model.
            if sql.contains("fz_q7") {
                continue;
            }
            let s = sql.trim_end_matches(';');
            if let Some(rest) = s.strip_prefix("CREATE TEXT SEARCH CONFIGURATION ") {
                let name = rest.split(' ').next().unwrap().to_string();
                mapped.insert(
                    name,
                    BASE_MAPPED.iter().map(|t| t.to_string()).collect(),
                );
            } else if s.contains(" ADD MAPPING FOR ") {
                let cfg = s.split(' ').nth(4).unwrap().to_string();
                let tok = s.split(" FOR ").nth(1).unwrap().split(' ').next().unwrap();
                let set = mapped.get_mut(&cfg).expect("add on unknown cfg");
                assert!(set.insert(tok.to_string()), "duplicate ADD {tok} on {cfg}");
            } else if s.contains(" ALTER MAPPING FOR ") {
                let cfg = s.split(' ').nth(4).unwrap().to_string();
                let toks = s.split(" FOR ").nth(1).unwrap().split(" WITH ").next().unwrap();
                let set = mapped.get_mut(&cfg).expect("alter on unknown cfg");
                for t in toks.split(", ") {
                    set.insert(t.to_string());
                }
            } else if s.contains(" DROP MAPPING IF EXISTS FOR ") {
                let cfg = s.split(' ').nth(4).unwrap().to_string();
                let tok = s.rsplit(' ').next().unwrap();
                if let Some(set) = mapped.get_mut(&cfg) {
                    set.remove(tok);
                }
            } else if s.contains(" DROP MAPPING FOR ") {
                let cfg = s.split(' ').nth(4).unwrap().to_string();
                let tok = s.rsplit(' ').next().unwrap();
                let set = mapped.get_mut(&cfg).expect("drop on unknown cfg");
                assert!(set.remove(tok), "plain DROP of unmapped {tok} on {cfg}");
            } else if s.starts_with("DROP TEXT SEARCH CONFIGURATION") && !s.contains("IF EXISTS")
            {
                let name = s.rsplit(' ').next().unwrap();
                mapped.remove(name);
            } else if s.contains("CONFIGURATION") && s.contains("RENAME TO") {
                let words: Vec<&str> = s.split(' ').collect();
                let old = words[words.len() - 4];
                let new = words[words.len() - 1].to_string();
                if let Some(set) = mapped.remove(old) {
                    mapped.insert(new, set);
                }
            }
        }
    }
}
