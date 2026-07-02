// Bison 2.3 automaton constants shared with gram.c's yyparse skeleton.
pub const YYEMPTY: i32 = -2;
pub const YYEOF: i32 = 0;
pub const YYUNDEFTOK: i32 = 2;
pub const YYMAXDEPTH: usize = 10000;

include!(concat!(env!("OUT_DIR"), "/tables.rs"));

pub mod names {
    include!(concat!(env!("OUT_DIR"), "/names.rs"));
}

const _: () = {
    assert!(YYPACT.len() == YYNSTATES as usize);
    assert!(YYR2.len() == YYNRULES + 1);
};

#[inline]
pub fn yytranslate(yychar: i32) -> i32 {
    if (0..=YYMAXUTOK).contains(&yychar) {
        YYTRANSLATE[yychar as usize] as i32
    } else {
        YYUNDEFTOK
    }
}
