// pl_comp.c + the pl_funcs.c namespace stack, phase-1 subset.
// Unported louds (named at their sites): %ROWTYPE, cword %TYPE beyond
// var/2-3-part type names, composite/record declarations, trigger compile,
// polymorphic argument resolution, OUT/INOUT/TABLE arg modes.
use types_core::{Oid, OidIsValid};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_UNDEFINED_OBJECT, ERROR,
};

use crate::ast::*;
use crate::scanner::{CwordRes, IdentifierLookup, PLcword, PLwdatum, PLword, WordRes, WordResolver};

pub const PLPGSQL_RESOLVE_ERROR: i32 = 0;
pub const PLPGSQL_RESOLVE_VARIABLE: i32 = 1;
pub const PLPGSQL_RESOLVE_COLUMN: i32 = 2;

const TYPTYPE_PSEUDO: i8 = b'p' as i8;

std::thread_local! {
    static NEXT_EXPR_ID: core::cell::Cell<u32> = const { core::cell::Cell::new(1) };
}

pub struct CompState {
    pub datums: Vec<PlDatum>,
    pub ns: Vec<NsItem>,
    pub ns_top: i32,
    pub identifier_lookup: IdentifierLookup,
    pub expr_ids: Vec<u32>,
    pub nstatements: u32,
    pub resolve_option: i32,
    pub print_strict_params: bool,
    datums_last: usize,
}

impl CompState {
    pub fn new() -> CompState {
        CompState {
            datums: Vec::new(),
            ns: Vec::new(),
            ns_top: -1,
            identifier_lookup: IdentifierLookup::Normal,
            expr_ids: Vec::new(),
            nstatements: 0,
            resolve_option: PLPGSQL_RESOLVE_ERROR,
            print_strict_params: false,
            datums_last: 0,
        }
    }

    // plpgsql_add_initdatums (pl_comp.c): datums added since the last call.
    pub fn add_initdatums(&mut self) -> Vec<Dno> {
        let mut out = Vec::new();
        for i in self.datums_last..self.datums.len() {
            if matches!(self.datums[i], PlDatum::Var(_) | PlDatum::Rec(_)) {
                out.push(i as Dno);
            }
        }
        self.datums_last = self.datums.len();
        out
    }

    // plpgsql_ns_push with the label-kind marker in itemno.
    pub fn ns_push_label(&mut self, label: Option<&str>, label_kind: i32) {
        self.ns.push(NsItem {
            itemtype: NsType::Label,
            itemno: label_kind,
            name: label.unwrap_or("").to_string(),
            prev: self.ns_top,
        });
        self.ns_top = self.ns.len() as i32 - 1;
    }

    // plpgsql_ns_find_nearest_loop.
    pub fn ns_has_loop_label(&self, mut cur: i32) -> bool {
        while cur >= 0 {
            let item = &self.ns[cur as usize];
            if item.itemtype == NsType::Label && item.itemno == crate::gram::LABEL_LOOP {
                return true;
            }
            cur = item.prev;
        }
        false
    }

    pub fn new_expr_id(&mut self) -> u32 {
        let id = NEXT_EXPR_ID.with(|c| {
            let v = c.get();
            c.set(v + 1);
            v
        });
        self.expr_ids.push(id);
        id
    }

    // plpgsql_ns_pop.
    pub fn ns_pop(&mut self) {
        let mut cur = self.ns_top;
        while cur >= 0 {
            let item = &self.ns[cur as usize];
            let prev = item.prev;
            if item.itemtype == NsType::Label {
                self.ns_top = prev;
                return;
            }
            cur = prev;
        }
        panic!("ns_pop: stack underflow");
    }

    // plpgsql_ns_additem.
    pub fn ns_additem(&mut self, itemtype: NsType, itemno: i32, name: &str) {
        self.ns.push(NsItem {
            itemtype,
            itemno,
            name: name.to_string(),
            prev: self.ns_top,
        });
        self.ns_top = self.ns.len() as i32 - 1;
    }

    // plpgsql_ns_lookup; returns (ns index, names_used).
    pub fn ns_lookup(
        &self,
        mut cur: i32,
        localmode: bool,
        name1: &str,
        name2: Option<&str>,
        name3: Option<&str>,
    ) -> Option<(i32, i32)> {
        while cur >= 0 {
            let mut i = cur;
            loop {
                let item = &self.ns[i as usize];
                if item.itemtype == NsType::Label {
                    break;
                }
                if item.name == name1
                    && (name2.is_none() || item.itemtype != NsType::Var)
                {
                    return Some((i, 1));
                }
                i = item.prev;
            }
            let label = &self.ns[i as usize];
            if let Some(n2) = name2 {
                if label.name == name1 {
                    let mut j = cur;
                    loop {
                        let item = &self.ns[j as usize];
                        if item.itemtype == NsType::Label {
                            break;
                        }
                        if item.name == n2
                            && (name3.is_none() || item.itemtype != NsType::Var)
                        {
                            return Some((j, 2));
                        }
                        j = item.prev;
                    }
                }
            }
            if localmode {
                break;
            }
            cur = label.prev;
        }
        None
    }

    // plpgsql_ns_lookup_label.
    pub fn ns_lookup_label(&self, mut cur: i32, name: &str) -> Option<i32> {
        while cur >= 0 {
            let item = &self.ns[cur as usize];
            if item.itemtype == NsType::Label && item.name == name {
                return Some(cur);
            }
            cur = item.prev;
        }
        None
    }

    // plpgsql_build_datatype (via lsyscache instead of an open pg_type tuple).
    pub fn build_datatype(typoid: Oid, typmod: i32, collation: Oid) -> PgResult<PlType> {
        let (typlen, typbyval) = lsyscache::typ::get_typlenbyval(typoid)?;
        let typtype = lsyscache::typ::get_typtype(typoid)?;
        let typcollation = lsyscache::typ::get_typcollation(typoid)?;
        let coll = if OidIsValid(collation) { collation } else { typcollation };
        let (typinput, typioparam) = lsyscache::typ::getTypeInputInfo(typoid)?;
        let elem = lsyscache::typ::get_element_type(typoid)?;
        let ttype = if typtype == TYPTYPE_PSEUDO {
            TypeKind::Pseudo
        } else {
            TypeKind::Scalar
        };
        Ok(PlType {
            typoid,
            ttype,
            typlen,
            typbyval,
            typtype,
            collation: coll,
            typisarray: OidIsValid(elem),
            atttypmod: typmod,
            typinput,
            typioparam,
        })
    }

    // plpgsql_build_variable, scalar arm; Row/Rec arms are separate builders.
    pub fn build_variable(
        &mut self,
        refname: &str,
        lineno: i32,
        datatype: PlType,
        add2namespace: bool,
    ) -> PgResult<Dno> {
        if datatype.ttype == TypeKind::Pseudo {
            return Err(comp_err(
                ERRCODE_FEATURE_NOT_SUPPORTED,
                format!(
                    "variable \"{refname}\" has pseudo-type {}",
                    format_type::format_type_be(datatype.typoid)?
                ),
            ));
        }
        let dno = self.datums.len() as Dno;
        self.datums.push(PlDatum::Var(PlVar {
            dno,
            refname: refname.to_string(),
            lineno,
            datatype,
            isconst: false,
            notnull: false,
            default_val: None,
        }));
        if add2namespace {
            self.ns_additem(NsType::Var, dno, refname);
        }
        Ok(dno)
    }

    pub fn build_rec(&mut self, refname: &str, lineno: i32, add2namespace: bool) -> Dno {
        let dno = self.datums.len() as Dno;
        self.datums.push(PlDatum::Rec(PlRec {
            dno,
            refname: refname.to_string(),
            lineno,
        }));
        if add2namespace {
            self.ns_additem(NsType::Rec, dno, refname);
        }
        dno
    }

    pub fn build_row(&mut self, refname: &str, lineno: i32, varnos: Vec<Dno>) -> Dno {
        let dno = self.datums.len() as Dno;
        let fieldnames = varnos
            .iter()
            .map(|&v| self.datums[v as usize].refname().to_string())
            .collect();
        self.datums.push(PlDatum::Row(PlRow {
            dno,
            refname: refname.to_string(),
            lineno,
            fieldnames,
            varnos,
        }));
        dno
    }

    // plpgsql_build_recfield: reuse an existing recfield for the same name.
    pub fn build_recfield(&mut self, recno: Dno, fieldname: &str) -> Dno {
        for d in &self.datums {
            if let PlDatum::RecField(f) = d {
                if f.recparentno == recno && f.fieldname == fieldname {
                    return f.dno;
                }
            }
        }
        let dno = self.datums.len() as Dno;
        self.datums.push(PlDatum::RecField(PlRecField {
            dno,
            recparentno: recno,
            fieldname: fieldname.to_string(),
        }));
        dno
    }

    // plpgsql_parse_wordtype (%TYPE on a bare variable name).
    pub fn parse_wordtype(&self, ident: &str) -> PgResult<PlType> {
        if let Some((idx, _)) = self.ns_lookup(self.ns_top, false, ident, None, None) {
            let item = &self.ns[idx as usize];
            if item.itemtype == NsType::Var {
                if let PlDatum::Var(v) = &self.datums[item.itemno as usize] {
                    return Ok(v.datatype.clone());
                }
            }
        }
        Err(comp_err(
            ERRCODE_UNDEFINED_OBJECT,
            format!("variable \"{ident}\" does not exist"),
        ))
    }

    // plpgsql_parse_cwordtype (table.column%TYPE / schema.table.column%TYPE).
    pub fn parse_cwordtype(&self, idents: &[String]) -> PgResult<PlType> {
        panic!(
            "plpgsql_parse_cwordtype (pl_comp.c): qualified %TYPE ({}) unported — \
             unit backend-pl-plpgsql-comp",
            idents.join(".")
        );
    }
}

impl Default for CompState {
    fn default() -> Self {
        Self::new()
    }
}

#[cold]
pub fn comp_err(code: types_error::SqlState, msg: String) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(code)
            .errmsg(msg)
            .into_error(),
    )
}

#[cold]
pub fn duplicate_declaration(name: &str) -> Box<PgError> {
    comp_err(
        ERRCODE_DUPLICATE_OBJECT,
        format!("duplicate declaration at or near \"{name}\""),
    )
}

impl WordResolver for CompState {
    fn parse_word(&mut self, word1: &str, yytxt: &str, lookup: bool) -> PgResult<WordRes> {
        if lookup && self.identifier_lookup == IdentifierLookup::Normal {
            if let Some((idx, _)) = self.ns_lookup(self.ns_top, false, word1, None, None) {
                let item = &self.ns[idx as usize];
                match item.itemtype {
                    NsType::Var | NsType::Rec => {
                        return Ok(WordRes::Datum(PLwdatum {
                            dno: item.itemno,
                            ident: word1.to_string(),
                            quoted: yytxt.starts_with('"'),
                            idents: Vec::new(),
                        }));
                    }
                    _ => {}
                }
            }
        }
        Ok(WordRes::Word(PLword {
            ident: word1.to_string(),
            quoted: yytxt.starts_with('"'),
        }))
    }

    fn parse_dblword(&mut self, word1: &str, word2: &str) -> PgResult<CwordRes> {
        let idents = vec![word1.to_string(), word2.to_string()];
        if self.identifier_lookup != IdentifierLookup::Declare {
            if let Some((idx, nnames)) = self.ns_lookup(self.ns_top, false, word1, Some(word2), None)
            {
                let item = &self.ns[idx as usize];
                match item.itemtype {
                    NsType::Var => {
                        return Ok(CwordRes::Datum(PLwdatum {
                            dno: item.itemno,
                            ident: String::new(),
                            quoted: false,
                            idents,
                        }));
                    }
                    NsType::Rec => {
                        let dno = if nnames == 1 {
                            let recno = item.itemno;
                            self.build_recfield(recno, word2)
                        } else {
                            item.itemno
                        };
                        return Ok(CwordRes::Datum(PLwdatum {
                            dno,
                            ident: String::new(),
                            quoted: false,
                            idents,
                        }));
                    }
                    _ => {}
                }
            }
        }
        Ok(CwordRes::Cword(PLcword { idents }))
    }

    fn parse_tripword(&mut self, word1: &str, word2: &str, word3: &str) -> PgResult<CwordRes> {
        let idents = vec![word1.to_string(), word2.to_string(), word3.to_string()];
        if self.identifier_lookup != IdentifierLookup::Declare {
            // C looks up word1.word2 requiring nnames == 2 (label.rec.field).
            if let Some((idx, 2)) =
                self.ns_lookup(self.ns_top, false, word1, Some(word2), Some(word3))
            {
                let item = &self.ns[idx as usize];
                if item.itemtype == NsType::Rec {
                    let dno = self.build_recfield(item.itemno, word3);
                    return Ok(CwordRes::Datum(PLwdatum {
                        dno,
                        ident: String::new(),
                        quoted: false,
                        idents,
                    }));
                }
            }
        }
        Ok(CwordRes::Cword(PLcword { idents }))
    }

    fn identifier_lookup(&self) -> IdentifierLookup {
        self.identifier_lookup
    }
}
