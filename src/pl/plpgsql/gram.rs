use crate::backend::parser::{ParseError, parse_type_name};

use super::ast::{Block, RaiseLevel, Stmt, VarDecl};

pub fn parse_block(sql: &str) -> Result<Block, ParseError> {
    let mut parser = Parser::new(sql);
    let block = parser.parse_block()?;
    parser.skip_ws();
    if !parser.is_eof() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of plpgsql block",
            actual: parser.remaining().into(),
        });
    }
    Ok(block)
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn remaining(&self) -> &'a str {
        &self.input[self.pos..]
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.input.len()
    }

    fn parse_block(&mut self) -> Result<Block, ParseError> {
        let mut declarations = Vec::new();
        if self.consume_keyword("declare") {
            loop {
                self.skip_ws();
                if self.peek_keyword("begin") {
                    break;
                }
                declarations.push(self.parse_decl()?);
            }
        }
        self.expect_keyword("begin")?;
        let statements = self.parse_statements_until(&["end"])?;
        self.expect_keyword("end")?;
        Ok(Block {
            declarations,
            statements,
        })
    }

    fn parse_decl(&mut self) -> Result<VarDecl, ParseError> {
        let name = self.parse_identifier()?;
        let ty_text = self.read_until_any(&[";"], &[":="])?;
        let default_expr = if self.consume_symbol(":=") {
            Some(self.read_until_any(&[";"], &[])?)
        } else {
            None
        };
        self.expect_symbol(";")?;
        Ok(VarDecl {
            name,
            ty: parse_type_name(ty_text.trim())?,
            default_expr: default_expr.map(|expr| expr.trim().to_string()),
        })
    }

    fn parse_statements_until(&mut self, end_keywords: &[&str]) -> Result<Vec<Stmt>, ParseError> {
        let mut statements = Vec::new();
        loop {
            self.skip_ws();
            if self.is_eof() || end_keywords.iter().any(|kw| self.peek_keyword(kw)) {
                break;
            }
            statements.push(self.parse_stmt()?);
        }
        Ok(statements)
    }

    fn parse_stmt(&mut self) -> Result<Stmt, ParseError> {
        if self.peek_keyword("declare") || self.peek_keyword("begin") {
            let block = self.parse_block()?;
            self.expect_symbol(";")?;
            return Ok(Stmt::Block(block));
        }
        if self.consume_keyword("null") {
            self.expect_symbol(";")?;
            return Ok(Stmt::Null);
        }
        if self.consume_keyword("if") {
            return self.parse_if();
        }
        if self.consume_keyword("for") {
            return self.parse_for_int();
        }
        if self.consume_keyword("raise") {
            return self.parse_raise();
        }
        self.parse_assignment()
    }

    fn parse_assignment(&mut self) -> Result<Stmt, ParseError> {
        let name = self.parse_identifier()?;
        self.expect_symbol(":=")?;
        let expr = self.read_until_any(&[";"], &[])?;
        self.expect_symbol(";")?;
        Ok(Stmt::Assign {
            name,
            expr: expr.trim().to_string(),
        })
    }

    fn parse_if(&mut self) -> Result<Stmt, ParseError> {
        let mut branches = Vec::new();
        let condition = self.read_until_keyword("then")?;
        self.expect_keyword("then")?;
        let body = self.parse_statements_until(&["elsif", "else", "end"])?;
        branches.push((condition.trim().to_string(), body));

        while self.consume_keyword("elsif") {
            let condition = self.read_until_keyword("then")?;
            self.expect_keyword("then")?;
            let body = self.parse_statements_until(&["elsif", "else", "end"])?;
            branches.push((condition.trim().to_string(), body));
        }

        let else_branch = if self.consume_keyword("else") {
            self.parse_statements_until(&["end"])?
        } else {
            Vec::new()
        };

        self.expect_keyword("end")?;
        self.expect_keyword("if")?;
        self.expect_symbol(";")?;
        Ok(Stmt::If {
            branches,
            else_branch,
        })
    }

    fn parse_for_int(&mut self) -> Result<Stmt, ParseError> {
        let var_name = self.parse_identifier()?;
        self.expect_keyword("in")?;
        let start_expr = self.read_until_any(&[], &[".."])?;
        self.expect_symbol("..")?;
        let end_expr = self.read_until_keyword("loop")?;
        self.expect_keyword("loop")?;
        let body = self.parse_statements_until(&["end"])?;
        self.expect_keyword("end")?;
        self.expect_keyword("loop")?;
        self.expect_symbol(";")?;
        Ok(Stmt::ForInt {
            var_name,
            start_expr: start_expr.trim().to_string(),
            end_expr: end_expr.trim().to_string(),
            body,
        })
    }

    fn parse_raise(&mut self) -> Result<Stmt, ParseError> {
        let level = if self.consume_keyword("notice") {
            RaiseLevel::Notice
        } else if self.consume_keyword("warning") {
            RaiseLevel::Warning
        } else if self.consume_keyword("exception") {
            RaiseLevel::Exception
        } else {
            RaiseLevel::Exception
        };
        self.skip_ws();
        let message = self.parse_sql_literal()?;
        let mut params = Vec::new();
        while self.consume_symbol(",") {
            let expr = self.read_until_any(&[";", ","], &[])?;
            params.push(expr.trim().to_string());
            if self.peek_symbol(";") {
                break;
            }
        }
        self.expect_symbol(";")?;
        Ok(Stmt::Raise {
            level,
            message,
            params,
        })
    }

    fn parse_sql_literal(&mut self) -> Result<String, ParseError> {
        self.skip_ws();
        let start = self.pos;
        if self.remaining().starts_with('\'')
            || self.remaining().starts_with("E'")
            || self.remaining().starts_with("e'")
            || self.remaining().starts_with('$')
        {
            self.skip_sql_token();
            return Ok(self.input[start..self.pos].to_string());
        }
        Err(ParseError::UnexpectedToken {
            expected: "SQL string literal",
            actual: self.remaining().into(),
        })
    }

    fn parse_identifier(&mut self) -> Result<String, ParseError> {
        self.skip_ws();
        let bytes = self.input.as_bytes();
        let start = self.pos;
        if self.pos >= bytes.len() {
            return Err(ParseError::UnexpectedEof);
        }
        if bytes[self.pos] == b'"' {
            self.pos += 1;
            while self.pos < bytes.len() {
                if bytes[self.pos] == b'"' {
                    if self.pos + 1 < bytes.len() && bytes[self.pos + 1] == b'"' {
                        self.pos += 2;
                    } else {
                        self.pos += 1;
                        break;
                    }
                } else {
                    self.pos += 1;
                }
            }
            return Ok(self.input[start + 1..self.pos - 1].replace("\"\"", "\""));
        }
        let ch = bytes[self.pos] as char;
        if !(ch.is_ascii_alphabetic() || ch == '_') {
            return Err(ParseError::UnexpectedToken {
                expected: "identifier",
                actual: self.remaining().into(),
            });
        }
        self.pos += 1;
        while self.pos < bytes.len() {
            let ch = bytes[self.pos] as char;
            if ch.is_ascii_alphanumeric() || ch == '_' {
                self.pos += 1;
            } else {
                break;
            }
        }
        Ok(self.input[start..self.pos].to_ascii_lowercase())
    }

    fn read_until_keyword(&mut self, keyword: &str) -> Result<String, ParseError> {
        self.read_until_any(&[keyword], &[])
    }

    fn read_until_any(
        &mut self,
        keywords: &[&str],
        symbols: &[&str],
    ) -> Result<String, ParseError> {
        self.skip_ws();
        let start = self.pos;
        let mut paren_depth = 0i32;
        let mut bracket_depth = 0i32;
        while !self.is_eof() {
            if paren_depth == 0 && bracket_depth == 0 {
                if keywords.iter().any(|kw| self.peek_keyword(kw)) || symbols.iter().any(|sym| self.peek_symbol(sym)) {
                    return Ok(self.input[start..self.pos].to_string());
                }
            }
            match self.current_char() {
                Some('(') => {
                    paren_depth += 1;
                    self.pos += 1;
                }
                Some(')') => {
                    paren_depth -= 1;
                    self.pos += 1;
                }
                Some('[') => {
                    bracket_depth += 1;
                    self.pos += 1;
                }
                Some(']') => {
                    bracket_depth -= 1;
                    self.pos += 1;
                }
                Some('\'') => self.skip_single_quoted_string(),
                Some('"') => self.skip_double_quoted_identifier(),
                Some('$') if self.is_dollar_quote_start() => self.skip_dollar_quoted_string(),
                Some(_) => self.pos += 1,
                None => break,
            }
        }
        Err(ParseError::UnexpectedToken {
            expected: "statement delimiter",
            actual: self.input[start..].into(),
        })
    }

    fn skip_ws(&mut self) {
        while let Some(ch) = self.current_char() {
            if ch.is_ascii_whitespace() {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn current_char(&self) -> Option<char> {
        self.remaining().chars().next()
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<(), ParseError> {
        if self.consume_keyword(keyword) {
            Ok(())
        } else {
            Err(ParseError::UnexpectedToken {
                expected: "keyword",
                actual: self.remaining().into(),
            })
        }
    }

    fn consume_keyword(&mut self, keyword: &str) -> bool {
        self.skip_ws();
        if !self.peek_keyword(keyword) {
            return false;
        }
        self.pos += keyword.len();
        true
    }

    fn peek_keyword(&self, keyword: &str) -> bool {
        let rem = self.remaining();
        if rem.len() < keyword.len() || !rem[..keyword.len()].eq_ignore_ascii_case(keyword) {
            return false;
        }
        rem[keyword.len()..]
            .chars()
            .next()
            .is_none_or(|ch| !(ch.is_ascii_alphanumeric() || ch == '_'))
    }

    fn expect_symbol(&mut self, symbol: &str) -> Result<(), ParseError> {
        if self.consume_symbol(symbol) {
            Ok(())
        } else {
            Err(ParseError::UnexpectedToken {
                expected: "symbol",
                actual: self.remaining().into(),
            })
        }
    }

    fn consume_symbol(&mut self, symbol: &str) -> bool {
        self.skip_ws();
        if self.peek_symbol(symbol) {
            self.pos += symbol.len();
            true
        } else {
            false
        }
    }

    fn peek_symbol(&self, symbol: &str) -> bool {
        self.remaining().starts_with(symbol)
    }

    fn skip_sql_token(&mut self) {
        match self.current_char() {
            Some('\'') => self.skip_single_quoted_string(),
            Some('$') if self.is_dollar_quote_start() => self.skip_dollar_quoted_string(),
            Some('E') | Some('e') if self.remaining().len() >= 2 && self.remaining().as_bytes()[1] == b'\'' => {
                self.pos += 1;
                self.skip_single_quoted_string();
            }
            _ => {}
        }
    }

    fn skip_single_quoted_string(&mut self) {
        let bytes = self.input.as_bytes();
        self.pos += 1;
        while self.pos < bytes.len() {
            if bytes[self.pos] == b'\'' {
                if self.pos + 1 < bytes.len() && bytes[self.pos + 1] == b'\'' {
                    self.pos += 2;
                } else {
                    self.pos += 1;
                    break;
                }
            } else {
                self.pos += 1;
            }
        }
    }

    fn skip_double_quoted_identifier(&mut self) {
        let bytes = self.input.as_bytes();
        self.pos += 1;
        while self.pos < bytes.len() {
            if bytes[self.pos] == b'"' {
                if self.pos + 1 < bytes.len() && bytes[self.pos + 1] == b'"' {
                    self.pos += 2;
                } else {
                    self.pos += 1;
                    break;
                }
            } else {
                self.pos += 1;
            }
        }
    }

    fn is_dollar_quote_start(&self) -> bool {
        let rem = self.remaining().as_bytes();
        if rem.is_empty() || rem[0] != b'$' {
            return false;
        }
        let mut idx = 1usize;
        while idx < rem.len() && (rem[idx].is_ascii_alphanumeric() || rem[idx] == b'_') {
            idx += 1;
        }
        idx < rem.len() && rem[idx] == b'$'
    }

    fn skip_dollar_quoted_string(&mut self) {
        let rem = self.remaining();
        let end = rem[1..]
            .find('$')
            .map(|offset| offset + 1)
            .expect("valid dollar quote");
        let tag = &rem[..=end];
        self.pos += tag.len();
        let rest = &self.input[self.pos..];
        if let Some(close) = rest.find(tag) {
            self.pos += close + tag.len();
        } else {
            self.pos = self.input.len();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::parser::SqlTypeKind;

    #[test]
    fn parse_basic_block_with_declare_if_for_and_raise() {
        let block = parse_block(
            "
            declare
                total int4 := 0;
            begin
                total := total + 1;
                if total > 0 then
                    raise notice 'value %', total;
                elsif total < 0 then
                    null;
                else
                    total := 1;
                end if;
                for i in 1..3 loop
                    total := total + i;
                end loop;
            end
            ",
        )
        .unwrap();
        assert_eq!(block.declarations.len(), 1);
        assert_eq!(block.declarations[0].name, "total");
        assert_eq!(block.declarations[0].ty.kind, SqlTypeKind::Int4);
        assert_eq!(block.statements.len(), 3);
    }

    #[test]
    fn parse_nested_block_statement() {
        let block = parse_block(
            "
            begin
                begin
                    null;
                end;
            end
            ",
        )
        .unwrap();
        assert!(matches!(block.statements[0], Stmt::Block(_)));
    }
}
