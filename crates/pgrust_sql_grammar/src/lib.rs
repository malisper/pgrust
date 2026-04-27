use pest::Parser as _;

#[derive(pest_derive::Parser)]
#[grammar = "gram.pest"]
pub struct SqlParser;

pub fn parse_rule(
    rule: Rule,
    input: &str,
) -> Result<pest::iterators::Pairs<'_, Rule>, pest::error::Error<Rule>> {
    SqlParser::parse(rule, input)
}
