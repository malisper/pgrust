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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_grouping_sets_query_shapes() {
        let queries = [
            "select sum(c) from gstest2
              group by grouping sets((), grouping sets((), grouping sets(())))
              order by 1 desc",
            "select a, b, sum(v), count(*) from gstest_empty
              group by grouping sets ((a,b),a)",
            "select four, x
              from (select four, ten, 'foo'::text as x from tenk1) as t
              group by grouping sets (four, x)
              having x = 'foo'",
            "select a, b, c, d from gstest2
              group by rollup(a,b),grouping sets(c,d)",
            "select distinct on (a, b) a, b
              from gstest2
              group by grouping sets ((a, b), (a))
              order by a, b",
        ];

        for sql in queries {
            parse_rule(Rule::statement, sql).unwrap_or_else(|err| panic!("{sql}\n{err}"));
        }
    }
}
