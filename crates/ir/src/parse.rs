/*
 *   Copyright (c) 2020
 *   All rights reserved.
 */

pub use pest::iterators::Pair;
pub use pest::iterators::Pairs;
pub use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "bql.pest"]
pub struct BqlParser;

//====== utils ======

pub fn pretty_parse_tree(pairs: Pairs<Rule>) -> String {
    let lines: Vec<_> = pairs.map(|pair| format_pair(pair, 0, true)).collect();
    let lines = lines.join("\n");
    return format!("{}", lines);
}

fn format_pair(
    pair: Pair<Rule>,
    indent_level: usize,
    is_newline: bool,
) -> String {
    let indent = if is_newline {
        "  ".repeat(indent_level)
    } else {
        "".to_string()
    };

    let children: Vec<_> = pair.clone().into_inner().collect();
    let len = children.len();
    let children: Vec<_> = children
        .into_iter()
        .map(|pair| {
            format_pair(
                pair,
                if len > 1 {
                    indent_level + 1
                } else {
                    indent_level
                },
                len > 1,
            )
        })
        .collect();

    let dash = if is_newline { "- " } else { "" };

    match len {
        0 => format!(
            "{}{}{:?}: {:?}",
            indent,
            dash,
            pair.as_rule(),
            pair.as_span().as_str()
        ),
        1 => {
            format!("{}{}{:?} > {}", indent, dash, pair.as_rule(), children[0])
        }
        _ => format!(
            "{}{}{:?}\n{}",
            indent,
            dash,
            pair.as_rule(),
            children.join("\n")
        ),
    }
}


// =============================
// =========== Tests ===========
// =============================
//FIXME move to test mod?
macro assert_parse($s:expr, $c:ident) {
    let _pairs =
        BqlParser::parse(Rule::$c, $s).unwrap_or_else(|e| panic!("{}", e));
    //println!("{:?} =>", $s);
    // for pair in pairs.flatten() {
    //     let _span = pair.clone().as_span();
    //     // A pair is a combination of the rule which matched and a span
    // of input     //println!("Rule:    {:?}", pair.as_rule());
    //     //println!("Span:    {:?}", span);
    //     //println!("Text:    {}", span.as_str());
    // }
}

// --- tests ---

#[cfg(test)]
mod unit_tests {
    use super::{
        assert_parse, pretty_parse_tree, BqlParser, Pair, Pairs, Rule,
    };
    use pest::Parser;

    /// Seek to given rule in a sequence of pairs.
    pub fn seek_to<'a, R: pest::RuleType>(
        pairs: &mut Pairs<'a, R>,
        to: R,
    ) -> Option<Pair<'a, R>> {
        for p in pairs {
            let r = p.as_rule();
            if r == to {
                return Some(p);
            }
        }
        None
    }

    #[test]
    fn test_seek_to() {
        //         let sql = r#"
        //         select c1, c2*c2 pc2, avg(c3) from tab where c3 > 1 group by
        // c1, pc2 order by c1, pc2 limit 5 "#;
        let sql = r#"
SELECT * FROM domain
WHERE domain_ID IN (SELECT domain_ID FROM domain_setting)
LIMIT 100;
"#;
        let queries = BqlParser::parse(Rule::query, sql)
            .unwrap_or_else(|e| panic!("{}", e));
        println!("{}", pretty_parse_tree(queries));
    }

    mod bql {
        use super::super::{assert_parse, pretty_parse_tree, BqlParser, Rule};
        use pest::Parser;

        // #[test]
        // fn xxx() {
        //     enum Rule {
        //         a
        //     }

        //     let input = "";
        //     let pairs = pest::state(input, |state| {
        //         // generating Token pair with Rule::a ...
        //         state.match_insensitive("xxx")
        //     }).unwrap();
        //     let tokens: Vec<_> = pairs.tokens().collect();

        //     assert_eq!(tokens.len(), 2);
        // }

        #[test]
        fn sanity_checks() {
            assert_parse!("1", expr);
            assert_parse!("7", number);
            assert_parse!("id", id);
            assert_parse!("1", select_column);

            assert_parse!("l_extendedprice * (1 - l_discount)", arith_expr);

            assert_parse!("sum(case when 1 then 2 else 3 end)", func_call_expr);
            assert_parse!(
                "sum(case when (d_day_name='Sunday') then sales_price else null end)",
                func_call_expr
            );
            assert_parse!(
                "sum(cr_refunded_cash+cr_reversed_charge+xxx_yyy)",
                func_call_expr
            );

            let c = "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax))";
            assert_parse!(c, func_call_expr);

            assert_parse!(
                "d_month_seq between (select distinct d_month_seq+1
                                 from   date_dim where d_year = 1998 and d_moy = 1)
                           and  (select distinct d_month_seq+3
                                 from   date_dim where d_year = 1998 and d_moy = 1)",
                between_expr
            );

            let c = "where  c_current_addr_sk = ca_address_sk
        and ca_county = s_county
        and ca_state = s_state
        and ss_sold_date_sk = d_date_sk
        and c_customer_sk = ss_customer_sk
        and d_month_seq between (select distinct d_month_seq+1
                                 from   date_dim where d_year = 1998 and d_moy = 1)
                           and  (select distinct d_month_seq+3
                                 from   date_dim where d_year = 1998 and d_moy = 1)";
            assert_parse!(c, where_clause);
            let c = "a between (select 1) and (select 2)";
            assert_parse!(c, logical_expr);

            // let c = "CASE
            //   WHEN (o_orderpriority = '1-URGENT')
            //        OR o_orderpriority = '2-HIGH'
            //     THEN 1
            //   ELSE 0
            //   END";
            // let pairs = BqlParser::parse(Rule::case_expr,
            // c).unwrap_or_else(|e| panic!("{}", e)); println!("{}"
            // , pretty_parse_tree(pairs));

            let c = "case when p_promo_sk is null then 1 else 0 end";
            assert_parse!(c, case_expr);
            let c = "sum(l_extendedprice) / 7.0 AS avg_yearly";
            assert_parse!(c, func_call_expr);
            let c = "x(7.0)";
            assert_parse!(c, arith_expr);

            let c = "(1 - l_discount) * l_extendedprice";
            assert_parse!(c, expr);
            // let pairs = BqlParser::parse(Rule::expr, c).unwrap_or_else(|e|
            // panic!("{}", e)); println!("{}",
            // pretty_parse_tree(pairs));

            let c = "substr(w_warehouse_name,1,20)";
            assert_parse!(c, func_call_expr);

            let c = "now()";
            assert_parse!(c, func_call_expr);

            let c = "-123123.12";
            assert_parse!(c, literal);
            let c = "`id`";
            assert_parse!(c, id);

            let c = "'hehe'";
            assert_parse!(c, literal);

            let c = "(1, -3,+5,-3.1415926e0, .618,9E9)";
            let pairs = BqlParser::parse(Rule::const_num_array, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));
        }

        #[test]
        fn test_comment_parse() {
            let c = r"-- abci
CREATE TABLE test (col INT32)";
            let pairs = BqlParser::parse(Rule::cmd_list, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("pairs: {}", pretty_parse_tree(pairs));
        }

        #[test]
        fn test_expr() {
            assert_parse!(
                "case when (d_day_name='Sunday') then sales_price else null end",
                case_expr
            );
            assert_parse!("case when 1 then 2 else 3 end", case_expr);
            assert_parse!(
                "case mean when 0 then null else stdev/mean end",
                case_expr
            );
        }

        #[test]
        fn test_logical_expr() {
            let c = "(x = y) AND z";
            assert_parse!(c, logical_expr);

            let c = "n2.n_name = 'FRANCE'
     AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'";
            assert_parse!(c, logical_expr);

            // let c = "(
            //        (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
            //        OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
            //      )
            //      AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'";
            // let c = "(
            //    (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
            //    OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
            //  )";
            let c = "(
    p_partkey = l_partkey
    AND p_brand = 'Brand#12'
    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    AND l_quantity >= 1 AND l_quantity <= 1 + 10
    AND p_size BETWEEN 1 AND 5
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
  )
  OR
  (
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    AND l_quantity >= 10 AND l_quantity <= 10 + 10
    AND p_size BETWEEN 1 AND 10
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
  )
  OR
  (
    p_partkey = l_partkey
    AND p_brand = 'Brand#34'
    AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    AND l_quantity >= 20 AND l_quantity <= 20 + 10
    AND p_size BETWEEN 1 AND 15
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
  )";
            assert_parse!(c, logical_expr);

            let c = "l_quantity < (
    SELECT 0.2 * avg(l_quantity)
    FROM
      lineitem
    WHERE
      l_partkey = p_partkey
  )";
            assert_parse!(c, comp_expr);

            let c = "(
           ((n1.n_name = 'FRANCE') AND (n2.n_name = 'GERMANY'))
           OR ((n1.n_name = 'GERMANY') AND (n2.n_name = 'FRANCE'))
         )
         AND l_shipdate";
            assert_parse!(c, logical_expr);

            // let c = "(
            // select count(*) as item_cnt
            //   from item
            //   where (i_manufact = i1.i_manufact) or
            //   (i_manufact = i1.i_manufact and
            //      (
            //       (i_category = 'Women'  ) or (i_category = 'Women' ) or
            // (i_category = 'Men'  ) or (i_category = 'Men' )
            //      )
            //   )
            // ) > 0";
            // let c = "(
            // select count(*) as item_cnt
            //   from item
            //   where (i_manufact = i1.i_manufact) ) > 0";
            let c = "( select count(*) from item ) > 0";
            let pairs = BqlParser::parse(Rule::logical_expr, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));
        }

        #[test]
        fn test_select() {
            assert_parse!("SELECT 1", select);
            assert_parse!("SELECT 1, 'test'", select);

            assert_parse!("SELECT * FROM test WHERE 1", select);

            let c =
                "SELECT t1.c1 as t1c1 FROM test WHERE 1 GROUP BY id HAVING count(*) > 1";
            let pairs = BqlParser::parse(Rule::select, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));

            let c = "select ss_item_sk
                  ,ss_ticket_number
                  ,ss_customer_sk
                  ,case when sr_return_quantity is not null then (ss_quantity-sr_return_quantity)*ss_sales_price
                                                            else (ss_quantity*ss_sales_price) end act_sales
            from store_sales left outer join store_returns on (sr_item_sk = ss_item_sk
                                                               and sr_ticket_number = ss_ticket_number)
                ,reason
            where sr_reason_sk = r_reason_sk
              and r_reason_desc = 'Package was damaged'";
            assert_parse!(c, select);
        }

        #[test]
        fn test_query() {
            assert_parse!("SELECT 1", cmd);
            assert_parse!("SELECT * FROM test ORDER BY 1", query);
            assert_parse!("SELECT * FROM test ORDER BY 1, id", query);
            assert_parse!("SELECT * FROM test LIMIT 1", query);
            assert_parse!(
                "select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
       ,stdev,mean, case mean when 0 then null else stdev/mean end cov",
                query
            );
            let c = "with bar as (select 1 from foo) select 2 from bar";
            let pairs = BqlParser::parse(Rule::query, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));
        }

        #[test]
        fn test_with_query() {
            let c = "with my_customers as (
 select distinct c_customer_sk
        , c_current_addr_sk
 from   
        ( select cs_sold_date_sk sold_date_sk,
                 cs_bill_customer_sk customer_sk,
                 cs_item_sk item_sk
          from   catalog_sales
          union all
          select ws_sold_date_sk sold_date_sk,
                 ws_bill_customer_sk customer_sk,
                 ws_item_sk item_sk
          from   web_sales
         ) cs_or_ws_sales,
         item,
         date_dim,
         customer
 where   sold_date_sk = d_date_sk
         and item_sk = i_item_sk
         and i_category = 'Women'
         and i_class = 'dresses'
         and c_customer_sk = cs_or_ws_sales.customer_sk
         and d_moy = 1
         and d_year = 1998
 )";
            let pairs = BqlParser::parse(Rule::with, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));

            let c = "with wscs as
 (select sold_date_sk
        ,sales_price
  from  (select ws_sold_date_sk sold_date_sk
              ,ws_ext_sales_price sales_price
        from web_sales )
        union all
        (select cs_sold_date_sk sold_date_sk
              ,cs_ext_sales_price sales_price
        from catalog_sales))";
            let pairs = BqlParser::parse(Rule::with, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));

            let c = "inv as (select case mean when 0 then null else stdev/mean end cov from foo)";
            let pairs = BqlParser::parse(Rule::with_query, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));
        }

        #[test]
        fn test_tpcds_1() {
            let c = "with customer_total_return as (
  select
    sr_customer_sk as ctr_customer_sk,
    sr_store_sk as ctr_store_sk,
    sum(sr_return_amt) as ctr_total_return
  from
    store_returns,
    date_dim
  where
    sr_returned_date_sk = d_date_sk
    and d_year = 1998
  group by
    sr_customer_sk,
    sr_store_sk
)
select
  c_customer_id
from
  customer_total_return ctr1,
  store,
  customer
where
  ctr1.ctr_total_return > (
    select
      avg(ctr_total_return) * 1.2
    from
      customer_total_return ctr2
    where
      ctr1.ctr_store_sk = ctr2.ctr_store_sk
  )
  and s_store_sk = ctr1.ctr_store_sk
  and s_state = 'TN'
  and ctr1.ctr_customer_sk = c_customer_sk
order by
  c_customer_id
limit
  100

-- end query 1 in stream 0 using template query1.tpl
";
            let pairs = BqlParser::parse(Rule::with, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));
        }

        #[test]
        fn test_create_table() {
            assert_parse!("CREATE TABLE test (col INT32)", create_table);
            assert_parse!(
                "CREATE TABLE main.test (col Decimal(7,2))",
                create_table
            );
            assert_parse!(
                "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                create_table
            );
            assert_parse!(
                "CREATE TABLE IF NOT EXISTS test (col STRING)",
                create_table
            );

            assert_parse!("CREATE TABLE test (id UINT64)", create_table);
            assert_parse!("CREATE TABLE test (id INT64)", create_table);
            assert_parse!("CREATE TABLE test (id VCHAR(111))", create_table);
            assert_parse!("CREATE TABLE test (id DOUBLE)", create_table);
            assert_parse!("CREATE TABLE test (id DECIMAL(12,5))", create_table);

            let c = "CREATE TABLE test (id INT64 default 123)";
            assert_parse!(c, create_table);

            let c = "CREATE TABLE test (id INT64 PRIMARY KEY)";
            let pairs = BqlParser::parse(Rule::create_table, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));
        }

        #[test]
        fn test_tpch_q6() {
            let c = "SELECT sum(l_extendedprice * l_discount) AS revenue
            FROM
              lineitem
            WHERE
              l_shipdate >= '1994-01-01'
              AND l_shipdate < '1995-01-01'
            AND l_discount BETWEEN 0.05 AND 0.07
            AND l_quantity < 24            
";
            let pairs = BqlParser::parse(Rule::cmd, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));
        }

        #[test]
        fn test_transform_tpch_q3() {
            let c = "SELECT
            l_orderkey,
            sum(l_extendedprice * (1 - l_discount)) AS revenue,
            o_orderdate,
            o_shippriority
          FROM
            customer,
            orders,
            lineitem
          WHERE
            c_mktsegment = 'BUILDING'
            AND c_custkey = o_custkey
            AND l_orderkey = o_orderkey
            AND o_orderdate < '1995-03-15'
            AND l_shipdate > '1995-03-15'
          GROUP BY
            l_orderkey,
            o_orderdate,
            o_shippriority
          ORDER BY
            revenue DESC,
            o_orderdate
          LIMIT 10
";
            let pairs = BqlParser::parse(Rule::cmd, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));
        }

        #[test]
        fn test_transform_tpch_x1() {
            let c = "SELECT
            o_shippriority,
            c_name,
            c_address
          FROM
            customer,
            orders
          WHERE
            c_mktsegment = 'BUILDING'
            AND c_custkey = o_custkey
            AND o_orderdate < '1995-03-15'
          ORDER BY
            o_shippriority DESC, c_name
          LIMIT 10
";
            let pairs = BqlParser::parse(Rule::cmd, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));
            /*
            * bir
            * op operand_list : [op_attr_list] [in_attr_list] [out_attr_list]
            *  * : [ c.c_custkey = o.o_custkey; ]
            *      [ data_order: [] ]
            *      [ data_count: 10; data_order： [o_shippriority=desc; c_name=asc; ] ]
            *    -> (c_name, c_address)
            *         : [c.c_mktsegment = 'BUILDING' & c.c_custkey is not null]
            *           []
            *           []
            *    -> (o_shippriority)
            *         : [o.o_orderdate < '1995-03-15' & o.o_custkey is not null ]
            *           []
            *           [data_count: 10; data_order： [o_shippriority=desc;]]
            *

            fork for p0:

            //load o_shippriority, sort desc, top 10
            o_custkey
            o_shippriority=
            for() {
            }
            //then build as sorted vec



            //join
            out= (sorted vec)
            for( load c_custkey as c_c,c_name,c_address ) {
                if ( c_c in o_custkey) {
                  out <- (c_name, c_address, o_shippriority[c_c])
                }
            }

            join for p0,p1
            out_p0 = (sorted vec)
            out_p1 = (sorted vec)
            out = merge(out_p0, out_p1, mfunc) //mfunc is top10


            *
            */
        }
    }
}
