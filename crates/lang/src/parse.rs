/*
 *   Copyright (c) 2020
 *   All rights reserved.
 */

use meta::types::{BqlType, ColumnInfo, EngineType, Table};
pub use pest::iterators::Pair;
pub use pest::iterators::Pairs;
use std::collections::HashSet;
use std::net::IpAddr;

pub(crate) use pest::Parser;
use pest_derive::Parser;

use crate::errs::{LangError, LangResult};

#[derive(Parser)]
#[grammar = "bql.pest"]
pub struct BqlParser;

//====== utils ======

pub fn pretty_parse_tree(pairs: Pairs<Rule>) -> String {
    let lines: Vec<_> = pairs.map(|pair| format_pair(pair, 0, true)).collect();
    let lines = lines.join("\n");
    return lines.to_string();
}

fn format_pair(pair: Pair<Rule>, indent_level: usize, is_newline: bool) -> String {
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

#[derive(Debug)]
pub struct DbInfo {
    pub dbname: String,
    pub fallible: bool,
}

pub fn parse_create_database(pair: Pair<Rule>) -> LangResult<DbInfo> {
    let mut rt = DbInfo {
        dbname: String::new(),
        fallible: true,
    };
    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::if_not_exists => rt.fallible = false,
            Rule::database_name => rt.dbname = p.as_str().trim().to_owned(),
            _ => {
                // return Err(LangError::DatabaseParsingError);
            }
        }
    }

    Ok(rt)
}

pub fn parse_optimize_table(pair: Pair<Rule>) -> LangResult<(Option<String>, String)> {
    let mut dbname = None;
    let mut tabname = String::new();
    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::qualified_table_name => {
                let qtn = p.as_str().trim();
                let qtn: Vec<&str> = qtn.split('.').collect();
                match qtn[..] {
                    [dbn, tn] => {
                        dbname = Some(dbn.to_owned());
                        tabname.push_str(tn);
                    }
                    [tn] => {
                        tabname.push_str(tn);
                    }
                    _ => return Err(LangError::QueryLangParsingError),
                }
            }
            _ => {}
        }
    }

    Ok((dbname, tabname))
}

pub fn parse_drop_database(pair: Pair<Rule>) -> LangResult<DbInfo> {
    let mut rt = DbInfo {
        dbname: String::new(),
        fallible: true,
    };
    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::if_exists => rt.fallible = false,
            Rule::database_name => rt.dbname = p.as_str().trim().to_owned(),
            _ => {
                // println!("{:?}", p);
                // return Err(LangError::DatabaseParsingError);
            }
        }
    }
    Ok(rt)
}

pub fn parse_drop_table(pair: Pair<Rule>) -> LangResult<(Option<String>, String, bool)> {
    // println!("{}", pretty_parse_tree(pair.clone().into_inner()));
    let mut dbname = None;
    let mut tabname = String::new();
    let mut fallible = true;
    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::if_exists => fallible = false,
            Rule::qualified_table_name => {
                let qtn = p.as_str().trim();
                let qtn: Vec<&str> = qtn.split('.').collect();
                match qtn[..] {
                    [dbn, tn] => {
                        dbname = Some(dbn.to_owned());
                        tabname.push_str(tn);
                    }
                    [tn] => {
                        tabname.push_str(tn);
                    }
                    _ => return Err(LangError::DatabaseParsingError),
                }
            }
            _ => return Err(LangError::DatabaseParsingError),
        }
    }
    Ok((dbname, tabname, fallible))
}

pub fn parse_show_create_table(pair: Pair<Rule>) -> LangResult<(Option<String>, String)> {
    let p = pair
        .into_inner()
        .next()
        .ok_or(LangError::DatabaseParsingError)?;
    match p.as_rule() {
        Rule::qualified_table_name => {
            let qtn = p.as_str().trim();
            let qtn: Vec<&str> = qtn.split('.').collect();
            match qtn[..] {
                [dbn, tn] => Ok((Some(dbn.to_owned()), tn.to_owned())),
                [tn] => Ok((None, tn.to_string())),
                _ => Err(LangError::DatabaseParsingError),
            }
        }
        _ => Err(LangError::DatabaseParsingError),
    }
}

pub fn parse_desc_table(pair: Pair<Rule>) -> LangResult<(Option<String>, String)> {
    let p = pair
        .into_inner()
        .next()
        .ok_or(LangError::DatabaseParsingError)?;
    match p.as_rule() {
        Rule::qualified_table_name => {
            let qtn = p.as_str().trim();
            let qtn: Vec<&str> = qtn.split('.').collect();
            match qtn[..] {
                [dbn, tn] => Ok((Some(dbn.to_owned()), tn.to_owned())),
                [tn] => Ok((None, tn.to_string())),
                _ => Err(LangError::DatabaseParsingError),
            }
        }
        _ => Err(LangError::DatabaseParsingError),
    }
}

#[derive(Debug, Clone)]
pub enum InsertFormat {
    InlineValues,
    Inline,
    CSV,
    Select(String),
}

pub struct InsertIntoContext {
    pub tab: Table,
    pub format: InsertFormat,
    pub values: Option<Vec<Vec<String>>>, //WARN gen vec of vec String is slow
}

impl InsertIntoContext {
    fn parse(&mut self, pair: Pair<Rule>) -> LangResult<()> {
        let r = pair.as_rule();
        //pre
        match r {
            Rule::rows => {
                self.format = InsertFormat::InlineValues;
                self.values = Some(Vec::new());
            }
            Rule::row => {
                if let Some(ref mut tab) = self.values {
                    tab.push(Vec::new());
                }
            }
            Rule::format_clause => {
                self.format = InsertFormat::Inline;
            }
            Rule::format_name => {
                self.format = InsertFormat::CSV;
            }
            Rule::select => {
                self.format = InsertFormat::Select(pair.as_str().trim().to_owned());
            }
            _ => {}
        }
        for p in pair.clone().into_inner() {
            self.parse(p)?;
        }

        //post
        match r {
            // database_name ~ ".")? ~ table_name
            Rule::database_name => {
                self.tab.dbname = pair.as_str().trim().to_owned();
            }
            Rule::table_name => {
                if !matches!(self.format, InsertFormat::Select(_)) {
                    self.tab.name = pair.as_str().trim().to_owned();
                }
            }
            Rule::column_name => {
                // let col = self
                //     .tab
                //     .columns
                //     .last_mut()
                //     .ok_or(LangError::CreateTableParsingError)?;
                self.tab
                    .columns
                    .push((pair.as_str().trim().to_string(), ColumnInfo::default()));
            }
            Rule::literal => {
                if let Some(ref mut tab) = self.values {
                    if let Some(row) = tab.last_mut() {
                        row.push(pair.as_str().trim().to_string());
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

//FIXME need to validate all tabs for malicious ddls

struct CreateTabContext {
    tab: Table,
    fallible: bool,
}

impl CreateTabContext {
    fn parse(&mut self, pair: Pair<Rule>) -> LangResult<()> {
        let r = pair.as_rule();
        //pre
        match r {
            Rule::if_not_exists => {
                self.fallible = false;
            }
            Rule::column_def => {
                self.tab.columns.push(Default::default());
            }
            Rule::table_attr_settings => {
                self.tab.tab_info.settings = Default::default();
            }
            Rule::table_attr_partition => {
                let ti = &mut self.tab.tab_info;
                //NOTE as mark of start of table_attr_partition
                ti.partition_keys_expr = "$".to_string();
            }
            _ => {}
        }
        for p in pair.clone().into_inner() {
            self.parse(p)?;
        }
        //post
        match r {
            // database_name ~ ".")? ~ table_name
            Rule::database_name => {
                self.tab.dbname = pair.as_str().trim().to_owned();
            }
            Rule::table_name => {
                self.tab.name = pair.as_str().trim().to_owned();
            }
            Rule::column_name => {
                let col_len = self.tab.columns.len() as u32;
                let col = self
                    .tab
                    .columns
                    .last_mut()
                    .ok_or(LangError::CreateTableParsingError)?;
                col.0 = pair.as_str().trim().to_string();
                col.1.ordinal = col_len - 1;
            }
            Rule::type_name => {
                let col = self
                    .tab
                    .columns
                    .last_mut()
                    .ok_or(LangError::CreateTableParsingError)?;
                let inner_typ_opt = pair.into_inner().next();
                match inner_typ_opt {
                    Some(p) if p.as_rule() == Rule::low_cardinality_string_type => {
                        col.1.data_type = BqlType::LowCardinalityString;
                    }
                    Some(p) if p.as_rule() == Rule::low_cardinality_tinytext_type => {
                        col.1.data_type = BqlType::LowCardinalityTinyText;
                    }
                    Some(p) if p.as_rule() == Rule::nullable_type => {
                        let typ = p
                            .into_inner()
                            .next()
                            .ok_or(LangError::UnsupportedBqlTypeError)?
                            .as_str()
                            .trim();

                        col.1.data_type = BqlType::from_str(typ)?;
                        col.1.is_nullable = true;
                    }
                    // Some(p) if p.as_rule() == Rule::decimal_type => {
                    //     col.1.data_type = BqlType::LowCardinalityString;
                    // }
                    Some(p)
                        if p.as_rule() == Rule::simple_type
                            || p.as_rule() == Rule::decimal_type
                            || p.as_rule() == Rule::fixed_string_type =>
                    {
                        let typ = p.as_str().trim();
                        col.1.data_type = BqlType::from_str(typ)?;
                    }
                    _ => return Err(LangError::UnsupportedBqlTypeError),
                }
            }
            Rule::column_constraint => {
                let col = self
                    .tab
                    .columns
                    .last_mut()
                    .ok_or(LangError::CreateTableParsingError)?;
                let constr = pair.as_str().trim().to_ascii_uppercase();
                match constr.as_str() {
                    "PRIMARY KEY" => col.1.is_primary_key = true,
                    _ => return Err(LangError::UnsupportedLangFeatureError),
                };
            }
            Rule::table_attr_engine => {
                let ti = &mut self.tab.tab_info;
                let eng_code = pair.into_inner().as_str().trim();
                ti.engine = EngineType::from_str(eng_code)?;
            }
            Rule::qualified_name => {
                let ti = &mut self.tab.tab_info;
                let ptk = pair.into_inner().as_str().trim();
                if ti.partition_keys_expr == "$" {
                    ti.partition_cols.push_str(ptk);
                    ti.partition_cols.push(',');
                }
            }
            Rule::table_attr_partition => {
                let ti = &mut self.tab.tab_info;
                let part_keys_expr = pair.into_inner().as_str().trim();
                //FIXME need more precise validations
                ti.partition_keys_expr = part_keys_expr.to_string();
            }

            _ => {}
        }

        Ok(())
    }
}

pub fn parse_insert_into(pair: Pair<Rule>) -> LangResult<InsertIntoContext> {
    let mut ctx = InsertIntoContext {
        tab: Default::default(),
        format: InsertFormat::Inline,
        values: None,
    };
    ctx.parse(pair)?;
    // println!("{:?}", ctx.tables);
    //FIXME need to pop up the tab info
    Ok(ctx)
}

pub fn parse_create_table(pair: Pair<Rule>) -> LangResult<(Table, bool)> {
    let mut ctx = CreateTabContext {
        tab: Default::default(),
        fallible: true,
    };
    ctx.parse(pair)?;
    // println!("{:?}", ctx.tables);
    //FIXME need to validate all tabs for malicious ddls
    Ok((ctx.tab, ctx.fallible))
}

#[derive(Debug)]
pub struct TablesContext {
    pub tabs: HashSet<String>,
    pub cols: HashSet<String>,
    pub has_count_all: bool,
    pub has_select_all: bool,
}

impl TablesContext {
    fn parse(&mut self, pair: Pair<Rule>) -> LangResult<()> {
        let r = pair.as_rule();
        for p in pair.clone().into_inner() {
            self.parse(p)?;
        }
        match r {
            Rule::qualified_table_name => {
                self.tabs.insert(pair.as_str().trim().to_owned());
            }
            Rule::qualified_name => {
                self.cols.insert(pair.as_str().trim().to_owned());
            }
            Rule::select_column_all => {
                self.has_select_all = true;
            }
            Rule::count_tuple_expr => {
                self.has_count_all = true;
            }
            _ => {}
        }

        Ok(())
    }
}

pub fn parse_tables(pair: Pair<Rule>) -> LangResult<TablesContext> {
    let mut ctx = TablesContext {
        tabs: Default::default(),
        cols: Default::default(),
        has_count_all: false,
        has_select_all: false,
    };
    ctx.parse(pair)?;
    // println!("{:?}", ctx.tables);
    //FIXME need to validate all tabs for malicious ddls
    Ok(ctx)
}

pub fn parse_system_numbers_table(tab: &str) -> LangResult<(i64, i64)> {
    let mut ps: Pairs<Rule> = BqlParser::parse(Rule::qualified_table_name, tab)
        .map_err(|e| LangError::ASTError(e.to_string()))?;
    // println!("{}", pretty_parse_tree(ps.clone()));
    let p =
        seek_to(&mut ps, Rule::qualified_table_name).ok_or(LangError::FailToUnwrap)?;
    let table_name = seek_to(&mut p.into_inner(), Rule::table_name);
    let inner = table_name.ok_or(LangError::UnsupportedSystemNumbersNamingFormatError)?;
    let numbers = seek_to(&mut inner.into_inner(), Rule::table_name_numbers)
        .ok_or(LangError::UnsupportedSystemNumbersNamingFormatError)?;
    let mut nums = numbers.into_inner();
    if let Some(a) = nums.next() {
        if let Some(b) = nums.next() {
            let s = a
                .as_str()
                .parse::<i64>()
                .map_err(|e| LangError::WrappingParseIntError(e))?;
            let e = b
                .as_str()
                .parse::<i64>()
                .map_err(|e| LangError::WrappingParseIntError(e))?;
            if s <= e {
                return Ok((s, e));
            } else {
                return Err(LangError::UnsupportedSystemNumbersNamingFormatError);
            }
        } else {
            let estr = a.as_str();
            let e = estr
                .parse::<i64>()
                .map_err(|e| LangError::WrappingParseIntError(e))?;
            if e >= 0 {
                return Ok((0, e));
            } else {
                return Err(LangError::UnsupportedSystemNumbersNamingFormatError);
            }
        }
    }
    Ok((0, 10))
}

pub fn parse_command(cmds: &str) -> LangResult<Pairs<Rule>> {
    let ps: Pairs<Rule> = BqlParser::parse(Rule::cmd_list, cmds)
        .map_err(|e| LangError::ASTError(e.to_string()))?;
    // println!("{}", pretty_parse_tree(ps.clone()));
    Ok(ps)
}

pub fn seek_to_sub_cmd<'a>(pairs: &mut Pairs<'a, Rule>) -> LangResult<Pair<'a, Rule>> {
    let p = seek_to(pairs, Rule::cmd).ok_or(LangError::FailToUnwrap)?;
    p.into_inner().next().ok_or(LangError::FailToUnwrap)
}

/// Seek to given rule in a sequence of pairs.
fn seek_to<'a, R: pest::RuleType>(
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

#[derive(Debug, PartialEq)]
pub struct RemoteDbInfo {
    addrs: Vec<RemoteAddr>,
    username: Option<String>,
    password: Option<String>,
    database_name: String,
    table_name: String,
}

#[derive(Debug, Default, PartialEq)]
pub struct RemoteAddr {
    ip_addr: Option<IpAddr>,
    host_name: Option<String>,
    port: Option<u16>,
}

impl Default for RemoteDbInfo {
    fn default() -> Self {
        Self {
            addrs: vec![],
            username: None,
            password: None,
            database_name: "default".to_string(),
            table_name: "".to_string(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum QueryFormat {
    Local,
    Remote(RemoteDbInfo),
}

#[derive(Debug)]
pub struct QueryContext {
    pub format: QueryFormat,
}

impl QueryContext {
    fn mut_remote_info<'a>(&'a mut self) -> Option<&'a mut RemoteDbInfo> {
        if let QueryFormat::Remote(format) = &mut self.format {
            Some(format)
        } else {
            None
        }
    }
}

pub fn parse_query(pair: Pair<Rule>) -> LangResult<QueryContext> {
    let mut ctx = QueryContext {
        format: QueryFormat::Local,
    };
    ctx.parse(pair)?;
    Ok(ctx)
}

#[inline]
fn parse_ip(pair: Pair<Rule>) -> LangResult<Option<IpAddr>> {
    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::ipv4_lit | Rule::ipv6_lit => {
                let ip: IpAddr = p
                    .as_str()
                    .parse()
                    .map_err(|e| LangError::WrappingParseIpAddrError(e))?;
                return Ok(Some(ip));
            }
            _ => {}
        }
    }
    Ok(None)
}

#[inline]
fn parse_ip_address(pair: Pair<Rule>) -> LangResult<RemoteAddr> {
    let mut addr = RemoteAddr::default();

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::ip => {
                addr.ip_addr = parse_ip(p)?;
            }
            Rule::port => {
                let port: u16 = p
                    .as_str()
                    .parse()
                    .map_err(|e| LangError::WrappingParseIntError(e))?;
                addr.port = Some(port)
            }
            _ => {}
        }
    }

    Ok(addr)
}

#[inline]
fn parse_host_address(pair: Pair<Rule>) -> RemoteAddr {
    let s = pair.as_str().trim().to_string();
    let vs: Vec<&str> = s.split(":").collect();
    let mut addr = RemoteAddr::default();

    addr.host_name = Some(vs[0].to_owned());
    if vs.len() == 2 {
        addr.port = Some(vs[1].parse().unwrap());
    }

    addr
}

impl QueryContext {
    fn parse(&mut self, pair: Pair<Rule>) -> LangResult<()> {
        let r = pair.as_rule();
        //pre
        match r {
            Rule::remote_func => self.format = QueryFormat::Remote(Default::default()),
            _ => {}
        }

        for p in pair.clone().into_inner() {
            self.parse(p)?;
        }

        //post
        match r {
            Rule::ip_address => {
                self.mut_remote_info()
                    .map(|format| match parse_ip_address(pair) {
                        Ok(addr) => Ok(format.addrs.push(addr)),
                        Err(e) => Err(e),
                    })
                    .transpose()?;
            }
            Rule::host_address => {
                self.mut_remote_info().map(|format| {
                    format.addrs.push(parse_host_address(pair));
                });
            }
            Rule::remote_database_name => {
                self.mut_remote_info().map(|format| {
                    format.database_name = pair.as_str().trim().to_string();
                });
            }
            Rule::remote_table_name => {
                self.mut_remote_info().map(|format| {
                    format.table_name = pair.as_str().trim().to_string();
                });
            }
            Rule::username => {
                self.mut_remote_info().map(|format| {
                    format.username = Some(pair.as_str().to_string());
                });
            }
            Rule::password => {
                self.mut_remote_info().map(|format| {
                    format.password = Some(pair.as_str().to_string());
                });
            }
            _ => {}
        }
        Ok(())
    }
}

// === tests ===

#[cfg(test)]
mod unit_tests {
    use crate::{
        errs::{LangError, LangResult},
        parse::{parse_query, parse_tables, TablesContext},
    };

    //FIXME move to test mod?
    // pub(crate) macro assert_parse($s:expr, $c:ident) {
    // #[macro_export]

    use super::{
        parse_create_database, parse_create_table, pretty_parse_tree, seek_to, BqlParser,
        QueryFormat, RemoteAddr, RemoteDbInfo, Rule,
    };
    use meta::types::BqlType;
    use pest::Parser;

    #[test]
    pub fn test_parse_create_database() -> LangResult<()> {
        let ddl = "create database if not exists xxx";
        let ps = BqlParser::parse(Rule::create_database, ddl)
            .map_err(|e| LangError::CreateTableParsingError)?;
        let cd = ps
            .into_iter()
            .next()
            .ok_or(LangError::DatabaseParsingError)?;
        // println!("{:?}", cd);
        // println!("{}", pretty_parse_tree(ps.clone()));
        let di = parse_create_database(cd)?;
        // println!("{:?}", di);
        assert_eq!(di.dbname, "xxx");
        assert_eq!(di.fallible, false);

        let ddl = "create database a_b_c_01";
        let ps = BqlParser::parse(Rule::create_database, ddl)
            .map_err(|e| LangError::CreateTableParsingError)?;
        let cd = ps
            .into_iter()
            .next()
            .ok_or(LangError::DatabaseParsingError)?;
        // println!("{:?}", cd);
        // println!("{}", pretty_parse_tree(ps.clone()));
        let di = parse_create_database(cd)?;
        // println!("{:?}", di);
        assert_eq!(di.dbname, "a_b_c_01");
        assert_eq!(di.fallible, true);

        Ok(())
    }

    #[test]
    pub fn test_parse_table() -> LangResult<()> {
        let ddl = "select id from tab";

        fn parse_to_tabctx(s: &str) -> LangResult<TablesContext> {
            let ps = BqlParser::parse(Rule::cmd_list, s)
                .map_err(|e| LangError::CreateTableParsingError)?;
            let ct = ps.into_iter().next().ok_or(LangError::GenericError)?;
            let tctx = parse_tables(ct)?;
            Ok(tctx)
        }

        let tctx = parse_to_tabctx(ddl)?;
        assert!(tctx.tabs.contains("tab"));
        assert!(tctx.cols.contains("id"));
        assert_eq!(tctx.has_count_all, false);
        assert_eq!(tctx.has_select_all, false);
        // println!("tctx: {:?}", tctx);

        let ddl = "select * from tab";
        let tctx = parse_to_tabctx(ddl)?;
        assert!(tctx.tabs.contains("tab"));
        assert!(tctx.cols.len() == 0);
        assert_eq!(tctx.has_count_all, false);
        assert_eq!(tctx.has_select_all, true);
        // println!("tctx: {:?}", tctx);

        let ddl = "select count(*) from tab";
        let tctx = parse_to_tabctx(ddl)?;
        assert!(tctx.tabs.contains("tab"));
        assert!(tctx.cols.len() == 0);
        assert_eq!(tctx.has_count_all, true);
        assert_eq!(tctx.has_select_all, false);
        // println!("tctx: {:?}", tctx);

        let ddl = "select count(1) from tab";
        let tctx = parse_to_tabctx(ddl)?;
        assert!(tctx.tabs.contains("tab"));
        assert!(tctx.cols.len() == 0);
        assert_eq!(tctx.has_count_all, true);
        assert_eq!(tctx.has_select_all, false);
        // println!("tctx: {:?}", tctx);

        Ok(())
    }

    #[allow(unaligned_references)]
    #[test]
    pub fn test_parse_create_table() -> LangResult<()> {
        let ddl = r##"create TABLE some_tab_v2
        (
            `did` Int64,
            `reason` LowCardinality(String),
            `some_col` Nullable(String),
            col_d4 Nullable(Decimal(10,5))
        )
        ENGINE = BaseStorage
        PARTITION BY toYYYYMM(ds)
        SETTINGS a=32768,
        b=0"##;
        let ps = BqlParser::parse(Rule::cmd_list, ddl)
            .map_err(|e| LangError::CreateTableParsingError)?;
        println!("{}", pretty_parse_tree(ps.clone()));

        let ct = ps
            .into_iter()
            .next()
            .ok_or(LangError::CreateTableParsingError)?;
        // println!("{:?}", ct.clone().as_rule());
        let t = parse_create_table(ct)?;
        println!("{:?}", t);
        assert_eq!(t.0.columns.len(), 4);
        assert_eq!(t.0.columns[0].0, "`did`"); //FIXME remove `` when parsing id
        assert_eq!(t.0.columns[0].1.data_type, BqlType::Int(64));
        assert_eq!(t.0.columns[1].1.data_type, BqlType::LowCardinalityString);
        assert_eq!(t.0.columns[2].1.data_type, BqlType::String);
        assert_eq!(t.0.columns[3].1.data_type, BqlType::Decimal(10, 5));
        assert_eq!(t.0.tab_info.partition_cols, "ds,");
        for i in 0u32..4 {
            let od = t.0.columns[i as usize].1.ordinal;
            assert_eq!(od, i);
        }

        let ddl = r##"CREATE TABLE trips_lite_n10
        (
            trip_id UInt32,
            pickup_datetime DateTime
        )
        ENGINE = BaseStorage"##;
        let ps = BqlParser::parse(Rule::cmd_list, ddl)
            .map_err(|e| LangError::CreateTableParsingError)?;
        println!("{}", pretty_parse_tree(ps.clone()));

        let ct = ps
            .into_iter()
            .next()
            .ok_or(LangError::CreateTableParsingError)?;
        println!("{:?}", ct.clone().as_rule());
        let t = parse_create_table(ct)?;
        println!("{:?}", t);
        assert_eq!(t.0.columns.len(), 2);
        assert_eq!(t.0.columns[0].0, "trip_id"); //FIXME remove `` when parsing id
        assert_eq!(t.0.columns[0].1.data_type, BqlType::UInt(32));
        assert_eq!(t.0.columns[1].1.data_type, BqlType::DateTime);

        for i in 0u32..2 {
            let od = t.0.columns[i as usize].1.ordinal;
            assert_eq!(od, i);
        }

        Ok(())
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
        let mut queries =
            BqlParser::parse(Rule::query, sql).unwrap_or_else(|e| panic!("{}", e));
        let limit = seek_to(&mut queries, Rule::limit);
        println!("{:?}", queries);
    }

    #[test]
    fn test_parse_select_remote() -> LangResult<()> {
        let pairs = BqlParser::parse(
            Rule::query,
            "SELECT * from remote('127.0.0.1', default.test)",
        )
        .unwrap_or_else(|e| panic!("{}", e));
        let r = parse_query(pairs.peek().unwrap())?.format;
        assert_eq!(
            r,
            QueryFormat::Remote(RemoteDbInfo {
                database_name: "default".into(),
                table_name: "test".into(),
                addrs: vec![RemoteAddr {
                    ip_addr: Some("127.0.0.1".parse().unwrap()),
                    host_name: None,
                    port: None
                }],
                username: None,
                password: None
            })
        );

        let pairs =
            BqlParser::parse(Rule::query, "SELECT * from remote('127.0.0.1:9528', test)")
                .unwrap_or_else(|e| panic!("{}", e));

        let r = parse_query(pairs.peek().unwrap())?.format;
        assert_eq!(
            r,
            QueryFormat::Remote(RemoteDbInfo {
                database_name: "default".into(),
                table_name: "test".into(),
                addrs: vec![RemoteAddr {
                    ip_addr: Some("127.0.0.1".parse().unwrap()),
                    host_name: None,
                    port: Some(9528)
                }],
                username: None,
                password: None
            })
        );

        let pairs = BqlParser::parse(
            Rule::query,
            "SELECT * from remote('127.0.0.1:9528', test, 'username', 'password')",
        )
        .unwrap_or_else(|e| panic!("{}", e));

        let r = parse_query(pairs.peek().unwrap())?.format;
        assert_eq!(
            r,
            QueryFormat::Remote(RemoteDbInfo {
                database_name: "default".into(),
                table_name: "test".into(),
                addrs: vec![RemoteAddr {
                    ip_addr: Some("127.0.0.1".parse().unwrap()),
                    host_name: None,
                    port: Some(9528)
                }],
                username: Some("username".into()),
                password: Some("password".into())
            })
        );

        let pairs = BqlParser::parse(
            Rule::query,
            "SELECT * from remote('[::1]', test, 'username', 'password')",
        )
        .unwrap_or_else(|e| panic!("{}", e));

        let r = parse_query(pairs.peek().unwrap())?.format;
        assert_eq!(
            r,
            QueryFormat::Remote(RemoteDbInfo {
                database_name: "default".into(),
                table_name: "test".into(),
                addrs: vec![RemoteAddr {
                    ip_addr: Some("::1".parse().unwrap()),
                    host_name: None,
                    port: None
                }],
                username: Some("username".into()),
                password: Some("password".into())
            })
        );

        let pairs = BqlParser::parse(
            Rule::query,
            "SELECT * from remote('[2a02:6b8:0:1111::11]:9528', test)",
        )
        .unwrap_or_else(|e| panic!("{}", e));

        let r = parse_query(pairs.peek().unwrap())?.format;
        assert_eq!(
            r,
            QueryFormat::Remote(RemoteDbInfo {
                database_name: "default".into(),
                table_name: "test".into(),
                addrs: vec![RemoteAddr {
                    ip_addr: Some("2a02:6b8:0:1111::11".parse().unwrap()),
                    host_name: None,
                    port: Some(9528)
                }],
                username: None,
                password: None
            })
        );

        let pairs = BqlParser::parse(
            Rule::query,
            "SELECT * from remote('localhost', default.test, 'username', 'password')",
        )
        .unwrap_or_else(|e| panic!("{}", e));

        let r = parse_query(pairs.peek().unwrap())?.format;
        assert_eq!(
            r,
            QueryFormat::Remote(RemoteDbInfo {
                database_name: "default".into(),
                table_name: "test".into(),
                addrs: vec![RemoteAddr {
                    ip_addr: None,
                    host_name: Some("localhost".into()),
                    port: None
                }],
                username: Some("username".into()),
                password: Some("password".into())
            })
        );

        let pairs = BqlParser::parse(
	    Rule::query,
	    "SELECT * from remote('proxy2.db.tensorbase.io,localhost,test.io,[::1]:123', cloud.test, 'username', 'password')")
	    .unwrap_or_else(|e| panic!("{}", e));

        let r = parse_query(pairs.peek().unwrap())?.format;
        assert_eq!(
            r,
            QueryFormat::Remote(RemoteDbInfo {
                database_name: "cloud".into(),
                table_name: "test".into(),
                addrs: vec![
                    RemoteAddr {
                        ip_addr: None,
                        host_name: Some("proxy2.db.tensorbase.io".into()),
                        port: None
                    },
                    RemoteAddr {
                        ip_addr: None,
                        host_name: Some("localhost".into()),
                        port: None
                    },
                    RemoteAddr {
                        ip_addr: None,
                        host_name: Some("test.io".into()),
                        port: None
                    },
                    RemoteAddr {
                        ip_addr: Some("::1".parse().unwrap()),
                        host_name: None,
                        port: Some(123)
                    },
                ],
                username: Some("username".into()),
                password: Some("password".into())
            })
        );

        Ok(())
    }

    mod bql {
        use super::{pretty_parse_tree, BqlParser, Rule};
        use pest::Parser;

        macro_rules! assert_parse {
            ($s:expr, $c:ident) => {
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
            };
        }

        // #[test]
        // fn xxx() {
        //     enum Rule {
        //         a
        //     }

        //     let input = "";
        //     let pairs = pest::state(input, |state| {
        //         // generating Token pair with Rule::a ...
        //         state.match_insensitive("xxx")
        //     }).ok_or(LangError::CreateTableParsingError)?;
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

            let c = "use some_database_123";
            let pairs =
                BqlParser::parse(Rule::use_db, c).unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));

            let c = "SELECT sum(a) FROM lineorder WHERE toYear(b) > 1990";
            // let c = "select a from tab WHERE toYear(b) > 1990";
            let pairs =
                BqlParser::parse(Rule::query, c).unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));
        }

        #[test]
        fn test_parse_insert_into() {
            //INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13)
            //INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
            //INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13),
            let c = r"INSERT INTO db.table (c1, c2, c3) VALUES";
            let pairs = BqlParser::parse(Rule::insert_into, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("pairs: {}", pretty_parse_tree(pairs));

            assert_parse!("INSERT INTO db.table VALUES", insert_into);
            assert_parse!("INSERT INTO table VALUES", insert_into);
            assert_parse!("INSERT INTO table FORMAT VALUES", insert_into);
            assert_parse!("INSERT INTO table FORMAT CSV", insert_into);

            assert_parse!("INSERT INTO table (c1) VALUES", insert_into);

            // let c = r"INSERT INTO table VALUES";
            let c = r"INSERT INTO table FORMAT CSV";
            let pairs = BqlParser::parse(Rule::insert_into, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("pairs: {}", pretty_parse_tree(pairs));

            let c = r"INSERT INTO db.table VALUES (1,2), (3,4)";
            let pairs = BqlParser::parse(Rule::insert_into, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("pairs: {}", pretty_parse_tree(pairs));
        }

        #[test]
        fn test_comment_parse() {
            let c = r"-- abci
CREATE TABLE test (col Int32)";
            let pairs =
                BqlParser::parse(Rule::cmd_list, c).unwrap_or_else(|e| panic!("{}", e));
            println!("pairs: {}", pretty_parse_tree(pairs));
        }

        #[test]
        fn test_expr() {
            assert_parse!(
                "case when (d_day_name='Sunday') then sales_price else null end",
                case_expr
            );
            assert_parse!("case when 1 then 2 else 3 end", case_expr);
            assert_parse!("case mean when 0 then null else stdev/mean end", case_expr);
        }

        #[test]
        fn test_logical_expr() {
            let c = "x <= y";
            assert_parse!(c, logical_expr);

            let c = "(x = y) AND z";
            assert_parse!(c, logical_expr);

            let c = "n2.n_name = 'FRANCE'
     AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'";
            assert_parse!(c, logical_expr);

            let c = "(a=b AND a=c) OR (a=d and a=e) ";
            // let pairs = BqlParser::parse(Rule::logical_expr, c)
            //     .unwrap_or_else(|e| panic!("{}", e));
            // println!("{}", pretty_parse_tree(pairs));
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
            let pairs = BqlParser::parse(Rule::logical_expr, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));
            assert_parse!(c, logical_expr);
            /*
            - logical_expr > and_logical_exprs_with_or
              - and_logical_expr
                - logical_val > comp_expr > comp_expr_cmp
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "p_partkey"
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "l_partkey"
                - and_logical_val > logical_val > comp_expr > comp_expr_cmp
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "p_brand"
                  - expr > arith_expr > arith_expr_item > literal > string_literal: "\'Brand#12\'"
                - and_logical_val > logical_val > comp_expr > in_expr
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "p_container"
                  - const_str_array
                    - string_literal: "\'SM CASE\'"
                    - string_literal: "\'SM BOX\'"
                    - string_literal: "\'SM PACK\'"
                    - string_literal: "\'SM PKG\'"
                - and_logical_val > logical_val > comp_expr > comp_expr_cmp
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "l_quantity"
                  - expr > arith_expr > arith_expr_item > literal > signed_number: "1"
                - and_logical_val > logical_val > comp_expr > comp_expr_cmp
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "l_quantity"
                  - expr > arith_expr > arith_expr_item
                    - literal > signed_number: "1"
                    - arith_op: "+"
                    - literal > signed_number: "10"
                - and_logical_val > logical_val > comp_expr > between_expr
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "p_size"
                  - expr > arith_expr > arith_expr_item > literal > signed_number: "1"
                  - expr > arith_expr > arith_expr_item > literal > signed_number: "5"
                - and_logical_val > logical_val > comp_expr > in_expr
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "l_shipmode"
                  - const_str_array
                    - string_literal: "\'AIR\'"
                    - string_literal: "\'AIR REG\'"
                - and_logical_val > logical_val > comp_expr > comp_expr_cmp
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "l_shipinstruct"
                  - expr > arith_expr > arith_expr_item > literal > string_literal: "\'DELIVER IN PERSON\'"
              - and_logical_expr
                - logical_val > comp_expr > comp_expr_cmp
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "p_partkey"
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "l_partkey"
                - and_logical_val > logical_val > comp_expr > comp_expr_cmp
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "p_brand"
                  - expr > arith_expr > arith_expr_item > literal > string_literal: "\'Brand#23\'"
                - and_logical_val > logical_val > comp_expr > in_expr
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "p_container"
                  - const_str_array
                    - string_literal: "\'MED BAG\'"
                    - string_literal: "\'MED BOX\'"
                    - string_literal: "\'MED PKG\'"
                    - string_literal: "\'MED PACK\'"
                - and_logical_val > logical_val > comp_expr > comp_expr_cmp
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "l_quantity"
                  - expr > arith_expr > arith_expr_item > literal > signed_number: "10"
                - and_logical_val > logical_val > comp_expr > comp_expr_cmp
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "l_quantity"
                  - expr > arith_expr > arith_expr_item
                    - literal > signed_number: "10"
                    - arith_op: "+"
                    - literal > signed_number: "10"
                - and_logical_val > logical_val > comp_expr > between_expr
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "p_size"
                  - expr > arith_expr > arith_expr_item > literal > signed_number: "1"
                  - expr > arith_expr > arith_expr_item > literal > signed_number: "10"
                - and_logical_val > logical_val > comp_expr > in_expr
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "l_shipmode"
                  - const_str_array
                    - string_literal: "\'AIR\'"
                    - string_literal: "\'AIR REG\'"
                - and_logical_val > logical_val > comp_expr > comp_expr_cmp
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "l_shipinstruct"
                  - expr > arith_expr > arith_expr_item > literal > string_literal: "\'DELIVER IN PERSON\'"
              - and_logical_expr
                - logical_val > comp_expr > comp_expr_cmp
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "p_partkey"
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "l_partkey"
                - and_logical_val > logical_val > comp_expr > comp_expr_cmp
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "p_brand"
                  - expr > arith_expr > arith_expr_item > literal > string_literal: "\'Brand#34\'"
                - and_logical_val > logical_val > comp_expr > in_expr
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "p_container"
                  - const_str_array
                    - string_literal: "\'LG CASE\'"
                    - string_literal: "\'LG BOX\'"
                    - string_literal: "\'LG PACK\'"
                    - string_literal: "\'LG PKG\'"
                - and_logical_val > logical_val > comp_expr > comp_expr_cmp
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "l_quantity"
                  - expr > arith_expr > arith_expr_item > literal > signed_number: "20"
                - and_logical_val > logical_val > comp_expr > comp_expr_cmp
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "l_quantity"
                  - expr > arith_expr > arith_expr_item
                    - literal > signed_number: "20"
                    - arith_op: "+"
                    - literal > signed_number: "10"
                - and_logical_val > logical_val > comp_expr > between_expr
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "p_size"
                  - expr > arith_expr > arith_expr_item > literal > signed_number: "1"
                  - expr > arith_expr > arith_expr_item > literal > signed_number: "15"
                - and_logical_val > logical_val > comp_expr > in_expr
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "l_shipmode"
                  - const_str_array
                    - string_literal: "\'AIR\'"
                    - string_literal: "\'AIR REG\'"
                - and_logical_val > logical_val > comp_expr > comp_expr_cmp
                  - expr > arith_expr > arith_expr_item > qualified_name > id: "l_shipinstruct"
                  - expr > arith_expr > arith_expr_item > literal > string_literal: "\'DELIVER IN PERSON\'"
            */

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
            assert_parse!(c, logical_expr);

            let c = "x > y";
            assert_parse!(c, logical_expr);
            // let pairs = BqlParser::parse(Rule::logical_expr, c)
            //     .unwrap_or_else(|e| panic!("{}", e));
            // println!("{}", pretty_parse_tree(pairs));
            //- logical_expr > logical_val > comp_expr
            //   - expr > arith_expr > arith_expr_item > qualified_name > id: "x"
            //   - expr > arith_expr > arith_expr_item > qualified_name > id: "y"
        }

        #[test]
        fn test_select() {
            assert_parse!("SELECT 1", select);
            assert_parse!("SELECT 1, 'test'", select);

            assert_parse!("SELECT * FROM test WHERE 1", select);

            let c =
                "SELECT t1.c1 as t1c1 FROM test WHERE 1 GROUP BY id HAVING count(*) > 1";
            let pairs =
                BqlParser::parse(Rule::select, c).unwrap_or_else(|e| panic!("{}", e));
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
        fn test_select_with_end_semicolon() {
            assert_parse!("SELECT 1;", select);
            assert_parse!("SELECT 1, 'test';", select);

            assert_parse!("SELECT * FROM test WHERE 1;", select);

            let c =
                "SELECT t1.c1 as t1c1 FROM test WHERE 1 GROUP BY id HAVING count(*) > 1;";
            let pairs =
                BqlParser::parse(Rule::select, c).unwrap_or_else(|e| panic!("{}", e));
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
              and r_reason_desc = 'Package was damaged';";

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
            assert_parse!("with bar as (select 1 from foo) select 2 from bar", query);

            let c = "SELECT sum(a) FROM lineorder WHERE toYear(b) > 1990";
            let pairs =
                BqlParser::parse(Rule::query, c).unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));
        }

        #[test]
        fn test_query_with_end_semicolon() {
            assert_parse!("SELECT 1;", cmd);
            assert_parse!("SELECT * FROM test ORDER BY 1;", query);
            assert_parse!("SELECT * FROM test ORDER BY 1, id;", query);
            assert_parse!("SELECT * FROM test LIMIT 1;", query);
            assert_parse!(
                "select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
       ,stdev,mean, case mean when 0 then null else stdev/mean end cov;",
                query
            );
            assert_parse!("with bar as (select 1 from foo) select 2 from bar;", query);

            let c = "SELECT sum(a) FROM lineorder WHERE toYear(b) > 1990;";
            let pairs =
                BqlParser::parse(Rule::query, c).unwrap_or_else(|e| panic!("{}", e));
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
            let pairs =
                BqlParser::parse(Rule::with, c).unwrap_or_else(|e| panic!("{}", e));
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
            let pairs =
                BqlParser::parse(Rule::with, c).unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));

            let c = "inv as (select case mean when 0 then null else stdev/mean end cov from foo)";
            let pairs =
                BqlParser::parse(Rule::with_query, c).unwrap_or_else(|e| panic!("{}", e));
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
            let pairs =
                BqlParser::parse(Rule::with, c).unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));
        }

        #[test]
        fn test_select_with_remote_func() {
            assert_parse!("SELECT * from remote('127.0.0.1', default.test)", select);
            assert_parse!(
                "SELECT * from remote('127.0.0.1:9528', default.test)",
                select
            );
            assert_parse!("SELECT * from remote('127.0.0.1:9528', test)", select);
            assert_parse!(
                "SELECT * from remote('127.0.0.1:9528', test, 'username', 'password')",
                select
            );
            assert_parse!(
                "SELECT * from remote('[::1]', test, 'username', 'password')",
                select
            );
            assert_parse!(
                "SELECT * from remote('[2a02:6b8:0:1111::11]:9528', test)",
                select
            );
            assert_parse!("SELECT * from remote('[2a02:6b8:0:1111::11]:9528', default.test, 'username', 'password')", select);
            assert_parse!(
                "SELECT * from remote('localhost', default.test, 'username', 'password')",
                select
            );
            assert_parse!("SELECT * from remote('tensorbase.io', default.test, 'username', 'password')", select);
            assert_parse!("SELECT * from remote('db.tensorbase.io', default.test, 'username', 'password')", select);
            assert_parse!("SELECT * from remote('proxy1.db.tensorbase.io', default.test, 'username', 'password')", select);
            assert_parse!("SELECT * from remote('proxy2.db.tensorbase.io', default.test, 'username', 'password')", select);
            assert_parse!("SELECT * from remote('proxy2.db.tensorbase.io,localhost,test.io,[::1]:123', default.test, 'username', 'password')", select);
        }

        #[test]
        fn test_create_table() {
            assert_parse!("CREATE TABLE test (col Int32)", create_table);
            assert_parse!("CREATE TABLE main.test (col Decimal(7,2))", create_table);
            assert_parse!(
                "CREATE TABLE test (id Int256 PRIMARY KEY, name String NOT NULL)",
                create_table
            );
            assert_parse!("CREATE TABLE IF NOT EXISTS test (col String)", create_table);

            assert_parse!("CREATE TABLE test (id UInt256)", create_table);
            assert_parse!("CREATE TABLE test (id Int64)", create_table);
            assert_parse!("CREATE TABLE test (id String)", create_table);
            assert_parse!("CREATE TABLE test (id Float64)", create_table);
            assert_parse!("CREATE TABLE test (id Decimal(12,5))", create_table);

            let c = "CREATE TABLE test (id Int64 default 123)";
            assert_parse!(c, create_table);

            let c = "CREATE TABLE test (id Int64 PRIMARY KEY)";
            let pairs = BqlParser::parse(Rule::create_table, c)
                .unwrap_or_else(|e| panic!("{}", e));

            //support table_attributes
            let c = "CREATE TABLE IF NOT EXISTS payment (a UInt16) Engine=Memory";
            // let pairs = BqlParser::parse(Rule::create_table, c)
            //     .unwrap_or_else(|e| panic!("{}", e));
            // println!("{}", pretty_parse_tree(pairs));
            assert_parse!(c, create_table);

            let c = "SETTINGS a=32768";
            let pairs = BqlParser::parse(Rule::table_attr_settings, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));

            let c = r##"create TABLE some_tab_v2
            (
                `did` Int64,
                `reason` LowCardinality(String)
            )
            ENGINE = BaseStorage
            PARTITION BY toYYYYMM(ds)
            SETTINGS a=32768,
            b=0"##;
            let pairs = BqlParser::parse(Rule::create_table, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));

            let c = r##"create TABLE some_tab_v2
            (
                `did` Int64,
                `reason` LowCardinality(String)
            )
            ENGINE = BaseStorage
            PARTITION BY mod(uuid, 100000)
            SETTINGS a=32768,
            b=0"##;
            let pairs = BqlParser::parse(Rule::create_table, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));

            //
            let c = "CREATE DATABASE a";
            assert_parse!(c, create_database);
            let c = "CREATE DATABASE if not exists a";
            let pairs = BqlParser::parse(Rule::create_database, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));

            let c = "drop TABLE a";
            assert_parse!(c, drop_table);

            let c = "drop database a";
            assert_parse!(c, drop_database);

            let c = "drop database if exists xxx ";
            let pairs = BqlParser::parse(Rule::drop_database, c)
                .unwrap_or_else(|e| panic!("{}", e));
            println!("{}", pretty_parse_tree(pairs));
            assert_parse!(c, drop_database);
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
            let pairs =
                BqlParser::parse(Rule::cmd, c).unwrap_or_else(|e| panic!("{}", e));
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
            let pairs =
                BqlParser::parse(Rule::cmd, c).unwrap_or_else(|e| panic!("{}", e));
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
            let pairs =
                BqlParser::parse(Rule::cmd, c).unwrap_or_else(|e| panic!("{}", e));
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
