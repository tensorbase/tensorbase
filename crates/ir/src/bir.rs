/*
*   Copyright (c) 2020 TensorBase, and its contributors
*   All rights reserved.

*   Licensed under the Apache License, Version 2.0 (the "License");
*   you may not use this file except in compliance with the License.
*   You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*   Unless required by applicable law or agreed to in writing, software
*   distributed under the License is distributed on an "AS IS" BASIS,
*   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*   See the License for the specific language governing permissions and
*   limitations under the License.
*/

/**
 * design:
 *  parse tree -> (transform) -> bir -> (rewrite ( multi-passes) ) ->
 * optimized bir -> codegen
 *
 * bir:
 *   op:  -> map (unary);  + union (binary); * join (binary)； <>
 * top (unary)   data:  tensor? (collection of col/vec/array) with
 * schema
 *
 */
use crate::intern::{Interner, Sym};
use crate::{
    meta::schemas::{Catalog, ColumnId, ColumnType},
    parse::*,
};
use base::debug;
use base::strings::remove_whitespace;
use pest_derive::Parser;
use std::collections::{HashMap, HashSet, LinkedList};
use typed_arena::Arena;

#[inline]
fn normalize(s: &mut String) {
    remove_whitespace(s);
}

/// (output_column_id, fn_body_input_included)
// common fn (scalar fn) works on cell
// agg fn (vector fn) works on column
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fn(pub Sym); //FIXME SymbolKind?

impl Fn {
    pub fn to_string(&self, inter: &Interner) -> String {
        // inter.resolve(self.0).to_owned() + "=" + inter.resolve(self.1)
        inter.resolve(self.0).to_owned()
    }
}

//FIXME support multiple aggs in one out column
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct AggInfo {
    pub agg_fn: Sym,
    pub inside: Sym,
    pub outside: Sym,
    pub inside_offset: u32,
    pub outside_offset: u32,
    pub fn_span_start: u32,
    pub fn_span_end: u32,
    pub syms: Sym, //FIXME multiple syms
    pub syms_span_start: u32,
    pub syms_span_end: u32,
}

impl Default for AggInfo {
    fn default() -> Self {
        AggInfo {
            agg_fn: 0,
            inside: 0,
            outside: 0,
            inside_offset: 0,
            outside_offset: 0,
            fn_span_start: 0,
            fn_span_end: 0,
            syms_span_start: 0,
            syms_span_end: 0,
            syms: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub enum Data {
    Init,
    Name(Sym),
    Column(ColumnId, ColumnType, Sym),
    Expr(Sym),
    Agg(AggInfo),
    Derived,  //TODO
    SubQuery, //TODO SubQueryAlias?
}

impl Data {
    pub fn resolve_as_column(
        self,
        tabs: &Vec<Sym>,
        inter: &mut Interner,
        cat: &Catalog,
    ) -> Data {
        match self {
            Data::Name(sym) => {
                let name = inter.resolve(sym).to_string();
                let ns: Vec<&str> = name.split('.').collect();
                match ns.as_slice() {
                    &[t, c] => {
                        let sym_t = inter.intern(t);
                        if !tabs.contains(&sym_t) {
                            panic!(
                                "table: {} is not specified in from clause!",
                                t
                            )
                        }
                        match cat.get_table_by_name(t) {
                            Some(tab) => {
                                if let Some(col) = tab.get_column_by_name(c) {
                                    Data::Column(col.id, col.data_type, sym)
                                } else {
                                    panic!("can not resolve column: {}!", c)
                                }
                            }
                            None => panic!("can not resolve table: {}!", t),
                        }
                    }
                    &[c] => {
                        let tns: Vec<&str> =
                            tabs.iter().map(|s| inter.resolve(*s)).collect();
                        let mut rt = Data::Init;
                        for tn in tns {
                            if let Some(tab) = cat.get_table_by_name(tn) {
                                if let Some(col) = tab.get_column_by_name(c) {
                                    rt = Data::Column(
                                        col.id,
                                        col.data_type,
                                        sym,
                                    );
                                    break;
                                }
                            }
                        }
                        if rt == Data::Init {
                            panic!("can not resolve column: {}!", c);
                        } else {
                            rt
                        }
                    }
                    _ => panic!("unsupported qualified name!"),
                }
            }
            // Data::Column(_) => { self }
            _ => unimplemented!(
                "only support to resolve the raw symbol to column!"
            ),
        }
    }
}

trait PrettyFormatWithInterner {
    fn pretty_format(&self, inter: &Interner) -> String;
}

impl PrettyFormatWithInterner for &Data {
    fn pretty_format(&self, inter: &Interner) -> String {
        match self {
            Data::Column(.., sym) => "(".to_owned() + inter.resolve(*sym) + ")",
            Data::Name(s) | Data::Expr(s) => inter.resolve(*s).to_owned(),
            Data::Agg(ref agg_info) => {
                inter.resolve(agg_info.outside).to_owned()
            }
            _ => {
                debug!(self);
                todo!()
            }
        }
    }
}

//FIXME ???
impl PrettyFormatWithInterner for &AggInfo {
    fn pretty_format(&self, inter: &Interner) -> String {
        inter.resolve(self.outside).to_owned()
    }
}

// #[derive(Clone, PartialEq, Eq)]
pub enum Op {
    OpNoop,
    OpMap(Vec<Fn>),
    OpAgg(Vec<Data>),
    OpTop {
        keys: Vec<Sym>,
        kinds: Vec<Sym>,
        limit: usize,
    },
    // OpJoin(OpJoin),
}

// BHIR: capture all semantics from parse tree
// #[derive(Clone)]
pub struct Node {
    pub data_in: HashSet<Data>,
    pub data_out: HashSet<Data>,
    pub op: Op,
}

fn pretty_format_iter<T: PrettyFormatWithInterner>(
    iter: impl std::iter::Iterator<Item = T>,
    inter: &Interner,
) -> String {
    iter.map(|s| s.pretty_format(inter))
        .collect::<Vec<String>>()
        .join(", ")
}

impl PrettyFormatWithInterner for &Node {
    fn pretty_format(&self, inter: &Interner) -> String {
        let Node {
            data_in,
            data_out,
            op,
        } = self;
        match op {
            Op::OpMap(v) => format!(
                "-> IN({})  OUT({})  [{}]",
                data_in
                    .iter()
                    .map(|s| s.pretty_format(inter))
                    .collect::<Vec<String>>()
                    .join(", "),
                data_out
                    .iter()
                    .map(|s| s.pretty_format(inter))
                    .collect::<Vec<String>>()
                    .join(", "),
                v.iter()
                    .map(|f| f.to_string(inter))
                    .collect::<Vec<String>>()
                    .join(", "),
            ),
            Op::OpAgg(keys) => format!(
                "+  IN({}) OUT({})  KEYS[{}]  AGG?",
                data_in
                    .iter()
                    .map(|s| s.pretty_format(inter))
                    .collect::<Vec<String>>()
                    .join(", "),
                data_out
                    .iter()
                    .map(|s| s.pretty_format(inter))
                    .collect::<Vec<String>>()
                    .join(", "),
                keys.iter()
                    .map(|f| f.pretty_format(inter))
                    .collect::<Vec<String>>()
                    .join(", "),
                // agg.pretty_format(inter),
            ),
            _ => todo!(),
        }
    }
}

// #[derive(Default)]
pub struct Pipeline {
    pub data_in: HashSet<Data>,
    pub data_out: HashSet<Data>,
    pub nodes: Vec<Node>,
    /* pub kind: PipelineKind,
     * pub node_agg: Option<Node>, */
}

/*
 * an upgraded version of map-reduce...
 * but let's just keep well known terms now
 */
pub enum PipelineOptKind {
    Init,
    MapKind(Map),
    ReduceKind(Reduce),
}

pub struct Map {
    map: Vec<Fn>,
}

pub struct Reduce {
    pub keys: Vec<Data>,
    pub aggs: AggInfo,
}

impl<'p> Default for Pipeline {
    fn default() -> Self {
        Pipeline {
            data_in: Default::default(),
            data_out: Default::default(),
            nodes: Default::default(),
            /* kind: PipelineKind::Map,
             * node_agg: None, */
        }
    }
}

impl Pipeline {
    pub fn get_agg_node(&mut self) -> Option<&mut Node> {
        for node in &mut self.nodes {
            if let Op::OpAgg { .. } = node.op {
                return Some(node);
            } else {
                continue;
            }
        }
        None
    }

    pub fn get_op_agg_info(&self) -> AggInfo {
        for data in &self.data_out {
            if let Data::Agg(agg) = data {
                return *agg;
            } else {
                continue;
            }
        }
        unreachable!("can not find OpAgg?")
    }
}

pub struct Context<'a> {
    pub inter: Interner<'a>,
    pub aliases: HashMap<Sym, (Data, Sym)>, //alias to origin
    pub tabs: Vec<Sym>,
    pub query: Pipeline,
    pub pigback_op_in: HashSet<Data>,
    pub pigback_data: Data,
    pub pigback_has_agg: bool, /*FIXME unnecessary?
                                * pub pigback_has_group_by: bool, */
}

impl Context<'_> {
    /*
     canonization of bir symbol:
     1. all ids of columns refer to qualified_names

     rules of the generation of bir:
     * reject and abandon for any ambiguity/unsupported cases
    */
    fn traverse(&mut self, pair: Pair<Rule>) {
        let r = pair.as_rule();
        //pre
        match r {
            // Rule::select => {}
            Rule::func_name => {
                let fname = pair.as_str();
                match fname {
                    "sum" | "count" | "avg" | "max" | "min" => {
                        let mut agg = AggInfo::default();
                        agg.agg_fn = self.inter.intern(fname);
                        self.pigback_data = Data::Agg(agg);
                        self.pigback_has_agg = true;
                    }
                    _ => {}
                }
            }
            Rule::where_clause => {
                self.pigback_op_in.clear();
            }
            Rule::group_by => {
                self.pigback_op_in.clear();
            }
            _ => {}
        }

        for p in pair.clone().into_inner() {
            self.traverse(p);
        }

        //post
        match r {
            Rule::select => {}
            Rule::select_column => {
                //for data_out
                let mut dout = &mut self.query.data_out;
                let children: &[_] = &pair.into_inner().collect::<Vec<_>>();
                match children {
                    //FIXME !!! select
                    [e] => {
                        // debug!(&self.pigback_op_in);
                        // debug!(e);
                        let data = self.pigback_data;
                        dout.insert(data);
                        // let mut expr = e.as_str().to_owned();
                        // normalize(&mut expr);
                    }
                    [e, a] => {
                        let mut expr = e.as_str().to_owned();
                        normalize(&mut expr);
                        let symb_expr = self.inter.intern(&expr);
                        let symb_as = self.inter.intern(a.as_str());
                        todo!()
                        // let kind = self.pigback_data;
                        // self.aliases.insert(symb_as, (kind, symb_expr));
                        // match kind {
                        //     Data::ColumnSym(..) => {
                        //         //FIXME
                        //         // qout.insert(Data::ColumnSym(0, symb_as));
                        //         todo!()
                        //     }
                        //     SymbolKind::Expr => {
                        //         qout.insert(Data::ExprSym(symb_as));
                        //     }
                        //     SymbolKind::Agg(agg_info) => {
                        //         qout.insert(Data::AggSym(symb_as));
                        //         self.pigback_has_agg = true;
                        //     }
                        //     _ => todo!(),
                        // }
                    }
                    _ => unreachable!(),
                }
            }
            // Rule::arith_op => self.pigback_data = Data::Expr(0),
            Rule::expr => {
                if let Data::Agg(ref mut agg_info) = self.pigback_data {
                    let mut s = pair.as_str().to_owned();
                    // normalize(&mut s);
                    agg_info.outside = self.inter.intern(&s);
                    agg_info.outside_offset = pair.as_span().start() as u32;
                } else if let Data::Expr(ref mut expr) = self.pigback_data {
                    //FIXME !!! not work now
                    let mut s = pair.as_str().to_owned();
                    // normalize(&mut s);
                    *expr = self.inter.intern(&s);
                }
            }
            Rule::func_call_expr => {
                if let Data::Agg(ref mut agg_info) = self.pigback_data {
                    let span = pair.as_span();
                    agg_info.fn_span_start = span.start() as u32;
                    agg_info.fn_span_end = span.end() as u32;
                }
            }
            Rule::func_arg => {
                if let Data::Agg(ref mut agg_info) = self.pigback_data {
                    let mut s = pair.as_str().to_owned();
                    // normalize(&mut s);
                    agg_info.inside = self.inter.intern(&s);
                    agg_info.inside_offset = pair.as_span().start() as u32;
                }
            }
            // Rule::func_name => {}
            Rule::qualified_name => {
                //for op_in
                let mut qin = &mut self.query.data_in;
                let span = &pair.as_span();
                let mut symbs = (0, 0);
                let mut qn = pair.as_str().to_string();
                normalize(&mut qn);
                let col_sym = self.inter.intern(&qn);
                let d = Data::Name(col_sym);
                qin.insert(d);
                self.pigback_op_in.insert(d);
                // debug!(self.pigback_has_agg);
                if let Data::Agg(ref mut agg_info) = self.pigback_data {
                    agg_info.syms_span_start = span.start() as u32;
                    agg_info.syms_span_end = span.end() as u32;
                    agg_info.syms = col_sym;
                }
                return; //inner most
            }
            Rule::where_clause => {
                //FIXME //TEMP
                // let out = HashSet::with_capacity(0);
                let wh = pair.into_inner().next().unwrap();
                let fns = vec![Fn(
                    // 0 as Symbol,
                    self.inter.intern(debug!(wh.as_str())),
                )];
                self.query.nodes.push(Node {
                    data_in: self.pigback_op_in.clone(), //FIXME
                    data_out: Default::default(),        //FIXME
                    op: Op::OpMap(fns),
                });
            }
            Rule::group_by => {
                //TEMP
                assert!(
                    self.pigback_has_agg,
                    "should has some explicit aggregation expr in select clause"
                );
                //FIXME more validation:
                //  1. any in op_in is Column or Expr
                //  2. any in op_in is not in Agg-kind out of query
                //note: here op_in != keys
                //move all Agg-kind out here?
                let keys: HashSet<_> = pair
                    .into_inner()
                    .into_iter()
                    .map(|p| {
                        let mut s = p.as_str().to_owned();
                        normalize(&mut s);
                        Data::Name(self.inter.intern(&s))
                    })
                    .collect();
                let op_in = self.pigback_op_in.clone();
                let agg_node = Node {
                    data_in: op_in,               //FIXME
                    data_out: Default::default(), //FIXME
                    op: Op::OpNoop,
                };
                self.query.nodes.push(agg_node);
            }
            _ => {}
        }
    }

    pub(crate) fn optimize(&mut self) -> PipelineOptKind {
        if self.pigback_has_agg {
            let agg_info = self.query.get_op_agg_info();
            let node_agg = self.query.get_agg_node();
            if let Some(node) = node_agg {
                if let Op::OpAgg(keys) = &node.op {
                    PipelineOptKind::ReduceKind(Reduce {
                        aggs: agg_info,
                        keys: keys.clone(),
                    })
                } else {
                    unimplemented!()
                }
            } else {
                //naive agg
                PipelineOptKind::ReduceKind(Reduce {
                    aggs: agg_info,
                    keys: Vec::with_capacity(0),
                })
            }
        } else {
            todo!()
        }
    }

    fn parse_tables(&mut self, pair: Pair<Rule>) {
        for p in pair.clone().into_inner() {
            self.parse_tables(p);
        }
        //post
        match pair.as_rule() {
            Rule::join_table => {
                let mut children = pair.into_inner();
                let mut qt = children.next().unwrap().as_str().to_owned();
                normalize(&mut qt);
                //FIXME check tab
                let tab = self.inter.intern(&qt);
                self.tabs.push(tab);

                if let Some(p) = children.next() {
                    debug_assert!(p.as_rule() == Rule::as_qualif);
                    let symb_as = self.inter.intern(debug!(p
                        .into_inner()
                        .next()
                        .unwrap()
                        .as_str()));
                    self.aliases.insert(symb_as, (Data::SubQuery, tab)); //FIXME
                }
                return;
            }
            _ => {}
        }
    }

    pub(crate) fn pretty_print(&self) {
        println!("--- Alias:");
        for (k, v) in &(self.aliases) {
            print!(
                "{}:{}[{:?}], ",
                self.inter.resolve(*k),
                self.inter.resolve(v.1),
                v.0
            );
        }
        println!("\n\n--- Tables:");
        for tab in &(self.tabs) {
            print!("{}, ", self.inter.resolve(*tab));
        }
        println!("\n\n--- BIR Flow (in -> out):");

        println!(
            "   {}",
            "(".to_owned()
                + &pretty_format_iter((self.query.data_in).iter(), &self.inter)
                + ")"
        );

        //\u{251C}
        for node in &(self.query.nodes) {
            println!("{}", node.pretty_format(&self.inter));
        }

        //\u{2514}
        println!(
            "   {}",
            "(".to_owned()
                + &pretty_format_iter(
                    (self.query.data_out).iter(),
                    &self.inter
                )
                + ")"
        );
    }

    pub fn from<'a>(raw_query: &str, arena: &'a Arena<u8>) -> Context<'a> {
        let mut ctx = Context {
            inter: Interner::new(&arena),
            aliases: Default::default(),
            query: Default::default(),
            tabs: Default::default(),
            pigback_op_in: Default::default(),
            pigback_data: Data::Init,
            pigback_has_agg: false,
            // pigback_has_group_by: false,
        };
        ctx.inter.intern("null"); //FIXME prealloc null in 0th

        let ast0 = BqlParser::parse(Rule::query, raw_query)
            .unwrap_or_else(|e| panic!("{}", e));
        // println!("{}", pretty_parse_tree(ast0.clone()));
        let ast = ast0.into_iter().next().unwrap();
        ctx.parse_tables(ast.clone());
        ctx.traverse(ast.clone());
        ctx
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::parse::*;
    use typed_arena::Arena;

    // #[test]
    // fn test_symbol_kind_from() {
    //     let sql = "avg(c3)";
    //     let ast = BqlParser::parse(Rule::expr, sql)
    //         .unwrap_or_else(|e| panic!("{}", e));
    //     println!("{}", pretty_parse_tree(ast.clone()));
    //     let k: SymbolKind = dbg!(ast.into_iter().next().unwrap().into());
    //     assert!(k == SymbolKind::Agg);
    // }

    #[ignore]
    #[test]
    pub fn basic_check() {
        /*
        select c1, c2*c2 pc2, avg(c3) from tab where c3 > 1 group by c1, pc2 order by c1, pc2 limit 5

            .  (c1,pc2,avg(c3))
         <- <> (c1,pc2,avg(c3)) [key=(c1,pc2), kind=asc, limit=5]
         <- +  (c1,pc2,c3)      [agg_key=(c1,pc2), avg(c3)=(c3,avg)]
         <- -> (c1,pc2,c3)      [c3>1]
         <- -> (c1,c2,c3)       [pc2=c2*c2]
         <- .  ((t,c1),(t,c2),(t,c3))
        */
        let sql = r#"
        select t.c1, 2 * (c2*c2+1) pc2, c1/2 c12, avg(c3), c4 from t where (c3 > 1 and c3 < 11) or (c4-c3)=1 group by c1, pc2 order by c1, pc2 limit 5
"#;
        let arena = Arena::with_capacity(16);
        let ctx = Context::from(sql, &arena);
        ctx.pretty_print();

        /*
        select
          sr_a as a,
          sr_b as b,
          sum(sr_c) as sc
        from
          sr,
          dd
        where
          sr_d = dd_d
          and dd_e = 1998
        group by
          sr_a,
          sr_b
        order by sr_a, sr_b desc
        limit 5

              (sr_a[a],sr_b[b],sum(sr_c)[sc])
        <- <> (sr_a,sr_b,sum(sr_c))            [order_key=(sr_a,sr_b), order_type=desc, order_limit=5]
        <- +  (sr_a,sr_a,sr_c)                 [key=(sr_a,sr_b), sum(sr_c)=(sr_c,sum)]
        <- -> (sr_a,sr_a,sr_c,dd_e)            [dd_e>1]
        <- *  (sr_a,sr_a,sr_c,sr_d,dd_d,dd_e)  [type=inner, keys=[(sr_d,dd_d)]]
        */
    }
}
