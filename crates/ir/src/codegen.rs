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

use crate::{
    bir::*,
    intern::Sym,
    meta::{
        schemas::{ColumnId, ColumnType},
        CAT, CONF,
    },
};
use base::{debug, strings::s};
use typed_arena::Arena;

/*
 * note: may support to gen in two modes: stripped or debug(verbose)
 */
pub fn gen(raw_query: &str) -> String {
    let arena = Arena::with_capacity(32);
    let mut ctx = Context::from(raw_query, &arena);
    // ctx.pretty_print();
    let opt = &mut ctx.optimize();

    let num_parts = debug!(num_cpus::get());

    //FIXME
    //data_in
    let query = ctx.query;
    let mut inter = ctx.inter;
    let tabs = ctx.tabs;
    let cat = CAT.lock().unwrap();
    let data_dirs = &CONF.storage.data_dirs;
    let meta_file = data_dirs.to_string() + "/meta";

    let mut cols: Vec<String> = Vec::new();
    for c in &query.data_in {
        let dc = match c {
            Data::Name(_) => c.resolve_as_column(&tabs, &mut inter, &cat),
            _ => panic!("data_in should be a kind of Data::Name!"),
        };

        if let Data::Column(cid, _, sym) = dc {
            let gen_col = &gen_col(cid);
            let gen_scan_col = s!(
                char *blk_raw_$gen_col$ = NULL;
                char fpath[64];
                sprintf(fpath, "$data_dirs$/%d", $cid$);//TEMP
                ker_scan(&blk_raw_$gen_col$, fpath);
            );
            cols.push(gen_scan_col);
        } else {
            unreachable!("all data_in must be resolved in Data::Column!");
        }
    }
    let scan_body = cols.join("\n\n");

    //TEMP
    let (m_body, reduce_fn): (String, String) = match &opt {
        PipelineOptKind::ReduceKind(reduce) => {
            //navie agg:
            //FIXME
            // 1. may be more than one agg
            // 2. count(column_name) is null-filtered
            if reduce.keys.len() == 0 {
                // debug!(reduce.aggs);
                let agg = reduce.aggs;
                let (gen_col, gen_col_type, gen_blk_len) =
                    &(if let Data::Column(cid, col_typ, _) =
                        Data::Name(agg.syms)
                            .resolve_as_column(&tabs, &mut inter, &cat)
                    {
                        //NAIVE
                        let metas = std::fs::read(meta_file).unwrap();
                        let metas = metas.as_ptr() as *const _ as *const u64;
                        let (col_typ_c, gen_blk_len) = {
                            let blk_siz =
                                unsafe { *metas.offset(cid as isize) as u64 };
                            let blk_len = blk_siz / (col_typ.size() as u64);
                            match col_typ {
                                ColumnType::INT8 => ("int8_t", blk_len),
                                ColumnType::UINT8 => ("uint8_t", blk_len),
                                ColumnType::INT32 => ("int32_t", blk_len),
                                ColumnType::UINT32
                                | ColumnType::UNIX_DATETIME => {
                                    ("uint32_t", blk_len)
                                }
                                _ => todo!("unsupported column type!"),
                            }
                        };
                        debug!((gen_col(cid), col_typ_c, gen_blk_len))
                    } else {
                        panic!("all data_in must be resolved in Data::Column!")
                    });

                let gen_inside = inter.resolve(agg.inside);
                let gen_outside = inter.resolve(agg.outside);
                // debug!(&raw_query[ agg.inside_offset as
                // usize..(agg.fn_span_end as usize)]);
                let ins0 = &gen_inside
                    [..(agg.syms_span_start - agg.inside_offset) as usize];
                let ins1 = &gen_inside
                    [(agg.syms_span_end - agg.inside_offset) as usize..];
                // debug!(ins1);
                let gen_expr_inside_sum = s!($ins0$$gen_col$$ins1$);
                // debug!(gen_expr_inside_sum);
                let ots0 = &gen_outside
                    [..(agg.fn_span_start - agg.outside_offset) as usize];
                let ots1 = &gen_outside
                    [(agg.fn_span_end - agg.outside_offset) as usize..];
                let gen_expr_outside_sum = s!($ots0$s$ots1$);
                // debug!(gen_expr_outside_sum);

                //FIXME //TEMP
                let m_body = s!(
                    s[i].id = i;
                    s[i].part_raw_$gen_col$ = blk_raw_$gen_col$;
                    pthread_create(&ths[i], NULL, reduce, &s[i]);
                );

                (
                    m_body,
                    s!(
                    struct Args
                    {
                        $gen_col_type$ *part_raw_$gen_col$;
                        int32_t id;
                        int64_t ret;
                    };

                    void reduce(void *args)
                    {
                        struct Args *a = (struct Args *)args;
                        int32_t id = a->id;
                        $gen_col_type$ *part_raw_$gen_col$ = a->part_raw_$gen_col$;
                        int32_t num_parts = $num_parts$;
                        size_t part_len_$gen_col$ = $gen_blk_len$;
                        size_t span = part_len_$gen_col$ / num_parts;
                        size_t blk_len_$gen_col$ = id == num_parts - 1 ? (part_len_$gen_col$ - id * span) : span;
                        $gen_col_type$ *blk_$gen_col$ = part_raw_$gen_col$ + id * span;
                        size_t s = 0;
                        for (size_t i = 0; i < blk_len_$gen_col$; i++)
                        {
                            $gen_col_type$ $gen_col$ = blk_$gen_col$[i];
                            s += $gen_expr_inside_sum$;
                        }
                        a->ret = $gen_expr_outside_sum$;
                    }
                    ),
                )
            // if inter.resolve(a.0).("sum(") {
            //    gen_sum(t_sym, c_sym);
            // } else {
            //     todo!()
            // }
            } else {
                unimplemented!("unsupported aggregation case")
            }
        }
        _ => todo!("unsupported query kind"),
    };

    //navie
    //read all data_in b
    //pipeline-process b-b-b
    //FIXME uint64_t->expected_type
    s! {
      #include <stdio.h>
      #include <stdint.h>
      #include <pthread.h>

      void ker_scan(char **, const char *);

      $reduce_fn$

      int64_t kernel() {
      $scan_body$

      struct Args s[$num_parts$];
      pthread_t ths[$num_parts$];
      for (size_t i = 0; i < $num_parts$; i++)
      {
          $m_body$
      }
      for (size_t i = 0; i < $num_parts$; i++)
      {
          pthread_join(ths[i], NULL);
      }
      int64_t sum = 0;
      for (size_t i = 0; i < $num_parts$; i++)
      {
          sum += s[i].ret;
      }

      return sum;
      }
    }
}

#[inline]
fn gen_col(cid: ColumnId) -> String {
    s!(c$cid$)
}

// #[inline]
// fn gen_prelude() -> String {
//     //TODO read meta from ...
//     s!()
// }

#[cfg(test)]
mod unit_tests {
    use super::gen;
    use base::debug;

    #[test]
    fn test_gen_basic() {
        let sql = "select 3 * sum ( 2 *t.c0 -1 ) from t";
        let ker = gen(sql);
        println!("{}", ker);
    }

    #[test]
    fn test_gen_more_aggs() {
        let sql = "select 3*sum(2*t.c0)*count(1), count(1) from t";
        let ker = gen(sql);
        println!("{}", ker);
    }
}
