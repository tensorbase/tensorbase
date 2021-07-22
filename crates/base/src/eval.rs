use std::cell::RefCell;

use fasteval::EmptyNamespace;
use fasteval::Evaler;
use fasteval::Parser;
use fasteval::Slab;

use crate::errs::BaseError;
use crate::errs::BaseResult;

const PARSER: Parser = Parser::new();
thread_local! {
    static SLAB: RefCell<Slab> = RefCell::new(Slab::new());
}

pub fn eval_literal_u64(expr: &str) -> BaseResult<u64> {
    eval_literal(expr).map(|e| e as u64)
}

pub fn eval_literal(expr: &str) -> BaseResult<f64> {
    SLAB.with(|slab| {
        let mut sm = slab.borrow_mut();
        let expr_ref = PARSER
            .parse(expr, &mut sm.ps)
            .map_err(|_e| BaseError::CanotEval)?
            .from(&sm.ps);
        let rt = expr_ref.eval(&sm, &mut EmptyNamespace);
        rt.map_err(|_e| BaseError::CanotEval)
    })
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn basic_check() -> BaseResult<()> {
        assert_eq!(25.0, eval_literal("2.5*10")?);
        assert_eq!(123, eval_literal_u64("0.123*1000")?);
        Ok(())
    }

    #[test]
    fn stress_eval_literal() {
        let chks = [25, 123, 365, 222];
        let exps = ["2.5*10", "0.123*1000", "365*1", "888-666"];
        for _ in 0..10000 {
            for (i, e) in exps.iter().enumerate() {
                assert_eq!(chks[i], eval_literal_u64(e).unwrap());
            }
        }
    }
}
