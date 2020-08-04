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

use std::{collections::HashMap};
use typed_arena::Arena;

pub type Sym = u32;
pub struct Interner<'a> {
    map: HashMap<&'a str, u32>,
    vec: Vec<&'a str>,
    arena: &'a Arena<u8>,
}

impl Interner<'_> {
    pub fn new(arena: &Arena<u8>) -> Interner {
        Interner {
            map: HashMap::new(),
            vec: Vec::new(),
            arena,
        }
    }

    pub fn with_capacity(arena: &Arena<u8>, capacity: usize) -> Interner {
        Interner {
            map: HashMap::with_capacity(capacity),
            vec: Vec::with_capacity(capacity),
            arena,
        }
    }

    pub fn intern(&mut self, name: &str) -> Sym {
        if let Some(&idx) = self.map.get(name) {
            return idx;
        }
        let idx = self.vec.len() as Sym;
        let name = self.arena.alloc_str(name);
        self.map.insert(name, idx);
        self.vec.push(name);

        debug_assert!(self.resolve(idx) == name);
        debug_assert!(self.intern(name) == idx);

        idx
    }

    pub fn resolve(&self, idx: Sym) -> &str {
        self.vec[idx as usize]
    }
}

#[cfg(test)]
mod unit_tests {
    use super::Interner;
    use typed_arena::Arena;

    #[test]
    pub fn basic_check() {
        let arena = Arena::with_capacity(16);
        let mut inter = Interner::with_capacity(&arena, 16);
        let id1 = inter.intern("t1");
        let id2 = inter.intern("t2");
        assert!(id1 != id2);
        // println!("id2:{}",id2);
        let id3 = inter.intern("t1");
        // println!("id3:{}",id3);
        assert!(id3 == id1);

        let mut last_id = 0;
        for i in 0..128 {
            last_id = inter.intern(format!("c{}", i).as_str());
        }
        assert!(last_id == inter.intern(String::from("c127").as_str()));
        assert!("c127" == inter.resolve(last_id));
        // println!("resolved last_id: {}",inter.resolve(last_id));
    }
}
