use base::syscalls::*;
use core::arch::x86_64::_rdtsc;

#[inline(never)]
fn bench() {
    unsafe {
        let s = _rdtsc();
        for _ in 0..10 {
            let _ = getpid();
            // let _ = read(1,2,3);
        }
        let elapsed_clocks = _rdtsc() - s;
        println!("elapsed_clocks: {}", elapsed_clocks);
    }
}

pub fn main() {
    bench();
}
