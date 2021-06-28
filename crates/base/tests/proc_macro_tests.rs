// #![recursion_limit="512"] //macros bang?
use base::strings::{bs, bytes_to_cstring, s};
use base::{debug, with_timer, with_timer_print};
use std::{ffi::CString, thread, time};

#[test]
fn integ_test_timing() {
    let ten_millis = time::Duration::from_millis(10);
    with_timer! {t0,
        thread::sleep(ten_millis);
    }
    println!("t0: {:?}", t0.elapsed());
    assert!((t0.elapsed() - ten_millis) < time::Duration::from_millis(1));

    with_timer_print! {t1,
        thread::sleep(ten_millis);
        thread::sleep(ten_millis);
    }
    assert!((t1.elapsed() - ten_millis - ten_millis) < time::Duration::from_millis(1));
}

#[test]
fn integ_test_str() {
    let dsadsa = "Test";
    let some_float = 123.45;
    let some_int = 123456789101112u64;

    let s = s! { let ten_millis=1
    ; };
    assert_eq!(s, "let ten_millis=1\n    ;");

    let s = s!(class $dsadsa$1 { float x = $some_float$; });
    assert_eq!(s, "class Test1 { float x = 123.45; }");

    let typ = "long long";
    let s = s!(class $dsadsa$1 { float x = $some_float$; $typ$ x });
    assert_eq!(s, "class Test1 { float x = 123.45; long long x }");

    let dsadsa = String::from("Test2");
    let s = s!(class $dsadsa$1 { float x = $some_float$; $typ$ x =$some_int$ });
    assert_eq!(
        s,
        "class Test21 { float x = 123.45; long long x =123456789101112 }"
    );


    let mut class_name = String::from("Test");
    // let member_name = String::from("var");
    let s = s!(class ${class_name.push('1');class_name}$ { });
    // debug!(&s);
    assert_eq!(s, "class Test1 { }");

    let mut class_name = String::from("Test");
    let s = s!(class ${class_name.push('1');class_name}$ { });
    assert_eq!(s, "class Test1 { }");


    let bs = bs!(dasdsadsa);
    let sbs = bytes_to_cstring(bs);
    // debug!(&sbs);
    assert_eq!(sbs, CString::new("dasdsadsa").unwrap());

    let f = bs!(float $String::from("var")$ = $some_float$;);
    let sf = bytes_to_cstring(f);
    // debug!(&sf);
    assert_eq!(sf, CString::new("float var = 123.45;").unwrap());
}
