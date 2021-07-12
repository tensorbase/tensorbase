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
#![feature(proc_macro_diagnostic)]
#![feature(proc_macro_span)]

// use proc_macro::{Diagnostic, Level};
use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;

#[proc_macro]
pub fn s(ts: proc_macro::TokenStream) -> proc_macro::TokenStream {
    expand_str(ts.into()).into()
}

fn expand_str(ts: TokenStream) -> TokenStream {
    let span = ts.span();
    let src = &span.unwrap().source_text().unwrap();
    // eprintln!("===========\n{:#?}",ts);
    // for t in ts {
    //      if let TokenTree::Ident(ident) = t {
    //          ident.
    //     }
    // }
    expand_str_raw(src)
}

//FIXME change algo to itoa et. al.
fn expand_str_raw(src: &str) -> TokenStream {
    let mut ss: &str = src;
    let mut ret: Vec<TokenStream> = vec![];
    let mut count_chars = 0;
    if ss.contains('$') {
        while let Some(e) = ss.find('$') {
            let rs: &str = &ss[..e];
            count_chars += rs.len();
            ret.push(quote! {  #rs.put_into_string(&mut buf); });
            ss = &ss[(e + 1)..];
            if let Some(e) = ss.find('$') {
                let id = &ss[..e];
                let idt: proc_macro2::TokenStream = id.parse().unwrap();
                let expr: syn::Expr = syn::parse2(idt).expect(
                    "it should provide a context-valid obj which has to_string \
                    method",
                );
                ret.push(quote! {  #expr.put_into_string(&mut buf); });
                ss = &ss[(e + 1)..];
            } else {
                // FIXME
                // ts.span()
                //     .unwrap()
                //     .error(
                //         "it should provide a pair of matched $ \
                //     around interpolated var.",
                //     )
                //     .emit();
                panic!(
                    "it should provide a pair of matched $ \
                around interpolated var."
                );
            }
        }
        if !ss.is_empty() {
            count_chars += ss.len();
            ret.push(quote! {  #ss.put_into_string(&mut buf); });
        }
        let t = quote!(
        {
            use base::strings::PutIntoString;
            let mut buf = String::with_capacity(#count_chars*2);
            #(#ret)*
            buf }
        );
        // t.span()
        //     .unwrap()
        //     .help(format!("!!!expanded macro:\n{}\n", t))
        //     .emit();
        t
    } else {
        quote! {  String::from(#src) }
    }
}

#[proc_macro]
pub fn bs(ts: proc_macro::TokenStream) -> proc_macro::TokenStream {
    expand_cstr(ts.into()).into()
}

fn expand_cstr(ts: TokenStream) -> TokenStream {
    let span = ts.span();
    let src = &span.unwrap().source_text().unwrap();
    // eprintln!("===========\n{:#?}",ts);
    // for t in ts {
    //      if let TokenTree::Ident(ident) = t {
    //          ident.
    //     }
    // }
    expand_cstr_raw(src)
}

//FIXME change algo to itoa et. al.
fn expand_cstr_raw(src: &str) -> TokenStream {
    let mut ss: &str = src;
    let mut ret: Vec<TokenStream> = vec![];
    let mut count_chars = 0;
    if ss.contains('$') {
        while let Some(e) = ss.find('$') {
            let rs: &str = &ss[..e];
            count_chars += rs.len();
            ret.push(quote! {  #rs.put_into_bytes(&mut buf); });
            ss = &ss[(e + 1)..];
            if let Some(e) = ss.find('$') {
                let id = &ss[..e];
                let idt: proc_macro2::TokenStream = id.parse().unwrap();
                let expr: syn::Expr = syn::parse2(idt).expect(
                    "it should provide a context-valid obj which has to_string \
                    method",
                );
                ret.push(quote! {  #expr.put_into_bytes(&mut buf); });
                ss = &ss[(e + 1)..];
            } else {
                // FIXME
                // ts.span()
                //     .unwrap()
                //     .error(
                //         "it should provide a pair of matched $ \
                //     around interpolated var.",
                //     )
                //     .emit();
                panic!(
                    "it should provide a pair of matched $ \
                around interpolated var."
                );
            }
        }
        if !ss.is_empty() {
            count_chars += ss.len();
            ret.push(quote! {  #ss.put_into_bytes(&mut buf); });
        }
        let t = quote!(
        {
            use base::strings::PutIntoBytes;
            let mut buf = Vec::<u8>::with_capacity(#count_chars*2);
            #(#ret)*
            buf
        });
        // t.span()
        //     .unwrap()
        //     .help(format!("!!!expanded macro:\n{}\n", t))
        //     .emit();
        t
    } else {
        quote! {  #src.as_bytes().to_vec() }
    }
}

#[proc_macro_attribute]
pub fn async_test(
    params: proc_macro::TokenStream,
    ts: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    assert!(
        params.is_empty(),
        "#[async_test] attribute has no parameter"
    );
    let ts: TokenStream = ts.into();
    let mut afn: syn::ItemFn = syn::parse2(ts).expect("function expected");

    assert!(
        afn.clone().sig.asyncness.take().is_some(),
        "#[async_test] can only be applied to async functions"
    );
    let afn_name = format!("{}", afn.sig.ident);
    let id_afn_name: TokenStream = afn_name.parse().unwrap();
    let id_patched_afn_name: TokenStream = (afn_name + "_async_").parse().unwrap();
    afn.sig.ident = syn::parse2(id_patched_afn_name.clone()).unwrap();

    let rt = quote! {
      #[test]
      fn #id_afn_name() {
        #afn
        futures::executor::block_on(#id_patched_afn_name());
      }
    };

    proc_macro::TokenStream::from(rt)
    // rt.span()
    //     .unwrap()
    //     .error(format!("!!!!!expanded macro:\n{}\n", rt))
    //     .emit();
}

// fn expand_async_test(ts: TokenStream, afn_name: &str) -> TokenStream {
//     let id_afn_name: proc_macro2::TokenStream = afn_name.parse().unwrap();
//     let id_patched_afn_name: proc_macro2::TokenStream =
//         (afn_name.to_string() + "_async_").parse().unwrap();
//     quote! {
//       #[test]
//       fn #id_patched_afn_name() {
//         #ts
//         futures::executor::block_on(#id_afn_name());
//       }
//     }
// }

#[cfg(test)]
mod unit_tests {
    #[test]
    fn test_expand_str_raw() {
        let src: &str = "class $dsadsa$1 { float x = $some_float$; }";
        let ts_out = super::expand_str_raw(src);
        println!("ts_out:\n{}", ts_out.to_string());
    }

    // #[test]
    // fn test_expand_async_test() {
    //     let ts = quote::quote! { async fn test_connect() -> Result<()> { } };
    //     let ts_out = super::expand_async_test(ts, "test_connect");
    //     println!("ts_out:\n{}", ts_out.to_string());
    // }

    // #[test]
    // fn test_async_test() {
    //     let ps = quote::quote! {};
    //     let ts = quote::quote! { #[async_test] async fn test_connect() -> Result<()> { } };
    //     let ts_out = super::async_test(ps.into(), ts.into());
    //     println!("ts_out:\n{}", ts_out.to_string());
    // }
}
