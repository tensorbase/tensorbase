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
use base::{contract, debug, mem::SyncPointer, strings::s};
use bytes::BufMut;
use chrono::NaiveDateTime;
use clap::Clap;
use memmap::Mmap;
use ir::meta::{
    conf::Conf,
    schemas::{ColumnId, ColumnType}, CAT,
};
use rayon::*;
use std::os::unix::prelude::*;
use std::{
    fs::{remove_file, File, OpenOptions},
    mem,
    path::Path,
    time::Instant,
};

use csv::*;

#[repr(C)]
pub struct ParsedCSV {
    n_indexes: u32,
    indexes: *mut u32,
}

extern "C" {
    //bool find_indexes(const uint8_t *buf, size_t len, ParsedCSV *pcsv, bool
    // enable_crlf)
    pub fn find_indexes(
        buf: *mut u8,
        len: usize,
        pcsv: *mut ParsedCSV,
        enable_crlf: bool,
    );
}

/// import csv files into Base storage
#[derive(Clap)]
pub(crate) struct Csv {
    #[clap(
        short,
        about = "[FIXME]support comma-separated multiple paths of csv files, \
                 which will be imported in the multi-threads. Example: \
                 /path/to/a.csv,/another_path/to/b.csv"
    )]
    csv_file_paths: String,

    #[clap(
        short = 'i',
        about = "[FIXME]Optionally provides a colon-separated lists for \
                 specifying which fields per line in csv will be read into \
                 corresponding columns in target table. Note: the file will \
                 be imported in the multi-threads. \
                 Example: target_table:c1,c2,c3:0,1,2:51"
    )]
    columns_indexs: String,

    #[clap(
        short = 'a',
        about = "[FIXME]Allow your select specified fields from one csv file \
         into another csv file combined with the columns_indexs option."
    )]
    exported_as_csv: Option<bool>,

    #[clap(
        short,
        about = "[FIXME]only when exported_as_csv enabled, \
        and you can not specify name. Example: /path/to"
    )]
    export_dir_path: Option<String>,
}

//NAIVE
fn open_column_file(data_dirs: &str, cid: ColumnId) -> File {
    let cf = Path::new(data_dirs).join(cid.to_string());
    //DEMO //REMOVE
    if Path::new(&cf).exists() {
        remove_file(&cf).unwrap();
    }
    OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(&cf)
        .expect(
            "can not find column store! \
        Have you provided a validate conf?",
        )
}

//FIXME
pub(crate) fn import(args: Csv) {
    let timer = Instant::now();
    let conf = Conf::load(None).unwrap();
    let data_dirs = conf.storage.data_dirs;

    let csv_file_paths = debug!(args.csv_file_paths);
    let columns_indexs = args.columns_indexs;
    // let mut export_dir_path = args.export_dir_path;
    // let exported_as_csv = args.exported_as_csv;
    let csv: Vec<_> = csv_file_paths.split(",").collect();
    // for c in csv {
    //     let cf = File::open(c).expect("can not find csv: ");
    // }
    let cf = csv[0];//FIXME first csv...
    if !Path::new(&cf).exists() {
        panic!("can not find csv file:".to_owned() + cf)
    }
    log::info!("to import csv: {}", cf);

    let f = File::open(cf).unwrap();
    let flen = f.metadata().unwrap().len();
    let mmap = unsafe { Mmap::map(&f).unwrap() };
    let addr0 = SyncPointer(mmap.as_ptr());
    let mut n = flen >> SIZ_PART_SHIFT;

    let ci: Vec<&str> = columns_indexs.split(":").collect();
    if ci.len() != 4 {
        panic!("wrong format for columns_indexs!")
    }
    let (tab, cols, idxs, nc) = (ci[0], ci[1], ci[2], ci[3]);
    let idxs: Vec<u32> =
        idxs.split(",").map(|s| s.parse::<u32>().unwrap()).collect();
    //validation
    let cat = CAT.lock().unwrap();
    let tab = cat
        .get_table_by_name(tab)
        .expect(&s!("table $tab$ does not exit!"));
    let col_id_types: Vec<(ColumnId, ColumnType, File)> = cols
        .split(",")
        .map(|s| {
            let s = s.trim();
            let col = tab
                .get_column_by_name(s)
                .expect(&s!("column $s$ does not exit!"));
            (col.id, col.data_type, open_column_file(&data_dirs, col.id))
        })
        .collect();

    //FIXME
    let col_idxs_csv: Vec<(u32, ColumnType, RawFd)> = idxs
        .into_iter()
        .zip(col_id_types.iter())
        .map(|(x, y)| (x, y.1, y.2.as_raw_fd()))
        .collect();

    let nf_row = nc.parse::<u32>().unwrap();


    log::debug!("n:{},flen:{}", n, flen);

    let wg = crossbeam::sync::WaitGroup::new();
    for tid in 0u32..=n as u32 {
        let wg = wg.clone();
        let col_idxs_csv = col_idxs_csv.clone();
        rayon::spawn(move || {
            process_csv(
                tid,
                addr0,
                flen as usize,
                col_idxs_csv,
                nf_row,
                // if exported_as_csv {
                //     export_csv
                // } else {
                export_bin, // },
            );
            drop(wg);
        });
    }

    wg.wait();

    //NAIVE
    let mut lens = [0u64; 3];
    let column_files: Vec<File> =
        col_id_types.into_iter().map(|e| e.2).collect();
    for (idx, f) in column_files.iter().enumerate() {
        lens[idx] = f.metadata().unwrap().len();
        // log::debug!("idx: {}, f: {:?}, len: {}", idx, f, lens[idx]);
    }
    //NAIVE gen metadata
    let data_meta_path = data_dirs + "/meta";
    let meta = OpenOptions::new()
        .write(true)
        .create(true)
        .append(false)
        .open(&data_meta_path)
        .unwrap();
    unsafe {
        let res = base::syscalls::write(
            meta.as_raw_fd() as usize,
            lens.as_ptr() as usize,
            lens.len() * mem::size_of::<u64>() as usize,
        );
    }

    log::info!(
        "csv:{} with {} columns has been imported in {} ms",
        cf,
        col_idxs_csv.len(),
        timer.elapsed().as_millis()
    );
}

fn export_bin(outs: Vec<(Vec<u8>, RawFd)>) {
    unsafe {
        for (buf, out) in outs {
            if let Err(s) = base::syscalls::write(
                out as usize,
                buf.as_ptr() as usize,
                buf.len() as usize,
            ) {
                debug!(s);
            }
        }
    }
}

//FIXME sub-efficient
// fn export_csv(outs: Vec<(Vec<u8>, RawFd)>) {
//     contract!(outs.len() > 0);
//     contract!(outs.windows(2).all(|w| w[0].1 == w[1].1));
//     let out_fd = outs[0].1;
//     let n_rows = outs[0].0.len as isize;
//     unsafe {
//         let mut row = String::with_capacity(32 * 1024); //FIXME
//         for i in 0isize..n_rows {
//             //FIXME
//             outs.iter().for_each(|e| {
//                 let cs = &e.0;
//                 match cs.typ {
//                     ColumnType::INT32 => {
//                         itoa::fmt(&mut row, *(cs.data as *const
// u32).offset(i))                             .unwrap();
//                         row.push(',');
//                     }
//                     ColumnType::UNIX_DATETIME => {
//                         let buf = cs.data as *const i32;
//                         row.push_str(
//                             &NaiveDateTime::from_timestamp(
//                                 *buf.offset(i) as i64,
//                                 0,
//                             )
//                             .format("%Y-%m-%d %H:%M:%S")
//                             .to_string(),
//                         );
//                         row.push(',');
//                     }
//                     _ => todo!(),
//                 }
//             });
//             row.pop();
//             row.push('\n');
//         }
//         // dbg!(&row);
//         let row_buf = row.as_bytes();
//         if let Err(s) = base::syscalls::write(
//             debug!(out_fd as usize),
//             debug!(row_buf.as_ptr() as usize),
//             debug!(row_buf.len()),
//         ) {
//             debug!(std::io::Error::from_raw_os_error(s as i32));
//         }
//     }
// }

const SIZ_PART_SHIFT: u8 = 20;
const SIZ_PART: usize = 1 << SIZ_PART_SHIFT;
//FIXME assumed there is at least one LR in one SIZ_PART span
fn process_csv(
    tid: u32,
    addr0: SyncPointer<u8>,
    len: usize,
    col_idxs_csv: Vec<(u32, ColumnType, RawFd)>,
    nfields_row_csv: u32,
    export: impl Fn(Vec<(Vec<u8>, RawFd)>),
) {
    // let mut wt = csv::Writer::from_writer(csv_dst);
    let addr = addr0.as_mut();
    let nc = col_idxs_csv.len();
    //FIXME Vec<u32>
    let mut cols: Vec<Vec<u8>> = Vec::with_capacity(nc);
    for _ in 0..nc {
        cols.push(Vec::with_capacity(8 * 1024));
    }
    unsafe {
        unsafe fn to_next_line(addr: *mut u8, mut offset: usize) -> usize {
            while *addr.offset(offset as isize) != 0xa {
                offset += 1;
            }
            offset
        }

        let sos = if tid != 0 {
            to_next_line(addr, tid as usize * SIZ_PART) + 1
        } else {
            0
        };
        let eos = if tid != (len >> SIZ_PART_SHIFT) as u32 {
            to_next_line(addr, (tid + 1) as usize * SIZ_PART)
        } else {
            len
        };

        let scan_len = (eos - sos) as usize;
        debug!((tid, sos, eos, scan_len));
        let mut idxv = Vec::<u32>::with_capacity(scan_len / 2); //FIXME in theory only scan_len
        let pcsv = ParsedCSV {
            n_indexes: 0,
            indexes: idxv.as_mut_ptr(),
        };
        let pc = addr.offset(sos as isize);
        find_indexes(
            pc,
            scan_len,
            &pcsv as *const ParsedCSV as *mut ParsedCSV,
            false,
        );

        let csv = std::slice::from_raw_parts(pc, scan_len);
        let mut rdr = ReaderBuilder::new().from_reader(csv);

        for result in rdr.records() {
            let record = result.unwrap();

            for i in 0..nc {
                let fi = col_idxs_csv[i].0 as usize;
                let ft = col_idxs_csv[i].1;

                match ft {
                    ColumnType::INT32 | ColumnType::UINT32 => {
                        let v = &record[i];
                        cols[i].put_u32_le(v.parse::<u32>().unwrap());

                    }
                    ColumnType::INT8 | ColumnType::UINT8 => {
                        let v = &record[i];
                        cols[i].put_u8(v.parse::<u8>().unwrap());
                    }
                    ColumnType::UNIX_DATETIME => {
                        let v = &record[i];
                        let ss_trimed = v.trim_matches(|c| c == '"');

                        cols[i].put_u32_le(
                            NaiveDateTime::parse_from_str(
                                ss_trimed,
                                "%Y-%m-%d %H:%M:%S",
                            )
                            .expect(ss_trimed)
                            .timestamp()
                                as u32,
                        );

                    }
                    _ => todo!(),
                }
            }
        }


        let outs_csv: Vec<(Vec<u8>, RawFd)> = cols
            .into_iter()
            .zip(col_idxs_csv.into_iter())
            .map(|(buf, cic)| {
                assert!(buf.len() < u32::MAX as usize);
                (buf, cic.2)
            })
            .collect();

        export(outs_csv);
        // wt.write_record(record)
        // log::debug!("sos: {}, eos: {}, len: {}", sos, eos, eos-sos);
    }
}

#[cfg(test)]
mod unit_tests {
    use super::{find_indexes, process_csv, ParsedCSV};
    use base::mem::aligned;
    use base::mem::{aligned::is_page_aligned, SyncPointer};
    use memmap::Mmap;
    use meta::schemas::ColumnType;
    use std::fs::{remove_file, File, OpenOptions};
    use std::os::unix::prelude::*;
    use std::{io::prelude::*, path::Path, time::Instant};

    #[test]
    pub fn test_csv_basic() -> std::io::Result<()> {
        let tfpath = format!(
            "{}{}",
            env!("CARGO_MANIFEST_DIR"),
            "/tests/data/trips_sample.csv"
        );
        // let out_dir = env::var("OUT_DIR").unwrap();
        //
        let mut tf = File::open(&tfpath)?;
        let flen = tf.metadata().unwrap().len();
        let mut contents = String::new();
        tf.read_to_string(&mut contents)?;
        // println!("{}", contents);
        let mmap = unsafe { Mmap::map(&tf)? };
        assert!(is_page_aligned(mmap.as_ptr()));

        let idxs = aligned::alloc::<u32>(flen as usize);
        let pcsv = ParsedCSV {
            n_indexes: 0,
            indexes: idxs,
        };
        unsafe {
            find_indexes(
                mmap.as_ptr() as *mut u8,
                flen as usize,
                &pcsv as *const ParsedCSV as *mut ParsedCSV,
                false,
            );

            println!("pcsv.n_indexes: {}", pcsv.n_indexes);
            let idxs = std::slice::from_raw_parts_mut(
                pcsv.indexes,
                pcsv.n_indexes as usize,
            );
            for i in &idxs[0..50] {
                print!("{} ", i);
            }
            println!(
                "\nmmap[idx[50]]:  {} or {}",
                mmap[idxs[50] as usize], mmap[idxs[50] as usize] as char
            );
        }

        Ok(())
    }

    #[test]
    pub fn test_csv_basic_2() -> std::io::Result<()> {
        let tfpath = "/jin/tensorbase/2cols_1m.csv";
        let mut tf = File::open(&tfpath)?;
        let flen = tf.metadata().unwrap().len();
        // let mut contents = String::new();
        // tf.read_to_string(&mut contents)?;
        // println!("{}", contents);
        let mmap = unsafe { Mmap::map(&tf)? };
        assert!(is_page_aligned(mmap.as_ptr()));

        let mut idxs = Vec::<u32>::with_capacity(flen as usize / 2);
        let pcsv = ParsedCSV {
            n_indexes: 0,
            indexes: idxs.as_mut_ptr(),
        };
        unsafe {
            find_indexes(
                mmap.as_ptr() as *mut u8,
                flen as usize,
                &pcsv as *const ParsedCSV as *mut ParsedCSV,
                false,
            );

            println!("flen: {}", flen);
            println!("pcsv.n_indexes: {}", pcsv.n_indexes);
            let idxs = std::slice::from_raw_parts_mut(
                pcsv.indexes,
                pcsv.n_indexes as usize,
            );
            // for i in &idxs[0..50] {
            //     print!("{} ", i);
            // }
            // println!(
            //     "\nmmap[idx[50]]:  {} or {}",
            //     mmap[idxs[50] as usize], mmap[idxs[50] as usize] as char
            // );
            println!(
                "pcsv.indexes: {}",
                *(pcsv.indexes.offset(pcsv.n_indexes as isize))
            );
        }

        Ok(())
    }

    #[test]
    pub fn test_process_csv() -> std::io::Result<()> {
        // let tfpath = format!(
        //     "{}{}",
        //     env!("CARGO_MANIFEST_DIR"),
        //     "/tests/data/trips_sample.csv"
        // );
        // let tid: u32 = 0;
        let tid: u32 = 41;
        let tfpath = "/jian/nyc_taxi_data/trips_xaa.csv";
        let mut tf = File::open(&tfpath)?;
        let flen = tf.metadata().unwrap().len();
        // let mut contents = String::new();
        // tf.read_to_string(&mut contents)?;
        // println!("{}", contents);
        let mmap = unsafe { Mmap::map(&tf)? };
        assert!(is_page_aligned(mmap.as_ptr()));
        let addr0: SyncPointer<u8> = SyncPointer(mmap.as_ptr());
        let len: usize = flen as usize;

        // let export_dir_path = "/data/n3/";
        // let mut fs = Vec::with_capacity(3);
        // for i in 0..3 {
        //     let export_dir_path_file =
        //         [export_dir_path, &i.to_string()].join("");
        //     //FIXME
        //     if Path::new(&export_dir_path_file).exists() {
        //         remove_file(&export_dir_path_file).unwrap();
        //     }
        //     fs.push(
        //         OpenOptions::new()
        //             .write(true)
        //             .create(true)
        //             .append(true)
        //             .open(&export_dir_path_file)
        //             .unwrap(),
        //     );
        // }
        // let fds = fs.iter().map(|f| f.as_raw_fd()).collect::<Vec<_>>();
        // let idxs_csv: Vec<_> = vec![
        //     (0, ColumnType::INT32, fds[0]),
        //     (1, ColumnType::INT32, fds[1]),
        //     (2, ColumnType::UNIX_DATETIME, fds[2]),
        // ];
        let idxs_csv: Vec<_> = vec![
            (0, ColumnType::INT32, -1),
            (2, ColumnType::UNIX_DATETIME, -1),
            (10, ColumnType::INT8, -1),
        ];

        let ncr: u32 = 51;
        let timer = Instant::now();
        process_csv(tid, addr0, len, idxs_csv.clone(), ncr, |_| {});
        process_csv(tid + 1, addr0, len, idxs_csv.clone(), ncr, |_| {});
        dbg!(timer.elapsed().as_millis());

        Ok(())
    }

    #[ignore]
    #[test]
    pub fn test_process_csv_loop() -> std::io::Result<()> {
        for tid in 2..3u32 {
            let ncr = 2;
            let tfpath = "/data/n3/exported.csv";
            let mut tf = File::open(&tfpath)?;
            let flen = tf.metadata().unwrap().len();
            // let mut contents = String::new();
            // tf.read_to_string(&mut contents)?;
            // println!("{}", contents);
            let mmap = unsafe { Mmap::map(&tf)? };
            assert!(is_page_aligned(mmap.as_ptr()));
            let addr0: SyncPointer<u8> = SyncPointer(mmap.as_ptr());
            let len: usize = flen as usize;

            let idxs_csv: Vec<_> = vec![
                (0, ColumnType::INT32, -1),
                (1, ColumnType::UNIX_DATETIME, -1),
            ];

            process_csv(tid, addr0, len, idxs_csv.clone(), ncr, |_| {});
        }

        Ok(())
    }

    #[ignore]
    #[test]
    pub fn test_process_csv_export_bin() -> std::io::Result<()> {
        let tid: u32 = 0;
        let ncr: u32 = 2;
        let tfpath = "/data/n3/exported.csv";
        // let tfpath = format!(
        //     "{}{}",
        //     env!("CARGO_MANIFEST_DIR"),
        //     "/tests/data/trips_sample.csv"
        // );
        // let tid: u32 = 21151;//21152-1;
        let mut tf = File::open(&tfpath)?;
        let flen = tf.metadata().unwrap().len();
        // let mut contents = String::new();
        // tf.read_to_string(&mut contents)?;
        // println!("{}", contents);
        let mmap = unsafe { Mmap::map(&tf)? };
        assert!(is_page_aligned(mmap.as_ptr()));
        let addr0: SyncPointer<u8> = SyncPointer(mmap.as_ptr());
        let len: usize = flen as usize;

        let export_dir_path = "/data/n2/";
        let mut fs = Vec::new();
        for i in 0..2 {
            let export_dir_path_file =
                [export_dir_path, &i.to_string()].join("");
            //FIXME
            if Path::new(&export_dir_path_file).exists() {
                remove_file(&export_dir_path_file).unwrap();
            }
            fs.push(
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(&export_dir_path_file)
                    .unwrap(),
            );
        }
        let fds = fs.iter().map(|f| f.as_raw_fd()).collect::<Vec<_>>();
        let idxs_csv: Vec<_> = vec![
            (0, ColumnType::INT32, fds[0]),
            (1, ColumnType::UNIX_DATETIME, fds[1]),
            (1, ColumnType::INT32, fds[2]),
        ];

        let timer = Instant::now();
        process_csv(tid, addr0, len, idxs_csv.clone(), ncr, super::export_bin);
        // process_csv(
        //     tid + 1,
        //     addr0,
        //     len,
        //     idxs_csv.clone(),
        //     ncr,
        //     super::export_bin,
        // );
        dbg!(timer.elapsed().as_millis());

        Ok(())
    }

    // #[ignore]
    // #[test]
    // pub fn test_process_csv_export_csv() -> std::io::Result<()> {
    //     let tid: u32 = 0;
    //     let tfpath = format!(
    //         "{}{}",
    //         env!("CARGO_MANIFEST_DIR"),
    //         "/tests/data/trips_sample.csv"
    //     );
    //     let mut tf = File::open(&tfpath)?;
    //     let flen = tf.metadata().unwrap().len();
    //     // let mut contents = String::new();
    //     // tf.read_to_string(&mut contents)?;
    //     // println!("{}", contents);
    //     let mmap = unsafe { Mmap::map(&tf)? };
    //     assert!(is_page_aligned(mmap.as_ptr()));
    //     let addr0: SyncPointer<u8> = SyncPointer(mmap.as_ptr());
    //     let len: usize = flen as usize;
    //     let export_dir_path_file = "/data/n3/exported.csv";
    //     //FIXME
    //     if Path::new(&export_dir_path_file).exists() {
    //         remove_file(&export_dir_path_file).unwrap();
    //     }
    //     let fd = OpenOptions::new()
    //         .write(true)
    //         .create(true)
    //         .append(true)
    //         .open(&export_dir_path_file)
    //         .unwrap();
    //     let fds = vec![fd.as_raw_fd(); 3];
    //     let idxs_csv: Vec<_> = vec![
    //         (0, ColumnType::INT32, fds[0]),
    //         (2, ColumnType::UNIX_DATETIME, fds[1]),
    //         (19, ColumnType::INT32, fds[2]),
    //     ];

    //     let ncr: u32 = 51;
    //     let timer = Instant::now();
    //     process_csv(tid, addr0, len, idxs_csv.clone(), ncr,
    // super::export_csv);     dbg!(timer.elapsed().as_millis());

    //     Ok(())
    // }
}
