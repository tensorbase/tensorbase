use std::io::Error;

use crate::mem::MemAddr;

use crate::errs::{BaseError, BaseResult};

#[inline]
pub fn mm_anon(size: usize) -> BaseResult<MemAddr> {
    let addr = unsafe {
        libc::mmap(
            0 as *mut libc::c_void,
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_ANON | libc::MAP_PRIVATE,
            -1,
            0,
        )
    };
    if addr == libc::MAP_FAILED {
        Err(BaseError::FailedToMmap)
    } else {
        Ok(addr)
    }
}

#[inline]
pub fn mm_mremap(
    addr: MemAddr,
    old_size: usize,
    new_size: usize,
) -> BaseResult<MemAddr> {
    let ret =
        unsafe { libc::mremap(addr, old_size, new_size, libc::MREMAP_MAYMOVE) };
    if addr == libc::MAP_FAILED {
        Err(BaseError::FailedToMremap)
    } else {
        Ok(ret)
    }
}

#[inline]
pub fn mm_file_ro(fd: u32, size: usize) -> BaseResult<MemAddr> {
    let addr = unsafe {
        libc::mmap(
            0 as *mut libc::c_void,
            size,
            libc::PROT_READ,
            libc::MAP_PRIVATE | libc::MAP_NORESERVE,
            fd as i32,
            0,
        )
    };
    if addr == libc::MAP_FAILED {
        // println!("", Err(Error::last_os_error()));
        Err(BaseError::WrappingIOError(Error::last_os_error()))
    } else {
        Ok(addr)
    }
}

#[inline]
pub fn mm_unmap(addr: MemAddr, size: usize) -> BaseResult<()> {
    let rt = unsafe { libc::munmap(addr, size) };
    if rt < 0 {
        // println!("", Err(Error::last_os_error()));
        Err(BaseError::WrappingIOError(Error::last_os_error()))
    } else {
        Ok(())
    }
}

// pub fn slice_from_mm_anon_static<T>(size: usize) -> BaseResult<&'static [T]> {
//     let addr = mm_anon(size)?;
//     unsafe {
//         Ok(std::slice::from_raw_parts(
//             addr as *const T,
//             size / mem::size_of::<T>(),
//         ))
//     }
// }

// ====== Tests ======
#[cfg(test)]
mod unit_tests {
    use std::{env, ffi::CString, fs, io::Error};
    use libc::c_void;    

    use super::*;

    pub fn open(path: &str) -> std::io::Result<u32> {
        unsafe {
            let p = CString::new(path).expect("CString::new failed");
            let mode =
                libc::S_IRUSR | libc::S_IWUSR | libc::S_IRGRP | libc::S_IROTH;
            let fd = libc::open(
                p.as_ptr(),
                libc::O_CREAT | libc::O_RDWR | libc::O_NOATIME,
                mode,
            );
            if fd < 0 {
                Err(Error::last_os_error())
            } else {
                Ok(fd as u32)
            }
        }
        // let mut f = OpenOptions::new().mode(mode).open(path);
        // Ok(f.as_raw_fd() as u32)
    }

    #[test]
    fn test_mm_file_ro() {
        let siz = 20_000usize;
        let buf1 = vec![1u8; siz];
        let buf2 = vec![2u8; siz];
        let mut tmpfile = env::temp_dir();
        tmpfile.push("foo.txt");

        if tmpfile.exists() {
            fs::remove_file(&tmpfile).unwrap();
        }

        let fd = open(tmpfile.to_str().unwrap()).unwrap();

        let addr = mm_file_ro(fd, 1024 * 1024).unwrap();
        unsafe {
            libc::pwrite(fd as i32, buf1.as_ptr() as *const c_void, siz, 0);
            assert_eq!(*(addr.offset((siz / 2) as isize) as *const u8), 1u8);
            libc::pwrite(
                fd as i32,
                buf2.as_ptr() as *const c_void,
                siz,
                siz as i64,
            );
            assert_eq!(
                *(addr.offset((siz + siz / 2) as isize) as *const u8),
                2u8
            );
        }
    }

    #[test]
    fn test_mm_anon() {
        let siz = 1024 * 1024;
        let addr = mm_anon(siz).unwrap();
        unsafe {
            assert_eq!(*(addr.offset((siz - 1) as isize) as *const u8), 0u8);
        }
    }
}
