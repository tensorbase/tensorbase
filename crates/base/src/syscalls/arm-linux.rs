use ::core::{
    hint::unreachable_unchecked,
    result::Result::{
        self,
        Err,
        Ok,
    },
};

/// Performs a system call and returns the result.
///
/// The first argument specifies the system call, and the second is a slice of
/// arguments to pass it.
///
#[inline(always)]
pub unsafe fn syscall(r7: usize, a: &[usize]) -> Result<usize, usize> {
    match a.len() {
        0 => syscall_0(r7),
        1 => syscall_1(r7, a[0]),
        2 => syscall_2(r7, a[0], a[1]),
        3 => syscall_3(r7, a[0], a[1], a[2]),
        4 => syscall_4(r7, a[0], a[1], a[2], a[3]),
        5 => syscall_5(r7, a[0], a[1], a[2], a[3], a[4]),
        6 => syscall_6(r7, a[0], a[1], a[2], a[3], a[4], a[5]),
        _ => unreachable_unchecked(),
    }
}

/// Performs a system call which never returns.
///
/// The first argument specifies the system call, and the second is a slice of
/// arguments to pass it.
///
/// This should only be used for calls like `exit` or `exit_group` which are
/// guaranteed to never return.
///
#[inline(always)]
pub unsafe fn syscall_nr(r7: usize, a: &[usize]) -> ! {
    match a.len() {
        0 => syscall_0_nr(r7),
        1 => syscall_1_nr(r7, a[0]),
        2 => syscall_2_nr(r7, a[0], a[1]),
        3 => syscall_3_nr(r7, a[0], a[1], a[2]),
        4 => syscall_4_nr(r7, a[0], a[1], a[2], a[3]),
        5 => syscall_5_nr(r7, a[0], a[1], a[2], a[3], a[4]),
        6 => syscall_6_nr(r7, a[0], a[1], a[2], a[3], a[4], a[5]),
        _ => unreachable_unchecked(),
    }
}

/// Performs a system call with no arguments and returns the result.
///
/// The argument specifies the system call.
///
#[inline(always)]
pub unsafe fn syscall_0(r7: usize) -> Result<usize, usize> {
    let r0: usize;
    asm!(
        "svc $$0"
        : "={r0}"(r0)
        : "{r7}"(r7)
        : "cc", "memory"
        : "volatile"
    );
    if r0 < 0xffff_f000 {
        Ok(r0)
    } else {
        Err(r0)
    }
}

/// Performs a system call with no arguments which never returns.
///
/// The argument specifies the system call.
///
/// This function should only be used for calls guaranteed to never return.
///
#[inline(always)]
pub unsafe fn syscall_0_nr(r7: usize) -> ! {
    asm!(
        "svc $$0"
        :
        : "{r7}"(r7)
        : "cc"
        : "volatile"
    );
    unreachable_unchecked()
}

/// Performs a system call with one argument and returns the result.
///
/// The first argument specifies the system call, and the second is the
/// argument to pass it.
///
#[inline(always)]
pub unsafe fn syscall_1(r7: usize, mut r0: usize) -> Result<usize, usize> {
    asm!(
        "svc $$0"
        : "+{r0}"(r0)
        : "{r7}"(r7)
        : "cc", "memory"
        : "volatile"
    );
    if r0 < 0xffff_f000 {
        Ok(r0)
    } else {
        Err(r0)
    }
}

/// Performs a system call with one argument and never returns.
///
/// The first argument specifies the system call, and the second is the
/// argument to pass it.
///
/// This function should only be used for calls guaranteed to never return.
///
#[inline(always)]
pub unsafe fn syscall_1_nr(r7: usize, r0: usize) -> ! {
    asm!(
        "svc $$0"
        :
        : "{r7}"(r7), "{r0}"(r0)
        : "cc"
        : "volatile"
    );
    unreachable_unchecked()
}

/// Performs a system call with two arguments and returns the result.
///
/// The first argument specifies the system call, and the remaining arguments
/// are the arguments to pass it.
///
#[inline(always)]
pub unsafe fn syscall_2(r7: usize, mut r0: usize, r1: usize) -> Result<usize, usize> {
    asm!(
        "svc $$0"
        : "+{r0}"(r0)
        : "{r7}"(r7), "{r1}"(r1)
        : "cc", "memory"
        : "volatile"
    );
    if r0 < 0xffff_f000 {
        Ok(r0)
    } else {
        Err(r0)
    }
}

/// Performs a system call with two arguments which never returns.
///
/// The first argument specifies the system call, and the remaining arguments
/// are the arguments to pass it.
///
/// This function should only be used for calls guaranteed to never return.
///
#[inline(always)]
pub unsafe fn syscall_2_nr(r7: usize, r0: usize, r1: usize) -> ! {
    asm!(
        "svc $$0"
        :
        : "{r7}"(r7), "{r0}"(r0), "{r1}"(r1)
        : "cc"
        : "volatile"
    );
    unreachable_unchecked()
}

/// Performs a system call with three arguments and returns the result.
///
/// The first argument specifies the system call, and the remaining arguments
/// are the arguments to pass it.
///
#[inline(always)]
pub unsafe fn syscall_3(r7: usize, mut r0: usize, r1: usize, r2: usize) -> Result<usize, usize> {
    asm!(
        "svc $$0"
        : "+{r0}"(r0)
        : "{r7}"(r7), "{r1}"(r1), "{r2}"(r2)
        : "cc", "memory"
        : "volatile"
    );
    if r0 < 0xffff_f000 {
        Ok(r0)
    } else {
        Err(r0)
    }
}

/// Performs a system call with three arguments which never returns.
///
/// The first argument specifies the system call, and the remaining arguments
/// are the arguments to pass it.
///
/// This function should only be used for calls guaranteed to never return.
///
#[inline(always)]
pub unsafe fn syscall_3_nr(r7: usize, r0: usize, r1: usize, r2: usize) -> ! {
    asm!(
        "svc $$0"
        :
        : "{r7}"(r7), "{r0}"(r0), "{r1}"(r1), "{r2}"(r2)
        : "cc"
        : "volatile"
    );
    unreachable_unchecked()
}

/// Performs a system call with four arguments and returns the result.
///
/// The first argument specifies the system call, and the remaining arguments
/// are the arguments to pass it.
///
#[inline(always)]
pub unsafe fn syscall_4(r7: usize, mut r0: usize, r1: usize, r2: usize, r3: usize) -> Result<usize, usize> {
    asm!(
        "svc $$0"
        : "+{r0}"(r0)
        : "{r7}"(r7), "{r1}"(r1), "{r2}"(r2), "{r3}"(r3)
        : "cc", "memory"
        : "volatile"
    );
    if r0 < 0xffff_f000 {
        Ok(r0)
    } else {
        Err(r0)
    }
}

/// Performs a system call with four arguments which never returns.
///
/// The first argument specifies the system call, and the remaining arguments
/// are the arguments to pass it.
///
/// This function should only be used for calls guaranteed to never return.
///
#[inline(always)]
pub unsafe fn syscall_4_nr(r7: usize, r0: usize, r1: usize, r2: usize, r3: usize) -> ! {
    asm!(
        "svc $$0"
        :
        : "{r7}"(r7), "{r0}"(r0), "{r1}"(r1), "{r2}"(r2), "{r3}"(r3)
        : "cc"
        : "volatile"
    );
    unreachable_unchecked()
}

/// Performs a system call with five arguments and returns the result.
///
/// The first argument specifies the system call, and the remaining arguments
/// are the arguments to pass it.
///
#[inline(always)]
pub unsafe fn syscall_5(r7: usize, mut r0: usize, r1: usize, r2: usize, r3: usize, r4: usize) -> Result<usize, usize> {
    asm!(
        "svc $$0"
        : "+{r0}"(r0)
        : "{r7}"(r7), "{r1}"(r1), "{r2}"(r2), "{r3}"(r3), "{r4}"(r4)
        : "cc", "memory"
        : "volatile"
    );
    if r0 < 0xffff_f000 {
        Ok(r0)
    } else {
        Err(r0)
    }
}

/// Performs a system call with five arguments which never returns.
///
/// The first argument specifies the system call, and the remaining arguments
/// are the arguments to pass it.
///
/// This function should only be used for calls guaranteed to never return.
///
#[inline(always)]
pub unsafe fn syscall_5_nr(r7: usize, r0: usize, r1: usize, r2: usize, r3: usize, r4: usize) -> ! {
    asm!(
        "svc $$0"
        :
        : "{r7}"(r7), "{r0}"(r0), "{r1}"(r1), "{r2}"(r2), "{r3}"(r3), "{r4}"(r4)
        : "cc"
        : "volatile"
    );
    unreachable_unchecked()
}

/// Performs a system call with six arguments and returns the result.
///
/// The first argument specifies the system call, and the remaining arguments
/// are the arguments to pass it.
///
#[inline(always)]
pub unsafe fn syscall_6(
    r7: usize,
    mut r0: usize,
    r1: usize,
    r2: usize,
    r3: usize,
    r4: usize,
    r5: usize,
) -> Result<usize, usize>
{
    asm!(
        "svc $$0"
        : "+{r0}"(r0)
        : "{r7}"(r7), "{r1}"(r1), "{r2}"(r2), "{r3}"(r3), "{r4}"(r4), "{r5}"(r5)
        : "cc", "memory"
        : "volatile"
    );
    if r0 < 0xffff_f000 {
        Ok(r0)
    } else {
        Err(r0)
    }
}

/// Performs a system call with six arguments which never returns.
///
/// The first argument specifies the system call, and the remaining arguments
/// are the arguments to pass it.
///
/// This function should only be used for calls guaranteed to never return.
///
#[inline(always)]
pub unsafe fn syscall_6_nr(r7: usize, r0: usize, r1: usize, r2: usize, r3: usize, r4: usize, r5: usize) -> ! {
    asm!(
        "svc $$0"
        :
        : "{r7}"(r7), "{r0}"(r0), "{r1}"(r1), "{r2}"(r2), "{r3}"(r3), "{r4}"(r4), "{r5}"(r5)
        : "cc"
        : "volatile"
    );
    unreachable_unchecked()
}
