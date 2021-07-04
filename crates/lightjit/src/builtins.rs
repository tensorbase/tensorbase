use base::datetimes::{days_to_ymd, unixtime_to_ymd};

//FIXME do we want to use the default tz in mgmt? rather than UTC
//FIXME expensive a little
#[allow(non_snake_case)]
pub fn toYYYY(ut: u64) -> u64 {
    let ymd = unixtime_to_ymd(ut as i32, 0);
    ymd.y as u64
}

#[allow(non_snake_case)]
pub fn date_toYYYY(ut: u64) -> u64 {
    let ymd = days_to_ymd(ut as i32);
    ymd.y as u64
}

#[allow(non_snake_case)]
pub fn date_toYYYYMM(ut: u64) -> u64 {
    let ymd = days_to_ymd(ut as i32);
    ymd.y as u64 * 100 + ymd.m as u64
}

#[allow(non_snake_case)]
pub fn toYYYYMM(ut: u64) -> u64 {
    let ymd = unixtime_to_ymd(ut as i32, 0);
    ymd.y as u64 * 100 + ymd.m as u64
}

#[allow(non_snake_case)]
pub fn toYYYYMMDD(ut: u64) -> u64 {
    let ymd = unixtime_to_ymd(ut as i32, 0);
    ymd.y as u64 * 10000 + ymd.m as u64 * 100 + ymd.d as u64
}

pub const fn rem(dd: u64, ds: u64) -> u64 {
    dd % ds
}

pub fn to_fn1(fn_ptr: *const u8) -> fn(u64) -> u64 {
    unsafe { std::mem::transmute::<_, fn(u64) -> u64>(fn_ptr) }
}
