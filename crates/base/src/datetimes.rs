use chrono::{Datelike, NaiveDate};
use num_integer::Integer;
use num_traits::ToPrimitive;

use crate::errs::{BaseError, BaseResult};

#[derive(Debug, Default)]
pub struct YMD {
    pub y: u16,
    pub m: u8,
    pub d: u8,
}

pub struct YMDHMS(pub i16, pub u8, pub u8, pub u8, pub u8, pub u8);

#[inline(always)]
pub fn ymdhms_to_unixtime(dt: YMDHMS) -> u32 {
    NaiveDate::from_ymd(dt.0 as i32, dt.1 as u32, dt.2 as u32)
        .and_hms(dt.3 as u32, dt.4 as u32, dt.5 as u32)
        .timestamp() as u32
}

#[inline(always)]
pub fn div_mod_floor<T: Integer>(x: T, y: T) -> (T, T) {
    x.div_mod_floor(&y)
}

#[inline(always)]
pub fn unixtime_to_ymd(unixtime: i32) -> YMD {
    let (days, _) = div_mod_floor(unixtime as i64, 86_400);
    days_to_ymd(days as i32)
}

#[inline(always)]
pub fn unixtime_to_year(unixtime: i32) -> u16 {
    let (days, _) = div_mod_floor(unixtime as i64, 86_400);
    days_to_year(days as i32)
}

#[inline(always)]
pub fn days_to_ymd(days: i32) -> YMD {
    days.to_i32()
        .and_then(|days| days.checked_add(719_163))
        .and_then(NaiveDate::from_num_days_from_ce_opt)
        .map(|date| YMD {
            y: date.year() as u16,
            m: date.month() as u8,
            d: date.day() as u8,
        })
        .unwrap_or(YMD::default())
}

#[inline(always)]
const fn leaps(years: i32) -> i32 {
    (years - 1) / 4 - (years - 1) / 100 + (years - 1) / 400
}

#[inline(always)]
pub const fn days_to_year(days0: i32) -> u16 {
    let days = days0 + 719162;
    (1 + (days - leaps(days / 365)) / 365) as u16
}

#[inline]
fn two_digits(b1: u8, b2: u8) -> Result<u64, BaseError> {
    if b1 < b'0' || b2 < b'0' || b1 > b'9' || b2 > b'9' {
        return Err(BaseError::InvalidDatetimeDigit);
    }
    Ok(((b1 - b'0') * 10 + (b2 - b'0')) as u64)
}

pub fn parse_to_epoch(s: &str) -> BaseResult<u32> {
    if s.len() < "2018-02-14T00:28:07".len() {
        return Err(BaseError::InvalidDatetimeFormat);
    }
    let b = s.as_bytes(); // for careless slicing
    if b[4] != b'-'
        || b[7] != b'-'
        || (b[10] != b'T' && b[10] != b' ')
        || b[13] != b':'
        || b[16] != b':'
    {
        return Err(BaseError::InvalidDatetimeFormat);
    }
    let year = two_digits(b[0], b[1])? * 100 + two_digits(b[2], b[3])?;
    let month = two_digits(b[5], b[6])?;
    let day = two_digits(b[8], b[9])?;
    let hour = two_digits(b[11], b[12])?;
    let minute = two_digits(b[14], b[15])?;
    let second = two_digits(b[17], b[18])?;

    if year < 1970 || hour > 23 || minute > 59 || second > 60 {
        return Err(BaseError::InvalidDatetimeFormat);
    }

    Ok(ymdhms_to_unixtime(YMDHMS(
        year as i16,
        month as u8,
        day as u8,
        hour as u8,
        minute as u8,
        second as u8,
    )))
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::show_option_size;
    use chrono::prelude::*;

    #[test]
    fn basic_check() -> BaseResult<()> {
        show_option_size!(YMDHMS);

        let ut = ymdhms_to_unixtime(YMDHMS(1970, 1, 1, 0, 0, 0));
        println!("unixtime: {}", ut);
        assert_eq!(ut, 0);

        let ut = ymdhms_to_unixtime(YMDHMS(2004, 9, 17, 0, 0, 0));
        println!("unixtime: {}", ut);
        assert_eq!(ut, 1095379200);

        // 1356388352
        // 1354291200
        let ymd = unixtime_to_ymd(1354291200);
        println!("1356388352 to ymd: {:?}", ymd);

        let s = "2018-02-14 00:28:07";
        let ut0 = parse_to_epoch("2018-02-14T00:28:07").unwrap();
        let ut1 = parse_to_epoch(s).unwrap();
        println!("ut: {:?}", ut);
        assert_eq!(ut0, ut1);
        let dt = NaiveDateTime::from_timestamp(ut0 as i64, 0);
        println!("dt: {:?}", dt);
        assert_eq!(s, dt.to_string());

        // Create a NaiveDateTime from the timestamp
        // let naive = NaiveDateTime::from_timestamp(ut as i64, 0);

        // Create a normal DateTime from the NaiveDateTime
        // let datetime: DateTime<Utc> = DateTime::from_utc(naive, Utc);

        // // Format the datetime how you want
        // let newdate = datetime.format("%Y-%m-%d %H:%M:%S");

        // println!("datetime: {}", datetime);

        // let x = epoch_to_days(1095379200);

        Ok(())
    }

    #[test]
    fn test_days_to_year() {
        for days in 0..4096 * 20 {
            let ymd = days_to_ymd(days);
            let y = days_to_year(days);
            assert_eq!(y, ymd.y);
        }
        // println!("y: {}", days_to_year(4096*10));
    }

    #[test]
    fn test_unixtime_to_year() {
        for epoch in (1..1000_000_000).step_by(1000) {
            let ymd = unixtime_to_ymd(epoch);
            let y = unixtime_to_year(epoch);
            assert_eq!(y, ymd.y);
        }
        // println!("y: {}", days_to_year(4096*10));
    }
}
