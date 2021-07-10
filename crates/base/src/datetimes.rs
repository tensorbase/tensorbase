use chrono::{Datelike, Local, NaiveDate, Offset, TimeZone};
use chrono_tz::{OffsetComponents, Tz, TZ_VARIANTS};
use num_integer::Integer;
use serde_derive::{Deserialize, Serialize};

use crate::errs::{BaseError, BaseResult};

use std::{fmt, str::FromStr};

#[derive(Debug, Default, Eq, PartialEq)]
pub struct YMD {
    pub y: u16,
    pub m: u8,
    pub d: u8,
}

#[derive(Debug, Default, Eq, PartialEq)]
pub struct HMS {
    pub h: u8,
    pub m: u8,
    pub s: u8,
}

pub struct YMDHMS(pub i16, pub u8, pub u8, pub u8, pub u8, pub u8);

/// The time zone is a string indicating the name of a time zone:
///
/// As used in the Olson time zone database (the "tz database" or "tzdata"),
/// such as "America/New_York".
///
/// To reduce space usage, `TimeZoneId` is stored instead of the name.
#[derive(
    Serialize,
    Deserialize,
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Default,
)]
pub struct BaseTimeZone {
    /// Id of the time zone.
    tz_id: TimeZoneId,
    /// Offset of the time zone in seconds.
    offset: i32,
}

/// The time zone ID is an `u16` which represents a variety of the `enum` `chrono_tz::Tz`.
///
/// There are 594 variants in total, so `u16` here is needed to store the index.
#[derive(
    Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
pub struct TimeZoneId(u16);

impl FromStr for BaseTimeZone {
    type Err = BaseError;

    fn from_str(tz_name: &str) -> BaseResult<BaseTimeZone> {
        let tz_id = TimeZoneId::from_str(tz_name)?;
        let offset = tz_id.offset();
        Ok(BaseTimeZone { tz_id, offset })
    }
}

impl FromStr for TimeZoneId {
    type Err = BaseError;

    fn from_str(tz_name: &str) -> BaseResult<TimeZoneId> {
        let tz = Tz::from_str(&tz_name)
            .map_err(|_| BaseError::InvalidTimeZone(tz_name.to_string()))?;
        Ok(TimeZoneId(tz as u16))
    }
}

impl From<Tz> for TimeZoneId {
    fn from(tz: Tz) -> TimeZoneId {
        TimeZoneId(tz as u16)
    }
}

impl From<TimeZoneId> for BaseTimeZone {
    fn from(tz_id: TimeZoneId) -> Self {
        BaseTimeZone {
            tz_id,
            offset: tz_id.offset(),
        }
    }
}

impl Default for TimeZoneId {
    fn default() -> TimeZoneId {
        TimeZoneId(Tz::UTC as u16)
    }
}

impl fmt::Display for BaseTimeZone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl fmt::Display for TimeZoneId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl BaseTimeZone {
    /// Get time zone from the local configuration
    pub fn from_local() -> Option<BaseTimeZone> {
        Some(TimeZoneId::from_local()?.into())
    }

    pub fn name(self) -> &'static str {
        self.tz_id.name()
    }
    pub fn offset(self) -> i32 {
        self.offset
    }
}

impl TimeZoneId {
    /// Get time zone from the local configuration
    pub fn from_local() -> Option<TimeZoneId> {
        let ctz = Local::now().offset().fix();
        TZ_VARIANTS.iter().find_map(|tz| {
            let some_time = tz.ymd(1, 1, 1).and_hms(0, 0, 0);
            if some_time.offset().fix() == ctz {
                return Some(TimeZoneId(*tz as u16));
            }
            None
        })
    }

    // SAFETY: the timezone id only comes from a variant of `chrono_tz::Tz`, so it
    // is always valid
    fn tz(self) -> Tz {
        unsafe { std::mem::transmute(self.0) }
    }

    pub fn name(self) -> &'static str {
        self.tz().name()
    }
    pub fn offset(self) -> i32 {
        self.tz()
            .ymd(1, 1, 1)
            .and_hms(0, 0, 0)
            .offset()
            .base_utc_offset()
            .num_seconds() as i32
    }
}

#[inline(always)]
pub fn ymdhms_to_unixtime(dt: YMDHMS, tz_offset: i32) -> u32 {
    sub_tz_offset(
        NaiveDate::from_ymd(dt.0 as i32, dt.1 as u32, dt.2 as u32)
            .and_hms(dt.3 as u32, dt.4 as u32, dt.5 as u32)
            .timestamp() as i32,
        tz_offset,
    ) as u32
}

#[inline(always)]
pub fn ymd_to_days(dt: YMD) -> u16 {
    (NaiveDate::from_ymd(dt.y as i32, dt.m as u32, dt.d as u32).num_days_from_ce()
        - 719_163) as u16
}

#[inline(always)]
pub fn div_mod_floor<T: Integer>(x: T, y: T) -> (T, T) {
    x.div_mod_floor(&y)
}

#[inline(always)]
fn add_tz_offset(unixtime: i32, tz_offset: i32) -> i32 {
    let offset = tz_offset;
    unixtime + offset
}

#[inline(always)]
fn sub_tz_offset(unixtime: i32, tz_offset: i32) -> i32 {
    let offset = tz_offset;
    unixtime - offset
}

#[inline(always)]
pub fn unixtime_to_days(unixtime: i32, tz_offset: i32) -> i64 {
    let unixtime = add_tz_offset(unixtime, tz_offset);
    let (days, _) = div_mod_floor(unixtime as i64, 86_400);
    days
}

#[inline(always)]
pub fn unixtime_to_ymd(unixtime: i32, tz_offset: i32) -> YMD {
    days_to_ymd(unixtime_to_days(unixtime, tz_offset) as i32)
}

#[inline(always)]
pub fn unixtime_to_hms(unixtime: i32, tz_offset: i32) -> HMS {
    let unixtime = add_tz_offset(unixtime, tz_offset);
    let (_, seconds) = div_mod_floor(unixtime, 86_400);
    let (hours, seconds) = div_mod_floor(seconds, 3_600);
    let (minutes, seconds) = div_mod_floor(seconds, 60);
    let (h, m, s) = (hours as u8, minutes as u8, seconds as u8);
    HMS { h, m, s }
}

#[inline(always)]
pub fn unixtime_to_second(unixtime: i32) -> u8 {
    let (_, seconds) = div_mod_floor(unixtime, 60);
    seconds as u8
}

#[inline(always)]
pub fn unixtime_to_year(unixtime: i32, tz_offset: i32) -> u16 {
    let unixtime = add_tz_offset(unixtime, tz_offset);
    let (days, _) = div_mod_floor(unixtime as i64, 86_400);
    days_to_year(days as i32)
}

#[inline(always)]
pub fn unixtime_to_ordinal(unixtime: i32, tz_offset: i32) -> u16 {
    days_to_ordinal(unixtime_to_days(unixtime, tz_offset) as i32)
}

#[inline(always)]
pub fn unixtime_to_weekday(unixtime: i32, tz_offset: i32) -> u8 {
    days_to_weekday(unixtime_to_days(unixtime, tz_offset) as i32)
}

#[inline(always)]
fn days_to_date_opt(days: i32) -> Option<NaiveDate> {
    days.checked_add(719_163)
        .and_then(NaiveDate::from_num_days_from_ce_opt)
}

#[inline(always)]
pub fn days_to_ymd(days: i32) -> YMD {
    days_to_date_opt(days)
        .map(|date| YMD {
            y: date.year() as u16,
            m: date.month() as u8,
            d: date.day() as u8,
        })
        .unwrap_or_default()
}

#[inline(always)]
const fn leaps(years: i32) -> i32 {
    let a = years - 1;
    let b = a >> 2;
    let c = a / 100;
    let d = c >> 2;
    b - c + d
}

#[inline(always)]
pub const fn days_to_year(days0: i32) -> u16 {
    let days = days0 + 719162;
    (1 + (days - leaps(days / 365)) / 365) as u16
}

#[inline(always)]
pub fn days_to_ordinal(days0: i32) -> u16 {
    days_to_date_opt(days0)
        .map(|date| date.ordinal() as u16)
        .unwrap_or_default()
}

#[inline(always)]
pub fn days_to_weekday(days0: i32) -> u8 {
    // day 0 (1970-01-01) is Thursday
    ((days0 + 3) % 7 + 1) as u8
}

#[inline]
fn one_digit(b: u8) -> Result<u64, BaseError> {
    if b < b'0' || b > b'9' {
        return Err(BaseError::InvalidDatetimeDigit);
    }
    Ok((b - b'0') as u64)
}

#[inline]
fn two_digits(b1: u8, b2: u8) -> Result<u64, BaseError> {
    if b1 < b'0' || b2 < b'0' || b1 > b'9' || b2 > b'9' {
        return Err(BaseError::InvalidDatetimeDigit);
    }
    Ok(((b1 - b'0') * 10 + (b2 - b'0')) as u64)
}

/// Parse date time string to the unixtime epoch.
///
/// Only `%Y-%m-%d %H:%M:%S` or `%Y-%m-%dT%H:%M:%S` format is supported,
/// where `%Y` requires **exactly** 4 digits, and `%m`, `%d`, `%H`, `%M`, `%S` requires
/// **exactly** 2 digits.
///
/// `tz_offset` is the offset of timezone in *seconds*.
///
/// # Examples
/// ```
/// use base::datetimes::parse_to_epoch;
/// // parse utc time
/// assert!(matches!(parse_to_epoch("1970-01-01 00:00:00", 0), Ok(0)));
/// // parse local time in Etc/GMT-8
/// assert!(matches!(parse_to_epoch("2021-07-10 12:00:00", 8 * 3600), Ok(1_625_889_600)));
/// // parse error
/// assert!(parse_to_epoch("1970-1-1 00:00:00", 0).is_err());
/// ```
pub fn parse_to_epoch(s: &str, tz_offset: i32) -> BaseResult<u32> {
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

    // TODO: handle the datetime string with timezone correctly
    Ok(ymdhms_to_unixtime(
        YMDHMS(
            year as i16,
            month as u8,
            day as u8,
            hour as u8,
            minute as u8,
            second as u8,
        ),
        tz_offset,
    ))
}

/// Parse date string to the unixtime epoch.
///
/// Only `%Y-%m-%d`format is supported, where `%Y` requires **exactly** 4 digits,
/// and `%m`, `%d` requires 1 or 2 digits.
///
/// # Examples
/// ```
/// use base::datetimes::parse_to_days;
/// // parse ok
/// assert!(matches!(parse_to_days("1970-1-1"), Ok(0)));
/// assert!(matches!(parse_to_days("2021-07-10"), Ok(18_818)));
/// // parse error
/// assert!(parse_to_days("1970-1-1-").is_err());
/// ```
pub fn parse_to_days(s: &str) -> BaseResult<u16> {
    if s.len() < "2018-2-1".len() {
        return Err(BaseError::InvalidDatetimeFormat);
    }
    let b = s.as_bytes(); // for careless slicing
    if b[4] != b'-' {
        return Err(BaseError::InvalidDatetimeFormat);
    }
    let year = two_digits(b[0], b[1])? * 100 + two_digits(b[2], b[3])?;
    let (month, day) = match &b[5..] {
        &[month, b'-', day] => (one_digit(month)?, one_digit(day)?),
        &[month, b'-', d0, d1] => (one_digit(month)?, two_digits(d0, d1)?),
        &[m0, m1, b'-', day] => (two_digits(m0, m1)?, one_digit(day)?),
        &[m0, m1, b'-', d0, d1] => (two_digits(m0, m1)?, two_digits(d0, d1)?),
        _ => return Err(BaseError::InvalidDatetimeFormat),
    };

    if year < 1970 {
        return Err(BaseError::InvalidDatetimeFormat);
    }

    Ok(ymd_to_days(YMD {
        y: year as u16,
        m: month as u8,
        d: day as u8,
    }))
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    use crate::show_option_size;
    use chrono::prelude::*;
    use chrono_tz::TZ_VARIANTS;
    use std::str::FromStr;

    #[test]
    fn basic_check() -> BaseResult<()> {
        show_option_size!(YMDHMS);
        show_option_size!(YMD);
        show_option_size!(HMS);
        let tz_offset = 0;

        let ut = ymdhms_to_unixtime(YMDHMS(1970, 1, 1, 0, 0, 0), tz_offset);
        println!("unixtime: {}", ut);
        assert_eq!(ut, 0);

        let d = ymd_to_days(YMD {
            y: 1970,
            m: 1,
            d: 1,
        });
        println!("days: {}", d);
        assert_eq!(d, 0);

        let ut = ymdhms_to_unixtime(YMDHMS(2004, 9, 17, 0, 0, 0), tz_offset);
        println!("unixtime: {}", ut);
        assert_eq!(ut, 1095379200);

        let d = ymd_to_days(YMD {
            y: 2004,
            m: 9,
            d: 17,
        });
        println!("days: {}", d);
        assert_eq!(d, 12678);

        // 1356388352
        // 1354291200
        let ymd = unixtime_to_ymd(1354291200, tz_offset);
        println!("1356388352 to ymd: {:?}", ymd);
        assert_eq!(
            ymd,
            YMD {
                y: 2012,
                m: 11,
                d: 30
            }
        );

        let s = "2018-02-14 00:28:07";
        let ut0 = parse_to_epoch("2018-02-14T00:28:07", tz_offset).unwrap();
        let ut1 = parse_to_epoch(s, tz_offset).unwrap();
        println!("ut: {:?}", ut);
        assert_eq!(ut0, ut1);
        let dt = NaiveDateTime::from_timestamp(ut0 as i64, 0);
        println!("dt: {:?}", dt);
        assert_eq!(s, dt.to_string());

        let s = "2018-02-14";
        let d0 = parse_to_days("2018-2-14").unwrap();
        let d1 = parse_to_days(s).unwrap();
        println!("d: {:?}", d);
        assert_eq!(d0, d1);

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
    fn test_days_to_ordinal() {
        for days in 0..4096 * 20 {
            let year = days_to_year(days);
            let ordinal = days_to_ordinal(days);
            let date = NaiveDate::from_yo(year as i32, ordinal as u32);
            assert_eq!(year, date.year() as u16);
            assert_eq!(ordinal, date.ordinal() as u16);
        }
    }

    #[test]
    fn test_days_to_weekday() {
        for days in 0..4096 * 20 {
            let weekday = days_to_weekday(days);
            let ymd = days_to_ymd(days);
            let date = NaiveDate::from_ymd(ymd.y as i32, ymd.m as u32, ymd.d as u32);
            assert_eq!(weekday, date.weekday().number_from_monday() as u8);
        }
    }

    #[test]
    fn test_unixtime_to_year() {
        let tz_offset = 8 * 3600;
        for epoch in (1..1000_000_000).step_by(1000) {
            let ymd = unixtime_to_ymd(epoch, tz_offset);
            let y = unixtime_to_year(epoch, tz_offset);
            assert_eq!(y, ymd.y);
        }
    }

    #[test]
    fn test_unixtime_to_ordinal() {
        let tz_offset = 8 * 3600;
        for epoch in (1..1000_000_000).step_by(1000) {
            let year = unixtime_to_year(epoch, tz_offset);
            let ordinal = unixtime_to_ordinal(epoch, tz_offset);
            let date = NaiveDate::from_yo(year as i32, ordinal as u32);
            assert_eq!(year, date.year() as u16);
            assert_eq!(ordinal, date.ordinal() as u16);
        }
    }

    #[test]
    fn test_unixtime_to_weekday() {
        let tz_offset = 8 * 3600;
        for epoch in (1..1000_000_000).step_by(1000) {
            let weekday = unixtime_to_weekday(epoch, tz_offset);
            let ymd = unixtime_to_ymd(epoch, tz_offset);
            let date = NaiveDate::from_ymd(ymd.y as i32, ymd.m as u32, ymd.d as u32);
            assert_eq!(weekday, date.weekday().number_from_monday() as u8);
        }
    }

    #[test]
    fn test_unixtime_to_hms() {
        let tz_offset = 8 * 3600;
        for epoch in 0..86_400 * 10 {
            let ymd = unixtime_to_ymd(epoch as i32, tz_offset);
            let hms = unixtime_to_hms(epoch as i32, tz_offset);
            let seconds = unixtime_to_second(epoch as i32);
            let converted_epoch = ymdhms_to_unixtime(
                YMDHMS(ymd.y as i16, ymd.m, ymd.d, hms.h, hms.m, hms.s),
                tz_offset,
            );
            assert_eq!(epoch, converted_epoch);
            assert_eq!(hms.s, seconds);
        }
    }

    #[test]
    fn test_timezones() {
        let timezones: Vec<_> = TZ_VARIANTS
            .iter()
            .map(|btz| BaseTimeZone::from_str(btz.name()).unwrap())
            .collect();
        let time = "2021-07-03 15:03:28";
        let epoch = 1625324608;
        for tz in timezones {
            let epoch_with_tz = parse_to_epoch(&time, tz.offset()).unwrap() as i32;
            println!(
                "epoch_with_tz - epoch = {}, offset = {}",
                epoch_with_tz - epoch,
                tz.offset()
            );
            assert_eq!(epoch - tz.offset(), epoch_with_tz);
        }
    }
}
