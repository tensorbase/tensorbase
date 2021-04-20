use crate::types::SCALE;
use std::convert::From;
use std::fmt::{self, Display, Formatter, Result};
use std::ops::{Div, Mul, Neg, Sub};

pub trait DecimalBits {
    /// Common precision for this datatype
    fn precision() -> u8;
    /// Check if datatype can hold value with certain precision
    /// For i32 it's 0-9, i64: 10-18, i128: 19-27
    fn fit(precision: u8) -> bool;
}
macro_rules! bits {
    ($t:ty => $min:expr, $max: expr) => {
        impl DecimalBits for $t {
            #[inline]
            fn precision() -> u8 {
                $max
            }
            #[allow(unused_comparisons)]
            fn fit(precision: u8) -> bool {
                precision <= $max && precision >= $min
            }
        }
    };
}

bits!(i32 => 0, 9);
bits!(i64 => 10, 18);
#[cfg(feature = "int128")]
bits!(i128 => 19, 27);

/// Provides arbitrary-precision floating point decimal.
#[derive(Clone)]
pub struct Decimal<T> {
    pub(crate) underlying: T,
    pub(crate) precision: u8,
    pub(crate) scale: u8,
}

pub type Decimal32 = Decimal<i32>;
pub type Decimal64 = Decimal<i64>;
#[cfg(feature = "int128")]
pub type Decimal128 = Decimal<i128>;

impl<T: fmt::Debug> fmt::Debug for Decimal<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Decimal")
            .field("underlying", &self.underlying)
            .field("precision", &self.precision)
            .field("scale", &self.scale)
            .finish()
    }
}

impl<T: DecimalBits + Copy> Decimal<T> {
    #[inline]
    pub fn from_parts(underlying: T, precision: u8, scale: u8) -> Decimal<T> {
        Decimal {
            underlying,
            precision,
            scale,
        }
    }

    #[inline]
    pub fn from(underlying: T, scale: u8) -> Decimal<T> {
        Decimal {
            underlying,
            precision: <T as DecimalBits>::precision(),
            scale,
        }
    }

    #[inline(always)]
    pub fn internal(&self) -> T {
        self.underlying
    }

    #[inline(always)]
    pub fn set_internal(&mut self, value: T) {
        self.underlying = value;
    }

    /// Determines how many decimal digits fraction can have.
    #[inline]
    pub fn scale(&self) -> usize {
        self.scale as usize
    }
}

impl<T: Default + DecimalBits> Default for Decimal<T> {
    fn default() -> Self {
        Decimal {
            underlying: Default::default(),
            precision: T::precision(),
            scale: 0,
        }
    }
}

fn format<T>(val: T, scale: usize, f: &mut Formatter<'_>) -> Result
where
    T: Display
        + Default
        + From<i64>
        + Copy
        + PartialOrd
        + Neg<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>
        + Div<Output = T>,
{
    if scale == 0 {
        f.write_fmt(format_args!("{}", val))
    } else {
        let (val, sign) = if val < Default::default() {
            (-val, "-")
        } else {
            (val, "")
        };
        // 128 bit workaround. Because MAGNITUDE has maximum 10^18 value
        // larger values can be obtained as 10^x = 10^18  * 10^(x-18)
        // it's case for i128 values only and can be optimized
        let div: T = if scale < 19 {
            SCALE[scale].into()
        } else {
            Into::<T>::into(SCALE[18]) * Into::<T>::into(SCALE[scale - 18])
        };

        let h = val / div;
        let r = val - h * div;

        f.write_fmt(format_args!(
            "{sign}{h:}.{r:0>scale$}",
            h = h,
            r = r,
            scale = scale,
            sign = sign
        ))
    }
}

impl Display for Decimal<i32> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        format(self.underlying as i64, self.scale as usize, f)
    }
}

impl Display for Decimal<i64> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        format(self.underlying, self.scale as usize, f)
    }
}
#[cfg(feature = "int128")]
impl Display for Decimal<i128> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        format(self.underlying, self.scale as usize, f)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_decimal_format() {
        assert_eq!("269.000", Decimal64::from(269000, 3).to_string());
        assert_eq!("1002", Decimal::from(1002_i32, 0).to_string());
        assert_eq!("-1002", Decimal::from(-1002_i32, 0).to_string());
        assert_eq!("100.2", Decimal::from(1002_i32, 1).to_string());
        assert_eq!("10.02", Decimal::from(1002_i32, 2).to_string());
        assert_eq!("0.1002", Decimal::from(1002_i32, 4).to_string());
        assert_eq!("0.001002", Decimal::from(1002_i32, 6).to_string());
        assert_eq!("-10.02", Decimal::from(-1002_i32, 2).to_string());
        assert_eq!("-0.001002", Decimal::from(-1002_i32, 6).to_string());
    }

    #[test]
    fn test_decimal_range() {
        assert_eq!(i32::fit(8), true);
        assert_ne!(i32::fit(10), true);
        assert_ne!(i64::fit(6), true);
        assert_eq!(i64::fit(14), true);
    }

    #[test]
    #[cfg(feature = "int128")]
    fn test_decimal_format_128() {
        assert_eq!(
            "3.14159265358979323846",
            Decimal::from(3_1415926535_8979323846_i128, 20).to_string()
        );
    }
}
