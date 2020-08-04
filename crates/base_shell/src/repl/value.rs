use base::errors::Result;
use std::fmt;

/// Value type. Has conversions to every primitive type.
#[derive(Clone, Debug)]
pub struct Value {
    value: String,
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

/// Trait to convert from a [Value](struct.Value.html) to some other type.
pub trait Convert<T> {
    fn convert(&self) -> Result<T>;
}

impl Value {
    pub(crate) fn new(value: &str) -> Self {
        Self {
            value: value.to_string(),
        }
    }
}

impl Convert<String> for Value {
    fn convert(&self) -> Result<String> {
        Ok(self.value.to_string())
    }
}

macro_rules! add_num_converter {
    ($type: ident) => {
        impl Convert<$type> for Value {
            fn convert(&self) -> Result<$type> {
                Ok(self.value.parse::<$type>()?)
            }
        }
    };
}

add_num_converter!(i8);
add_num_converter!(i16);
add_num_converter!(i32);
add_num_converter!(i64);
add_num_converter!(i128);
add_num_converter!(isize);
add_num_converter!(u8);
add_num_converter!(u16);
add_num_converter!(u32);
add_num_converter!(u64);
add_num_converter!(u128);
add_num_converter!(usize);
add_num_converter!(f32);
add_num_converter!(f64);
