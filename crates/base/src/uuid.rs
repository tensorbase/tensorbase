use crate::errs::{BaseError, BaseResult};
use uuid::adapter::HyphenatedRef;
use uuid::Uuid;

pub fn uuid() -> [u8; 16] {
    *Uuid::new_v4().as_bytes()
}

pub fn parse_uuid(input: &str) -> BaseResult<[u8; 16]> {
    Uuid::parse_str(input)
        .map(|uuid| *uuid.as_bytes())
        .map_err(|_| BaseError::ParseUuidError(input.to_string()))
}

pub fn to_hyphenated_lower(bytes: &[u8; 16]) -> String {
    let mut result = vec![0; HyphenatedRef::LENGTH];
    let bytes = unsafe { std::mem::transmute(bytes) };
    HyphenatedRef::from_uuid_ref(bytes).encode_lower(&mut result);
    unsafe { String::from_utf8_unchecked(result) }
}

#[cfg(test)]
mod tests {
    use crate::uuid::{parse_uuid, to_hyphenated_lower, uuid};

    #[test]
    fn test_uuid() {
        let a = uuid();
        let b = uuid();
        assert_ne!(a, b);
    }

    #[test]
    fn test_parse_uuid() {
        let uuid = parse_uuid("612f3c40-5d3b-217e-707b-6a546a3d7b29").unwrap();
        assert_eq!(b"a/<@];!~p{jTj={)", &uuid);
        let uuid = parse_uuid("00000000-0000-0000-0000-000000000000").unwrap();
        assert_eq!([0; 16], uuid);
        assert!(parse_uuid("err").is_err());
    }

    #[test]
    fn test_to_hyphenated_lower() {
        let uuid = to_hyphenated_lower(b"a/<@];!~p{jTj={)");
        assert_eq!("612f3c40-5d3b-217e-707b-6a546a3d7b29", uuid);
        let uuid = to_hyphenated_lower(&[0; 16]);
        assert_eq!("00000000-0000-0000-0000-000000000000", uuid);
    }
}
