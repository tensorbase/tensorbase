use std::{fs, path::PathBuf};

pub fn validate_path(path: &str) -> Option<PathBuf> {
    if fs::metadata(path).is_ok() {
        Some(PathBuf::from(path))
    } else {
        None
    }
}
