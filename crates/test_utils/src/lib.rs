use std::{
    env::temp_dir,
    fs::{create_dir_all, remove_dir_all},
    path::Path,
};

pub fn prepare_empty_tmp_dir(dir: Option<&str>) -> String {
    let t = temp_dir();
    let tmp_fallback = [t.to_str().unwrap(), "base_test"].join("/");
    let tmp_dir = dir.unwrap_or(&tmp_fallback);
    println!("prepare tmp dir: {}...", tmp_dir);
    if Path::new(tmp_dir).exists() {
        remove_dir_all(tmp_dir).unwrap();
        println!("tmp dir: {} existed, remove firstly", tmp_dir);
    }
    create_dir_all(tmp_dir).unwrap();
    println!("tmp dir: {} created", tmp_dir);

    tmp_dir.to_string()
}

#[cfg(test)]
mod tests {
    use crate::prepare_empty_tmp_dir;

    #[test]
    fn test_prepare_empty_tmp_dir() {
        assert_eq!(prepare_empty_tmp_dir(None), "/tmp/base_test".to_string());
        assert_eq!(
            prepare_empty_tmp_dir(Some("/tmp/base_test2")),
            "/tmp/base_test2".to_string()
        );
    }
}
