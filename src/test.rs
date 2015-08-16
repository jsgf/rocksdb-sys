use super::*;
use std::ffi::CString;
use std::ptr;

#[test]
fn simple() {
    unsafe {
        let mut err = ptr::null_mut();
        let name = CString::new("foo-test.db").unwrap();
        let options = rocksdb_options_create();
        assert!(!options.is_null());

        rocksdb_options_set_create_if_missing(options, 1);

        let db = rocksdb_open(options, name.as_ptr(), &mut err);
        assert!(err.is_null());
        assert!(!db.is_null());

        rocksdb_close(db);

        rocksdb_destroy_db(options, name.as_ptr(), &mut err);
        assert!(err.is_null());

        rocksdb_options_destroy(options);
    }
}
