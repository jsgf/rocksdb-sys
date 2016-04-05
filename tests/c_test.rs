/*
   Copyright (c) 2011 The LevelDB Authors. All rights reserved.
   Use of this source code is governed by a BSD-style license that can be
   found in the LICENSE file. See the AUTHORS file for names of contributors.
*/
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(unused_unsafe)]
#![allow(unused_variables)]
#![allow(unused_mut)]

extern crate libc;
extern crate rocksdb_sys as ffi;

use self::libc::{c_char, c_int, c_void, uint64_t, size_t};

use std::{env, mem, ptr, slice};
use std::ffi::{CStr, CString};
use std::path::PathBuf;

use ffi::*;

fn cstr(s: &str) -> CString {
    CString::new(s).unwrap()
}

#[cfg(feature = "cstr_memory")]
unsafe fn cstrr(s: &str) -> *const c_char {
    CString::new(s).unwrap().into_ptr()
}
#[cfg(not(feature = "cstr_memory"))]
unsafe fn cstrr(s: &str) -> *const c_char {
    cstrm(s)
}

#[cfg(feature = "cstr_to_str")]
unsafe fn cstrd(s: *const c_char) {
    CString::from_ptr(s as *mut c_char);
}
#[cfg(not(feature = "cstr_to_str"))]
unsafe fn cstrd(s: *const c_char) {
    libc::free(s as *mut c_void);
}

unsafe fn cstrm(s: &str) -> *const c_char {
    let cs = cstr(s);
    let result = libc::malloc(s.len() as size_t);
    ptr::copy(mem::transmute(cs.as_ptr()), result, s.len() as usize);
    result as *const c_char
}

unsafe fn rstr(s: *const c_char) -> String {
    if s.is_null() {
        return "(null)".to_string();
    }
    rstr_(s)
}
#[cfg(feature = "cstr_to_str")]
unsafe fn rstr_(s: *const c_char) -> String {
    CStr::from_ptr(s).to_string_lossy().into_owned()
}
#[cfg(not(feature = "cstr_to_str"))]
unsafe fn rstr_(s: *const c_char) -> String {
    String::from(std::str::from_utf8(
        CStr::from_ptr(s).to_bytes()
    ).unwrap())
}

unsafe fn rvstr(s: *const c_char, v: size_t) -> String {
    if s.is_null() {
        return "(null)".to_string();
    }
    String::from(std::str::from_utf8(
        slice::from_raw_parts(s as *const u8, v as usize)
    ).unwrap())
}

static mut phase: &'static str = "";

fn StartPhase(name: &'static str) {
    unsafe { phase = name };
}

fn GetTempDir() -> PathBuf {
    return match option_env!("TEST_TMPDIR") {
        Some("") | None => env::temp_dir(),
        Some(s) => PathBuf::from(s),
    };
}

macro_rules! CheckNoError {
    ($err:ident) => {{
        assert!($err.is_null(), "{}: {}", phase, rstr($err));
    }};
}

macro_rules! CheckCondition {
    ($cond:expr) => {{
        assert!($cond, "{}: {}", unsafe { phase }, stringify!($cond));
    }};
}

fn CheckEqual(expected: *const c_char, v: *const c_char, n: size_t) {
    if expected.is_null() && v.is_null() {
        // ok
    } else if !expected.is_null() && !v.is_null() && unsafe {
        n == libc::strlen(expected) &&
        libc::memcmp(expected as *const c_void, v as *const c_void, n) == 0 } {
        // ok
    } else {
        unsafe { panic!("{} <> {}", rvstr(expected, n), rvstr(v, n)) };
    }
}

fn Free<T>(ptr: *mut *mut T) {
    unsafe {
        if !(*ptr).is_null() {
            libc::free(*ptr as *mut c_void);
        }
        *ptr = ptr::null_mut();
    }
}

fn CheckGet<'a, 'b>(
    mut db: *mut rocksdb_t,
    options: *mut rocksdb_readoptions_t,
    key: &'a str,
    expected: Option<&'b str>) {

    let mut err: *mut c_char = ptr::null_mut();
    let mut val_len: size_t = 0;
    let ckey = cstr(key);
    let mut val: *mut c_char = unsafe {
        rocksdb_get(db, options, ckey.as_ptr(), key.len() as size_t,
            &mut val_len, &mut err)
    };
    CheckCondition!(err.is_null());
    let oexp = expected.map_or(None, |s| Some(cstr(s)));
    let cexp = oexp.map_or(ptr::null(), |s| s.as_ptr());
    CheckEqual(cexp, val, val_len);
    Free(&mut val);
}

fn CheckGetCF<'a, 'b>(
    db: *mut rocksdb_t,
    options: *const rocksdb_readoptions_t,
    handle: *mut rocksdb_column_family_handle_t,
    key: &'a str,
    expected: Option<&'b str>) {
    unsafe {
        let mut err: *mut c_char = ptr::null_mut();
        let mut val_len: size_t = 0;
        let k = cstr(key);
        let mut val: *mut c_char = rocksdb_get_cf(db, options, handle,
            k.as_ptr(), key.len() as size_t, &mut val_len, &mut err);
        CheckNoError!(err);
        let oexp = expected.map_or(None, |s| Some(cstr(s)));
        let cexp = oexp.map_or(ptr::null(), |s| s.as_ptr());
        CheckEqual(cexp, val, val_len);
        Free(&mut val);
    }
}


fn CheckIter<'a, 'b>(iter: *mut rocksdb_iterator_t,
             key: &'a str, val: &'b str) {
  let mut len: size_t = 0;
  let mut str: *const c_char;
  str = unsafe { rocksdb_iter_key(iter, &mut len) };
  { let k = cstr(key); CheckEqual(k.as_ptr(), str, len) };
  str = unsafe { rocksdb_iter_value(iter, &mut len) };
  { let v = cstr(val); CheckEqual(v.as_ptr(), str, len) };
}

// Callback from rocksdb_writebatch_iterate()
extern "C" fn CheckPut(ptr: *mut c_void,
                       k: *const c_char, klen: size_t,
                       v: *const c_char, vlen: size_t) {
  let mut state: *mut c_int = ptr as *mut c_int;
  unsafe {
    CheckCondition!(*state < 2);
    match *state {
        0 => {
            let l = cstr("bar"); let b = cstr("b");
            CheckEqual(l.as_ptr(), k, klen);
            CheckEqual(b.as_ptr(), v, vlen);
        }
        1 => {
            let l = cstr("box"); let b = cstr("c");
            CheckEqual(l.as_ptr(), k, klen);
            CheckEqual(b.as_ptr(), v, vlen);
        }
        _ => {}
    }
    (*state) += 1;
  }
}

// Callback from rocksdb_writebatch_iterate()
extern "C" fn CheckDel(ptr: *mut c_void,
                       k: *const c_char, klen: size_t) {
    let mut state: *mut c_int = ptr as *mut c_int;
    CheckCondition!(unsafe { *state == 2 });
    let v = cstr("bar");
    CheckEqual(v.as_ptr(), k, klen);
    unsafe { (*state) += 1 };
}

extern "C" fn cmp_destroy(arg: *mut c_void) {}

extern "C" fn cmp_compare(arg: *mut c_void, a: *const c_char, alen: size_t,
                                                    b: *const c_char, blen: size_t) -> c_int {
    let n = if alen < blen { alen } else { blen };
    let mut r = unsafe {
        libc::memcmp(a as *const c_void, b as *const c_void, n)
    };
    if r == 0 {
        if alen < blen {
            r = -1;
        } else if alen > blen {
            r = 1;
        }
    }
    return r;
}

extern "C" fn cmp_name(arg: *mut c_void) -> *const c_char {
    const name: &'static [u8] = b"foo\0";
    name.as_ptr() as *const c_char
}

// Custom filter policy
static mut fake_filter_result: u8 = 1;
extern "C" fn FilterDestroy(arg: *mut c_void) { }
extern "C" fn FilterName(arg: *mut c_void) -> *const c_char {
    const name: &'static [u8] = b"TestFilter\0";
    name.as_ptr() as *const c_char
}
extern "C" fn FilterCreate(
    arg: *mut c_void,
    key_array: *const *const c_char,
    key_length_array: *const size_t,
    num_keys: c_int,
    filter_length: *mut size_t
    ) -> *mut c_char {
    unsafe {
        *filter_length = 4;
        cstrm("fake") as *mut c_char
    }
}
extern "C" fn FilterKeyMatch(
    arg: *mut c_void,
    key: *const c_char, length: size_t,
    filter: *const c_char, filter_length: size_t
    ) -> u8 {
    let cfake = cstr("fake");
    unsafe {
        CheckCondition!(filter_length == 4);
        CheckCondition!(libc::memcmp(
            filter as *const c_void,
            cfake.as_ptr() as *const c_void,
            filter_length) == 0);
        fake_filter_result
    }
}

// Custom compaction filter
extern "C" fn CFilterDestroy(arg: *mut c_void) {}
extern "C" fn CFilterName(arg: *mut c_void) -> *const c_char {
    const name: &'static [u8] = b"foo\0";
    name.as_ptr() as *const c_char
}
extern "C" fn CFilterFilter(
    arg: *mut c_void,
    level: c_int,
    key: *const c_char,
    key_length: size_t,
    existing_value: *const c_char,
    value_length: size_t,
    new_value: *mut *mut c_char,
    new_value_length: *mut size_t,
    value_changed: *mut u8
    ) -> u8 {
    if key_length == 3 {
        let bar = cstr("bar");
        let baz = cstr("baz");
        unsafe {
            if libc::memcmp(
                mem::transmute(key),
                mem::transmute(bar.as_ptr()), key_length) == 0 {
                return 1;
            } else if libc::memcmp(
                mem::transmute(key),
                mem::transmute(baz.as_ptr()), key_length) == 0 {
                *value_changed = 1;
                *new_value = cstrm("newbazvalue") as *mut c_char;
                *new_value_length = 11;
                return 0;
            }
        }
    }
    return 0;
}

extern "C" fn CFilterFactoryDestroy(arg: *mut c_void) {}
extern "C" fn CFilterFactoryName(arg: *mut c_void) -> *const c_char {
    const name: &'static [u8] = b"foo\0";
    name.as_ptr() as *const c_char
}
extern "C" fn CFilterCreate(
    arg: *mut c_void, context: *mut rocksdb_compactionfiltercontext_t)
    -> *mut rocksdb_compactionfilter_t {
    unsafe {
        rocksdb_compactionfilter_create(ptr::null_mut(),
            Some(CFilterDestroy), Some(CFilterFilter), Some(CFilterName))
    }
}

fn CheckCompaction(db: *mut rocksdb_t,
    roptions: *mut rocksdb_readoptions_t,
    woptions: *mut rocksdb_writeoptions_t) -> *mut rocksdb_t {
    unsafe {
        let mut err: *mut c_char = ptr::null_mut();
        CheckNoError!(err);
        {
            let k = cstr("foo"); let v = cstr("foovalue");
            rocksdb_put(db, woptions, k.as_ptr(), 3, v.as_ptr(), 8, &mut err);
        }
        CheckNoError!(err);
        CheckGet(db, roptions, "foo", Some("foovalue"));
        {
            let k = cstr("bar"); let b = cstr("barvalue");
            rocksdb_put(db, woptions, k.as_ptr(), 3, b.as_ptr(), 8, &mut err);
        }
        CheckNoError!(err);
        CheckGet(db, roptions, "bar", Some("barvalue"));
        {
            let k = cstr("baz"); let b = cstr("bazvalue");
            rocksdb_put(db, woptions, k.as_ptr(), 3, b.as_ptr(), 8, &mut err);
        }
        CheckNoError!(err);
        CheckGet(db, roptions, "baz", Some("bazvalue"));

        // Force compaction
        rocksdb_compact_range(db, ptr::null(), 0, ptr::null(), 0);
        // should have filtered bar, but not foo
        CheckGet(db, roptions, "foo", Some("foovalue"));
        CheckGet(db, roptions, "bar", None);
        CheckGet(db, roptions, "baz", Some("newbazvalue"));
        return db;
    }
}

// Custom merge operator
extern "C" fn MergeOperatorDestroy(arg: *mut c_void) { }
extern "C" fn MergeOperatorName(arg: *mut c_void) -> *const c_char {
    const name: &'static [u8] = b"TestMergeOperator\0";
    name.as_ptr() as *const c_char
}
extern "C" fn MergeOperatorFullMerge(
    arg: *mut c_void,
    key: *const c_char,
    key_length: size_t,
    existing_value: *const c_char,
    existing_value_length: size_t,
    operands_list: *const *const c_char,
    operands_list_length: *const size_t,
    num_operands: c_int,
    success: *mut u8,
    new_value_length: *mut size_t
    ) -> *mut c_char {
    unsafe {
        *new_value_length = 4;
        *success = 1;
        cstrm("fake") as *mut c_char
    }
}
extern "C" fn MergeOperatorPartialMerge(
    arg: *mut c_void,
    key: *const c_char,
    key_length: size_t,
    operands_list: *const *const c_char,
    operands_list_length: *const size_t,
    num_operands: c_int,
    success: *mut u8,
    new_value_length: *mut size_t
    ) -> *mut c_char {
    unsafe {
        *new_value_length = 4;
        *success = 1;
        cstrm("fake") as *mut c_char
    }
}

#[test]
fn test_ffi() {
    unsafe {
        let mut db: *mut rocksdb_t;
        let mut cmp: *mut rocksdb_comparator_t;
        let mut cache: *mut rocksdb_cache_t;
        let mut env: *mut rocksdb_env_t;
        let mut options: *mut rocksdb_options_t;
        let mut table_options: *mut rocksdb_block_based_table_options_t;
        let mut roptions: *mut rocksdb_readoptions_t;
        let mut woptions: *mut rocksdb_writeoptions_t;
        let mut err: *mut c_char = ptr::null_mut();

        let dbname: *const c_char = {
            let mut dir = GetTempDir();
            dir.push(format!("rocksdb_c_test-{}", libc::geteuid()));
            let path = dir.to_str().unwrap();
            cstrr(path)
        };
        let dbbackupname: *const c_char = {
            let mut dir = GetTempDir();
            dir.push(format!("rocksdb_c_test-{}-backup", libc::geteuid()));
            let path = dir.to_str().unwrap();
            cstrr(path)
        };

        StartPhase("create_objects");
        cmp = rocksdb_comparator_create(ptr::null_mut(),
            Some(cmp_destroy), Some(cmp_compare), Some(cmp_name));
        env = rocksdb_create_default_env();
        cache = rocksdb_cache_create_lru(100000);

        options = rocksdb_options_create();
        rocksdb_options_set_comparator(options, cmp);
        rocksdb_options_set_error_if_exists(options, 1);
        rocksdb_options_set_env(options, env);
        rocksdb_options_set_info_log(options, ptr::null_mut());
        rocksdb_options_set_write_buffer_size(options, 100000);
        rocksdb_options_set_paranoid_checks(options, 1);
        rocksdb_options_set_max_open_files(options, 10);
        table_options = rocksdb_block_based_options_create();
        rocksdb_block_based_options_set_block_cache(table_options, cache);
        rocksdb_options_set_block_based_table_factory(options, table_options);

        let no_compression = rocksdb_no_compression as c_int;
        rocksdb_options_set_compression(options, no_compression);
        rocksdb_options_set_compression_options(options, -14, -1, 0);
        let compression_levels = vec![
            no_compression,
            no_compression,
            no_compression,
            no_compression,
            ];
        rocksdb_options_set_compression_per_level(options,
            mem::transmute(compression_levels.as_ptr()),
            compression_levels.len() as size_t);

        roptions = rocksdb_readoptions_create();
        rocksdb_readoptions_set_verify_checksums(roptions, 1);
        rocksdb_readoptions_set_fill_cache(roptions, 0);

        woptions = rocksdb_writeoptions_create();
        rocksdb_writeoptions_set_sync(woptions, 1);

        StartPhase("destroy");
        rocksdb_destroy_db(options, dbname, &mut err);
        Free(&mut err);

        StartPhase("open_error");
        rocksdb_open(options, dbname, &mut err);
        CheckCondition!(!err.is_null());
        Free(&mut err);

        StartPhase("open");
        rocksdb_options_set_create_if_missing(options, 1);
        db = rocksdb_open(options, dbname, &mut err);
        CheckNoError!(err);
        CheckGet(db, roptions, "foo", None);

        StartPhase("put");
        {
            let k = cstr("foo"); let v = cstr("hello");
            rocksdb_put(db, woptions, k.as_ptr(), 3, v.as_ptr(), 5, &mut err);
        }
        CheckNoError!(err);
        CheckGet(db, roptions, "foo", Some("hello"));

        StartPhase("backup_and_restore");
        {
            rocksdb_destroy_db(options, dbbackupname, &mut err);
            // FIXME:
            // CheckNoError!(err);
            if !err.is_null() { Free(&mut err); }

            let be = rocksdb_backup_engine_open(options, dbbackupname, &mut err);
            CheckNoError!(err);

            rocksdb_backup_engine_create_new_backup(be, db, &mut err);
            CheckNoError!(err);

            {
                let k = cstr("foo");
                rocksdb_delete(db, woptions, k.as_ptr(), 3, &mut err);
            }
            CheckNoError!(err);

            rocksdb_close(db);

            rocksdb_destroy_db(options, dbname, &mut err);
            CheckNoError!(err);

            let restore_options = rocksdb_restore_options_create();
            rocksdb_restore_options_set_keep_log_files(restore_options, 0);
            rocksdb_backup_engine_restore_db_from_latest_backup(be, dbname, dbname, restore_options, &mut err);
            CheckNoError!(err);
            rocksdb_restore_options_destroy(restore_options);

            rocksdb_options_set_error_if_exists(options, 0);
            db = rocksdb_open(options, dbname, &mut err);
            CheckNoError!(err);
            rocksdb_options_set_error_if_exists(options, 1);

            CheckGet(db, roptions, "foo", Some("hello"));

            rocksdb_backup_engine_close(be);
        }

        StartPhase("compactall");
        rocksdb_compact_range(db, ptr::null(), 0, ptr::null(), 0);
        CheckGet(db, roptions, "foo", Some("hello"));

        StartPhase("compactrange");
        {
            let a = cstr("a"); let z = cstr("z");
            rocksdb_compact_range(db, a.as_ptr(), 1, z.as_ptr(), 1);
        }
        CheckGet(db, roptions, "foo", Some("hello"));

        StartPhase("writebatch");
        {
            let mut wb = rocksdb_writebatch_create();
            {
                let k = cstr("foo"); let v = cstr("a");
                rocksdb_writebatch_put(wb, k.as_ptr(), 3, v.as_ptr(), 1);
            }
            rocksdb_writebatch_clear(wb);
            {
                let k = cstr("bar"); let v = cstr("b");
                rocksdb_writebatch_put(wb, k.as_ptr(), 3, v.as_ptr(), 1);
            }
            {
                let k = cstr("box"); let v = cstr("c");
                rocksdb_writebatch_put(wb, k.as_ptr(), 3, v.as_ptr(), 1);
            }
            {
                let k = cstr("bar");
                rocksdb_writebatch_delete(wb, k.as_ptr(), 3);
            }
            rocksdb_write(db, woptions, wb, &mut err);
            CheckNoError!(err);
            CheckGet(db, roptions, "foo", Some("hello"));
            CheckGet(db, roptions, "bar", None);
            CheckGet(db, roptions, "box", Some("c"));
            let mut pos: c_int = 0;
            rocksdb_writebatch_iterate(wb, mem::transmute(&mut pos),
                Some(CheckPut), Some(CheckDel));
            CheckCondition!(pos == 3);
            rocksdb_writebatch_destroy(wb);
        }

        StartPhase("writebatch_vectors");
        {
            let wb = rocksdb_writebatch_create();
            let k_listm = vec![cstr("z"), cstr("ap")];
            let k_list: Vec<*const c_char> = k_listm.iter().map(|s| s.as_ptr()).collect();
            let k_sizes: Vec<size_t> = vec![1, 2];
            let v_listm = vec![cstr("x"), cstr("y"), cstr("z")];
            let v_list: Vec<*const c_char> = v_listm.iter().map(|s| s.as_ptr()).collect();
            let v_sizes: Vec<size_t> = vec![1, 1, 1];
            rocksdb_writebatch_putv(wb,
                k_list.len() as c_int,
                k_list.as_ptr(),
                k_sizes.as_ptr(),
                v_list.len() as c_int,
                v_list.as_ptr(),
                v_sizes.as_ptr()
                );
            rocksdb_write(db, woptions, wb, &mut err);
            CheckNoError!(err);
            CheckGet(db, roptions, "zap", Some("xyz"));
            {
                let k = cstr("zap");
                rocksdb_writebatch_delete(wb, k.as_ptr(), 3);
            }
            rocksdb_write(db, woptions, wb, &mut err);
            CheckNoError!(err);
            CheckGet(db, roptions, "zap", None);
            rocksdb_writebatch_destroy(wb);
        }

        StartPhase("writebatch_rep");
        {
            let wb1 = rocksdb_writebatch_create();
            {
                let k = cstr("baz"); let v = cstr("d");
                rocksdb_writebatch_put(wb1, k.as_ptr(), 3, v.as_ptr(), 1);
            }
            {
                let k = cstr("quux"); let v = cstr("e");
                rocksdb_writebatch_put(wb1, k.as_ptr(), 4, v.as_ptr(), 1);
            }
            {
                let k = cstr("quux");
                rocksdb_writebatch_delete(wb1, k.as_ptr(), 4);
            }
            let mut repsize1: size_t = 0;
            let mut rep = rocksdb_writebatch_data(wb1, &mut repsize1) as *const c_void;
            let mut wb2 = rocksdb_writebatch_create_from(rep as *const c_char, repsize1);
            CheckCondition!(rocksdb_writebatch_count(wb1) ==
                            rocksdb_writebatch_count(wb2));
            let mut repsize2: size_t = 0;
            CheckCondition!(
                libc::memcmp(rep,
                    rocksdb_writebatch_data(wb2, &mut repsize2) as *const c_void,
                    repsize1) == 0);
            rocksdb_writebatch_destroy(wb1);
            rocksdb_writebatch_destroy(wb2);
        }

        StartPhase("iter");
        {
            let mut iter = rocksdb_create_iterator(db, roptions);
            CheckCondition!(rocksdb_iter_valid(iter) == 0);
            rocksdb_iter_seek_to_first(iter);
            CheckCondition!(rocksdb_iter_valid(iter) != 0);
            CheckIter(iter, "box", "c");
            rocksdb_iter_next(iter);
            CheckIter(iter, "foo", "hello");
            rocksdb_iter_prev(iter);
            CheckIter(iter, "box", "c");
            rocksdb_iter_prev(iter);
            CheckCondition!(rocksdb_iter_valid(iter) == 0);
            rocksdb_iter_seek_to_last(iter);
            CheckIter(iter, "foo", "hello");
            {
                let k = cstr("b");
                rocksdb_iter_seek(iter, k.as_ptr(), 1);
            }
            CheckIter(iter, "box", "c");
            rocksdb_iter_get_error(iter, &mut err);
            CheckNoError!(err);
            rocksdb_iter_destroy(iter);
        }

        StartPhase("multiget");
        {
            let keysm = vec![cstr("box"), cstr("foo"), cstr("notfound")];
            let keys: Vec<*const c_char> = keysm.iter().map(|s| s.as_ptr()).collect();
            let keys_sizes: [size_t; 3] = [3, 3, 8];
            let mut vals: [*mut c_char; 3] = [ptr::null_mut(), ptr::null_mut(), ptr::null_mut()];
            let mut vals_sizes: [size_t; 3] = [0, 0, 0];
            let mut errs: [*mut c_char; 3] = [ptr::null_mut(), ptr::null_mut(), ptr::null_mut()];
            rocksdb_multi_get(db, roptions, 3,
                keys.as_ptr(), keys_sizes.as_ptr(),
                vals.as_mut_ptr(),
                vals_sizes.as_mut_ptr(),
                errs.as_mut_ptr());

            for i in 0..3 {
                CheckEqual(ptr::null(), errs[i], 0);
                match i {
                    0 => {
                        let v = cstr("c");
                        CheckEqual(v.as_ptr(), vals[i], vals_sizes[i])
                    }
                    1 => {
                        let v = cstr("hello");
                        CheckEqual(v.as_ptr(), vals[i], vals_sizes[i])
                    }
                    2 => CheckEqual(ptr::null(), vals[i], vals_sizes[i]),
                    _ => {}
                }
                Free(&mut vals[i]);
            }
        }

        StartPhase("approximate_sizes");
        {
            let mut sizes: [uint64_t; 2] = [0, 0];
            let startm = vec![cstr("a"), cstr("k00000000000000010000")];
            let start: Vec<*const c_char> = startm.iter().map(|s| s.as_ptr()).collect();
            let start_len: [size_t; 2] = [1, 21];
            let limitm = vec![cstr("k00000000000000010000"), cstr("z")];
            let limit: Vec<*const c_char> = limitm.iter().map(|s| s.as_ptr()).collect();
            let limit_len: [size_t; 2] = [21, 1];
            rocksdb_writeoptions_set_sync(woptions, 0);
            for i in 0..20000 {
                let keybuf = CString::new(format!("k{:020}", i)).unwrap();
                let key = keybuf.to_bytes_with_nul();
                let valbuf = CString::new(format!("v{:020}", i)).unwrap();
                let val = valbuf.to_bytes_with_nul();
                rocksdb_put(db, woptions,
                    key.as_ptr() as *const c_char, key.len() as size_t,
                    val.as_ptr() as *const c_char, val.len() as size_t,
                    &mut err);
                CheckNoError!(err);
            }
            rocksdb_approximate_sizes(db, 2,
                start.as_ptr(),
                start_len.as_ptr(),
                limit.as_ptr(),
                limit_len.as_ptr(),
                sizes.as_mut_ptr());
            CheckCondition!(sizes[0] > 0);
            CheckCondition!(sizes[1] > 0);
        }

        StartPhase("property");
        {
            let mut prop: *mut c_char;
            prop = {
                let v = cstr("nosuchprop");
                rocksdb_property_value(db, v.as_ptr())
            };
            CheckCondition!(prop.is_null());
            prop = {
                let v = cstr("rocksdb.stats");
                rocksdb_property_value(db, v.as_ptr())
            };
            CheckCondition!(!prop.is_null());
            Free(&mut prop);
        }

        StartPhase("snapshot");
        {
            let snap: *const rocksdb_snapshot_t = rocksdb_create_snapshot(db);
            {
                let k = cstr("foo");
                rocksdb_delete(db, woptions, k.as_ptr(), 3, &mut err);
            }
            CheckNoError!(err);
            rocksdb_readoptions_set_snapshot(roptions, snap);
            CheckGet(db, roptions, "foo", Some("hello"));
            rocksdb_readoptions_set_snapshot(roptions, ptr::null());
            CheckGet(db, roptions, "foo", None);
            rocksdb_release_snapshot(db, snap);
        }

        StartPhase("repair");
        {
            // If we do not compact here, then the lazy deletion of
            // files (https://reviews.facebook.net/D6123) would leave
            // around deleted files and the repair process will find
            // those files and put them back into the database.
            rocksdb_compact_range(db, ptr::null(), 0, ptr::null(), 0);
            rocksdb_close(db);
            rocksdb_options_set_create_if_missing(options, 0);
            rocksdb_options_set_error_if_exists(options, 0);
            rocksdb_repair_db(options, dbname, &mut err);
            CheckNoError!(err);
            db = rocksdb_open(options, dbname, &mut err);
            CheckNoError!(err);
            CheckGet(db, roptions, "foo", None);
            CheckGet(db, roptions, "bar", None);
            CheckGet(db, roptions, "box", Some("c"));
            rocksdb_options_set_create_if_missing(options, 1);
            rocksdb_options_set_error_if_exists(options, 1);
        }

        StartPhase("filter");
        for run in 0..2 {
            // First run uses custom filter, second run uses bloom filter
            CheckNoError!(err);
            let mut policy: *mut rocksdb_filterpolicy_t = if run == 0 {
                rocksdb_filterpolicy_create(ptr::null_mut(),
                    Some(FilterDestroy), Some(FilterCreate), Some(FilterKeyMatch),
                    None, Some(FilterName))
            } else {
                rocksdb_filterpolicy_create_bloom(10)
            };

            rocksdb_block_based_options_set_filter_policy(table_options, policy);

            // Create new database
            rocksdb_close(db);
            rocksdb_destroy_db(options, dbname, &mut err);
            rocksdb_options_set_block_based_table_factory(options, table_options);
            db = rocksdb_open(options, dbname, &mut err);
            CheckNoError!(err);
            {
                let k = cstr("foo"); let v = cstr("foovalue");
                rocksdb_put(db, woptions, k.as_ptr(), 3, v.as_ptr(), 8, &mut err);
            }
            CheckNoError!(err);
            {
                let k = cstr("bar"); let v = cstr("barvalue");
                rocksdb_put(db, woptions, k.as_ptr(), 3, v.as_ptr(), 8, &mut err);
            }
            CheckNoError!(err);
            rocksdb_compact_range(db, ptr::null(), 0, ptr::null(), 0);

            fake_filter_result = 1;
            CheckGet(db, roptions, "foo", Some("foovalue"));
            CheckGet(db, roptions, "bar", Some("barvalue"));
            if phase == "" {
              // Must not find value when custom filter returns false
              fake_filter_result = 0;
              CheckGet(db, roptions, "foo", None);
              CheckGet(db, roptions, "bar", None);
              fake_filter_result = 1;

              CheckGet(db, roptions, "foo", Some("foovalue"));
              CheckGet(db, roptions, "bar", Some("barvalue"));
            }
            // Reset the policy
            rocksdb_block_based_options_set_filter_policy(table_options, ptr::null_mut());
            rocksdb_options_set_block_based_table_factory(options, table_options);
        }

        StartPhase("compaction_filter");
        {
            let options_with_filter = rocksdb_options_create();
            rocksdb_options_set_create_if_missing(options_with_filter, 1);
            let cfilter = rocksdb_compactionfilter_create(ptr::null_mut(),
                Some(CFilterDestroy), Some(CFilterFilter), Some(CFilterName));
            // Create new database
            rocksdb_close(db);
            rocksdb_destroy_db(options_with_filter, dbname, &mut err);
            rocksdb_options_set_compaction_filter(options_with_filter, cfilter);
            db = rocksdb_open(options_with_filter, dbname, &mut err);
            CheckNoError!(err);
            db = CheckCompaction(db, roptions, woptions);

            rocksdb_options_set_compaction_filter(options_with_filter, ptr::null_mut());
            rocksdb_compactionfilter_destroy(cfilter);
            rocksdb_options_destroy(options_with_filter);
        }

        StartPhase("compaction_filter_factory");
        {
            let mut options_with_filter_factory = rocksdb_options_create();
            rocksdb_options_set_create_if_missing(options_with_filter_factory, 1);
            let mut factory = rocksdb_compactionfilterfactory_create(
                ptr::null_mut(),
                Some(CFilterFactoryDestroy), Some(CFilterCreate), Some(CFilterFactoryName));
            // Create new database
            rocksdb_close(db);
            rocksdb_destroy_db(options_with_filter_factory, dbname, &mut err);
            rocksdb_options_set_compaction_filter_factory(options_with_filter_factory,
                                                          factory);
            db = rocksdb_open(options_with_filter_factory, dbname, &mut err);
            CheckNoError!(err);
            db = CheckCompaction(db, roptions, woptions);

            rocksdb_options_set_compaction_filter_factory(
                options_with_filter_factory, ptr::null_mut());
            rocksdb_options_destroy(options_with_filter_factory);
        }

        StartPhase("merge_operator");
        {
            let mut merge_operator = rocksdb_mergeoperator_create(
                ptr::null_mut(),
                Some(MergeOperatorDestroy),
                Some(MergeOperatorFullMerge),
                Some(MergeOperatorPartialMerge),
                None,
                Some(MergeOperatorName));
            // Create new database
            rocksdb_close(db);
            rocksdb_destroy_db(options, dbname, &mut err);
            rocksdb_options_set_merge_operator(options, merge_operator);
            db = rocksdb_open(options, dbname, &mut err);
            CheckNoError!(err);
            {
                let k = cstr("foo"); let v = cstr("foovalue");
                rocksdb_put(db, woptions, k.as_ptr(), 3, v.as_ptr(), 8, &mut err);
            }
            CheckNoError!(err);
            CheckGet(db, roptions, "foo", Some("foovalue"));
            {
                let k = cstr("foo"); let v = cstr("barvalue");
                rocksdb_merge(db, woptions, k.as_ptr(), 3, v.as_ptr(), 8, &mut err);
            }
            CheckNoError!(err);
            CheckGet(db, roptions, "foo", Some("fake"));

            // Merge of a non-existing value
            {
                let k = cstr("bar"); let v = cstr("barvalue");
                rocksdb_merge(db, woptions, k.as_ptr(), 3, v.as_ptr(), 8, &mut err);
            }
            CheckNoError!(err);
            CheckGet(db, roptions, "bar", Some("fake"));
        }

        StartPhase("columnfamilies");
        {
            rocksdb_close(db);
            rocksdb_destroy_db(options, dbname, &mut err);
            CheckNoError!(err);

            let mut db_options = rocksdb_options_create();
            rocksdb_options_set_create_if_missing(db_options, 1);
            db = rocksdb_open(db_options, dbname, &mut err);
            CheckNoError!(err);
            let mut cfh = {
                let k = cstr("cf1");
                rocksdb_create_column_family(db, db_options, k.as_ptr(), & mut err)
            };
            rocksdb_column_family_handle_destroy(cfh);
            CheckNoError!(err);
            rocksdb_close(db);

            let mut cflen: size_t = 0;
            let column_fams_raw = rocksdb_list_column_families(db_options, dbname, &mut cflen, &mut err);
            let column_fams = slice::from_raw_parts(column_fams_raw, cflen as usize);
            CheckNoError!(err);
            {
                let v = cstr("default");
                CheckEqual(v.as_ptr(), column_fams[0], 7);
            }
            {
                let v = cstr("cf1");
                CheckEqual(v.as_ptr(), column_fams[1], 3);
            }
            CheckCondition!(cflen == 2);
            rocksdb_list_column_families_destroy(column_fams_raw, cflen);

            let mut cf_options = rocksdb_options_create();

            let cf_namesm = vec![cstr("default"), cstr("cf1")];
            let cf_names: Vec<*const c_char> = cf_namesm.iter().map(|s| s.as_ptr()).collect();
            let cf_opts: Vec<*mut rocksdb_options_t> = vec![cf_options, cf_options];
            let mut handles: [*mut rocksdb_column_family_handle_t; 2] = [ptr::null_mut(), ptr::null_mut()];
            db = rocksdb_open_column_families(db_options, dbname, 2,
                cf_names.as_ptr(), cf_opts.as_ptr(), handles.as_mut_ptr(), &mut err);
            CheckNoError!(err);

            {
                let k = cstr("foo"); let v = cstr("hello");
                rocksdb_put_cf(db, woptions, handles[1], k.as_ptr(), 3, v.as_ptr(), 5, &mut err);
            }
            CheckNoError!(err);

            CheckGetCF(db, roptions, handles[1], "foo", Some("hello"));

            {
                let k = cstr("foo");
                rocksdb_delete_cf(db, woptions, handles[1], k.as_ptr(), 3, &mut err);
            }
            CheckNoError!(err);

            CheckGetCF(db, roptions, handles[1], "foo", None);

            let mut wb = rocksdb_writebatch_create();
            {
                let k = cstr("baz"); let v = cstr("a");
                rocksdb_writebatch_put_cf(wb, handles[1], k.as_ptr(), 3, v.as_ptr(), 1);
            }
            rocksdb_writebatch_clear(wb);
            {
                let k = cstr("bar"); let v = cstr("b");
                rocksdb_writebatch_put_cf(wb, handles[1], k.as_ptr(), 3, v.as_ptr(), 1);
            }
            {
                let k = cstr("box"); let v = cstr("c");
                rocksdb_writebatch_put_cf(wb, handles[1], k.as_ptr(), 3, v.as_ptr(), 1);
            }
            {
                let k = cstr("bar");
                rocksdb_writebatch_delete_cf(wb, handles[1], k.as_ptr(), 3);
            }
            rocksdb_write(db, woptions, wb, &mut err);
            CheckNoError!(err);
            CheckGetCF(db, roptions, handles[1], "baz", None);
            CheckGetCF(db, roptions, handles[1], "bar", None);
            CheckGetCF(db, roptions, handles[1], "box", Some("c"));
            rocksdb_writebatch_destroy(wb);

            let get_handles: [*const rocksdb_column_family_handle_t; 3] = [handles[0], handles[1], handles[1]];
            let keysm = [cstr("box"), cstr("box"), cstr("barfooxx")];
            let keys: Vec<*const c_char> = keysm.iter().map(|s| s.as_ptr()).collect();
            let keys_sizes: [size_t; 3] = [3, 3, 8];
            let mut vals: [*mut c_char; 3] = [ptr::null_mut(), ptr::null_mut(), ptr::null_mut()];
            let mut vals_sizes: [size_t; 3] = [0, 0, 0];
            let mut errs: [*mut c_char; 3] = [ptr::null_mut(), ptr::null_mut(), ptr::null_mut()];
            rocksdb_multi_get_cf(db, roptions, get_handles.as_ptr(),
                3, keys.as_ptr(), keys_sizes.as_ptr(),
                vals.as_mut_ptr(), vals_sizes.as_mut_ptr(),
                errs.as_mut_ptr());

            for i in 0..3 {
                CheckEqual(ptr::null(), errs[i], 0);
                match i {
                    0 => CheckEqual(ptr::null(), vals[i], vals_sizes[i]), // wrong cf
                    1 => {
                        let v = cstr("c");
                        CheckEqual(v.as_ptr(), vals[i], vals_sizes[i])
                    } // bingo
                    2 => CheckEqual(ptr::null(), vals[i], vals_sizes[i]), // normal not found
                    _ => {}
                }
                Free(&mut vals[i]);
            }

            let mut iter = rocksdb_create_iterator_cf(db, roptions, handles[1]);
            CheckCondition!(rocksdb_iter_valid(iter) == 0);
            rocksdb_iter_seek_to_first(iter);
            CheckCondition!(rocksdb_iter_valid(iter) != 0);

            let mut i: u32 = 0;
            while rocksdb_iter_valid(iter) != 0 {
                rocksdb_iter_next(iter);
                i += 1;
            }
            CheckCondition!(i == 1);
            rocksdb_iter_get_error(iter, &mut err);
            CheckNoError!(err);
            rocksdb_iter_destroy(iter);

            rocksdb_drop_column_family(db, handles[1], &mut err);
            CheckNoError!(err);
            for i in 0..2 {
                rocksdb_column_family_handle_destroy(handles[i]);
            }
            rocksdb_close(db);
            rocksdb_destroy_db(options, dbname, &mut err);
            rocksdb_options_destroy(db_options);
            rocksdb_options_destroy(cf_options);
        }

        StartPhase("prefix");
        {
            // Create new database
            rocksdb_options_set_allow_mmap_reads(options, 1);
            rocksdb_options_set_prefix_extractor(options, rocksdb_slicetransform_create_fixed_prefix(3));
            rocksdb_options_set_hash_skip_list_rep(options, 5000, 4, 4);
            rocksdb_options_set_plain_table_factory(options, 4, 10, 0.75, 16);

            db = rocksdb_open(options, dbname, &mut err);
            CheckNoError!(err);

            {
                let k = cstr("foo1"); let v = cstr("foo");
                rocksdb_put(db, woptions, k.as_ptr(), 4, v.as_ptr(), 3, &mut err);
            }
            CheckNoError!(err);
            {
                let k = cstr("foo2"); let v = cstr("foo");
                rocksdb_put(db, woptions, k.as_ptr(), 4, v.as_ptr(), 3, &mut err);
            }
            CheckNoError!(err);
            {
                let k = cstr("foo3"); let v = cstr("foo");
                rocksdb_put(db, woptions, k.as_ptr(), 4, v.as_ptr(), 3, &mut err);
            }
            CheckNoError!(err);
            {
                let k = cstr("bar1"); let v = cstr("bar");
                rocksdb_put(db, woptions, k.as_ptr(), 4, v.as_ptr(), 3, &mut err);
            }
            CheckNoError!(err);
            {
                let k = cstr("bar2"); let v = cstr("bar");
                rocksdb_put(db, woptions, k.as_ptr(), 4, v.as_ptr(), 3, &mut err);
            }
            CheckNoError!(err);
            {
                let k = cstr("bar3"); let v = cstr("bar");
                rocksdb_put(db, woptions, k.as_ptr(), 4, v.as_ptr(), 3, &mut err);
            }
            CheckNoError!(err);

            let mut iter = rocksdb_create_iterator(db, roptions);
            CheckCondition!(rocksdb_iter_valid(iter) == 0);

            {
                let k = cstr("bar");
                rocksdb_iter_seek(iter, k.as_ptr(), 3);
            }
            rocksdb_iter_get_error(iter, &mut err);
            CheckNoError!(err);
            CheckCondition!(rocksdb_iter_valid(iter) != 0);

            CheckIter(iter, "bar1", "bar");
            rocksdb_iter_next(iter);
            CheckIter(iter, "bar2", "bar");
            rocksdb_iter_next(iter);
            CheckIter(iter, "bar3", "bar");
            rocksdb_iter_get_error(iter, &mut err);
            CheckNoError!(err);
            rocksdb_iter_destroy(iter);

            rocksdb_close(db);
            rocksdb_destroy_db(options, dbname, &mut err);
        }

        StartPhase("cuckoo_options");
        {
            let mut cuckoo_options = rocksdb_cuckoo_options_create();
            rocksdb_cuckoo_options_set_hash_ratio(cuckoo_options, 0.5);
            rocksdb_cuckoo_options_set_max_search_depth(cuckoo_options, 200);
            rocksdb_cuckoo_options_set_cuckoo_block_size(cuckoo_options, 10);
            rocksdb_cuckoo_options_set_identity_as_first_hash(cuckoo_options, 1);
            rocksdb_cuckoo_options_set_use_module_hash(cuckoo_options, 0);
            rocksdb_options_set_cuckoo_table_factory(options, cuckoo_options);

            db = rocksdb_open(options, dbname, &mut err);
            CheckNoError!(err);

            rocksdb_cuckoo_options_destroy(cuckoo_options);
        }

        StartPhase("iterate_upper_bound");
        {
            // Create new empty database
            rocksdb_close(db);
            rocksdb_destroy_db(options, dbname, &mut err);
            CheckNoError!(err);

            rocksdb_options_set_prefix_extractor(options, ptr::null_mut());
            db = rocksdb_open(options, dbname, &mut err);
            CheckNoError!(err);

            {
                let k = cstr("a"); let v = cstr("0");
                rocksdb_put(db, woptions, k.as_ptr(), 1, v.as_ptr(), 1, &mut err);
                CheckNoError!(err);
            }
            {
                let k = cstr("foo"); let v = cstr("bar");
                rocksdb_put(db, woptions, k.as_ptr(), 3, v.as_ptr(), 3, &mut err);
                CheckNoError!(err);
            }
            {
                let k = cstr("foo1"); let v = cstr("bar1");
                rocksdb_put(db, woptions, k.as_ptr(), 4, v.as_ptr(), 4, &mut err);
                CheckNoError!(err);
            }
            {
                let k = cstr("g1"); let v = cstr("0");
                rocksdb_put(db, woptions, k.as_ptr(), 2, v.as_ptr(), 1, &mut err);
                CheckNoError!(err);
            }

            // testing basic case with no iterate_upper_bound and no prefix_extractor
            {
                rocksdb_readoptions_set_iterate_upper_bound(roptions, ptr::null(), 0);
                let mut iter = rocksdb_create_iterator(db, roptions);

                {
                    let k = cstr("foo");
                    rocksdb_iter_seek(iter, k.as_ptr(), 3);
                }
                CheckCondition!(rocksdb_iter_valid(iter) != 0);
                CheckIter(iter, "foo", "bar");

                rocksdb_iter_next(iter);
                CheckCondition!(rocksdb_iter_valid(iter) != 0);
                CheckIter(iter, "foo1", "bar1");

                rocksdb_iter_next(iter);
                CheckCondition!(rocksdb_iter_valid(iter) != 0);
                CheckIter(iter, "g1", "0");

                rocksdb_iter_destroy(iter);
            }

            // testing iterate_upper_bound and forward iterator
            // to make sure it stops at bound
            {
                // iterate_upper_bound points beyond the last expected entry
                let upper_bound = cstr("foo2");
                rocksdb_readoptions_set_iterate_upper_bound(roptions, upper_bound.as_ptr(), 4);

                let mut iter = rocksdb_create_iterator(db, roptions);

                {
                    let k = cstr("foo");
                    rocksdb_iter_seek(iter, k.as_ptr(), 3);
                }
                CheckCondition!(rocksdb_iter_valid(iter) != 0);
                CheckIter(iter, "foo", "bar");

                rocksdb_iter_next(iter);
                CheckCondition!(rocksdb_iter_valid(iter) != 0);
                CheckIter(iter, "foo1", "bar1");

                rocksdb_iter_next(iter);
                // should stop here...
                CheckCondition!(rocksdb_iter_valid(iter) == 0);

                rocksdb_iter_destroy(iter);
            }
        }

        StartPhase("cleanup");
        rocksdb_close(db);
        rocksdb_options_destroy(options);
        rocksdb_block_based_options_destroy(table_options);
        rocksdb_readoptions_destroy(roptions);
        rocksdb_writeoptions_destroy(woptions);
        rocksdb_cache_destroy(cache);
        rocksdb_comparator_destroy(cmp);
        rocksdb_env_destroy(env);
        println!("PASS!");

        cstrd(dbname);
        cstrd(dbbackupname);
    }
}
