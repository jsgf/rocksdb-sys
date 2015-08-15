extern crate pkg_config;

use std::process::Command;
use std::env;
use std::path::Path;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();

    env::set_current_dir(Path::new("rocksdb")).unwrap();

    Command::new("make")
        .arg("EXTRA_CFLAGS=-fPIC")
        .arg("EXTRA_CXXFLAGS=-fPIC")
        .arg(format!("INSTALL_PATH={}", out_dir))
        .arg("install-static")
        .output().unwrap();

    println!("cargo:rustc-link-search=native={}/lib", out_dir);
    println!("cargo:rustc-link-lib=static=rocksdb");
    println!("cargo:rustc-link-lib=stdc++");

    if pkg_config::find_library("liblz4").is_ok() {
        println!("cargo:rustc-link-lib=lz4");
    }
    println!("cargo:rustc-link-lib=snappy");
    if pkg_config::find_library("zlib").is_ok() {
        println!("cargo:rustc-link-lib=z");
    }
}
