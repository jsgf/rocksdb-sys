use std::process::{Command, exit};
use std::env;
use std::path::Path;
use std::fs::File;
use std::io::{BufRead, BufReader, Write, stderr};

fn main() {
    let mut stderr = stderr();
    let out_dir = env::var("OUT_DIR").unwrap();
    let num_jobs = env::var("NUM_JOBS");

    let mut cmd = Command::new("make");

    cmd.current_dir(Path::new("rocksdb"))
        .arg("EXTRA_CFLAGS=-fPIC")
        .arg("EXTRA_CXXFLAGS=-fPIC")
        .arg(format!("INSTALL_PATH={}", out_dir));

    if let Ok(jobs) = num_jobs {
        cmd.arg(format!("-j{}", jobs));
    }

    cmd.arg("install");

    match cmd.output() {
        Ok(out) => if !out.status.success() {
            let _ = writeln!(&mut stderr, "build failed:\nstdout:\n{}\nstderr:\n{}", String::from_utf8(out.stdout).unwrap(), String::from_utf8(out.stderr).unwrap());
            exit(1);
        },
        Err(e) => { let _ = writeln!(&mut stderr, "command execution failed: {:?}", e); exit(1) }
    }

    let config = match File::open("rocksdb/make_config.mk") {
        Ok(c) => c,
        Err(e) => { let _ = writeln!(&mut stderr, "Failed to open `rocksdb/make_config.mk`: {}", e); exit(1) }
    };
    let config = BufReader::new(config);

    let mut lz4 = false;
    let mut snappy = false;
    let mut zlib = false;
    let mut bzip2 = false;

    for line in config.lines() {
        let line = line.unwrap();
        let words: Vec<_> = line.split_whitespace().collect();

        if !words[0].starts_with("PLATFORM_LDFLAGS=") {
            continue;
        }

        lz4 = words.iter().any(|w| *w == "-llz4");
        snappy = words.iter().any( |w| *w == "-lsnappy");
        zlib = words.iter().any(|w| *w == "-lz");
        bzip2 = words.iter().any(|w| *w == "-lbz2");
        break;
    }

    println!("cargo:rustc-link-search=native={}/lib", out_dir);
    println!("cargo:rustc-link-lib=rocksdb");
    if lz4 { println!("cargo:rustc-link-lib=lz4"); }
    if snappy { println!("cargo:rustc-link-lib=snappy"); }
    if zlib { println!("cargo:rustc-link-lib=z"); }
    if bzip2 { println!("cargo:rustc-link-lib=bz2"); }
    println!("cargo:rustc-link-lib=stdc++");
}
