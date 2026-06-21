//! Heap-profile unpickling a single pickle file with dhat.
//!
//! Reports the resident `Value` tree size and, via dhat, the transient peak
//! during the unpickle (where the internal de-tree, the memo, and the converted
//! public tree coexist). The process is single-threaded and quiescent at exit,
//! so `Profiler::drop` converges and writes dhat-heap.json (view at
//! <https://nnethercote.github.io/dh_view/dh_view.html>, sort by "at t-gmax").
//!
//! Usage: cargo run --release --features dhat-heap --example dhat_unpickle -- <pickle-file>
//!
//! Options mirror `mem_profile`: --replace-globals --replace-recursive --decode-strings

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::env;
use std::fs;

use pickled::DeOptions;
use pickled::Value;

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn main() {
    let mut path = None;
    let mut opts = DeOptions::new();
    for arg in env::args().skip(1) {
        match arg.as_str() {
            "--replace-globals" => opts = opts.replace_unresolved_globals(),
            "--replace-recursive" => opts = opts.replace_recursive_structures(),
            "--decode-strings" => opts = opts.decode_strings(),
            s if s.starts_with('-') => {
                eprintln!("unknown option: {s}");
                std::process::exit(1);
            }
            s => path = Some(s.to_owned()),
        }
    }
    let path = path.unwrap_or_else(|| {
        eprintln!("usage: dhat_unpickle [--replace-globals] [--replace-recursive] [--decode-strings] <pickle-file>");
        std::process::exit(1);
    });

    let profiler = dhat::Profiler::builder().trim_backtraces(Some(32)).build();

    let data = fs::read(&path).expect("read pickle file");
    eprintln!("read {:.1} MiB", mib(data.len()));
    let after_read = dhat::HeapStats::get();

    let value: Value = pickled::value_from_slice(&data, opts).expect("unpickle");
    drop(data);
    let after = dhat::HeapStats::get();
    eprintln!(
        "value tree resident: {:.1} MiB  |  transient peak during unpickle: {:.1} MiB",
        mib(after.curr_bytes.saturating_sub(after_read.curr_bytes)),
        mib(after.max_bytes),
    );

    std::hint::black_box(&value);
    drop(profiler);
}
