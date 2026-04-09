//! Memory profiler for pickle files.
//!
//! Reads a raw pickle file, unpickles it to a Value tree, then walks the tree
//! reporting node counts, Rc sharing rates, content duplication, container
//! sizes, and estimated memory breakdown.
//!
//! Usage: cargo run --release --example mem_profile -- <pickle-file>
//!
//! Options:
//!   --replace-globals        Replace unresolved globals with None
//!   --replace-recursive      Replace recursive structures with None
//!   --replace-reconstructor  Treat _reconstructor objects as dicts

use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::mem;
use std::process;
use std::rc::Rc;
use std::time::Instant;

use pickled::{self, DeOptions, HashableValue, PickleObject, Value};

// ---------------------------------------------------------------------------
// RSS measurement
// ---------------------------------------------------------------------------

#[cfg(windows)]
fn current_rss_bytes() -> usize {
    use std::mem::MaybeUninit;

    #[repr(C)]
    #[allow(non_snake_case)]
    struct PROCESS_MEMORY_COUNTERS {
        cb: u32,
        PageFaultCount: u32,
        PeakWorkingSetSize: usize,
        WorkingSetSize: usize,
        QuotaPeakPagedPoolUsage: usize,
        QuotaPagedPoolUsage: usize,
        QuotaPeakNonPagedPoolUsage: usize,
        QuotaNonPagedPoolUsage: usize,
        PagefileUsage: usize,
        PeakPagefileUsage: usize,
    }

    unsafe extern "system" {
        fn GetCurrentProcess() -> *mut std::ffi::c_void;
        fn K32GetProcessMemoryInfo(
            process: *mut std::ffi::c_void,
            pmc: *mut PROCESS_MEMORY_COUNTERS,
            cb: u32,
        ) -> i32;
    }

    unsafe {
        let mut pmc = MaybeUninit::<PROCESS_MEMORY_COUNTERS>::zeroed();
        let size = mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
        (*pmc.as_mut_ptr()).cb = size;
        let ok = K32GetProcessMemoryInfo(GetCurrentProcess(), pmc.as_mut_ptr(), size);
        if ok != 0 {
            (*pmc.as_ptr()).WorkingSetSize
        } else {
            0
        }
    }
}

#[cfg(not(windows))]
fn current_rss_bytes() -> usize {
    if let Ok(statm) = fs::read_to_string("/proc/self/statm") {
        let rss_pages: usize = statm
            .split_whitespace()
            .nth(1)
            .unwrap_or("0")
            .parse()
            .unwrap_or(0);
        rss_pages * 4096
    } else {
        0
    }
}

fn mb(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn rss_mb() -> f64 {
    mb(current_rss_bytes())
}

// ---------------------------------------------------------------------------
// Value tree walker
// ---------------------------------------------------------------------------

#[derive(Default)]
struct ValueStats {
    // Node counts by type
    count_none: u64,
    count_bool: u64,
    count_i64: u64,
    count_int: u64,
    count_f64: u64,
    count_bytes: u64,
    count_string: u64,
    count_list: u64,
    count_tuple: u64,
    count_set: u64,
    count_frozenset: u64,
    count_dict: u64,
    count_object: u64,

    // Payload sizes
    bytes_bytes: usize,
    bytes_strings: usize,
    bytes_bigint: usize,

    // Rc sharing tracking
    seen_string_ptrs: HashSet<usize>,
    seen_bytes_ptrs: HashSet<usize>,
    seen_tuple_ptrs: HashSet<usize>,
    seen_list_ptrs: HashSet<usize>,
    seen_dict_ptrs: HashSet<usize>,
    seen_object_ptrs: HashSet<usize>,

    unique_strings: u64,
    shared_strings: u64,
    unique_bytes: u64,
    shared_bytes: u64,
    unique_tuples: u64,
    shared_tuples: u64,
    unique_lists: u64,
    shared_lists: u64,
    unique_dicts: u64,
    shared_dicts: u64,
    unique_objects: u64,
    shared_objects: u64,

    // Content dedup analysis
    string_content_map: HashMap<String, u64>,
    bytes_content_map: HashMap<Vec<u8>, u64>,

    // DictObject analysis
    object_state_entries_total: u64,
    object_module_class_bytes: usize,
    object_class_names: HashMap<String, u64>,

    // Container element counts (unique containers only)
    total_list_elements: u64,
    total_tuple_elements: u64,
    total_dict_entries: u64,
}

impl ValueStats {
    fn walk(&mut self, val: &Value) {
        match val {
            Value::None => self.count_none += 1,
            Value::Bool(_) => self.count_bool += 1,
            Value::I64(_) => self.count_i64 += 1,
            Value::Int(n) => {
                self.count_int += 1;
                let (_, digits) = n.to_u32_digits();
                self.bytes_bigint += digits.len() * 4 + 24;
            }
            Value::F64(_) => self.count_f64 += 1,
            Value::Bytes(b) => {
                self.count_bytes += 1;
                let ptr = Rc::as_ptr(b.rc_ref()) as usize;
                if self.seen_bytes_ptrs.insert(ptr) {
                    self.unique_bytes += 1;
                    self.bytes_bytes += b.inner().len();
                } else {
                    self.shared_bytes += 1;
                }
                *self.bytes_content_map.entry(b.inner().clone()).or_insert(0) += 1;
            }
            Value::String(s) => {
                self.count_string += 1;
                let ptr = Rc::as_ptr(s.rc_ref()) as usize;
                if self.seen_string_ptrs.insert(ptr) {
                    self.unique_strings += 1;
                    self.bytes_strings += s.inner().len();
                } else {
                    self.shared_strings += 1;
                }
                *self.string_content_map.entry(s.inner().clone()).or_insert(0) += 1;
            }
            Value::List(l) => {
                self.count_list += 1;
                let ptr = l.rc_ptr() as usize;
                if self.seen_list_ptrs.insert(ptr) {
                    self.unique_lists += 1;
                    self.total_list_elements += l.inner().len() as u64;
                    for item in l.inner().iter() {
                        self.walk(item);
                    }
                } else {
                    self.shared_lists += 1;
                }
            }
            Value::Tuple(t) => {
                self.count_tuple += 1;
                let ptr = Rc::as_ptr(t.rc_ref()) as usize;
                if self.seen_tuple_ptrs.insert(ptr) {
                    self.unique_tuples += 1;
                    self.total_tuple_elements += t.inner().len() as u64;
                    for item in t.inner().iter() {
                        self.walk(item);
                    }
                } else {
                    self.shared_tuples += 1;
                }
            }
            Value::Set(s) => {
                self.count_set += 1;
                for item in s.inner().iter() {
                    self.walk_hashable(item);
                }
            }
            Value::FrozenSet(s) => {
                self.count_frozenset += 1;
                for item in s.inner().iter() {
                    self.walk_hashable(item);
                }
            }
            Value::Dict(d) => {
                self.count_dict += 1;
                let ptr = d.rc_ptr() as usize;
                if self.seen_dict_ptrs.insert(ptr) {
                    self.unique_dicts += 1;
                    self.total_dict_entries += d.inner().len() as u64;
                    for (k, v) in d.inner().iter() {
                        self.walk_hashable(k);
                        self.walk(v);
                    }
                } else {
                    self.shared_dicts += 1;
                }
            }
            Value::Object(o) => {
                self.count_object += 1;
                let ptr = o.rc_ptr() as usize;
                if self.seen_object_ptrs.insert(ptr) {
                    self.unique_objects += 1;
                    self.walk_object(o.inner().as_ref());
                } else {
                    self.shared_objects += 1;
                }
            }
        }
    }

    fn walk_object(&mut self, obj: &dyn PickleObject) {
        let (module, class) = obj.class_info();
        let key = format!("{}.{}", module, class);
        *self.object_class_names.entry(key).or_insert(0) += 1;
        self.object_module_class_bytes += module.len() + class.len() + 48;

        use pickled::object::DictObject;
        if let Some(dict_obj) = obj.as_any().downcast_ref::<DictObject>() {
            self.object_state_entries_total += dict_obj.state().len() as u64;
            for (k, v) in dict_obj.state().iter() {
                self.walk_hashable(k);
                self.walk(v);
            }
        }
    }

    fn walk_hashable(&mut self, val: &HashableValue) {
        match val {
            HashableValue::None => self.count_none += 1,
            HashableValue::Bool(_) => self.count_bool += 1,
            HashableValue::I64(_) => self.count_i64 += 1,
            HashableValue::Int(n) => {
                self.count_int += 1;
                let (_, digits) = n.to_u32_digits();
                self.bytes_bigint += digits.len() * 4 + 24;
            }
            HashableValue::F64(_) => self.count_f64 += 1,
            HashableValue::Bytes(b) => {
                self.count_bytes += 1;
                let ptr = Rc::as_ptr(b.rc_ref()) as usize;
                if self.seen_bytes_ptrs.insert(ptr) {
                    self.unique_bytes += 1;
                    self.bytes_bytes += b.inner().len();
                } else {
                    self.shared_bytes += 1;
                }
            }
            HashableValue::String(s) => {
                self.count_string += 1;
                let ptr = Rc::as_ptr(s.rc_ref()) as usize;
                if self.seen_string_ptrs.insert(ptr) {
                    self.unique_strings += 1;
                    self.bytes_strings += s.inner().len();
                } else {
                    self.shared_strings += 1;
                }
                *self.string_content_map.entry(s.inner().clone()).or_insert(0) += 1;
            }
            HashableValue::Tuple(t) => {
                self.count_tuple += 1;
                let ptr = Rc::as_ptr(t.rc_ref()) as usize;
                if self.seen_tuple_ptrs.insert(ptr) {
                    self.unique_tuples += 1;
                    for item in t.inner().iter() {
                        self.walk_hashable(item);
                    }
                } else {
                    self.shared_tuples += 1;
                }
            }
            HashableValue::FrozenSet(s) => {
                self.count_frozenset += 1;
                for item in s.inner().iter() {
                    self.walk_hashable(item);
                }
            }
        }
    }

    fn total_nodes(&self) -> u64 {
        self.count_none
            + self.count_bool
            + self.count_i64
            + self.count_int
            + self.count_f64
            + self.count_bytes
            + self.count_string
            + self.count_list
            + self.count_tuple
            + self.count_set
            + self.count_frozenset
            + self.count_dict
            + self.count_object
    }

    fn report(&self) {
        let total = self.total_nodes();

        println!("\n=== Value Tree Statistics ===");
        println!("Node counts:");
        println!("  None:      {:>10}", self.count_none);
        println!("  Bool:      {:>10}", self.count_bool);
        println!("  I64:       {:>10}", self.count_i64);
        println!("  Int:       {:>10}", self.count_int);
        println!("  F64:       {:>10}", self.count_f64);
        println!("  Bytes:     {:>10}", self.count_bytes);
        println!("  String:    {:>10}", self.count_string);
        println!("  List:      {:>10}", self.count_list);
        println!("  Tuple:     {:>10}", self.count_tuple);
        println!("  Set:       {:>10}", self.count_set);
        println!("  FrozenSet: {:>10}", self.count_frozenset);
        println!("  Dict:      {:>10}", self.count_dict);
        println!("  Object:    {:>10}", self.count_object);
        println!("  TOTAL:     {:>10}", total);

        println!("\nPayload sizes:");
        println!("  Bytes payloads:  {:>10.2} MB", mb(self.bytes_bytes));
        println!("  String payloads: {:>10.2} MB", mb(self.bytes_strings));
        println!("  BigInt est:      {:>10.2} MB", mb(self.bytes_bigint));

        self.report_sharing();
        self.report_content_duplication();
        if self.count_object > 0 {
            self.report_objects();
        }
        self.report_containers();
        self.report_memory_estimates(total);
    }

    fn report_sharing(&self) {
        println!("\nRc sharing (unique / shared references):");
        let items: &[(&str, u64, u64, u64)] = &[
            ("Strings", self.unique_strings, self.shared_strings, self.count_string),
            ("Bytes", self.unique_bytes, self.shared_bytes, self.count_bytes),
            ("Tuples", self.unique_tuples, self.shared_tuples, self.count_tuple),
            ("Lists", self.unique_lists, self.shared_lists, self.count_list),
            ("Dicts", self.unique_dicts, self.shared_dicts, self.count_dict),
            ("Objects", self.unique_objects, self.shared_objects, self.count_object),
        ];
        for (name, unique, shared, count) in items {
            if *count > 0 {
                println!(
                    "  {:<8} {} unique, {} shared ({:.1}% sharing)",
                    format!("{name}:"),
                    unique,
                    shared,
                    *shared as f64 / *count as f64 * 100.0
                );
            }
        }
    }

    fn report_content_duplication(&self) {
        for (label, content_map) in [
            ("String", &self.string_content_map),
            // Bytes are handled separately below for display
        ] {
            let total_refs: u64 = content_map.values().sum();
            if total_refs == 0 {
                continue;
            }
            let with_dupes: Vec<_> = content_map.iter().filter(|&(_, c)| *c > 1).collect();
            let duped_refs: u64 = with_dupes.iter().map(|(_, c)| **c).sum();
            let wasted: usize = with_dupes.iter().map(|(s, c)| s.len() * (**c as usize - 1)).sum();
            println!("\n{label} content-level duplication:");
            println!("  Total refs:            {}", total_refs);
            println!("  Unique content:        {}", content_map.len());
            println!("  Contents appearing >1: {} (covering {} refs)", with_dupes.len(), duped_refs);
            println!("  Wasted if not Rc-shared: {:.2} MB", mb(wasted));

            let mut top: Vec<_> = with_dupes;
            top.sort_by(|a, b| b.1.cmp(a.1));
            if !top.is_empty() {
                println!("  Top 20 most duplicated:");
                for (s, count) in top.iter().take(20) {
                    let display = if s.len() > 60 { format!("{}...", &s[..60]) } else { s.to_string() };
                    println!("    {:>8}x  {:>6}B  {:?}", count, s.len(), display);
                }
            }
        }

        // Bytes content
        let total_refs: u64 = self.bytes_content_map.values().sum();
        if total_refs > 0 {
            let with_dupes: Vec<_> = self.bytes_content_map.iter().filter(|&(_, c)| *c > 1).collect();
            let duped_refs: u64 = with_dupes.iter().map(|(_, c)| **c).sum();
            let wasted: usize = with_dupes.iter().map(|(b, c)| b.len() * (**c as usize - 1)).sum();
            println!("\nBytes content-level duplication:");
            println!("  Total refs:            {}", total_refs);
            println!("  Unique content:        {}", self.bytes_content_map.len());
            println!("  Contents appearing >1: {} (covering {} refs)", with_dupes.len(), duped_refs);
            println!("  Wasted if not Rc-shared: {:.2} MB", mb(wasted));

            let mut top: Vec<_> = with_dupes;
            top.sort_by(|a, b| b.1.cmp(a.1));
            if !top.is_empty() {
                println!("  Top 20 most duplicated:");
                for (b, count) in top.iter().take(20) {
                    let display = String::from_utf8_lossy(if b.len() > 60 { &b[..60] } else { b });
                    println!("    {:>8}x  {:>6}B  {:?}", count, b.len(), display);
                }
            }
        }
    }

    fn report_objects(&self) {
        println!("\n=== Object Analysis ===");
        println!("  Total objects:         {}", self.count_object);
        println!("  Unique (by Rc):        {}", self.unique_objects);
        println!("  Shared refs:           {}", self.shared_objects);
        println!("  Total state entries:   {}", self.object_state_entries_total);
        if self.unique_objects > 0 {
            println!(
                "  Avg entries/object:    {:.1}",
                self.object_state_entries_total as f64 / self.unique_objects as f64
            );
        }
        println!(
            "  Module+class strings:  {:.2} MB",
            mb(self.object_module_class_bytes)
        );

        if !self.object_class_names.is_empty() {
            let mut class_counts: Vec<_> = self.object_class_names.iter().collect();
            class_counts.sort_by(|a, b| b.1.cmp(a.1));
            println!("  Top classes:");
            for (name, count) in class_counts.iter().take(20) {
                println!("    {:>8}x  {}", count, name);
            }
        }
    }

    fn report_containers(&self) {
        println!("\n=== Container Elements (unique containers only) ===");
        println!("  List elements:   {:>10}", self.total_list_elements);
        println!("  Tuple elements:  {:>10}", self.total_tuple_elements);
        println!("  Dict entries:    {:>10}", self.total_dict_entries);
    }

    fn report_memory_estimates(&self, total: u64) {
        let value_size = mem::size_of::<Value>();
        let hashable_size = mem::size_of::<HashableValue>();
        let btree_entry_overhead = 64usize;

        println!("\n=== Memory Estimates ===");
        println!("  sizeof(Value) = {} bytes", value_size);
        println!("  sizeof(HashableValue) = {} bytes", hashable_size);

        let node_overhead = value_size * total as usize;
        println!(
            "  Value enum nodes:      {:.2} MB  ({} nodes * {} B)",
            mb(node_overhead), total, value_size
        );

        let list_backing = self.total_list_elements as usize * value_size;
        println!(
            "  List Vec backing:      {:.2} MB  ({} elements)",
            mb(list_backing), self.total_list_elements
        );

        let tuple_backing = self.total_tuple_elements as usize * value_size;
        println!(
            "  Tuple Vec backing:     {:.2} MB  ({} elements)",
            mb(tuple_backing), self.total_tuple_elements
        );

        let dict_kv_size = self.total_dict_entries as usize * (hashable_size + value_size);
        let dict_btree = self.total_dict_entries as usize * btree_entry_overhead;
        println!(
            "  Dict storage:          {:.2} MB  ({} entries, kv + BTree overhead)",
            mb(dict_kv_size + dict_btree), self.total_dict_entries
        );

        let obj_struct_size = mem::size_of::<Box<dyn PickleObject>>()
            + mem::size_of::<pickled::object::DictObject>();
        let obj_box_overhead = self.unique_objects as usize * obj_struct_size;
        let obj_btree = self.object_state_entries_total as usize * btree_entry_overhead;
        let obj_kv = self.object_state_entries_total as usize * (hashable_size + value_size);
        println!(
            "  Object structs:        {:.2} MB  ({} unique objects)",
            mb(obj_box_overhead), self.unique_objects
        );
        println!(
            "  Object state storage:  {:.2} MB  ({} entries, kv + BTree overhead)",
            mb(obj_kv + obj_btree), self.object_state_entries_total
        );

        let rc_overhead = 24usize;
        let shared_frozen_overhead = 16usize;
        let rc_total = self.unique_lists as usize * rc_overhead
            + self.unique_dicts as usize * rc_overhead
            + self.unique_objects as usize * rc_overhead
            + self.unique_tuples as usize * shared_frozen_overhead
            + self.unique_strings as usize * shared_frozen_overhead
            + self.unique_bytes as usize * shared_frozen_overhead;
        println!("  Rc wrapper overhead:   {:.2} MB", mb(rc_total));

        let est_total = node_overhead
            + list_backing
            + tuple_backing
            + dict_kv_size
            + dict_btree
            + obj_box_overhead
            + obj_btree
            + obj_kv
            + rc_total
            + self.bytes_bytes
            + self.bytes_strings
            + self.bytes_bigint
            + self.object_module_class_bytes;
        println!(
            "\n  ESTIMATED TOTAL:       {:.2} MB",
            mb(est_total)
        );
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let args: Vec<String> = env::args().collect();

    let mut path = None;
    let mut replace_globals = false;
    let mut replace_recursive = false;
    let mut replace_reconstructor = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--replace-globals" => replace_globals = true,
            "--replace-recursive" => replace_recursive = true,
            "--replace-reconstructor" => replace_reconstructor = true,
            s if s.starts_with('-') => {
                eprintln!("Unknown option: {s}");
                process::exit(1);
            }
            s => path = Some(s.to_owned()),
        }
        i += 1;
    }

    let path = match path {
        Some(p) => p,
        None => {
            eprintln!("Usage: mem_profile [options] <pickle-file>");
            eprintln!();
            eprintln!("Options:");
            eprintln!("  --replace-globals        Replace unresolved globals with None");
            eprintln!("  --replace-recursive      Replace recursive structures with None");
            eprintln!("  --replace-reconstructor  Treat _reconstructor objects as dicts");
            process::exit(1);
        }
    };

    let rss_start = rss_mb();
    println!("RSS at start: {:.1} MB", rss_start);

    // Read file
    let t0 = Instant::now();
    let data = fs::read(&path).expect("failed to read file");
    let rss_after_read = rss_mb();
    println!(
        "Read {} bytes ({:.1} MB) in {:.1}ms  |  RSS: {:.1} MB (+{:.1})",
        data.len(),
        mb(data.len()),
        t0.elapsed().as_secs_f64() * 1000.0,
        rss_after_read,
        rss_after_read - rss_start
    );

    // Unpickle
    let t1 = Instant::now();
    let mut opts = DeOptions::new();
    if replace_globals {
        opts = opts.replace_unresolved_globals();
    }
    if replace_recursive {
        opts = opts.replace_recursive_structures();
    }
    if replace_reconstructor {
        opts = opts.replace_reconstructor_objects_structures();
    }
    let value: Value = pickled::value_from_slice(&data, opts).expect("failed to unpickle");
    let unpickle_ms = t1.elapsed().as_secs_f64() * 1000.0;
    let rss_after_unpickle = rss_mb();
    println!(
        "Unpickled in {:.1}ms  |  RSS: {:.1} MB (+{:.1})",
        unpickle_ms,
        rss_after_unpickle,
        rss_after_unpickle - rss_after_read
    );

    // Drop input buffer
    drop(data);
    let rss_value_only = rss_mb();
    println!(
        "After dropping input buf  |  RSS: {:.1} MB (value tree: ~{:.1} MB)",
        rss_value_only,
        rss_value_only - rss_start
    );

    // Walk the value tree
    println!("\nWalking value tree...");
    let t2 = Instant::now();
    let mut stats = ValueStats::default();
    stats.walk(&value);
    println!(
        "Walk completed in {:.1}ms",
        t2.elapsed().as_secs_f64() * 1000.0
    );

    stats.report();

    // Drop value tree
    println!("\nDropping value tree...");
    let t3 = Instant::now();
    drop(value);
    let rss_after_drop = rss_mb();
    println!(
        "Dropped in {:.1}ms  |  RSS: {:.1} MB (freed ~{:.1} MB)",
        t3.elapsed().as_secs_f64() * 1000.0,
        rss_after_drop,
        rss_value_only - rss_after_drop
    );
}
