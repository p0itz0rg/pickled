Serde Pickle Serialization Library
==================================

This is a fork of https://crates.io/crates/serde-pickle with the following notable differences:

1. Support for recursive data structures.
2. Support for refcounted data objects.
3. Support for deserializing custom types either as real Rust struct definitions or a dictionary.
4. Easier management of unwrapping value types.
5. Lower memory overhead, reducing extremely large pickle objects from ~20 GiB peak memory to ~800MiB (tested with an extremely complex object from a video game).
6. Support for wider string encodings (via the `encoding` feature).

[![Latest Version](https://img.shields.io/crates/v/pickled.svg)](https://crates.io/crates/pickled)

[Documentation](https://docs.rs/pickled)

This crate is a Rust library for parsing and generating Python pickle
streams. It is built upon [Serde](https://github.com/serde-rs/serde), a high
performance generic serialization framework.

Installation
============

To add this crate to your project, along with serde, simply run:

```
cargo add pickled serde
```

Usage
=====

As with other serde serialization implementations, this library provides
toplevel functions for simple en/decoding of supported objects.

Example:

```rust
use std::collections::BTreeMap;

fn main() {
    let mut map = BTreeMap::new();
    map.insert("x".to_string(), 1.0);
    map.insert("y".to_string(), 2.0);

    // Serialize the map into a pickle stream.
    // The second argument are serialization options.
    let serialized = pickled::to_vec(&map, Default::default()).unwrap();

    // Deserialize the pickle stream back into a map.
    // Because we compare it to the original `map` below, Rust infers
    // the type of `deserialized` and lets serde work its magic.
    // The second argument are additional deserialization options.
    let deserialized = pickled::from_slice(&serialized, Default::default()).unwrap();
    assert_eq!(map, deserialized);
}
```

Serializing and deserializing structs and enums that implement the
serde-provided traits is supported, and works analogous to other crates
(using `serde_derive`).

What's new in this fork
=======================

The examples below use `value_from_slice`, which decodes into this crate's
`Value` enum (preserving Python types serde's data model can't express, like
big integers and sets). `DeOptions` configures how decoding behaves.

Ergonomic value access
----------------------

Every `Value` (and `HashableValue`) variant gets generated accessor methods,
specifically to avoid `match` soup. Each variant `foo` gives you
`is_foo()`, `foo()` / `foo_ref()` / `foo_mut()` (returning an `Option`),
`unwrap_foo()`, `foo_or(err)`, and more.

```rust
use pickled::{value_from_slice, DeOptions, Value};

let value: Value = value_from_slice(&data, DeOptions::new()).unwrap();

// list_ref() -> Option<&Shared<Vec<Value>>>; inner() borrows the contents.
if let Some(items) = value.list_ref() {
    for item in items.inner().iter() {
        println!("{item}");
    }
}
```

Decoding byte strings
---------------------

By default byte strings stay as `Value::Bytes`. `decode_strings()` turns
valid UTF-8 (then latin-1, which always succeeds) into `Value::String`.

```rust
use pickled::{value_from_slice, DeOptions};

let value = value_from_slice(&data, DeOptions::new().decode_strings()).unwrap();
```

With the `encoding` feature you can add more encodings as fallbacks, tried
after UTF-8 but before latin-1:

```rust
use pickled::{value_from_slice, DeOptions};

let opts = DeOptions::new()
    .decode_utf8()
    .decode_encoding(encoding_rs::WINDOWS_1251);
let value = value_from_slice(&data, opts).unwrap();
```

Custom Python objects
---------------------

Arbitrary Python class instances can be reconstructed as plain dictionaries
of their state instead of erroring on an unknown class:

```rust
use pickled::{value_from_slice, DeOptions};

let opts = DeOptions::new().replace_reconstructor_objects_structures();
let value = value_from_slice(&data, opts).unwrap();
```

To map specific classes onto your own Rust types, implement `PickleObject`
and register an `ObjectFactory` via `DeOptions::object_factory`.

Recursive structures
--------------------

Self-referential objects (a list that contains itself, say) unpickle without
looping: the back-edge becomes a `Value::Weak`, which you can follow with
`upgrade()`. Pass `.replace_recursive_structures()` to `DeOptions` to replace
back-edges with `None` instead.

```rust
use pickled::{value_from_slice, DeOptions, Value};

let value = value_from_slice(&data, DeOptions::new()).unwrap();

if let Some(list) = value.list_ref() {
    if let Some(Value::Weak(back)) = list.inner().last() {
        let _target = back.upgrade(); // Option<Value> back into the list
    }
}
```
