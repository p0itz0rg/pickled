// Copyright (c) 2015-2021 Georg Brandl.  Licensed under the Apache License,
// Version 2.0 <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0>
// or the MIT license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at
// your option. This file may not be copied, modified, or distributed except
// according to those terms.

//! Serialization and deserialization for Python's pickle format
//!
//! # Pickle format
//!
//! Please see the [Python docs](http://docs.python.org/library/pickle) for
//! details on the Pickle format.
//!
//! This crate supports all Pickle protocols (0 to 5) when reading, and writing
//! protocol 2 (compatible with Python 2 and 3), or protocol 3 (compatible with
//! Python 3 only).
//!
//! # Supported types
//!
//! Pickle is very powerful.  It is capable of serializing pretty arbitrary
//! graphs of Python objects, with most custom classes being serialized out
//! of the box.  Currently, this crate only supports Python's built-in types
//! that map easily to Rust constructs.  There are:
//!
//! * None
//! * Boolean (Rust `bool`)
//! * Integers (Rust `i64` or bigints from num)
//! * Floats (Rust `f64`)
//! * Bytes objects and bytearrays (see below)
//! * (Unicode) strings (Rust `String`)
//! * Lists and tuples (Rust `Vec<Value>`)
//! * Sets and frozensets (Rust `HashSet<Value>`)
//! * Dictionaries (Rust `HashMap<Value, Value>`)
//!
//! When deserializing, arbitrary Python objects saved using a pickled instance
//! dictionary or `__setstate__` are replaced by that state, since version
//! 0.5 of this library.
//!
//! *Note on enums:* Enum variants are serialized as Python tuples `(name,
//! [data])` instead of mappings (or a plain string for unit variants), which is
//! the representation selected by e.g. `serde_json`.  On deserialization, both
//! the tuple form and the string/mapping form is accepted.
//!
//! *Note on bytes objects:* when deserializing bytes objects, you have to use a
//! Rust wrapper type that enables deserialization from the serde data model's
//! "bytes" type.  The [`serde_bytes`](https://docs.serde.rs/serde_bytes/) crate
//! provides such wrappers.
//!
//! Likewise, `Vec<u8>`, `[u8; N]` and `&[u8]` are treated as sequences when
//! serializing.  This means that they will be serialized as a tuple or list of
//! integers unless you use one of the wrappers in `serde_bytes`.
//!
//! # Unsupported features
//!
//! - Recursive objects using the `PERSID` and `EXT` type opcodes.
//! - Out-of-band data as introduced in Pickle protocol 5.
//!
//! # Exported API
//!
//! The library exports generic serde (de)serializing functions `to_*` and
//! `from_*`.  It also exports functions that produce or take only the specific
//! `Value` struct exposed by this library, which supports all built-in Python
//! types (notably, long integers and sets, which serde's generic types don't
//! handle).  These functions, called `value_from_*` and `value_to_*`, will
//! correctly (un)pickle these types.
//!
//! # Minimum Supported Rust Version
//!
//! The minimum supported version of the toolchain is 1.41.1.

pub use self::ser::SerOptions;
pub use self::ser::Serializer;
pub use self::ser::to_vec;
pub use self::ser::to_writer;
pub use self::ser::value_to_vec;
pub use self::ser::value_to_writer;

pub use self::de::DeOptions;
pub use self::de::Deserializer;
pub use self::de::from_iter;
pub use self::de::from_reader;
pub use self::de::from_slice;
pub use self::de::value_from_iter;
pub use self::de::value_from_reader;
pub use self::de::value_from_slice;

pub use self::value::HashableValue;
pub use self::value::Value;
pub use self::value::from_value;
pub use self::value::to_value;

pub use self::error::Error;
pub use self::error::ErrorCode;
pub use self::error::Result;
pub use self::object::ObjectConstructionInfo;
pub use self::object::ObjectFactory;
pub use self::object::PickleObject;
pub use self::object::ReduceResult;
pub use num_bigint;
pub use num_traits;

mod consts;
pub mod de;
pub mod error;
pub mod object;
pub mod ser;
pub mod value;
mod value_impls;

#[cfg(test)]
#[path = "../test/mod.rs"]
mod test;
