// Copyright (c) 2015-2021 Georg Brandl.  Licensed under the Apache License,
// Version 2.0 <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0>
// or the MIT license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at
// your option. This file may not be copied, modified, or distributed except
// according to those terms.

//! # Pickle deserialization
//!
//! Note: Serde's interface doesn't support all of Python's primitive types.  In
//! order to deserialize a pickle stream to `value::Value`, use the
//! `value_from_*` functions exported here, not the generic `from_*` functions.

use byteorder::BigEndian;
use byteorder::ByteOrder;
use byteorder::LittleEndian;
use iter_read::IterRead;
use iter_read::IterReadItem;
use num_bigint::BigInt;
use num_bigint::Sign;
use num_traits::ToPrimitive;
use serde::de;
use serde::de::Visitor;
use serde::forward_to_deserialize_any;
use std::borrow::Cow;
use std::char;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::fmt;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::iter::FusedIterator;
use std::mem;
use std::str;
use std::str::FromStr;
use std::vec;

use crate::object::DictObject;
use crate::object::ObjectFactory;
use crate::value::Dict;
use crate::value::HashableValue;
use crate::value::RawHashableValue;
use crate::value::Shared;
use crate::value::SharedFrozen;
use crate::value::Value as V;
use std::collections::BTreeSet;

use super::consts::*;
use super::error::Error;
use super::error::ErrorCode;
use super::error::Result;
use super::value;

type MemoId = u32;

#[derive(Clone, Debug, Eq, PartialEq)]
enum Global {
    Set,           // builtins/__builtin__.set
    Frozenset,     // builtins/__builtin__.frozenset
    Bytearray,     // builtins/__builtin__.bytearray
    List,          // builtins/__builtin__.list
    Int,           // builtins/__builtin__.int
    Encode,        // _codecs.encode
    Reconstructor, // copy_reg._reconstructor
    // anything else (may be a classobj that is later discarded). Boxed to keep
    // `Global` (and thus every `de::Value` slot in the transient tree) small;
    // this variant is rare relative to the millions of other nodes.
    Other(Box<(Cow<'static, str>, Cow<'static, str>)>),
}

/// Our intermediate representation of a value.
///
/// A stack/memo entry during single-pass unpickling.
///
/// The opcode loop builds public `value::Value`s directly; the only non-value
/// thing that ever lives on the stack is a module `Global` (a callable/class
/// marker consumed by REDUCE/NEWOBJ). There is no `MemoRef` variant: `GET`
/// pushes a clone of the memoized item (an `Rc` clone for containers, so
/// identity is preserved and in-place `APPEND`/`SETITEM` mutate the shared
/// instance, exactly as CPython relies on).
#[derive(Clone, Debug)]
enum Item {
    Global(Global),
    Value(value::Value),
    // A tuple that still contains at least one bare `Global` (e.g. the args
    // tuple of a `copy_reg._reconstructor` REDUCE, whose first element is a
    // class global). Kept transient so REDUCE can recover the globals; if such
    // a tuple is used as a plain value instead, it lossily becomes a
    // `Value::Tuple` with each global replaced by `None`.
    Args(Vec<Item>),
}

impl Item {
    /// Unwrap to a public value, mapping a bare global to `None` (matching the
    /// old converter, which turned an unresolved global inside a container into
    /// `Value::None`). Used when an item is placed into a container.
    fn into_value_lossy(self) -> value::Value {
        match self {
            Item::Value(v) => v,
            Item::Global(_) => value::Value::None,
            Item::Args(items) => value::Value::Tuple(SharedFrozen::new(
                items.into_iter().map(Item::into_value_lossy).collect(),
            )),
        }
    }
}

impl From<value::Value> for Item {
    fn from(v: value::Value) -> Self {
        Item::Value(v)
    }
}

/// Options for deserializing.
///
/// # String decoding
///
/// Python 2 byte strings (`SHORT_BINSTRING`, `BINSTRING`, `STRING` opcodes)
/// are raw byte sequences whose encoding depends on the producing application.
/// By default they are kept as `Value::Bytes`. You can enable decoders that
/// are tried in order until one succeeds:
///
/// 1. **UTF-8** ([`decode_utf8`](DeOptions::decode_utf8)) -- free, lossless for valid UTF-8
/// 2. **Custom encoding** ([`decode_encoding`](DeOptions::decode_encoding)) -- via `encoding_rs` (requires `encoding` feature)
/// 3. **Latin-1** ([`decode_latin1`](DeOptions::decode_latin1)) -- always succeeds (every byte is valid)
/// 4. Fall back to `Value::Bytes`
///
/// The convenience method [`decode_strings`](DeOptions::decode_strings) enables
/// both UTF-8 and latin-1, which handles the vast majority of pickle files.
///
/// Unicode opcodes (`BINUNICODE`, `SHORT_BINUNICODE`) are always decoded as
/// UTF-8 regardless of these settings.
#[derive(Default)]
pub struct DeOptions {
    decode_utf8: bool,
    decode_latin1: bool,
    #[cfg(feature = "encoding")]
    fallback_encodings: Vec<&'static encoding_rs::Encoding>,
    replace_unresolved_globals: bool,
    replace_recursive_structures: bool,
    replace_reconstructor_objects_with_dict: bool,
    object_factory: Option<ObjectFactory>,
}

impl fmt::Debug for DeOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("DeOptions");
        s.field("decode_utf8", &self.decode_utf8)
            .field("decode_latin1", &self.decode_latin1);
        #[cfg(feature = "encoding")]
        s.field(
            "fallback_encodings",
            &self
                .fallback_encodings
                .iter()
                .map(|e| e.name())
                .collect::<Vec<_>>(),
        );
        s.field(
            "replace_unresolved_globals",
            &self.replace_unresolved_globals,
        )
        .field(
            "replace_recursive_structures",
            &self.replace_recursive_structures,
        )
        .field(
            "replace_reconstructor_objects_with_dict",
            &self.replace_reconstructor_objects_with_dict,
        )
        .field(
            "object_factory",
            &self.object_factory.as_ref().map(|_| "..."),
        )
        .finish()
    }
}

impl DeOptions {
    /// Construct with default options:
    ///
    /// - don't decode byte strings (they stay as `Value::Bytes`)
    /// - don't replace unresolvable globals by `None`
    pub fn new() -> Self {
        Default::default()
    }

    /// Enable UTF-8 decoding for byte strings.
    ///
    /// Byte strings that are valid UTF-8 become `Value::String`.
    /// Non-UTF-8 data falls through to the next enabled decoder, or
    /// stays as `Value::Bytes`.
    pub fn decode_utf8(mut self) -> Self {
        self.decode_utf8 = true;
        self
    }

    /// Enable latin-1 (ISO 8859-1) decoding as a fallback.
    ///
    /// Latin-1 maps every byte 0x00-0xFF to the Unicode codepoint of the
    /// same value, so this always succeeds. When enabled, byte strings
    /// that weren't caught by an earlier decoder always become `Value::String`.
    pub fn decode_latin1(mut self) -> Self {
        self.decode_latin1 = true;
        self
    }

    /// Add a fallback encoding via [`encoding_rs`].
    ///
    /// Requires the `encoding` feature. Can be called multiple times to try
    /// encodings in order. Tried after UTF-8 but before latin-1.
    /// Unmappable bytes are replaced with U+FFFD.
    #[cfg(feature = "encoding")]
    pub fn decode_encoding(mut self, encoding: &'static encoding_rs::Encoding) -> Self {
        self.fallback_encodings.push(encoding);
        self
    }

    /// Append multiple fallback encodings at once.
    ///
    /// Equivalent to calling [`decode_encoding`](Self::decode_encoding) for
    /// each entry. Encodings are tried in slice order, after UTF-8 but
    /// before latin-1.
    #[cfg(feature = "encoding")]
    pub fn decode_encodings(mut self, encodings: &[&'static encoding_rs::Encoding]) -> Self {
        self.fallback_encodings.reserve(encodings.len());
        self.fallback_encodings.extend_from_slice(encodings);
        self
    }

    /// Enable UTF-8 + latin-1 decoding (recommended for most pickle files).
    ///
    /// Equivalent to `.decode_utf8().decode_latin1()`. Since latin-1
    /// always succeeds, this guarantees byte strings become `Value::String`.
    pub fn decode_strings(self) -> Self {
        self.decode_utf8().decode_latin1()
    }

    /// Activate replacing unresolved globals by `None`.
    pub fn replace_unresolved_globals(mut self) -> Self {
        self.replace_unresolved_globals = true;
        self
    }

    /// Activate replacing recursive structures by `None`, instead of erroring out.
    pub fn replace_recursive_structures(mut self) -> Self {
        self.replace_recursive_structures = true;
        self
    }

    /// Activate replacing reconstructor objects with a best-attempt dictionary, instead of erroring out.
    pub fn replace_reconstructor_objects_structures(mut self) -> Self {
        self.replace_reconstructor_objects_with_dict = true;
        self
    }

    /// Set a custom object factory for constructing Python objects during deserialization.
    ///
    /// The factory receives an `ObjectConstructionInfo` with the module and class name,
    /// and should return `Some(object)` for classes it handles, or `None` to fall back
    /// to the default `DictObject` behavior.
    pub fn object_factory(mut self, factory: ObjectFactory) -> Self {
        self.object_factory = Some(factory);
        self
    }
}

/// Decodes pickle streams into values.
pub struct Deserializer<R: Read> {
    rdr: BufReader<R>,
    options: DeOptions,
    pos: usize,
    value: Option<V>, // next public value to feed the serde visitor (post-parse)
    // Pickle memo (item, number of refs), indexed by memo id. A flat Vec
    // rather than a BTreeMap: memo ids are dense (MEMOIZE assigns them
    // sequentially; PUT/BINPUT use small explicit ids), so direct indexing
    // avoids the per-node overhead of a BTreeMap over a large memo. Absent
    // slots are `None`.
    memo: Vec<Option<(Item, i32)>>,
    stack: Vec<Item>,                // topmost items on the stack
    stacks: Vec<Vec<Item>>,          // items further down the stack, between MARKs
    strings_rc: HashMap<Vec<u8>, V>, // content-dedup of decoded strings
    tuple_rc: BTreeMap<Vec<value::RawHashableValue>, V>, // content-dedup of tuples
    // Set when an unresolved global was demoted to `None` while building a
    // value. Used to error at the end unless `replace_unresolved_globals`.
    saw_unresolved_global: bool,
}

impl<R: Read> Deserializer<R> {
    /// Construct a new Deserializer.
    pub fn new(rdr: R, options: DeOptions) -> Deserializer<R> {
        Deserializer {
            rdr: BufReader::new(rdr),
            pos: 0,
            value: None,
            memo: Vec::new(),
            stack: Vec::with_capacity(128),
            stacks: Vec::with_capacity(16),
            options,
            strings_rc: Default::default(),
            tuple_rc: Default::default(),
            saw_unresolved_global: false,
        }
    }

    /// Reset internal state, allowing reading multiple pickle dump calls from
    /// a single stream.
    ///
    /// By default `(value_)from_reader` closes the input stream. It is possible
    /// to deserialize multiple pickle objects from a single stream by
    /// implementing a custom reader and resetting the internal state before
    /// reading the next value.
    ///
    /// # Example
    ///
    /// Using `reset_memo` inside a custom deserializer to deserialize multiple
    /// objects from a single stream.
    ///
    /// ```
    /// # use std::io::Read;
    /// # use pickled::{Deserializer, Result, DeOptions};
    /// # use serde::Deserialize;
    /// struct PickleReader<R: Read + Sized>
    /// {
    ///     de: Deserializer<R>,
    /// }
    ///
    /// impl<R: Read + Sized> PickleReader<R>
    /// {
    ///    fn new(reader: R) -> PickleReader<R> {
    ///        PickleReader {
    ///            de: Deserializer::new(reader, DeOptions::new()),
    ///        }
    ///    }
    ///
    ///    pub fn read_object<'de, T: Deserialize<'de>>(&mut self) -> Result<T> {
    ///        self.de.reset_memo();
    ///        let value = Deserialize::deserialize(&mut self.de)?;
    ///        Ok(value)
    ///    }
    /// }
    ///
    /// let input = vec![0x80, 0x04, 0x95, 0x09, 0x00, 0x00, 0x00, 0x00,
    ///                  0x00, 0x00, 0x00, 0x8c, 0x05, 0x48, 0x65, 0x6c,
    ///                  0x6c, 0x6f, 0x94, 0x2e, 0x80, 0x04, 0x95, 0x0a,
    ///                  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x8c,
    ///                  0x06, 0x70, 0x69, 0x63, 0x6b, 0x6c, 0x65, 0x94,
    ///                  0x2e, 0x00];
    /// let mut reader = PickleReader::new(std::io::Cursor::new(input.as_slice()));
    /// let string1: String = reader.read_object().unwrap();
    /// let string2: String = reader.read_object().unwrap();
    /// assert_eq!(&string1, "Hello");
    /// assert_eq!(&string2, "pickle");
    /// ```
    pub fn reset_memo(&mut self) {
        self.memo.clear();
    }

    fn memo_get(&self, id: MemoId) -> Option<&(Item, i32)> {
        self.memo.get(id as usize).and_then(Option::as_ref)
    }

    /// Insert (or overwrite) a memo entry, growing the backing vec with `None`
    /// holes as needed.
    fn memo_insert(&mut self, id: MemoId, entry: (Item, i32)) {
        let idx = id as usize;
        if idx >= self.memo.len() {
            self.memo.resize_with(idx + 1, || None);
        }
        self.memo[idx] = Some(entry);
    }

    /// Decode a Value from this pickle.  This is different from going through
    /// the generic serde `deserialize`, since it preserves some types that are
    /// not in the serde data model, such as big integers.
    pub fn deserialize_value(&mut self) -> Result<value::Value> {
        let mut value = self.parse_value()?;
        if self.saw_unresolved_global && !self.options.replace_unresolved_globals {
            return Err(Error::Syntax(ErrorCode::UnresolvedGlobal));
        }
        self.break_cycles(&mut value)?;
        Ok(value)
    }

    /// Convert an item to a public value for placement into a container or as
    /// the result, flagging unresolved globals so the caller can error.
    fn demote(&mut self, item: Item) -> V {
        match &item {
            Item::Global(_) => self.saw_unresolved_global = true,
            Item::Args(items) if items.iter().any(|i| matches!(i, Item::Global(_))) => {
                self.saw_unresolved_global = true;
            }
            _ => {}
        }
        item.into_value_lossy()
    }

    /// Get the next value to deserialize, either by parsing the pickle stream
    /// or from `self.value`.
    fn get_next_value(&mut self) -> Result<V> {
        match self.value.take() {
            Some(v) => Ok(v),
            None => self.parse_value(),
        }
    }

    fn push_val(&mut self, v: V) {
        self.stack.push(Item::Value(v));
    }

    fn tuple_from_items(&mut self, items: Vec<Item>) -> Item {
        // A tuple containing a bare global stays transient so REDUCE can use it.
        if items
            .iter()
            .any(|it| matches!(it, Item::Global(_) | Item::Args(_)))
        {
            return Item::Args(items);
        }
        let values: Vec<V> = items.into_iter().map(Item::into_value_lossy).collect();
        // Content-dedup pure-value tuples, matching the old converter's tuple_rc.
        let hashable: std::result::Result<Vec<RawHashableValue>, _> = values
            .iter()
            .cloned()
            .map(|v| v.into_raw_hashable())
            .collect();
        if let Ok(key) = hashable {
            if let Some(cached) = self.tuple_rc.get(&key) {
                return Item::Value(cached.clone());
            }
            let value = V::Tuple(SharedFrozen::new(values));
            self.tuple_rc.insert(key, value.clone());
            Item::Value(value)
        } else {
            Item::Value(V::Tuple(SharedFrozen::new(values)))
        }
    }

    fn list_from_items(items: Vec<V>) -> V {
        V::List(Shared::new(items))
    }

    /// Build a dict from a flat `[k, v, k, v, ...]` mark, dropping any entry
    /// whose key is not hashable (matching the old converter).
    fn dict_from_items(items: Vec<V>) -> V {
        V::Dict(Shared::new(Dict::from_iter(Self::pairs_from_flat(items))))
    }

    /// Turn a flat `[k, v, k, v, ...]` vec into hashable (key, value) pairs,
    /// skipping entries whose key is not hashable.
    fn pairs_from_flat(items: Vec<V>) -> Vec<(HashableValue, V)> {
        let mut pairs = Vec::with_capacity(items.len() / 2);
        let mut it = items.into_iter();
        while let Some(k) = it.next() {
            let Some(v) = it.next() else { break };
            if let Ok(hk) = k.into_hashable() {
                pairs.push((hk, v));
            }
        }
        pairs
    }

    /// Parse a value from the underlying stream.  This consumes the whole
    /// pickle until the STOP opcode, building the public `value::Value` directly.
    fn parse_value(&mut self) -> Result<V> {
        loop {
            let value = self.read_byte()?;
            let opcode = Opcode::try_from(value).map_err(|code| self.inner_error(code))?;

            match opcode {
                // Specials
                Opcode::Proto => {
                    self.read_byte()?;
                }
                Opcode::Frame => {
                    self.read_fixed_8_bytes()?;
                }
                Opcode::Stop => {
                    let item = self.pop()?;
                    return Ok(self.demote(item));
                }
                Opcode::Mark => {
                    let stack = mem::replace(&mut self.stack, Vec::with_capacity(128));
                    self.stacks.push(stack);
                }
                Opcode::Pop => {
                    if self.stack.is_empty() {
                        self.pop_mark()?;
                    } else {
                        self.pop()?;
                    }
                }
                Opcode::PopMark => {
                    self.pop_mark()?;
                }
                Opcode::Dup => {
                    let top = self.stack.last().cloned();
                    match top {
                        Some(item) => self.stack.push(item),
                        None => return self.error(ErrorCode::StackUnderflow),
                    }
                }

                // Memo saving ops
                Opcode::Put => {
                    let bytes = self.read_line()?;
                    let memo_id = self.parse_ascii(bytes)?;
                    self.memoize_next(memo_id)?;
                }
                Opcode::BinPut => {
                    let memo_id = self.read_byte()?;
                    self.memoize_next(memo_id.into())?;
                }
                Opcode::LongBinPut => {
                    let bytes = self.read_fixed_4_bytes()?;
                    let memo_id = LittleEndian::read_u32(&bytes);
                    self.memoize_next(memo_id)?;
                }
                Opcode::Memoize => {
                    let memo_id = self.memo.len();
                    self.memoize_next(memo_id as MemoId)?;
                }

                // Memo getting ops
                Opcode::Get => {
                    let bytes = self.read_line()?;
                    let memo_id = self.parse_ascii(bytes)?;
                    self.get_memo(memo_id)?;
                }
                Opcode::BinGet => {
                    let memo_id = self.read_byte()?;
                    self.get_memo(memo_id.into())?;
                }
                Opcode::LongBinGet => {
                    let bytes = self.read_fixed_4_bytes()?;
                    let memo_id = LittleEndian::read_u32(&bytes);
                    self.get_memo(memo_id)?;
                }

                // Singletons
                Opcode::None => self.push_val(V::None),
                Opcode::NewFalse => self.push_val(V::Bool(false)),
                Opcode::NewTrue => self.push_val(V::Bool(true)),

                // ASCII-formatted numbers
                Opcode::Int => {
                    let line = self.read_line()?;
                    let val = self.decode_text_int(line)?;
                    self.push_val(val);
                }
                Opcode::Long => {
                    let line = self.read_line()?;
                    let long = self.decode_text_long(line)?;
                    self.push_val(long);
                }
                Opcode::Float => {
                    let line = self.read_line()?;
                    let f = self.parse_ascii(line)?;
                    self.push_val(V::F64(f));
                }

                // ASCII-formatted strings
                Opcode::String => {
                    let line = self.read_line()?;
                    let string = if let Some(cached_str) = self.strings_rc.get(&line) {
                        cached_str.clone()
                    } else {
                        let string = self.decode_escaped_string(&line)?;
                        self.strings_rc.insert(line, string.clone());
                        string
                    };
                    self.push_val(string);
                }
                Opcode::Unicode => {
                    let line = self.read_line()?;
                    let string = if let Some(cached_str) = self.strings_rc.get(&line) {
                        cached_str.clone()
                    } else {
                        let string = self.decode_escaped_unicode(&line)?;
                        self.strings_rc.insert(line, string.clone());
                        string
                    };
                    self.push_val(string);
                }

                // Binary-coded numbers
                Opcode::BinFloat => {
                    let bytes = self.read_fixed_8_bytes()?;
                    self.push_val(V::F64(BigEndian::read_f64(&bytes)));
                }
                Opcode::BinInt => {
                    let bytes = self.read_fixed_4_bytes()?;
                    self.push_val(V::I64(LittleEndian::read_i32(&bytes).into()));
                }
                Opcode::BinInt1 => {
                    let byte = self.read_byte()?;
                    self.push_val(V::I64(byte.into()));
                }
                Opcode::BinInt2 => {
                    let bytes = self.read_fixed_2_bytes()?;
                    self.push_val(V::I64(LittleEndian::read_u16(&bytes).into()));
                }
                Opcode::Long1 => {
                    let bytes = self.read_u8_prefixed_bytes()?;
                    let long = self.decode_binary_long(bytes);
                    self.push_val(long);
                }
                Opcode::Long4 => {
                    let bytes = self.read_i32_prefixed_bytes()?;
                    let long = self.decode_binary_long(bytes);
                    self.push_val(long);
                }

                // Length-prefixed (byte)strings
                Opcode::ShortBinBytes => {
                    let string = self.read_u8_prefixed_bytes()?;
                    self.push_val(V::Bytes(SharedFrozen::new(string)));
                }
                Opcode::BinBytes => {
                    let string = self.read_u32_prefixed_bytes()?;
                    self.push_val(V::Bytes(SharedFrozen::new(string)));
                }
                Opcode::BinBytes8 => {
                    let string = self.read_u64_prefixed_bytes()?;
                    self.push_val(V::Bytes(SharedFrozen::new(string)));
                }
                Opcode::ShortBinString => {
                    let string = self.read_u8_prefixed_bytes()?;
                    let decoded = self.decode_string(string)?;
                    self.push_val(decoded);
                }
                Opcode::BinString => {
                    let string = self.read_i32_prefixed_bytes()?;
                    let decoded = self.decode_string(string)?;
                    self.push_val(decoded);
                }
                Opcode::ShortBinUnicode => {
                    let string = self.read_u8_prefixed_bytes()?;
                    let decoded = self.decode_unicode(string)?;
                    self.push_val(decoded);
                }
                Opcode::BinUnicode => {
                    let string = self.read_u32_prefixed_bytes()?;
                    let decoded = self.decode_unicode(string)?;
                    self.push_val(decoded);
                }
                Opcode::BinUnicode8 => {
                    let string = self.read_u64_prefixed_bytes()?;
                    let decoded = self.decode_unicode(string)?;
                    self.push_val(decoded);
                }
                Opcode::ByteArray8 => {
                    let string = self.read_u64_prefixed_bytes()?;
                    self.push_val(V::Bytes(SharedFrozen::new(string)));
                }

                // Tuples
                Opcode::EmptyTuple => {
                    let tuple = self.tuple_from_items(Vec::new());
                    self.stack.push(tuple);
                }
                Opcode::Tuple1 => {
                    let item = self.pop()?;
                    let tuple = self.tuple_from_items(vec![item]);
                    self.stack.push(tuple);
                }
                Opcode::Tuple2 => {
                    let item2 = self.pop()?;
                    let item1 = self.pop()?;
                    let tuple = self.tuple_from_items(vec![item1, item2]);
                    self.stack.push(tuple);
                }
                Opcode::Tuple3 => {
                    let item3 = self.pop()?;
                    let item2 = self.pop()?;
                    let item1 = self.pop()?;
                    let tuple = self.tuple_from_items(vec![item1, item2, item3]);
                    self.stack.push(tuple);
                }
                Opcode::Tuple => {
                    let items = self.pop_mark()?;
                    let tuple = self.tuple_from_items(items);
                    self.stack.push(tuple);
                }

                // Lists
                Opcode::EmptyList => self.push_val(V::List(Shared::new(Vec::new()))),
                Opcode::List => {
                    let items = self.pop_mark_values()?;
                    self.push_val(Self::list_from_items(items));
                }
                Opcode::Append => {
                    let item = self.pop()?;
                    let value = self.demote(item);
                    self.modify_list(|list| list.push(value))?;
                }
                Opcode::Appends => {
                    let items = self.pop_mark_values()?;
                    self.modify_list(|list| list.extend(items))?;
                }

                // Dicts
                Opcode::EmptyDict => self.push_val(V::Dict(Shared::new(Dict::new()))),
                Opcode::Dict => {
                    let items = self.pop_mark_values()?;
                    self.push_val(Self::dict_from_items(items));
                }
                Opcode::SetItem => {
                    let value_item = self.pop()?;
                    let value = self.demote(value_item);
                    let key_item = self.pop()?;
                    let key = self.demote(key_item);
                    let pairs = Self::pairs_from_flat(vec![key, value]);
                    self.modify_dict(pairs)?;
                }
                Opcode::SetItems => {
                    let items = self.pop_mark_values()?;
                    let pairs = Self::pairs_from_flat(items);
                    self.modify_dict(pairs)?;
                }

                // Sets and frozensets
                Opcode::EmptySet => self.push_val(V::Set(Shared::new(BTreeSet::new()))),
                Opcode::FrozenSet => {
                    let items = self.pop_mark_values()?;
                    let set: BTreeSet<HashableValue> = items
                        .into_iter()
                        .filter_map(|v| v.into_hashable().ok())
                        .collect();
                    self.push_val(V::FrozenSet(SharedFrozen::new(set)));
                }
                Opcode::AddItems => {
                    let items = self.pop_mark_values()?;
                    let hashables: Vec<HashableValue> = items
                        .into_iter()
                        .filter_map(|v| v.into_hashable().ok())
                        .collect();
                    self.modify_set(hashables)?;
                }

                // Arbitrary module globals, used here for unpickling set and frozenset
                // from protocols < 4
                Opcode::Global => {
                    let modname = self.read_line()?;
                    let globname = self.read_line()?;
                    let item = self.decode_global(modname, globname)?;
                    self.stack.push(item);
                }
                Opcode::StackGlobal => {
                    let globname = match self.pop()? {
                        Item::Value(V::String(string)) => string.into_raw_or_cloned().into_bytes(),
                        other => return Self::stack_error("string", &other, self.pos),
                    };
                    let modname = match self.pop()? {
                        Item::Value(V::String(string)) => string.into_raw_or_cloned().into_bytes(),
                        other => return Self::stack_error("string", &other, self.pos),
                    };
                    let item = self.decode_global(modname, globname)?;
                    self.stack.push(item);
                }
                Opcode::Reduce => {
                    let argtuple: Vec<Item> = match self.pop()? {
                        Item::Args(items) => items,
                        Item::Value(V::Tuple(args)) => args
                            .into_raw_or_cloned()
                            .into_iter()
                            .map(Item::Value)
                            .collect(),
                        other => return Self::stack_error("tuple", &other, self.pos),
                    };
                    let global = self.pop()?;
                    self.reduce_global(global, argtuple)?;
                }

                // Arbitrary classes - make a best effort attempt to recover some data
                Opcode::Inst => {
                    let modname = self.read_line()?;
                    let globname = self.read_line()?;
                    let _args = self.pop_mark()?;
                    let modname = String::from_utf8(modname)
                        .map_err(|_| self.inner_error(ErrorCode::StringNotUTF8))?;
                    let globname = String::from_utf8(globname)
                        .map_err(|_| self.inner_error(ErrorCode::StringNotUTF8))?;
                    let obj = self.make_object(&modname, &globname);
                    self.stack.push(obj);
                }
                Opcode::Obj => {
                    let mut args = self.pop_mark()?;
                    let cls = if args.is_empty() {
                        self.pop()?
                    } else {
                        args.remove(0)
                    };
                    let (modname, globname) = match &cls {
                        Item::Global(Global::Other(names)) => (names.0.as_ref(), names.1.as_ref()),
                        _ => return Self::stack_error("global reference", &cls, self.pos),
                    };
                    let obj = self.make_object(modname, globname);
                    self.stack.push(obj);
                }
                Opcode::NewObj => {
                    let _args = self.pop()?;
                    let cls = self.pop()?;
                    let (modname, globname) = match &cls {
                        Item::Global(Global::Other(names)) => (names.0.as_ref(), names.1.as_ref()),
                        _ => return Self::stack_error("global reference", &cls, self.pos),
                    };
                    let obj = self.make_object(modname, globname);
                    self.stack.push(obj);
                }
                Opcode::NewObjEx => {
                    let _kwargs = self.pop()?;
                    let _args = self.pop()?;
                    let cls = self.pop()?;
                    let (modname, globname) = match &cls {
                        Item::Global(Global::Other(names)) => (names.0.as_ref(), names.1.as_ref()),
                        _ => return Self::stack_error("global reference", &cls, self.pos),
                    };
                    let obj = self.make_object(modname, globname);
                    self.stack.push(obj);
                }
                Opcode::Build => {
                    let state = self.pop()?.into_value_lossy();
                    let obj = self.pop()?;
                    match obj {
                        Item::Value(V::Object(shared)) => {
                            // GET handed out an Rc clone, so mutating through the
                            // shared cell also updates the memoized instance.
                            shared.inner_mut().__setstate__(state);
                            self.push_val(V::Object(shared));
                        }
                        // Legacy: a non-object standin is replaced by the state.
                        _ => self.push_val(state),
                    }
                }
            }
        }
    }

    // Pop the stack top item.
    fn pop(&mut self) -> Result<Item> {
        match self.stack.pop() {
            Some(v) => Ok(v),
            None => self.error(ErrorCode::StackUnderflow),
        }
    }

    // Pop all topmost stack items until the next MARK.
    fn pop_mark(&mut self) -> Result<Vec<Item>> {
        match self.stacks.pop() {
            Some(new) => Ok(mem::replace(&mut self.stack, new)),
            None => self.error(ErrorCode::StackUnderflow),
        }
    }

    // Pop a MARK group as public values (a bare global becomes `None`).
    fn pop_mark_values(&mut self) -> Result<Vec<V>> {
        let items = self.pop_mark()?;
        Ok(items.into_iter().map(|it| self.demote(it)).collect())
    }

    // Push a clone of a memoized item. For containers this is an `Rc` clone, so
    // the pushed value shares identity with the memoized one and later in-place
    // mutation (APPEND/SETITEM/BUILD) updates both.
    fn get_memo(&mut self, memo_id: MemoId) -> Result<()> {
        match self.memo_get(memo_id) {
            Some((item, _)) => {
                let item = item.clone();
                self.stack.push(item);
                Ok(())
            }
            None => Err(Error::Eval(ErrorCode::MissingMemo(memo_id), self.pos)),
        }
    }

    // Memoize the current stack top under the given id, leaving it on the stack
    // (a cheap `Rc` clone is stored, sharing identity with the stack value).
    fn memoize_next(&mut self, memo_id: MemoId) -> Result<()> {
        let item = self.pop()?;
        self.memo_insert(memo_id, (item.clone(), 1));
        self.stack.push(item);
        Ok(())
    }

    // APPEND/APPENDS: mutate the stack-top list in place.
    fn modify_list(&mut self, f: impl FnOnce(&mut Vec<V>)) -> Result<()> {
        match self.stack.last() {
            Some(Item::Value(V::List(list))) => {
                f(&mut list.inner_mut());
                Ok(())
            }
            other => {
                let pos = self.pos;
                match other {
                    Some(item) => Self::stack_error("list", &item.clone(), pos),
                    None => self.error(ErrorCode::StackUnderflow),
                }
            }
        }
    }

    // SETITEM/SETITEMS: insert sorted (key, value) pairs into the stack-top dict,
    // or dispatch to an object's `__setitem__`.
    fn modify_dict(&mut self, pairs: Vec<(HashableValue, V)>) -> Result<()> {
        match self.stack.last() {
            Some(Item::Value(V::Dict(dict))) => {
                dict.inner_mut().extend_sorted(pairs);
                Ok(())
            }
            Some(Item::Value(V::Object(obj))) => {
                let mut o = obj.inner_mut();
                for (k, v) in pairs {
                    o.__setitem__(k.into_value(), v);
                }
                Ok(())
            }
            other => {
                let pos = self.pos;
                match other {
                    Some(item) => Self::stack_error("dict", &item.clone(), pos),
                    None => self.error(ErrorCode::StackUnderflow),
                }
            }
        }
    }

    // ADDITEMS: insert into the stack-top set.
    fn modify_set(&mut self, items: Vec<HashableValue>) -> Result<()> {
        match self.stack.last() {
            Some(Item::Value(V::Set(set))) => {
                set.inner_mut().extend(items);
                Ok(())
            }
            other => {
                let pos = self.pos;
                match other {
                    Some(item) => Self::stack_error("set", &item.clone(), pos),
                    None => self.error(ErrorCode::StackUnderflow),
                }
            }
        }
    }

    /// Assert that we reached the end of the stream.
    pub fn end(&mut self) -> Result<()> {
        let mut buf = [0];
        match self.rdr.read(&mut buf) {
            Err(err) => Err(Error::Io(err)),
            Ok(1) => self.error(ErrorCode::TrailingBytes),
            _ => Ok(()),
        }
    }

    fn read_line(&mut self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(16);
        match self.rdr.read_until(b'\n', &mut buf) {
            Ok(_) => {
                self.pos += buf.len();
                buf.pop(); // remove newline
                if buf.last() == Some(&b'\r') {
                    buf.pop();
                }
                Ok(buf)
            }
            Err(err) => Err(Error::Io(err)),
        }
    }

    #[inline]
    fn read_byte(&mut self) -> Result<u8> {
        let mut buf = [0];
        match self.rdr.read(&mut buf) {
            Ok(1) => {
                self.pos += 1;
                Ok(buf[0])
            }
            Ok(_) => self.error(ErrorCode::EOFWhileParsing),
            Err(err) => Err(Error::Io(err)),
        }
    }

    #[inline]
    fn read_bytes(&mut self, n: usize) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        match self.rdr.by_ref().take(n as u64).read_to_end(&mut buf) {
            Ok(m) if n == m => {
                self.pos += n;
                Ok(buf)
            }
            Ok(_) => self.error(ErrorCode::EOFWhileParsing),
            Err(err) => Err(Error::Io(err)),
        }
    }

    #[inline]
    fn read_fixed_2_bytes(&mut self) -> Result<[u8; 2]> {
        let mut buf = [0; 2];
        match self.rdr.by_ref().take(2).read_exact(&mut buf) {
            Ok(()) => {
                self.pos += 2;
                Ok(buf)
            }
            Err(err) => {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    self.error(ErrorCode::EOFWhileParsing)
                } else {
                    Err(Error::Io(err))
                }
            }
        }
    }

    #[inline]
    fn read_fixed_4_bytes(&mut self) -> Result<[u8; 4]> {
        let mut buf = [0; 4];
        match self.rdr.by_ref().take(4).read_exact(&mut buf) {
            Ok(()) => {
                self.pos += 4;
                Ok(buf)
            }
            Err(err) => {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    self.error(ErrorCode::EOFWhileParsing)
                } else {
                    Err(Error::Io(err))
                }
            }
        }
    }

    #[inline]
    fn read_fixed_8_bytes(&mut self) -> Result<[u8; 8]> {
        let mut buf = [0; 8];
        match self.rdr.by_ref().take(8).read_exact(&mut buf) {
            Ok(()) => {
                self.pos += 8;
                Ok(buf)
            }
            Err(err) => {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    self.error(ErrorCode::EOFWhileParsing)
                } else {
                    Err(Error::Io(err))
                }
            }
        }
    }

    fn read_i32_prefixed_bytes(&mut self) -> Result<Vec<u8>> {
        let lenbytes = self.read_fixed_4_bytes()?;
        match LittleEndian::read_i32(&lenbytes) {
            0 => Ok(vec![]),
            l if l < 0 => self.error(ErrorCode::NegativeLength),
            l => self.read_bytes(l as usize),
        }
    }

    fn read_u64_prefixed_bytes(&mut self) -> Result<Vec<u8>> {
        let lenbytes = self.read_fixed_8_bytes()?;
        self.read_bytes(LittleEndian::read_u64(&lenbytes) as usize)
    }

    fn read_u32_prefixed_bytes(&mut self) -> Result<Vec<u8>> {
        let lenbytes = self.read_fixed_4_bytes()?;
        self.read_bytes(LittleEndian::read_u32(&lenbytes) as usize)
    }

    fn read_u8_prefixed_bytes(&mut self) -> Result<Vec<u8>> {
        let lenbyte = self.read_byte()?;
        self.read_bytes(lenbyte as usize)
    }

    // Parse an expected ASCII literal from the stream or raise an error.
    fn parse_ascii<T: FromStr>(&self, bytes: Vec<u8>) -> Result<T> {
        match str::from_utf8(&bytes).unwrap_or("").parse() {
            Ok(v) => Ok(v),
            Err(_) => self.error(ErrorCode::InvalidLiteral(bytes)),
        }
    }

    // Decode a text-encoded integer.
    fn decode_text_int(&self, line: Vec<u8>) -> Result<V> {
        // Handle protocol 1 way of spelling true/false
        Ok(if line == b"00" {
            V::Bool(false)
        } else if line == b"01" {
            V::Bool(true)
        } else {
            let i = self.parse_ascii(line)?;
            V::I64(i)
        })
    }

    // Decode a text-encoded long integer.
    fn decode_text_long(&self, mut line: Vec<u8>) -> Result<V> {
        // Remove "L" suffix.
        if line.last() == Some(&b'L') {
            line.pop();
        }
        match BigInt::parse_bytes(&line, 10) {
            Some(i) => Ok(Self::normalize_int(i)),
            None => self.error(ErrorCode::InvalidLiteral(line)),
        }
    }

    // Decode an escaped string.  These are encoded with "normal" Python string
    // escape rules.
    fn decode_escaped_string(&self, slice: &[u8]) -> Result<V> {
        // Remove quotes if they appear.
        let slice = if (slice.len() >= 2)
            && (slice[0] == slice[slice.len() - 1])
            && (slice[0] == b'"' || slice[0] == b'\'')
        {
            &slice[1..slice.len() - 1]
        } else {
            slice
        };
        let mut result = Vec::with_capacity(slice.len());
        let mut iter = slice.iter();
        while let Some(&b) = iter.next() {
            match b {
                b'\\' => match iter.next() {
                    Some(&b'\\') => result.push(b'\\'),
                    Some(&b'a') => result.push(b'\x07'),
                    Some(&b'b') => result.push(b'\x08'),
                    Some(&b't') => result.push(b'\x09'),
                    Some(&b'n') => result.push(b'\x0a'),
                    Some(&b'v') => result.push(b'\x0b'),
                    Some(&b'f') => result.push(b'\x0c'),
                    Some(&b'r') => result.push(b'\x0d'),
                    Some(&b'x') => {
                        match iter
                            .next()
                            .and_then(|&ch1| (ch1 as char).to_digit(16))
                            .and_then(|v1| {
                                iter.next()
                                    .and_then(|&ch2| (ch2 as char).to_digit(16))
                                    .map(|v2| 16 * (v1 as u8) + (v2 as u8))
                            }) {
                            Some(v) => result.push(v),
                            None => return self.error(ErrorCode::InvalidLiteral(slice.into())),
                        }
                    }
                    _ => return self.error(ErrorCode::InvalidLiteral(slice.into())),
                },
                _ => result.push(b),
            }
        }
        self.decode_string(result)
    }

    // Decode escaped Unicode strings. These are encoded with "raw-unicode-escape",
    // which only knows the \uXXXX and \UYYYYYYYY escapes. The backslash is escaped
    // in this way, too.
    fn decode_escaped_unicode(&self, s: &[u8]) -> Result<V> {
        let mut result = String::with_capacity(s.len());
        let mut iter = s.iter();
        while let Some(&b) = iter.next() {
            match b {
                b'\\' => {
                    let nescape = match iter.next() {
                        Some(&b'u') => 4,
                        Some(&b'U') => 8,
                        _ => return self.error(ErrorCode::InvalidLiteral(s.into())),
                    };
                    let mut accum = 0;
                    for _i in 0..nescape {
                        accum *= 16;
                        match iter.next().and_then(|&ch| (ch as char).to_digit(16)) {
                            Some(v) => accum += v,
                            None => return self.error(ErrorCode::InvalidLiteral(s.into())),
                        }
                    }
                    match char::from_u32(accum) {
                        Some(v) => result.push(v),
                        None => return self.error(ErrorCode::InvalidLiteral(s.into())),
                    }
                }
                _ => result.push(b as char),
            }
        }
        Ok(V::String(SharedFrozen::new(result)))
    }

    // Decode a byte string by trying each enabled decoder in order:
    // UTF-8 -> custom encodings -> latin-1 -> Bytes
    fn decode_string(&self, string: Vec<u8>) -> Result<V> {
        let mut bytes = string;

        if self.options.decode_utf8 {
            match String::from_utf8(bytes) {
                Ok(v) => return Ok(V::String(SharedFrozen::new(v))),
                Err(e) => bytes = e.into_bytes(),
            }
        }

        #[cfg(feature = "encoding")]
        for encoding in &self.options.fallback_encodings {
            let (decoded, _, had_errors) = encoding.decode(&bytes);
            if !had_errors {
                return Ok(V::String(SharedFrozen::new(decoded.into_owned())));
            }
        }

        // latin1 can (apparently) be trivially and accurately converted
        // to utf-8, so if that's enabled, try that as a last resort
        if self.options.decode_latin1 {
            let decoded: String = bytes.iter().map(|&b| b as char).collect();
            return Ok(V::String(SharedFrozen::new(decoded)));
        }

        Ok(V::Bytes(SharedFrozen::new(bytes)))
    }

    // Decode a Unicode string from UTF-8.
    //
    // Unlike `decode_string`, this errors on invalid UTF-8 rather than
    // falling back: the calling opcodes (BINUNICODE, SHORT_BINUNICODE,
    // UNICODE) are defined to carry Python str data, so bad bytes here
    // mean a broken pickle, not an unknown encoding.
    fn decode_unicode(&self, string: Vec<u8>) -> Result<V> {
        match String::from_utf8(string) {
            Ok(v) => Ok(V::String(SharedFrozen::new(v))),
            Err(_) => self.error(ErrorCode::StringNotUTF8),
        }
    }

    // Decode a binary-encoded long integer.
    fn decode_binary_long(&self, bytes: Vec<u8>) -> V {
        // BigInt::from_bytes_le doesn't like a sign bit in the bytes, therefore
        // we have to extract that ourselves and do the two-s complement.
        let negative = !bytes.is_empty() && (bytes[bytes.len() - 1] & 0x80 != 0);
        let mut val = BigInt::from_bytes_le(Sign::Plus, &bytes);
        if negative {
            val -= BigInt::from(1) << (bytes.len() * 8);
        }
        Self::normalize_int(val)
    }

    // Long opcodes carry arbitrary-precision ints; normalize to I64 when they
    // fit, matching how short integer opcodes (and the old converter) behaved.
    fn normalize_int(i: BigInt) -> V {
        match i.to_i64() {
            Some(n) => V::I64(n),
            None => V::Int(Box::new(i)),
        }
    }

    /// Create an object using the factory callback, or fall back to DictObject.
    fn make_object(&self, modname: &str, globname: &str) -> Item {
        let info = crate::object::ObjectConstructionInfo {
            module: modname,
            class: globname,
        };
        if let Some(ref factory) = self.options.object_factory
            && let Some(obj) = factory(info)
        {
            return Item::Value(V::Object(Shared::new(obj)));
        }
        Item::Value(V::Object(Shared::new(Box::new(DictObject::new(
            modname.to_owned(),
            globname.to_owned(),
        )))))
    }

    // Push the global referenced by modname and globname.
    fn decode_global(&mut self, modname: Vec<u8>, globname: Vec<u8>) -> Result<Item> {
        let value = match (&*modname, &*globname) {
            (b"_codecs", b"encode") => Item::Global(Global::Encode),
            (b"__builtin__", b"set") | (b"builtins", b"set") => Item::Global(Global::Set),
            (b"__builtin__", b"frozenset") | (b"builtins", b"frozenset") => {
                Item::Global(Global::Frozenset)
            }
            (b"__builtin__", b"list") | (b"builtins", b"list") => Item::Global(Global::List),
            (b"__builtin__", b"bytearray") | (b"builtins", b"bytearray") => {
                Item::Global(Global::Bytearray)
            }
            (b"__builtin__", b"int") | (b"builtins", b"int") => Item::Global(Global::Int),
            (b"copy_reg", b"_reconstructor") => Item::Global(Global::Reconstructor),
            _ => {
                let modname = String::from_utf8(modname)
                    .map_err(|_| self.inner_error(ErrorCode::StringNotUTF8))?;
                let globname = String::from_utf8(globname)
                    .map_err(|_| self.inner_error(ErrorCode::StringNotUTF8))?;

                Item::Global(Global::Other(Box::new((
                    Cow::Owned(modname),
                    Cow::Owned(globname),
                ))))
            }
        };
        Ok(value)
    }

    // Handle the REDUCE opcode for the few Global objects we support.
    fn reduce_global(&mut self, global: Item, mut argtuple: Vec<Item>) -> Result<()> {
        let global = match global {
            Item::Global(g) => g,
            other => return Self::stack_error("global reference", &other, self.pos),
        };
        match global {
            Global::Set => match argtuple.pop().map(Item::into_value_lossy) {
                Some(V::List(items)) => {
                    self.push_val(V::Set(Shared::new(Self::to_hashable_set(items))));
                    Ok(())
                }
                _ => self.error(ErrorCode::InvalidValue("set() arg".into())),
            },
            Global::Frozenset => match argtuple.pop().map(Item::into_value_lossy) {
                Some(V::List(items)) => {
                    self.push_val(V::FrozenSet(SharedFrozen::new(Self::to_hashable_set(
                        items,
                    ))));
                    Ok(())
                }
                _ => self.error(ErrorCode::InvalidValue("frozenset() arg".into())),
            },
            Global::Bytearray => {
                // On Py2, the call is encoded as bytearray(u"foo", "latin-1").
                argtuple.truncate(1);
                match argtuple.pop().map(Item::into_value_lossy) {
                    Some(V::Bytes(bytes)) => {
                        self.push_val(V::Bytes(bytes));
                        Ok(())
                    }
                    Some(V::String(string)) => {
                        // The code points in the string are actually bytes values.
                        self.push_val(V::Bytes(SharedFrozen::new(
                            string.inner().chars().map(|ch| ch as u32 as u8).collect(),
                        )));
                        Ok(())
                    }
                    _ => self.error(ErrorCode::InvalidValue("bytearray() arg".into())),
                }
            }
            Global::List => match argtuple.pop().map(Item::into_value_lossy) {
                Some(V::List(items)) => {
                    self.push_val(V::List(items));
                    Ok(())
                }
                _ => self.error(ErrorCode::InvalidValue("list() arg".into())),
            },
            Global::Int => match argtuple.pop().map(Item::into_value_lossy) {
                Some(V::Int(integer)) => {
                    self.push_val(V::Int(integer));
                    Ok(())
                }
                _ => self.error(ErrorCode::InvalidValue("int() arg".into())),
            },
            Global::Encode => {
                // Byte object encoded as _codecs.encode(x, 'latin1')
                match argtuple.pop().map(Item::into_value_lossy) {
                    Some(V::String(_)) => {} // encoding, always latin1
                    _ => return self.error(ErrorCode::InvalidValue("encode() arg".into())),
                }
                match argtuple.pop().map(Item::into_value_lossy) {
                    Some(V::String(s)) => {
                        let bytes = s.inner().chars().map(|ch| ch as u8).collect();
                        self.push_val(V::Bytes(SharedFrozen::new(bytes)));
                        Ok(())
                    }
                    _ => self.error(ErrorCode::InvalidValue("encode() arg".into())),
                }
            }
            Global::Reconstructor => {
                let _state = argtuple.pop();
                let _base_cls = argtuple.pop();
                let cls = argtuple.pop();
                if self.options.object_factory.is_some()
                    || self.options.replace_reconstructor_objects_with_dict
                {
                    let (modname, globname) = match &cls {
                        Some(Item::Global(Global::Other(names))) => {
                            (names.0.as_ref().to_owned(), names.1.as_ref().to_owned())
                        }
                        _ => {
                            let item = cls.unwrap_or(Item::Value(V::None));
                            return Self::stack_error("global reference", &item, self.pos);
                        }
                    };
                    let obj = self.make_object(&modname, &globname);
                    self.stack.push(obj);
                } else {
                    // Keep it as an unresolved global so the error bubbles up.
                    self.stack.push(Item::Global(Global::Other(Box::new((
                        Cow::Borrowed("copy_reg"),
                        Cow::Borrowed("_reconstructor"),
                    )))));
                }
                Ok(())
            }
            Global::Other(names) => {
                // Anything else; keep it on the stack as an opaque global.
                self.stack.push(Item::Global(Global::Other(names)));
                Ok(())
            }
        }
    }

    // Convert a list of public values into a hashable set, skipping unhashable
    // elements (matching the old converter's set/frozenset handling).
    fn to_hashable_set(items: Shared<Vec<V>>) -> BTreeSet<HashableValue> {
        items
            .into_raw_or_cloned()
            .into_iter()
            .filter_map(|v| v.into_hashable().ok())
            .collect()
    }

    fn stack_error<T>(what: &'static str, value: &Item, pos: usize) -> Result<T> {
        let it = format!("{value:?}");
        Err(Error::Eval(ErrorCode::InvalidStackTop(what, it), pos))
    }

    fn error<T>(&self, reason: ErrorCode) -> Result<T> {
        Err(self.inner_error(reason))
    }

    fn inner_error(&self, reason: ErrorCode) -> Error {
        Error::Eval(reason, self.pos)
    }

    /// Break reference cycles in the finished value so they do not leak.
    /// Identity-memoized DFS keyed by container `Rc` pointer, visiting each
    /// unique container once (shared subtrees are not re-walked). A back-edge
    /// inside a mutable container (`List`/`Dict`) is downgraded to
    /// `Value::Weak`, or replaced with `None` when `replace_recursive_structures`
    /// is set. For acyclic data (all real WoWs pickles) it walks once and
    /// changes nothing.
    fn break_cycles(&mut self, value: &mut V) -> Result<()> {
        let mut visited: HashSet<*const ()> = HashSet::new();
        let mut path: HashSet<*const ()> = HashSet::new();
        break_walk(
            value,
            self.options.replace_recursive_structures,
            &mut visited,
            &mut path,
        );
        Ok(())
    }
}

/// Identity of a shared container, or `None` for a leaf value.
fn container_ptr(v: &V) -> Option<*const ()> {
    use std::rc::Rc;
    match v {
        V::List(s) => Some(s.rc_ptr() as *const ()),
        V::Dict(s) => Some(s.rc_ptr() as *const ()),
        V::Set(s) => Some(s.rc_ptr() as *const ()),
        V::Object(s) => Some(s.rc_ptr() as *const ()),
        V::Tuple(s) => Some(Rc::as_ptr(s.rc_ref()) as *const ()),
        V::FrozenSet(s) => Some(Rc::as_ptr(s.rc_ref()) as *const ()),
        _ => None,
    }
}

/// Replace a back-edge slot with a `Weak` (or `None`), or recurse into a
/// non-back-edge child container.
fn fix_or_recurse(
    slot: &mut V,
    replace: bool,
    visited: &mut HashSet<*const ()>,
    path: &mut HashSet<*const ()>,
) {
    let Some(cptr) = container_ptr(slot) else {
        return;
    };
    if path.contains(&cptr) {
        *slot = if replace {
            V::None
        } else {
            crate::value::downgrade_value(slot)
                .map(V::Weak)
                .unwrap_or(V::None)
        };
    } else {
        break_walk(slot, replace, visited, path);
    }
}

fn break_walk(
    v: &V,
    replace: bool,
    visited: &mut HashSet<*const ()>,
    path: &mut HashSet<*const ()>,
) {
    let Some(ptr) = container_ptr(v) else {
        return;
    };
    if !visited.insert(ptr) {
        return;
    }
    path.insert(ptr);
    match v {
        V::List(s) => {
            for slot in s.inner_mut().iter_mut() {
                fix_or_recurse(slot, replace, visited, path);
            }
        }
        V::Dict(s) => {
            for slot in s.inner_mut().values_mut() {
                fix_or_recurse(slot, replace, visited, path);
            }
        }
        // Tuples are immutable: we cannot replace a direct back-edge slot, but we
        // still recurse to break cycles in nested mutable containers.
        V::Tuple(s) => {
            for child in s.inner().iter() {
                if let Some(cptr) = container_ptr(child)
                    && !path.contains(&cptr)
                {
                    break_walk(child, replace, visited, path);
                }
            }
        }
        // Object state is read through the trait; recurse to break cycles in
        // nested mutable containers (a direct back-edge into object state is
        // left strong -- only reachable from exotic hand-built pickles).
        V::Object(s) => {
            let inner = s.inner();
            if let Some(dict_obj) = inner.as_any().downcast_ref::<DictObject>() {
                for (_, child) in dict_obj.state().iter() {
                    if let Some(cptr) = container_ptr(child)
                        && !path.contains(&cptr)
                    {
                        break_walk(child, replace, visited, path);
                    }
                }
            }
        }
        // Sets/frozensets hold only hashable (immutable) elements.
        _ => {}
    }
    path.remove(&ptr);
}

impl<'de: 'a, 'a, R: Read> de::Deserializer<'de> for &'a mut Deserializer<R> {
    type Error = Error;

    fn deserialize_any<V2: Visitor<'de>>(self, visitor: V2) -> Result<V2::Value> {
        let value = self.get_next_value()?;
        match value {
            V::None => visitor.visit_unit(),
            V::Bool(v) => visitor.visit_bool(v),
            V::I64(v) => visitor.visit_i64(v),
            V::Int(v) => {
                if let Some(i) = v.to_i64() {
                    visitor.visit_i64(i)
                } else {
                    Err(Error::Syntax(ErrorCode::InvalidValue(
                        "integer too large".into(),
                    )))
                }
            }
            V::F64(v) => visitor.visit_f64(v),
            V::Bytes(v) => visitor.visit_byte_buf(v.into_raw_or_cloned()),
            V::String(v) => visitor.visit_string(v.into_raw_or_cloned()),
            V::List(v) => {
                let v = v.into_raw_or_cloned();
                let len = v.len();
                visitor.visit_seq(SeqAccess {
                    de: self,
                    iter: v.into_iter(),
                    len,
                })
            }
            V::Tuple(v) => {
                let v = v.into_raw_or_cloned();
                visitor.visit_seq(SeqAccess {
                    len: v.len(),
                    iter: v.into_iter(),
                    de: self,
                })
            }
            V::Set(v) => {
                let items: Vec<V> = v
                    .into_raw_or_cloned()
                    .into_iter()
                    .map(HashableValue::into_value)
                    .collect();
                let len = items.len();
                visitor.visit_seq(SeqAccess {
                    de: self,
                    len,
                    iter: items.into_iter(),
                })
            }
            V::FrozenSet(v) => {
                let items: Vec<V> = v
                    .into_raw_or_cloned()
                    .into_iter()
                    .map(HashableValue::into_value)
                    .collect();
                let len = items.len();
                visitor.visit_seq(SeqAccess {
                    de: self,
                    len,
                    iter: items.into_iter(),
                })
            }
            V::Dict(v) => {
                let pairs: Vec<(V, V)> = v
                    .into_raw_or_cloned()
                    .into_entries()
                    .into_iter()
                    .map(|(k, val)| (k.into_value(), val))
                    .collect();
                let len = pairs.len();
                visitor.visit_map(MapAccess {
                    de: self,
                    iter: pairs.into_iter(),
                    value: None,
                    len,
                })
            }
            V::Object(o) => {
                let public_val = o.inner().__reduce__().state_or_none();
                let mut de = crate::value_impls::Deserializer::new(public_val);
                serde::de::Deserializer::deserialize_any(&mut de, visitor)
            }
            // A recursive back-edge: deserialize the live target, else unit.
            V::Weak(w) => match w.upgrade() {
                Some(v) => {
                    self.value = Some(v);
                    self.deserialize_any(visitor)
                }
                None => visitor.visit_unit(),
            },
        }
    }

    #[inline]
    fn deserialize_option<V2: Visitor<'de>>(self, visitor: V2) -> Result<V2::Value> {
        let value = self.get_next_value()?;
        match value {
            V::None => visitor.visit_none(),
            _ => {
                self.value = Some(value);
                visitor.visit_some(self)
            }
        }
    }

    #[inline]
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_newtype_struct(self)
    }

    #[inline]
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_enum(VariantAccess { de: self })
    }

    forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit seq
        bytes byte_buf map tuple_struct struct identifier
        tuple ignored_any unit_struct
    }
}

struct VariantAccess<'a, R: Read + 'a> {
    de: &'a mut Deserializer<R>,
}

impl<'de: 'a, 'a, R: Read + 'a> de::EnumAccess<'de> for VariantAccess<'a, R> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V2: de::DeserializeSeed<'de>>(self, seed: V2) -> Result<(V2::Value, Self)> {
        let value = self.de.get_next_value()?;
        match value {
            V::Tuple(v) => {
                let mut v = v.into_raw_or_cloned();
                if v.len() == 2 {
                    let args = v.pop();
                    self.de.value = v.pop();
                    let val = seed.deserialize(&mut *self.de)?;
                    self.de.value = args;
                    Ok((val, self))
                } else {
                    self.de.value = v.pop();
                    let val = seed.deserialize(&mut *self.de)?;
                    Ok((val, self))
                }
            }
            V::Dict(v) => {
                let mut entries = v.into_raw_or_cloned().into_entries();
                if entries.len() != 1 {
                    Err(Error::Syntax(ErrorCode::Structure(
                        "enum variants must have one dict entry".into(),
                    )))
                } else {
                    let (name, args) = entries.pop().unwrap();
                    self.de.value = Some(name.into_value());
                    let val = seed.deserialize(&mut *self.de)?;
                    self.de.value = Some(args);
                    Ok((val, self))
                }
            }
            s @ V::String(_) => {
                self.de.value = Some(s);
                let val = seed.deserialize(&mut *self.de)?;
                Ok((val, self))
            }
            _ => Err(Error::Syntax(ErrorCode::Structure(
                "enums must be represented as dicts or tuples".into(),
            ))),
        }
    }
}

impl<'de: 'a, 'a, R: Read + 'a> de::VariantAccess<'de> for VariantAccess<'a, R> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T: de::DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value> {
        seed.deserialize(self.de)
    }

    fn tuple_variant<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value> {
        de::Deserializer::deserialize_any(self.de, visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        de::Deserializer::deserialize_any(self.de, visitor)
    }
}

struct SeqAccess<'a, R: Read + 'a> {
    de: &'a mut Deserializer<R>,
    iter: vec::IntoIter<V>,
    len: usize,
}

impl<'de: 'a, 'a, R: Read> de::SeqAccess<'de> for SeqAccess<'a, R> {
    type Error = Error;

    fn next_element_seed<T: de::DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>> {
        match self.iter.next() {
            Some(value) => {
                self.len -= 1;
                self.de.value = Some(value);
                Ok(Some(seed.deserialize(&mut *self.de)?))
            }
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.len)
    }
}

struct MapAccess<'a, R: Read + 'a> {
    de: &'a mut Deserializer<R>,
    iter: vec::IntoIter<(V, V)>,
    value: Option<V>,
    len: usize,
}

impl<'de: 'a, 'a, R: Read> de::MapAccess<'de> for MapAccess<'a, R> {
    type Error = Error;

    fn next_key_seed<T: de::DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        match self.iter.next() {
            Some((key, value)) => {
                self.len -= 1;
                self.value = Some(value);
                self.de.value = Some(key);
                Ok(Some(seed.deserialize(&mut *self.de)?))
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<T: de::DeserializeSeed<'de>>(&mut self, seed: T) -> Result<T::Value> {
        let value = self.value.take().unwrap();
        self.de.value = Some(value);
        seed.deserialize(&mut *self.de)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.len)
    }
}

/// Decodes a value from a `std::io::Read`.
pub fn from_reader<'de, R: io::Read, T: de::Deserialize<'de>>(
    rdr: R,
    options: DeOptions,
) -> Result<T> {
    let mut de = Deserializer::new(rdr, options);
    let value = de::Deserialize::deserialize(&mut de)?;
    // Make sure the whole stream has been consumed.
    de.end()?;
    Ok(value)
}

/// Decodes a value from a byte slice `&[u8]`.
pub fn from_slice<'de, T: de::Deserialize<'de>>(v: &[u8], options: DeOptions) -> Result<T> {
    from_reader(io::Cursor::new(v), options)
}

/// Decodes a value from any iterator supported as a reader.
pub fn from_iter<'de, E, I, T>(it: I, options: DeOptions) -> Result<T>
where
    E: IterReadItem,
    I: FusedIterator<Item = E>,
    T: de::Deserialize<'de>,
{
    from_reader(IterRead::new(it), options)
}

/// Decodes a value from a `std::io::Read`.
pub fn value_from_reader<R: io::Read>(rdr: R, options: DeOptions) -> Result<value::Value> {
    let mut de = Deserializer::new(rdr, options);
    let value = de.deserialize_value()?;
    de.end()?;
    Ok(value)
}

/// Decodes a value from a byte slice `&[u8]`.
pub fn value_from_slice(v: &[u8], options: DeOptions) -> Result<value::Value> {
    value_from_reader(io::Cursor::new(v), options)
}

/// Decodes a value from any iterator supported as a reader.
pub fn value_from_iter<E, I>(it: I, options: DeOptions) -> Result<value::Value>
where
    E: IterReadItem,
    I: FusedIterator<Item = E>,
{
    value_from_reader(IterRead::new(it), options)
}
