// Copyright (c) 2015-2021 Georg Brandl.  Licensed under the Apache License,
// Version 2.0 <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0>
// or the MIT license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at
// your option. This file may not be copied, modified, or distributed except
// according to those terms.

//! Python values, and serialization instances for them.

use num_bigint::BigInt;
use num_bigint::Sign;
use num_traits::Float;
use num_traits::Signed;
use num_traits::ToPrimitive;
use std::borrow::Cow;
use std::cell::Ref;
use std::cell::RefCell;
use std::cell::RefMut;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;
use std::rc::Rc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering as AtomicOrdering;

pub use crate::value_impls::from_value;
pub use crate::value_impls::to_value;

use crate::error::Error;
use crate::error::ErrorCode;
use crate::object::PickleObject;

static NEXT_SHARED_ID: AtomicU64 = AtomicU64::new(0);

fn next_id() -> u64 {
    NEXT_SHARED_ID.fetch_add(1, AtomicOrdering::Relaxed)
}

pub struct Shared<T>(Rc<RefCell<T>>, u64);

impl<T> Clone for Shared<T> {
    fn clone(&self) -> Self {
        Shared(Rc::clone(&self.0), self.1)
    }
}

impl<T> Shared<T> {
    pub fn new(value: T) -> Self {
        Shared(Rc::new(RefCell::new(value)), next_id())
    }

    pub fn inner<'a>(&'a self) -> Ref<'a, T> {
        self.0.borrow()
    }

    pub fn inner_mut<'a>(&'a self) -> RefMut<'a, T> {
        self.0.borrow_mut()
    }

    /// Returns true if two `Shared` values point to the same allocation.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }

    pub(crate) fn stable_id(&self) -> u64 {
        self.1
    }

    /// Returns the raw pointer to the inner `RefCell<T>` for identity comparison.
    pub fn rc_ptr(&self) -> *const T {
        self.0.as_ptr()
    }
}

impl<T: fmt::Debug> fmt::Debug for Shared<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Shared(")?;
        self.0.fmt(f)?;
        write!(f, ")")
    }
}

impl<T> From<T> for Shared<T> {
    fn from(value: T) -> Self {
        Shared::new(value)
    }
}

impl<T> From<SharedFrozen<T>> for Shared<T>
where
    T: Clone,
{
    fn from(value: SharedFrozen<T>) -> Self {
        Shared::new(value.into_raw_or_cloned())
    }
}

impl<T> Shared<T>
where
    T: Clone,
{
    pub fn into_raw_or_cloned(self) -> T {
        if Rc::strong_count(&self.0) == 1 {
            if let Some(inner) = Rc::into_inner(self.0) {
                RefCell::into_inner(inner)
            } else {
                panic!("TOCTOU while trying to serialize Shared")
            }
        } else {
            self.0.borrow().clone()
        }
    }
}

impl<T> std::cmp::PartialEq for Shared<T>
where
    T: std::cmp::PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        if Rc::ptr_eq(&self.0, &other.0) {
            return true;
        }

        let this_inner = self.0.borrow();
        let other_inner = other.0.borrow();

        this_inner.eq(&other_inner)
    }
}

impl<T> Eq for Shared<T> where T: Eq {}

impl<T> PartialOrd for Shared<T>
where
    T: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.borrow().partial_cmp(&*other.0.borrow())
    }
}

impl<T> Ord for Shared<T>
where
    T: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.borrow().cmp(&*other.0.borrow())
    }
}

#[derive(Clone)]
pub struct SharedFrozen<T>(Rc<T>, u64);

impl<T> SharedFrozen<T> {
    pub fn new(value: T) -> Self {
        SharedFrozen(Rc::new(value), next_id())
    }

    pub fn inner(&self) -> &T {
        self.0.as_ref()
    }

    /// Returns true if two `SharedFrozen` values point to the same allocation.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }

    pub(crate) fn stable_id(&self) -> u64 {
        self.1
    }

    /// Returns a reference to the inner `Rc<T>`.
    pub fn rc_ref(&self) -> &Rc<T> {
        &self.0
    }
}

impl<T: fmt::Debug> fmt::Debug for SharedFrozen<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SharedFrozen(")?;
        self.0.fmt(f)?;
        write!(f, ")")
    }
}

impl<T> From<T> for SharedFrozen<T> {
    fn from(value: T) -> Self {
        SharedFrozen::new(value)
    }
}

impl<T> From<Shared<T>> for SharedFrozen<T>
where
    T: Clone,
{
    fn from(value: Shared<T>) -> Self {
        SharedFrozen::new(value.into_raw_or_cloned())
    }
}

impl<T> SharedFrozen<T>
where
    T: Clone,
{
    pub fn into_raw_or_cloned(self) -> T {
        if Rc::strong_count(&self.0) == 1 {
            if let Some(inner) = Rc::into_inner(self.0) {
                inner
            } else {
                panic!("TOCTOU while trying to serialize Shared")
            }
        } else {
            self.inner().clone()
        }
    }
}

impl<T> std::cmp::PartialEq for SharedFrozen<T>
where
    T: std::cmp::PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        if Rc::ptr_eq(&self.0, &other.0) {
            return true;
        }

        let this_inner = self.inner();
        let other_inner = other.inner();

        this_inner.eq(other_inner)
    }
}

impl<T> Eq for SharedFrozen<T> where T: Eq {}

impl<T> PartialOrd for SharedFrozen<T>
where
    T: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.inner().partial_cmp(other.inner())
    }
}

impl<T> Ord for SharedFrozen<T>
where
    T: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.inner().cmp(other.inner())
    }
}

/// A Python dictionary: `(key, value)` entries kept **sorted by key**.
///
/// Backed by a flat `Vec` rather than a `BTreeMap`. Pickle dict and
/// object-state maps are overwhelmingly tiny, and a `BTreeMap` allocates a
/// fixed-capacity node per map regardless of how few entries it holds, so for
/// the many small maps in a typical pickle a sorted `Vec` is several times
/// tighter in memory. Lookups stay `O(log n)` via binary search.
///
/// # Invariant
///
/// Entries are always sorted by key (using `HashableValue`'s `Ord`) with no
/// duplicate keys. Every constructor and mutator upholds this, so iteration
/// yields entries in key order -- identical to the order a `BTreeMap` with the
/// same keys would produce. `get`/`contains_key` rely on this ordering.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Dict {
    entries: Vec<(HashableValue, Value)>,
}

impl Dict {
    pub fn new() -> Self {
        Dict {
            entries: Vec::new(),
        }
    }

    /// Wrap an already-sorted, duplicate-free entry vector without re-checking.
    /// The caller must guarantee the sort-by-key invariant.
    pub fn from_sorted_unchecked(entries: Vec<(HashableValue, Value)>) -> Self {
        debug_assert!(
            entries.windows(2).all(|w| w[0].0 < w[1].0),
            "Dict::from_sorted_unchecked given unsorted entries"
        );
        Dict { entries }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn position(&self, key: &HashableValue) -> std::result::Result<usize, usize> {
        self.entries.binary_search_by(|(k, _)| k.cmp(key))
    }

    pub fn get(&self, key: &HashableValue) -> Option<&Value> {
        self.position(key).ok().map(|i| &self.entries[i].1)
    }

    pub fn get_mut(&mut self, key: &HashableValue) -> Option<&mut Value> {
        match self.position(key) {
            Ok(i) => Some(&mut self.entries[i].1),
            Err(_) => None,
        }
    }

    pub fn contains_key(&self, key: &HashableValue) -> bool {
        self.position(key).is_ok()
    }

    /// Insert a key/value pair, keeping entries sorted. Returns the previous
    /// value if the key was already present.
    pub fn insert(&mut self, key: HashableValue, value: Value) -> Option<Value> {
        match self.position(&key) {
            Ok(i) => Some(std::mem::replace(&mut self.entries[i].1, value)),
            Err(i) => {
                self.entries.insert(i, (key, value));
                None
            }
        }
    }

    /// Bulk-insert a batch of entries (e.g. a whole pickle `SETITEMS`), keeping
    /// the dict sorted with last-wins on duplicate keys. When the dict starts
    /// empty -- the common case for a freshly built pickle dict -- the batch is
    /// sorted once (`O(n log n)`) and taken directly, avoiding `O(n^2)` inserts.
    pub fn extend_sorted(&mut self, mut batch: Vec<(HashableValue, Value)>) {
        if batch.is_empty() {
            return;
        }
        if self.entries.is_empty() {
            batch.sort_by(|a, b| a.0.cmp(&b.0));
            batch.dedup_by(|later, kept| {
                later.0.cmp(&kept.0) == Ordering::Equal && {
                    std::mem::swap(&mut kept.1, &mut later.1);
                    true
                }
            });
            self.entries = batch;
        } else {
            for (k, v) in batch {
                self.insert(k, v);
            }
        }
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&HashableValue, &Value)> {
        self.entries.iter().map(|(k, v)| (k, v))
    }

    pub fn keys(&self) -> impl ExactSizeIterator<Item = &HashableValue> {
        self.entries.iter().map(|(k, _)| k)
    }

    pub fn values(&self) -> impl ExactSizeIterator<Item = &Value> {
        self.entries.iter().map(|(_, v)| v)
    }

    pub fn values_mut(&mut self) -> impl ExactSizeIterator<Item = &mut Value> {
        self.entries.iter_mut().map(|(_, v)| v)
    }

    /// The backing entries as a sorted slice.
    pub fn as_slice(&self) -> &[(HashableValue, Value)] {
        &self.entries
    }

    pub fn into_entries(self) -> Vec<(HashableValue, Value)> {
        self.entries
    }
}

impl FromIterator<(HashableValue, Value)> for Dict {
    fn from_iter<I: IntoIterator<Item = (HashableValue, Value)>>(iter: I) -> Self {
        let mut entries: Vec<(HashableValue, Value)> = iter.into_iter().collect();
        // Stable sort by key, then drop duplicate keys keeping the last value
        // written (matches repeated `BTreeMap::insert`). Dedup compares with
        // `Ord` (not `PartialEq`) so it collapses exactly the keys that sort and
        // binary-search treat as equal -- `HashableValue`'s cross-type numeric
        // ordering makes e.g. `Bool(true)` and `Int(1)` `Ordering::Equal`.
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries.dedup_by(|later, kept| {
            later.0.cmp(&kept.0) == Ordering::Equal && {
                std::mem::swap(&mut kept.1, &mut later.1);
                true
            }
        });
        Dict { entries }
    }
}

impl IntoIterator for Dict {
    type Item = (HashableValue, Value);
    type IntoIter = std::vec::IntoIter<(HashableValue, Value)>;
    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl<'a> IntoIterator for &'a Dict {
    type Item = (&'a HashableValue, &'a Value);
    type IntoIter = std::iter::Map<
        std::slice::Iter<'a, (HashableValue, Value)>,
        fn(&'a (HashableValue, Value)) -> (&'a HashableValue, &'a Value),
    >;
    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter().map(|(k, v)| (k, v))
    }
}

impl Extend<(HashableValue, Value)> for Dict {
    fn extend<I: IntoIterator<Item = (HashableValue, Value)>>(&mut self, iter: I) {
        for (k, v) in iter {
            self.insert(k, v);
        }
    }
}

/// Represents all primitive builtin Python values that can be restored by
/// unpickling.
///
/// Note on integers: the distinction between the two types (short and long) is
/// very fuzzy in Python, and they can be used interchangeably.  In Python 3,
/// all integers are long integers, so all are pickled as such.  While decoding,
/// we simply put all integers that fit into an i64, and use `BigInt` for the
/// rest.
#[derive(Debug)]
pub enum Value {
    /// None
    None,
    /// Boolean
    Bool(bool),
    /// Short integer
    I64(i64),
    /// Long integer (unbounded length)
    Int(Box<BigInt>),
    /// Float
    F64(f64),
    /// Bytestring
    Bytes(SharedFrozen<Vec<u8>>),
    /// Unicode string
    String(SharedFrozen<String>),
    /// List
    List(Shared<Vec<Value>>),
    /// Tuple
    Tuple(SharedFrozen<Vec<Value>>),
    /// Set
    Set(Shared<BTreeSet<HashableValue>>),
    /// Frozen (immutable) set
    FrozenSet(SharedFrozen<BTreeSet<HashableValue>>),
    /// Dictionary (map), stored as sorted entries (see [`Dict`]).
    Dict(Shared<Dict>),
    /// Python object reconstructed during unpickling
    Object(Shared<Box<dyn PickleObject>>),
}

impl Clone for Value {
    fn clone(&self) -> Self {
        match self {
            Value::None => Value::None,
            Value::Bool(b) => Value::Bool(*b),
            Value::I64(i) => Value::I64(*i),
            Value::Int(i) => Value::Int(i.clone()),
            Value::F64(f) => Value::F64(*f),
            Value::Bytes(b) => Value::Bytes(b.clone()),
            Value::String(s) => Value::String(s.clone()),
            Value::List(l) => Value::List(l.clone()),
            Value::Tuple(t) => Value::Tuple(t.clone()),
            Value::Set(s) => Value::Set(s.clone()),
            Value::FrozenSet(s) => Value::FrozenSet(s.clone()),
            Value::Dict(d) => Value::Dict(d.clone()),
            Value::Object(o) => Value::Object(o.clone()),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::None, Value::None) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::I64(a), Value::I64(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::I64(a), Value::Int(b)) => BigInt::from(*a) == **b,
            (Value::Int(a), Value::I64(b)) => **a == BigInt::from(*b),
            (Value::F64(a), Value::F64(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::List(a), Value::List(b)) => a == b,
            (Value::Tuple(a), Value::Tuple(b)) => a == b,
            (Value::Set(a), Value::Set(b)) => a == b,
            (Value::FrozenSet(a), Value::FrozenSet(b)) => a == b,
            (Value::Dict(a), Value::Dict(b)) => a == b,
            (Value::Object(a), Value::Object(b)) => {
                a.ptr_eq(b) || a.inner().eq_dyn(b.inner().as_ref())
            }
            _ => false,
        }
    }
}

variant_accessors!(Value {
    tuple Bool(bool) => bool;
    tuple I64(i64) => i64;
    tuple Int(Box<BigInt>) => int;
    tuple F64(f64) => f64;
    tuple Bytes(SharedFrozen<Vec<u8>>) => bytes;
    tuple String(SharedFrozen<String>) => string;
    tuple List(Shared<Vec<Value>>) => list;
    tuple Tuple(SharedFrozen<Vec<Value>>) => tuple;
    tuple Set(Shared<BTreeSet<HashableValue>>) => set;
    tuple FrozenSet(SharedFrozen<BTreeSet<HashableValue>>) => frozen_set;
    tuple Dict(Shared<Dict>) => dict;
    tuple Object(Shared<Box<dyn PickleObject>>) => object;
    unit None => none;
});

/// Represents all primitive builtin Python values that can be contained
/// in a "hashable" context (i.e., as dictionary keys and set elements).
///
/// In Rust, the type is *not* hashable, since we use B-tree maps and sets
/// instead of the hash variants.  To be able to put all Value instances
/// into these B-trees, we implement a consistent ordering between all
/// the possible types (see below).
#[derive(Clone, Debug)]
pub enum HashableValue {
    /// None
    None,
    /// Boolean
    Bool(bool),
    /// Short integer
    I64(i64),
    /// Long integer
    Int(Box<BigInt>),
    /// Float
    F64(f64),
    /// Bytestring
    Bytes(SharedFrozen<Vec<u8>>),
    /// Unicode string
    String(SharedFrozen<String>),
    /// Tuple
    Tuple(SharedFrozen<Vec<HashableValue>>),
    /// Frozen (immutable) set
    FrozenSet(SharedFrozen<BTreeSet<HashableValue>>),
}

variant_accessors!(HashableValue {
    tuple Bool(bool) => bool;
    tuple I64(i64) => i64;
    tuple Int(Box<BigInt>) => int;
    tuple F64(f64) => f64;
    tuple Bytes(SharedFrozen<Vec<u8>>) => bytes;
    tuple String(SharedFrozen<String>) => string;
    tuple Tuple(SharedFrozen<Vec<HashableValue>>) => tuple;
    tuple FrozenSet(SharedFrozen<BTreeSet<HashableValue>>) => frozen_set;
    unit None => none;
});

fn values_to_raw_hashable(
    values: SharedFrozen<Vec<Value>>,
) -> Result<SharedFrozen<Vec<RawHashableValue>>, Error> {
    Ok(values
        .inner()
        .iter()
        .cloned()
        .map(Value::into_raw_hashable)
        .collect::<Result<Vec<_>, _>>()?
        .into())
}

fn values_to_hashable(
    values: SharedFrozen<Vec<Value>>,
) -> Result<SharedFrozen<Vec<HashableValue>>, Error> {
    Ok(values
        .inner()
        .iter()
        .cloned()
        .map(Value::into_hashable)
        .collect::<Result<Vec<_>, _>>()?
        .into())
}

fn hashable_to_values(values: SharedFrozen<Vec<HashableValue>>) -> SharedFrozen<Vec<Value>> {
    values
        .inner()
        .iter()
        .cloned()
        .map(HashableValue::into_value)
        .collect::<Vec<_>>()
        .into()
}

impl Value {
    /// Convert the value into a hashable version, if possible.  If not, return
    /// a ValueNotHashable error.
    pub fn into_hashable(self) -> Result<HashableValue, Error> {
        match self {
            Value::None => Ok(HashableValue::None),
            Value::Bool(b) => Ok(HashableValue::Bool(b)),
            Value::I64(i) => Ok(HashableValue::I64(i)),
            Value::Int(i) => Ok(HashableValue::Int(i)),
            Value::F64(f) => Ok(HashableValue::F64(f)),
            Value::Bytes(b) => Ok(HashableValue::Bytes(b)),
            Value::String(s) => Ok(HashableValue::String(s)),
            Value::FrozenSet(v) => Ok(HashableValue::FrozenSet(v)),
            Value::Tuple(v) => values_to_hashable(v).map(HashableValue::Tuple),
            Value::Object(o) => o.inner().__hash__(),
            _ => Err(Error::Syntax(ErrorCode::ValueNotHashable)),
        }
    }

    /// Returns true if this value or any nested value contains a NaN float.
    pub fn contains_nan(&self) -> bool {
        match self {
            Value::F64(f) => f.is_nan(),
            Value::List(v) => v.inner().iter().any(|v| v.contains_nan()),
            Value::Tuple(v) => v.inner().iter().any(|v| v.contains_nan()),
            Value::Set(v) => v.inner().iter().any(|v| v.contains_nan()),
            Value::FrozenSet(v) => v.inner().iter().any(|v| v.contains_nan()),
            Value::Dict(v) => v
                .inner()
                .iter()
                .any(|(k, v)| k.contains_nan() || v.contains_nan()),
            _ => false,
        }
    }

    pub(crate) fn into_raw_hashable(self) -> Result<RawHashableValue, Error> {
        match self {
            Value::None => Ok(RawHashableValue::None),
            Value::Bool(b) => Ok(RawHashableValue::Bool(b)),
            Value::I64(i) => Ok(RawHashableValue::I64(i)),
            Value::Int(i) => Ok(RawHashableValue::Int(i)),
            Value::F64(f) => Ok(RawHashableValue::F64(f)),
            Value::Bytes(b) => Ok(RawHashableValue::Bytes(b)),
            Value::String(s) => Ok(RawHashableValue::String(s)),
            Value::FrozenSet(v) => {
                let v = v.inner();
                let new = BTreeSet::from_iter(v.iter().cloned().map(|v| {
                    v.into_value()
                        .into_raw_hashable()
                        .expect("failed to round-trip")
                }));

                Ok(RawHashableValue::FrozenSet(SharedFrozen::new(new)))
            }
            Value::Tuple(v) => values_to_raw_hashable(v).map(RawHashableValue::Tuple),
            Value::Object(o) => o.inner().__hash__()?.into_value().into_raw_hashable(),
            _ => Err(Error::Syntax(ErrorCode::ValueNotHashable)),
        }
    }
}

impl HashableValue {
    /// Returns true if this value or any nested value contains a NaN float.
    pub fn contains_nan(&self) -> bool {
        match self {
            HashableValue::F64(f) => f.is_nan(),
            HashableValue::Tuple(v) => v.inner().iter().any(|v| v.contains_nan()),
            HashableValue::FrozenSet(v) => v.inner().iter().any(|v| v.contains_nan()),
            _ => false,
        }
    }

    /// Convert the value into its non-hashable version.  This always works.
    pub fn into_value(self) -> Value {
        match self {
            HashableValue::None => Value::None,
            HashableValue::Bool(b) => Value::Bool(b),
            HashableValue::I64(i) => Value::I64(i),
            HashableValue::Int(i) => Value::Int(i),
            HashableValue::F64(f) => Value::F64(f),
            HashableValue::Bytes(b) => Value::Bytes(b),
            HashableValue::String(s) => Value::String(s),
            HashableValue::FrozenSet(v) => Value::FrozenSet(v),
            HashableValue::Tuple(v) => Value::Tuple(hashable_to_values(v)),
        }
    }

    /// Returns a value that's suitable for use as a string key
    pub fn to_string_key(&self) -> Option<Cow<'static, str>> {
        let result = match *self {
            HashableValue::String(ref s) => Cow::Owned(s.inner().to_owned()),
            HashableValue::None => Cow::Borrowed("null"),
            HashableValue::Bool(b) => Cow::Owned(b.to_string()),
            HashableValue::I64(i) => Cow::Owned(i.to_string()),
            HashableValue::Int(ref big_int) => Cow::Owned(big_int.to_string()),
            HashableValue::F64(f) => {
                let mut as_str = f.to_string();
                if !as_str.contains('.') {
                    as_str += ".0";
                }

                Cow::Owned(as_str)
            }
            _ => {
                // All other key types are invalid
                return None;
            }
        };

        Some(result)
    }
}

fn write_elements<'a, I, T>(
    f: &mut fmt::Formatter,
    it: I,
    prefix: &'static str,
    suffix: &'static str,
    len: usize,
    always_comma: bool,
) -> fmt::Result
where
    I: Iterator<Item = &'a T>,
    T: fmt::Display + 'a,
{
    f.write_str(prefix)?;
    for (i, item) in it.enumerate() {
        if i < len - 1 || always_comma {
            write!(f, "{item}, ")?;
        } else {
            write!(f, "{item}")?;
        }
    }
    f.write_str(suffix)
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Value::None => write!(f, "None"),
            Value::Bool(b) => write!(f, "{}", if b { "True" } else { "False" }),
            Value::I64(i) => write!(f, "{i}"),
            Value::Int(ref i) => write!(f, "{i}"),
            Value::F64(v) => write!(f, "{v}"),
            Value::Bytes(ref b) => write!(f, "b{b:?}"),
            Value::String(ref s) => write!(f, "{s:?}"),
            Value::List(ref v) => {
                let v = v.inner();
                write_elements(f, v.iter(), "[", "]", v.len(), false)
            }
            Value::Tuple(ref v) => {
                let v = v.inner();
                write_elements(f, v.iter(), "(", ")", v.len(), v.len() == 1)
            }
            Value::FrozenSet(ref v) => {
                let v = v.inner();
                write_elements(f, v.iter(), "frozenset([", "])", v.len(), false)
            }
            Value::Set(ref v) => {
                let v = v.inner();
                if v.is_empty() {
                    write!(f, "set()")
                } else {
                    write_elements(f, v.iter(), "{", "}", v.len(), false)
                }
            }
            Value::Dict(ref v) => {
                write!(f, "{{")?;
                let v = v.inner();
                for (i, (key, value)) in v.iter().enumerate() {
                    if i < v.len() - 1 {
                        write!(f, "{key}: {value}, ")?;
                    } else {
                        write!(f, "{key}: {value}")?;
                    }
                }
                write!(f, "}}")
            }
            Value::Object(ref o) => write!(f, "{}", o.inner()),
        }
    }
}

impl fmt::Display for HashableValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            HashableValue::None => write!(f, "None"),
            HashableValue::Bool(b) => write!(f, "{}", if b { "True" } else { "False" }),
            HashableValue::I64(i) => write!(f, "{i}"),
            HashableValue::Int(ref i) => write!(f, "{i}"),
            HashableValue::F64(v) => write!(f, "{v}"),
            HashableValue::Bytes(ref b) => {
                let b = b.inner();
                write!(f, "b{b:?}")
            }
            HashableValue::String(ref s) => {
                let s = s.inner();
                write!(f, "{s:?}")
            }
            HashableValue::Tuple(ref v) => {
                let v = v.inner();
                write_elements(f, v.iter(), "(", ")", v.len(), v.len() == 1)
            }
            HashableValue::FrozenSet(ref v) => {
                let v = v.inner();
                write_elements(f, v.iter(), "frozenset([", "])", v.len(), false)
            }
        }
    }
}

impl PartialEq for HashableValue {
    fn eq(&self, other: &HashableValue) -> bool {
        use self::HashableValue::*;
        match (self, other) {
            (None, None) => true,
            (Bool(a), Bool(b)) => a == b,
            // Cross-type numeric equality, matching Python semantics
            (Bool(a), I64(b)) => (*a as i64) == *b,
            (I64(a), Bool(b)) => *a == (*b as i64),
            (I64(a), I64(b)) => a == b,
            (I64(a), Int(b)) => BigInt::from(*a) == **b,
            (Int(a), I64(b)) => **a == BigInt::from(*b),
            (Int(a), Int(b)) => a == b,
            // Float comparisons use IEEE 754 semantics (NaN != NaN, -0.0 == 0.0)
            (F64(a), F64(b)) => a == b,
            (Bool(a), F64(b)) => (*a as i64 as f64) == *b,
            (F64(a), Bool(b)) => *a == (*b as i64 as f64),
            // Use exact comparison via float_bigint_ord to avoid f64 precision loss
            (I64(a), F64(b)) => float_bigint_ord(&BigInt::from(*a), *b) == Ordering::Equal,
            (F64(a), I64(b)) => float_bigint_ord(&BigInt::from(*b), *a) == Ordering::Equal,
            (Int(a), F64(b)) => float_bigint_ord(a, *b) == Ordering::Equal,
            (F64(a), Int(b)) => float_bigint_ord(b, *a) == Ordering::Equal,
            (Bytes(a), Bytes(b)) => a == b,
            (String(a), String(b)) => a == b,
            (FrozenSet(a), FrozenSet(b)) => a == b,
            (Tuple(a), Tuple(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for HashableValue {}

impl PartialOrd for HashableValue {
    fn partial_cmp(&self, other: &HashableValue) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Implement a (more or less) consistent ordering for `HashableValue`s
/// so that they can be added to dictionaries and sets.
///
/// Also, like in Python, numeric values with the same value (integral or not)
/// must compare equal.
///
/// For other types, we define an ordering between all types A and B so that all
/// objects of type A are always lesser than objects of type B.  This is done
/// similar to Python 2's ordering of different types.
impl Ord for HashableValue {
    fn cmp(&self, other: &HashableValue) -> Ordering {
        use self::HashableValue::*;
        match *self {
            None => match *other {
                None => Ordering::Equal,
                _ => Ordering::Less,
            },
            Bool(b) => match *other {
                None => Ordering::Greater,
                Bool(b2) => b.cmp(&b2),
                I64(i2) => (b as i64).cmp(&i2),
                Int(ref bi) => BigInt::from(b as i64).cmp(bi.as_ref()),
                F64(f) => float_ord(b as i64 as f64, f),
                _ => Ordering::Less,
            },
            I64(i) => match *other {
                None => Ordering::Greater,
                Bool(b) => i.cmp(&(b as i64)),
                I64(i2) => i.cmp(&i2),
                Int(ref bi) => BigInt::from(i).cmp(bi.as_ref()),
                F64(f) => float_bigint_ord(&BigInt::from(i), f),
                _ => Ordering::Less,
            },
            Int(ref bi) => match *other {
                None => Ordering::Greater,
                Bool(b) => bi.as_ref().cmp(&BigInt::from(b as i64)),
                I64(i) => bi.as_ref().cmp(&BigInt::from(i)),
                Int(ref bi2) => bi.cmp(bi2),
                F64(f) => float_bigint_ord(bi, f),
                _ => Ordering::Less,
            },
            F64(f) => match *other {
                None => Ordering::Greater,
                Bool(b) => float_ord(f, b as i64 as f64),
                I64(i) => float_bigint_ord(&BigInt::from(i), f).reverse(),
                Int(ref bi) => float_bigint_ord(bi, f).reverse(),
                F64(f2) => float_ord(f, f2),
                _ => Ordering::Less,
            },
            Bytes(ref bs) => match *other {
                String(_) | FrozenSet(_) | Tuple(_) => Ordering::Less,
                Bytes(ref bs2) => bs.cmp(bs2),
                _ => Ordering::Greater,
            },
            String(ref s) => match *other {
                FrozenSet(_) | Tuple(_) => Ordering::Less,
                String(ref s2) => s.cmp(s2),
                _ => Ordering::Greater,
            },
            FrozenSet(ref s) => match *other {
                Tuple(_) => Ordering::Less,
                FrozenSet(ref s2) => s.cmp(s2),
                _ => Ordering::Greater,
            },
            Tuple(ref t) => match *other {
                Tuple(ref t2) => t.cmp(t2),
                _ => Ordering::Greater,
            },
        }
    }
}

/// Total ordering for floats. Uses IEEE comparison (-0.0 == 0.0) with
/// NaN sorted after all other values for BTreeSet consistency.
fn float_ord(f: f64, g: f64) -> Ordering {
    match f.partial_cmp(&g) {
        Some(ord) => ord,
        None => match (f.is_nan(), g.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => unreachable!(),
        },
    }
}

/// Alias used by the derived Ord for RawHashableValue.
fn total_float_ord(f: f64, g: f64) -> Ordering {
    float_ord(f, g)
}

/// Exact ordering between a BigInt and an f64, matching CPython's
/// float_richcompare algorithm.
fn float_bigint_ord(bi: &BigInt, f: f64) -> Ordering {
    // If f is NaN or infinity, the int's magnitude is irrelevant.
    if !f.is_finite() {
        // int < +inf, int < NaN, int > -inf
        return if f < 0.0 {
            Ordering::Greater
        } else {
            Ordering::Less
        };
    }

    let fsign: i32 = if f == 0.0 {
        0
    } else if f < 0.0 {
        -1
    } else {
        1
    };
    let isign: i32 = match bi.sign() {
        Sign::Minus => -1,
        Sign::Plus => 1,
        Sign::NoSign => 0,
    };

    // Different signs determine the outcome immediately.
    if isign != fsign {
        return isign.cmp(&fsign);
    }

    // Both zero.
    if isign == 0 {
        return Ordering::Equal;
    }

    // Same sign. Compare bit counts.
    let nbits = bi.bits();

    if nbits <= 48 {
        // Safe to convert to f64 without precision loss.
        if let Some(bi_f) = bi.to_f64() {
            return bi_f.partial_cmp(&f).unwrap_or(Ordering::Equal);
        }
    }

    // Use integer_decode to get the exact mantissa and exponent.
    // f = sign * mantissa * 2^exponent, where mantissa is a u64.
    let (mantissa, exponent, _sign) = Float::integer_decode(f.abs());

    // Number of significant bits in the mantissa.
    let mantissa_bits = 64 - mantissa.leading_zeros() as i64;
    // Number of bits before the radix point in the float.
    let float_nbits = mantissa_bits + exponent as i64;

    if float_nbits < nbits as i64 {
        // Float has fewer bits; int is larger in magnitude.
        return if isign > 0 {
            Ordering::Greater
        } else {
            Ordering::Less
        };
    }
    if float_nbits > nbits as i64 {
        // Float has more bits; float is larger in magnitude.
        return if isign > 0 {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }

    // Same number of bits. Construct exact BigInt representation of the float
    // and compare. f = mantissa * 2^exponent (for the absolute value).
    let float_as_bigint = if exponent >= 0 {
        BigInt::from(mantissa) << exponent as u64
    } else {
        // Float has a fractional part. Shift the int up instead so we
        // compare mantissa vs (int << -exponent), avoiding fractions.
        let shift = (-exponent) as u64;
        let ww = bi.abs() << shift;
        // We want bi.cmp(f), i.e. (int << shift).cmp(mantissa)
        let magnitude_ord = ww.cmp(&BigInt::from(mantissa));
        return if isign < 0 {
            magnitude_ord.reverse()
        } else {
            magnitude_ord
        };
    };

    // We want bi.cmp(f), i.e. bi.abs().cmp(float_as_bigint)
    let magnitude_ord = bi.abs().cmp(&float_as_bigint);
    if isign < 0 {
        magnitude_ord.reverse()
    } else {
        magnitude_ord
    }
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum RawHashableValue {
    /// None
    None,
    /// Boolean
    Bool(bool),
    /// Short integer
    I64(i64),
    /// Long integer
    Int(Box<BigInt>),
    /// Float
    F64(f64),
    /// Bytestring
    Bytes(SharedFrozen<Vec<u8>>),
    /// Unicode string
    String(SharedFrozen<String>),
    /// Tuple
    Tuple(SharedFrozen<Vec<RawHashableValue>>),
    /// Frozen (immutable) set
    FrozenSet(SharedFrozen<BTreeSet<RawHashableValue>>),
}

impl std::cmp::Eq for RawHashableValue {}

impl std::cmp::PartialOrd for RawHashableValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::Ord for RawHashableValue {
    #[inline]
    fn cmp(&self, other: &RawHashableValue) -> ::core::cmp::Ordering {
        let __self_discr = match self {
            RawHashableValue::None => 0,
            RawHashableValue::Bool(_) => 1,
            RawHashableValue::I64(_) => 2,
            RawHashableValue::Int(_) => 3,
            RawHashableValue::F64(_) => 4,
            RawHashableValue::Bytes(_) => 5,
            RawHashableValue::String(_) => 6,
            RawHashableValue::Tuple(_) => 7,
            RawHashableValue::FrozenSet(_) => 8,
        };
        let __arg1_discr = match other {
            RawHashableValue::None => 0,
            RawHashableValue::Bool(_) => 1,
            RawHashableValue::I64(_) => 2,
            RawHashableValue::Int(_) => 3,
            RawHashableValue::F64(_) => 4,
            RawHashableValue::Bytes(_) => 5,
            RawHashableValue::String(_) => 6,
            RawHashableValue::Tuple(_) => 7,
            RawHashableValue::FrozenSet(_) => 8,
        };

        match ::core::cmp::Ord::cmp(&__self_discr, &__arg1_discr) {
            ::core::cmp::Ordering::Equal => match (self, other) {
                (RawHashableValue::Bool(__self_0), RawHashableValue::Bool(__arg1_0)) => {
                    ::core::cmp::Ord::cmp(__self_0, __arg1_0)
                }
                (RawHashableValue::I64(__self_0), RawHashableValue::I64(__arg1_0)) => {
                    ::core::cmp::Ord::cmp(__self_0, __arg1_0)
                }
                (RawHashableValue::Int(__self_0), RawHashableValue::Int(__arg1_0)) => {
                    ::core::cmp::Ord::cmp(__self_0, __arg1_0)
                }
                (RawHashableValue::Bytes(__self_0), RawHashableValue::Bytes(__arg1_0)) => {
                    ::core::cmp::Ord::cmp(__self_0, __arg1_0)
                }
                (RawHashableValue::String(__self_0), RawHashableValue::String(__arg1_0)) => {
                    ::core::cmp::Ord::cmp(__self_0, __arg1_0)
                }
                (RawHashableValue::Tuple(__self_0), RawHashableValue::Tuple(__arg1_0)) => {
                    ::core::cmp::Ord::cmp(__self_0, __arg1_0)
                }
                (RawHashableValue::FrozenSet(__self_0), RawHashableValue::FrozenSet(__arg1_0)) => {
                    ::core::cmp::Ord::cmp(__self_0, __arg1_0)
                }
                (RawHashableValue::F64(__self_0), RawHashableValue::F64(__self_1)) => {
                    total_float_ord(*__self_0, *__self_1)
                }
                _ => ::core::cmp::Ordering::Equal,
            },
            cmp => cmp,
        }
    }
}
