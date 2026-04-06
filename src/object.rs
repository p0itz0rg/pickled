//! Trait for custom Python objects reconstructed during unpickling.

use std::any::Any;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;

use crate::error::Error;
use crate::error::ErrorCode;
use crate::value::HashableValue;
use crate::value::Shared;
use crate::value::SharedFrozen;
use crate::value::Value;

/// Result of `__reduce__` — describes how to serialize/reconstruct a Python object.
///
/// Mirrors Python's `__reduce__` protocol. The pickle serializer emits:
/// 1. `GLOBAL module.class` + `args` + `NEWOBJ` — to recreate the object
/// 2. `state` + `BUILD` — to restore state (if `state` is `Some`)
/// 3. `APPENDS` — to append list items (if `list_items` is `Some`)
/// 4. `SETITEMS` — to set dict items (if `dict_items` is `Some`)
pub struct ReduceResult {
    /// Python module name (e.g. `"__main__"`).
    pub module: String,
    /// Python class name (e.g. `"MyClass"`).
    pub class: String,
    /// Constructor arguments. Typically an empty tuple for `NEWOBJ`.
    pub args: Value,
    /// Object state for `BUILD` opcode. `None` means no `BUILD` is emitted.
    pub state: Option<Value>,
    /// List items to append via `APPENDS`. `None` means no `APPENDS` are emitted.
    pub list_items: Option<Vec<Value>>,
    /// Dict items to set via `SETITEMS`. `None` means no `SETITEMS` are emitted.
    pub dict_items: Option<Vec<(HashableValue, Value)>>,
}

impl ReduceResult {
    /// Returns the state as a `Value`, or `Value::None` if there is no state.
    /// Useful for non-pickle serialization contexts (JSON, serde, etc.) where
    /// only the object's data matters.
    pub fn state_or_none(&self) -> Value {
        self.state.clone().unwrap_or(Value::None)
    }
}

/// Trait for custom Python objects reconstructed during unpickling.
///
/// Method names mirror Python's dunder protocol where applicable.
/// Methods suffixed with `_dyn` are object-safe plumbing for Rust trait dispatch.
pub trait PickleObject: fmt::Debug + fmt::Display {
    /// `__setstate__`: called by the BUILD opcode to restore object state.
    ///
    /// In CPython, BUILD first checks for `__setstate__` — if present, it calls
    /// `__setstate__(state)` and returns. Otherwise it falls back to updating
    /// `__dict__` directly and applying slot state via `setattr`.
    ///
    /// The `state` is typically a Dict (for `__dict__` update), or a 2-tuple
    /// `(dict_state, slot_state)` when the object has `__slots__`.
    fn __setstate__(&mut self, _state: Value) {}

    /// `__setitem__`: called by SETITEM/SETITEMS opcodes.
    ///
    /// In CPython, both opcodes use `PyObject_SetItem(dict, key, value)` which
    /// dispatches to `__setitem__` if present on the object.
    fn __setitem__(&mut self, _key: Value, _value: Value) {}

    /// `__hash__`: convert to a hashable representation for use as a dict key
    /// or set member. Return Err if this object is not hashable.
    fn __hash__(&self) -> Result<HashableValue, Error> {
        Err(Error::Syntax(ErrorCode::ValueNotHashable))
    }

    /// The module and class name this object was constructed from.
    fn class_info(&self) -> (&str, &str);

    /// `__reduce__`: describes how to serialize/reconstruct this object.
    ///
    /// In CPython, `__reduce__` returns a 2-5 tuple that the pickler uses to
    /// emit GLOBAL + NEWOBJ/REDUCE + BUILD + APPENDS + SETITEMS opcodes.
    /// See [`ReduceResult`] for details.
    fn __reduce__(&self) -> ReduceResult;

    /// Object-safe equality. Implementors should downcast via `as_any()`.
    ///
    /// Required to implement as the concrete types for `cmp/eq/etc.` aren't known,
    /// so forcing `PickleObject: Cmp + Eq + ...` doesn't really work for our use-case.
    fn eq_dyn(&self, other: &dyn PickleObject) -> bool;

    /// Object-safe ordering. Implementors should downcast via `as_any()`,
    /// falling back to `class_info()` comparison for different types.
    ///
    /// Required to implement as the concrete types for `cmp/eq/etc.` aren't known,
    /// so forcing `PickleObject: Cmp + Eq + ...` doesn't really work for our use-case.
    fn cmp_dyn(&self, other: &dyn PickleObject) -> Ordering;

    /// Object-safe clone.
    fn clone_dyn(&self) -> Box<dyn PickleObject>;

    /// Downcast support.
    fn as_any(&self) -> &dyn Any;
}

/// Context passed to the `ObjectFactory` when a class instance is being constructed.
#[derive(Debug)]
pub struct ObjectConstructionInfo<'a> {
    /// Python module name (e.g. `"__main__"`, `"copy_reg"`).
    pub module: &'a str,
    /// Python class/global name (e.g. `"MyClass"`, `"_reconstructor"`).
    pub class: &'a str,
}

/// Factory callback for constructing custom objects during deserialization.
///
/// Called when a class instance is being constructed (NEWOBJ, Reduce, etc.).
/// Return `Some` to provide a custom object, `None` to fall back to `DictObject`.
pub type ObjectFactory = Box<dyn Fn(ObjectConstructionInfo<'_>) -> Option<Box<dyn PickleObject>>>;

/// Default implementation of `PickleObject` that stores state as a dictionary.
/// Used as the fallback when no custom `ObjectFactory` is provided.
///
/// This is provided mostly as a convenience when you just want custom objects to work,
/// but don't want to write custom types. It's somewhat of a hack since custom classes
/// can be used as hashable objects, but dictionaries themselves cannot... so to bypass
/// that, we use this wrapper object to bypass that check without having to do some
/// even hackier changes to the `pickled` internals just for this usecase.
#[derive(Clone, Debug)]
pub struct DictObject {
    module: String,
    class: String,
    state: BTreeMap<HashableValue, Value>,
}

impl DictObject {
    pub fn new(module: String, class: String) -> Self {
        DictObject {
            module,
            class,
            state: BTreeMap::new(),
        }
    }

    /// Access the internal state dictionary.
    pub fn state(&self) -> &BTreeMap<HashableValue, Value> {
        &self.state
    }

    /// Convert this object into a `Value::Dict` containing the state.
    pub fn into_value(self) -> Value {
        Value::Dict(Shared::new(self.state))
    }
}

impl fmt::Display for DictObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<{}.{} {{", self.module, self.class)?;
        for (i, (key, value)) in self.state.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{key}: {value}")?;
        }
        write!(f, "}}>")
    }
}

impl PickleObject for DictObject {
    fn __setstate__(&mut self, state: Value) {
        // CPython BUILD logic:
        // 1. If state is a 2-tuple, split into (dict_state, slot_state)
        // 2. Update __dict__ with dict_state
        // 3. Use setattr for slot_state entries
        // Since DictObject has no real slots, both go into self.state.
        let (dict_state, slot_state) = match &state {
            Value::Tuple(t) if t.inner().len() == 2 => {
                let inner = t.inner();
                (inner[0].clone(), Some(inner[1].clone()))
            }
            _ => (state, None),
        };

        // Apply dict_state (__dict__.update equivalent)
        match dict_state {
            Value::Dict(d) => {
                let d = d.inner();
                self.state
                    .extend(d.iter().map(|(k, v)| (k.clone(), v.clone())));
            }
            Value::None => {} // No dict state
            other => {
                // Non-dict state: store under special key
                self.state.insert(
                    HashableValue::String(SharedFrozen::new("__state__".into())),
                    other,
                );
            }
        }

        // Apply slot_state (setattr equivalent)
        if let Some(Value::Dict(d)) = slot_state {
            let d = d.inner();
            self.state
                .extend(d.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
    }

    fn __setitem__(&mut self, key: Value, value: Value) {
        if let Ok(hk) = key.into_hashable() {
            self.state.insert(hk, value);
        }
    }

    fn __hash__(&self) -> Result<HashableValue, Error> {
        // Convert state entries to a FrozenSet of (key, value) tuples
        let items: std::collections::BTreeSet<HashableValue> = self
            .state
            .iter()
            .map(|(k, v)| {
                let hv = v.clone().into_hashable()?;
                Ok(HashableValue::Tuple(SharedFrozen::new(vec![k.clone(), hv])))
            })
            .collect::<Result<_, Error>>()?;
        Ok(HashableValue::FrozenSet(SharedFrozen::new(items)))
    }

    fn class_info(&self) -> (&str, &str) {
        (&self.module, &self.class)
    }

    fn __reduce__(&self) -> ReduceResult {
        ReduceResult {
            module: self.module.clone(),
            class: self.class.clone(),
            args: Value::Tuple(SharedFrozen::new(vec![])),
            state: Some(Value::Dict(Shared::new(self.state.clone()))),
            list_items: None,
            dict_items: None,
        }
    }

    fn eq_dyn(&self, other: &dyn PickleObject) -> bool {
        match other.as_any().downcast_ref::<Self>() {
            Some(other) => {
                self.module == other.module
                    && self.class == other.class
                    && self.state == other.state
            }
            None => false,
        }
    }

    fn cmp_dyn(&self, other: &dyn PickleObject) -> Ordering {
        match other.as_any().downcast_ref::<Self>() {
            Some(other) => self.class_info().cmp(&other.class_info()).then_with(|| {
                // Compare by keys (HashableValue is Ord), then by length
                self.state
                    .keys()
                    .cmp(other.state.keys())
                    .then_with(|| self.state.len().cmp(&other.state.len()))
            }),
            None => self.class_info().cmp(&other.class_info()),
        }
    }

    fn clone_dyn(&self) -> Box<dyn PickleObject> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
