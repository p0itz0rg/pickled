// Copyright (c) 2015-2021 Georg Brandl.  Licensed under the Apache License,
// Version 2.0 <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0>
// or the MIT license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at
// your option. This file may not be copied, modified, or distributed except
// according to those terms.

mod arby;

macro_rules! pyobj {
    (n=None)     => { Value::None };
    (b=True)     => { Value::Bool(true) };
    (b=False)    => { Value::Bool(false) };
    (i=$i:expr)  => { Value::I64($i) };
    (ii=$i:expr) => { Value::Int($i.clone()) };
    (f=$f:expr)  => { Value::F64($f) };
    (bb=$b:expr) => { Value::Bytes(crate::value::SharedFrozen::new($b.to_vec())) };
    (s=$s:expr)  => { Value::String(crate::value::SharedFrozen::new($s.to_string())) };
    (t=($($m:ident=$v:tt),*))  => { Value::Tuple(crate::value::SharedFrozen::new(vec![$(pyobj!($m=$v)),*])) };
    (l=[$($m:ident=$v:tt),*])  => { Value::List(crate::value::Shared::new(vec![$(pyobj!($m=$v)),*])) };
    (ss=($($m:ident=$v:tt),*)) => { Value::Set(crate::value::Shared::new(BTreeSet::from_iter(vec![$(hpyobj!($m=$v)),*]))) };
    (fs=($($m:ident=$v:tt),*)) => { Value::FrozenSet(crate::value::SharedFrozen::new(BTreeSet::from_iter(vec![$(hpyobj!($m=$v)),*]))) };
    (d={$($km:ident=$kv:tt => $vm:ident=$vv:tt),*}) => {
        Value::Dict(crate::value::Shared::new(BTreeMap::from_iter(vec![$((hpyobj!($km=$kv),
                                                pyobj!($vm=$vv))),*]))) };
}

macro_rules! hpyobj {
    (n=None)     => { HashableValue::None };
    (b=True)     => { HashableValue::Bool(true) };
    (b=False)    => { HashableValue::Bool(false) };
    (i=$i:expr)  => { HashableValue::I64($i) };
    (ii=$i:expr) => { HashableValue::Int($i.clone()) };
    (f=$f:expr)  => { HashableValue::F64($f) };
    (bb=$b:expr) => { HashableValue::Bytes(crate::value::SharedFrozen::new($b.to_vec())) };
    (s=$s:expr)  => { HashableValue::String(crate::value::SharedFrozen::new($s.to_string())) };
    (t=($($m:ident=$v:tt),*))  => { HashableValue::Tuple(crate::value::SharedFrozen::new(vec![$(hpyobj!($m=$v)),*])) };
    (fs=($($m:ident=$v:tt),*)) => { HashableValue::FrozenSet(crate::value::SharedFrozen::new(BTreeSet::from_iter(vec![$(hpyobj!($m=$v)),*]))) };
}

mod struct_tests {
    use crate::{
        HashableValue, SerOptions, Value, from_slice, from_value, to_value, to_vec,
        value_from_slice, value_to_vec,
    };
    use serde::{de, ser};
    use serde_derive::{Deserialize, Serialize};
    use std::collections::BTreeMap;
    use std::fmt;
    use std::iter::FromIterator;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Inner {
        a: (),
        b: usize,
        c: Vec<String>,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Outer {
        inner: Vec<Inner>,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Unit;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Newtype(i32);

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Tuple(i32, bool);

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    enum Animal {
        Dog,
        AntHive(Vec<String>),
        Frog(String, Vec<isize>),
        Cat { age: usize, name: String },
    }

    fn test_encode_ok<T>(value: T, target: Value)
    where
        T: PartialEq + ser::Serialize,
    {
        // Test serialization via pickle.
        let vec = to_vec(&value, Default::default()).unwrap();
        let py_val: Value = value_from_slice(&vec, Default::default()).unwrap();
        assert_eq!(py_val, target);
        // Test direct serialization to Value.
        let py_val: Value = to_value(&value).unwrap();
        assert_eq!(py_val, target);
    }

    fn test_encode_ok_with_opt<T>(value: T, target: Value, options: SerOptions)
    where
        T: PartialEq + ser::Serialize,
    {
        let vec = to_vec(&value, options).unwrap();
        let py_val: Value = value_from_slice(&vec, Default::default()).unwrap();
        assert_eq!(py_val, target);
    }

    fn test_decode_ok<'de, T>(pyvalue: Value, target: T)
    where
        T: PartialEq + fmt::Debug + de::Deserialize<'de>,
    {
        // Test deserialization from pickle.
        let vec = value_to_vec(&pyvalue, Default::default()).unwrap();
        let val: T = from_slice(&vec, Default::default()).unwrap();
        assert_eq!(val, target);
        // Test direct deserialization from Value.
        let val: T = from_value(pyvalue).unwrap();
        assert_eq!(val, target);
    }

    #[test]
    fn encode_types() {
        test_encode_ok((), pyobj!(n = None));
        test_encode_ok(true, pyobj!(b = True));
        test_encode_ok(None::<i32>, pyobj!(n = None));
        test_encode_ok(Some(false), pyobj!(b = False));
        test_encode_ok(10000000000_i64, pyobj!(i = 10000000000));
        test_encode_ok(4.5_f64, pyobj!(f = 4.5));
        test_encode_ok('ä', pyobj!(s = "ä"));
        test_encode_ok("string", pyobj!(s = "string"));
        // serde doesn't encode into bytes...
        test_encode_ok(&b"\x00\x01"[..], pyobj!(l = [i = 0, i = 1]));
        test_encode_ok(vec![1, 2, 3], pyobj!(l = [i = 1, i = 2, i = 3]));
        test_encode_ok((1, 2, 3), pyobj!(t = (i = 1, i = 2, i = 3)));
        test_encode_ok(&[1, 2, 3][..], pyobj!(l = [i = 1, i = 2, i = 3]));
        // serde 1.0: fixed-size arrays are now tuples...
        test_encode_ok([1, 2, 3], pyobj!(t = (i = 1, i = 2, i = 3)));
        test_encode_ok(
            BTreeMap::from_iter(vec![(1, 2), (3, 4)]),
            pyobj!(d={i=1 => i=2, i=3 => i=4}),
        );
    }

    #[test]
    fn encode_struct() {
        test_encode_ok(Unit, pyobj!(n = None));
        test_encode_ok(Newtype(42), pyobj!(i = 42));
        test_encode_ok(Tuple(42, false), pyobj!(t = (i = 42, b = False)));
        test_encode_ok(
            Inner {
                a: (),
                b: 32,
                c: vec!["doc".into()],
            },
            pyobj!(d={s="a" => n=None, s="b" => i=32,
                                 s="c" => l=[s="doc"]}),
        );
    }

    #[test]
    fn encode_enum() {
        test_encode_ok(Animal::Dog, pyobj!(s = "Dog"));
        test_encode_ok(
            Animal::AntHive(vec!["ant".into(), "aunt".into()]),
            pyobj!(d={s="AntHive" => l=[s="ant", s="aunt"]}),
        );
        test_encode_ok(
            Animal::Frog("Henry".into(), vec![1, 5]),
            pyobj!(d={s="Frog" => l=[s="Henry", l=[i=1, i=5]]}),
        );
        test_encode_ok(
            Animal::Cat {
                age: 5,
                name: "Molyneux".into(),
            },
            pyobj!(d={s="Cat" => d={s="age" => i=5, s="name" => s="Molyneux"}}),
        );
    }

    #[test]
    fn encode_enum_compat() {
        test_encode_ok_with_opt(
            Animal::Dog,
            pyobj!(t = (s = "Dog")),
            SerOptions::new().compat_enum_repr(),
        );
        test_encode_ok_with_opt(
            Animal::AntHive(vec!["ant".into(), "aunt".into()]),
            pyobj!(t = (s = "AntHive", l = [s = "ant", s = "aunt"])),
            SerOptions::new().compat_enum_repr(),
        );
        test_encode_ok_with_opt(
            Animal::Frog("Henry".into(), vec![1, 5]),
            pyobj!(t = (s = "Frog", l = [s = "Henry", l = [i = 1, i = 5]])),
            SerOptions::new().compat_enum_repr(),
        );
        test_encode_ok_with_opt(
            Animal::Cat {
                age: 5,
                name: "Molyneux".into(),
            },
            pyobj!(t=(s="Cat", d={s="age" => i=5, s="name" => s="Molyneux"})),
            SerOptions::new().compat_enum_repr(),
        );
    }

    #[test]
    fn decode_types() {
        test_decode_ok(pyobj!(n = None), ());
        test_decode_ok(pyobj!(b = True), true);
        test_decode_ok(pyobj!(b = True), Some(true));
        test_decode_ok::<Option<bool>>(pyobj!(n = None), None);
        test_decode_ok(pyobj!(i = 10000000000), 10000000000_i64);
        test_decode_ok(pyobj!(f = 4.5), 4.5_f64);
        test_decode_ok(pyobj!(s = "ä"), 'ä');
        test_decode_ok(pyobj!(s = "string"), String::from("string"));
        // Vec<u8> doesn't decode from serde bytes...
        test_decode_ok(pyobj!(bb = b"bytes"), String::from("bytes"));
        test_decode_ok(pyobj!(l = [i = 1, i = 2, i = 3]), vec![1, 2, 3]);
        test_decode_ok(pyobj!(t = (i = 1, i = 2, i = 3)), (1, 2, 3));
        test_decode_ok(pyobj!(l = [i = 1, i = 2, i = 3]), [1, 2, 3]);
        test_decode_ok(
            pyobj!(d={i=1 => i=2, i=3 => i=4}),
            BTreeMap::from_iter(vec![(1, 2), (3, 4)]),
        );
    }

    #[test]
    fn decode_struct() {
        test_decode_ok(pyobj!(n = None), Unit);
        test_decode_ok(pyobj!(i = 42), Newtype(42));
        test_decode_ok(pyobj!(t = (i = 42, b = False)), Tuple(42, false));
        test_decode_ok(
            pyobj!(d={s="a" => n=None, s="b" => i=32, s="c" => l=[s="doc"]}),
            Inner {
                a: (),
                b: 32,
                c: vec!["doc".into()],
            },
        );
    }

    #[test]
    fn decode_enum() {
        // tuple representation
        test_decode_ok(pyobj!(t = (s = "Dog")), Animal::Dog);
        test_decode_ok(
            pyobj!(t = (s = "AntHive", l = [s = "ant", s = "aunt"])),
            Animal::AntHive(vec!["ant".into(), "aunt".into()]),
        );
        test_decode_ok(
            pyobj!(t = (s = "Frog", l = [s = "Henry", l = [i = 1, i = 5]])),
            Animal::Frog("Henry".into(), vec![1, 5]),
        );
        test_decode_ok(
            pyobj!(t=(s="Cat", d={s="age" => i=5, s="name" => s="Molyneux"})),
            Animal::Cat {
                age: 5,
                name: "Molyneux".into(),
            },
        );
        test_decode_ok(
            pyobj!(l=[t=(s="Dog"), t=(s="Dog"),
                                 t=(s="Cat", d={s="age" => i=5, s="name" => s="?"})]),
            vec![
                Animal::Dog,
                Animal::Dog,
                Animal::Cat {
                    age: 5,
                    name: "?".into(),
                },
            ],
        );

        // string/dict representation
        test_decode_ok(pyobj!(s = "Dog"), Animal::Dog);
        test_decode_ok(
            pyobj!(d={s="AntHive" => l=[s="ant", s="aunt"]}),
            Animal::AntHive(vec!["ant".into(), "aunt".into()]),
        );
        test_decode_ok(
            pyobj!(d={s="Frog" => l=[s="Henry", l=[i=1, i=5]]}),
            Animal::Frog("Henry".into(), vec![1, 5]),
        );
        test_decode_ok(
            pyobj!(d={s="Cat" => d={s="age" => i=5, s="name" => s="Molyneux"}}),
            Animal::Cat {
                age: 5,
                name: "Molyneux".into(),
            },
        );
        test_decode_ok(
            pyobj!(l=[s="Dog", s="Dog",
                                 d={s="Cat" => d={s="age" => i=5, s="name" => s="?"}}]),
            vec![
                Animal::Dog,
                Animal::Dog,
                Animal::Cat {
                    age: 5,
                    name: "?".into(),
                },
            ],
        );
    }
}

mod value_tests {
    use crate::Deserializer;
    use crate::error::{Error, ErrorCode};
    use crate::object::{DictObject, PickleObject};
    use crate::{DeOptions, HashableValue, SerOptions, Value};
    use crate::{from_slice, to_vec, value_from_reader, value_from_slice, value_to_vec};
    use num_bigint::BigInt;
    use quickcheck::{Gen, QuickCheck};
    use rand::{RngCore, rng};
    use std::collections::{BTreeMap, BTreeSet};
    use std::fs::File;
    use std::iter::FromIterator;

    // combinations of (python major, pickle proto) to test
    const TEST_CASES: &[(u32, u32)] = &[
        (2, 0),
        (2, 1),
        (2, 2),
        (3, 0),
        (3, 1),
        (3, 2),
        (3, 3),
        (3, 4),
        (3, 5),
    ];

    fn get_test_object(pyver: u32, proto: u32) -> Value {
        // Reproduces the test_object from test/data/generate.py.
        let longish = BigInt::from(10000000000u64) * BigInt::from(10000000000u64);
        let mut obj = pyobj!(d={
            n=None           => n=None,
            b=False          => t=(b=False, b=True),
            i=10             => i=100000,
            ii=longish       => ii=longish,
            f=1.0            => f=1.0,
            bb=b"bytes"      => bb=b"bytes",
            s="string"       => s="string",
            fs=(i=0, i=42)   => fs=(i=0, i=42),
            t=(i=1, i=2)     => t=(i=1, i=2, i=3),
            t=()             => l=[
                l=[i=1, i=2, i=3],
                ss=(i=0, i=42),
                d={},
                bb=b"\x00\x55\xaa\xff"
            ]
        });
        // For protocols that use NEWOBJ (proto >= 2), class instances are
        // deserialized as Objects. For older protocols that use
        // _reconstructor via REDUCE, BUILD replaces the standin with the
        // state dict directly.
        let uses_newobj = proto >= 2;
        match &mut obj {
            Value::Dict(map) => {
                let mut map = map.inner_mut();
                if uses_newobj {
                    let mut class_obj = DictObject::new("__main__".into(), "Class".into());
                    if pyver == 2 {
                        class_obj.__setstate__(pyobj!(d={bb=b"attr" => i=5}));
                    } else {
                        class_obj.__setstate__(pyobj!(d={s="attr" => i=5}));
                    }
                    map.insert(hpyobj!(i = 7), Value::Object(Box::new(class_obj)));
                } else {
                    // _reconstructor path: BUILD replaces standin with state dict
                    if pyver == 2 {
                        map.insert(hpyobj!(i = 7), pyobj!(d={bb=b"attr" => i=5}));
                    } else {
                        map.insert(hpyobj!(i = 7), pyobj!(d={s="attr" => i=5}));
                    }
                }
            }
            _ => unreachable!(),
        }
        obj
    }

    #[test]
    fn unpickle_all() {
        for &(major, proto) in TEST_CASES {
            let file =
                File::open(format!("test/data/tests_py{}_proto{}.pickle", major, proto)).unwrap();
            let comparison = get_test_object(major, proto);
            let unpickled = value_from_reader(file, Default::default()).unwrap();
            assert_eq!(unpickled, comparison, "py {}, proto {}", major, proto);
        }
    }

    #[test]
    fn roundtrip() {
        // Use proto 0 (no NEWOBJ) so the test object has no Object variants.
        let dict = get_test_object(2, 0);
        let vec: Vec<_> = value_to_vec(&dict, Default::default()).unwrap();
        let tripped = value_from_slice(&vec, Default::default()).unwrap();
        assert_eq!(dict, tripped);
    }

    #[test]
    fn recursive() {
        for proto in &[0, 1, 2, 3, 4, 5] {
            let file =
                File::open(format!("test/data/test_recursive_proto{}.pickle", proto)).unwrap();
            match value_from_reader(file, Default::default()) {
                Err(Error::Syntax(ErrorCode::Recursive)) => {}
                Ok(value) => {
                    let list = value.list_ref().expect("recursive structure is not a list");
                    let list_inner = list.inner();
                    assert!(
                        list_inner.is_empty(),
                        "recursive list structure is not empty"
                    );
                }
                value => {
                    panic!(
                        "wrong/no error returned for recursive structure, {:?}",
                        value
                    );
                }
            }
        }
    }

    #[test]
    fn recursive_with_replace_reconstructor() {
        for proto in &[0, 1, 2, 3, 4, 5] {
            let file =
                File::open(format!("test/data/test_recursive_proto{}.pickle", proto)).unwrap();
            match value_from_reader(
                file,
                DeOptions::new().replace_reconstructor_objects_structures(),
            ) {
                Err(Error::Syntax(ErrorCode::Recursive)) => {}
                Ok(value) => {
                    let list = value.list_ref().expect("recursive structure is not a list");
                    let list_inner = list.inner();
                    assert!(
                        list_inner.is_empty(),
                        "recursive list structure is not empty"
                    );
                }
                value => {
                    panic!(
                        "wrong/no error returned for recursive structure, {:?}",
                        value
                    );
                }
            }
        }
    }

    #[test]
    fn fuzzing() {
        // Tries to ensure that we don't panic when encountering strange streams.
        for _ in 0..1000 {
            let mut stream = [0u8; 1000];
            rng().fill_bytes(&mut stream);
            if *stream.last().unwrap() == b'.' {
                continue;
            }
            // These must all fail with an error, since we skip the check if the
            // last byte is a STOP opcode.
            assert!(value_from_slice(&stream, Default::default()).is_err());
        }
    }

    #[test]
    fn qc_roundtrip() {
        fn roundtrip(original: Value) -> quickcheck::TestResult {
            // NaN != NaN per IEEE 754 / Python semantics, so values
            // containing NaN can never compare equal after roundtrip.
            if original.contains_nan() {
                return quickcheck::TestResult::discard();
            }
            let vec: Vec<_> = value_to_vec(&original, Default::default()).unwrap();
            let tripped = value_from_slice(&vec, Default::default()).unwrap();
            quickcheck::TestResult::from_bool(original == tripped)
        }
        QuickCheck::new()
            .r#gen(Gen::new(10))
            .tests(5000)
            .quickcheck(roundtrip as fn(_) -> quickcheck::TestResult);
    }

    #[test]
    fn roundtrip_json() {
        let original: serde_json::Value = serde_json::from_str(
            r#"[
            {"null": null,
             "false": false,
             "true": true,
             "int": -1238571,
             "float": 1.5e10,
             "list": [false, 5, "true", 3.8]
            }
        ]"#,
        )
        .unwrap();
        let vec: Vec<_> = to_vec(&original, Default::default()).unwrap();
        let tripped: serde_json::Value = from_slice(&vec, Default::default()).unwrap();
        assert_eq!(original, tripped);
    }

    #[test]
    fn bytestring_v2_py3_roundtrip() {
        let original = Value::Bytes(b"123\xff\xfe".to_vec().into());
        let vec: Vec<_> = value_to_vec(&original, SerOptions::new().proto_v2()).unwrap();
        // Python 3 default deserializer attempts to decode strings
        let mut de = Deserializer::new(vec.as_slice(), DeOptions::new().decode_strings());
        let tripped: Value = de.deserialize_value().unwrap();
        assert_eq!(original, tripped);
        de.end().unwrap();
    }

    #[test]
    fn unresolvable_global() {
        let data = std::fs::read("test/data/test_unresolvable_global.pickle").unwrap();
        assert!(value_from_slice(&data, Default::default()).is_err());
        let val = value_from_slice(&data, DeOptions::new().replace_unresolved_globals()).unwrap();
        assert_eq!(val, Value::None);
        let serde_val: serde_json::Value =
            from_slice(&data, DeOptions::new().replace_unresolved_globals()).unwrap();
        assert_eq!(serde_val, serde_json::Value::Null);
    }

    #[test]
    fn simple_class_as_dict_object() {
        for proto in 2..=5 {
            let data = std::fs::read(format!(
                "test/data/test_simple_class_proto{proto}.pickle"
            ))
            .unwrap();
            let val = value_from_slice(&data, Default::default()).unwrap();
            let obj = val.object_ref().unwrap_or_else(|| {
                panic!("proto {proto}: expected Object, got {val:?}")
            });
            let (module, class) = obj.class_info();
            assert_eq!(module, "__main__");
            assert_eq!(class, "SimpleClass");

            let dict_obj = obj
                .as_any()
                .downcast_ref::<DictObject>()
                .expect("expected DictObject");
            let state = dict_obj.state();
            assert_eq!(
                state.get(&hpyobj!(s = "x")),
                Some(&pyobj!(i = 42)),
                "proto {proto}"
            );
            assert_eq!(
                state.get(&hpyobj!(s = "name")),
                Some(&pyobj!(s = "hello")),
                "proto {proto}"
            );
        }

        // Proto < 2 needs replace_reconstructor to produce Objects.
        for proto in 0..2 {
            let data = std::fs::read(format!(
                "test/data/test_simple_class_proto{proto}.pickle"
            ))
            .unwrap();
            let opts = DeOptions::new().replace_reconstructor_objects_structures();
            let val = value_from_slice(&data, opts).unwrap();
            let obj = val.object_ref().unwrap_or_else(|| {
                panic!("proto {proto} with replace_reconstructor: expected Object, got {val:?}")
            });
            let dict_obj = obj
                .as_any()
                .downcast_ref::<DictObject>()
                .expect("expected DictObject");
            let state = dict_obj.state();
            assert_eq!(
                state.get(&hpyobj!(s = "x")),
                Some(&pyobj!(i = 42)),
                "proto {proto}"
            );
        }
    }

    #[test]
    fn slotted_class_as_dict_object() {
        for proto in 2..=5 {
            let data = std::fs::read(format!(
                "test/data/test_slotted_class_proto{proto}.pickle"
            ))
            .unwrap();
            let val = value_from_slice(&data, Default::default()).unwrap();
            let obj = val.object_ref().expect("expected Object");
            let dict_obj = obj
                .as_any()
                .downcast_ref::<DictObject>()
                .expect("expected DictObject");
            let state = dict_obj.state();
            assert_eq!(
                state.get(&hpyobj!(s = "x")),
                Some(&pyobj!(i = 10)),
                "proto {proto}"
            );
            assert_eq!(
                state.get(&hpyobj!(s = "y")),
                Some(&pyobj!(i = 20)),
                "proto {proto}"
            );
        }
    }

    #[test]
    fn nested_class_as_dict_object() {
        for proto in 2..=5 {
            let data = std::fs::read(format!(
                "test/data/test_nested_class_proto{proto}.pickle"
            ))
            .unwrap();
            let val = value_from_slice(&data, Default::default()).unwrap();
            let outer = val.object_ref().expect("expected Object");
            let outer_dict = outer
                .as_any()
                .downcast_ref::<DictObject>()
                .expect("expected DictObject");

            // "value" should be [1, 2, 3]
            let value_key = hpyobj!(s = "value");
            let list = outer_dict.state().get(&value_key).unwrap();
            assert_eq!(*list, pyobj!(l = [i = 1, i = 2, i = 3]), "proto {proto}");

            // "inner" should be another DictObject
            let inner_key = hpyobj!(s = "inner");
            let inner_val = outer_dict.state().get(&inner_key).unwrap();
            let inner_obj = inner_val.object_ref().expect("inner should be Object");
            let inner_dict = inner_obj
                .as_any()
                .downcast_ref::<DictObject>()
                .expect("inner should be DictObject");
            assert_eq!(
                inner_dict.state().get(&hpyobj!(s = "x")),
                Some(&pyobj!(i = 42)),
                "proto {proto}"
            );
        }
    }

    #[test]
    fn empty_class_as_dict_object() {
        for proto in 2..=5 {
            let data = std::fs::read(format!(
                "test/data/test_empty_class_proto{proto}.pickle"
            ))
            .unwrap();
            let val = value_from_slice(&data, Default::default()).unwrap();
            let obj = val.object_ref().expect("expected Object");
            let dict_obj = obj
                .as_any()
                .downcast_ref::<DictObject>()
                .expect("expected DictObject");
            assert!(
                dict_obj.state().is_empty(),
                "proto {proto}: empty class should have empty state"
            );
        }
    }

    #[test]
    fn custom_object_factory() {
        use crate::object::ObjectConstructionInfo;
        use std::any::Any;

        #[derive(Clone, Debug)]
        struct CustomObj {
            module: String,
            class: String,
            state_value: Option<Value>,
        }

        impl std::fmt::Display for CustomObj {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "<custom {}.{}>", self.module, self.class)
            }
        }

        impl PickleObject for CustomObj {
            fn __setstate__(&mut self, state: Value) {
                self.state_value = Some(state);
            }
            fn class_info(&self) -> (&str, &str) {
                (&self.module, &self.class)
            }
            fn __reduce__(&self) -> crate::object::ReduceResult {
                crate::object::ReduceResult {
                    module: self.module.clone(),
                    class: self.class.clone(),
                    args: Value::Tuple(crate::value::SharedFrozen::new(vec![])),
                    state: self.state_value.clone(),
                    list_items: None,
                    dict_items: None,
                }
            }
            fn eq_dyn(&self, other: &dyn PickleObject) -> bool {
                other
                    .as_any()
                    .downcast_ref::<Self>()
                    .is_some_and(|o| {
                        self.module == o.module
                            && self.class == o.class
                            && self.state_value == o.state_value
                    })
            }
            fn cmp_dyn(&self, other: &dyn PickleObject) -> std::cmp::Ordering {
                self.class_info().cmp(&other.class_info())
            }
            fn clone_dyn(&self) -> Box<dyn PickleObject> {
                Box::new(self.clone())
            }
            fn as_any(&self) -> &dyn Any {
                self
            }
        }

        let factory: crate::ObjectFactory = Box::new(|info: ObjectConstructionInfo| {
            if info.class == "SimpleClass" {
                Some(Box::new(CustomObj {
                    module: info.module.to_owned(),
                    class: info.class.to_owned(),
                    state_value: None,
                }))
            } else {
                None // fall back to DictObject
            }
        });

        let data =
            std::fs::read("test/data/test_simple_class_proto4.pickle").unwrap();
        let val =
            value_from_slice(&data, DeOptions::new().object_factory(factory)).unwrap();
        let obj = val.object_ref().expect("expected Object");
        assert!(
            obj.as_any().downcast_ref::<CustomObj>().is_some(),
            "SimpleClass should be handled by custom factory"
        );
        let custom = obj.as_any().downcast_ref::<CustomObj>().unwrap();
        assert!(
            custom.state_value.is_some(),
            "__setstate__ should have been called"
        );

        // Factory only handles NestedClass; inner SimpleClass falls back to DictObject.
        let factory2: crate::ObjectFactory = Box::new(|info: ObjectConstructionInfo| {
            if info.class == "NestedClass" {
                Some(Box::new(CustomObj {
                    module: info.module.to_owned(),
                    class: info.class.to_owned(),
                    state_value: None,
                }))
            } else {
                None
            }
        });
        let data =
            std::fs::read("test/data/test_nested_class_proto4.pickle").unwrap();
        let val =
            value_from_slice(&data, DeOptions::new().object_factory(factory2)).unwrap();
        let obj = val.object_ref().expect("expected Object");
        let custom = obj.as_any().downcast_ref::<CustomObj>().unwrap();
        if let Some(state) = &custom.state_value {
            if let Value::Dict(d) = state {
                let d = d.inner();
                let inner = d.iter().find(|(k, _)| **k == hpyobj!(s = "inner"));
                if let Some((_, inner_val)) = inner {
                    assert!(
                        inner_val.object_ref().is_some(),
                        "inner SimpleClass should be a DictObject fallback"
                    );
                }
            }
        }
    }

    #[test]
    fn float_bigint_exact_comparison() {
        let two_53 = BigInt::from(1u64 << 53);
        let two_53_f = (1u64 << 53) as f64;

        // 2^53 is exactly representable as f64
        assert_eq!(HashableValue::Int(two_53.clone()), HashableValue::F64(two_53_f));

        // 2^53 + 1 loses precision in f64; the int is strictly greater
        let two_53_plus_1 = &two_53 + BigInt::from(1);
        assert_ne!(HashableValue::Int(two_53_plus_1.clone()), HashableValue::F64(two_53_f));
        assert!(HashableValue::Int(two_53_plus_1) > HashableValue::F64(two_53_f));

        let huge = BigInt::from(2).pow(100);
        assert!(HashableValue::Int(huge.clone()) > HashableValue::F64(1.0e30));
        assert!(HashableValue::Int(-huge) < HashableValue::F64(-1.0e30));
    }

    #[test]
    fn neg_zero_equals_pos_zero() {
        assert_eq!(HashableValue::F64(-0.0), HashableValue::F64(0.0));
        assert_eq!(Value::F64(-0.0), Value::F64(0.0));
        assert_eq!(
            HashableValue::F64(-0.0).cmp(&HashableValue::F64(0.0)),
            std::cmp::Ordering::Equal
        );
    }

    #[test]
    fn nan_not_equal_to_self() {
        let nan = f64::NAN;
        assert_ne!(HashableValue::F64(nan), HashableValue::F64(nan));
        assert_ne!(Value::F64(nan), Value::F64(nan));
    }

    #[test]
    fn cross_type_numeric_equality() {
        assert_eq!(HashableValue::I64(1), HashableValue::F64(1.0));
        assert_eq!(HashableValue::Bool(true), HashableValue::I64(1));
        assert_eq!(HashableValue::Bool(true), HashableValue::F64(1.0));
        assert_eq!(HashableValue::I64(0), HashableValue::F64(0.0));
        assert_eq!(HashableValue::Bool(false), HashableValue::I64(0));
        assert_eq!(HashableValue::I64(256), HashableValue::F64(256.0));
        // Precision boundary: 2^53 + 1 is not exactly representable as f64
        assert_ne!(
            HashableValue::I64((1i64 << 53) + 1),
            HashableValue::F64((1u64 << 53) as f64)
        );
    }

    #[test]
    fn unpickle_numeric_edges() {
        let data = std::fs::read("test/data/test_numeric_edges.pickle").unwrap();
        let val = value_from_slice(&data, Default::default()).unwrap();
        let dict = val.dict_ref().expect("expected dict");
        let dict = dict.inner();

        let key = hpyobj!(s = "neg_zero");
        let entry = dict.get(&key).unwrap();
        if let Value::Tuple(t) = entry {
            let inner = t.inner();
            assert_eq!(inner[0], inner[1], "-0.0 should equal 0.0");
        }

        // I64 and F64 are distinct Value types but equal as HashableValue
        let key = hpyobj!(s = "int_one_float_one");
        let entry = dict.get(&key).unwrap();
        if let Value::Tuple(t) = entry {
            let inner = t.inner();
            let a = inner[0].clone().into_hashable().unwrap();
            let b = inner[1].clone().into_hashable().unwrap();
            assert_eq!(a, b, "1 should equal 1.0 as HashableValue");
        }
    }

    #[test]
    fn unpickle_nan_and_zeros() {
        let data = std::fs::read("test/data/test_nan_and_zeros.pickle").unwrap();
        let val = value_from_slice(&data, Default::default()).unwrap();
        let dict = val.dict_ref().expect("expected dict");
        let dict = dict.inner();

        let key = hpyobj!(s = "nan_in_list");
        let list = dict.get(&key).unwrap().list_ref().unwrap();
        let list = list.inner();
        assert_eq!(list.len(), 3);
        assert!(matches!(list[0], Value::F64(f) if f.is_nan()));

        // Python deduplicates -0.0/0.0 and 1/1.0 in sets
        let key = hpyobj!(s = "neg_zero_in_set");
        let set = dict.get(&key).unwrap().set_ref().unwrap();
        assert_eq!(set.inner().len(), 1);

        let key = hpyobj!(s = "int_float_in_set");
        let set = dict.get(&key).unwrap().set_ref().unwrap();
        assert_eq!(set.inner().len(), 1);
    }

    #[test]
    fn unpickle_set_dedup() {
        // Python: {1, 1.0, True} -> {1}
        let data = std::fs::read("test/data/test_set_dedup.pickle").unwrap();
        let val = value_from_slice(&data, Default::default()).unwrap();
        assert_eq!(val.set_ref().unwrap().inner().len(), 1);
    }

    #[test]
    fn unpickle_dict_numeric_keys() {
        // Python: d[1]="int"; d[1.0]="float"; d[True]="bool" -> {1: "bool"}
        let data = std::fs::read("test/data/test_dict_numeric_keys.pickle").unwrap();
        let val = value_from_slice(&data, Default::default()).unwrap();
        assert_eq!(val.dict_ref().unwrap().inner().len(), 1);
    }
}
