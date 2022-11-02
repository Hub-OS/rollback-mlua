#![cfg(feature = "serialize")]

use std::collections::HashMap;

use rollback_mlua::{
    DeserializeOptions, Error, Lua, LuaSerdeExt, Result as LuaResult, SerializeOptions, Value,
};
use serde::{Deserialize, Serialize};

#[test]
fn test_serialize() -> Result<(), Box<dyn std::error::Error>> {
    let lua = Lua::new();
    let globals = lua.globals();

    let empty_array = lua.create_table()?;
    empty_array.set_metatable(Some(lua.array_metatable()));
    globals.set("empty_array", empty_array)?;

    let val = lua
        .load(
            r#"
        {
            _bool = true,
            _integer = 123,
            _number = 321.99,
            _string = "test string serialization",
            _table_arr = {nil, "value 1", nil, "value 2", {}},
            _table_map = {["table"] = "map"},
            _bytes = "\240\040\140\040",
            _empty_map = {},
            _empty_array = empty_array,
        }
    "#,
        )
        .eval::<Value>()?;

    let json = serde_json::json!({
        "_bool": true,
        "_integer": 123,
        "_number": 321.99,
        "_string": "test string serialization",
        "_table_arr": [null, "value 1", null, "value 2", {}],
        "_table_map": {"table": "map"},
        "_bytes": [240, 40, 140, 40],
        "_empty_map": {},
        "_empty_array": [],
    });

    assert_eq!(serde_json::to_value(&val)?, json);

    // Test to-from loop
    let val = lua.to_value(&json)?;
    let expected_json = lua.from_value::<serde_json::Value>(val)?;
    assert_eq!(expected_json, json);

    Ok(())
}

#[test]
fn test_serialize_failure() -> Result<(), Box<dyn std::error::Error>> {
    let lua = Lua::new();

    let func = lua.create_function(|_, _: ()| Ok(()))?;
    match serde_json::to_value(&Value::Function(func.clone())) {
        Ok(v) => panic!("expected serialization error, got {}", v),
        Err(serde_json::Error { .. }) => {}
    }

    Ok(())
}

#[test]
fn test_to_value_struct() -> LuaResult<()> {
    let lua = Lua::new();
    let globals = lua.globals();

    #[derive(Serialize)]
    struct Test {
        name: String,
        key: i64,
        data: Option<bool>,
    }

    let test = Test {
        name: "alex".to_string(),
        key: -16,
        data: None,
    };

    globals.set("value", lua.to_value(&test)?)?;
    lua.load(
        r#"
            assert(value["name"] == "alex")
            assert(value["key"] == -16)
            assert(value["data"] == null)
        "#,
    )
    .exec()
}

#[test]
fn test_to_value_enum() -> LuaResult<()> {
    let lua = Lua::new();
    let globals = lua.globals();

    #[derive(Serialize)]
    enum E {
        Unit,
        Integer(u32),
        Tuple(u32, u32),
        Struct { a: u32 },
    }

    let u = E::Unit;
    globals.set("value", lua.to_value(&u)?)?;
    lua.load(r#"assert(value == "Unit")"#).exec()?;

    let n = E::Integer(1);
    globals.set("value", lua.to_value(&n)?)?;
    lua.load(r#"assert(value["Integer"] == 1)"#).exec()?;

    let t = E::Tuple(1, 2);
    globals.set("value", lua.to_value(&t)?)?;
    lua.load(
        r#"
            assert(value["Tuple"][1] == 1)
            assert(value["Tuple"][2] == 2)
        "#,
    )
    .exec()?;

    let s = E::Struct { a: 1 };
    globals.set("value", lua.to_value(&s)?)?;
    lua.load(r#"assert(value["Struct"]["a"] == 1)"#).exec()?;
    Ok(())
}

#[test]
fn test_to_value_with_options() -> Result<(), Box<dyn std::error::Error>> {
    let lua = Lua::new();
    let globals = lua.globals();

    // set_array_metatable
    let data = lua.to_value_with(
        &Vec::<i32>::new(),
        SerializeOptions::new().set_array_metatable(false),
    )?;
    globals.set("data", data)?;
    lua.load(
        r#"
        assert(type(data) == "table" and #data == 0)
        assert(getmetatable(data) == nil)
    "#,
    )
    .exec()?;

    #[derive(Serialize)]
    struct UnitStruct;

    #[derive(Serialize)]
    struct MyData {
        map: HashMap<&'static str, Option<i32>>,
        unit: (),
        unitstruct: UnitStruct,
    }

    // serialize_none_to_null
    let mut map = HashMap::new();
    map.insert("key", None);
    let mydata = MyData {
        map,
        unit: (),
        unitstruct: UnitStruct,
    };
    let data2 = lua.to_value(&mydata)?;
    globals.set("data2", data2)?;
    lua.load(
        r#"
        assert(data2.map.key == nil)
        assert(data2.unit == null)
        assert(data2.unitstruct == null)
    "#,
    )
    .exec()?;

    // serialize_unit_to_null
    let data3 = lua.to_value(&mydata)?;
    globals.set("data3", data3)?;
    lua.load(
        r#"
        assert(data3.map.key == null)
        assert(data3.unit == nil)
        assert(data3.unitstruct == nil)
    "#,
    )
    .exec()?;

    Ok(())
}

#[test]
fn test_from_value_nested_tables() -> Result<(), Box<dyn std::error::Error>> {
    let lua = Lua::new();

    let value = lua
        .load(
            r#"
            local table_a = {a = "a"}
            local table_b = {"b"}
            return {
                a = table_a,
                b = {table_b, table_b},
                ab = {a = table_a, b = table_b}
            }
        "#,
        )
        .eval::<Value>()?;
    let got = lua.from_value::<serde_json::Value>(value)?;
    assert_eq!(
        got,
        serde_json::json!({
            "a": {"a": "a"},
            "b": [["b"], ["b"]],
            "ab": {"a": {"a": "a"}, "b": ["b"]},
        })
    );

    Ok(())
}

#[test]
fn test_from_value_struct() -> Result<(), Box<dyn std::error::Error>> {
    let lua = Lua::new();

    #[derive(Deserialize, PartialEq, Debug)]
    struct Test {
        int: u32,
        seq: Vec<String>,
        map: HashMap<i32, i32>,
        empty: Vec<()>,
        tuple: (u8, u8, u8),
    }

    let value = lua
        .load(
            r#"
            {
                int = 1,
                seq = {"a", "b"},
                map = {2, [4] = 1},
                empty = {},
                tuple = {10, 20, 30},
            }
        "#,
        )
        .eval::<Value>()?;
    let got = lua.from_value(value)?;
    assert_eq!(
        Test {
            int: 1,
            seq: vec!["a".into(), "b".into()],
            map: vec![(1, 2), (4, 1)].into_iter().collect(),
            empty: vec![],
            tuple: (10, 20, 30),
        },
        got
    );

    Ok(())
}

#[test]
fn test_from_value_newtype_struct() -> Result<(), Box<dyn std::error::Error>> {
    let lua = Lua::new();

    #[derive(Deserialize, PartialEq, Debug)]
    struct Test(f64);

    let got = lua.from_value(Value::Number(123.456))?;
    assert_eq!(Test(123.456), got);

    Ok(())
}

#[test]
fn test_from_value_enum() -> Result<(), Box<dyn std::error::Error>> {
    let lua = Lua::new();

    #[derive(Deserialize, PartialEq, Debug)]
    enum E {
        Unit,
        Integer(u32),
        Tuple(u32, u32),
        Struct { a: u32 },
    }

    let value = lua.load(r#""Unit""#).eval()?;
    let got = lua.from_value(value)?;
    assert_eq!(E::Unit, got);

    let value = lua.load(r#"{Integer = 1}"#).eval()?;
    let got = lua.from_value(value)?;
    assert_eq!(E::Integer(1), got);

    let value = lua.load(r#"{Tuple = {1, 2}}"#).eval()?;
    let got = lua.from_value(value)?;
    assert_eq!(E::Tuple(1, 2), got);

    let value = lua.load(r#"{Struct = {a = 3}}"#).eval()?;
    let got = lua.from_value(value)?;
    assert_eq!(E::Struct { a: 3 }, got);

    Ok(())
}

#[test]
fn test_from_value_enum_untagged() -> Result<(), Box<dyn std::error::Error>> {
    let lua = Lua::new();

    #[derive(Deserialize, PartialEq, Debug)]
    #[serde(untagged)]
    enum Eut {
        Unit,
        Integer(u64),
        Tuple(u32, u32),
        Struct { a: u32 },
    }

    let value = lua.load(r#"null"#).eval()?;
    let got = lua.from_value(value)?;
    assert_eq!(Eut::Unit, got);

    let value = lua.load(r#"1"#).eval()?;
    let got = lua.from_value(value)?;
    assert_eq!(Eut::Integer(1), got);

    let value = lua.load(r#"{3, 1}"#).eval()?;
    let got = lua.from_value(value)?;
    assert_eq!(Eut::Tuple(3, 1), got);

    let value = lua.load(r#"{a = 10}"#).eval()?;
    let got = lua.from_value(value)?;
    assert_eq!(Eut::Struct { a: 10 }, got);

    let value = lua.load(r#"{b = 12}"#).eval()?;
    match lua.from_value::<Eut>(value) {
        Ok(v) => panic!("expected Error::DeserializeError, got {:?}", v),
        Err(Error::DeserializeError(_)) => {}
        Err(e) => panic!("expected Error::DeserializeError, got {}", e),
    }

    Ok(())
}

#[test]
fn test_from_value_with_options() -> Result<(), Box<dyn std::error::Error>> {
    let lua = Lua::new();

    // Deny unsupported types by default
    let value = Value::Function(lua.create_function(|_, ()| Ok(()))?);
    match lua.from_value::<Option<String>>(value) {
        Ok(v) => panic!("expected deserialization error, got {:?}", v),
        Err(Error::DeserializeError(err)) => {
            assert!(err.contains("unsupported value type"))
        }
        Err(err) => panic!("expected `DeserializeError` error, got {:?}", err),
    };

    // Allow unsupported types
    let value = Value::Function(lua.create_function(|_, ()| Ok(()))?);
    let options = DeserializeOptions::new().deny_unsupported_types(false);
    assert_eq!(lua.from_value_with::<()>(value, options)?, ());

    // Allow unsupported types (in a table seq)
    let value = lua.load(r#"{"a", "b", function() end, "c"}"#).eval()?;
    let options = DeserializeOptions::new().deny_unsupported_types(false);
    assert_eq!(
        lua.from_value_with::<Vec<String>>(value, options)?,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );

    // Deny recursive tables by default
    let value = lua.load(r#"local t = {}; t.t = t; return t"#).eval()?;
    match lua.from_value::<HashMap<String, Option<String>>>(value) {
        Ok(v) => panic!("expected deserialization error, got {:?}", v),
        Err(Error::DeserializeError(err)) => {
            assert!(err.contains("recursive table detected"))
        }
        Err(err) => panic!("expected `DeserializeError` error, got {:?}", err),
    };

    // Check recursion when using `Serialize` impl
    let t = lua.create_table()?;
    t.set("t", t.clone())?;
    assert!(serde_json::to_string(&t).is_err());

    // Serialize Lua globals table
    #[derive(Debug, Deserialize)]
    struct Globals {
        hello: String,
    }
    let options = DeserializeOptions::new()
        .deny_unsupported_types(false)
        .deny_recursive_tables(false);
    lua.load(r#"hello = "world""#).exec()?;
    let globals: Globals = lua.from_value_with(Value::Table(lua.globals()), options)?;
    assert_eq!(globals.hello, "world");

    Ok(())
}
