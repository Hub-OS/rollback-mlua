use rollback_mlua::{Error, Function, Lua, Nil, Result, Table, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

struct DropChecker {
    dropped: Arc<AtomicBool>,
}

impl DropChecker {
    pub fn dropped(&self) -> bool {
        self.dropped.load(Ordering::Relaxed)
    }
}

struct DropTester {
    dropped: Arc<AtomicBool>,
}

impl DropTester {
    pub fn new() -> Self {
        Self {
            dropped: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn create_checker(&self) -> DropChecker {
        DropChecker {
            dropped: self.dropped.clone(),
        }
    }
}

impl Drop for DropTester {
    fn drop(&mut self) {
        assert_eq!(self.dropped.load(Ordering::Relaxed), false, "double free");

        self.dropped.store(true, Ordering::Relaxed);
    }
}

#[test]
fn test_counter() -> Result<()> {
    let mut lua = Lua::new_rollback(20000, 1);

    lua.load(
        r#"
        local i = 0

        function inc()
            i = i + 1
            return i
        end
        "#,
    )
    .exec()?;

    lua.snap();

    {
        let inc = lua.globals().get::<_, Function>("inc")?;
        inc.call::<_, ()>(())?;
        inc.call::<_, ()>(())?;
        assert_eq!(inc.call::<_, i32>(())?, 3);
    }

    lua.rollback(1);

    {
        let inc = lua.globals().get::<_, Function>("inc")?;
        assert_eq!(inc.call::<_, i32>(())?, 1);
    }

    Ok(())
}

#[test]
fn test_modified_uncompressed() -> Result<()> {
    // this test is unclear, but it is something the modified compression had issues with before

    let mut lua = Lua::new_rollback(20000, 5);

    lua.load(
        r#"
        local i = 0

        function inc()
            i = i + 1
            return i
        end
        "#,
    )
    .exec()?;

    lua.snap();

    {
        let inc = lua.globals().get::<_, Function>("inc")?;
        inc.call::<_, ()>(())?;
        inc.call::<_, ()>(())?;
        assert_eq!(inc.call::<_, i32>(())?, 3);
    }

    lua.snap();
    lua.rollback(2);

    {
        let inc = lua.globals().get::<_, Function>("inc")?;
        assert_eq!(inc.call::<_, i32>(())?, 1);
    }

    Ok(())
}

#[test]
fn test_modified_compressed() -> Result<()> {
    // a little more clear, a test the modified compression had issues with as well

    let mut lua = Lua::new_rollback(20000, 5);

    lua.snap();

    lua.load(
        r#"
        local i = 0

        function inc()
            i = i + 1
            return i
        end
        "#,
    )
    .exec()?;

    lua.snap();
    lua.snap();

    {
        let inc = lua.globals().get::<_, Function>("inc")?;
        inc.call::<_, ()>(())?;
        inc.call::<_, ()>(())?;
        assert_eq!(inc.call::<_, i32>(())?, 3);
    }

    lua.rollback(2);

    {
        let inc = lua.globals().get::<_, Function>("inc")?;
        assert_eq!(inc.call::<_, i32>(())?, 1);
    }

    Ok(())
}

#[test]
fn test_table() -> Result<()> {
    let mut lua = Lua::new_rollback(20000, 2);

    lua.snap();

    {
        let table = lua.create_table()?;
        table.set("key", 1)?;
        lua.globals().set("test", table)?;
    }

    lua.snap();

    {
        let table: Table = lua.globals().get("test")?;
        table.set("key", 2)?;
    }

    lua.rollback(1);

    {
        let table: Table = lua.globals().get("test")?;
        assert_eq!(table.get::<_, i32>("key")?, 1);
    }

    lua.rollback(2);

    {
        let table: Value = lua.globals().get("test")?;
        assert_eq!(table, Nil);
    }

    Ok(())
}

#[test]
fn test_registry() -> Result<()> {
    let mut lua = Lua::new_rollback(20000, 2);

    lua.snap();

    let key = lua.create_registry_value("a")?;
    assert_eq!(lua.registry_value::<String>(&key)?, "a");

    lua.snap();

    lua.replace_registry_value(&key, "b")?;
    assert_eq!(lua.registry_value::<String>(&key)?, "b");

    lua.rollback(1);

    assert_eq!(lua.registry_value::<String>(&key)?, "a");

    lua.rollback(2);

    assert!(matches!(
        lua.registry_value::<String>(&key),
        Err(Error::MismatchedRegistryKey)
    ));

    Ok(())
}

#[test]
fn test_named_registry() -> Result<()> {
    let mut lua = Lua::new_rollback(20000, 2);

    lua.snap();

    lua.set_named_registry_value("test", "hello")?;
    assert_eq!(lua.named_registry_value::<_, String>("test")?, "hello");

    lua.rollback(1);

    assert_eq!(lua.named_registry_value::<_, Value>("test")?, Nil);

    Ok(())
}

#[test]
fn test_function_double_free_safety() -> Result<()> {
    let drop_test = DropTester::new();
    let checker = drop_test.create_checker();

    {
        let mut lua = Lua::new_rollback(20000, 2);

        lua.snap();

        lua.globals().set(
            "test",
            lua.create_function(move |_, _: ()| Ok(drop_test.create_checker().dropped()))?,
        )?;

        lua.snap();

        // set the function to nil to attempt to free it
        lua.globals().set("test", Nil)?;

        lua.gc_collect()?;
        lua.gc_collect()?;

        // roll back to just after the function was added and attempt to free again
        lua.rollback(1);

        assert!(lua.globals().get::<_, Value>("test")? != Nil);

        lua.globals().set("test", Nil)?;

        lua.gc_collect()?;
        lua.gc_collect()?;

        // roll back to before the function was created
        lua.rollback(2);
    }

    assert_eq!(checker.dropped(), true, "should have freed, memory leak");

    Ok(())
}

#[test]
fn test_scoped_function_double_free_safety() -> Result<()> {
    let drop_test = DropTester::new();
    let checker = drop_test.create_checker();

    {
        let mut lua = Lua::new_rollback(20000, 2);

        lua.snap();

        lua.scope(|scope| {
            lua.globals().set(
                "test",
                scope.create_function(move |_, _: ()| Ok(drop_test.create_checker().dropped()))?,
            )
        })?;

        lua.snap();

        // set the function to nil to attempt to free it
        lua.globals().set("test", Nil)?;

        lua.gc_collect()?;
        lua.gc_collect()?;

        // roll back to just after the function was added and attempt to free again
        lua.rollback(1);

        assert!(lua.globals().get::<_, Value>("test")? != Nil);

        lua.globals().set("test", Nil)?;

        lua.gc_collect()?;
        lua.gc_collect()?;

        // roll back to before the function was created
        lua.rollback(2);
    }

    assert_eq!(checker.dropped(), true, "should have freed, memory leak");

    Ok(())
}

#[test]
fn test_error_safety() -> rollback_mlua::Result<()> {
    let mut lua = Lua::new_rollback(20000, 2);

    lua.globals().set(
        "create_error",
        lua.create_function::<(), (), _>(|_, _: ()| {
            Err(Error::RuntimeError(String::from("test")))
        })?,
    )?;

    // catch error
    lua.load("_, err = pcall(create_error)").exec()?;

    lua.snap();

    // delete err
    lua.globals().set("err", Nil)?;
    lua.gc_collect()?;
    lua.gc_collect()?;

    lua.rollback(1);

    let err: Error = lua.globals().get("err")?;

    assert!(matches!(err, Error::CallbackError { .. }));

    // double free is a test failure
    // when this is fixed, maybe non scoped functions will no longer need to be stored in the registry
    Ok(())
}

#[test]
fn test_zero_snapshots() -> rollback_mlua::Result<()> {
    let drop_test = DropTester::new();
    let drop_checker = drop_test.create_checker();

    let lua = Lua::new();

    lua.globals().set(
        "test",
        lua.create_function(move |_, _: ()| Ok(drop_test.create_checker().dropped()))?,
    )?;

    // set the function to nil to attempt to free it
    lua.globals().set("test", Nil)?;

    lua.gc_collect()?;
    lua.gc_collect()?;

    // closure shouldve dropped
    assert_eq!(drop_checker.dropped(), true);

    Ok(())
}

#[test]
fn test_memory_error() -> rollback_mlua::Result<()> {
    // a bit of a stress test
    // going from one of the smallest vm sizes to 3x, testing to see if we ever fail to create a WrappedFailure

    let min_memory = {
        let lua = Lua::new_rollback(20000, 2);
        lua.used_memory() + 1024 // adding 1024, for some room to create the function
    };

    for memory_size in min_memory..min_memory * 3 {
        let lua = Lua::new_rollback(memory_size, 0);

        let func = lua.create_function(|lua, _: ()| -> rollback_mlua::Result<()> {
            let table_table = lua.create_table()?;

            loop {
                table_table.push(lua.create_table()?)?;
            }
        })?;

        // catch error
        let res = func.call::<_, ()>(());

        assert!(
            matches!(
                res,
                Err(Error::CallbackError {
                    traceback: _,
                    ref cause
                })
                if matches!(cause.as_ref(), &Error::MemoryError(_) )
            ),
            "{res:?}"
        );
    }

    Ok(())
}
