# Rollback mlua

Slimmed down [mlua](https://github.com/khvzak/mlua) built for rollback, anything unsafe for rolling back has been removed.

## Major Differences:

- LuaJIT + Luau support dropped
- `Lua::new_rollback(memory_size: usize, max_snapshots: usize)`
- `lua.snap()`
- `lua.rollback(n: usize)` where n should be 1..max_snapshots
- Custom UserData/LightUserData support has been dropped
- Removed coroutines/threads/async from mlua reducing feature scope

## How is safety provided?

lua.snap() and lua.rollback(n) take a mutable reference to lua, other functions are immutable.
Base mlua has no mutable functions on relevant structs.
Rust does not allow for mutable and immutable references to overlap.

```rust
fn main() -> LuaResult<()> {
    let mut lua = Lua::new_rollback(1024 * 1024, 1);

    lua.snap();

    let table = lua.create_table()?;
    table.set("value", 1)?;

    // Rollback to before the table was created
    lua.rollback(1);

    // Does not compile, lua.rollback() requires mutable reference, table is an immutable reference to lua.
    let value = table.get("value")?;
    println!("{}", value);

    Ok(())
}
```

Callbacks provide immutable access to lua preventing snapshot/rollback within a callback.

```rust
fn main() -> LuaResult<()> {
    let mut lua = Lua::new_rollback(1024 * 1024, 1);

    lua.snap();

    lua.create_function(|lua: &Lua, _: ()| {
        // Does not compile, lua is immutable
        lua.rollback(1);
        Ok(())
    })?.call(());

    lua.create_function(|_, _: ()| {
        // Does not compile, can't take mutable reference to lua while create_function requires immutable reference
        // Even if it did take a mutable reference, Rust does not allow for more than one mutable reference at a time
        lua.rollback(1);
        Ok(())
    })?.call(());

    lua.create_function(move |_, _: ()| {
        // Does not compile, we can't move lua into lua
        lua.rollback(1);
        Ok(())
    })?.call(());

    Ok(())
}
```

Registry keys are the only way to keep a reference to a value past the mutable function line.
However rolling back to a snapshot

```rust
// a test exists for this:
// tests/rollback.rs: fn test_registry()

fn main() -> LuaResult<()> {
    let mut lua = Lua::new_rollback(1024 * 1024, 1);

    lua.snap();

    let key = lua.create_registry_value(1)?;

    lua.snap();

    // Rollback to a safe point
    lua.rollback(1);

    // Perfectly valid use
    lua.registry_value(&key)?;

    // Rollback to before the value was created
    lua.rollback(1);

    // Returns an Err, as the registry key has been invalidated
    lua.registry_value(&key)?;

    Ok(())
}
```

Removal of UserData prevents use after free and double frees from external values.
As a bonus it also prevents external values ignoring rollback, sadly Rust closures will still have this issue.

```rust
// Example purposes only, this does not compile as the UserData trait has been removed

struct MyUserData(Arc<i32>);

impl UserData for MyUserData {}

fn main() -> LuaResult<()> {
    let mut lua = Lua::new_rollback(1024 * 1024, 1);

    // Arc points to data outside of lua's memory
    lua.globals().set("myobject", MyUserData(Arc::new(1)))?;

    lua.snap();

    // Modify the value
    let value: MyUserData = lua.globals().get("myobject")?;
    *value.0 = 2;

    // Roll to before the value was modified
    lua.rollback(1);

    // If this could compile, the value would not roll back since rollback only applies to memory in the VM
    let value: MyUserData = lua.globals().get("myobject")?;
    println!("{}", value.0); // 2

    // Free the object by setting to nil, then collecting garbage
    lua.globals().set("myobject", Nil)?;
    lua.gc_collect()?;
    lua.gc_collect()?;

    // Roll to before the value was deleted
    lua.rollback(1);

    let value: MyUserData = lua.globals().get("myobject")?;

    // Use after free! Good thing UserData has been removed
    println!("{}", value.0);

    // Double free! Good thing UserData has been removed
    lua.globals().set("myobject", Nil)?;
    lua.gc_collect()?;
    lua.gc_collect()?;

    Ok(())
}
```

Functions (and Errors) have usage tracked across snapshots to prevent double frees and memory leaks.

lua.create_function_mut has been removed as rust closures will not roll back (the data is not stored in the VM as functions are Boxed before passed to lua)

```rust
fn main() -> LuaResult<()> {
    let mut lua = Lua::new_rollback(1024 * 1024, 2);

    lua.snap();

    // create_function_mut has been removed, interior mutability is the only way around this protection
    let mut i = RefCell::new(0);
    let mut boxed_value: Box<i32> = Box::new(1);

    // create_function_mut has been removed
    let fun = lua.create_function(move |lua: &Lua, _: ()| {
        // This operation has issues with rollback!
        *i.borrow_mut() += 1;

        // Capturing boxed_value, as an example of how a double free could occur without protection
        Ok(*boxed_value)
    })?;

    lua.globals().set("fun", fun)?;

    lua.snap();

    // Free the object by setting to nil, then collecting garbage
    lua.globals().set("fun", Nil)?;
    lua.gc_collect()?;
    lua.gc_collect()?;

    lua.rollback(1);

    // No double free
    lua.globals().set("fun", Nil)?;
    lua.gc_collect()?;
    lua.gc_collect()?;

    // Rolling back to before the function was created, this will free the function
    lua.rollback(2);

    // Creating enough snapshots to destroy the last usage would also free the function
    lua.snap();
    lua.snap();

    // Destruction of Lua would also free the function
    Ok(())
}
```

## Known Remaining Issues

- Multithreading is untested, I'm uncertain of RegistryKey's usage of AtomicBool.
- StdLib::COROUTINE is available for use, however the test_multi_states test is not passing.
- Tests in `tests/compile` should be adjusted for mlua and rollback_mlua project differences.
