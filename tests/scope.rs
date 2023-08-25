use rollback_mlua::{Error, Function, Lua, Result, String};
use std::cell::Cell;
use std::rc::Rc;

#[test]
fn test_scope_func() -> Result<()> {
    let lua = Lua::new();

    let rc = Rc::new(Cell::new(0));
    lua.scope(|scope| {
        let r = rc.clone();
        let f = scope.create_function(move |_, ()| {
            r.set(42);
            Ok(())
        })?;
        lua.globals().set("bad", f.clone())?;
        f.call::<_, ()>(())?;
        assert_eq!(Rc::strong_count(&rc), 2);
        Ok(())
    })?;
    assert_eq!(rc.get(), 42);
    assert_eq!(Rc::strong_count(&rc), 1);

    match lua.globals().get::<_, Function>("bad")?.call::<_, ()>(()) {
        Err(Error::CallbackError { ref cause, .. }) => match *cause.as_ref() {
            Error::CallbackDestructed => {}
            ref err => panic!("wrong error type {:?}", err),
        },
        r => panic!("improper return for destructed function: {:?}", r),
    };

    Ok(())
}

#[test]
fn test_scope_capture() -> Result<()> {
    let lua = Lua::new();

    let mut i = 0;
    lua.scope(|scope| {
        scope
            .create_function_mut(|_, ()| {
                i = 42;
                Ok(())
            })?
            .call::<_, ()>(())
    })?;
    assert_eq!(i, 42);

    Ok(())
}

#[test]
fn test_scope_outer_lua_access() -> Result<()> {
    let lua = Lua::new();

    let table = lua.create_table()?;
    lua.scope(|scope| {
        scope
            .create_function_mut(|_, ()| table.set("a", "b"))?
            .call::<_, ()>(())
    })?;
    assert_eq!(table.get::<_, String>("a")?, "b");

    Ok(())
}
