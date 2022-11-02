use std::any::Any;
use std::cell::{Cell, RefCell};
use std::marker::PhantomData;
use std::mem;

use crate::error::{Error, Result};
use crate::ffi;
use crate::function::Function;
use crate::lua::Lua;
use crate::types::{Callback, LuaRef};

use crate::util::{check_stack, take_gc_userdata, StackGuard};
use crate::value::{FromLuaMulti, ToLuaMulti};

/// Constructed by the [`Lua::scope`] method, allows temporarily creating Lua userdata and
/// callbacks that are not required to be Send or 'static.
///
/// See [`Lua::scope`] for more details.
///
/// [`Lua::scope`]: crate::Lua.html::scope
pub struct Scope<'lua, 'scope> {
    lua: &'lua Lua,
    destructors: RefCell<Vec<(LuaRef<'lua>, DestructorCallback<'lua>)>>,
    _scope_invariant: PhantomData<Cell<&'scope ()>>,
}

type DestructorCallback<'lua> = Box<dyn Fn(LuaRef<'lua>) -> Vec<Box<dyn Any>> + 'lua>;

impl<'lua, 'scope> Scope<'lua, 'scope> {
    pub(crate) fn new(lua: &'lua Lua) -> Scope<'lua, 'scope> {
        Scope {
            lua,
            destructors: RefCell::new(Vec::new()),
            _scope_invariant: PhantomData,
        }
    }

    /// Wraps a Rust function or closure, creating a callable Lua function handle to it.
    ///
    /// This is a version of [`Lua::create_function`] that creates a callback which expires on
    /// scope drop. See [`Lua::scope`] for more details.
    ///
    /// [`Lua::create_function`]: crate::Lua::create_function
    /// [`Lua::scope`]: crate::Lua::scope
    pub fn create_function<'callback, A, R, F>(&'callback self, func: F) -> Result<Function<'lua>>
    where
        A: FromLuaMulti<'callback>,
        R: ToLuaMulti<'callback>,
        F: 'scope + Fn(&'callback Lua, A) -> Result<R>,
    {
        // Safe, because 'scope must outlive 'callback (due to Self containing 'scope), however the
        // callback itself must be 'scope lifetime, so the function should not be able to capture
        // anything of 'callback lifetime. 'scope can't be shortened due to being invariant, and
        // the 'callback lifetime here can't be enlarged due to coming from a universal
        // quantification in Lua::scope.
        //
        // I hope I got this explanation right, but in any case this is tested with compiletest_rs
        // to make sure callbacks can't capture handles with lifetime outside the scope, inside the
        // scope, and owned inside the callback itself.
        unsafe {
            self.create_callback(Box::new(move |lua, args| {
                func(lua, A::from_lua_multi(args, lua)?)?.to_lua_multi(lua)
            }))
        }
    }

    /// Wraps a Rust mutable closure, creating a callable Lua function handle to it.
    ///
    /// This is a version of [`Lua::create_function_mut`] that creates a callback which expires
    /// on scope drop. See [`Lua::scope`] and [`Scope::create_function`] for more details.
    ///
    /// [`Lua::create_function_mut`]: crate::Lua::create_function_mut
    /// [`Lua::scope`]: crate::Lua::scope
    /// [`Scope::create_function`]: #method.create_function
    pub fn create_function_mut<'callback, A, R, F>(
        &'callback self,
        func: F,
    ) -> Result<Function<'lua>>
    where
        A: FromLuaMulti<'callback>,
        R: ToLuaMulti<'callback>,
        F: 'scope + FnMut(&'callback Lua, A) -> Result<R>,
    {
        let func = RefCell::new(func);
        self.create_function(move |lua, args| {
            (*func
                .try_borrow_mut()
                .map_err(|_| Error::RecursiveMutCallback)?)(lua, args)
        })
    }

    // Unsafe, because the callback can improperly capture any value with 'callback scope, such as
    // improperly capturing an argument. Since the 'callback lifetime is chosen by the user and the
    // lifetime of the callback itself is 'scope (non-'static), the borrow checker will happily pick
    // a 'callback that outlives 'scope to allow this. In order for this to be safe, the callback
    // must NOT capture any parameters.
    unsafe fn create_callback<'callback>(
        &self,
        f: Callback<'callback, 'scope>,
    ) -> Result<Function<'lua>> {
        let f = mem::transmute::<Callback<'callback, 'scope>, Callback<'lua, 'static>>(f);
        let f = self.lua.create_callback(f)?;

        let destructor: DestructorCallback = Box::new(|f| {
            let state = f.lua.state;
            let _sg = StackGuard::new(state);
            check_stack(state, 3).unwrap();

            f.lua.push_ref(&f);

            // We know the destructor has not run yet because we hold a reference to the callback.

            ffi::lua_getupvalue(state, -1, 1);
            let ud = take_gc_userdata::<Callback>(state);
            ffi::lua_pushnil(state);
            ffi::lua_setupvalue(state, -2, 1);

            vec![Box::new(ud)]
        });
        self.destructors
            .borrow_mut()
            .push((f.0.clone(), destructor));

        Ok(f)
    }
}

impl<'lua, 'scope> Drop for Scope<'lua, 'scope> {
    fn drop(&mut self) {
        // We separate the action of invalidating the userdata in Lua and actually dropping the
        // userdata type into two phases. This is so that, in the event a userdata drop panics, we
        // can be sure that all of the userdata in Lua is actually invalidated.

        // All destructors are non-panicking, so this is fine
        let to_drop = self
            .destructors
            .get_mut()
            .drain(..)
            .flat_map(|(r, dest)| dest(r))
            .collect::<Vec<_>>();

        drop(to_drop);
    }
}
