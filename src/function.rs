use std::mem;
use std::os::raw::{c_int, c_void};
use std::ptr;
use std::slice;

use crate::error::{Error, Result};
use crate::ffi;
use crate::lua_ref::LuaRef;
use crate::util::{check_stack, error_traceback, pop_error, ptr_to_cstr_bytes, StackGuard};
use crate::value::{FromLuaMulti, ToLuaMulti};

/// Handle to an internal Lua function.
#[derive(Clone, Debug)]
pub struct Function<'lua>(pub(crate) LuaRef<'lua>);

#[derive(Clone, Debug)]
pub struct FunctionInfo {
    pub name: Option<Vec<u8>>,
    pub name_what: Option<Vec<u8>>,
    pub what: Option<Vec<u8>>,
    pub source: Option<Vec<u8>>,
    pub short_src: Option<Vec<u8>>,
    pub line_defined: i32,
    pub last_line_defined: i32,
}

impl<'lua> Function<'lua> {
    /// Calls the function, passing `args` as function arguments.
    ///
    /// The function's return values are converted to the generic type `R`.
    ///
    /// # Examples
    ///
    /// Call Lua's built-in `tostring` function:
    ///
    /// ```
    /// # use rollback_mlua::{Function, Lua, Result};
    /// # fn main() -> Result<()> {
    /// # let lua = Lua::new();
    /// let globals = lua.globals();
    ///
    /// let tostring: Function = globals.get("tostring")?;
    ///
    /// assert_eq!(tostring.call::<_, String>(123)?, "123");
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Call a function with multiple arguments:
    ///
    /// ```
    /// # use rollback_mlua::{Function, Lua, Result};
    /// # fn main() -> Result<()> {
    /// # let lua = Lua::new();
    /// let sum: Function = lua.load(
    ///     r#"
    ///         function(a, b)
    ///             return a + b
    ///         end
    /// "#).eval()?;
    ///
    /// assert_eq!(sum.call::<_, u32>((3, 4))?, 3 + 4);
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn call<A: ToLuaMulti<'lua>, R: FromLuaMulti<'lua>>(&self, args: A) -> Result<R> {
        let lua = self.0.lua;

        let mut args = args.to_lua_multi(lua)?;
        let nargs = args.len() as c_int;

        let results = unsafe {
            let _sg = StackGuard::new(lua.state);
            check_stack(lua.state, nargs + 3)?;
            check_stack(lua.ref_thread, 1)?;

            ffi::lua_pushcfunction(lua.state, error_traceback);
            let stack_start = ffi::lua_gettop(lua.state);
            lua.push_ref(&self.0);
            for arg in args.drain_all() {
                lua.push_value(arg)?;
            }
            let ret = ffi::lua_pcall(lua.state, nargs, ffi::LUA_MULTRET, stack_start);
            if ret != ffi::LUA_OK {
                return Err(pop_error(lua.state, ret));
            }
            let nresults = ffi::lua_gettop(lua.state) - stack_start;
            let mut results = args; // Reuse MultiValue container
            check_stack(lua.state, 2)?;
            for _ in 0..nresults {
                results.push_front(lua.pop_value());
            }
            ffi::lua_pop(lua.state, 1);
            results
        };
        R::from_lua_multi(results, lua)
    }

    /// Returns a function that, when called, calls `self`, passing `args` as the first set of
    /// arguments.
    ///
    /// If any arguments are passed to the returned function, they will be passed after `args`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rollback_mlua::{Function, Lua, Result};
    /// # fn main() -> Result<()> {
    /// # let lua = Lua::new();
    /// let sum: Function = lua.load(
    ///     r#"
    ///         function(a, b)
    ///             return a + b
    ///         end
    /// "#).eval()?;
    ///
    /// let bound_a = sum.bind(1)?;
    /// assert_eq!(bound_a.call::<_, u32>(2)?, 1 + 2);
    ///
    /// let bound_a_and_b = sum.bind(13)?.bind(57)?;
    /// assert_eq!(bound_a_and_b.call::<_, u32>(())?, 13 + 57);
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn bind<A: ToLuaMulti<'lua>>(&self, args: A) -> Result<Function<'lua>> {
        unsafe extern "C" fn args_wrapper_impl(state: *mut ffi::lua_State) -> c_int {
            let nargs = ffi::lua_gettop(state);
            let nbinds = ffi::lua_tointeger(state, ffi::lua_upvalueindex(1)) as c_int;
            ffi::luaL_checkstack(state, nbinds, ptr::null());

            for i in 0..nbinds {
                ffi::lua_pushvalue(state, ffi::lua_upvalueindex(i + 2));
            }
            if nargs > 0 {
                ffi::lua_rotate(state, 1, nbinds);
            }

            nargs + nbinds
        }

        let lua = self.0.lua;

        let args = args.to_lua_multi(lua)?;
        let nargs = args.len() as c_int;

        if nargs == 0 {
            return Ok(self.clone());
        }

        if nargs + 1 > ffi::LUA_MAX_UPVALUES {
            return Err(Error::BindError);
        }

        let args_wrapper = unsafe {
            let _sg = StackGuard::new(lua.state);
            check_stack(lua.state, nargs + 3)?;

            ffi::lua_pushinteger(lua.state, nargs as ffi::lua_Integer);
            for arg in args {
                lua.push_value(arg)?;
            }
            protect_lua!(lua.state, nargs + 1, 1, fn(state) {
                ffi::lua_pushcclosure(state, args_wrapper_impl, ffi::lua_gettop(state));
            })?;

            Function(lua.pop_ref())
        };

        lua.load(&lua.compiled_bind_func)
            .set_name("_mlua_bind")?
            .call((self.clone(), args_wrapper))
    }

    /// Returns information about the function.
    ///
    /// Corresponds to the `>Sn` what mask for [`lua_getinfo`] when applied to the function.
    ///
    /// [`lua_getinfo`]: https://www.lua.org/manual/5.4/manual.html#lua_getinfo
    pub fn info(&self) -> FunctionInfo {
        let lua = self.0.lua;
        unsafe {
            let _sg = StackGuard::new(lua.state);
            check_stack(lua.state, 1).unwrap();

            let mut ar: ffi::lua_Debug = mem::zeroed();
            lua.push_ref(&self.0);
            let res = ffi::lua_getinfo(lua.state, cstr!(">Sn"), &mut ar);
            assert!(res != 0, "lua_getinfo failed with `>Sn`");

            FunctionInfo {
                name: ptr_to_cstr_bytes(ar.name).map(|s| s.to_vec()),
                name_what: ptr_to_cstr_bytes(ar.namewhat).map(|s| s.to_vec()),
                what: ptr_to_cstr_bytes(ar.what).map(|s| s.to_vec()),
                source: ptr_to_cstr_bytes(ar.source).map(|s| s.to_vec()),
                short_src: ptr_to_cstr_bytes(&ar.short_src as *const _).map(|s| s.to_vec()),
                line_defined: ar.linedefined as i32,
                last_line_defined: ar.lastlinedefined as i32,
            }
        }
    }

    /// Dumps the function as a binary chunk.
    ///
    /// If `strip` is true, the binary representation may not include all debug information
    /// about the function, to save space.
    ///
    /// For Luau a [Compiler] can be used to compile Lua chunks to bytecode.
    ///
    /// [Compiler]: crate::chunk::Compiler
    pub fn dump(&self, strip: bool) -> Vec<u8> {
        unsafe extern "C" fn writer(
            _state: *mut ffi::lua_State,
            buf: *const c_void,
            buf_len: usize,
            data: *mut c_void,
        ) -> c_int {
            let data = &mut *(data as *mut Vec<u8>);
            let buf = slice::from_raw_parts(buf as *const u8, buf_len);
            data.extend_from_slice(buf);
            0
        }

        let lua = self.0.lua;
        let mut data: Vec<u8> = Vec::new();
        unsafe {
            let _sg = StackGuard::new(lua.state);
            check_stack(lua.state, 1).unwrap();

            lua.push_ref(&self.0);
            let data_ptr = &mut data as *mut Vec<u8> as *mut c_void;
            let strip = if strip { 1 } else { 0 };
            ffi::lua_dump(lua.state, writer, data_ptr, strip);
            ffi::lua_pop(lua.state, 1);
        }

        data
    }
}

impl<'lua> PartialEq for Function<'lua> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
