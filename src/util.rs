use crate::lua::LuaInner;
use crate::types::Callback;
use generational_arena::Index as GenerationalIndex;
use std::any::{Any, TypeId};
use std::ffi::CStr;
use std::fmt::Write;
use std::mem::MaybeUninit;
use std::os::raw::{c_char, c_int, c_void};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::{mem, ptr, slice};

use once_cell::sync::Lazy;
use rustc_hash::FxHashMap;

use crate::error::{Error, Result};
use crate::{ffi, Lua, UserDataRef};

static METATABLE_CACHE: Lazy<FxHashMap<TypeId, u8>> = Lazy::new(|| {
    let mut map = FxHashMap::with_capacity_and_hasher(32, Default::default());
    map.insert(TypeId::of::<Callback>(), 0);
    map.insert(TypeId::of::<WrappedFailure>(), 0);
    map.insert(TypeId::of::<String>(), 0);
    map
});

// Checks that Lua has enough free stack space and returns `Error::StackError` on failure.
#[inline]
pub unsafe fn check_stack(state: *mut ffi::lua_State, amount: c_int) -> Result<()> {
    if ffi::lua_checkstack(state, amount) == 0 {
        Err(Error::StackError)
    } else {
        Ok(())
    }
}

pub struct StackGuard {
    state: *mut ffi::lua_State,
    top: c_int,
    extra: c_int,
}

impl StackGuard {
    // Creates a StackGuard instance with record of the stack size, and on Drop will check the
    // stack size and drop any extra elements. If the stack size at the end is *smaller* than at
    // the beginning, this is considered a fatal logic error and will result in a panic.
    #[inline]
    pub unsafe fn new(state: *mut ffi::lua_State) -> StackGuard {
        StackGuard {
            state,
            top: ffi::lua_gettop(state),
            extra: 0,
        }
    }
}

impl Drop for StackGuard {
    fn drop(&mut self) {
        unsafe {
            let top = ffi::lua_gettop(self.state);
            if top < self.top + self.extra {
                panic!("{} too many stack values popped", self.top - top)
            }
            if top > self.top + self.extra {
                if self.extra > 0 {
                    ffi::lua_rotate(self.state, self.top + 1, self.extra);
                }
                ffi::lua_settop(self.state, self.top + self.extra);
            }
        }
    }
}

// Call a function that calls into the Lua API and may trigger a Lua error (longjmp) in a safe way.
// Wraps the inner function in a call to `lua_pcall`, so the inner function only has access to a
// limited lua stack. `nargs` is the same as the the parameter to `lua_pcall`, and `nresults` is
// always `LUA_MULTRET`. Provided function must *not* panic, and since it will generally be lonjmping,
// should not contain any values that implements Drop.
// Internally uses 2 extra stack spaces, and does not call checkstack.
pub unsafe fn protect_lua_call(
    state: *mut ffi::lua_State,
    nargs: c_int,
    f: unsafe extern "C" fn(*mut ffi::lua_State) -> c_int,
) -> Result<()> {
    let stack_start = ffi::lua_gettop(state) - nargs;

    ffi::lua_pushcfunction(state, error_traceback);
    ffi::lua_pushcfunction(state, f);
    if nargs > 0 {
        ffi::lua_rotate(state, stack_start + 1, 2);
    }

    let ret = ffi::lua_pcall(state, nargs, ffi::LUA_MULTRET, stack_start + 1);
    ffi::lua_remove(state, stack_start + 1);

    if ret == ffi::LUA_OK {
        Ok(())
    } else {
        Err(pop_error(state, ret))
    }
}

// Call a function that calls into the Lua API and may trigger a Lua error (longjmp) in a safe way.
// Wraps the inner function in a call to `lua_pcall`, so the inner function only has access to a
// limited lua stack. `nargs` and `nresults` are similar to the parameters of `lua_pcall`, but the
// given function return type is not the return value count, instead the inner function return
// values are assumed to match the `nresults` param. Provided function must *not* panic, and since it
// will generally be lonjmping, should not contain any values that implements Drop.
// Internally uses 3 extra stack spaces, and does not call checkstack.
pub unsafe fn protect_lua_closure<F, R>(
    state: *mut ffi::lua_State,
    nargs: c_int,
    nresults: c_int,
    f: F,
) -> Result<R>
where
    F: Fn(*mut ffi::lua_State) -> R,
    R: Copy,
{
    struct Params<F, R: Copy> {
        function: F,
        result: MaybeUninit<R>,
        nresults: c_int,
    }

    unsafe extern "C" fn do_call<F, R>(state: *mut ffi::lua_State) -> c_int
    where
        F: Fn(*mut ffi::lua_State) -> R,
        R: Copy,
    {
        let params = ffi::lua_touserdata(state, -1) as *mut Params<F, R>;
        ffi::lua_pop(state, 1);

        (*params).result.write(((*params).function)(state));

        if (*params).nresults == ffi::LUA_MULTRET {
            ffi::lua_gettop(state)
        } else {
            (*params).nresults
        }
    }

    let stack_start = ffi::lua_gettop(state) - nargs;

    ffi::lua_pushcfunction(state, error_traceback);
    ffi::lua_pushcfunction(state, do_call::<F, R>);
    if nargs > 0 {
        ffi::lua_rotate(state, stack_start + 1, 2);
    }

    let mut params = Params {
        function: f,
        result: MaybeUninit::uninit(),
        nresults,
    };

    ffi::lua_pushlightuserdata(state, &mut params as *mut Params<F, R> as *mut c_void);
    let ret = ffi::lua_pcall(state, nargs + 1, nresults, stack_start + 1);
    ffi::lua_remove(state, stack_start + 1);

    if ret == ffi::LUA_OK {
        // `LUA_OK` is only returned when the `do_call` function has completed successfully, so
        // `params.result` is definitely initialized.
        Ok(params.result.assume_init())
    } else {
        Err(pop_error(state, ret))
    }
}

// Pops an error off of the stack and returns it. The specific behavior depends on the type of the
// error at the top of the stack:
//   1) If the error is actually a WrappedPanic, this will continue the panic.
//   2) If the error on the top of the stack is actually a WrappedError, just returns it.
//   3) Otherwise, interprets the error as the appropriate lua error.
// Uses 2 stack spaces, does not call checkstack.
pub unsafe fn pop_error(state: *mut ffi::lua_State, err_code: c_int) -> Error {
    debug_assert!(
        err_code != ffi::LUA_OK && err_code != ffi::LUA_YIELD,
        "pop_error called with non-error return code"
    );

    match get_gc_userdata::<WrappedFailure>(state, -1, ptr::null()).as_mut() {
        Some(WrappedFailure::Error(err)) => {
            ffi::lua_pop(state, 1);
            err.clone()
        }
        Some(WrappedFailure::Panic(panic)) => {
            if let Some(p) = panic.take() {
                resume_unwind(p);
            } else {
                Error::PreviouslyResumedPanic
            }
        }
        _ => {
            let err_string = to_string(state, -1);
            ffi::lua_pop(state, 1);

            match err_code {
                ffi::LUA_ERRRUN => Error::RuntimeError(err_string),
                ffi::LUA_ERRSYNTAX => {
                    Error::SyntaxError {
                        // This seems terrible, but as far as I can tell, this is exactly what the
                        // stock Lua REPL does.
                        incomplete_input: err_string.ends_with("<eof>")
                            || err_string.ends_with("'<eof>'"),
                        message: err_string,
                    }
                }
                ffi::LUA_ERRERR => {
                    // This error is raised when the error handler raises an error too many times
                    // recursively, and continuing to trigger the error handler would cause a stack
                    // overflow. It is not very useful to differentiate between this and "ordinary"
                    // runtime errors, so we handle them the same way.
                    Error::RuntimeError(err_string)
                }
                ffi::LUA_ERRMEM => Error::MemoryError(err_string),
                #[cfg(any(feature = "lua53", feature = "lua52"))]
                ffi::LUA_ERRGCMM => Error::GarbageCollectorError(err_string),
                _ => panic!("unrecognized lua error code"),
            }
        }
    }
}

// Uses 3 (or 1 if unprotected) stack spaces, does not call checkstack.
#[inline(always)]
pub unsafe fn push_string(state: *mut ffi::lua_State, s: &[u8], protect: bool) -> Result<()> {
    if protect {
        protect_lua!(state, 0, 1, |state| {
            ffi::lua_pushlstring(state, s.as_ptr() as *const c_char, s.len());
        })
    } else {
        ffi::lua_pushlstring(state, s.as_ptr() as *const c_char, s.len());
        Ok(())
    }
}

// Uses 3 stack spaces, does not call checkstack.
#[inline]
pub unsafe fn push_table(
    state: *mut ffi::lua_State,
    narr: c_int,
    nrec: c_int,
    protect: bool,
) -> Result<()> {
    if protect {
        protect_lua!(state, 0, 1, |state| ffi::lua_createtable(state, narr, nrec))
    } else {
        ffi::lua_createtable(state, narr, nrec);
        Ok(())
    }
}

// Uses 4 stack spaces, does not call checkstack.
pub unsafe fn rawset_field<S>(state: *mut ffi::lua_State, table: c_int, field: &S) -> Result<()>
where
    S: AsRef<[u8]> + ?Sized,
{
    let field = field.as_ref();
    ffi::lua_pushvalue(state, table);
    protect_lua!(state, 2, 0, |state| {
        ffi::lua_pushlstring(state, field.as_ptr() as *const c_char, field.len());
        ffi::lua_rotate(state, -3, 2);
        ffi::lua_rawset(state, -3);
    })
}

// Internally uses 4 stack spaces, does not call checkstack.
#[inline]
unsafe fn push_userdata<T>(state: *mut ffi::lua_State, t: T, protect: bool) -> Result<()> {
    let ud = if protect {
        protect_lua!(state, 0, 1, |state| {
            ffi::lua_newuserdata(state, mem::size_of::<T>()) as *mut T
        })?
    } else {
        ffi::lua_newuserdata(state, mem::size_of::<T>()) as *mut T
    };
    ptr::write(ud, t);
    Ok(())
}

#[inline]
unsafe fn get_userdata<T>(state: *mut ffi::lua_State, index: c_int) -> *mut T {
    let ud = ffi::lua_touserdata(state, index) as *mut T;
    debug_assert!(!ud.is_null(), "userdata pointer is null");
    ud
}

#[inline]
pub(crate) unsafe fn get_userdata_ref(state: *mut ffi::lua_State, index: c_int) -> UserDataRef {
    let ud = ffi::lua_touserdata(state, index) as *const UserDataRef;
    debug_assert!(!ud.is_null(), "userdata pointer is null");
    *ud
}

// Pops the userdata off of the top of the stack and returns it to rust, invalidating the lua
// userdata and gives it the special "destructed" userdata metatable. Userdata must not have been
// previously invalidated, and this method does not check for this.
// Uses 1 extra stack space and does not call checkstack.
pub unsafe fn take_gc_userdata<T>(state: *mut ffi::lua_State) -> T {
    // We set the metatable of userdata on __gc to a special table with no __gc method and with
    // metamethods that trigger an error on access. We do this so that it will not be double
    // dropped, and also so that it cannot be used or identified as any particular userdata type
    // after the first call to __gc.
    get_destructed_userdata_metatable(state);
    ffi::lua_setmetatable(state, -2);
    let ud_ref = get_userdata_ref(state, -1);

    let lua_inner = &(*ud_ref.lua_inner);
    let ud = get_userdata::<T>(lua_inner.ref_thread, ud_ref.ref_index);
    let t = ptr::read(ud);
    lua_inner.drop_ref_index(ud_ref.ref_index);

    if lua_inner.memory.is_some() {
        let mut snapshot = lua_inner.snapshot.borrow_mut();
        snapshot.ud_recent_destruction.push(ud_ref.destructor_index);
    }

    ffi::lua_pop(state, 1);
    t
}

// Pushes the userdata and attaches a metatable with __gc method.
// Internally uses 4 stack spaces and one reference stack space, does not call checkstack.
pub unsafe fn push_gc_userdata<T: Any>(lua: &Lua, t: T, protect: bool) -> Result<()> {
    let state = lua.state;

    push_userdata(state, t, protect)?;

    // move to the refthread for GC control
    let lua_ref = lua.pop_ref();
    let ref_index = lua_ref.index;

    let mut ud_destructors = lua.user_data_destructors.borrow_mut();
    let destructor_index = if lua.max_snapshots() == 0 {
        // destructor doesn't need to be stored for non rollback vms
        GenerationalIndex::from_raw_parts(0, 0)
    } else {
        let lua_inner = lua.0;

        ud_destructors.insert(Box::new(move || {
            let lua_inner = &*lua_inner;
            let ud = get_userdata::<T>(lua_inner.ref_thread, ref_index);
            mem::drop(ptr::read(ud));
            lua_inner.drop_ref_index(ref_index);
        }))
    };

    let wrapped = UserDataRef {
        ref_index,
        destructor_index,
        lua_inner: lua.0,
    };

    if let Err(e) = push_userdata(state, wrapped, protect) {
        let ud = get_userdata::<T>(lua.ref_thread, ref_index);
        let _ = ptr::read(ud);
        ud_destructors.remove(destructor_index);

        return Err(e);
    }

    // forgetting after error detection
    // the ref should drop for errors
    mem::forget(lua_ref);

    get_gc_metatable::<T>(state);
    ffi::lua_setmetatable(state, -2);

    if lua.max_snapshots() > 0 {
        let mut snapshot = lua.snapshot.borrow_mut();
        snapshot.ud_recent_construction.push(destructor_index);
    }

    Ok(())
}

// Uses 2 stack spaces, does not call checkstack
pub unsafe fn get_gc_userdata<T: Any>(
    state: *mut ffi::lua_State,
    index: c_int,
    mt_ptr: *const c_void,
) -> *mut T {
    let ud_ref = ffi::lua_touserdata(state, index) as *const UserDataRef;
    if ud_ref.is_null() || ffi::lua_getmetatable(state, index) == 0 {
        return ptr::null_mut();
    }
    if !mt_ptr.is_null() {
        let ud_mt_ptr = ffi::lua_topointer(state, -1);
        ffi::lua_pop(state, 1);
        if !ptr::eq(ud_mt_ptr, mt_ptr) {
            return ptr::null_mut();
        }
    } else {
        get_gc_metatable::<T>(state);
        let res = ffi::lua_rawequal(state, -1, -2);
        ffi::lua_pop(state, 2);
        if res == 0 {
            return ptr::null_mut();
        }
    }

    ffi::lua_touserdata((*(*ud_ref).lua_inner).ref_thread, (*ud_ref).ref_index) as *mut T
}

pub unsafe extern "C" fn userdata_destructor<T>(state: *mut ffi::lua_State) -> c_int {
    // It's probably NOT a good idea to catch Rust panics in finalizer
    // Lua 5.4 ignores it, other versions generates `LUA_ERRGCMM` without calling message handler
    let ud_ref = get_userdata_ref(state, -1);
    let lua_inner = &*(ud_ref.lua_inner);

    if lua_inner.memory.is_some() {
        let mut snapshot = lua_inner.snapshot.borrow_mut();
        snapshot
            .ud_pending_destruction
            .push(ud_ref.destructor_index);
    } else {
        take_gc_userdata::<T>(state);
    }

    0
}

// In the context of a lua callback, this will call the given function and if the given function
// returns an error, *or if the given function panics*, this will result in a call to `lua_error` (a
// longjmp). The error or panic is wrapped in such a way that when calling `pop_error` back on
// the Rust side, it will resume the panic.
//
// This function assumes the structure of the stack at the beginning of a callback, that the only
// elements on the stack are the arguments to the callback.
//
// This function uses some of the bottom of the stack for error handling, the given callback will be
// given the number of arguments available as an argument, and should return the number of returns
// as normal, but cannot assume that the arguments available start at 0.
pub unsafe fn callback_error<F, R>(state: *mut ffi::lua_State, f: F) -> R
where
    F: FnOnce(c_int) -> Result<R>,
{
    let nargs = ffi::lua_gettop(state);

    // We need 4 extra stack spaces to store preallocated memory and error/panic metatable
    let extra_stack = if nargs < 4 { 4 - nargs } else { 1 };

    ffi::luaL_checkstack(
        state,
        extra_stack,
        cstr!("not enough stack space for callback error handling"),
    );

    // We cannot shadow Rust errors with Lua ones, we pre-allocate enough memory
    // to store a wrapped error or panic *before* we proceed.
    let temp_lua = TempLua::from_state(state);

    ffi::luaL_checkstack(
        temp_lua.lua.ref_thread,
        1,
        cstr!("not enough reference stack space for callback error handling"),
    );

    if let Err(_) = push_gc_userdata(&temp_lua.lua, WrappedFailure::None, true) {
        ffi::lua_pushnil(state);
        ffi::lua_error(state);
    }

    let wrapped_error = get_gc_userdata::<WrappedFailure>(state, -1, ptr::null());

    ffi::lua_rotate(state, 1, 1);

    match catch_unwind(AssertUnwindSafe(|| f(nargs))) {
        Ok(Ok(r)) => {
            // dropping user data to reduce ref_thread usage
            ffi::lua_rotate(state, 1, -1);
            take_gc_userdata::<WrappedFailure>(state);
            r
        }
        Ok(Err(err)) => {
            ffi::lua_settop(state, 1);

            // Build `CallbackError` with traceback
            let traceback = if ffi::lua_checkstack(state, ffi::LUA_TRACEBACK_STACK) != 0 {
                ffi::luaL_traceback(state, state, ptr::null(), 0);
                let traceback = to_string(state, -1);
                ffi::lua_pop(state, 1);
                traceback
            } else {
                "<not enough stack space for traceback>".to_string()
            };
            let cause = Arc::new(err);
            ptr::write(
                wrapped_error,
                WrappedFailure::Error(Error::CallbackError { traceback, cause }),
            );

            ffi::lua_error(state)
        }
        Err(p) => {
            ffi::lua_settop(state, 1);
            ptr::write(wrapped_error, WrappedFailure::Panic(Some(p)));
            ffi::lua_error(state)
        }
    }
}

pub unsafe extern "C" fn error_traceback(state: *mut ffi::lua_State) -> c_int {
    if ffi::lua_checkstack(state, 2) == 0 {
        // If we don't have enough stack space to even check the error type, do
        // nothing so we don't risk shadowing a rust panic.
        return 1;
    }

    if get_gc_userdata::<WrappedFailure>(state, -1, ptr::null()).is_null() {
        let s = ffi::luaL_tolstring(state, -1, ptr::null_mut());
        if ffi::lua_checkstack(state, ffi::LUA_TRACEBACK_STACK) != 0 {
            ffi::luaL_traceback(state, state, s, 0);
            ffi::lua_remove(state, -2);
        }
    }

    1
}

// A variant of `pcall` that does not allow Lua to catch Rust panics from `callback_error`.
pub unsafe extern "C" fn safe_pcall(state: *mut ffi::lua_State) -> c_int {
    ffi::luaL_checkstack(state, 2, ptr::null());

    let top = ffi::lua_gettop(state);
    if top == 0 {
        ffi::lua_pushstring(state, cstr!("not enough arguments to pcall"));
        ffi::lua_error(state);
    }

    if ffi::lua_pcall(state, top - 1, ffi::LUA_MULTRET, 0) == ffi::LUA_OK {
        ffi::lua_pushboolean(state, 1);
        ffi::lua_insert(state, 1);
        ffi::lua_gettop(state)
    } else {
        if let Some(WrappedFailure::Panic(_)) =
            get_gc_userdata::<WrappedFailure>(state, -1, ptr::null()).as_ref()
        {
            ffi::lua_error(state);
        }
        ffi::lua_pushboolean(state, 0);
        ffi::lua_insert(state, -2);
        2
    }
}

// A variant of `xpcall` that does not allow Lua to catch Rust panics from `callback_error`.
pub unsafe extern "C" fn safe_xpcall(state: *mut ffi::lua_State) -> c_int {
    unsafe extern "C" fn xpcall_msgh(state: *mut ffi::lua_State) -> c_int {
        ffi::luaL_checkstack(state, 2, ptr::null());

        if let Some(WrappedFailure::Panic(_)) =
            get_gc_userdata::<WrappedFailure>(state, -1, ptr::null()).as_ref()
        {
            1
        } else {
            ffi::lua_pushvalue(state, ffi::lua_upvalueindex(1));
            ffi::lua_insert(state, 1);
            ffi::lua_call(state, ffi::lua_gettop(state) - 1, ffi::LUA_MULTRET);
            ffi::lua_gettop(state)
        }
    }

    ffi::luaL_checkstack(state, 2, ptr::null());

    let top = ffi::lua_gettop(state);
    if top < 2 {
        ffi::lua_pushstring(state, cstr!("not enough arguments to xpcall"));
        ffi::lua_error(state);
    }

    ffi::lua_pushvalue(state, 2);
    ffi::lua_pushcclosure(state, xpcall_msgh, 1);
    ffi::lua_copy(state, 1, 2);
    ffi::lua_replace(state, 1);

    if ffi::lua_pcall(state, ffi::lua_gettop(state) - 2, ffi::LUA_MULTRET, 1) == ffi::LUA_OK {
        ffi::lua_pushboolean(state, 1);
        ffi::lua_insert(state, 2);
        ffi::lua_gettop(state) - 1
    } else {
        if let Some(WrappedFailure::Panic(_)) =
            get_gc_userdata::<WrappedFailure>(state, -1, ptr::null()).as_ref()
        {
            ffi::lua_error(state);
        }
        ffi::lua_pushboolean(state, 0);
        ffi::lua_insert(state, -2);
        2
    }
}

// Initialize the internal (with __gc method) metatable for a type T.
// Uses 6 stack spaces and calls checkstack.
pub unsafe fn init_gc_metatable<T: Any>(
    state: *mut ffi::lua_State,
    customize_fn: Option<fn(*mut ffi::lua_State) -> Result<()>>,
) -> Result<()> {
    check_stack(state, 6)?;

    push_table(state, 0, 3, true)?;

    ffi::lua_pushcfunction(state, userdata_destructor::<T>);
    rawset_field(state, -2, "__gc")?;

    ffi::lua_pushboolean(state, 0);
    rawset_field(state, -2, "__metatable")?;

    if let Some(f) = customize_fn {
        f(state)?;
    }

    let type_id = TypeId::of::<T>();
    let ref_addr = &METATABLE_CACHE[&type_id] as *const u8;
    protect_lua!(state, 1, 0, |state| {
        ffi::lua_rawsetp(state, ffi::LUA_REGISTRYINDEX, ref_addr as *const c_void);
    })?;

    Ok(())
}

pub unsafe fn get_gc_metatable<T: Any>(state: *mut ffi::lua_State) {
    let type_id = TypeId::of::<T>();
    let ref_addr = METATABLE_CACHE
        .get(&type_id)
        .expect("gc metatable does not exist") as *const u8;
    ffi::lua_rawgetp(state, ffi::LUA_REGISTRYINDEX, ref_addr as *const c_void);
}

unsafe fn lua_inner(state: *mut ffi::lua_State) -> *mut LuaInner {
    let lua_inner_key = &LUA_INNER_KEY as *const u8 as *const c_void;
    ffi::lua_rawgetp(state, ffi::LUA_REGISTRYINDEX, lua_inner_key);
    let lua_inner = ffi::lua_touserdata(state, -1) as *mut LuaInner;
    ffi::lua_pop(state, 1);

    lua_inner
}

pub unsafe fn init_lua_inner_registry(lua: &Lua) -> Result<()> {
    let state = lua.state;
    check_stack(state, 2)?;

    ffi::lua_pushlightuserdata(state, lua.0 as *mut c_void);

    protect_lua!(state, 1, 0, fn(state) {
        let lua_inner_key = &LUA_INNER_KEY as *const u8 as *const c_void;
        ffi::lua_rawsetp(state, ffi::LUA_REGISTRYINDEX, lua_inner_key);
    })?;

    Ok(())
}

// Initialize the error, panic, and destructed userdata metatables.
pub unsafe fn init_error_registry(lua: &Lua) -> Result<()> {
    let state = lua.state;
    check_stack(state, 7)?;

    // Create error and panic metatables

    unsafe extern "C" fn error_tostring(state: *mut ffi::lua_State) -> c_int {
        callback_error(state, |_| {
            check_stack(state, 3)?;

            let err_buf = match get_gc_userdata::<WrappedFailure>(state, -1, ptr::null()).as_ref() {
                Some(WrappedFailure::Error(error)) => {
                    let err_buf_key = &ERROR_PRINT_BUFFER_KEY as *const u8 as *const c_void;
                    ffi::lua_rawgetp(state, ffi::LUA_REGISTRYINDEX, err_buf_key);
                    let err_buf = get_gc_userdata::<String>(state, -1, ptr::null());
                    ffi::lua_pop(state, 2);

                    (*err_buf).clear();
                    // Depending on how the API is used and what error types scripts are given, it may
                    // be possible to make this consume arbitrary amounts of memory (for example, some
                    // kind of recursive error structure?)
                    let _ = write!(&mut (*err_buf), "{}", error);
                    Ok(err_buf)
                }
                Some(WrappedFailure::Panic(Some(ref panic))) => {
                    let err_buf_key = &ERROR_PRINT_BUFFER_KEY as *const u8 as *const c_void;
                    ffi::lua_rawgetp(state, ffi::LUA_REGISTRYINDEX, err_buf_key);
                    let err_buf = get_gc_userdata::<String>(state, -1, ptr::null());
                    (*err_buf).clear();
                    ffi::lua_pop(state, 2);

                    if let Some(msg) = panic.downcast_ref::<&str>() {
                        let _ = write!(&mut (*err_buf), "{}", msg);
                    } else if let Some(msg) = panic.downcast_ref::<String>() {
                        let _ = write!(&mut (*err_buf), "{}", msg);
                    } else {
                        let _ = write!(&mut (*err_buf), "<panic>");
                    };
                    Ok(err_buf)
                }
                Some(WrappedFailure::Panic(None)) => Err(Error::PreviouslyResumedPanic),
                _ => {
                    // I'm not sure whether this is possible to trigger without bugs in mlua?
                    Err(Error::UserDataTypeMismatch)
                }
            }?;

            push_string(state, (*err_buf).as_bytes(), true)?;
            (*err_buf).clear();

            Ok(1)
        })
    }

    init_gc_metatable::<WrappedFailure>(
        state,
        Some(|state| {
            ffi::lua_pushcfunction(state, error_tostring);
            rawset_field(state, -2, "__tostring")
        }),
    )?;

    // Create destructed userdata metatable

    unsafe extern "C" fn destructed_error(state: *mut ffi::lua_State) -> c_int {
        callback_error(state, |_| Err(Error::CallbackDestructed))
    }

    push_table(state, 0, 26, true)?;
    ffi::lua_pushcfunction(state, destructed_error);
    for &method in &[
        "__add",
        "__sub",
        "__mul",
        "__div",
        "__mod",
        "__pow",
        "__unm",
        #[cfg(any(feature = "lua54", feature = "lua53"))]
        "__idiv",
        #[cfg(any(feature = "lua54", feature = "lua53"))]
        "__band",
        #[cfg(any(feature = "lua54", feature = "lua53"))]
        "__bor",
        #[cfg(any(feature = "lua54", feature = "lua53"))]
        "__bxor",
        #[cfg(any(feature = "lua54", feature = "lua53"))]
        "__bnot",
        #[cfg(any(feature = "lua54", feature = "lua53"))]
        "__shl",
        #[cfg(any(feature = "lua54", feature = "lua53"))]
        "__shr",
        "__concat",
        "__len",
        "__eq",
        "__lt",
        "__le",
        "__index",
        "__newindex",
        "__call",
        "__tostring",
        #[cfg(any(feature = "lua54", feature = "lua53", feature = "lua52"))]
        "__pairs",
        #[cfg(any(feature = "lua53", feature = "lua52"))]
        "__ipairs",
        #[cfg(feature = "lua54")]
        "__close",
    ] {
        ffi::lua_pushvalue(state, -1);
        rawset_field(state, -3, method)?;
    }
    ffi::lua_pop(state, 1);

    protect_lua!(state, 1, 0, fn(state) {
        let destructed_mt_key = &DESTRUCTED_USERDATA_METATABLE as *const u8 as *const c_void;
        ffi::lua_rawsetp(state, ffi::LUA_REGISTRYINDEX, destructed_mt_key);
    })?;

    // Create error print buffer
    init_gc_metatable::<String>(state, None)?;
    push_gc_userdata(lua, String::new(), true)?;
    protect_lua!(state, 1, 0, fn(state) {
        let err_buf_key = &ERROR_PRINT_BUFFER_KEY as *const u8 as *const c_void;
        ffi::lua_rawsetp(state, ffi::LUA_REGISTRYINDEX, err_buf_key);
    })?;

    Ok(())
}

pub(crate) enum WrappedFailure {
    None,
    Error(Error),
    Panic(Option<Box<dyn Any + Send + 'static>>),
}

struct TempLua {
    lua: Lua,
}

impl TempLua {
    unsafe fn from_state(state: *mut ffi::lua_State) -> Self {
        Self {
            lua: Lua(lua_inner(state)),
        }
    }
}

impl Drop for TempLua {
    fn drop(&mut self) {
        self.lua.0 = ptr::null_mut();
    }
}

// Converts the given lua value to a string in a reasonable format without causing a Lua error or
// panicking.
pub(crate) unsafe fn to_string(state: *mut ffi::lua_State, index: c_int) -> String {
    match ffi::lua_type(state, index) {
        ffi::LUA_TNONE => "<none>".to_string(),
        ffi::LUA_TNIL => "<nil>".to_string(),
        ffi::LUA_TBOOLEAN => (ffi::lua_toboolean(state, index) != 1).to_string(),
        ffi::LUA_TLIGHTUSERDATA => {
            format!("<lightuserdata {:?}>", ffi::lua_topointer(state, index))
        }
        ffi::LUA_TNUMBER => {
            let mut isint = 0;
            let i = ffi::lua_tointegerx(state, -1, &mut isint);
            if isint == 0 {
                ffi::lua_tonumber(state, index).to_string()
            } else {
                i.to_string()
            }
        }
        ffi::LUA_TSTRING => {
            let mut size = 0;
            // This will not trigger a 'm' error, because the reference is guaranteed to be of
            // string type
            let data = ffi::lua_tolstring(state, index, &mut size);
            String::from_utf8_lossy(slice::from_raw_parts(data as *const u8, size)).into_owned()
        }
        ffi::LUA_TTABLE => format!("<table {:?}>", ffi::lua_topointer(state, index)),
        ffi::LUA_TFUNCTION => format!("<function {:?}>", ffi::lua_topointer(state, index)),
        ffi::LUA_TUSERDATA => format!("<userdata {:?}>", ffi::lua_topointer(state, index)),
        ffi::LUA_TTHREAD => format!("<thread {:?}>", ffi::lua_topointer(state, index)),
        _ => "<unknown>".to_string(),
    }
}

pub(crate) unsafe fn get_destructed_userdata_metatable(state: *mut ffi::lua_State) {
    let key = &DESTRUCTED_USERDATA_METATABLE as *const u8 as *const c_void;
    ffi::lua_rawgetp(state, ffi::LUA_REGISTRYINDEX, key);
}

pub(crate) unsafe fn ptr_to_cstr_bytes<'a>(input: *const c_char) -> Option<&'a [u8]> {
    if input.is_null() {
        return None;
    }
    Some(CStr::from_ptr(input).to_bytes())
}

static DESTRUCTED_USERDATA_METATABLE: u8 = 0;
static ERROR_PRINT_BUFFER_KEY: u8 = 0;
static LUA_INNER_KEY: u8 = 0;
