use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::ffi::{CStr, CString};
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::os::raw::{c_char, c_int, c_void};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe, Location};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::{Arc, Mutex};
use std::{mem, ptr, str};

use rustc_hash::FxHashMap;
use slotmap::{DefaultKey as GenerationalIndex, SlotMap};

use crate::chunk::{AsChunk, Chunk, ChunkMode};
use crate::error::{Error, Result};
use crate::function::Function;
use crate::hook::Debug;
use crate::photographic_memory::PhotographicMemory;
use crate::registry::RegistryTracker;
use crate::scope::Scope;
use crate::stdlib::StdLib;
use crate::string::String;
use crate::table::Table;
use crate::thread::Thread;
use crate::types::{Callback, Integer, LightUserData, LuaRef, MaybeSend, Number, RegistryKey};
use crate::util::{
    self, check_stack, get_gc_metatable, get_gc_userdata, init_error_registry, init_gc_metatable,
    init_lua_inner_registry, pop_error, push_gc_userdata, push_string, push_table, rawset_field,
    safe_pcall, safe_xpcall, StackGuard, WrappedFailure,
};
use crate::value::{FromLua, FromLuaMulti, IntoLua, IntoLuaMulti, MultiValue, Nil, Value};

/// Mode of the Lua garbage collector (GC).
///
/// In Lua 5.4 GC can work in two modes: incremental and generational.
/// Previous Lua versions support only incremental GC.
///
/// More information can be found in the Lua [documentation].
///
/// [documentation]: https://www.lua.org/manual/5.4/manual.html#2.5
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GCMode {
    Incremental,
    /// Requires `feature = "lua54"`
    #[cfg(feature = "lua54")]
    #[cfg_attr(docsrs, doc(cfg(feature = "lua54")))]
    Generational,
}

#[derive(Clone)]
pub(crate) struct LuaSnapshot {
    app_data: RefCell<FxHashMap<TypeId, Rc<dyn Any>>>,
    ref_stack_top: c_int,
    ref_stack_size: c_int,
    ref_free: Vec<c_int>,
    registry_unref_list: Vec<c_int>,
    recent_key_validity: Vec<Arc<(c_int, AtomicBool)>>,
    pub(crate) ud_recent_construction: Vec<GenerationalIndex>,
    pub(crate) ud_pending_destruction: Vec<GenerationalIndex>,
    wrapped_failure_pool: Vec<c_int>,
    libs: StdLib,
    modified: bool,
}

impl LuaSnapshot {
    pub(crate) fn mark_modified(&mut self) {
        self.modified = true;
    }

    fn reset_tracking(&mut self) {
        self.modified = false;
        self.ud_recent_construction.clear();
        self.ud_pending_destruction.clear();
    }
}

pub struct LuaInner {
    pub(crate) state: AtomicPtr<ffi::lua_State>,
    pub(crate) ref_thread: *mut ffi::lua_State,
    wrapped_failure_mt_ptr: *const c_void,
    pub(crate) compiled_bind_func: Vec<u8>,
    registry_tracker: Arc<Mutex<RegistryTracker>>,
    pub(crate) snapshot: RefCell<LuaSnapshot>,
    snapshots: VecDeque<LuaSnapshot>,
    multivalue_pool: Vec<Vec<Value<'static>>>,
    pub(crate) user_data_destructors: RefCell<SlotMap<GenerationalIndex, Box<dyn FnOnce()>>>,
    pub(crate) memory: Option<Box<PhotographicMemory>>,
}

impl Drop for LuaInner {
    fn drop(&mut self) {
        unsafe { ffi::lua_close(self.state()) }
    }
}

impl LuaInner {
    #[inline(always)]
    pub(crate) fn state(&self) -> *mut ffi::lua_State {
        self.state.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub(crate) fn ref_thread(&self) -> *mut ffi::lua_State {
        self.ref_thread
    }

    pub(crate) fn drop_ref_index(&self, index: c_int) {
        unsafe {
            let mut snapshot = self.snapshot.borrow_mut();

            ffi::lua_pushnil(self.ref_thread);
            ffi::lua_replace(self.ref_thread, index);
            snapshot.ref_free.push(index);
        }
    }
}

const WRAPPED_FAILURE_POOL_SIZE: usize = 64;
const MULTIVALUE_POOL_SIZE: usize = 64;

#[repr(transparent)]
pub struct Lua(pub(crate) *mut LuaInner);

impl fmt::Debug for Lua {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Lua({:p})", self.state())
    }
}

impl Deref for Lua {
    type Target = LuaInner;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.0 }
    }
}

impl DerefMut for Lua {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.0 }
    }
}

impl Drop for Lua {
    fn drop(&mut self) {
        unsafe {
            if self.0.is_null() {
                // TempLua
                return;
            }

            if self.memory.is_some() {
                self.rollback_user_data_construction(self.snapshots.len() + 1);

                let destructors = mem::take(&mut *self.user_data_destructors.borrow_mut());
                for (_, destructor) in destructors {
                    destructor();
                }
            }

            let _ = Box::from_raw(self.0);
        }
    }
}

impl Default for Lua {
    #[inline]
    fn default() -> Self {
        Lua::new()
    }
}

impl Lua {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let state = unsafe { ffi::luaL_newstate() };

        assert!(!state.is_null(), "failed to instantiate Lua VM");

        Self::inner_new(state, None)
    }

    pub fn new_rollback(memory_size: usize, max_snapshots: usize) -> Self {
        unsafe extern "C-unwind" fn allocator(
            ud: *mut c_void,
            ptr: *mut c_void,
            osize: usize,
            nsize: usize,
        ) -> *mut c_void {
            let memory = &mut *(ud as *mut PhotographicMemory);
            memory.realloc(ptr, osize, nsize)
        }

        let memory = PhotographicMemory::new(memory_size, max_snapshots);
        let boxed_memory = Box::new(memory);

        let ud = Box::into_raw(boxed_memory);
        let state = unsafe { ffi::lua_newstate(allocator, ud as *mut c_void) };
        let memory = unsafe { Box::from_raw(ud) };

        assert!(!state.is_null(), "failed to instantiate Lua VM");

        Self::inner_new(state, Some(memory))
    }

    fn inner_new(state: *mut ffi::lua_State, memory: Option<Box<PhotographicMemory>>) -> Self {
        let start_stack = unsafe { ffi::lua_gettop(state) };
        let ref_thread = unsafe {
            let thread = ffi::lua_newthread(state);
            // store the thread in the registry to prevent garbage collection
            ffi::luaL_ref(state, ffi::LUA_REGISTRYINDEX);
            thread
        };

        let inner = Box::new(LuaInner {
            state: AtomicPtr::new(state),
            ref_thread,
            wrapped_failure_mt_ptr: ptr::null(),
            registry_tracker: Arc::new(Mutex::new(RegistryTracker::new())),
            snapshot: RefCell::new(LuaSnapshot {
                app_data: RefCell::new(FxHashMap::default()),
                ref_stack_top: unsafe { ffi::lua_gettop(ref_thread) },
                // We need 1 extra stack space to move values in and out of the ref stack.
                ref_stack_size: ffi::LUA_MINSTACK - 1,
                ref_free: Vec::new(),
                registry_unref_list: Vec::new(),
                recent_key_validity: Vec::new(),
                ud_recent_construction: Vec::new(),
                ud_pending_destruction: Vec::new(),
                wrapped_failure_pool: Vec::with_capacity(WRAPPED_FAILURE_POOL_SIZE),
                libs: StdLib::NONE,
                modified: true,
            }),
            snapshots: VecDeque::with_capacity(
                memory
                    .as_ref()
                    .map(|m| m.max_snapshots())
                    .unwrap_or_default(),
            ),
            multivalue_pool: Vec::with_capacity(MULTIVALUE_POOL_SIZE),
            memory,
            user_data_destructors: RefCell::new(SlotMap::new()),
            compiled_bind_func: Vec::new(),
        });

        let mut lua = Lua(Box::into_raw(inner));

        unsafe {
            init_lua_inner_registry(&lua).unwrap();
            init_error_registry(&lua).unwrap();

            // Create the internal metatables and place them in the registry
            // to prevent them from being garbage collected.

            init_gc_metatable::<Callback>(state, None).unwrap();

            // Init serde metatables
            #[cfg(feature = "serialize")]
            crate::serde::init_metatables(state).unwrap();

            ffi::luaL_requiref(state, cstr!("_G"), ffi::luaopen_base, 1);
            ffi::lua_pop(state, 1);

            let _sg = StackGuard::new(state);

            #[cfg(any(feature = "lua54", feature = "lua53", feature = "lua52"))]
            ffi::lua_rawgeti(state, ffi::LUA_REGISTRYINDEX, ffi::LUA_RIDX_GLOBALS);
            #[cfg(feature = "lua51")]
            ffi::lua_pushvalue(state, ffi::LUA_GLOBALSINDEX);

            ffi::lua_pushcfunction(state, safe_pcall);
            rawset_field(state, -2, "pcall").unwrap();

            ffi::lua_pushcfunction(state, safe_xpcall);
            rawset_field(state, -2, "xpcall").unwrap();
        }

        lua.wrapped_failure_mt_ptr = unsafe {
            get_gc_metatable::<WrappedFailure>(state);
            let ptr = ffi::lua_topointer(state, -1);
            ffi::lua_pop(state, 1);
            ptr
        };

        lua.compiled_bind_func = {
            // used in function.rs
            let mut bind_chunk = lua.load(
                r#"
            local func, args_wrapper = ...
            return function(...)
                return func(args_wrapper(...))
            end
            "#,
            );
            bind_chunk.compile();

            bind_chunk.source.unwrap().into_owned()
        };

        debug_assert_eq!(unsafe { ffi::lua_gettop(state) }, start_stack, "stack leak");

        lua
    }

    /// Loads the specified subset of the standard libraries into an existing Lua state.
    ///
    /// Use the [`StdLib`] flags to specify the libraries you want to load.
    ///
    /// [`StdLib`]: crate::StdLib
    pub fn load_from_std_lib(&self, libs: StdLib) -> Result<()> {
        unsafe {
            load_from_std_lib(self.state(), libs)?;
        }

        let curr_libs = {
            let mut snapshot = self.snapshot.borrow_mut();

            snapshot.libs |= libs;
            snapshot.mark_modified();

            snapshot.libs
        };

        // If `package` library loaded into a safe lua state then disable C modules
        if curr_libs.contains(StdLib::PACKAGE) {
            self.disable_c_modules()
                .expect("Error during disabling C modules");
        }

        Ok(())
    }

    /// Loads module `modname` into an existing Lua state using the specified entrypoint
    /// function.
    ///
    /// Internally calls the Lua function `func` with the string `modname` as an argument,
    /// sets the call result to `package.loaded[modname]` and returns copy of the result.
    ///
    /// If `package.loaded[modname]` value is not nil, returns copy of the value without
    /// calling the function.
    ///
    /// If the function does not return a non-nil value then this method assigns true to
    /// `package.loaded[modname]`.
    ///
    /// Behavior is similar to Lua's [`require`] function.
    ///
    /// [`require`]: https://www.lua.org/manual/5.4/manual.html#pdf-require
    pub fn load_from_function<'lua, T>(&'lua self, modname: &str, func: Function<'lua>) -> Result<T>
    where
        T: FromLua<'lua>,
    {
        let state = self.state();
        let loaded = unsafe {
            let _sg = StackGuard::new(state);
            check_stack(state, 2)?;
            protect_lua!(state, 0, 1, fn(state) {
                ffi::luaL_getsubtable(state, ffi::LUA_REGISTRYINDEX, cstr!("_LOADED"));
            })?;
            Table(self.pop_ref())
        };

        let modname = self.create_string(modname)?;
        let value = match loaded.raw_get(modname.clone())? {
            Value::Nil => {
                let result = match func.call(modname.clone())? {
                    Value::Nil => Value::Boolean(true),
                    res => res,
                };
                loaded.raw_set(modname, result.clone())?;
                result
            }
            res => res,
        };
        T::from_lua(value, self)
    }

    /// Unloads module `modname`.
    ///
    /// Removes module from the [`package.loaded`] table which allows to load it again.
    /// It does not support unloading binary Lua modules since they are internally cached and can be
    /// unloaded only by closing Lua state.
    ///
    /// [`package.loaded`]: https://www.lua.org/manual/5.4/manual.html#pdf-package.loaded
    pub fn unload(&self, modname: &str) -> Result<()> {
        let state = self.state();
        let loaded = unsafe {
            let _sg = StackGuard::new(state);
            check_stack(state, 2)?;
            protect_lua!(state, 0, 1, fn(state) {
                ffi::luaL_getsubtable(state, ffi::LUA_REGISTRYINDEX, cstr!("_LOADED"));
            })?;
            Table(self.pop_ref())
        };

        let modname = self.create_string(modname)?;
        loaded.raw_remove(modname)?;
        Ok(())
    }

    pub fn environment(&self) -> Result<Table> {
        self.snapshot.borrow_mut().mark_modified();

        unsafe {
            let state = self.state();
            let _sg = StackGuard::new(state);
            check_stack(state, 2)?;

            let mut ar: ffi::lua_Debug = mem::zeroed();

            let top = ffi::lua_gettop(state) as c_int;

            let mut level = 1;

            while ffi::lua_getstack(state, level, &mut ar) != 0 {
                let status = ffi::lua_getinfo(state, cstr!("ful"), &mut ar);
                debug_assert!(status != 0);

                for n in 0..ar.nups as i32 {
                    let name_str_ptr = ffi::lua_getupvalue(state, -1, n + 1);

                    let name = CStr::from_ptr(name_str_ptr);
                    let is_table = ffi::lua_istable(state, top + 2) == 1;

                    if is_table && name.to_bytes() == "_ENV".as_bytes() {
                        return Ok(Table(self.pop_ref()));
                    }

                    // take arg off
                    ffi::lua_settop(state, top + 1);
                }

                // take func off
                ffi::lua_settop(state, top);

                level += 1;
            }

            Ok(self.globals())
        }
    }

    pub fn max_snapshots(&self) -> usize {
        self.memory
            .as_ref()
            .map(|m| m.max_snapshots())
            .unwrap_or_default()
    }

    pub fn snap(&mut self) {
        let inner_self = self.deref_mut();

        let memory = match inner_self.memory.as_mut() {
            Some(memory) => memory,
            None => return,
        };

        if memory.max_snapshots() == 0 {
            return;
        } else if inner_self.snapshots.len() >= memory.max_snapshots() {
            // delete user data that's finally safe to delete
            let dropped_snapshot = inner_self.snapshots.pop_front().unwrap();

            let mut destructors = inner_self.user_data_destructors.borrow_mut();

            for index in dropped_snapshot.ud_pending_destruction {
                let destructor = destructors.remove(index).unwrap();
                destructor();
            }
        }

        let mut snapshot = inner_self.snapshot.borrow().clone();

        if snapshot.modified {
            // snap memory
            memory.snap();
        }

        {
            // snap registry tracker
            let mut registry_tracker = inner_self
                .registry_tracker
                .lock()
                .expect("unref list poisoned");
            snapshot.registry_unref_list = registry_tracker.unref_list.clone();
            snapshot.recent_key_validity.clear();

            mem::swap(
                &mut snapshot.recent_key_validity,
                &mut registry_tracker.recent_key_validity,
            );
        }

        inner_self.snapshots.push_back(snapshot);

        let mut current_snapshot = inner_self.snapshot.borrow_mut();
        current_snapshot.reset_tracking();
    }

    /// Roll back to a previous snap. n should be within the range: 1..=max_snapshots.
    ///
    /// Snapshots will be dropped when n > 1.
    pub fn rollback(&mut self, n: usize) {
        if self.memory.is_none() {
            log::error!("Not a rollback VM");
            return;
        };

        if self.snapshots.len() < n || n == 0 {
            log::error!(
                "Not enough snapshots to roll back: existing: {}, required: {}",
                self.snapshots.len(),
                n
            );
            return;
        }

        let index = self.snapshots.len() - n;

        self.rollback_user_data_construction(n);

        {
            // roll back registry tracker
            let state = self.state();
            let mut registry_tracker = self.registry_tracker.lock().expect("unref list poisoned");

            // invalidate current keys
            for validity in &registry_tracker.recent_key_validity {
                validity.1.store(false, Ordering::Relaxed);

                unsafe {
                    ffi::luaL_unref(state, ffi::LUA_REGISTRYINDEX, validity.0);
                }
            }

            // invalidate keys in dropped snapshots
            for snapshot in self.snapshots.range(index + 1..) {
                for validity in &snapshot.recent_key_validity {
                    validity.1.store(false, Ordering::Relaxed);

                    unsafe {
                        ffi::luaL_unref(state, ffi::LUA_REGISTRYINDEX, validity.0);
                    }
                }
            }

            // expecting a clear stack
            self.gc_collect().unwrap();
            self.gc_collect().unwrap();

            // clearing as these keys have been invalidated
            registry_tracker.recent_key_validity.clear();

            // rolling back unref list
            let previous_snapshot = &self.snapshots[index];
            registry_tracker.unref_list.clear();
            registry_tracker
                .unref_list
                .extend(previous_snapshot.registry_unref_list.iter().cloned());
        }

        let memory_n = {
            // roll back memory
            let mut memory_n = self
                .snapshots
                .range(index + 1..)
                .filter(|snapshot| snapshot.modified)
                .count();

            memory_n += 1;

            if !self.snapshot.borrow().modified && memory_n == 1 {
                // no need to roll back if no data has changed
                0
            } else {
                memory_n
            }
        };

        if memory_n > 0 {
            self.memory.as_mut().unwrap().rollback(memory_n);
        }

        {
            // roll back lua snapshot

            let previous_snapshot = self.snapshots[index].clone();

            let mut snapshot = self.snapshot.borrow_mut();

            *snapshot = previous_snapshot;
            snapshot.reset_tracking();
        }

        self.snapshots.resize_with(index + 1, || unreachable!());
    }

    fn rollback_user_data_construction(&self, n: usize) {
        // track user data that hasn't been deleted yet
        let mut current_snapshot = self.snapshot.borrow_mut();
        let mut ud_constructed = mem::take(&mut current_snapshot.ud_recent_construction);
        mem::drop(current_snapshot);

        if !self.snapshots.is_empty() {
            let start_index = (self.snapshots.len() as isize - n as isize) + 1;

            for snapshot in self.snapshots.range(start_index as usize..) {
                ud_constructed.extend(snapshot.ud_recent_construction.iter().cloned());
            }
        }

        // destructing ud
        let mut destructors = self.user_data_destructors.borrow_mut();

        for index in ud_constructed {
            if let Some(destructor) = destructors.remove(index) {
                destructor();
            }
        }
    }

    // /// Sets a 'hook' function that will periodically be called as Lua code executes.
    // ///
    // /// When exactly the hook function is called depends on the contents of the `triggers`
    // /// parameter, see [`HookTriggers`] for more details.
    // ///
    // /// The provided hook function can error, and this error will be propagated through the Lua code
    // /// that was executing at the time the hook was triggered. This can be used to implement a
    // /// limited form of execution limits by setting [`HookTriggers.every_nth_instruction`] and
    // /// erroring once an instruction limit has been reached.
    // ///
    // /// This method sets a hook function for the current thread of this Lua instance.
    // /// If you want to set a hook function for another thread (coroutine), use [`Thread::set_hook()`] instead.
    // ///
    // /// Please note you cannot have more than one hook function set at a time for this Lua instance.
    // ///
    // /// # Example
    // ///
    // /// Shows each line number of code being executed by the Lua interpreter.
    // ///
    // /// ```
    // /// # use rollback_mlua::{Lua, HookTriggers, Result};
    // /// # fn main() -> Result<()> {
    // /// let lua = Lua::new();
    // /// lua.set_hook(HookTriggers::EVERY_LINE, |_lua, debug| {
    // ///     println!("line {}", debug.curr_line());
    // ///     Ok(())
    // /// });
    // ///
    // /// lua.load(r#"
    // ///     local x = 2 + 3
    // ///     local y = x * 63
    // ///     local z = string.len(x..", "..y)
    // /// "#).exec()
    // /// # }
    // /// ```
    // ///
    // /// [`HookTriggers`]: crate::HookTriggers
    // /// [`HookTriggers.every_nth_instruction`]: crate::HookTriggers::every_nth_instruction
    // pub fn set_hook<F>(&self, triggers: HookTriggers, callback: F)
    // where
    //     F: Fn(&Lua, Debug) -> Result<()> + MaybeSend + 'static,
    // {
    //     unsafe { self.set_thread_hook(self.state(), triggers, callback) };
    // }

    // /// Sets a 'hook' function for a thread (coroutine).
    // pub(crate) unsafe fn set_thread_hook<F>(
    //     &self,
    //     state: *mut ffi::lua_State,
    //     triggers: HookTriggers,
    //     callback: F,
    // ) where
    //     F: Fn(&Lua, Debug) -> Result<()> + MaybeSend + 'static,
    // {
    //     unsafe extern "C-unwind" fn hook_proc(state: *mut ffi::lua_State, ar: *mut ffi::lua_Debug) {
    //         let extra = extra_data(state);
    //         if (*extra).hook_thread != state {
    //             // Hook was destined for a different thread, ignore
    //             ffi::lua_sethook(state, None, 0, 0);
    //             return;
    //         }
    //         callback_error_ext(state, extra, move |_| {
    //             let hook_cb = (*extra).hook_callback.clone();
    //             let hook_cb = mlua_expect!(hook_cb, "no hook callback set in hook_proc");
    //             if Arc::strong_count(&hook_cb) > 2 {
    //                 return Ok(()); // Don't allow recursion
    //             }
    //             let lua: &Lua = mem::transmute((*extra).inner.assume_init_ref());
    //             let _guard = StateGuard::new(&lua.0, state);
    //             let debug = Debug::new(lua, ar);
    //             hook_cb(lua, debug)
    //         })
    //     }

    //     (*self.extra.get()).hook_callback = Some(Arc::new(callback));
    //     (*self.extra.get()).hook_thread = state; // Mark for what thread the hook is set
    //     ffi::lua_sethook(state, Some(hook_proc), triggers.mask(), triggers.count());
    // }

    // /// Removes any hook previously set by [`Lua::set_hook()`] or [`Thread::set_hook()`].
    // ///
    // /// This function has no effect if a hook was not previously set.
    // pub fn remove_hook(&self) {
    //     unsafe {
    //         let state = self.state();
    //         ffi::lua_sethook(state, None, 0, 0);
    //         match get_main_state(self.main_state) {
    //             Some(main_state) if !ptr::eq(state, main_state) => {
    //                 // If main_state is different from state, remove hook from it too
    //                 ffi::lua_sethook(main_state, None, 0, 0);
    //             }
    //             _ => {}
    //         };
    //         (*self.extra.get()).hook_callback = None;
    //         (*self.extra.get()).hook_thread = ptr::null_mut();
    //     }
    // }

    // /// Sets the warning function to be used by Lua to emit warnings.
    // ///
    // /// Requires `feature = "lua54"`
    // #[cfg(feature = "lua54")]
    // #[cfg_attr(docsrs, doc(cfg(feature = "lua54")))]
    // pub fn set_warning_function<F>(&self, callback: F)
    // where
    //     F: Fn(&Lua, &str, bool) -> Result<()> + MaybeSend + 'static,
    // {
    //     unsafe extern "C-unwind" fn warn_proc(ud: *mut c_void, msg: *const c_char, tocont: c_int) {
    //         let extra = ud as *mut LuaInner;
    //         let lua: &Lua = mem::transmute((*extra).inner.assume_init_ref());
    //         callback_error_ext(lua.state(), extra, |_| {
    //             let cb = mlua_expect!(
    //                 (*extra).warn_callback.as_ref(),
    //                 "no warning callback set in warn_proc"
    //             );
    //             let msg = std::string::String::from_utf8_lossy(CStr::from_ptr(msg).to_bytes());
    //             cb(lua, &msg, tocont != 0)
    //         });
    //     }

    //     let state = self.main_state;
    //     unsafe {
    //         (*self.extra.get()).warn_callback = Some(Box::new(callback));
    //         ffi::lua_setwarnf(state, Some(warn_proc), self.extra.get() as *mut c_void);
    //     }
    // }

    // /// Removes warning function previously set by `set_warning_function`.
    // ///
    // /// This function has no effect if a warning function was not previously set.
    // ///
    // /// Requires `feature = "lua54"`
    // #[cfg(feature = "lua54")]
    // #[cfg_attr(docsrs, doc(cfg(feature = "lua54")))]
    // pub fn remove_warning_function(&self) {
    //     unsafe {
    //         (*self.extra.get()).warn_callback = None;
    //         ffi::lua_setwarnf(self.main_state, None, ptr::null_mut());
    //     }
    // }

    // /// Emits a warning with the given message.
    // ///
    // /// A message in a call with `incomplete` set to `true` should be continued in
    // /// another call to this function.
    // ///
    // /// Requires `feature = "lua54"`
    // #[cfg(feature = "lua54")]
    // #[cfg_attr(docsrs, doc(cfg(feature = "lua54")))]
    // pub fn warning(&self, msg: impl AsRef<str>, incomplete: bool) {
    //     let msg = msg.as_ref();
    //     let mut bytes = vec![0; msg.len() + 1];
    //     bytes[..msg.len()].copy_from_slice(msg.as_bytes());
    //     let real_len = bytes.iter().position(|&c| c == 0).unwrap();
    //     bytes.truncate(real_len);
    //     unsafe {
    //         ffi::lua_warning(
    //             self.state(),
    //             bytes.as_ptr() as *const c_char,
    //             incomplete as c_int,
    //         );
    //     }
    // }

    /// Gets information about the interpreter runtime stack.
    ///
    /// This function returns [`Debug`] structure that can be used to get information about the function
    /// executing at a given level. Level `0` is the current running function, whereas level `n+1` is the
    /// function that has called level `n` (except for tail calls, which do not count in the stack).
    ///
    /// [`Debug`]: crate::hook::Debug
    pub fn inspect_stack(&self, level: usize) -> Option<Debug> {
        unsafe {
            let mut ar: ffi::lua_Debug = mem::zeroed();
            let level = level as c_int;
            if ffi::lua_getstack(self.state(), level, &mut ar) == 0 {
                return None;
            }
            Some(Debug::new_owned(self, level, ar))
        }
    }

    /// Returns the max amount of memory (in bytes) usable within this Lua state.
    pub fn memory_limit(&self) -> Option<usize> {
        self.memory.as_ref().map(|m| m.len())
    }

    /// Returns the amount of memory (in bytes) currently unused inside this Lua state.
    pub fn unused_memory(&self) -> Option<usize> {
        self.memory.as_ref().map(|m| m.unused_memory())
    }

    /// Returns the amount of memory (in bytes) currently used inside this Lua state.
    pub fn used_memory(&self) -> usize {
        match self.memory.as_ref() {
            Some(memory) => memory.used_memory(),
            None => unsafe {
                // Get data from the Lua GC
                let state = self.state();
                let used_kbytes = ffi::lua_gc(state, ffi::LUA_GCCOUNT, 0);
                let used_kbytes_rem = ffi::lua_gc(state, ffi::LUA_GCCOUNTB, 0);
                (used_kbytes as usize) * 1024 + (used_kbytes_rem as usize)
            },
        }
    }

    /// Returns true if the garbage collector is currently running automatically.
    ///
    /// Requires `feature = "lua54/lua53/lua52"`
    #[cfg(any(feature = "lua54", feature = "lua53", feature = "lua52"))]
    pub fn gc_is_running(&self) -> bool {
        let state = self.state();
        unsafe { ffi::lua_gc(state, ffi::LUA_GCISRUNNING, 0) != 0 }
    }

    /// Stop the Lua GC from running
    pub fn gc_stop(&self) {
        let state = self.state();
        unsafe { ffi::lua_gc(state, ffi::LUA_GCSTOP, 0) };
    }

    /// Restarts the Lua GC if it is not running
    pub fn gc_restart(&self) {
        let state = self.state();
        unsafe { ffi::lua_gc(state, ffi::LUA_GCRESTART, 0) };
    }

    /// Perform a full garbage-collection cycle.
    ///
    /// It may be necessary to call this function twice to collect all currently unreachable
    /// objects. Once to finish the current gc cycle, and once to start and finish the next cycle.
    pub fn gc_collect(&self) -> Result<()> {
        let state = self.state();
        unsafe {
            check_stack(state, 2)?;
            protect_lua!(state, 0, 0, fn(state) ffi::lua_gc(state, ffi::LUA_GCCOLLECT, 0))
        }
    }

    /// Steps the garbage collector one indivisible step.
    ///
    /// Returns true if this has finished a collection cycle.
    pub fn gc_step(&self) -> Result<bool> {
        self.gc_step_kbytes(0)
    }

    /// Steps the garbage collector as though memory had been allocated.
    ///
    /// if `kbytes` is 0, then this is the same as calling `gc_step`. Returns true if this step has
    /// finished a collection cycle.
    pub fn gc_step_kbytes(&self, kbytes: c_int) -> Result<bool> {
        unsafe {
            let state = self.state();
            check_stack(state, 3)?;
            protect_lua!(state, 0, 0, |state| {
                ffi::lua_gc(state, ffi::LUA_GCSTEP, kbytes) != 0
            })
        }
    }

    /// Sets the 'pause' value of the collector.
    ///
    /// Returns the previous value of 'pause'. More information can be found in the Lua
    /// [documentation].
    ///
    /// For Luau this parameter sets GC goal
    ///
    /// [documentation]: https://www.lua.org/manual/5.4/manual.html#2.5
    pub fn gc_set_pause(&self, pause: c_int) -> c_int {
        let state = self.state();
        unsafe { ffi::lua_gc(state, ffi::LUA_GCSETPAUSE, pause) }
    }

    /// Sets the 'step multiplier' value of the collector.
    ///
    /// Returns the previous value of the 'step multiplier'. More information can be found in the
    /// Lua [documentation].
    ///
    /// [documentation]: https://www.lua.org/manual/5.4/manual.html#2.5
    pub fn gc_set_step_multiplier(&self, step_multiplier: c_int) -> c_int {
        let state = self.state();
        unsafe { ffi::lua_gc(state, ffi::LUA_GCSETSTEPMUL, step_multiplier) }
    }

    /// Changes the collector to incremental mode with the given parameters.
    ///
    /// Returns the previous mode (always `GCMode::Incremental` in Lua < 5.4).
    /// More information can be found in the Lua [documentation].
    ///
    /// [documentation]: https://www.lua.org/manual/5.4/manual.html#2.5.1
    pub fn gc_inc(&self, pause: c_int, step_multiplier: c_int, step_size: c_int) -> GCMode {
        let state = self.state();

        #[cfg(any(feature = "lua53", feature = "lua52", feature = "lua51"))]
        unsafe {
            if pause > 0 {
                ffi::lua_gc(state, ffi::LUA_GCSETPAUSE, pause);
            }

            if step_multiplier > 0 {
                ffi::lua_gc(state, ffi::LUA_GCSETSTEPMUL, step_multiplier);
            }

            let _ = step_size; // Ignored

            GCMode::Incremental
        }

        #[cfg(feature = "lua54")]
        let prev_mode =
            unsafe { ffi::lua_gc(state, ffi::LUA_GCINC, pause, step_multiplier, step_size) };
        #[cfg(feature = "lua54")]
        match prev_mode {
            ffi::LUA_GCINC => GCMode::Incremental,
            ffi::LUA_GCGEN => GCMode::Generational,
            _ => unreachable!(),
        }
    }

    /// Changes the collector to generational mode with the given parameters.
    ///
    /// Returns the previous mode. More information about the generational GC
    /// can be found in the Lua 5.4 [documentation][lua_doc].
    ///
    /// Requires `feature = "lua54"`
    ///
    /// [lua_doc]: https://www.lua.org/manual/5.4/manual.html#2.5.2
    #[cfg(feature = "lua54")]
    #[cfg_attr(docsrs, doc(cfg(feature = "lua54")))]
    pub fn gc_gen(&self, minor_multiplier: c_int, major_multiplier: c_int) -> GCMode {
        let state = self.state();
        let prev_mode =
            unsafe { ffi::lua_gc(state, ffi::LUA_GCGEN, minor_multiplier, major_multiplier) };
        match prev_mode {
            ffi::LUA_GCGEN => GCMode::Generational,
            ffi::LUA_GCINC => GCMode::Incremental,
            _ => unreachable!(),
        }
    }

    /// Returns Lua source code as a `Chunk` builder type.
    ///
    /// In order to actually compile or run the resulting code, you must call [`Chunk::exec`] or
    /// similar on the returned builder. Code is not even parsed until one of these methods is
    /// called.
    ///
    /// [`Chunk::exec`]: crate::Chunk::exec
    #[track_caller]
    pub fn load<'lua, 'a>(&'lua self, chunk: impl AsChunk<'lua, 'a>) -> Chunk<'lua, 'a> {
        self.snapshot.borrow_mut().mark_modified();
        let caller = Location::caller();

        Chunk {
            lua: self,
            name: chunk.name().unwrap_or_else(|| caller.to_string()),
            env: chunk.environment(self),
            mode: chunk.mode(),
            source: chunk.source(),
        }
    }

    pub(crate) fn load_chunk<'lua>(
        &'lua self,
        name: Option<&CStr>,
        env: Option<Table>,
        mode: Option<ChunkMode>,
        source: &[u8],
    ) -> Result<Function<'lua>> {
        let state = self.state();
        unsafe {
            let _sg = StackGuard::new(state);
            check_stack(state, 1)?;

            let mode_str = match mode {
                Some(ChunkMode::Binary) => cstr!("b"),
                Some(ChunkMode::Text) => cstr!("t"),
                None => cstr!("bt"),
            };

            match ffi::luaL_loadbufferx(
                state,
                source.as_ptr() as *const c_char,
                source.len(),
                name.map(|n| n.as_ptr()).unwrap_or_else(ptr::null),
                mode_str,
            ) {
                ffi::LUA_OK => {
                    if let Some(env) = env {
                        self.push_ref(&env.0);
                        #[cfg(any(feature = "lua54", feature = "lua53", feature = "lua52"))]
                        ffi::lua_setupvalue(self.state(), -2, 1);
                        #[cfg(feature = "lua51")]
                        ffi::lua_setfenv(self.state(), -2);
                    }

                    #[cfg(feature = "luau-jit")]
                    if (*self.extra.get()).enable_jit && ffi::luau_codegen_supported() != 0 {
                        ffi::luau_codegen_compile(state, -1);
                    }

                    Ok(Function(self.pop_ref()))
                }
                err => Err(pop_error(state, err)),
            }
        }
    }

    /// Create and return an interned Lua string. Lua strings can be arbitrary [u8] data including
    /// embedded nulls, so in addition to `&str` and `&String`, you can also pass plain `&[u8]`
    /// here.
    pub fn create_string(&self, s: impl AsRef<[u8]>) -> Result<String> {
        let state = self.state();
        unsafe {
            if self.unlikely_memory_error() {
                push_string(self.ref_thread, s.as_ref(), false)?;
                return Ok(String(self.pop_ref_thread()));
            }

            let _sg = StackGuard::new(state);
            check_stack(state, 3)?;
            push_string(state, s.as_ref(), true)?;
            Ok(String(self.pop_ref()))
        }
    }

    /// Creates and returns a new empty table.
    pub fn create_table(&self) -> Result<Table> {
        self.create_table_with_capacity(0, 0)
    }

    /// Creates and returns a new empty table, with the specified capacity.
    /// `narr` is a hint for how many elements the table will have as a sequence;
    /// `nrec` is a hint for how many other elements the table will have.
    /// Lua may use these hints to preallocate memory for the new table.
    pub fn create_table_with_capacity(&self, narr: usize, nrec: usize) -> Result<Table> {
        let state = self.state();
        unsafe {
            if self.unlikely_memory_error() {
                push_table(self.ref_thread, narr, nrec, false)?;
                return Ok(Table(self.pop_ref_thread()));
            }

            let _sg = StackGuard::new(state);
            check_stack(state, 3)?;
            push_table(state, narr, nrec, true)?;
            Ok(Table(self.pop_ref()))
        }
    }

    /// Creates a table and fills it with values from an iterator.
    pub fn create_table_from<'lua, K, V, I>(&'lua self, iter: I) -> Result<Table<'lua>>
    where
        K: IntoLua<'lua>,
        V: IntoLua<'lua>,
        I: IntoIterator<Item = (K, V)>,
    {
        let state = self.state();
        unsafe {
            let _sg = StackGuard::new(state);
            check_stack(state, 6)?;

            let iter = iter.into_iter();
            let lower_bound = iter.size_hint().0;
            let protect = !self.unlikely_memory_error();
            push_table(state, 0, lower_bound, protect)?;
            for (k, v) in iter {
                self.push_value(k.into_lua(self)?)?;
                self.push_value(v.into_lua(self)?)?;
                if protect {
                    protect_lua!(state, 3, 1, fn(state) ffi::lua_rawset(state, -3))?;
                } else {
                    ffi::lua_rawset(state, -3);
                }
            }

            Ok(Table(self.pop_ref()))
        }
    }

    /// Creates a table from an iterator of values, using `1..` as the keys.
    pub fn create_sequence_from<'lua, T, I>(&'lua self, iter: I) -> Result<Table<'lua>>
    where
        T: IntoLua<'lua>,
        I: IntoIterator<Item = T>,
    {
        let state = self.state();
        unsafe {
            let _sg = StackGuard::new(state);
            check_stack(state, 5)?;

            let iter = iter.into_iter();
            let lower_bound = iter.size_hint().0;
            let protect = !self.unlikely_memory_error();
            push_table(state, lower_bound, 0, protect)?;
            for (i, v) in iter.enumerate() {
                self.push_value(v.into_lua(self)?)?;
                if protect {
                    protect_lua!(state, 2, 1, |state| {
                        ffi::lua_rawseti(state, -2, (i + 1) as Integer);
                    })?;
                } else {
                    ffi::lua_rawseti(state, -2, (i + 1) as Integer);
                }
            }

            Ok(Table(self.pop_ref()))
        }
    }

    /// Wraps a Rust function or closure, creating a callable Lua function handle to it.
    ///
    /// The function's return value is always a `Result`: If the function returns `Err`, the error
    /// is raised as a Lua error, which can be caught using `(x)pcall` or bubble up to the Rust code
    /// that invoked the Lua code. This allows using the `?` operator to propagate errors through
    /// intermediate Lua code.
    ///
    /// If the function returns `Ok`, the contained value will be converted to one or more Lua
    /// values. For details on Rust-to-Lua conversions, refer to the [`IntoLua`] and [`IntoLuaMulti`]
    /// traits.
    ///
    /// # Examples
    ///
    /// Create a function which prints its argument:
    ///
    /// ```
    /// # use rollback_mlua::{Lua, Result};
    /// # fn main() -> Result<()> {
    /// # let lua = Lua::new();
    /// let greet = lua.create_function(|_, name: String| {
    ///     println!("Hello, {}!", name);
    ///     Ok(())
    /// });
    /// # let _ = greet;    // used
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Use tuples to accept multiple arguments:
    ///
    /// ```
    /// # use rollback_mlua::{Lua, Result};
    /// # fn main() -> Result<()> {
    /// # let lua = Lua::new();
    /// let print_person = lua.create_function(|_, (name, age): (String, u8)| {
    ///     println!("{} is {} years old!", name, age);
    ///     Ok(())
    /// });
    /// # let _ = print_person;    // used
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`IntoLua`]: crate::IntoLua
    /// [`IntoLuaMulti`]: crate::IntoLuaMulti
    pub fn create_function<'lua, A, R, F>(&'lua self, func: F) -> Result<Function<'lua>>
    where
        A: FromLuaMulti<'lua>,
        R: IntoLuaMulti<'lua>,
        F: Fn(&'lua Lua, A) -> Result<R> + MaybeSend + 'static,
    {
        self.create_callback(Box::new(move |lua, nargs| unsafe {
            let args = A::from_stack_args(nargs, 1, None, lua)?;
            func(lua, args)?.push_into_stack_multi(lua)
        }))
    }

    /// Returns a handle to the global environment.
    pub fn globals(&self) -> Table {
        self.snapshot.borrow_mut().mark_modified();

        unsafe {
            let state = self.state();
            let _sg = StackGuard::new(state);
            check_stack(state, 1).unwrap();
            #[cfg(any(feature = "lua54", feature = "lua53", feature = "lua52"))]
            ffi::lua_rawgeti(state, ffi::LUA_REGISTRYINDEX, ffi::LUA_RIDX_GLOBALS);
            #[cfg(feature = "lua51")]
            ffi::lua_pushvalue(state, ffi::LUA_GLOBALSINDEX);
            Table(self.pop_ref())
        }
    }

    /// Calls the given function with a `Scope` parameter, giving the function the ability to create
    /// userdata and callbacks from rust types that are !Send or non-'static.
    ///
    /// The lifetime of any function or userdata created through `Scope` lasts only until the
    /// completion of this method call, on completion all such created values are automatically
    /// dropped and Lua references to them are invalidated. If a script accesses a value created
    /// through `Scope` outside of this method, a Lua error will result. Since we can ensure the
    /// lifetime of values created through `Scope`, and we know that `Lua` cannot be sent to another
    /// thread while `Scope` is live, it is safe to allow !Send datatypes and whose lifetimes only
    /// outlive the scope lifetime.
    ///
    /// Inside the scope callback, all handles created through Scope will share the same unique 'lua
    /// lifetime of the parent `Lua`. This allows scoped and non-scoped values to be mixed in
    /// API calls, which is very useful (e.g. passing a scoped userdata to a non-scoped function).
    /// However, this also enables handles to scoped values to be trivially leaked from the given
    /// callback. This is not dangerous, though!  After the callback returns, all scoped values are
    /// invalidated, which means that though references may exist, the Rust types backing them have
    /// dropped. `Function` types will error when called, and `AnyUserData` will be typeless. It
    /// would be impossible to prevent handles to scoped values from escaping anyway, since you
    /// would always be able to smuggle them through Lua state.
    pub fn scope<'lua, 'scope, R>(
        &'lua self,
        f: impl FnOnce(&Scope<'lua, 'scope>) -> Result<R>,
    ) -> Result<R>
    where
        'lua: 'scope,
    {
        f(&Scope::new(self))
    }

    /// Attempts to coerce a Lua value into a String in a manner consistent with Lua's internal
    /// behavior.
    ///
    /// To succeed, the value must be a string (in which case this is a no-op), an integer, or a
    /// number.
    pub fn coerce_string<'lua>(&'lua self, v: Value<'lua>) -> Result<Option<String<'lua>>> {
        Ok(match v {
            Value::String(s) => Some(s),
            v => unsafe {
                let state = self.state();
                let _sg = StackGuard::new(state);
                check_stack(state, 4)?;

                self.push_value(v)?;
                let res = if self.unlikely_memory_error() {
                    ffi::lua_tolstring(state, -1, ptr::null_mut())
                } else {
                    protect_lua!(state, 1, 1, |state| {
                        ffi::lua_tolstring(state, -1, ptr::null_mut())
                    })?
                };
                if !res.is_null() {
                    Some(String(self.pop_ref()))
                } else {
                    None
                }
            },
        })
    }

    /// Attempts to coerce a Lua value into an integer in a manner consistent with Lua's internal
    /// behavior.
    ///
    /// To succeed, the value must be an integer, a floating point number that has an exact
    /// representation as an integer, or a string that can be converted to an integer. Refer to the
    /// Lua manual for details.
    pub fn coerce_integer(&self, v: Value) -> Result<Option<Integer>> {
        Ok(match v {
            Value::Integer(i) => Some(i),
            v => unsafe {
                let state = self.state();
                let _sg = StackGuard::new(state);
                check_stack(state, 2)?;

                self.push_value(v)?;
                let mut isint = 0;
                let i = ffi::lua_tointegerx(state, -1, &mut isint);
                if isint == 0 {
                    None
                } else {
                    Some(i)
                }
            },
        })
    }

    /// Attempts to coerce a Lua value into a Number in a manner consistent with Lua's internal
    /// behavior.
    ///
    /// To succeed, the value must be a number or a string that can be converted to a number. Refer
    /// to the Lua manual for details.
    pub fn coerce_number(&self, v: Value) -> Result<Option<Number>> {
        Ok(match v {
            Value::Number(n) => Some(n),
            v => unsafe {
                let state = self.state();
                let _sg = StackGuard::new(state);
                check_stack(state, 2)?;

                self.push_value(v)?;
                let mut isnum = 0;
                let n = ffi::lua_tonumberx(state, -1, &mut isnum);
                if isnum == 0 {
                    None
                } else {
                    Some(n)
                }
            },
        })
    }

    /// Converts a value that implements `IntoLua` into a `Value` instance.
    pub fn pack<'lua, T: IntoLua<'lua>>(&'lua self, t: T) -> Result<Value<'lua>> {
        t.into_lua(self)
    }

    /// Converts a `Value` instance into a value that implements `FromLua`.
    pub fn unpack<'lua, T: FromLua<'lua>>(&'lua self, value: Value<'lua>) -> Result<T> {
        T::from_lua(value, self)
    }

    /// Converts a value that implements `IntoLuaMulti` into a `MultiValue` instance.
    pub fn pack_multi<'lua, T: IntoLuaMulti<'lua>>(&'lua self, t: T) -> Result<MultiValue<'lua>> {
        t.into_lua_multi(self)
    }

    /// Converts a `MultiValue` instance into a value that implements `FromLuaMulti`.
    pub fn unpack_multi<'lua, T: FromLuaMulti<'lua>>(
        &'lua self,
        value: MultiValue<'lua>,
    ) -> Result<T> {
        T::from_lua_multi(value, self)
    }

    /// Set a value in the Lua registry based on a string name.
    ///
    /// This value will be available to rust from all `Lua` instances which share the same main
    /// state.
    pub fn set_named_registry_value<'lua, T>(&'lua self, name: &str, t: T) -> Result<()>
    where
        T: IntoLua<'lua>,
    {
        self.snapshot.borrow_mut().mark_modified();

        let state = self.state();
        let t = t.into_lua(self)?;
        unsafe {
            let _sg = StackGuard::new(state);
            check_stack(state, 5)?;

            self.push_value(t)?;
            rawset_field(state, ffi::LUA_REGISTRYINDEX, name)
        }
    }

    /// Get a value from the Lua registry based on a string name.
    ///
    /// Any Lua instance which shares the underlying main state may call this method to
    /// get a value previously set by [`set_named_registry_value`].
    ///
    /// [`set_named_registry_value`]: #method.set_named_registry_value
    pub fn named_registry_value<'lua, T>(&'lua self, name: &str) -> Result<T>
    where
        T: FromLua<'lua>,
    {
        self.snapshot.borrow_mut().mark_modified();

        let state = self.state();
        let value = unsafe {
            let _sg = StackGuard::new(state);
            check_stack(state, 3)?;

            let protect = !self.unlikely_memory_error();
            push_string(state, name.as_bytes(), protect)?;
            ffi::lua_rawget(state, ffi::LUA_REGISTRYINDEX);

            self.pop_value()
        };
        T::from_lua(value, self)
    }

    /// Removes a named value in the Lua registry.
    ///
    /// Equivalent to calling [`set_named_registry_value`] with a value of Nil.
    ///
    /// [`set_named_registry_value`]: #method.set_named_registry_value
    pub fn unset_named_registry_value(&self, name: &str) -> Result<()> {
        self.set_named_registry_value(name, Nil)
    }

    /// Place a value in the Lua registry with an auto-generated key.
    ///
    /// This value will be available to Rust from all `Lua` instances which share the same main
    /// state.
    ///
    /// Be warned, garbage collection of values held inside the registry is not automatic, see
    /// [`RegistryKey`] for more details.
    /// However, dropped [`RegistryKey`]s automatically reused to store new values.
    ///
    /// [`RegistryKey`]: crate::RegistryKey
    pub fn create_registry_value<'lua, T: IntoLua<'lua>>(&'lua self, t: T) -> Result<RegistryKey> {
        self.snapshot.borrow_mut().mark_modified();

        let t = t.into_lua(self)?;
        if t == Value::Nil {
            // Special case to skip calling `luaL_ref` and use `LUA_REFNIL` instead
            return Ok(RegistryKey::new(
                ffi::LUA_REFNIL,
                self.registry_tracker.clone(),
            ));
        }

        let state = self.state();

        unsafe {
            let _sg = StackGuard::new(state);
            check_stack(state, 4)?;

            self.push_value(t)?;

            // Try to reuse previously allocated RegistryKey
            let mut registry_tracker = self.registry_tracker.lock().expect("unref list poisoned");

            let registry_id = if let Some(registry_id) = registry_tracker.unref_list.pop() {
                // It must be safe to replace the value without triggering memory error
                ffi::lua_rawseti(state, ffi::LUA_REGISTRYINDEX, registry_id as Integer);
                registry_id
            } else {
                // Allocate a new RegistryKey
                protect_lua!(state, 1, 0, |state| {
                    ffi::luaL_ref(state, ffi::LUA_REGISTRYINDEX)
                })?
            };

            let registry_key = RegistryKey::new(registry_id, self.registry_tracker.clone());

            if self.max_snapshots() > 0 {
                // no need to track validity for rollbacks if rolling back is not possible
                registry_tracker
                    .recent_key_validity
                    .push(registry_key.validity.clone());
            }

            Ok(registry_key)
        }
    }

    /// Get a value from the Lua registry by its `RegistryKey`
    ///
    /// Any Lua instance which shares the underlying main state may call this method to get a value
    /// previously placed by [`create_registry_value`].
    ///
    /// [`create_registry_value`]: #method.create_registry_value
    pub fn registry_value<'lua, T: FromLua<'lua>>(&'lua self, key: &RegistryKey) -> Result<T> {
        if !self.owns_registry_value(key) {
            return Err(Error::MismatchedRegistryKey);
        }

        self.snapshot.borrow_mut().mark_modified();

        let state = self.state();
        let value = match key.is_nil() {
            true => Value::Nil,
            false => unsafe {
                let _sg = StackGuard::new(state);
                check_stack(state, 1)?;

                let id = key.registry_id as Integer;
                ffi::lua_rawgeti(state, ffi::LUA_REGISTRYINDEX, id);
                self.pop_value()
            },
        };
        T::from_lua(value, self)
    }

    /// Removes a value from the Lua registry.
    ///
    /// You may call this function to manually remove a value placed in the registry with
    /// [`create_registry_value`]. In addition to manual `RegistryKey` removal, you can also call
    /// [`expire_registry_values`] to automatically remove values from the registry whose
    /// `RegistryKey`s have been dropped.
    ///
    /// [`create_registry_value`]: #method.create_registry_value
    /// [`expire_registry_values`]: #method.expire_registry_values
    pub fn remove_registry_value(&self, key: RegistryKey) -> Result<()> {
        if !self.owns_registry_value(&key) {
            return Err(Error::MismatchedRegistryKey);
        }

        self.snapshot.borrow_mut().mark_modified();

        unsafe {
            ffi::luaL_unref(self.state(), ffi::LUA_REGISTRYINDEX, key.take());
        }

        Ok(())
    }

    /// Replaces a value in the Lua registry by its `RegistryKey`.
    ///
    /// See [`create_registry_value`] for more details.
    ///
    /// [`create_registry_value`]: #method.create_registry_value
    pub fn replace_registry_value<'lua, T: IntoLua<'lua>>(
        &'lua self,
        key: &RegistryKey,
        t: T,
    ) -> Result<()> {
        if !self.owns_registry_value(key) {
            return Err(Error::MismatchedRegistryKey);
        }

        let t = t.into_lua(self)?;

        if t == Value::Nil && key.is_nil() {
            // Nothing to replace
            return Ok(());
        } else if t != Value::Nil && key.registry_id == ffi::LUA_REFNIL {
            // We cannot update `LUA_REFNIL` slot
            return Err(Error::runtime("cannot replace nil value with non-nil"));
        }

        self.snapshot.borrow_mut().mark_modified();

        let state = self.state();
        unsafe {
            let _sg = StackGuard::new(state);
            check_stack(state, 2)?;

            let id = key.registry_id as Integer;
            if t == Value::Nil {
                self.push_value(Value::Integer(id))?;
                key.set_nil(true);
            } else {
                self.push_value(t)?;
                key.set_nil(false);
            }
            // It must be safe to replace the value without triggering memory error
            ffi::lua_rawseti(state, ffi::LUA_REGISTRYINDEX, id);
        }
        Ok(())
    }

    /// Returns true if the given `RegistryKey` was created by a `Lua` which shares the underlying
    /// main state with this `Lua` instance.
    ///
    /// Other than this, methods that accept a `RegistryKey` will return
    /// `Error::MismatchedRegistryKey` if passed a `RegistryKey` that was not created with a
    /// matching `Lua` state.
    pub fn owns_registry_value(&self, key: &RegistryKey) -> bool {
        Arc::ptr_eq(&key.registry_tracker, &self.registry_tracker) && key.is_valid()
    }

    /// Remove any registry values whose `RegistryKey`s have all been dropped.
    ///
    /// Unlike normal handle values, `RegistryKey`s do not automatically remove themselves on Drop,
    /// but you can call this method to remove any unreachable registry values not manually removed
    /// by `Lua::remove_registry_value`.
    pub fn expire_registry_values(&self) {
        let state = self.state();

        unsafe {
            self.snapshot.borrow_mut().mark_modified();

            let mut registry_tracker = self.registry_tracker.lock().expect("unref list poisoned");
            let unref_list = mem::take(&mut registry_tracker.unref_list);
            for id in unref_list {
                ffi::luaL_unref(state, ffi::LUA_REGISTRYINDEX, id);
            }
        }
    }

    /// Sets or replaces an application data object of type `T`.
    ///
    /// Application data could be accessed at any time by using [`Lua::app_data()`]
    /// methods where `T` is the data type.
    ///
    /// # Panics
    ///
    /// Panics if the app data container is currently borrowed.
    #[track_caller]
    pub fn set_app_data<T: 'static>(&self, data: Rc<T>) -> Option<Rc<T>> {
        let snapshot = self.snapshot.borrow();

        let old_data = snapshot
            .app_data
            .try_borrow_mut()
            .expect("cannot borrow mutably app data container")
            .insert(TypeId::of::<T>(), data)?;

        Rc::downcast(old_data).ok()
    }

    /// Gets a reference to an application data object stored by [`Lua::set_app_data()`] of type `T`.
    ///
    /// # Panics
    ///
    /// Panics if the app data container is currently mutably borrowed. Multiple immutable reads can be
    /// taken out at the same time.
    #[track_caller]
    pub fn app_data<T: 'static>(&self) -> Option<Rc<T>> {
        let snapshot = self.snapshot.borrow();
        let app_data = snapshot
            .app_data
            .try_borrow()
            .expect("cannot borrow app data container");

        let data = app_data.get(&TypeId::of::<T>())?;
        Rc::downcast(data.clone()).ok()
    }

    /// Removes an application data of type `T`.
    ///
    /// # Panics
    ///
    /// Panics if the app data container is currently borrowed.
    #[track_caller]
    pub fn remove_app_data<T: 'static>(&self) -> Option<Rc<T>> {
        let snapshot = self.snapshot.borrow();
        let app_data = snapshot.app_data.try_borrow_mut();

        let data = app_data
            .expect("cannot mutably borrow app data container")
            .remove(&TypeId::of::<T>())?;

        Rc::downcast(data).ok()
    }

    /// Pushes a value onto the Lua stack.
    ///
    /// Uses 2 stack spaces, does not call checkstack.
    #[doc(hidden)]
    pub unsafe fn push_value(&self, value: Value) -> Result<()> {
        let state = self.state();
        match value {
            Value::Nil => {
                ffi::lua_pushnil(state);
            }

            Value::Boolean(b) => {
                ffi::lua_pushboolean(state, b as c_int);
            }

            Value::LightUserData(ud) => {
                ffi::lua_pushlightuserdata(state, ud.0);
            }

            Value::Integer(i) => {
                ffi::lua_pushinteger(state, i);
            }

            Value::Number(n) => {
                ffi::lua_pushnumber(state, n);
            }

            Value::String(s) => {
                self.push_ref(&s.0);
            }

            Value::Table(t) => {
                self.push_ref(&t.0);
            }

            Value::Function(f) => {
                self.push_ref(&f.0);
            }

            Value::Thread(t) => {
                self.push_ref(&t.0);
            }

            Value::Error(err) => {
                let protect = !self.unlikely_memory_error();
                check_stack(state, 4)?;
                check_stack(self.ref_thread, 1)?;
                push_gc_userdata(self, WrappedFailure::Error(err), protect)?;
            }
        }

        Ok(())
    }

    /// Pops a value from the Lua stack.
    ///
    /// Uses 2 stack spaces, does not call checkstack.
    #[doc(hidden)]
    pub(crate) unsafe fn pop_value(&self) -> Value {
        let state = self.state();
        match ffi::lua_type(state, -1) {
            ffi::LUA_TNIL => {
                ffi::lua_pop(state, 1);
                Value::Nil
            }

            ffi::LUA_TBOOLEAN => {
                let b = Value::Boolean(ffi::lua_toboolean(state, -1) != 0);
                ffi::lua_pop(state, 1);
                b
            }

            ffi::LUA_TLIGHTUSERDATA => {
                let ud = Value::LightUserData(LightUserData(ffi::lua_touserdata(state, -1)));
                ffi::lua_pop(state, 1);
                ud
            }

            #[cfg(any(feature = "lua54", feature = "lua53"))]
            ffi::LUA_TNUMBER => {
                let v = if ffi::lua_isinteger(state, -1) != 0 {
                    Value::Integer(ffi::lua_tointeger(state, -1))
                } else {
                    Value::Number(ffi::lua_tonumber(state, -1))
                };
                ffi::lua_pop(state, 1);
                v
            }

            #[cfg(any(feature = "lua52", feature = "lua51",))]
            ffi::LUA_TNUMBER => {
                let n = ffi::lua_tonumber(state, -1);
                ffi::lua_pop(state, 1);
                match num_traits::cast(n) {
                    Some(i) if (n - (i as Number)).abs() < Number::EPSILON => Value::Integer(i),
                    _ => Value::Number(n),
                }
            }

            ffi::LUA_TSTRING => Value::String(String(self.pop_ref())),

            ffi::LUA_TTABLE => Value::Table(crate::Table(self.pop_ref())),

            ffi::LUA_TFUNCTION => Value::Function(crate::Function(self.pop_ref())),

            ffi::LUA_TUSERDATA => {
                let wrapped_failure_mt_ptr = self.wrapped_failure_mt_ptr;

                // We must prevent interaction with userdata types other than UserData OR a WrappedError.
                // WrappedPanics are automatically resumed.
                match get_gc_userdata::<WrappedFailure>(state, -1, wrapped_failure_mt_ptr).as_mut()
                {
                    Some(WrappedFailure::Error(err)) => {
                        let err = err.clone();
                        ffi::lua_pop(state, 1);
                        Value::Error(err)
                    }
                    Some(WrappedFailure::Panic(panic)) => {
                        if let Some(panic) = panic.take() {
                            ffi::lua_pop(state, 1);
                            resume_unwind(panic);
                        }
                        // Previously resumed panic?
                        ffi::lua_pop(state, 1);
                        Value::Nil
                    }
                    _ => unreachable!(),
                }
            }

            ffi::LUA_TTHREAD => Value::Thread(Thread::new(self.pop_ref())),

            _ => panic!("LUA_TNONE in pop_value"),
        }
    }

    /// Returns value at given stack index without popping it.
    ///
    /// Uses 2 stack spaces, does not call checkstack.
    pub(crate) unsafe fn stack_value(&self, idx: c_int) -> Value {
        let state = self.state();
        match ffi::lua_type(state, idx) {
            ffi::LUA_TNIL => Nil,

            ffi::LUA_TBOOLEAN => Value::Boolean(ffi::lua_toboolean(state, idx) != 0),

            ffi::LUA_TLIGHTUSERDATA => {
                Value::LightUserData(LightUserData(ffi::lua_touserdata(state, idx)))
            }

            #[cfg(any(feature = "lua54", feature = "lua53"))]
            ffi::LUA_TNUMBER => {
                if ffi::lua_isinteger(state, idx) != 0 {
                    Value::Integer(ffi::lua_tointeger(state, idx))
                } else {
                    Value::Number(ffi::lua_tonumber(state, idx))
                }
            }

            #[cfg(any(feature = "lua52", feature = "lua51"))]
            ffi::LUA_TNUMBER => {
                let n = ffi::lua_tonumber(state, idx);
                match num_traits::cast(n) {
                    Some(i) if (n - (i as Number)).abs() < Number::EPSILON => Value::Integer(i),
                    _ => Value::Number(n),
                }
            }

            ffi::LUA_TSTRING => {
                ffi::lua_xpush(state, self.ref_thread(), idx);
                Value::String(String(self.pop_ref_thread()))
            }

            ffi::LUA_TTABLE => {
                ffi::lua_xpush(state, self.ref_thread(), idx);
                Value::Table(Table(self.pop_ref_thread()))
            }

            ffi::LUA_TFUNCTION => {
                ffi::lua_xpush(state, self.ref_thread(), idx);
                Value::Function(Function(self.pop_ref_thread()))
            }

            ffi::LUA_TUSERDATA => {
                let wrapped_failure_mt_ptr = self.wrapped_failure_mt_ptr;
                // We must prevent interaction with userdata types other than UserData OR a WrappedError.
                // WrappedPanics are automatically resumed.
                match get_gc_userdata::<WrappedFailure>(state, idx, wrapped_failure_mt_ptr).as_mut()
                {
                    Some(WrappedFailure::Error(err)) => Value::Error(err.clone()),
                    Some(WrappedFailure::Panic(panic)) => {
                        if let Some(panic) = panic.take() {
                            resume_unwind(panic);
                        }
                        // Previously resumed panic?
                        Value::Nil
                    }
                    _ => unreachable!(),
                }
            }

            ffi::LUA_TTHREAD => {
                ffi::lua_xpush(state, self.ref_thread(), idx);
                Value::Thread(Thread::new(self.pop_ref_thread()))
            }

            _ => panic!("LUA_TNONE in pop_value"),
        }
    }

    // Pushes a LuaRef value onto the stack, uses 1 stack space, does not call checkstack
    pub(crate) unsafe fn push_ref(&self, lref: &LuaRef) {
        assert!(
            ptr::eq(&lref.lua.0, &self.0),
            "Lua instance passed Value created from a different main Lua state"
        );
        ffi::lua_xpush(self.ref_thread(), self.state(), lref.index);
    }

    // Pops the topmost element of the stack and stores a reference to it. This pins the object,
    // preventing garbage collection until the returned `LuaRef` is dropped.
    //
    // References are stored in the stack of a specially created auxiliary thread that exists only
    // to store reference values. This is much faster than storing these in the registry, and also
    // much more flexible and requires less bookkeeping than storing them directly in the currently
    // used stack. The implementation is somewhat biased towards the use case of a relatively small
    // number of short term references being created, and `RegistryKey` being used for long term
    // references.
    pub(crate) unsafe fn pop_ref(&self) -> LuaRef {
        ffi::lua_xmove(self.state(), self.ref_thread(), 1);
        let index = self.ref_stack_pop();
        LuaRef::new(self, index)
    }

    // Same as `pop_ref` but assumes the value is already on the reference thread
    pub(crate) unsafe fn pop_ref_thread(&self) -> LuaRef {
        let index = self.ref_stack_pop();
        LuaRef::new(self, index)
    }

    pub(crate) fn clone_ref(&self, lref: &LuaRef) -> LuaRef {
        unsafe {
            ffi::lua_pushvalue(self.ref_thread(), lref.index);
            let index = self.ref_stack_pop();
            LuaRef::new(self, index)
        }
    }

    pub(crate) fn drop_ref(&self, lref: &LuaRef) {
        self.drop_ref_index(lref.index);
    }

    unsafe fn ref_stack_pop(&self) -> c_int {
        let mut snapshot = self.snapshot.borrow_mut();

        if let Some(free) = snapshot.ref_free.pop() {
            ffi::lua_replace(self.ref_thread, free);
            return free;
        }

        // Try to grow max stack size
        if snapshot.ref_stack_top >= snapshot.ref_stack_size {
            let mut inc = snapshot.ref_stack_size; // Try to double stack size
            while inc > 0 && ffi::lua_checkstack(self.ref_thread, inc) == 0 {
                inc /= 2;
            }
            if inc == 0 {
                // Pop item on top of the stack to avoid stack leaking and successfully run destructors
                // during unwinding.
                ffi::lua_pop(self.ref_thread, 1);
                let top = snapshot.ref_stack_top;
                // It is a user error to create enough references to exhaust the Lua max stack size for
                // the ref thread.
                panic!(
                    "cannot create a Lua reference, out of auxiliary stack space (used {} slots)",
                    top
                );
            }
            snapshot.ref_stack_size += inc;
        }
        snapshot.ref_stack_top += 1;
        snapshot.ref_stack_top
    }

    #[cfg(all(feature = "unstable", not(feature = "send")))]
    pub(crate) fn adopt_owned_ref(&self, loref: crate::types::LuaOwnedRef) -> LuaRef {
        assert!(
            Arc::ptr_eq(&loref.inner, &self.0),
            "Lua instance passed Value created from a different main Lua state"
        );
        let index = loref.index;
        unsafe {
            ptr::read(&loref.inner);
            mem::forget(loref);
        }
        LuaRef::new(self, index)
    }

    // Creates a Function out of a Callback containing a 'static Fn. This is safe ONLY because the
    // Fn is 'static, otherwise it could capture 'lua arguments improperly. Without ATCs, we
    // cannot easily deal with the "correct" callback type of:
    //
    // Box<for<'lua> Fn(&'lua Lua, MultiValue<'lua>) -> Result<MultiValue<'lua>>)>
    //
    // So we instead use a caller provided lifetime, which without the 'static requirement would be
    // unsafe.
    pub(crate) fn create_callback<'lua>(
        &'lua self,
        func: Callback<'lua, 'static>,
    ) -> Result<Function<'lua>> {
        unsafe extern "C-unwind" fn call_callback(state: *mut ffi::lua_State) -> c_int {
            // Normal functions can be scoped and therefore destroyed,
            // so we need to check that the first upvalue is valid
            let upvalue = match ffi::lua_type(state, ffi::lua_upvalueindex(1)) {
                ffi::LUA_TUSERDATA => {
                    get_gc_userdata::<Callback>(state, ffi::lua_upvalueindex(1), ptr::null())
                }
                _ => ptr::null_mut(),
            };

            let lua_inner = crate::util::lua_inner(state);

            callback_error_ext(state, lua_inner, |nargs| {
                // Lua ensures that `LUA_MINSTACK` stack spaces are available (after pushing arguments)
                if upvalue.is_null() {
                    return Err(Error::CallbackDestructed);
                }

                let lua: &Lua = mem::transmute(&lua_inner);
                let _guard = StateGuard::new(lua, state);
                let func = &*upvalue;

                func(lua, nargs)
            })
        }

        self.snapshot.borrow_mut().mark_modified();

        let state = self.state();
        unsafe {
            let _sg = StackGuard::new(state);
            check_stack(state, 4)?;

            let func: Callback = mem::transmute(func);
            let protect = !self.unlikely_memory_error();
            push_gc_userdata(self, func, protect)?;
            if protect {
                protect_lua!(state, 1, 1, fn(state) {
                    ffi::lua_pushcclosure(state, call_callback, 1);
                })?;
            } else {
                ffi::lua_pushcclosure(state, call_callback, 1);
            }

            Ok(Function(self.pop_ref()))
        }
    }

    fn disable_c_modules(&self) -> Result<()> {
        let package: Table = self.globals().get("package")?;

        package.set(
            "loadlib",
            self.create_function(|_, ()| -> Result<()> {
                Err(Error::SafetyError(
                    "package.loadlib is disabled in safe mode".to_string(),
                ))
            })?,
        )?;

        #[cfg(any(feature = "lua54", feature = "lua53", feature = "lua52"))]
        let searchers: Table = package.get("searchers")?;
        #[cfg(feature = "lua51")]
        let searchers: Table = package.get("loaders")?;

        let loader = self.create_function(|_, ()| Ok("\n\tcan't load C modules in safe mode"))?;

        // The third and fourth searchers looks for a loader as a C library
        searchers.raw_set(3, loader.clone())?;
        searchers.raw_remove(4)?;

        Ok(())
    }

    #[inline]
    pub(crate) fn pop_multivalue_from_pool(&self) -> Option<Vec<Value>> {
        let lua_inner = unsafe { &mut *self.0 };

        lua_inner.multivalue_pool.pop()
    }

    #[inline]
    pub(crate) fn push_multivalue_to_pool(&self, mut multivalue: Vec<Value>) {
        unsafe {
            let lua_inner = &mut *self.0;

            if lua_inner.multivalue_pool.len() < MULTIVALUE_POOL_SIZE {
                multivalue.clear();
                lua_inner.multivalue_pool.push(mem::transmute(multivalue));
            }
        }
    }

    #[inline]
    pub(crate) unsafe fn unlikely_memory_error(&self) -> bool {
        self.memory.is_none()
    }

    #[cfg(feature = "unstable")]
    #[inline]
    pub(crate) fn clone(&self) -> Arc<LuaInner> {
        Arc::clone(&self.0)
    }
}

struct StateGuard<'a>(&'a LuaInner, *mut ffi::lua_State);

impl<'a> StateGuard<'a> {
    fn new(inner: &'a LuaInner, mut state: *mut ffi::lua_State) -> Self {
        state = inner.state.swap(state, Ordering::Relaxed);
        Self(inner, state)
    }
}

impl<'a> Drop for StateGuard<'a> {
    fn drop(&mut self) {
        self.0.state.store(self.1, Ordering::Relaxed);
    }
}

// An optimized version of `callback_error` that does not allocate `WrappedFailure` userdata
// and instead reuses unsed values from previous calls (or allocates new).
unsafe fn callback_error_ext<F, R>(
    state: *mut ffi::lua_State,
    mut lua_inner: *mut LuaInner,
    f: F,
) -> R
where
    F: FnOnce(c_int) -> Result<R>,
{
    if lua_inner.is_null() {
        lua_inner = crate::util::lua_inner(state);
    }

    let nargs = ffi::lua_gettop(state);

    enum PreallocatedFailure {
        New(*mut WrappedFailure),
        Existing(i32),
    }

    impl PreallocatedFailure {
        unsafe fn reserve(state: *mut ffi::lua_State, lua_inner: *mut LuaInner) -> Result<Self> {
            let lua: &Lua = mem::transmute(&lua_inner);
            let mut snapshot = lua.snapshot.borrow_mut();
            let next_free = snapshot.wrapped_failure_pool.pop();
            // drop snapshot as userdata creation makes use of it
            std::mem::drop(snapshot);

            match next_free {
                Some(index) => Ok(PreallocatedFailure::Existing(index)),
                None => {
                    // Place it to the beginning of the stack
                    let ud = WrappedFailure::new_userdata(lua)?;
                    ffi::lua_insert(state, 1);
                    Ok(PreallocatedFailure::New(ud))
                }
            }
        }

        unsafe fn r#use(
            &self,
            state: *mut ffi::lua_State,
            lua_inner: *mut LuaInner,
        ) -> *mut WrappedFailure {
            let lua: &Lua = mem::transmute(&lua_inner);
            let ref_thread = lua.ref_thread;

            match *self {
                PreallocatedFailure::New(ud) => {
                    ffi::lua_settop(state, 1);
                    ud
                }
                PreallocatedFailure::Existing(index) => {
                    ffi::lua_settop(state, 0);
                    ffi::lua_pushvalue(ref_thread, index);
                    ffi::lua_xmove(ref_thread, state, 1);
                    ffi::lua_pushnil(ref_thread);
                    ffi::lua_replace(ref_thread, index);
                    lua.snapshot.borrow_mut().ref_free.push(index);
                    get_gc_userdata(state, -1, std::ptr::null_mut())
                }
            }
        }

        unsafe fn release(self, state: *mut ffi::lua_State, lua_inner: *mut LuaInner) {
            let lua: &Lua = mem::transmute(&lua_inner);
            let ref_thread = lua.ref_thread;

            match self {
                PreallocatedFailure::New(_) => {
                    let snapshot = lua.snapshot.borrow();

                    if snapshot.wrapped_failure_pool.len() < WRAPPED_FAILURE_POOL_SIZE {
                        // drop snapshot as lua.ref_stack_pop() uses it
                        std::mem::drop(snapshot);

                        ffi::lua_rotate(state, 1, -1);
                        ffi::lua_xmove(state, ref_thread, 1);
                        let index = lua.ref_stack_pop();

                        let mut snapshot = lua.snapshot.borrow_mut();
                        snapshot.wrapped_failure_pool.push(index);
                    } else {
                        ffi::lua_remove(state, 1);
                    }
                }
                PreallocatedFailure::Existing(index) => {
                    let mut snapshot = lua.snapshot.borrow_mut();

                    if snapshot.wrapped_failure_pool.len() < WRAPPED_FAILURE_POOL_SIZE {
                        snapshot.wrapped_failure_pool.push(index);
                    } else {
                        ffi::lua_pushnil(ref_thread);
                        ffi::lua_replace(ref_thread, index);
                        snapshot.ref_free.push(index);
                    }
                }
            }
        }
    }

    // We cannot shadow Rust errors with Lua ones, so we need to reserve pre-allocated memory
    // to store a wrapped failure (error or panic) *before* we proceed.
    let prealloc_failure = match PreallocatedFailure::reserve(state, lua_inner) {
        Ok(prealloc_failure) => prealloc_failure,
        Err(err) => {
            // Pass the error back as a lua string. Failing this should cause lua to create a memory error.
            let s = err.to_string();
            ffi::lua_pushlstring(state, s.as_ptr() as *const c_char, s.len());
            ffi::lua_error(state);
        }
    };

    match catch_unwind(AssertUnwindSafe(|| f(nargs))) {
        Ok(Ok(r)) => {
            // Return unused `WrappedFailure` to the pool
            prealloc_failure.release(state, lua_inner);
            r
        }
        Ok(Err(err)) => {
            let wrapped_error = prealloc_failure.r#use(state, lua_inner);

            // Build `CallbackError` with traceback
            let traceback = if ffi::lua_checkstack(state, ffi::LUA_TRACEBACK_STACK) != 0 {
                ffi::luaL_traceback(state, state, ptr::null(), 0);
                let traceback = util::to_string(state, -1);
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
            get_gc_metatable::<WrappedFailure>(state);
            ffi::lua_setmetatable(state, -2);

            ffi::lua_error(state)
        }
        Err(p) => {
            let wrapped_panic = prealloc_failure.r#use(state, lua_inner);
            ptr::write(wrapped_panic, WrappedFailure::Panic(Some(p)));
            get_gc_metatable::<WrappedFailure>(state);
            ffi::lua_setmetatable(state, -2);
            ffi::lua_error(state)
        }
    }
}

// Uses 3 stack spaces
unsafe fn load_from_std_lib(state: *mut ffi::lua_State, libs: StdLib) -> Result<()> {
    #[inline(always)]
    pub unsafe fn requiref(
        state: *mut ffi::lua_State,
        modname: &str,
        openf: ffi::lua_CFunction,
        glb: c_int,
    ) -> Result<()> {
        let modname = CString::new(modname).expect("modname contains nil byte");
        protect_lua!(state, 0, 1, |state| {
            ffi::luaL_requiref(state, modname.as_ptr() as *const c_char, openf, glb)
        })
    }

    #[cfg(any(feature = "lua54", feature = "lua53", feature = "lua52",))]
    {
        if libs.contains(StdLib::COROUTINE) {
            requiref(state, ffi::LUA_COLIBNAME, ffi::luaopen_coroutine, 1)?;
            ffi::lua_pop(state, 1);
        }
    }

    if libs.contains(StdLib::TABLE) {
        requiref(state, ffi::LUA_TABLIBNAME, ffi::luaopen_table, 1)?;
        ffi::lua_pop(state, 1);
    }

    if libs.contains(StdLib::STRING) {
        requiref(state, ffi::LUA_STRLIBNAME, ffi::luaopen_string, 1)?;
        ffi::lua_pop(state, 1);
    }

    #[cfg(any(feature = "lua54", feature = "lua53"))]
    {
        if libs.contains(StdLib::UTF8) {
            requiref(state, ffi::LUA_UTF8LIBNAME, ffi::luaopen_utf8, 1)?;
            ffi::lua_pop(state, 1);
        }
    }

    #[cfg(feature = "lua52")]
    {
        if libs.contains(StdLib::BIT) {
            requiref(state, ffi::LUA_BITLIBNAME, ffi::luaopen_bit32, 1)?;
            ffi::lua_pop(state, 1);
        }
    }

    if libs.contains(StdLib::MATH) {
        requiref(state, ffi::LUA_MATHLIBNAME, ffi::luaopen_math, 1)?;
        ffi::lua_pop(state, 1);
    }

    if libs.contains(StdLib::PACKAGE) {
        requiref(state, ffi::LUA_LOADLIBNAME, ffi::luaopen_package, 1)?;
        ffi::lua_pop(state, 1);
    }

    Ok(())
}

#[cfg(test)]
mod assertions {
    use super::*;

    // Lua has lots of interior mutability, should not be RefUnwindSafe
    static_assertions::assert_not_impl_any!(Lua: std::panic::RefUnwindSafe);

    #[cfg(not(feature = "send"))]
    static_assertions::assert_not_impl_any!(Lua: Send);
    #[cfg(feature = "send")]
    static_assertions::assert_impl_all!(Lua: Send);
}
