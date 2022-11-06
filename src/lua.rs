use crate::chunk::{AsChunk, Chunk, ChunkMode};
use crate::error::{Error, Result};
use crate::function::Function;
use crate::lua_ref::LuaRef;
use crate::photographic_memory::PhotographicMemory;
use crate::util::{
    callback_error, check_stack, get_gc_metatable, get_gc_userdata, get_userdata_ref,
    init_error_registry, init_gc_metatable, init_lua_inner_registry, pop_error, push_gc_userdata,
    push_string, push_table, rawset_field, safe_pcall, safe_xpcall, StackGuard, WrappedFailure,
};
use crate::value::{MultiValue, Value};
use crate::{
    ffi, Callback, FromLua, FromLuaMulti, Integer, Number, RegistryKey, RegistryTracker, Scope,
    StdLib, String, Table, ToLua, ToLuaMulti,
};
use generational_arena::{Arena, Index as GenerationalIndex};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::ffi::{CStr, CString};
use std::ops::{Deref, DerefMut};
use std::os::raw::{c_char, c_int, c_void};
use std::panic::Location;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::{mem, ptr};

const MULTIVALUE_CACHE_SIZE: usize = 32;

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
    #[cfg(any(feature = "lua54"))]
    #[cfg_attr(docsrs, doc(cfg(feature = "lua54")))]
    Generational,
}

#[derive(Clone)]
pub(crate) struct LuaSnapshot {
    ref_stack_top: c_int,
    ref_stack_size: c_int,
    ref_free: Vec<c_int>,
    registry_unref_list: Vec<c_int>,
    recent_key_validity: Vec<Arc<(c_int, AtomicBool)>>,
    pub(crate) ud_recent_construction: Vec<GenerationalIndex>,
    pub(crate) ud_pending_destruction: Vec<GenerationalIndex>,
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
    pub(crate) state: *mut ffi::lua_State,
    pub(crate) ref_thread: *mut ffi::lua_State,
    wrapped_failure_mt_ptr: *const c_void,
    pub(crate) compiled_bind_func: Vec<u8>,
    registry_tracker: Arc<Mutex<RegistryTracker>>,
    pub(crate) snapshot: RefCell<LuaSnapshot>,
    snapshots: VecDeque<LuaSnapshot>,
    multivalue_cache: RefCell<Vec<MultiValue<'static>>>,
    pub(crate) user_data_destructors: RefCell<Arena<Box<dyn FnOnce()>>>,
    pub(crate) memory: Option<Box<PhotographicMemory>>,
}

impl Drop for LuaInner {
    fn drop(&mut self) {
        unsafe { ffi::lua_close(self.state) }
    }
}

impl LuaInner {
    pub(crate) fn drop_ref_index(&self, index: c_int) {
        unsafe {
            let mut snapshot = self.snapshot.borrow_mut();

            ffi::lua_pushnil(self.ref_thread);
            ffi::lua_replace(self.ref_thread, index);
            snapshot.ref_free.push(index);
        }
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub struct Lua(pub(crate) *mut LuaInner);

impl Deref for Lua {
    type Target = LuaInner;

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
                for destructor in destructors {
                    destructor();
                }
            }

            let _ = Box::from_raw(self.0);
        }
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
        unsafe extern "C" fn allocator(
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
            state,
            ref_thread,
            wrapped_failure_mt_ptr: ptr::null(),
            registry_tracker: Arc::new(Mutex::new(RegistryTracker::new())),
            snapshot: RefCell::new(LuaSnapshot {
                ref_stack_top: unsafe { ffi::lua_gettop(ref_thread) },
                // We need 1 extra stack space to move values in and out of the ref stack.
                ref_stack_size: ffi::LUA_MINSTACK - 1,
                ref_free: Vec::new(),
                registry_unref_list: Vec::new(),
                recent_key_validity: Vec::new(),
                ud_recent_construction: Vec::new(),
                ud_pending_destruction: Vec::new(),
                libs: StdLib::NONE,
                modified: true,
            }),
            snapshots: VecDeque::with_capacity(
                memory
                    .as_ref()
                    .map(|m| m.max_snapshots())
                    .unwrap_or_default(),
            ),
            multivalue_cache: RefCell::new(Vec::with_capacity(MULTIVALUE_CACHE_SIZE)),
            memory,
            user_data_destructors: RefCell::new(Arena::new()),
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
            #[cfg(any(feature = "lua51"))]
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
            load_from_std_lib(self.state, libs)?;
        }

        // If `package` library loaded into a safe lua state then disable C modules

        let curr_libs = {
            let mut snapshot = self.snapshot.borrow_mut();

            snapshot.libs |= libs;
            snapshot.mark_modified();

            snapshot.libs
        };

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
    pub fn load_from_function<'lua, S, T>(
        &'lua self,
        modname: &S,
        func: Function<'lua>,
    ) -> Result<T>
    where
        S: AsRef<[u8]> + ?Sized,
        T: FromLua<'lua>,
    {
        let loaded = unsafe {
            let _sg = StackGuard::new(self.state);
            check_stack(self.state, 2)?;
            protect_lua!(self.state, 0, 1, fn(state) {
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
    pub fn unload<S>(&self, modname: &S) -> Result<()>
    where
        S: AsRef<[u8]> + ?Sized,
    {
        let loaded = unsafe {
            let _sg = StackGuard::new(self.state);
            check_stack(self.state, 2)?;
            protect_lua!(self.state, 0, 1, fn(state) {
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
            let _sg = StackGuard::new(self.state);
            check_stack(self.state, 2)?;

            let mut ar: ffi::lua_Debug = mem::zeroed();

            let top = ffi::lua_gettop(self.state) as c_int;

            let mut level = 1;

            while ffi::lua_getstack(self.state, level, &mut ar) != 0 {
                let status = ffi::lua_getinfo(self.state, cstr!("ful"), &mut ar);
                debug_assert!(status != 0);

                for n in 0..ar.nups as i32 {
                    let name_str_ptr = ffi::lua_getupvalue(self.state, -1, n + 1);

                    let name = CStr::from_ptr(name_str_ptr);
                    let is_table = ffi::lua_istable(self.state, top + 2) == 1;

                    if is_table && name.to_bytes() == "_ENV".as_bytes() {
                        return Ok(Table(self.pop_ref()));
                    }

                    // take arg off
                    ffi::lua_settop(self.state, top + 1);
                }

                // take func off
                ffi::lua_settop(self.state, top);

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
            // rollback registry tracker
            let mut registry_tracker = self.registry_tracker.lock().expect("unref list poisoned");

            // invalidate current keys
            for validity in &registry_tracker.recent_key_validity {
                validity.1.store(false, Ordering::Relaxed);

                unsafe {
                    ffi::luaL_unref(self.state, ffi::LUA_REGISTRYINDEX, validity.0);
                }
            }

            // invalidate keys in dropped snapshots
            for snapshot in self.snapshots.range(index + 1..) {
                for validity in &snapshot.recent_key_validity {
                    validity.1.store(false, Ordering::Relaxed);

                    unsafe {
                        ffi::luaL_unref(self.state, ffi::LUA_REGISTRYINDEX, validity.0);
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
            // rollback memory
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
            // rollback lua snapshot

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
                let used_kbytes = ffi::lua_gc(self.state, ffi::LUA_GCCOUNT, 0);
                let used_kbytes_rem = ffi::lua_gc(self.state, ffi::LUA_GCCOUNTB, 0);
                (used_kbytes as usize) * 1024 + (used_kbytes_rem as usize)
            },
        }
    }

    /// Returns true if the garbage collector is currently running automatically.
    ///
    /// Requires `feature = "lua54/lua53/lua52"`
    #[cfg(any(feature = "lua54", feature = "lua53", feature = "lua52"))]
    pub fn gc_is_running(&self) -> bool {
        unsafe { ffi::lua_gc(self.state, ffi::LUA_GCISRUNNING, 0) != 0 }
    }

    /// Stop the Lua GC from running
    pub fn gc_stop(&self) {
        unsafe { ffi::lua_gc(self.state, ffi::LUA_GCSTOP, 0) };
    }

    /// Restarts the Lua GC if it is not running
    pub fn gc_restart(&self) {
        unsafe { ffi::lua_gc(self.state, ffi::LUA_GCRESTART, 0) };
    }

    /// Perform a full garbage-collection cycle.
    ///
    /// It may be necessary to call this function twice to collect all currently unreachable
    /// objects. Once to finish the current gc cycle, and once to start and finish the next cycle.
    pub fn gc_collect(&self) -> Result<()> {
        unsafe {
            check_stack(self.state, 2)?;
            protect_lua!(self.state, 0, 0, fn(state) ffi::lua_gc(state, ffi::LUA_GCCOLLECT, 0))
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
            check_stack(self.state, 3)?;
            protect_lua!(self.state, 0, 0, |state| {
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
        unsafe { ffi::lua_gc(self.state, ffi::LUA_GCSETPAUSE, pause) }
    }

    /// Sets the 'step multiplier' value of the collector.
    ///
    /// Returns the previous value of the 'step multiplier'. More information can be found in the
    /// Lua [documentation].
    ///
    /// [documentation]: https://www.lua.org/manual/5.4/manual.html#2.5
    pub fn gc_set_step_multiplier(&self, step_multiplier: c_int) -> c_int {
        unsafe { ffi::lua_gc(self.state, ffi::LUA_GCSETSTEPMUL, step_multiplier) }
    }

    /// Changes the collector to incremental mode with the given parameters.
    ///
    /// Returns the previous mode (always `GCMode::Incremental` in Lua < 5.4).
    /// More information can be found in the Lua [documentation].
    ///
    /// [documentation]: https://www.lua.org/manual/5.4/manual.html#2.5.1
    pub fn gc_inc(&self, pause: c_int, step_multiplier: c_int, step_size: c_int) -> GCMode {
        let state = self.state;

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
    #[cfg(any(feature = "lua54"))]
    #[cfg_attr(docsrs, doc(cfg(feature = "lua54")))]
    pub fn gc_gen(&self, minor_multiplier: c_int, major_multiplier: c_int) -> GCMode {
        let state = self.state;
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
    pub fn load<'lua, 'a, S>(&'lua self, chunk: &'a S) -> Chunk<'lua, 'a>
    where
        S: AsChunk<'lua> + ?Sized,
    {
        self.snapshot.borrow_mut().mark_modified();

        let caller = Location::caller();
        let name = chunk.name().unwrap_or_else(|| caller.to_string());

        Chunk {
            lua: self,
            source: chunk.source(),
            name: Some(name),
            env: chunk.env(self),
            mode: chunk.mode(),
        }
    }

    pub(crate) fn load_chunk<'lua>(
        &'lua self,
        source: &[u8],
        name: Option<&CStr>,
        env: Option<Value<'lua>>,
        mode: Option<ChunkMode>,
    ) -> Result<Function<'lua>> {
        unsafe {
            let _sg = StackGuard::new(self.state);
            check_stack(self.state, 1)?;

            let mode_str = match mode {
                Some(ChunkMode::Binary) => cstr!("b"),
                Some(ChunkMode::Text) => cstr!("t"),
                None => cstr!("bt"),
            };

            match ffi::luaL_loadbufferx(
                self.state,
                source.as_ptr() as *const c_char,
                source.len(),
                name.map(|n| n.as_ptr()).unwrap_or_else(ptr::null),
                mode_str,
            ) {
                ffi::LUA_OK => {
                    if let Some(env) = env {
                        self.push_value(env)?;
                        #[cfg(any(feature = "lua54", feature = "lua53", feature = "lua52"))]
                        ffi::lua_setupvalue(self.state, -2, 1);
                        #[cfg(any(feature = "lua51"))]
                        ffi::lua_setfenv(self.state, -2);
                    }
                    Ok(Function(self.pop_ref()))
                }
                err => Err(pop_error(self.state, err)),
            }
        }
    }

    /// Create and return an interned Lua string. Lua strings can be arbitrary [u8] data including
    /// embedded nulls, so in addition to `&str` and `&String`, you can also pass plain `&[u8]`
    /// here.
    pub fn create_string<S>(&self, s: &S) -> Result<String>
    where
        S: AsRef<[u8]> + ?Sized,
    {
        unsafe {
            if self.unlikely_memory_error() {
                push_string(self.ref_thread, s.as_ref(), false)?;
                return Ok(String(self.pop_ref_thread()));
            }

            let _sg = StackGuard::new(self.state);
            check_stack(self.state, 3)?;

            push_string(self.state, s.as_ref(), true)?;
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
    pub fn create_table_with_capacity(&self, narr: c_int, nrec: c_int) -> Result<Table> {
        unsafe {
            if self.unlikely_memory_error() {
                push_table(self.ref_thread, narr, nrec, false)?;
                return Ok(Table(self.pop_ref_thread()));
            }

            let _sg = StackGuard::new(self.state);
            check_stack(self.state, 3)?;

            push_table(self.state, narr, nrec, true)?;
            Ok(Table(self.pop_ref()))
        }
    }

    /// Creates a table and fills it with values from an iterator.
    pub fn create_table_from<'lua, K, V, I>(&'lua self, iter: I) -> Result<Table<'lua>>
    where
        K: ToLua<'lua>,
        V: ToLua<'lua>,
        I: IntoIterator<Item = (K, V)>,
    {
        unsafe {
            let _sg = StackGuard::new(self.state);
            check_stack(self.state, 6)?;

            let iter = iter.into_iter();
            let lower_bound = iter.size_hint().0;
            let protect = !self.unlikely_memory_error();
            push_table(self.state, 0, lower_bound as c_int, protect)?;
            for (k, v) in iter {
                self.push_value(k.to_lua(self)?)?;
                self.push_value(v.to_lua(self)?)?;
                if protect {
                    protect_lua!(self.state, 3, 1, fn(state) ffi::lua_rawset(state, -3))?;
                } else {
                    ffi::lua_rawset(self.state, -3);
                }
            }

            Ok(Table(self.pop_ref()))
        }
    }

    /// Creates a table from an iterator of values, using `1..` as the keys.
    pub fn create_sequence_from<'lua, T, I>(&'lua self, iter: I) -> Result<Table<'lua>>
    where
        T: ToLua<'lua>,
        I: IntoIterator<Item = T>,
    {
        unsafe {
            let _sg = StackGuard::new(self.state);
            check_stack(self.state, 5)?;

            let iter = iter.into_iter();
            let lower_bound = iter.size_hint().0;
            let protect = !self.unlikely_memory_error();
            push_table(self.state, lower_bound as c_int, 0, protect)?;
            for (i, v) in iter.enumerate() {
                self.push_value(v.to_lua(self)?)?;
                if protect {
                    protect_lua!(self.state, 2, 1, |state| {
                        ffi::lua_rawseti(state, -2, (i + 1) as Integer);
                    })?;
                } else {
                    ffi::lua_rawseti(self.state, -2, (i + 1) as Integer);
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
    /// values. For details on Rust-to-Lua conversions, refer to the [`ToLua`] and [`ToLuaMulti`]
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
    /// [`ToLua`]: crate::ToLua
    /// [`ToLuaMulti`]: crate::ToLuaMulti
    pub fn create_function<'lua, A, R, F>(&'lua self, func: F) -> Result<Function<'lua>>
    where
        A: FromLuaMulti<'lua>,
        R: ToLuaMulti<'lua>,
        F: 'static + Fn(&'lua Lua, A) -> Result<R>,
    {
        self.create_callback(Box::new(move |lua, args| {
            func(lua, A::from_lua_multi(args, lua)?)?.to_lua_multi(lua)
        }))
    }

    /// Returns a handle to the global environment.
    pub fn globals(&self) -> Table {
        self.snapshot.borrow_mut().mark_modified();

        unsafe {
            let _sg = StackGuard::new(self.state);
            check_stack(self.state, 1).unwrap();
            #[cfg(any(feature = "lua54", feature = "lua53", feature = "lua52"))]
            ffi::lua_rawgeti(self.state, ffi::LUA_REGISTRYINDEX, ffi::LUA_RIDX_GLOBALS);
            #[cfg(any(feature = "lua51"))]
            ffi::lua_pushvalue(self.state, ffi::LUA_GLOBALSINDEX);
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
    pub fn scope<'lua, 'scope, R, F>(&'lua self, f: F) -> Result<R>
    where
        'lua: 'scope,
        R: 'static,
        F: FnOnce(&Scope<'lua, 'scope>) -> Result<R>,
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
                let _sg = StackGuard::new(self.state);
                check_stack(self.state, 4)?;

                self.push_value(v)?;
                let res = if self.unlikely_memory_error() {
                    ffi::lua_tolstring(self.state, -1, ptr::null_mut())
                } else {
                    protect_lua!(self.state, 1, 1, |state| {
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
                let _sg = StackGuard::new(self.state);
                check_stack(self.state, 2)?;

                self.push_value(v)?;
                let mut isint = 0;
                let i = ffi::lua_tointegerx(self.state, -1, &mut isint);
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
                let _sg = StackGuard::new(self.state);
                check_stack(self.state, 2)?;

                self.push_value(v)?;
                let mut isnum = 0;
                let n = ffi::lua_tonumberx(self.state, -1, &mut isnum);
                if isnum == 0 {
                    None
                } else {
                    Some(n)
                }
            },
        })
    }

    /// Converts a value that implements `ToLua` into a `Value` instance.
    pub fn pack<'lua, T: ToLua<'lua>>(&'lua self, t: T) -> Result<Value<'lua>> {
        t.to_lua(self)
    }

    /// Converts a `Value` instance into a value that implements `FromLua`.
    pub fn unpack<'lua, T: FromLua<'lua>>(&'lua self, value: Value<'lua>) -> Result<T> {
        T::from_lua(value, self)
    }

    /// Converts a value that implements `ToLuaMulti` into a `MultiValue` instance.
    pub fn pack_multi<'lua, T: ToLuaMulti<'lua>>(&'lua self, t: T) -> Result<MultiValue<'lua>> {
        t.to_lua_multi(self)
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
    pub fn set_named_registry_value<'lua, S, T>(&'lua self, name: &S, t: T) -> Result<()>
    where
        S: AsRef<[u8]> + ?Sized,
        T: ToLua<'lua>,
    {
        self.snapshot.borrow_mut().mark_modified();

        let t = t.to_lua(self)?;
        unsafe {
            let _sg = StackGuard::new(self.state);
            check_stack(self.state, 5)?;

            self.push_value(t)?;
            rawset_field(self.state, ffi::LUA_REGISTRYINDEX, name)
        }
    }

    /// Get a value from the Lua registry based on a string name.
    ///
    /// Any Lua instance which shares the underlying main state may call this method to
    /// get a value previously set by [`set_named_registry_value`].
    ///
    /// [`set_named_registry_value`]: #method.set_named_registry_value
    pub fn named_registry_value<'lua, S, T>(&'lua self, name: &S) -> Result<T>
    where
        S: AsRef<[u8]> + ?Sized,
        T: FromLua<'lua>,
    {
        self.snapshot.borrow_mut().mark_modified();

        let value = unsafe {
            let _sg = StackGuard::new(self.state);
            check_stack(self.state, 3)?;

            let protect = !self.unlikely_memory_error();
            push_string(self.state, name.as_ref(), protect)?;
            ffi::lua_rawget(self.state, ffi::LUA_REGISTRYINDEX);

            self.pop_value()
        };
        T::from_lua(value, self)
    }

    /// Removes a named value in the Lua registry.
    ///
    /// Equivalent to calling [`set_named_registry_value`] with a value of Nil.
    ///
    /// [`set_named_registry_value`]: #method.set_named_registry_value
    pub fn unset_named_registry_value<S>(&self, name: &S) -> Result<()>
    where
        S: AsRef<[u8]> + ?Sized,
    {
        self.set_named_registry_value(name, Value::Nil)
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
    pub fn create_registry_value<'lua, T: ToLua<'lua>>(&'lua self, t: T) -> Result<RegistryKey> {
        self.snapshot.borrow_mut().mark_modified();

        let t = t.to_lua(self)?;
        unsafe {
            let _sg = StackGuard::new(self.state);
            check_stack(self.state, 4)?;

            self.push_value(t)?;

            // Try to reuse previously allocated RegistryKey
            let mut registry_tracker = self.registry_tracker.lock().expect("unref list poisoned");

            let registry_id = if let Some(registry_id) = registry_tracker.unref_list.pop() {
                // It must be safe to replace the value without triggering memory error
                ffi::lua_rawseti(self.state, ffi::LUA_REGISTRYINDEX, registry_id as Integer);
                registry_id
            } else {
                // Allocate a new RegistryKey
                protect_lua!(self.state, 1, 0, |state| {
                    ffi::luaL_ref(state, ffi::LUA_REGISTRYINDEX)
                })?
            };

            let validity = Arc::new((registry_id, AtomicBool::new(true)));

            if self.max_snapshots() > 0 {
                // no need to track validity for rollbacks if rolling back is not possible
                registry_tracker.recent_key_validity.push(validity.clone());
            }

            Ok(RegistryKey {
                registry_id,
                validity,
                registry_tracker: self.registry_tracker.clone(),
            })
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

        let value = unsafe {
            let _sg = StackGuard::new(self.state);
            check_stack(self.state, 1)?;

            ffi::lua_rawgeti(
                self.state,
                ffi::LUA_REGISTRYINDEX,
                key.registry_id as Integer,
            );
            self.pop_value()
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
            ffi::luaL_unref(self.state, ffi::LUA_REGISTRYINDEX, key.take());
        }

        Ok(())
    }

    /// Replaces a value in the Lua registry by its `RegistryKey`.
    ///
    /// See [`create_registry_value`] for more details.
    ///
    /// [`create_registry_value`]: #method.create_registry_value
    pub fn replace_registry_value<'lua, T: ToLua<'lua>>(
        &'lua self,
        key: &RegistryKey,
        t: T,
    ) -> Result<()> {
        if !self.owns_registry_value(key) {
            return Err(Error::MismatchedRegistryKey);
        }

        self.snapshot.borrow_mut().mark_modified();

        let t = t.to_lua(self)?;
        unsafe {
            let _sg = StackGuard::new(self.state);
            check_stack(self.state, 2)?;

            self.push_value(t)?;
            // It must be safe to replace the value without triggering memory error
            let id = key.registry_id as Integer;
            ffi::lua_rawseti(self.state, ffi::LUA_REGISTRYINDEX, id);
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
        unsafe {
            self.snapshot.borrow_mut().mark_modified();

            let mut registry_tracker = self.registry_tracker.lock().expect("unref list poisoned");
            let unref_list = mem::take(&mut registry_tracker.unref_list);
            for id in unref_list {
                ffi::luaL_unref(self.state, ffi::LUA_REGISTRYINDEX, id);
            }
        }
    }

    // Uses 2 stack spaces, does not call checkstack
    pub(crate) unsafe fn push_value(&self, value: Value) -> Result<()> {
        match value {
            Value::Nil => {
                ffi::lua_pushnil(self.state);
            }

            Value::Boolean(b) => {
                ffi::lua_pushboolean(self.state, b as c_int);
            }

            Value::Integer(i) => {
                ffi::lua_pushinteger(self.state, i);
            }

            Value::Number(n) => {
                ffi::lua_pushnumber(self.state, n);
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

            Value::Error(err) => {
                let protect = !self.unlikely_memory_error();
                check_stack(self.state, 4)?;
                check_stack(self.ref_thread, 1)?;
                push_gc_userdata(self, WrappedFailure::Error(err), protect)?;
            }
        }

        Ok(())
    }

    // Uses 2 stack spaces, does not call checkstack
    pub(crate) unsafe fn pop_value(&self) -> Value {
        let state = self.state;

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
                use std::panic::resume_unwind;

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

            ffi::LUA_TTHREAD => {
                ffi::lua_pop(state, 1);
                Value::Nil
            }

            _ => panic!("LUA_TNONE in pop_value"),
        }
    }

    // Pushes a LuaRef value onto the stack, uses 1 stack space, does not call checkstack
    pub(crate) unsafe fn push_ref(&self, lref: &LuaRef) {
        assert!(
            ptr::eq(lref.lua.0, self.0),
            "Lua instance passed Value created from a different main Lua state"
        );
        ffi::lua_xpush(self.ref_thread, self.state, lref.index);
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
        ffi::lua_xmove(self.state, self.ref_thread, 1);
        let index = self.ref_stack_pop();
        LuaRef { lua: self, index }
    }

    // Same as `pop_ref` but assumes the value is already on the reference thread
    pub(crate) unsafe fn pop_ref_thread(&self) -> LuaRef {
        let index = self.ref_stack_pop();
        LuaRef { lua: self, index }
    }

    pub(crate) fn clone_ref<'lua>(&'lua self, lref: &LuaRef<'lua>) -> LuaRef<'lua> {
        unsafe {
            ffi::lua_pushvalue(self.ref_thread, lref.index);
            let index = self.ref_stack_pop();
            LuaRef { lua: self, index }
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
        unsafe extern "C" fn call_callback(state: *mut ffi::lua_State) -> c_int {
            let lua_inner = match ffi::lua_type(state, ffi::lua_upvalueindex(1)) {
                ffi::LUA_TUSERDATA => {
                    let data_ref = get_userdata_ref(state, ffi::lua_upvalueindex(1));
                    data_ref.lua_inner
                }
                _ => ptr::null_mut(),
            };
            callback_error(state, |nargs| {
                let upvalue_idx = ffi::lua_upvalueindex(1);
                if ffi::lua_type(state, upvalue_idx) == ffi::LUA_TNIL {
                    return Err(Error::CallbackDestructed);
                }
                let upvalue = get_gc_userdata::<Callback>(state, upvalue_idx, ptr::null());

                if nargs < ffi::LUA_MINSTACK {
                    check_stack(state, ffi::LUA_MINSTACK - nargs)?;
                }

                let lua: &Lua = mem::transmute(&lua_inner);

                let mut args = MultiValue::new_or_cached(lua);
                args.reserve(nargs as usize);
                for _ in 0..nargs {
                    args.push_front(lua.pop_value());
                }

                let func = &*upvalue;
                let mut results = func(lua, args)?;
                let nresults = results.len() as c_int;

                check_stack(state, nresults)?;
                for r in results.drain_all() {
                    lua.push_value(r)?;
                }
                lua.cache_multivalue(results);

                Ok(nresults)
            })
        }

        self.snapshot.borrow_mut().mark_modified();

        unsafe {
            let _sg = StackGuard::new(self.state);
            check_stack(self.state, 4)?;

            let func: Callback = mem::transmute(func);
            let protect = !self.unlikely_memory_error();
            push_gc_userdata(self, func, protect)?;
            if protect {
                protect_lua!(self.state, 1, 1, fn(state) {
                    ffi::lua_pushcclosure(state, call_callback, 1);
                })?;
            } else {
                ffi::lua_pushcclosure(self.state, call_callback, 1);
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
        #[cfg(any(feature = "lua51"))]
        let searchers: Table = package.get("loaders")?;

        let loader = self.create_function(|_, ()| Ok("\n\tcan't load C modules in safe mode"))?;

        // The third and fourth searchers looks for a loader as a C library
        searchers.raw_set(3, loader.clone())?;
        searchers.raw_remove(4)?;

        Ok(())
    }

    #[inline]
    pub(crate) fn new_or_cached_multivalue(&self) -> MultiValue {
        let mut multivalue_cache = self.multivalue_cache.borrow_mut();
        multivalue_cache.pop().unwrap_or_default()
    }

    #[inline]
    pub(crate) fn cache_multivalue(&self, mut multivalue: MultiValue) {
        unsafe {
            let mut multivalue_cache = self.multivalue_cache.borrow_mut();

            if multivalue_cache.len() < MULTIVALUE_CACHE_SIZE {
                multivalue.clear();
                multivalue_cache.push(mem::transmute(multivalue));
            }
        }
    }

    #[inline]
    pub(crate) unsafe fn unlikely_memory_error(&self) -> bool {
        self.memory.is_none()
    }
}

// Uses 3 stack spaces
unsafe fn load_from_std_lib(state: *mut ffi::lua_State, libs: StdLib) -> Result<()> {
    #[inline(always)]
    pub unsafe fn requiref<S: AsRef<[u8]> + ?Sized>(
        state: *mut ffi::lua_State,
        modname: &S,
        openf: ffi::lua_CFunction,
        glb: c_int,
    ) -> Result<()> {
        let modname = CString::new(modname.as_ref()).expect("modname contains nil byte");
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

    #[cfg(any(feature = "lua52"))]
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
