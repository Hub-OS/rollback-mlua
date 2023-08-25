use slotmap::DefaultKey as GenerationalIndex;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::os::raw::{c_int, c_void};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::lua::{Lua, LuaInner};

#[cfg(feature = "unstable")]
use std::marker::PhantomData;

/// Type of Lua integer numbers.
pub type Integer = ffi::lua_Integer;
/// Type of Lua floating point numbers.
pub type Number = ffi::lua_Number;

/// A "light" userdata value. Equivalent to an unmanaged raw pointer.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct LightUserData(pub *mut c_void);

pub(crate) type Callback<'lua, 'a> = Box<dyn Fn(&'lua Lua, c_int) -> Result<c_int> + 'a>;

#[derive(Clone, Copy)]
pub(crate) struct UserDataRef {
    pub(crate) ref_index: c_int,
    pub(crate) destructor_index: GenerationalIndex,
    pub(crate) lua_inner: *const LuaInner,
}

use crate::registry::RegistryTracker;
use crate::util;

#[cfg(feature = "send")]
pub trait MaybeSend: Send {}
#[cfg(feature = "send")]
impl<T: Send> MaybeSend for T {}

#[cfg(not(feature = "send"))]
pub trait MaybeSend {}
#[cfg(not(feature = "send"))]
impl<T> MaybeSend for T {}

/// An auto generated key into the Lua registry.
///
/// This is a handle to a value stored inside the Lua registry. It is not automatically
/// garbage collected on Drop, but it can be removed with [`Lua::remove_registry_value`],
/// and instances not manually removed can be garbage collected with [`Lua::expire_registry_values`].
///
/// Be warned, If you place this into Lua via a [`UserData`] type or a rust callback, it is *very
/// easy* to accidentally cause reference cycles that the Lua garbage collector cannot resolve.
/// Instead of placing a [`RegistryKey`] into a [`UserData`] type, prefer instead to use
/// [`AnyUserData::set_user_value`] / [`AnyUserData::user_value`].
///
/// [`UserData`]: crate::UserData
/// [`RegistryKey`]: crate::RegistryKey
/// [`Lua::remove_registry_value`]: crate::Lua::remove_registry_value
/// [`Lua::expire_registry_values`]: crate::Lua::expire_registry_values
/// [`AnyUserData::set_user_value`]: crate::AnyUserData::set_user_value
/// [`AnyUserData::user_value`]: crate::AnyUserData::user_value

pub struct RegistryKey {
    pub(crate) registry_id: c_int,
    pub(crate) validity: Arc<(c_int, AtomicBool)>,
    pub(crate) is_nil: AtomicBool,
    pub(crate) registry_tracker: Arc<Mutex<RegistryTracker>>,
}

impl fmt::Debug for RegistryKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RegistryKey({})", self.validity.0)
    }
}

impl Hash for RegistryKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.registry_id.hash(state)
    }
}

impl PartialEq for RegistryKey {
    fn eq(&self, other: &RegistryKey) -> bool {
        Arc::ptr_eq(&self.validity, &other.validity)
    }
}

impl Eq for RegistryKey {}

impl Drop for RegistryKey {
    fn drop(&mut self) {
        if self.is_valid() && !self.is_nil() {
            let mut registry_tracker = self.registry_tracker.lock().expect("unref list poisoned");
            registry_tracker.unref_list.push(self.validity.0);
        }
    }
}

impl RegistryKey {
    // Creates a new instance of `RegistryKey`
    pub(crate) fn new(id: c_int, registry_tracker: Arc<Mutex<RegistryTracker>>) -> Self {
        RegistryKey {
            registry_id: id,
            validity: Arc::new((id, AtomicBool::new(true))),
            is_nil: AtomicBool::new(id == ffi::LUA_REFNIL),
            registry_tracker,
        }
    }

    // Destroys the `RegistryKey` without adding to the unref list
    pub(crate) fn take(self) -> c_int {
        let registry_id = self.registry_id;
        unsafe {
            std::ptr::read(&self.validity);
            std::ptr::read(&self.registry_tracker);
            std::mem::forget(self);
        }
        registry_id
    }

    // Returns true if this `RegistryKey` holds a nil value
    #[inline(always)]
    pub(crate) fn is_nil(&self) -> bool {
        self.is_nil.load(Ordering::Relaxed)
    }

    // Marks value of this `RegistryKey` as `Nil`
    #[inline(always)]
    pub(crate) fn set_nil(&self, enabled: bool) {
        // We cannot replace previous value with nil in as this will break
        // Lua mechanism to find free keys.
        // Instead, we set a special flag to mark value as nil.
        self.is_nil.store(enabled, Ordering::Relaxed);
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.validity.1.load(Ordering::Relaxed)
    }
}

pub(crate) struct LuaRef<'lua> {
    pub(crate) lua: &'lua Lua,
    pub(crate) index: c_int,
    pub(crate) drop: bool,
}

impl<'lua> LuaRef<'lua> {
    pub(crate) const fn new(lua: &'lua Lua, index: c_int) -> Self {
        LuaRef {
            lua,
            index,
            drop: true,
        }
    }

    #[inline]
    pub(crate) fn to_pointer(&self) -> *const c_void {
        unsafe { ffi::lua_topointer(self.lua.ref_thread(), self.index) }
    }

    #[cfg(feature = "unstable")]
    #[inline]
    pub(crate) fn into_owned(self) -> LuaOwnedRef {
        assert!(self.drop, "Cannot turn non-drop reference into owned");
        let owned_ref = LuaOwnedRef::new(self.lua.clone(), self.index);
        mem::forget(self);
        owned_ref
    }
}

impl<'lua> fmt::Debug for LuaRef<'lua> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Ref({:p})", self.to_pointer())
    }
}

impl<'lua> Clone for LuaRef<'lua> {
    fn clone(&self) -> Self {
        self.lua.clone_ref(self)
    }
}

impl<'lua> Drop for LuaRef<'lua> {
    fn drop(&mut self) {
        if self.drop {
            self.lua.drop_ref(self);
        }
    }
}

impl<'lua> PartialEq for LuaRef<'lua> {
    fn eq(&self, other: &Self) -> bool {
        let lua = self.lua;
        let state = lua.state();
        unsafe {
            let _sg = util::StackGuard::new(state);
            util::check_stack(state, 2).unwrap();
            lua.push_ref(self);
            lua.push_ref(other);
            ffi::lua_rawequal(state, -1, -2) == 1
        }
    }
}

#[cfg(feature = "unstable")]
pub(crate) struct LuaOwnedRef {
    pub(crate) inner: Arc<LuaInner>,
    pub(crate) index: c_int,
    _non_send: PhantomData<*const ()>,
}

#[cfg(feature = "unstable")]
impl fmt::Debug for LuaOwnedRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OwnedRef({:p})", self.to_ref().to_pointer())
    }
}

#[cfg(feature = "unstable")]
impl Clone for LuaOwnedRef {
    fn clone(&self) -> Self {
        self.to_ref().clone().into_owned()
    }
}

#[cfg(feature = "unstable")]
impl Drop for LuaOwnedRef {
    fn drop(&mut self) {
        let lua: &Lua = unsafe { mem::transmute(&self.inner) };
        lua.drop_ref_index(self.index);
    }
}

#[cfg(feature = "unstable")]
impl LuaOwnedRef {
    pub(crate) const fn new(inner: Arc<LuaInner>, index: c_int) -> Self {
        LuaOwnedRef {
            inner,
            index,
            _non_send: PhantomData,
        }
    }

    pub(crate) const fn to_ref(&self) -> LuaRef {
        LuaRef {
            lua: unsafe { mem::transmute(&self.inner) },
            index: self.index,
            drop: false,
        }
    }
}

#[cfg(test)]
mod assertions {
    use super::*;

    static_assertions::assert_impl_all!(RegistryKey: Send, Sync);
    static_assertions::assert_not_impl_any!(LuaRef: Send);

    #[cfg(feature = "unstable")]
    static_assertions::assert_not_impl_any!(LuaOwnedRef: Send);
}
