use crate::error::Result;
use crate::ffi;
use crate::lua::{Lua, LuaInner};
use crate::value::MultiValue;
use slotmap::DefaultKey as GenerationalIndex;
use std::ffi::c_int;

/// Type of Lua integer numbers.
pub type Integer = ffi::lua_Integer;
/// Type of Lua floating point numbers.
pub type Number = ffi::lua_Number;

pub(crate) type Callback<'lua, 'a> =
    Box<dyn Fn(&'lua Lua, MultiValue<'lua>) -> Result<MultiValue<'lua>> + 'a>;

#[derive(Clone, Copy)]
pub(crate) struct UserDataRef {
    pub(crate) ref_index: c_int,
    pub(crate) destructor_index: GenerationalIndex,
    pub(crate) lua_inner: *const LuaInner,
}

pub(crate) use crate::lua_ref::LuaRef;

pub use crate::registry::RegistryKey;
