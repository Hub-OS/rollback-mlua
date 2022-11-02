use crate::ffi;
use crate::lua::Lua;
use crate::util;
use std::fmt;
use std::os::raw::c_int;

pub(crate) struct LuaRef<'lua> {
    pub(crate) lua: &'lua Lua,
    pub(crate) index: c_int,
}

impl<'lua> fmt::Debug for LuaRef<'lua> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Ref({})", self.index)
    }
}

impl<'lua> Clone for LuaRef<'lua> {
    fn clone(&self) -> Self {
        self.lua.clone_ref(self)
    }
}

impl<'lua> Drop for LuaRef<'lua> {
    fn drop(&mut self) {
        if self.index > 0 {
            self.lua.drop_ref(self);
        }
    }
}

impl<'lua> PartialEq for LuaRef<'lua> {
    fn eq(&self, other: &Self) -> bool {
        let lua = self.lua;
        unsafe {
            let _sg = util::StackGuard::new(lua.state);
            util::check_stack(lua.state, 2).unwrap();
            lua.push_ref(self);
            lua.push_ref(other);
            ffi::lua_rawequal(lua.state, -1, -2) == 1
        }
    }
}
