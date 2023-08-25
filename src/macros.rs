macro_rules! cstr {
    ($s:expr) => {
        concat!($s, "\0") as *const str as *const [::std::os::raw::c_char]
            as *const ::std::os::raw::c_char
    };
}

macro_rules! protect_lua {
    ($state:expr, $nargs:expr, $nresults:expr, $f:expr) => {
        crate::util::protect_lua_closure($state, $nargs, $nresults, $f)
    };

    ($state:expr, $nargs:expr, $nresults:expr, fn($state_inner:ident) $code:expr) => {{
        use ::std::os::raw::c_int;
        unsafe extern "C-unwind" fn do_call($state_inner: *mut ffi::lua_State) -> c_int {
            $code;
            let nresults = $nresults;
            if nresults == ::ffi::LUA_MULTRET {
                ffi::lua_gettop($state_inner)
            } else {
                nresults
            }
        }

        crate::util::protect_lua_call($state, $nargs, do_call)
    }};
}
