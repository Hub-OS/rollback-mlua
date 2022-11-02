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
        unsafe extern "C" fn do_call($state_inner: *mut ffi::lua_State) -> ::std::os::raw::c_int {
            $code;
            $nresults
        }

        crate::util::protect_lua_call($state, $nargs, do_call)
    }};
}
