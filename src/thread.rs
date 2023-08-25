use std::os::raw::c_int;

use crate::error::{Error, Result};
#[allow(unused)]
use crate::lua::Lua;
use crate::types::LuaRef;
use crate::util::{check_stack, error_traceback_thread, pop_error, StackGuard};
use crate::value::{FromLuaMulti, IntoLuaMulti};

/// Status of a Lua thread (coroutine).
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ThreadStatus {
    /// The thread was just created, or is suspended because it has called `coroutine.yield`.
    ///
    /// If a thread is in this state, it can be resumed by calling [`Thread::resume`].
    ///
    /// [`Thread::resume`]: crate::Thread::resume
    Resumable,
    /// Either the thread has finished executing, or the thread is currently running.
    Unresumable,
    /// The thread has raised a Lua error during execution.
    Error,
}

/// Handle to an internal Lua thread (coroutine).
#[derive(Clone, Debug)]
pub struct Thread<'lua>(pub(crate) LuaRef<'lua>, pub(crate) *mut ffi::lua_State);

/// Owned handle to an internal Lua thread (coroutine).
///
/// The owned handle holds a *strong* reference to the current Lua instance.
/// Be warned, if you place it into a Lua type (eg. [`UserData`] or a Rust callback), it is *very easy*
/// to accidentally cause reference cycles that would prevent destroying Lua instance.
///
/// [`UserData`]: crate::UserData
#[cfg(feature = "unstable")]
#[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
#[derive(Clone, Debug)]
pub struct OwnedThread(
    pub(crate) crate::types::LuaOwnedRef,
    pub(crate) *mut ffi::lua_State,
);

#[cfg(feature = "unstable")]
impl OwnedThread {
    /// Get borrowed handle to the underlying Lua table.
    #[cfg_attr(feature = "send", allow(unused))]
    pub const fn to_ref(&self) -> Thread {
        Thread(self.0.to_ref(), self.1)
    }
}

impl<'lua> Thread<'lua> {
    #[inline(always)]
    pub(crate) fn new(r#ref: LuaRef<'lua>) -> Self {
        let state = unsafe { ffi::lua_tothread(r#ref.lua.ref_thread(), r#ref.index) };
        Thread(r#ref, state)
    }

    const fn state(&self) -> *mut ffi::lua_State {
        self.1
    }

    /// Resumes execution of this thread.
    ///
    /// Equivalent to `coroutine.resume`.
    ///
    /// Passes `args` as arguments to the thread. If the coroutine has called `coroutine.yield`, it
    /// will return these arguments. Otherwise, the coroutine wasn't yet started, so the arguments
    /// are passed to its main function.
    ///
    /// If the thread is no longer in `Active` state (meaning it has finished execution or
    /// encountered an error), this will return `Err(CoroutineInactive)`, otherwise will return `Ok`
    /// as follows:
    ///
    /// If the thread calls `coroutine.yield`, returns the values passed to `yield`. If the thread
    /// `return`s values from its main function, returns those.
    ///
    /// # Examples
    ///
    /// ```
    /// # use mlua::{Error, Lua, Result, Thread};
    /// # fn main() -> Result<()> {
    /// # let lua = Lua::new();
    /// let thread: Thread = lua.load(r#"
    ///     coroutine.create(function(arg)
    ///         assert(arg == 42)
    ///         local yieldarg = coroutine.yield(123)
    ///         assert(yieldarg == 43)
    ///         return 987
    ///     end)
    /// "#).eval()?;
    ///
    /// assert_eq!(thread.resume::<_, u32>(42)?, 123);
    /// assert_eq!(thread.resume::<_, u32>(43)?, 987);
    ///
    /// // The coroutine has now returned, so `resume` will fail
    /// match thread.resume::<_, u32>(()) {
    ///     Err(Error::CoroutineInactive) => {},
    ///     unexpected => panic!("unexpected result {:?}", unexpected),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn resume<A, R>(&self, args: A) -> Result<R>
    where
        A: IntoLuaMulti<'lua>,
        R: FromLuaMulti<'lua>,
    {
        let lua = self.0.lua;
        let state = lua.state();
        let thread_state = self.state();
        unsafe {
            let _sg = StackGuard::new(state);
            let _thread_sg = StackGuard::with_top(thread_state, 0);

            let nresults = self.resume_inner(args)?;
            check_stack(state, nresults + 1)?;
            ffi::lua_xmove(thread_state, state, nresults);

            R::from_stack_multi(nresults, lua)
        }
    }

    /// Resumes execution of this thread.
    ///
    /// It's similar to `resume()` but leaves `nresults` values on the thread stack.
    unsafe fn resume_inner<A: IntoLuaMulti<'lua>>(&self, args: A) -> Result<c_int> {
        let lua = self.0.lua;
        let state = lua.state();
        let thread_state = self.state();

        if self.status() != ThreadStatus::Resumable {
            return Err(Error::CoroutineInactive);
        }

        let nargs = args.push_into_stack_multi(lua)?;
        if nargs > 0 {
            check_stack(thread_state, nargs)?;
            ffi::lua_xmove(state, thread_state, nargs);
        }

        let mut nresults = 0;
        let ret = ffi::lua_resume(thread_state, state, nargs, &mut nresults as *mut c_int);
        if ret != ffi::LUA_OK && ret != ffi::LUA_YIELD {
            if ret == ffi::LUA_ERRMEM {
                // Don't call error handler for memory errors
                return Err(pop_error(thread_state, ret));
            }
            check_stack(state, 3)?;
            protect_lua!(state, 0, 1, |state| error_traceback_thread(
                state,
                thread_state
            ))?;
            return Err(pop_error(state, ret));
        }

        Ok(nresults)
    }

    /// Gets the status of the thread.
    pub fn status(&self) -> ThreadStatus {
        let thread_state = self.state();
        unsafe {
            let status = ffi::lua_status(thread_state);
            if status != ffi::LUA_OK && status != ffi::LUA_YIELD {
                ThreadStatus::Error
            } else if status == ffi::LUA_YIELD || ffi::lua_gettop(thread_state) > 0 {
                ThreadStatus::Resumable
            } else {
                ThreadStatus::Unresumable
            }
        }
    }

    /// Resets a thread
    ///
    /// In [Lua 5.4]: cleans its call stack and closes all pending to-be-closed variables.
    /// Returns a error in case of either the original error that stopped the thread or errors
    /// in closing methods.
    ///
    /// In Luau: resets to the initial state of a newly created Lua thread.
    /// Lua threads in arbitrary states (like yielded or errored) can be reset properly.
    ///
    /// Sets a Lua function for the thread afterwards.
    ///
    /// Requires `feature = "lua54"`.
    ///
    /// [Lua 5.4]: https://www.lua.org/manual/5.4/manual.html#lua_closethread
    #[cfg(feature = "lua54")]
    pub fn reset(&self, func: crate::function::Function<'lua>) -> Result<()> {
        let lua = self.0.lua;
        let thread_state = self.state();
        unsafe {
            #[cfg(all(feature = "lua54", not(feature = "vendored")))]
            let status = ffi::lua_resetthread(thread_state);
            #[cfg(all(feature = "lua54", feature = "vendored"))]
            let status = ffi::lua_closethread(thread_state, lua.state());
            #[cfg(feature = "lua54")]
            if status != ffi::LUA_OK {
                return Err(pop_error(thread_state, status));
            }

            // Push function to the top of the thread stack
            ffi::lua_xpush(lua.ref_thread(), thread_state, func.0.index);

            Ok(())
        }
    }

    /// Convert this handle to owned version.
    #[cfg(all(feature = "unstable", any(not(feature = "send"), doc)))]
    #[cfg_attr(docsrs, doc(cfg(all(feature = "unstable", not(feature = "send")))))]
    #[inline]
    pub fn into_owned(self) -> OwnedThread {
        OwnedThread(self.0.into_owned(), self.1)
    }
}

impl<'lua> PartialEq for Thread<'lua> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

// Additional shortcuts
#[cfg(feature = "unstable")]
impl OwnedThread {
    /// Resumes execution of this thread.
    ///
    /// See [`Thread::resume()`] for more details.
    pub fn resume<'lua, A, R>(&'lua self, args: A) -> Result<R>
    where
        A: IntoLuaMulti<'lua>,
        R: FromLuaMulti<'lua>,
    {
        self.to_ref().resume(args)
    }

    /// Gets the status of the thread.
    pub fn status(&self) -> ThreadStatus {
        self.to_ref().status()
    }
}

#[cfg(test)]
mod assertions {
    use super::*;

    static_assertions::assert_not_impl_any!(Thread: Send);
}
