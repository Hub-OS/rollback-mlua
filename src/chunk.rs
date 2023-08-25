use std::borrow::Cow;
use std::ffi::CString;
use std::io::Result as IoResult;
use std::path::{Path, PathBuf};
use std::string::String as StdString;

use crate::error::{Error, ErrorContext, Result};
use crate::function::Function;
use crate::lua::Lua;
use crate::table::Table;
use crate::value::{FromLuaMulti, IntoLua, IntoLuaMulti};

/// Trait for types [loadable by Lua] and convertible to a [`Chunk`]
///
/// [loadable by Lua]: https://www.lua.org/manual/5.4/manual.html#3.3.2
/// [`Chunk`]: crate::Chunk
pub trait AsChunk<'lua, 'a> {
    /// Returns optional chunk name
    fn name(&self) -> Option<StdString> {
        None
    }

    /// Returns optional chunk [environment]
    ///
    /// [environment]: https://www.lua.org/manual/5.4/manual.html#2.2
    fn environment(&self, lua: &'lua Lua) -> Result<Option<Table<'lua>>> {
        let _lua = lua; // suppress warning
        Ok(None)
    }

    /// Returns optional chunk mode (text or binary)
    fn mode(&self) -> Option<ChunkMode> {
        None
    }

    /// Returns chunk data (can be text or binary)
    fn source(self) -> IoResult<Cow<'a, [u8]>>;
}

impl<'a> AsChunk<'_, 'a> for &'a str {
    fn source(self) -> IoResult<Cow<'a, [u8]>> {
        Ok(Cow::Borrowed(self.as_ref()))
    }
}

impl AsChunk<'_, 'static> for StdString {
    fn source(self) -> IoResult<Cow<'static, [u8]>> {
        Ok(Cow::Owned(self.into_bytes()))
    }
}

impl<'a> AsChunk<'_, 'a> for &'a StdString {
    fn source(self) -> IoResult<Cow<'a, [u8]>> {
        Ok(Cow::Borrowed(self.as_bytes()))
    }
}

impl<'a> AsChunk<'_, 'a> for &'a [u8] {
    fn source(self) -> IoResult<Cow<'a, [u8]>> {
        Ok(Cow::Borrowed(self))
    }
}

impl AsChunk<'_, 'static> for Vec<u8> {
    fn source(self) -> IoResult<Cow<'static, [u8]>> {
        Ok(Cow::Owned(self))
    }
}

impl<'a> AsChunk<'_, 'a> for &'a Vec<u8> {
    fn source(self) -> IoResult<Cow<'a, [u8]>> {
        Ok(Cow::Borrowed(self.as_ref()))
    }
}

impl AsChunk<'_, 'static> for &Path {
    fn name(&self) -> Option<StdString> {
        Some(format!("@{}", self.display()))
    }

    fn source(self) -> IoResult<Cow<'static, [u8]>> {
        std::fs::read(self).map(Cow::Owned)
    }
}

impl AsChunk<'_, 'static> for PathBuf {
    fn name(&self) -> Option<StdString> {
        Some(format!("@{}", self.display()))
    }

    fn source(self) -> IoResult<Cow<'static, [u8]>> {
        std::fs::read(self).map(Cow::Owned)
    }
}

/// Returned from [`Lua::load`] and is used to finalize loading and executing Lua main chunks.
///
/// [`Lua::load`]: crate::Lua::load
#[must_use = "`Chunk`s do nothing unless one of `exec`, `eval`, `call`, or `into_function` are called on them"]
pub struct Chunk<'lua, 'a> {
    pub(crate) lua: &'lua Lua,
    pub(crate) name: StdString,
    pub(crate) env: Result<Option<Table<'lua>>>,
    pub(crate) mode: Option<ChunkMode>,
    pub(crate) source: IoResult<Cow<'a, [u8]>>,
}

/// Represents chunk mode (text or binary).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChunkMode {
    Text,
    Binary,
}

impl<'lua, 'a> Chunk<'lua, 'a> {
    /// Sets the name of this chunk, which results in more informative error traces.
    pub fn set_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Sets the environment of the loaded chunk to the given value.
    ///
    /// In Lua >=5.2 main chunks always have exactly one upvalue, and this upvalue is used as the `_ENV`
    /// variable inside the chunk. By default this value is set to the global environment.
    ///
    /// Calling this method changes the `_ENV` upvalue to the value provided, and variables inside
    /// the chunk will refer to the given environment rather than the global one.
    ///
    /// All global variables (including the standard library!) are looked up in `_ENV`, so it may be
    /// necessary to populate the environment in order for scripts using custom environments to be
    /// useful.
    pub fn set_environment<V: IntoLua<'lua>>(mut self, env: V) -> Self {
        self.env = env
            .into_lua(self.lua)
            .and_then(|val| self.lua.unpack(val))
            .context("bad environment value");
        self
    }

    /// Sets whether the chunk is text or binary (autodetected by default).
    ///
    /// Be aware, Lua does not check the consistency of the code inside binary chunks.
    /// Running maliciously crafted bytecode can crash the interpreter.
    pub fn set_mode(mut self, mode: ChunkMode) -> Self {
        self.mode = Some(mode);
        self
    }

    /// Execute this chunk of code.
    ///
    /// This is equivalent to calling the chunk function with no arguments and no return values.
    pub fn exec(self) -> Result<()> {
        self.call(())?;
        Ok(())
    }

    /// Evaluate the chunk as either an expression or block.
    ///
    /// If the chunk can be parsed as an expression, this loads and executes the chunk and returns
    /// the value that it evaluates to. Otherwise, the chunk is interpreted as a block as normal,
    /// and this is equivalent to calling `exec`.
    pub fn eval<R: FromLuaMulti<'lua>>(self) -> Result<R> {
        // Bytecode is always interpreted as a statement.
        // For source code, first try interpreting the lua as an expression by adding
        // "return", then as a statement. This is the same thing the
        // actual lua repl does.
        if self.detect_mode() == ChunkMode::Binary {
            self.call(())
        } else {
            let res = self.to_expression();

            match res {
                Ok(function) => function.call(()),
                Err(Error::MemoryError(_)) => Err(res.unwrap_err()),
                _ => self.call(()),
            }
        }
    }

    /// Load the chunk function and call it with the given arguments.
    ///
    /// This is equivalent to `into_function` and calling the resulting function.
    pub fn call<A: IntoLuaMulti<'lua>, R: FromLuaMulti<'lua>>(self, args: A) -> Result<R> {
        self.into_function()?.call(args)
    }

    /// Load this chunk into a regular `Function`.
    ///
    /// This simply compiles the chunk without actually executing it.
    pub fn into_function(self) -> Result<Function<'lua>> {
        let name = Self::convert_name(self.name)?;
        self.lua
            .load_chunk(Some(&name), self.env?, self.mode, self.source?.as_ref())
    }

    /// Compiles the chunk and changes mode to binary.
    ///
    /// It does nothing if the chunk is already binary.
    pub(crate) fn compile(&mut self) {
        if let Ok(ref source) = self.source {
            if self.detect_mode() == ChunkMode::Text {
                if let Ok(func) = self.lua.load_chunk(None, None, None, source.as_ref()) {
                    let data = func.dump(false);
                    self.source = Ok(Cow::Owned(data));
                    self.mode = Some(ChunkMode::Binary);
                }
            }
        }
    }

    fn to_expression(&self) -> Result<Function<'lua>> {
        // We assume that mode is Text
        let source = self.source.as_ref();
        let source = source.map_err(Error::runtime)?;
        let source = Self::expression_source(source);

        let name = Self::convert_name(self.name.clone())?;
        self.lua
            .load_chunk(Some(&name), self.env.clone()?, None, &source)
    }

    fn detect_mode(&self) -> ChunkMode {
        match (self.mode, &self.source) {
            (Some(mode), _) => mode,
            (None, Ok(source)) => {
                if source.starts_with(ffi::LUA_SIGNATURE) {
                    return ChunkMode::Binary;
                }
                ChunkMode::Text
            }
            (None, Err(_)) => ChunkMode::Text, // any value is fine
        }
    }

    fn convert_name(name: String) -> Result<CString> {
        CString::new(name).map_err(|err| Error::runtime(format!("invalid name: {err}")))
    }

    fn expression_source(source: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(b"return ".len() + source.len());
        buf.extend(b"return ");
        buf.extend(source);
        buf
    }
}
