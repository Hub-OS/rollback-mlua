//! Re-exports most types with an extra `Lua*` prefix to prevent name clashes.

#[doc(no_inline)]
pub use crate::{
    Chunk as LuaChunk, Error as LuaError, ExternalError as LuaExternalError,
    ExternalResult as LuaExternalResult, FromLua, FromLuaMulti, Function as LuaFunction,
    FunctionInfo as LuaFunctionInfo, GCMode as LuaGCMode, Integer as LuaInteger, Lua,
    MultiValue as LuaMultiValue, Nil as LuaNil, Number as LuaNumber, RegistryKey as LuaRegistryKey,
    Result as LuaResult, StdLib as LuaStdLib, String as LuaString, Table as LuaTable,
    TableExt as LuaTableExt, TablePairs as LuaTablePairs, TableSequence as LuaTableSequence, ToLua,
    ToLuaMulti, Value as LuaValue,
};
