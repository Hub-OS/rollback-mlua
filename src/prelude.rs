//! Re-exports most types with an extra `Lua*` prefix to prevent name clashes.

#[doc(no_inline)]
pub use crate::{
    Chunk as LuaChunk, Error as LuaError, ErrorContext as LuaErrorContext,
    ExternalError as LuaExternalError, ExternalResult as LuaExternalResult, FromLua, FromLuaMulti,
    Function as LuaFunction, FunctionInfo as LuaFunctionInfo, GCMode as LuaGCMode,
    Integer as LuaInteger, IntoLua, IntoLuaMulti, LightUserData as LuaLightUserData, Lua,
    MultiValue as LuaMultiValue, Nil as LuaNil, Number as LuaNumber, RegistryKey as LuaRegistryKey,
    Result as LuaResult, StdLib as LuaStdLib, String as LuaString, Table as LuaTable,
    TableExt as LuaTableExt, TablePairs as LuaTablePairs, TableSequence as LuaTableSequence,
    Value as LuaValue,
};

#[doc(no_inline)]
pub use crate::HookTriggers as LuaHookTriggers;

#[cfg(feature = "serialize")]
#[doc(no_inline)]
pub use crate::{
    DeserializeOptions as LuaDeserializeOptions, LuaSerdeExt,
    SerializeOptions as LuaSerializeOptions,
};

#[cfg(feature = "unstable")]
#[doc(no_inline)]
pub use crate::{
    OwnedAnyUserData as LuaOwnedAnyUserData, OwnedFunction as LuaOwnedFunction,
    OwnedString as LuaOwnedString, OwnedTable as LuaOwnedTable, OwnedThread as LuaOwnedThread,
};
