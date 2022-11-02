#[macro_use]
mod macros;

mod chunk;
mod conversion;
mod error;
mod ffi;
mod function;
mod lua;
mod lua_ref;
mod memory;
mod multi;
mod photographic_memory;
mod registry;
mod scope;
mod stdlib;
mod string;
mod table;
mod types;
mod util;
mod value;

pub mod prelude;

pub use chunk::*;
pub use conversion::*;
pub use error::*;
pub use function::*;
pub use lua::{GCMode, Lua};
pub use multi::*;
pub use registry::*;
pub use scope::*;
pub use stdlib::*;
pub use string::*;
pub use table::*;
pub use types::*;
pub use value::*;

#[cfg(feature = "serialize")]
#[doc(inline)]
pub use crate::serde::{
    de::Options as DeserializeOptions, ser::Options as SerializeOptions, LuaSerdeExt,
};

#[cfg(feature = "serialize")]
#[cfg_attr(docsrs, doc(cfg(feature = "serialize")))]
pub mod serde;
