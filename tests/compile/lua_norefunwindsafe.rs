use std::panic::catch_unwind;

use rollback_mlua::Lua;

fn main() {
    let lua = Lua::new();
    catch_unwind(|| lua.create_table().unwrap());
}
