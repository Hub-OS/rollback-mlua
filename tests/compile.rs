#[test]
#[ignore]
fn test_compilation() {
    let t = trybuild::TestCases::new();

    t.compile_fail("tests/compile/function_borrow.rs");
    t.compile_fail("tests/compile/lua_norefunwindsafe.rs");
    t.compile_fail("tests/compile/ref_nounwindsafe.rs");
    t.compile_fail("tests/compile/scope_callback_capture.rs");
    t.compile_fail("tests/compile/scope_callback_inner.rs");
    t.compile_fail("tests/compile/scope_callback_outer.rs");
    t.compile_fail("tests/compile/scope_invariance.rs");
    t.compile_fail("tests/compile/scope_mutable_aliasing.rs");
    t.compile_fail("tests/compile/static_callback_args.rs");

    #[cfg(not(feature = "send"))]
    t.pass("tests/compile/non_send.rs");
}
