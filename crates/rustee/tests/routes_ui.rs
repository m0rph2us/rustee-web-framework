#![cfg(feature = "macros")]

#[test]
fn routes_macro_reports_stable_misuse_diagnostics() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/ui/routes/standard_methods.rs");
    cases.compile_fail("tests/ui/routes/unsupported_method.rs");
    cases.compile_fail("tests/ui/routes/no_routes.rs");
}
