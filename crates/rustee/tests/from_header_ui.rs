#![cfg(feature = "macros")]

#[test]
fn from_header_derive_reports_stable_misuse_diagnostics() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/ui/from_header/newtype.rs");
    cases.compile_fail("tests/ui/from_header/missing_header.rs");
    cases.compile_fail("tests/ui/from_header/wrong_shape.rs");
    cases.compile_fail("tests/ui/from_header/invalid_header.rs");
}
