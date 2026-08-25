use rustee::is_standard_json_media_type;

#[test]
fn facade_reexports_standard_json_media_type_admission() {
    assert!(is_standard_json_media_type("application/problem+json"));
    assert!(!is_standard_json_media_type("application/+json"));
}
