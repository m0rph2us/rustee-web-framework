use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;
use serde_json::{Value, json};

use crate::model::valid_route_parameter;
use crate::{MAX_SCHEMA_BYTES, OpenApiError, OpenApiRoute, OpenApiSchema};

#[test]
fn route_and_schema_validation_are_fail_closed() {
    assert_eq!(
        OpenApiRoute::from_rustee("/todos/{todo_id}").unwrap_err(),
        OpenApiError::InvalidRoute
    );
    assert_eq!(
        OpenApiRoute::from_rustee("/todos//:todo_id").unwrap_err(),
        OpenApiError::InvalidRoute
    );
    assert_eq!(
        OpenApiSchema::from_value(Value::String("not a schema".to_owned())).unwrap_err(),
        OpenApiError::InvalidSchema
    );
    assert_eq!(
        OpenApiSchema::from_value(json!({ "description": "x".repeat(MAX_SCHEMA_BYTES) }))
            .unwrap_err(),
        OpenApiError::InvalidSchema
    );
    assert_eq!(
        OpenApiSchema::object(BTreeMap::new(), ["missing".to_owned()]).unwrap_err(),
        OpenApiError::UnknownRequiredProperty
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    #[test]
    fn route_conversion_preserves_the_accepted_rustee_template_grammar(
        route in prop::collection::vec(any::<char>(), 0..128)
            .prop_map(|characters| characters.into_iter().collect::<String>()),
    ) {
        let result = OpenApiRoute::from_rustee(&route);
        if let Ok(openapi_route) = result {
            prop_assert!(route.starts_with('/'));
            let has_forbidden_character = route.contains('?')
                || route.contains('#')
                || route.contains('{')
                || route.contains('}')
                || route.contains("//");
            prop_assert!(!has_forbidden_character);

            let mut parameter_names = BTreeSet::new();
            let mut rendered_segments = Vec::new();
            for segment in route
                .trim_matches('/')
                .split('/')
                .filter(|segment| !segment.is_empty())
            {
                if let Some(parameter) = segment.strip_prefix(':') {
                    prop_assert!(valid_route_parameter(parameter));
                    prop_assert!(parameter_names.insert(parameter.to_owned()));
                    rendered_segments.push(format!("{{{parameter}}}"));
                } else {
                    rendered_segments.push(segment.to_owned());
                }
            }
            let expected_path = if rendered_segments.is_empty() {
                "/".to_owned()
            } else {
                format!("/{}", rendered_segments.join("/"))
            };

            prop_assert_eq!(openapi_route.as_str(), expected_path);
            prop_assert_eq!(openapi_route.parameters, parameter_names);
        }
    }
}
