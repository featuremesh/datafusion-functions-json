use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::common::{invoke, parse_jsonpath, return_type_check};
use crate::common_macros::make_udf_function;
use crate::common_union::JsonUnion;
use crate::json_get::jiter_json_get_union;

make_udf_function!(
    JsonExtractScalar,
    json_extract_scalar,
    json_data path,
    r#"Get a value from a JSON string by its "path""#
);

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct JsonExtractScalar {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for JsonExtractScalar {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: ["json_extract_scalar".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonExtractScalar {
    fn name(&self) -> &str {
        self.aliases[0].as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        return_type_check(arg_types, self.name(), JsonUnion::data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!(
                "'{}' expects exactly 2 arguments (JSON data, path), got {}",
                self.name(),
                args.args.len()
            );
        }

        let json_arg = &args.args[0];
        let path_arg = &args.args[1];

        let ColumnarValue::Scalar(
            ScalarValue::Utf8(Some(path_str))
            | ScalarValue::Utf8View(Some(path_str))
            | ScalarValue::LargeUtf8(Some(path_str)),
        ) = path_arg
        else {
            return exec_err!(
                "'{}' expects a valid JSONPath string (e.g., '$.key[0]') as second argument",
                self.name()
            );
        };

        let path = parse_jsonpath(path_str);

        invoke::<JsonUnion>(std::slice::from_ref(json_arg), |json, _| {
            jiter_json_get_union(json, &path)
        })
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}
