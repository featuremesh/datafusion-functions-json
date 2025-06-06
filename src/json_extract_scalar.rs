use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result as DataFusionResult};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion::scalar::ScalarValue;

use crate::common::parse_jsonpath;
use crate::common::{invoke, return_type_check};
use crate::common_macros::make_udf_function;
use crate::common_union::JsonUnion;
use crate::json_get::jiter_json_get_union;

make_udf_function!(
    JsonExtractScalar,
    json_extract_scalar,
    json_data path,
    r#"Get a value from a JSON string by its "path""#
);

#[derive(Debug)]
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
    fn as_any(&self) -> &dyn Any {
        self
    }

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

        let path_str = match path_arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s,
            _ => {
                return exec_err!(
                    "'{}' expects a valid JSONPath string (e.g., '$.key[0]') as second argument",
                    self.name()
                )
            }
        };

        let path = parse_jsonpath(path_str);

        invoke::<JsonUnion>(&[json_arg.clone()], |json, _| jiter_json_get_union(json, &path))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}
