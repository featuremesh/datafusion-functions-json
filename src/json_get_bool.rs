use std::any::Any;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use jiter::Peek;

use crate::common::{get_err, invoke, jiter_json_find, return_type_check, GetError, JsonPath};
use crate::common_macros::make_udf_function;

make_udf_function!(
    JsonGetBool,
    json_get_bool,
    json_data path,
    r#"Get an boolean value from a JSON string by its "path""#
);

#[derive(Debug)]
pub(super) struct JsonGetBool {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for JsonGetBool {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: ["json_get_bool".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonGetBool {
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
        return_type_check(arg_types, self.name(), DataType::Boolean).map(|_| DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        invoke::<BooleanArray>(&args.args, jiter_json_get_bool)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn jiter_json_get_bool(json_data: Option<&str>, path: &[JsonPath]) -> Result<bool, GetError> {
    if let Some((mut jiter, peek)) = jiter_json_find(json_data, path) {
        match peek {
            Peek::True | Peek::False => Ok(jiter.known_bool(peek)?),
            _ => get_err!(),
        }
    } else {
        get_err!()
    }
}
