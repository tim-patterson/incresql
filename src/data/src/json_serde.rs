use crate::json::{Json, JsonType};
use serde::ser::{SerializeMap, SerializeSeq, SerializeStruct};
use serde::{Serialize, Serializer};

/// This serializer treats Json as a Serializable object rather than a serializer,
/// This allows us to use serde json to serialize from our Json objects to json strings.
impl Serialize for Json<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        match self.json_type() {
            JsonType::Null => serializer.serialize_none(),
            JsonType::Number => {
                // This is a little bit sneaky, we're using the internal raw value functionality of
                // serde-json to format the numbers ourselves allowing us to have numbers outside
                // the float precision offered by js
                let mut s = serializer.serialize_struct("$serde_json::private::RawValue", 1)?;
                s.serialize_field(
                    "$serde_json::private::RawValue",
                    &self.get_number().unwrap().to_string(),
                )?;
                s.end()
            }
            JsonType::Boolean => serializer.serialize_bool(self.get_boolean().unwrap()),
            JsonType::String => serializer.serialize_str(self.get_string().unwrap()),
            JsonType::Object => {
                let mut obj = serializer.serialize_map(None)?;
                for (key, value) in self.iter_object().unwrap() {
                    obj.serialize_entry(key, &value)?;
                }
                obj.end()
            }
            JsonType::Array => {
                let mut array = serializer.serialize_seq(None)?;
                for item in self.iter_array().unwrap() {
                    array.serialize_element(&item)?;
                }
                array.end()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json::JsonBuilder;
    use rust_decimal::Decimal;

    #[test]
    fn test_to_json() {
        let builder = JsonBuilder::default();
        let tape = builder.object(|object| {
            object.push_null("null_key");
            object.push_bool("bool_key", true);
            object.push_int("int_key", 1);
            object.push_string("string_key", "va\"lue");
            object.push_array("array_key", |array| {
                array.push_int(1);
                array.push_int(2);
                array.push_int(3);
            });
        });
        let json = Json::from_bytes(&tape);

        let actual = serde_json::to_string(&json).unwrap();
        let expected = r#"{"null_key":null,"bool_key":true,"int_key":1,"string_key":"va\"lue","array_key":[1,2,3]}"#;
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_high_precision_numbers() {
        let builder = JsonBuilder::default();
        let tape = builder.array(|array| {
            array.push_decimal(Decimal::from_i128_with_scale(-1234567890123456789012345, 0));
            array.push_decimal(Decimal::from_i128_with_scale(
                -1234567890123456789012345,
                10,
            ))
        });
        let json = Json::from_bytes(&tape);

        let actual = serde_json::to_string(&json).unwrap();
        let expected = r#"[-1234567890123456789012345,-123456789012345.6789012345]"#;
        assert_eq!(actual, expected);
    }
}
