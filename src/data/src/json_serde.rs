use crate::json::{Json, JsonBuilder, JsonBuilderInner, JsonType, OwnedJson};
use rust_decimal::prelude::{FromPrimitive, FromStr};
use rust_decimal::Decimal;
use serde::de::{DeserializeSeed, Error, MapAccess, SeqAccess, Visitor};
use serde::export::Formatter;
use serde::ser::{SerializeMap, SerializeSeq, SerializeStruct};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// This serializer treats Json as a Serializable object rather than a serializer,
/// This allows us to use serde json to serialize from our Json objects to json strings
/// (or any other supported serde format)
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
                let mut s = serializer.serialize_struct("$serde_json::private::Number", 1)?;
                s.serialize_field(
                    "$serde_json::private::Number",
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

/// This deserialize treats Json(OwnedJson) as a Serializable object rather than a serializer,
/// This allows us to use serde json to deserialize to our Json objects from json strings
/// (or any other supported serde format, ie csv).
/// Json Serde seems to be the only json parser around at the moment that supports properly parsing
/// in numbers as decimals rather than going to floats.
/// Once this lands https://github.com/simdjson/simdjson/issues/489
/// and https://github.com/simdjson/simdjson/issues/923 this land then switching to simdjson is
/// probably the way to go given how common parsing json is.
impl<'de> Deserialize<'de> for OwnedJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let mut json_builder = JsonBuilder::default();
        deserializer.deserialize_any(&mut json_builder.inner)?;
        Ok(json_builder.inner.build())
    }
}

impl<'de> Visitor<'de> for &mut JsonBuilderInner {
    type Value = ();

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("Anything")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: Error,
    {
        self.push_bool(v);
        Ok(())
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        self.push_int(v);
        Ok(())
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        self.push_decimal(Decimal::from_i128_with_scale(v as i128, 0));
        Ok(())
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        self.push_decimal(Decimal::from_f64(v).unwrap());
        Ok(())
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        self.push_string(v);
        Ok(())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: Error,
    {
        self.push_null();
        Ok(())
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: Error,
    {
        self.push_null();
        Ok(())
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        self.push_array(|array| {
            while seq
                .next_element_seed::<&mut JsonBuilderInner>(&mut array.inner)
                .unwrap()
                .is_some()
            {}
        });
        Ok(())
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut key = if let Some(str) = map.next_key::<&str>()? {
            if str == "$serde_json::private::Number" {
                let value = map.next_value::<String>()?;
                self.push_decimal(Decimal::from_str(&value).unwrap());
                return Ok(());
            }
            Some(str)
        } else {
            None
        };

        self.push_object(move |object| {
            while let Some(str) = key {
                object.inner.push_string(&str);
                map.next_value_seed::<&mut JsonBuilderInner>(&mut object.inner)
                    .unwrap();
                key = map.next_key::<&str>().unwrap();
            }
        });
        Ok(())
    }
}

impl<'de> DeserializeSeed<'de> for &mut JsonBuilderInner {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_json() {
        let builder = JsonBuilder::default();
        let owned_json = builder.object(|object| {
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

        let json = owned_json.as_json();

        let actual = serde_json::to_string(&json).unwrap();
        let expected = r#"{"null_key":null,"bool_key":true,"int_key":1,"string_key":"va\"lue","array_key":[1,2,3]}"#;
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_round_trip() {
        let input = r#"{"null_key":null,"bool_key":true,"int_key":-1234567890123456789012345,"string_key":"va\"lue","array_key":[1,2,3]}"#;

        let owned_json: OwnedJson = serde_json::from_str(input).unwrap();
        let json = owned_json.as_json();

        let actual = serde_json::to_string(&json).unwrap();
        assert_eq!(actual, input);
    }

    #[test]
    fn test_high_precision_numbers() {
        let builder = JsonBuilder::default();
        let owned_json = builder.array(|array| {
            array.push_decimal(Decimal::from_i128_with_scale(-1234567890123456789012345, 0));
            array.push_decimal(Decimal::from_i128_with_scale(
                -1234567890123456789012345,
                10,
            ))
        });

        let json = owned_json.as_json();

        let actual = serde_json::to_string(&json).unwrap();
        let expected = r#"[-1234567890123456789012345,-123456789012345.6789012345]"#;
        assert_eq!(actual, expected);
    }
}
