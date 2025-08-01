use fractic_server_error::{CriticalError, ServerError};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::Timestamp;

// ---------- Convenience ----------

impl Timestamp {
    pub fn now() -> Self {
        Self::from_utc_datetime(&chrono::Utc::now())
    }
    pub fn from_seconds(seconds: i64) -> Self {
        Self { seconds, nanos: 0 }
    }
    pub fn from_utc_datetime(dt: &chrono::DateTime<chrono::Utc>) -> Self {
        Self {
            seconds: dt.timestamp(),
            nanos: dt.timestamp_subsec_nanos(),
        }
    }
    pub fn to_utc_datetime(&self) -> Result<chrono::DateTime<chrono::Utc>, ServerError> {
        match chrono::DateTime::from_timestamp(self.seconds, self.nanos) {
            Some(dt) => Ok(dt),
            None => Err(CriticalError::new(
                "failed to convert Timestamp to UTC DateTime",
            )),
        }
    }
    // Print as "YYYY-MM-DDTHH:MM:SSZ".
    pub fn to_iso_8601_string(&self) -> Result<String, ServerError> {
        Ok(self
            .to_utc_datetime()?
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
    }
}

// ---------- Display ----------

/// Human-readable format, **NOT** the same format as what is stored in the DB.
impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Example output:
        //   2021-08-26 17:46:40 UTC
        write!(
            f,
            "{}",
            self.to_utc_datetime()
                .map_err(|_| std::fmt::Error)?
                .format("%Y-%m-%d %H:%M:%S %Z")
        )
    }
}

// ---------- Sorting ----------

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.seconds.cmp(&other.seconds) {
            std::cmp::Ordering::Equal => self.nanos.cmp(&other.nanos),
            ord => ord,
        }
    }
}

// ---------- Serialize ----------

impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        format!("{:011}.{:09}", self.seconds, self.nanos).serialize(serializer)
    }
}

// ---------- Deserialize ----------

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Timestamp, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(TimestampVisitor)
    }
}

// The main serialization format is "seconds.nanos" as a string. However, it was
// previously stored directly as the map. For now, support either form of
// deserialization.
struct TimestampVisitor;
impl<'de> serde::de::Visitor<'de> for TimestampVisitor {
    type Value = Timestamp;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a timestamp as a string or a map")
    }

    // ---- compact format -----------------------------------------------------
    fn visit_str<E>(self, value: &str) -> Result<Timestamp, E>
    where
        E: serde::de::Error,
    {
        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() != 2 {
            return Err(serde::de::Error::custom("invalid timestamp format"));
        }
        let seconds = parts[0].parse().map_err(serde::de::Error::custom)?;
        let nanos = parts[1].parse().map_err(serde::de::Error::custom)?;
        Ok(Timestamp { seconds, nanos })
    }

    // ---- legacy map format --------------------------------------------------
    fn visit_map<A>(self, map: A) -> Result<Timestamp, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        #[derive(Deserialize)]
        struct LegacyMap {
            seconds: i64,
            nanos: u32,
        }
        let legacy_map: LegacyMap =
            Deserialize::deserialize(serde::de::value::MapAccessDeserializer::new(map))?;
        Ok(Timestamp {
            seconds: legacy_map.seconds,
            nanos: legacy_map.nanos,
        })
    }
}

// ---------- Tests -----------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_now() {
        let timestamp = Timestamp::now();
        let now = Utc::now();

        // Check if the timestamp is close enough to now.
        assert!(timestamp.seconds <= now.timestamp());
        assert!(timestamp.nanos <= now.timestamp_subsec_nanos());
    }

    #[test]
    fn test_from_utc_datetime() {
        let dt = Utc::now();
        let timestamp = Timestamp::from_utc_datetime(&dt);

        assert_eq!(timestamp.seconds, dt.timestamp());
        assert_eq!(timestamp.nanos, dt.timestamp_subsec_nanos());
    }

    #[test]
    fn test_to_utc_datetime() {
        let dt = Utc::now();
        let timestamp = Timestamp::from_utc_datetime(&dt);
        let result = timestamp.to_utc_datetime();

        match result {
            Ok(converted_dt) => {
                assert_eq!(converted_dt.timestamp(), dt.timestamp());
                assert_eq!(
                    converted_dt.timestamp_subsec_nanos(),
                    dt.timestamp_subsec_nanos()
                );
            }
            Err(_) => panic!("Conversion failed."),
        }
    }

    #[test]
    fn test_to_utc_datetime_error() {
        let timestamp = Timestamp {
            seconds: 0,
            nanos: 2_000_000_000, // Invalid nanoseconds value
        };
        let result = timestamp.to_utc_datetime();

        assert!(result.is_err());
    }

    #[test]
    fn test_to_iso_8601_string() {
        let timestamp = Timestamp {
            seconds: 1630000000,
            nanos: 123456789,
        };
        let result = timestamp.to_iso_8601_string();

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "2021-08-26T17:46:40Z".to_string());
    }

    #[test]
    fn test_serialize_zero_padding() {
        let timestamp = Timestamp {
            seconds: 1, // Should be zero-padded.
            nanos: 123, // Should be zero-padded.
        };

        let serialized = serde_json::to_string(&timestamp).unwrap();
        assert_eq!(serialized, "\"00000000001.000000123\"");
    }

    #[test]
    fn test_serialize_full_padding() {
        let timestamp = Timestamp {
            seconds: 21630000000, // No padding needed.
            nanos: 123456789,     // No padding needed.
        };

        let serialized = serde_json::to_string(&timestamp).unwrap();
        assert_eq!(serialized, "\"21630000000.123456789\"");
    }

    #[test]
    fn test_deserialize_string_format() {
        let json = "\"01630000000.123456789\"";
        let timestamp: Timestamp = serde_json::from_str(json).unwrap();

        assert_eq!(timestamp.seconds, 1630000000);
        assert_eq!(timestamp.nanos, 123456789);
    }

    #[test]
    fn test_deserialize_legacy_map_format() {
        let json = "{\"seconds\": 1630000000, \"nanos\": 123456789}";
        let timestamp: Timestamp = serde_json::from_str(json).unwrap();

        assert_eq!(timestamp.seconds, 1630000000);
        assert_eq!(timestamp.nanos, 123456789);
    }

    #[test]
    fn test_deserialize_invalid_string_format() {
        let json = "\"invalid.timestamp\"";
        let result: Result<Timestamp, _> = serde_json::from_str(json);

        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_invalid_map_format() {
        let json = "{\"invalid_field\": 123}";
        let result: Result<Timestamp, _> = serde_json::from_str(json);

        assert!(result.is_err());
    }

    #[test]
    fn test_display_with_zero_padded_seconds_and_nanos() {
        let timestamp = Timestamp {
            seconds: 1,
            nanos: 123,
        };

        assert_eq!(format!("{}", timestamp), "1970-01-01 00:00:01 UTC");
    }

    #[test]
    fn test_display_full_precision() {
        let timestamp = Timestamp {
            seconds: 1630000000,
            nanos: 123456789,
        };

        assert_eq!(format!("{}", timestamp), "2021-08-26 17:46:40 UTC");
    }

    #[test]
    fn test_serialize_and_deserialize_string_format() {
        let original = Timestamp {
            seconds: 1630000000,
            nanos: 123456789,
        };

        // Serialize to string format.
        let serialized = serde_json::to_string(&original).unwrap();

        // Deserialize back to Timestamp.
        let deserialized: Timestamp = serde_json::from_str(&serialized).unwrap();

        assert_eq!(original.seconds, deserialized.seconds);
        assert_eq!(original.nanos, deserialized.nanos);
    }

    #[test]
    fn test_serialize_and_deserialize_legacy_map_format() {
        let original = Timestamp {
            seconds: 1630000000,
            nanos: 123456789,
        };

        // Serialize to legacy map format.
        let serialized = serde_json::json!({
            "seconds": original.seconds,
            "nanos": original.nanos
        })
        .to_string();

        // Deserialize back to Timestamp.
        let deserialized: Timestamp = serde_json::from_str(&serialized).unwrap();

        assert_eq!(original.seconds, deserialized.seconds);
        assert_eq!(original.nanos, deserialized.nanos);
    }
}
