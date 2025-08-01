//! Coordinate - compact ↔ legacy Serde helper
//!
//! * New, space-efficient representation: **comma-separated string**
//!   * `"37.7749,-122.4194"`
//!   * `"37.7749,-122.4194,12.0"`
//! * Legacy representation: **JSON map**
//!   ```json
//!   { "latitude": 37.7749, "longitude": -122.4194, "zoom": 12.0 }
//!   ```
//!
//! Deserialization seamlessly supports *both* forms; serialization always
//! produces the compact string.
//

use serde::{de, de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

use crate::schema::Coordinate;

// ---------- Display ----------

/// Human-readable format, **NOT** the same format as what is stored in the DB.
impl fmt::Display for Coordinate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Example output with zoom:
        //   (12.34567, 89.01234) @ x2.0
        // Example output without zoom:
        //   (12.34567, 89.01234)
        let zoom_str = self.zoom.map_or("".to_string(), |z| format!(" @ x{z:.1}"));
        write!(
            f,
            "({:.5}, {:.5}){}",
            self.latitude, self.longitude, zoom_str
        )
    }
}

// ---------- Serialize ----------

impl Serialize for Coordinate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Always emit the new compact string format.
        match self.zoom {
            Some(z) => format!("{},{},{}", self.latitude, self.longitude, z).serialize(serializer),
            None => format!("{},{}", self.latitude, self.longitude).serialize(serializer),
        }
    }
}

// ---------- Deserialize ----------

impl<'de> Deserialize<'de> for Coordinate {
    fn deserialize<D>(deserializer: D) -> Result<Coordinate, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(CoordinateVisitor)
    }
}

struct CoordinateVisitor;

impl<'de> de::Visitor<'de> for CoordinateVisitor {
    type Value = Coordinate;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a comma-separated coordinate string or a legacy map")
    }

    // ---- New, compact format ------------------------------------------------
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        // Split on ',' and trim whitespace in case someone prettified JSON.
        let parts: Vec<&str> = v.split(',').map(str::trim).collect();
        match parts.len() {
            2 | 3 => {
                let latitude: f64 = parts[0]
                    .parse()
                    .map_err(|e| E::custom(format!("invalid latitude: {e}")))?;
                let longitude: f64 = parts[1]
                    .parse()
                    .map_err(|e| E::custom(format!("invalid longitude: {e}")))?;
                let zoom = if parts.len() == 3 {
                    Some(
                        parts[2]
                            .parse()
                            .map_err(|e| E::custom(format!("invalid zoom: {e}")))?,
                    )
                } else {
                    None
                };

                validate_range(latitude, longitude).map_err(E::custom)?;

                Ok(Coordinate {
                    latitude,
                    longitude,
                    zoom,
                })
            }
            _ => Err(E::custom(
                "coordinate string must have 2 or 3 comma-separated parts",
            )),
        }
    }

    // ---- Legacy map format --------------------------------------------------
    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        #[derive(Deserialize)]
        struct Legacy {
            latitude: f64,
            longitude: f64,
            #[serde(default)]
            zoom: Option<f64>,
        }

        let legacy: Legacy =
            Deserialize::deserialize(serde::de::value::MapAccessDeserializer::new(map))?;

        validate_range(legacy.latitude, legacy.longitude).map_err(A::Error::custom)?;

        Ok(Coordinate {
            latitude: legacy.latitude,
            longitude: legacy.longitude,
            zoom: legacy.zoom,
        })
    }
}

// ---------- Helpers ----------

/// Best-effort validation; keeps catastrophic typos out of the DB.
fn validate_range(lat: f64, lon: f64) -> Result<(), &'static str> {
    match (lat, lon) {
        (lat, _) if !(-90.0..=90.0).contains(&lat) => Err("latitude out of range"),
        (_, lon) if !(-180.0..=180.0).contains(&lon) => Err("longitude out of range"),
        _ => Ok(()),
    }
}

// ---------- Tests -----------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq; // avoids FP equality headaches
    use serde_json;

    // --- helpers -----------------------------------------------------------

    fn assert_same(a: &Coordinate, b: &Coordinate) {
        assert_relative_eq!(a.latitude, b.latitude);
        assert_relative_eq!(a.longitude, b.longitude);
        assert_eq!(a.zoom, b.zoom);
    }

    // --- serialize ---------------------------------------------------------

    #[test]
    fn serialize_without_zoom() {
        let c = Coordinate {
            latitude: 37.0,
            longitude: 10.5,
            zoom: None,
        };
        let json = serde_json::to_string(&c).unwrap();
        assert_eq!(json, "\"37,10.5\"");
    }

    #[test]
    fn serialize_with_zoom() {
        let c = Coordinate {
            latitude: -33.123_456,
            longitude: 151.456_789,
            zoom: Some(8.0),
        };
        let json = serde_json::to_string(&c).unwrap();
        assert_eq!(json, "\"-33.123456,151.456789,8\"");
    }

    // --- deserialize – compact string -------------------------------------

    #[test]
    fn deserialize_string_without_zoom() {
        let json = "\"48.8566,2.3522\"";
        let c: Coordinate = serde_json::from_str(json).unwrap();
        assert_relative_eq!(c.latitude, 48.8566);
        assert_relative_eq!(c.longitude, 2.3522);
        assert!(c.zoom.is_none());
    }

    #[test]
    fn deserialize_string_with_zoom() {
        let json = "\"35.6895,139.6917,11.25\"";
        let c: Coordinate = serde_json::from_str(json).unwrap();
        assert_same(
            &c,
            &Coordinate {
                latitude: 35.6895,
                longitude: 139.6917,
                zoom: Some(11.25),
            },
        );
    }

    // --- deserialize – legacy map -----------------------------------------

    #[test]
    fn deserialize_legacy_map_without_zoom() {
        let json = r#"{"latitude":-23.5,"longitude":-46.6333}"#;
        let c: Coordinate = serde_json::from_str(json).unwrap();
        assert_same(
            &c,
            &Coordinate {
                latitude: -23.5,
                longitude: -46.6333,
                zoom: None,
            },
        );
    }

    #[test]
    fn deserialize_legacy_map_with_zoom() {
        let json = r#"{"latitude":51.5074,"longitude":-0.1278,"zoom":5}"#;
        let c: Coordinate = serde_json::from_str(json).unwrap();
        assert_same(
            &c,
            &Coordinate {
                latitude: 51.5074,
                longitude: -0.1278,
                zoom: Some(5.0),
            },
        );
    }

    // --- round-trip --------------------------------------------------------

    #[test]
    fn roundtrip_with_and_without_zoom() {
        let originals = [
            Coordinate {
                latitude: 0.0,
                longitude: 0.0,
                zoom: None,
            },
            Coordinate {
                latitude: 12.345678,
                longitude: -9.876543,
                zoom: Some(14.0),
            },
        ];

        for original in originals {
            let ser = serde_json::to_string(&original).unwrap();
            let de: Coordinate = serde_json::from_str(&ser).unwrap();
            assert_same(&original, &de);
        }
    }

    // --- error handling ----------------------------------------------------

    #[test]
    fn error_too_many_parts() {
        let json = "\"1,2,3,4\"";
        let err: Result<Coordinate, _> = serde_json::from_str(json);
        assert!(err.is_err());
    }

    #[test]
    fn error_invalid_number() {
        let json = "\"a,b\"";
        let err: Result<Coordinate, _> = serde_json::from_str(json);
        assert!(err.is_err());
    }

    #[test]
    fn error_out_of_range() {
        let json = "\"100,0\""; // latitude > 90
        let err: Result<Coordinate, _> = serde_json::from_str(json);
        assert!(err.is_err());
    }
}
