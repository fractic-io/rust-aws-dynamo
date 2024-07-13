use fractic_generic_server_error::{common::CriticalError, GenericServerError};

use super::Timestamp;

impl Timestamp {
    pub fn now() -> Self {
        Self::from_utc_datetime(&chrono::Utc::now())
    }
    pub fn from_utc_datetime(dt: &chrono::DateTime<chrono::Utc>) -> Self {
        Self {
            seconds: dt.timestamp(),
            nanos: dt.timestamp_subsec_nanos(),
        }
    }
    pub fn to_utc_datetime(&self) -> Result<chrono::DateTime<chrono::Utc>, GenericServerError> {
        let dbg_cxt = "Timestamp::to_utc_datetime";
        match chrono::DateTime::from_timestamp(self.seconds, self.nanos) {
            Some(dt) => Ok(dt),
            None => Err(CriticalError::new(
                dbg_cxt,
                "failed to convert Timestamp to UTC DateTime",
            )),
        }
    }
}

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
            Err(_) => panic!("Conversion failed"),
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
}
