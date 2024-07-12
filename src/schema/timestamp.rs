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
