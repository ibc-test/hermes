use crate::prelude::*;

use flex_error::{define_error, TraceError};

use crate::core::ics02_client::error::Error as Ics02Error;

define_error! {
    #[derive(Debug, PartialEq, Eq)]
    Error {
        Crypto
            |_| { "crypto error" },

        InvalidRawClientState
            { reason: String }
            |e| { format_args!("invalid raw client state: {}", e.reason) },

        Decode
            [ TraceError<prost::DecodeError> ]
            | _ | { "decode error" },
    }
}

impl From<Error> for Ics02Error {
    fn from(e: Error) -> Self {
        Self::client_specific(e.to_string())
    }
}
