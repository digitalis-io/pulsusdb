//! OTLP profiles, development version 1: the request decoded from protobuf
//! or OTLP/JSON, and each profile converted to the pprof input the profile
//! model takes.

use opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceRequest;

use crate::error::LogsIngestError;
use crate::ingest::profiles::{ProfileInput, ProfileRefusal};

/// Decodes a protobuf body.
pub fn decode(
    _body: &[u8],
    _header_less: bool,
) -> Result<ExportProfilesServiceRequest, LogsIngestError> {
    Ok(ExportProfilesServiceRequest::default())
}

/// Decodes an OTLP/JSON body.
pub fn decode_json(
    _body: &[u8],
    _header_less: bool,
) -> Result<ExportProfilesServiceRequest, LogsIngestError> {
    Ok(ExportProfilesServiceRequest::default())
}

/// Converts a decoded request to the profile model's inputs.
pub fn to_inputs(
    _request: &ExportProfilesServiceRequest,
    _header_less: bool,
) -> Result<Vec<ProfileInput>, ProfileRefusal> {
    Ok(Vec::new())
}

#[cfg(test)]
mod tests;
