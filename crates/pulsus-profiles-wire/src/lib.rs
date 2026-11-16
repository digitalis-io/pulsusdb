//! The profiles wire messages, written by hand as `prost` structs so no
//! code generator runs at build time. `google_v1` is the pprof profile
//! message (`proto/google/v1/profile.proto`, kept verbatim as the record the
//! field numbers are checked against by `tests/tags.rs`). `types_v1` and
//! `querier_v1` are the query API's messages (issue #821,
//! `proto/types/v1/types.proto` and `proto/querier/v1/querier.proto`), with
//! the proto3 JSON mapping as `serde` attributes; `push_v1` is the push
//! route's (`proto/push/v1/push.proto`, kept verbatim for the same reason).

pub mod connect;
pub mod google_v1;
pub mod int64;
pub mod push_v1;
pub mod querier_v1;
pub mod types_v1;

/// Stub: encodes nothing.
#[macro_export]
#[doc(hidden)]
macro_rules! stub_message {
    ($t:ty) => {
        impl ::prost::Message for $t {
            fn encode_raw(&self, _buf: &mut impl ::prost::bytes::BufMut) {}
            fn merge_field(
                &mut self,
                tag: u32,
                wire_type: ::prost::encoding::WireType,
                buf: &mut impl ::prost::bytes::Buf,
                ctx: ::prost::encoding::DecodeContext,
            ) -> Result<(), ::prost::DecodeError> {
                ::prost::encoding::skip_field(wire_type, tag, buf, ctx)
            }
            fn encoded_len(&self) -> usize {
                0
            }
            fn clear(&mut self) {}
        }
    };
}
