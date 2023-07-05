use std::net::IpAddr;

use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

use crate::{prelude::ShardMessage, DriverMessage};

#[derive(Deserialize, Serialize, Debug)]
pub(crate) enum NetworkMessage {
	Hello {
		id: u16,
		num_shard_per_system: u16,
		connect_to: Vec<(IpAddr, u16)>,
	},
	Id {
		id: u16,
	},
	DriverMessages {
		driver_idx: u16,
		batch: Vec<DriverMessage>,
	},
	ShardMessages {
		shard_id: u16,
		batch: Vec<ShardMessage>,
	},
}

#[derive(Default)]
pub(crate) struct Codec {}

impl Decoder for Codec {
	type Item = NetworkMessage;

	type Error = anyhow::Error;

	fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
		if src.len() < 4 {
			// Not enough data to read length marker.
			return Ok(None);
		}

		// Read length marker.
		let mut length_bytes = [0u8; 4];
		length_bytes.copy_from_slice(&src[..4]);
		let length = u32::from_le_bytes(length_bytes) as usize;

		if src.len() < 4 + length {
			// The full string has not yet arrived.
			//
			// We reserve more space in the buffer. This is not strictly
			// necessary, but is a good idea performance-wise.
			src.reserve(4 + length - src.len());

			// We inform the Framed that we need more bytes to form the next
			// frame.
			return Ok(None);
		}

		let data = &src[4..4 + length];

		let data = bincode::deserialize(data);

		// Use advance to modify src such that it no longer contains
		// this frame.
		src.advance(4 + length);

		Ok(Some(data?))
	}
}

impl Encoder<NetworkMessage> for Codec {
	type Error = anyhow::Error;

	fn encode(&mut self, item: NetworkMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
		let len = bincode::serialized_size(&item)?;
		let len_slice = u32::to_le_bytes(len as u32);
		dst.reserve(4 + len as usize);

		dst.extend_from_slice(&len_slice);
		bincode::serialize_into(dst.writer(), &item)?;
		Ok(())
	}
}
