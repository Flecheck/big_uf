use serde::{Deserialize, Serialize};

use crate::prelude::*;

#[derive(Deserialize, Serialize, Debug)]
pub enum DriverMessage {
	UnionDone { req_id: ReqId },
	FindDone { req_id: ReqId, response: Key },
	AddNodeDone { req_id: ReqId, response: Key },
	ShutdownDone { req_id: ReqId },
}

impl DriverMessage {
	pub(crate) fn target_driver(&self) -> usize {
		match *self {
			DriverMessage::UnionDone { req_id } => req_id.driver(),
			DriverMessage::FindDone { req_id, .. } => req_id.driver(),
			DriverMessage::AddNodeDone { req_id, .. } => req_id.driver(),
			DriverMessage::ShutdownDone { req_id, .. } => req_id.driver(),
		}
	}
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Deserialize, Serialize)]
pub struct ReqId {
	inner: u64,
}

impl ReqId {
	pub fn new(driver: usize, shard_specific_id: u64) -> Self {
		assert!(driver <= (u16::MAX as usize) && shard_specific_id <= 0x0000FFFFFFFF);
		Self {
			inner: ((driver as u64) << 48) | shard_specific_id,
		}
	}
	pub fn driver(self) -> usize {
		(self.inner >> 48) as usize
	}
	pub fn driver_specific_id(self) -> u64 {
		self.inner & 0x0000FFFFFFFF
	}
}

impl std::fmt::Debug for ReqId {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("ReqId")
			.field("shard", &self.driver())
			.field("shard_specific_id", &self.driver_specific_id())
			.finish()
	}
}
