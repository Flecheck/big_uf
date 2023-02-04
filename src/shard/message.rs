use crate::prelude::*;

pub(crate) enum ShardMessage {
	AddNode {
		shard: u16,
		req_id: ReqId,
	},
	Union {
		node: Key,
		to: Key,
		child: Key,
		req_id: ReqId,
	},
	SetChild {
		node: Key,
		to: Key,
		req_id: ReqId,
	},
	SetSibling {
		node: Key,
		to: Key,
		req_id: ReqId,
	},
	SetParent {
		node: Key,
		to: Key,
	},
	Find {
		node: Key,
		child: Key,
		req_id: ReqId,
	},
	GracefulShutdown {
		shard: u16,
		req_id: ReqId,
	},
}

impl ShardMessage {
	pub fn target_shard(&self) -> usize {
		match *self {
			ShardMessage::Union { node, .. } => node.shard(),
			ShardMessage::SetChild { node, .. } => node.shard(),
			ShardMessage::SetSibling { node, .. } => node.shard(),
			ShardMessage::SetParent { node, .. } => node.shard(),
			ShardMessage::Find { node, .. } => node.shard(),
			ShardMessage::AddNode { shard, .. } => shard as usize,
			ShardMessage::GracefulShutdown { shard, .. } => shard as usize,
		}
	}
}
