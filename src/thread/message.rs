use crate::prelude::*;

pub(crate) enum ThreadMessage {
	AddNode {
		thread: u16,
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
		thread: u16,
		req_id: ReqId,
	},
}

impl ThreadMessage {
	pub fn target_thread(&self) -> usize {
		match *self {
			ThreadMessage::Union { node, .. } => node.thread(),
			ThreadMessage::SetChild { node, .. } => node.thread(),
			ThreadMessage::SetSibling { node, .. } => node.thread(),
			ThreadMessage::SetParent { node, .. } => node.thread(),
			ThreadMessage::Find { node, .. } => node.thread(),
			ThreadMessage::AddNode { thread, .. } => thread as usize,
			ThreadMessage::GracefulShutdown { thread, .. } => thread as usize,
		}
	}
}
