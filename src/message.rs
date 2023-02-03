use crate::prelude::*;

pub(crate) enum Message {
	Union {
		node: Key,
		to: Key,
		child: Key,
	},
	SetChild {
		node: Key,
		to: Key,
	},
	SetSibling {
		node: Key,
		to: Key,
	},
	SetParent {
		node: Key,
		to: Key,
	},
	Find {
		node: Key,
		child: Key,
		ret: futures::channel::oneshot::Sender<Key>,
	},
}

impl Message {
	pub fn target_thread(&self) -> usize {
		match self {
			Message::Union { node, .. } => node.thread(),
			Message::SetChild { node, .. } => node.thread(),
			Message::SetSibling { node, .. } => node.thread(),
			Message::SetParent { node, .. } => node.thread(),
			Message::Find { node, .. } => node.thread(),
		}
	}
}
