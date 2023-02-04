pub(crate) mod message;

use std::sync::Arc;

use crate::prelude::*;

pub(crate) fn spawn<S: Storage>(
	system: Arc<System>,
	receiver: crossbeam_channel::Receiver<Vec<ShardMessage>>,
	shard_idx: usize,
) -> std::thread::JoinHandle<()> {
	std::thread::spawn(move || {
		let mut shard_data = UnionFindShardData {
			other_shard_batching: MessageBatching::new(system),
			current_shard_pending_messages: Vec::new(),
			shard_idx,
			storage: S::default(),
		};
		let mut n_processed_messages_without_flush = 0;

		loop {
			let mut maybe_flush = |shard_data: &mut UnionFindShardData<S>| {
				n_processed_messages_without_flush += 1;
				if n_processed_messages_without_flush > 100_000 {
					shard_data.other_shard_batching.flush();
					n_processed_messages_without_flush = 0;
				}
			};
			let mut should_stop = None;
			let mut process_received_batch = |batch: Vec<ShardMessage>| {
				for msg in batch {
					should_stop = should_stop.or(shard_data.process_message(msg));
					maybe_flush(&mut shard_data);
					while let Some(msg) = shard_data.current_shard_pending_messages.pop() {
						should_stop = should_stop.or(shard_data.process_message(msg));
						maybe_flush(&mut shard_data);
					}
				}
			};
			let batch = receiver.recv().expect("Sender disconnected");
			process_received_batch(batch);
			loop {
				match receiver.try_recv() {
					Ok(batch) => process_received_batch(batch),
					Err(crossbeam_channel::TryRecvError::Disconnected) => {
						panic!("Sender disconnected");
					}
					Err(crossbeam_channel::TryRecvError::Empty) => {
						break;
					}
				}
			}
			shard_data.other_shard_batching.flush();
			if let Some(req_id) = should_stop {
				shard_data.send_to_driver(DriverMessage::ShutdownDone { req_id });
				break;
			}
			n_processed_messages_without_flush = 0;
		}
	})
}

struct UnionFindShardData<S> {
	other_shard_batching: MessageBatching,
	current_shard_pending_messages: Vec<ShardMessage>,
	shard_idx: usize,
	storage: S,
}

impl<S: Storage> UnionFindShardData<S> {
	fn send(&mut self, message: ShardMessage) {
		let target_shard = message.target_shard();
		if target_shard == self.shard_idx {
			self.current_shard_pending_messages.push(message);
		} else {
			self.other_shard_batching.send_to_shard(message);
		}
	}

	fn send_to_driver(&mut self, message: DriverMessage) {
		self.other_shard_batching.send_to_driver(message);
	}

	fn process_message(&mut self, message: ShardMessage) -> Option<ReqId> {
		match message {
			ShardMessage::AddNode { shard, req_id } => {
				debug_assert!(self.shard_idx == shard as usize);
				let new_node = self.storage.add_node(shard as usize);
				self.send_to_driver(DriverMessage::AddNodeDone {
					req_id,
					response: new_node,
				});
			}
			ShardMessage::Union {
				node,
				to,
				child,
				req_id,
			} => {
				let parent = match self.storage.get_parent(node) {
					None => {
						self.storage.set_parent(node, to);
						self.send(ShardMessage::SetChild {
							node: to,
							to: node,
							req_id,
						});
						to
					}
					Some(parent) => {
						self.send(ShardMessage::Union {
							node: parent,
							to,
							child: node,
							req_id,
						});
						parent
					}
				};
				// Path compression
				if child != node {
					// node is a special value for child that specifies that it's the starting point
					// of our search so nothing to compress in that case
					self.send(ShardMessage::SetParent {
						node: child,
						to: parent,
					});
				}
			}
			ShardMessage::SetChild { node, to, req_id } => {
				let prev_child = self.storage.swap_child(node, to);
				self.send(ShardMessage::SetSibling {
					node: to,
					to: prev_child,
					req_id,
				});
			}
			ShardMessage::SetSibling { node, to, req_id } => {
				self.storage.set_sibling(node, to);
				self.send_to_driver(DriverMessage::UnionDone { req_id })
			}
			ShardMessage::SetParent { node, to } => {
				self.storage.set_parent(node, to);
			}
			ShardMessage::Find {
				node,
				child,
				req_id,
			} => {
				match self.storage.get_parent(node) {
					None => {
						self.send_to_driver(DriverMessage::FindDone {
							req_id,
							response: node,
						});
					}
					Some(parent) => {
						self.send(ShardMessage::Find {
							node: parent,
							child: node,
							req_id,
						});
						// Path compression
						if child != node {
							// node is a special value for child that specifies that it's the
							// starting point of our search so nothing to compress in that case
							self.send(ShardMessage::SetParent {
								node: child,
								to: parent,
							});
						}
					}
				};
			}
			ShardMessage::GracefulShutdown { shard, req_id } => {
				debug_assert!(self.shard_idx == shard as usize);
				return Some(req_id);
			}
		}
		None
	}
}

pub(crate) trait ShardAccess: Sync + Send {
	fn send_messages(&self, batch: Vec<ShardMessage>);
}

impl ShardAccess for crossbeam_channel::Sender<Vec<ShardMessage>> {
	fn send_messages(&self, batch: Vec<ShardMessage>) {
		self.send(batch).unwrap()
	}
}
