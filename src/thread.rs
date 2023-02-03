use std::sync::Arc;

use crate::prelude::*;

pub(crate) fn spawn<S: Storage>(
	system: Arc<UnionFindSystem>,
	receiver: crossbeam_channel::Receiver<Vec<Message>>,
	thread_idx: usize,
) -> std::thread::JoinHandle<()> {
	std::thread::spawn(move || {
		let mut thread_data = UnionFindThreadData {
			other_thread_batching: SystemMessageBatching::new(&system),
			current_thread_pending_messages: Vec::new(),
			thread_idx,
			storage: S::default(),
		};
		let mut n_processed_messages_without_flush = 0;

		loop {
			let mut maybe_flush = |thread_data: &mut UnionFindThreadData<'_, S>| {
				n_processed_messages_without_flush += 1;
				if n_processed_messages_without_flush > 10000 {
					thread_data.other_thread_batching.flush();
				}
			};
			let mut process_received_batch = |batch: Vec<Message>| {
				for msg in batch {
					thread_data.process_message(msg);
					maybe_flush(&mut thread_data);
					while let Some(msg) = thread_data.current_thread_pending_messages.pop() {
						thread_data.process_message(msg);
						maybe_flush(&mut thread_data);
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
			thread_data.other_thread_batching.flush();
			n_processed_messages_without_flush = 0;
		}
	})
}

impl UnionFindThreadAccess for crossbeam_channel::Sender<Vec<Message>> {
	fn send_messages(&self, batch: Vec<Message>) {
		self.send(batch).unwrap()
	}
}

struct UnionFindThreadData<'s, S> {
	other_thread_batching: SystemMessageBatching<'s>,
	current_thread_pending_messages: Vec<Message>,
	thread_idx: usize,
	storage: S,
}

impl<S: Storage> UnionFindThreadData<'_, S> {
	fn send(&mut self, message: Message) {
		let target_thread = message.target_thread();
		if target_thread == self.thread_idx {
			self.current_thread_pending_messages.push(message);
		} else {
			self.other_thread_batching.send(message);
		}
	}

	fn process_message(&mut self, message: Message) {
		match message {
			Message::Union { node, to, child } => {
				let parent = match self.storage.get_parent(node) {
					None => {
						self.storage.set_parent(node, to);
						self.send(Message::SetChild { node: to, to: node });
						to
					}
					Some(parent) => {
						self.send(Message::Union {
							node: parent,
							to,
							child: node,
						});
						parent
					}
				};
				// Path compression
				if child != node {
					// node is a special value for child that specifies that it's the starting point
					// of our search so nothing to compress in that case
					self.send(Message::SetParent {
						node: child,
						to: parent,
					});
				}
			}
			Message::SetChild { node, to } => {
				let prev_child = self.storage.swap_child(node, to);
				self.send(Message::SetSibling {
					node: to,
					to: prev_child,
				});
			}
			Message::SetSibling { node, to } => {
				self.storage.set_sibling(node, to);
			}
			Message::SetParent { node, to } => {
				self.storage.set_parent(node, to);
			}
			Message::Find { node, child, ret } => {
				match self.storage.get_parent(node) {
					None => {
						ret.send(node).unwrap();
					}
					Some(parent) => {
						self.send(Message::Find {
							node: parent,
							child: node,
							ret,
						});
						// Path compression
						if child != node {
							// node is a special value for child that specifies that it's the
							// starting point of our search so nothing to compress in that case
							self.send(Message::SetParent {
								node: child,
								to: parent,
							});
						}
					}
				};
			}
		}
	}
}
