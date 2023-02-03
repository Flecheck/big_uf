pub(crate) mod message;

use std::sync::Arc;

use crate::prelude::*;

pub(crate) fn spawn<S: Storage>(
	system: Arc<Driver>,
	receiver: crossbeam_channel::Receiver<Vec<ThreadMessage>>,
	thread_idx: usize,
) -> std::thread::JoinHandle<()> {
	std::thread::spawn(move || {
		let mut thread_data = UnionFindThreadData {
			other_thread_batching: MessageBatching::new(&system),
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
			let mut should_stop = None;
			let mut process_received_batch = |batch: Vec<ThreadMessage>| {
				for msg in batch {
					should_stop = should_stop.or(thread_data.process_message(msg));
					maybe_flush(&mut thread_data);
					while let Some(msg) = thread_data.current_thread_pending_messages.pop() {
						should_stop = should_stop.or(thread_data.process_message(msg));
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
			if let Some(req_id) = should_stop {
				thread_data.send_to_driver(DriverMessage::ShutdownDone { req_id });
				break;
			}
			n_processed_messages_without_flush = 0;
		}
	})
}

struct UnionFindThreadData<'s, S> {
	other_thread_batching: MessageBatching<'s>,
	current_thread_pending_messages: Vec<ThreadMessage>,
	thread_idx: usize,
	storage: S,
}

impl<S: Storage> UnionFindThreadData<'_, S> {
	fn send(&mut self, message: ThreadMessage) {
		let target_thread = message.target_thread();
		if target_thread == self.thread_idx {
			self.current_thread_pending_messages.push(message);
		} else {
			self.other_thread_batching.send_to_thread(message);
		}
	}

	fn send_to_driver(&mut self, message: DriverMessage) {
		self.other_thread_batching.send_to_driver(message);
	}

	fn process_message(&mut self, message: ThreadMessage) -> Option<ReqId> {
		match message {
			ThreadMessage::AddNode { thread, req_id } => {
				debug_assert!(self.thread_idx == thread as usize);
				let new_node = self.storage.add_node(thread as usize);
				self.send_to_driver(DriverMessage::AddNodeDone {
					req_id,
					response: new_node,
				});
			}
			ThreadMessage::Union {
				node,
				to,
				child,
				req_id,
			} => {
				let parent = match self.storage.get_parent(node) {
					None => {
						self.storage.set_parent(node, to);
						self.send(ThreadMessage::SetChild {
							node: to,
							to: node,
							req_id,
						});
						to
					}
					Some(parent) => {
						self.send(ThreadMessage::Union {
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
					self.send(ThreadMessage::SetParent {
						node: child,
						to: parent,
					});
				}
			}
			ThreadMessage::SetChild { node, to, req_id } => {
				let prev_child = self.storage.swap_child(node, to);
				self.send(ThreadMessage::SetSibling {
					node: to,
					to: prev_child,
					req_id,
				});
			}
			ThreadMessage::SetSibling { node, to, req_id } => {
				self.storage.set_sibling(node, to);
				self.send_to_driver(DriverMessage::UnionDone { req_id })
			}
			ThreadMessage::SetParent { node, to } => {
				self.storage.set_parent(node, to);
			}
			ThreadMessage::Find {
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
						self.send(ThreadMessage::Find {
							node: parent,
							child: node,
							req_id,
						});
						// Path compression
						if child != node {
							// node is a special value for child that specifies that it's the
							// starting point of our search so nothing to compress in that case
							self.send(ThreadMessage::SetParent {
								node: child,
								to: parent,
							});
						}
					}
				};
			}
			ThreadMessage::GracefulShutdown { thread, req_id } => {
				debug_assert!(self.thread_idx == thread as usize);
				return Some(req_id);
			}
		}
		None
	}
}

pub(crate) trait ThreadAccess: Sync + Send {
	fn send_messages(&self, batch: Vec<ThreadMessage>);
}

impl ThreadAccess for crossbeam_channel::Sender<Vec<ThreadMessage>> {
	fn send_messages(&self, batch: Vec<ThreadMessage>) {
		self.send(batch).unwrap()
	}
}
