use crate::prelude::*;

pub struct SystemMessageBatching<'s> {
	system: &'s UnionFindSystem,
	batches: Vec<Vec<Message>>,
}

impl<'s> SystemMessageBatching<'s> {
	/// You should flush if you want stuff to happen
	pub fn union(&mut self, node: Key, to: Key) {
		self.send(Message::Union {
			node,
			to,
			child: node,
		})
	}

	/// You should flush if you want to get a result at some point
	pub fn find(&mut self, node: Key) -> impl futures::Future<Output = Key> {
		let (s, r) = futures::channel::oneshot::channel();
		self.send(Message::Find {
			node,
			child: node,
			ret: s,
		});
		async {
			r.await
				.expect("Find ret was dropped without sending response")
		}
	}

	pub(crate) fn new(system: &'s UnionFindSystem) -> Self {
		SystemMessageBatching {
			system: &system,
			batches: (0..system.n_threads()).map(|_| Vec::new()).collect(),
		}
	}

	pub(crate) fn send(&mut self, message: Message) {
		let target_thread = message.target_thread();
		let batch = &mut self.batches[target_thread];
		batch.push(message);
		if batch.len() > 500 {
			self.system
				.thread(target_thread)
				.send_messages(std::mem::replace(batch, Vec::new()));
		}
	}

	pub fn flush(&mut self) {
		for (target_thread, batch) in self.batches.iter_mut().enumerate() {
			if batch.len() > 0 {
				self.system
					.thread(target_thread)
					.send_messages(std::mem::replace(batch, Vec::new()));
			}
		}
	}
}

impl Drop for SystemMessageBatching<'_> {
	fn drop(&mut self) {
		self.flush()
	}
}
