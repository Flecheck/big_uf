use std::sync::Arc;

use crate::prelude::*;

pub struct MessageBatching {
	pub(crate) system: Arc<System>,
	pub batch_len: usize,
	shard_message_batches: Vec<Vec<ShardMessage>>,
	driver_message_batches: Vec<Vec<DriverMessage>>,
}

impl MessageBatching {
	pub(crate) fn new(system: Arc<System>) -> Self {
		MessageBatching {
			shard_message_batches: (0..system.n_shards()).map(|_| Vec::new()).collect(),
			driver_message_batches: (0..system.n_drivers()).map(|_| Vec::new()).collect(),
			batch_len: 50_000,
			system,
		}
	}

	pub(crate) fn send_to_shard(&mut self, message: ShardMessage) {
		let target_shard = message.target_shard();
		let batch = &mut self.shard_message_batches[target_shard];
		batch.push(message);
		if batch.len() > self.batch_len {
			self.system
				.shard(target_shard)
				.send_messages(std::mem::replace(batch, Vec::new()));
		}
	}

	pub(crate) fn send_to_driver(&mut self, message: DriverMessage) {
		let target_driver = message.target_driver();
		let batch = &mut self.driver_message_batches[target_driver];
		batch.push(message);
		if batch.len() > self.batch_len {
			self.system
				.driver(target_driver)
				.send_messages(std::mem::replace(batch, Vec::new()));
		}
	}

	pub fn flush(&mut self) {
		for (target_shard, batch) in self.shard_message_batches.iter_mut().enumerate() {
			if !batch.is_empty() {
				self.system
					.shard(target_shard)
					.send_messages(std::mem::replace(batch, Vec::new()));
			}
		}
		for (target_driver, batch) in self.driver_message_batches.iter_mut().enumerate() {
			if !batch.is_empty() {
				self.system
					.driver(target_driver)
					.send_messages(std::mem::replace(batch, Vec::new()));
			}
		}
	}
}

impl Drop for MessageBatching {
	fn drop(&mut self) {
		self.flush()
	}
}
