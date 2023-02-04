pub(crate) mod message;

use crate::prelude::*;

pub struct Driver {
	message_batching: MessageBatching,
	driver_id: usize,
	receiver: crossbeam_channel::Receiver<Vec<DriverMessage>>,
}

impl Driver {
	pub(crate) fn new(
		message_batching: MessageBatching,
		driver_id: usize,
		receiver: crossbeam_channel::Receiver<Vec<DriverMessage>>,
	) -> Self {
		Driver {
			message_batching,
			driver_id,
			receiver,
		}
	}

	pub fn driver_id(&self) -> usize {
		self.driver_id
	}

	pub fn receiver(&self) -> &crossbeam_channel::Receiver<Vec<DriverMessage>> {
		&self.receiver
	}

	// You should flush if you want stuff to happen
	pub fn add_node(&mut self, req_id: u64, shard: u16) {
		self.message_batching.send_to_shard(ShardMessage::AddNode {
			shard,
			req_id: self.req_id(req_id),
		})
	}

	/// You should flush if you want stuff to happen
	pub fn union(&mut self, req_id: u64, node: Key, to: Key) {
		self.message_batching.send_to_shard(ShardMessage::Union {
			node,
			to,
			child: node,
			req_id: self.req_id(req_id),
		})
	}

	/// You should flush if you want to get a result at some point
	pub fn find(&mut self, req_id: u64, node: Key) {
		self.message_batching.send_to_shard(ShardMessage::Find {
			node,
			child: node,
			req_id: self.req_id(req_id),
		});
	}

	/// Expects that all the message queues are empty (all sent messages have already
	/// been processed), otherwise may trigger a panic
	pub fn shutdown_all_and_wait_for_completion(mut self) {
		for shard in 0..(self.system().n_shards() as u64) {
			self.message_batching
				.send_to_shard(ShardMessage::GracefulShutdown {
					shard: shard as u16,
					req_id: self.req_id(shard),
				});
		}
		self.message_batching.flush();

		let mut messages = self.receiver().into_iter().flatten();
		for _ in 0..self.system().n_shards() {
			match messages
				.next()
				.expect("Not all shards have shutdown and we lost the driver channel")
			{
				DriverMessage::ShutdownDone { .. } => {}
				_ => panic!("all other send messages should have already been processed"),
			}
		}
	}
	pub fn flush(&mut self) {
		self.message_batching.flush();
	}

	pub(crate) fn req_id(&self, req_id: u64) -> ReqId {
		ReqId::new(self.driver_id(), req_id)
	}

	pub(crate) fn system(&self) -> &System {
		&self.message_batching.system
	}
}

pub(crate) trait DriverAccess: Sync + Send {
	fn send_messages(&self, batch: Vec<DriverMessage>);
}

impl DriverAccess for crossbeam_channel::Sender<Vec<DriverMessage>> {
	fn send_messages(&self, batch: Vec<DriverMessage>) {
		self.send(batch).unwrap()
	}
}
