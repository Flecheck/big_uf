use crate::prelude::*;

pub struct MessageBatching<'s> {
	driver: &'s Driver,
	pub batch_len: usize,
	thread_message_batches: Vec<Vec<ThreadMessage>>,
	driver_message_batches: Vec<Vec<DriverMessage>>,
}

impl<'s> MessageBatching<'s> {
	/// You should flush if you want stuff to happen
	pub fn add_node(&mut self, req_id: u64, thread: u16) {
		self.send_to_thread(ThreadMessage::AddNode {
			thread,
			req_id: self.req_id(req_id),
		})
	}

	/// You should flush if you want stuff to happen
	pub fn union(&mut self, req_id: u64, node: Key, to: Key) {
		self.send_to_thread(ThreadMessage::Union {
			node,
			to,
			child: node,
			req_id: self.req_id(req_id),
		})
	}

	/// You should flush if you want to get a result at some point
	pub fn find(&mut self, req_id: u64, node: Key) {
		self.send_to_thread(ThreadMessage::Find {
			node,
			child: node,
			req_id: self.req_id(req_id),
		});
	}

	/// Expects that all the message queues are empty (all sent messages have already
	/// been processed), otherwise may trigger a panic
	pub fn shutdown_all_and_wait_for_completion(mut self) {
		for thread in 0..(self.driver.n_threads() as u64) {
			self.send_to_thread(ThreadMessage::GracefulShutdown {
				thread: thread as u16,
				req_id: self.req_id(thread),
			});
		}
		for id in 0..self.driver.n_drivers() {
			if id != self.driver.driver_id() {
				self.send_to_driver(DriverMessage::ShutdownDriver {
					target_driver: id as u16,
				});
			}
		}
		self.flush();

		let mut messages = self.driver.receiver().into_iter().flatten();
		for _ in 0..self.driver.n_threads() {
			match messages
				.next()
				.expect("Not all threads have shutdown and we lost the driver channel")
			{
				DriverMessage::ShutdownDone { .. } => {}
				_ => panic!("all other send messages should have already been processed"),
			}
		}
	}

	pub(crate) fn new(driver: &'s Driver) -> Self {
		MessageBatching {
			driver: &driver,
			thread_message_batches: (0..driver.n_threads()).map(|_| Vec::new()).collect(),
			driver_message_batches: (0..driver.n_drivers()).map(|_| Vec::new()).collect(),
			batch_len: 1_000_000,
		}
	}

	pub(crate) fn req_id(&self, req_id: u64) -> ReqId {
		ReqId::new(self.driver.driver_id(), req_id)
	}

	pub(crate) fn send_to_thread(&mut self, message: ThreadMessage) {
		let target_thread = message.target_thread();
		let batch = &mut self.thread_message_batches[target_thread];
		batch.push(message);
		if batch.len() > self.batch_len {
			self.driver
				.thread(target_thread)
				.send_messages(std::mem::replace(batch, Vec::new()));
		}
	}

	pub(crate) fn send_to_driver(&mut self, message: DriverMessage) {
		let target_driver = message.target_driver();
		let batch = &mut self.driver_message_batches[target_driver];
		batch.push(message);
		if batch.len() > self.batch_len {
			self.driver
				.driver(target_driver)
				.send_messages(std::mem::replace(batch, Vec::new()));
		}
	}

	pub fn flush(&mut self) {
		for (target_thread, batch) in self.thread_message_batches.iter_mut().enumerate() {
			if !batch.is_empty() {
				self.driver
					.thread(target_thread)
					.send_messages(std::mem::replace(batch, Vec::new()));
			}
		}
		for (target_driver, batch) in self.driver_message_batches.iter_mut().enumerate() {
			if !batch.is_empty() {
				self.driver
					.driver(target_driver)
					.send_messages(std::mem::replace(batch, Vec::new()));
			}
		}
	}
}

impl Drop for MessageBatching<'_> {
	fn drop(&mut self) {
		self.flush()
	}
}
