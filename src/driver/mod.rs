pub(crate) mod message;

use std::sync::Arc;

use crate::prelude::*;

pub struct Driver {
	driver_id: usize,
	drivers: Vec<Box<dyn DriverAccess>>,
	threads: Vec<Box<dyn ThreadAccess>>,
	receiver: crossbeam_channel::Receiver<Vec<DriverMessage>>,
}

impl Driver {
	pub fn ram_local_threads(n: u16) -> (Arc<Self>, Vec<std::thread::JoinHandle<()>>) {
		let (system_sender, system_receiver) = crossbeam_channel::unbounded();
		let (threads, receivers): (Vec<_>, Vec<_>) = (0..n)
			.map(|_| {
				let (s, r) = crossbeam_channel::unbounded::<Vec<ThreadMessage>>();
				(Box::new(s) as Box<dyn ThreadAccess>, r)
			})
			.unzip();
		let union_find_system = Arc::new(Self {
			driver_id: 0,
			drivers: vec![Box::new(system_sender) as Box<dyn DriverAccess>],
			threads,
			receiver: system_receiver,
		});
		let local_threads_join_handles = receivers
			.into_iter()
			.enumerate()
			.map(|(idx, receiver)| {
				crate::thread::spawn::<crate::storage::ram::RamStorage>(
					union_find_system.clone(),
					receiver,
					idx,
				)
			})
			.collect();
		(union_find_system, local_threads_join_handles)
	}

	pub(crate) fn thread(&self, thread_id: usize) -> &dyn ThreadAccess {
		&*self.threads[thread_id]
	}

	pub(crate) fn driver(&self, driver_id: usize) -> &dyn DriverAccess {
		&*self.drivers[driver_id]
	}

	pub fn batches_sender(&self) -> MessageBatching {
		MessageBatching::new(self)
	}

	pub fn n_threads(&self) -> usize {
		self.threads.len()
	}

	pub fn n_drivers(&self) -> usize {
		self.drivers.len()
	}

	pub fn driver_id(&self) -> usize {
		self.driver_id
	}

	pub fn receiver(&self) -> &crossbeam_channel::Receiver<Vec<DriverMessage>> {
		&self.receiver
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
