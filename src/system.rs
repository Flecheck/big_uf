use std::sync::Arc;

use crate::prelude::*;

pub struct UnionFindSystem {
	threads: Vec<Box<dyn UnionFindThreadAccess>>,
}

impl UnionFindSystem {
	pub fn ram_local_threads(n: u16) -> (Arc<Self>, Vec<std::thread::JoinHandle<()>>) {
		let (storages, receivers): (Vec<_>, Vec<_>) = (0..n)
			.map(|_| {
				let (s, r) = crossbeam_channel::unbounded::<Vec<Message>>();
				(Box::new(s) as Box<dyn UnionFindThreadAccess>, r)
			})
			.unzip();
		let union_find_system = Arc::new(Self { threads: storages });
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

	pub(crate) fn thread(&self, n: usize) -> &dyn UnionFindThreadAccess {
		&*self.threads[n]
	}

	pub fn batches_sender(&self) -> SystemMessageBatching {
		SystemMessageBatching::new(self)
	}

	pub fn n_threads(&self) -> usize {
		self.threads.len()
	}
}

pub(crate) trait UnionFindThreadAccess: Sync + Send {
	fn send_messages(&self, batch: Vec<Message>);
}
