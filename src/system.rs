use std::sync::Arc;

use crate::prelude::*;

pub struct System {
	drivers: Vec<Box<dyn DriverAccess>>,
	shards: Vec<Box<dyn ShardAccess>>,
}

impl System {
	pub fn ram_local_shards(
		n_drivers: usize,
		n_shards: u16,
	) -> (Vec<Driver>, Vec<std::thread::JoinHandle<()>>) {
		let (drivers_accesses, drivers_receivers): (Vec<_>, Vec<_>) = (0..n_drivers)
			.map(|_| {
				let (s, r) = crossbeam_channel::unbounded::<Vec<DriverMessage>>();
				(Box::new(s) as Box<dyn DriverAccess>, r)
			})
			.unzip();

		let (shards_accesses, shards_receivers): (Vec<_>, Vec<_>) = (0..n_shards)
			.map(|_| {
				let (s, r) = crossbeam_channel::unbounded::<Vec<ShardMessage>>();
				(Box::new(s) as Box<dyn ShardAccess>, r)
			})
			.unzip();

		let system = Arc::new(Self {
			drivers: drivers_accesses,
			shards: shards_accesses,
		});

		let drivers = drivers_receivers
			.into_iter()
			.enumerate()
			.map(|(driver_id, receiver)| {
				Driver::new(MessageBatching::new(system.clone()), driver_id, receiver)
			})
			.collect();

		let local_shards_join_handles = shards_receivers
			.into_iter()
			.enumerate()
			.map(|(idx, receiver)| {
				crate::shard::spawn::<crate::storage::ram::RamStorage>(
					system.clone(),
					receiver,
					idx,
				)
			})
			.collect();
		(drivers, local_shards_join_handles)
	}

	pub(crate) fn shard(&self, shard_id: usize) -> &dyn ShardAccess {
		&*self.shards[shard_id]
	}

	pub(crate) fn driver(&self, driver_id: usize) -> &dyn DriverAccess {
		&*self.drivers[driver_id]
	}

	pub fn n_shards(&self) -> usize {
		self.shards.len()
	}

	pub fn n_drivers(&self) -> usize {
		self.drivers.len()
	}
}
