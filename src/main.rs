use big_uf::*;

fn main() {
	let shard_count: u16 = 30;
	let driver_count: usize = 10;
	let id_count: u64 = 10_000_000;

	rayon::ThreadPoolBuilder::new()
		.num_threads(2 * (driver_count as usize) + 1)
		.build_global()
		.unwrap();

	let (mut drivers, shards) = System::local_shards(
		|_| storage::ram::RamStorage::default,
		driver_count,
		shard_count,
	);

	let barrier = std::sync::Barrier::new(2 * (driver_count as usize) + 1);

	let start_time = rayon::scope(|s| {
		drivers.iter_mut().for_each(|driver| {
			let ids_range = (id_count * driver.driver_id() as u64 / (driver_count as u64))
				..(id_count * (driver.driver_id() as u64 + 1) / (driver_count as u64));
			let ids_range_len = ids_range.size_hint().0;
			let barrier = &barrier;

			let driver_receiver = driver.receiver().clone();
			s.spawn(move |_| {
				barrier.wait();
				for id in ids_range {
					driver.add_node(id, (id % shard_count as u64) as u16)
				}
				driver.flush();
			});
			s.spawn(move |_| {
				barrier.wait();
				let mut received_count = 0;
				while received_count < ids_range_len {
					for msg in driver_receiver.recv().unwrap() {
						match msg {
							DriverMessage::UnionDone { req_id: _ } => {
								panic!("There should be no unions")
							}
							DriverMessage::FindDone {
								req_id: _,
								response: _,
							} => {
								panic!("There should be no find")
							}
							DriverMessage::AddNodeDone {
								req_id: _,
								response: _,
							} => {
								received_count += 1;
							}
							DriverMessage::ShutdownDone { req_id: _ } => {
								panic!("There should be no Shutdown");
							}
						}
					}
				}
			});
		});

		barrier.wait();
		std::time::Instant::now()
	});

	let elapsed = start_time.elapsed();
	dbg!(elapsed);

	drivers
		.into_iter()
		.next()
		.unwrap()
		.shutdown_all_and_wait_for_completion();

	for t in shards {
		t.join().unwrap();
	}
}
