use std::net::{IpAddr, Ipv4Addr};

use big_uf::*;

#[tokio::main()]
async fn main() {
	let (mut driver, system, _threads, _futures) = System::connect(
		1,
		vec![
			(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 10000),
			(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 10001),
		],
	)
	.await
	.unwrap();

	let driver_count = system.n_drivers();
	let shard_count = system.n_shards();
	let id_count = 10_000_000;
	let barrier = std::sync::Barrier::new(2 * (driver_count as usize) + 1);

	let start_time = rayon::scope(|s| {
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

		barrier.wait();
		std::time::Instant::now()
	});

	let elapsed = start_time.elapsed();
	dbg!(elapsed);
}
