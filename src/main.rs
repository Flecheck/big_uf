use big_uf::*;

fn main() {
	let thread_count: u16 = 1;
	let id_count: u64 = 10_000_000;

	let (driver, threads) = Driver::ram_local_threads(thread_count);
	let mut batcher = driver.batches_sender();
	let recv = driver.receiver().clone();

	let end_time = std::thread::spawn(move || {
		let mut nodes = Vec::new();
		while (nodes.len() as u64) < id_count {
			for msg in recv.recv().unwrap() {
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
						response: key,
					} => {
						nodes.push(key);
					}
					DriverMessage::ShutdownDone { req_id: _ } => {
						panic!("There should be no Shutdown");
					}
					DriverMessage::ShutdownDriver { target_driver } => {
						panic!("There should be no other drivers")
					}
				}
			}
		}
		std::time::Instant::now()
	});

	let start_time = std::time::Instant::now();
	for id in 0..id_count {
		batcher.add_node(id, (id % thread_count as u64) as u16)
	}
	batcher.flush();
	let post_flush: std::time::Duration = start_time.elapsed();
	let elapsed = end_time.join().unwrap().duration_since(start_time);
	dbg!(post_flush, elapsed);

	batcher.shutdown_all_and_wait_for_completion();

	for t in threads {
		t.join().unwrap();
	}
}
