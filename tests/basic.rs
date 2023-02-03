use big_uf::*;

#[test]
fn basic() {
	let thread_count: u16 = 20;
	let id_count: u64 = 10000000;

	let (driver, threads) = Driver::ram_local_threads(thread_count);
	let mut batcher = driver.batches_sender();
	let recv = driver.receiver().clone();

	let results_processing = std::thread::spawn(move || {
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
				}
			}
		}
	});

	let start_time = std::time::Instant::now();
	for id in 0..id_count {
		batcher.add_node(id, (id % thread_count as u64) as u16)
	}
	batcher.flush();
	results_processing.join().unwrap();
	let elapsed = start_time.elapsed();
	dbg!(elapsed);

	batcher.shutdown_all_and_wait_for_completion();

	for t in threads {
		t.join().unwrap();
	}
}
