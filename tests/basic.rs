use big_uf::*;

#[test]
fn basic() {
	let thread_count = 20;
	let id_count = 1000000;

	let (driver, threads) = Driver::ram_local_threads(thread_count);
	let mut batcher = driver.batches_sender();
	let recv = driver.receiver();

	let start_time = std::time::Instant::now();
	for thread in 0..thread_count {
		for id in 0..id_count {
			batcher.add_node(id, thread)
		}
	}
	batcher.flush();
	let mut nodes = Vec::new();
	while nodes.len() < (thread_count as u64 * id_count) as usize {
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
	let elapsed = start_time.elapsed();
	dbg!(elapsed);

	batcher.shutdown_all_and_wait_for_completion();

	for t in threads {
		t.join().unwrap();
	}
}
