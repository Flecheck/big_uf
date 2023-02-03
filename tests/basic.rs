use big_uf::*;

#[test]
fn basic() {
	let (system, threads) = UnionFindSystem::ram_local_threads(4);
	for t in threads {
		t.join().unwrap();
	}
}
