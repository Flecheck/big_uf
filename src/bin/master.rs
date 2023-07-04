use std::net::{IpAddr, Ipv4Addr};

use big_uf::*;

#[tokio::main(flavor = "current_thread")]
async fn main() {
	System::connect(
		1,
		vec![
			(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 10000),
			(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 10001),
		],
	)
	.await
	.unwrap();
}
