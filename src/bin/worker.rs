use big_uf::*;

#[tokio::main(flavor = "current_thread")]
async fn main() {
	let args = std::env::args()
		.nth(1)
		.expect("You should put the port as first parameter")
		.parse()
		.expect("Couldn't parse the port");
	Driver::server(args).await.unwrap();
}
