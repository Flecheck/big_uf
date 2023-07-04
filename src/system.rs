use std::{collections::HashMap, net::IpAddr, sync::Arc};

use crate::{
	network_message::{Codec, NetworkMessage},
	prelude::*,
	shard::RemoteShardAccess,
};
use anyhow::{anyhow, bail, Result};
use futures::{stream::FuturesUnordered, SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

pub struct System {
	drivers: Vec<Box<dyn DriverAccess>>,
	shards: Vec<Box<dyn ShardAccess>>,
}

impl System {
	pub fn local_shards<S: Storage, F, F2>(
		storage: F,
		n_drivers: usize,
		n_shards: u16,
	) -> (Vec<Driver>, Vec<std::thread::JoinHandle<()>>)
	where
		F: Fn(usize) -> F2,
		F2: FnOnce() -> S + Send + 'static,
	{
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
			.map(|(idx, receiver)| crate::shard::spawn(storage(idx), system.clone(), receiver, idx))
			.collect();
		(drivers, local_shards_join_handles)
	}

	pub async fn connect(
		num_shard_per_driver: u16,
		connect_to: Vec<(IpAddr, u16)>,
	) -> Result<(
		Arc<Self>,
		Vec<std::thread::JoinHandle<()>>,
		FuturesUnordered<tokio::task::JoinHandle<()>>,
	)> {
		println!("{connect_to:?}");
		let total_drivers = 1 + connect_to.len();
		let total_threads = num_shard_per_driver as usize * total_drivers;

		let mut sockets = HashMap::new();

		for ((ip, port), id) in connect_to.iter().zip(1..) {
			let stream = TcpStream::connect(format!("{ip}:{port}")).await?;
			let mut socket_framed = Framed::new(stream, Codec::default());
			let connect = if id < connect_to.len() {
				connect_to[(id).into()..].to_vec()
			} else {
				Vec::new()
			};
			println!("Sending to {id} {connect:?}");
			socket_framed
				.send(NetworkMessage::Hello {
					id: id as u16,
					num_shard: num_shard_per_driver,
					connect_to: connect,
				})
				.await?;
			sockets.insert(id, socket_framed);
		}

		let (s, r) = crossbeam_channel::unbounded();
		let (driver_access, receiver_driver) = (Box::new(s) as Box<dyn DriverAccess>, r);

		let (system_senders, receivers_system): (Vec<_>, Vec<_>) = (1..connect_to.len())
			.map(|idx| futures::channel::mpsc::unbounded())
			.unzip();

		let (local_shard_access, local_receivers_shard): (Vec<_>, Vec<_>) = (0
			..num_shard_per_driver)
			.map(|_| {
				let (s, r) = crossbeam_channel::unbounded::<Vec<ShardMessage>>();
				(Box::new(s) as Box<dyn ShardAccess>, r)
			})
			.unzip();

		let remote_shard_access = system_senders.iter().zip(1..).map(|(s, shard_idx)| {
			Box::new(RemoteShardAccess {
				shard_idx,
				system_channel: s.clone(),
			}) as Box<dyn ShardAccess>
		});

		let union_find_system = Arc::new(Self {
			drivers: vec![driver_access],
			shards: local_shard_access
				.into_iter()
				.chain(remote_shard_access)
				.collect(),
		});

		let local_threads_join_handles = local_receivers_shard
			.into_iter()
			.enumerate()
			.map(|(idx, receiver)| {
				crate::shard::spawn(
					|| crate::storage::ram::RamStorage::default(),
					union_find_system.clone(),
					receiver.clone(),
					idx,
				)
			})
			.collect();

		// let futures = threads_join_handles
		// 	.map(|(driver_id, channels)| {
		// 		let socket = sockets.remove(&driver_id).unwrap();

		// 		let receiver_driver = receiver_drivers[driver_id].clone();
		// 		let union_find_system = union_find_system.clone();
		// 		let channels = channels.to_vec();

		// 		tokio::spawn(async move {
		// 			handle_network_forwarding(
		// 				driver_id,
		// 				receiver_driver,
		// 				channels,
		// 				socket,
		// 				union_find_system,
		// 			);
		// 		})
		// 	})
		// 	.collect::<FuturesUnordered<_>>();
		let futures = todo!();
		Ok((union_find_system, local_threads_join_handles, futures))
	}

	pub async fn server(port: u16) -> Result<()> {
		let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;
		let (stream, _) = listener.accept().await?;
		let mut soket_master = Framed::new(stream, Codec::default());

		let message = soket_master
			.next()
			.await
			.ok_or_else(|| anyhow!("The stream was empty"))??;

		let (self_id, num_thread, connect_to) = if let NetworkMessage::Hello {
			id,
			num_shard: num_thread,
			connect_to,
		} = message
		{
			(id, num_thread, connect_to)
		} else {
			bail!("The first message from the master should be Hello")
		};

		let mut threads = HashMap::new();

		threads.insert(0, soket_master);

		for i in 1..self_id {
			let (stream, _) = listener.accept().await?;
			let mut stream_framed = Framed::new(stream, Codec::default());
			let message = stream_framed
				.next()
				.await
				.ok_or_else(|| anyhow!("The stream was empty"))??;

			if let NetworkMessage::Id { id } = message {
				threads.insert(id, stream_framed);
			} else {
				bail!("The first message from a peer should be Id")
			}
		}

		for ((ip, port), id) in connect_to.iter().zip(self_id + 1..) {
			let stream = TcpStream::connect(format!("{ip}:{port}")).await?;
			let mut socket_framed = Framed::new(stream, Codec::default());
			socket_framed
				.send(NetworkMessage::Id { id: self_id })
				.await?;
		}

		unimplemented!()
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

fn handle_network_forwarding(
	driver_id: usize,
	receiver_driver: futures::channel::mpsc::UnboundedReceiver<Vec<DriverMessage>>,
	socket: Framed<TcpStream, Codec>,
	union_find_system: Arc<System>,
) {
	let message_batching = MessageBatching::new(union_find_system);

	todo!()
}
