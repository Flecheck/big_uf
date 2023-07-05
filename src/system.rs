use std::{collections::HashMap, net::IpAddr, sync::Arc};

use crate::{
	driver::RemoteDriverAccess,
	network_message::{Codec, NetworkMessage},
	prelude::*,
	shard::RemoteShardAccess,
};
use anyhow::{anyhow, bail, Result};
use futures::{stream::FuturesUnordered, Future, FutureExt, SinkExt, StreamExt, TryStreamExt};
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
			.map(|(id, receiver)| crate::shard::spawn(storage(id), system.clone(), receiver, id))
			.collect();
		(drivers, local_shards_join_handles)
	}

	pub async fn connect(
		num_shard_per_system: u16,
		connect_to: Vec<(IpAddr, u16)>,
	) -> Result<(
		Driver,
		Arc<Self>,
		Vec<std::thread::JoinHandle<()>>,
		impl Future<Output = Result<()>>,
	)> {
		let mut sockets = HashMap::new();

		for ((ip, port), id) in connect_to.iter().zip(1..) {
			let stream = TcpStream::connect(format!("{ip}:{port}")).await?;
			let mut socket_framed = Framed::new(stream, Codec::default());
			let connect = if id < connect_to.len() {
				connect_to[(id).into()..].to_vec()
			} else {
				Vec::new()
			};
			socket_framed
				.send(NetworkMessage::Hello {
					id: id as u16,
					num_shard_per_system,
					connect_to: connect,
				})
				.await?;
			sockets.insert(id, socket_framed);
		}

		let (s, r) = crossbeam_channel::unbounded();
		let (driver_access, receiver_driver) = (Box::new(s) as Box<dyn DriverAccess>, r);

		let (system_senders, receivers_system): (Vec<_>, Vec<_>) = (0..connect_to.len())
			.map(|_idx| futures::channel::mpsc::unbounded())
			.unzip();

		let (local_shard_access, local_receivers_shard): (Vec<_>, Vec<_>) = (0
			..num_shard_per_system)
			.map(|_| {
				let (s, r) = crossbeam_channel::unbounded::<Vec<ShardMessage>>();
				(Box::new(s) as Box<dyn ShardAccess>, r)
			})
			.unzip();

		let remote_shard_access = system_senders.iter().zip(1..).flat_map(|(s, shard_id)| {
			(0..num_shard_per_system).map(move |i| {
				Box::new(RemoteShardAccess {
					shard_id: shard_id * num_shard_per_system + i,
					system_channel: s.clone(),
				}) as Box<dyn ShardAccess>
			})
		});

		let system = Arc::new(Self {
			drivers: vec![driver_access],
			shards: local_shard_access
				.into_iter()
				.chain(remote_shard_access)
				.collect(),
		});

		let local_threads_join_handles = local_receivers_shard
			.into_iter()
			.enumerate()
			.map(|(id, receiver)| {
				crate::shard::spawn(
					|| crate::storage::ram::RamStorage::default(),
					system.clone(),
					receiver.clone(),
					id,
				)
			})
			.collect();

		let cloned_system = system.clone();
		let forwarding = tokio::spawn(async {
			receivers_system
				.into_iter()
				.zip(1..)
				.map(move |(channel, system_id)| {
					let socket = sockets.remove(&system_id).unwrap();

					let system = cloned_system.clone();
					tokio::spawn(async { handle_network_forwarding(channel, socket, system).await })
				})
				.collect::<FuturesUnordered<_>>()
				.map(|x| Ok(x??))
				.try_for_each(|_| async { anyhow::Ok(()) })
				.await
		})
		.map(|x| Ok(x??));

		Ok((
			Driver::new(MessageBatching::new(system.clone()), 0, receiver_driver),
			system,
			local_threads_join_handles,
			forwarding,
		))
	}

	pub async fn server(port: u16) -> Result<()> {
		let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;
		let (stream, _) = listener.accept().await?;
		let mut socket_master = Framed::new(stream, Codec::default());

		let message = socket_master
			.next()
			.await
			.ok_or_else(|| anyhow!("The stream was empty"))??;

		let (self_id, num_shard_per_system, connect_to) = if let NetworkMessage::Hello {
			id,
			num_shard_per_system,
			connect_to,
		} = message
		{
			(id, num_shard_per_system, connect_to)
		} else {
			bail!("The first message from the master should be Hello")
		};

		let mut sockets = HashMap::new();

		sockets.insert(0, socket_master);

		for _ in 1..self_id {
			let (stream, _) = listener.accept().await?;
			let mut stream_framed = Framed::new(stream, Codec::default());
			let message = stream_framed
				.next()
				.await
				.ok_or_else(|| anyhow!("The stream was empty"))??;

			if let NetworkMessage::Id { id } = message {
				sockets.insert(id, stream_framed);
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
			sockets.insert(id, socket_framed);
		}

		let (local_shard_access, local_receivers_shard): (Vec<_>, Vec<_>) = (0
			..num_shard_per_system)
			.map(|_| {
				let (s, r) = crossbeam_channel::unbounded::<Vec<ShardMessage>>();
				(Box::new(s) as Box<dyn ShardAccess>, r)
			})
			.unzip();

		let (ref system_senders, receivers_system): (Vec<_>, Vec<_>) = (0..sockets.len())
			.map(|_idx| futures::channel::mpsc::unbounded())
			.unzip();

		let shards = (0..self_id)
			.flat_map(|system_id| {
				(0..num_shard_per_system).map(move |i| {
					Box::new(RemoteShardAccess {
						shard_id: system_id * num_shard_per_system + i,
						system_channel: system_senders[system_id as usize].clone(),
					}) as Box<dyn ShardAccess>
				})
			})
			.chain(local_shard_access)
			.chain(
				(self_id + 1..sockets.len() as u16 + 1).flat_map(|system_id| {
					(0..num_shard_per_system).map(move |i| {
						Box::new(RemoteShardAccess {
							shard_id: system_id * num_shard_per_system + i,
							system_channel: (&system_senders)[system_id as usize - 1].clone(),
						}) as Box<dyn ShardAccess>
					})
				}),
			)
			.collect();

		let system = Arc::new(Self {
			drivers: vec![Box::new(RemoteDriverAccess {
				driver_idx: 0,
				system_channel: system_senders[0].clone(),
			}) as Box<dyn DriverAccess>],
			shards,
		});

		let local_threads_join_handles = local_receivers_shard
			.into_iter()
			.enumerate()
			.map(|(id, receiver)| {
				crate::shard::spawn(
					|| crate::storage::ram::RamStorage::default(),
					system.clone(),
					receiver.clone(),
					id + (self_id * num_shard_per_system) as usize,
				)
			})
			.collect::<Vec<_>>();

		let forwarding = async {
			receivers_system
				.into_iter()
				.zip(0..)
				.map(|(channel, system_id)| {
					let system_id = if system_id == self_id {
						system_id + 1
					} else {
						system_id
					};
					let socket = sockets.remove(&system_id).unwrap();

					let union_find_system = system.clone();
					tokio::spawn(async {
						handle_network_forwarding(channel, socket, union_find_system).await
					})
				})
				.collect::<FuturesUnordered<_>>()
				.map(|x| anyhow::Ok(x??))
				.try_for_each(|_| async { Ok(()) })
				.await
		};

		// let handle = tokio::task::spawn_blocking(|| {
		// 	for t in local_threads_join_handles {
		// 		t.join().unwrap()
		// 	}
		// })
		// .map_err(|err| err.into());

		futures::try_join!(/* handle, */ forwarding)?;

		Ok(())
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

async fn handle_network_forwarding(
	receiver: futures::channel::mpsc::UnboundedReceiver<NetworkMessage>,
	socket: Framed<TcpStream, Codec>,
	system: Arc<System>,
) -> Result<()> {
	let (sink, stream) = socket.split();
	let forward_to_remote = async {
		receiver.map(|x| Ok(x)).forward(sink).await?;
		Err::<(), anyhow::Error>(anyhow!("The channel was closed"))
	};

	let forward_from_remote = async move {
		stream
			.try_for_each(|m| async {
				match m {
					NetworkMessage::Hello {
						id: _,
						num_shard_per_system: _,
						connect_to: _,
					} => bail!("There should be no Hello messages at this point"),
					NetworkMessage::Id { id: _ } => {
						bail!("There should be no Id messages at this point")
					}
					NetworkMessage::DriverMessages { driver_idx, batch } => {
						system.driver(driver_idx as usize).send_messages(batch)
					}
					NetworkMessage::ShardMessages { shard_id, batch } => {
						system.shard(shard_id as usize).send_messages(batch)
					}
				};
				Ok(())
			})
			.await?;

		Err::<(), anyhow::Error>(anyhow!("The socket was closed"))
	};

	futures::try_join!(forward_from_remote, forward_to_remote).map(|_| ())
}
