pub(crate) mod message;
pub(crate) mod network_message;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use futures::stream::FuturesUnordered;
use futures::Future;
use futures::SinkExt;
use futures::StreamExt;
use std::collections::HashMap;
use std::io::prelude::*;
use std::{net::IpAddr, sync::Arc};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::driver::network_message::Codec;
use crate::driver::network_message::NetworkMessage;
use crate::prelude::*;
use crate::thread;
use crate::thread::RemoteThreadAccess;

pub struct Driver {
	driver_id: usize,
	drivers: Vec<Box<dyn DriverAccess>>,
	threads: Vec<Box<dyn ThreadAccess>>,
	receiver: crossbeam_channel::Receiver<Vec<DriverMessage>>,
}

impl Driver {
	pub fn ram_local_threads(n: u16) -> (Arc<Self>, Vec<std::thread::JoinHandle<()>>) {
		let (system_sender, system_receiver) = crossbeam_channel::unbounded();
		let (threads, receivers): (Vec<_>, Vec<_>) = (0..n)
			.map(|_| {
				let (s, r) = crossbeam_channel::unbounded::<Vec<ThreadMessage>>();
				(Box::new(s) as Box<dyn ThreadAccess>, r)
			})
			.unzip();
		let union_find_system = Arc::new(Self {
			driver_id: 0,
			drivers: vec![Box::new(system_sender) as Box<dyn DriverAccess>],
			threads,
			receiver: system_receiver,
		});
		let local_threads_join_handles = receivers
			.into_iter()
			.enumerate()
			.map(|(idx, receiver)| {
				crate::thread::spawn::<crate::storage::ram::RamStorage>(
					union_find_system.clone(),
					receiver,
					idx,
				)
			})
			.collect();
		(union_find_system, local_threads_join_handles)
	}

	pub async fn connect(
		num_thread_per_driver: u16,
		connect_to: Vec<(IpAddr, u16)>,
	) -> Result<(
		Arc<Self>,
		Vec<std::thread::JoinHandle<()>>,
		FuturesUnordered<tokio::task::JoinHandle<()>>,
	)> {
		println!("{connect_to:?}");
		let total_drivers = 1 + connect_to.len();
		let total_threads = num_thread_per_driver as usize * total_drivers;

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
					num_thread: num_thread_per_driver,
					connect_to: connect,
				})
				.await?;
			sockets.insert(id, socket_framed);
		}

		let (s, r) = crossbeam_channel::unbounded();
		let (driver_access, receiver_driver) = (Box::new(s) as Box<dyn DriverAccess>, r);

		let (driver_senders, receivers_drivers): (Vec<_>, Vec<_>) = (1..connect_to.len())
			.map(|_| futures::channel::mpsc::unbounded())
			.unzip();

		let (local_threads_access, local_receivers_threads): (Vec<_>, Vec<_>) = (0
			..num_thread_per_driver)
			.map(|_| {
				let (s, r) = crossbeam_channel::unbounded::<Vec<ThreadMessage>>();
				(Box::new(s) as Box<dyn ThreadAccess>, r)
			})
			.unzip();

		let remote_thread_access = driver_senders.iter().zip(1..).map(|(s, thread_id)| {
			Box::new(RemoteThreadAccess {
				thread_id,
				driver_channel: s.clone(),
			}) as Box<dyn ThreadAccess>
		});

		let union_find_system = Arc::new(Self {
			driver_id: 0,
			drivers: std::iter::once(driver_access)
				.chain(
					driver_senders
						.iter()
						.map(|s| Box::new(s.clone()) as Box<dyn DriverAccess>),
				)
				.collect(),
			threads: local_threads_access
				.into_iter()
				.chain(remote_thread_access)
				.collect(),
			receiver: receiver_driver,
		});

		let local_threads_join_handles = local_receivers_threads
			.into_iter()
			.enumerate()
			.map(|(idx, receiver)| {
				crate::thread::spawn::<crate::storage::ram::RamStorage>(
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
			num_thread,
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

	pub(crate) fn thread(&self, thread_id: usize) -> &dyn ThreadAccess {
		&*self.threads[thread_id]
	}

	pub(crate) fn driver(&self, driver_id: usize) -> &dyn DriverAccess {
		&*self.drivers[driver_id]
	}

	pub fn batches_sender(&self) -> MessageBatching {
		MessageBatching::new(self)
	}

	pub fn n_threads(&self) -> usize {
		self.threads.len()
	}

	pub fn n_drivers(&self) -> usize {
		self.drivers.len()
	}

	pub fn driver_id(&self) -> usize {
		self.driver_id
	}

	pub fn receiver(&self) -> &crossbeam_channel::Receiver<Vec<DriverMessage>> {
		&self.receiver
	}
}

pub(crate) trait DriverAccess: Sync + Send {
	fn send_messages(&self, batch: Vec<DriverMessage>);
}

impl DriverAccess for crossbeam_channel::Sender<Vec<DriverMessage>> {
	fn send_messages(&self, batch: Vec<DriverMessage>) {
		self.send(batch).unwrap()
	}
}

impl DriverAccess for futures::channel::mpsc::UnboundedSender<Vec<DriverMessage>> {
	fn send_messages(&self, batch: Vec<DriverMessage>) {
		futures::executor::block_on(self.clone().send(batch)).unwrap()
	}
}

fn handle_network_forwarding(
	driver_id: usize,
	receiver_driver: futures::channel::mpsc::UnboundedReceiver<Vec<DriverMessage>>,
	socket: Framed<TcpStream, Codec>,
	union_find_driver: Arc<Driver>,
) {
	let message_batching = MessageBatching::new(&union_find_driver);

	todo!()
}
