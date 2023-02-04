#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct Key {
	pub(crate) inner: u64,
}

impl Key {
	pub fn new(shard: usize, shard_specific_id: u64) -> Self {
		assert!(shard <= (u16::MAX as usize) && shard_specific_id <= 0x0000FFFFFFFF);
		Self {
			inner: ((shard as u64) << 48) | shard_specific_id,
		}
	}
	pub fn shard(self) -> usize {
		(self.inner >> 48) as usize
	}
	pub fn shard_specific_id(self) -> u64 {
		self.inner & 0x0000FFFFFFFF
	}
}

impl std::fmt::Debug for Key {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Key")
			.field("shard", &self.shard())
			.field("shard_specific_id", &self.shard_specific_id())
			.finish()
	}
}
