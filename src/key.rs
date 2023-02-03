#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct Key {
	inner: u64,
}

impl Key {
	pub fn new(thread: usize, thread_specific_id: u64) -> Self {
		assert!(thread <= (u16::MAX as usize) && thread_specific_id <= 0x0000FFFFFFFF);
		Self {
			inner: ((thread as u64) << 48) | thread_specific_id,
		}
	}
	pub fn thread(self) -> usize {
		(self.inner >> 48) as usize
	}
	pub fn thread_specific_id(self) -> u64 {
		self.inner & 0x0000FFFFFFFF
	}
}

impl std::fmt::Debug for Key {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Key")
			.field("thread", &self.thread())
			.field("thread_specific_id", &self.thread_specific_id())
			.finish()
	}
}
