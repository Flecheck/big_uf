use rocksdb::WriteBatchWithTransaction;

use {crate::prelude::*, rocksdb::Options};

#[ouroboros::self_referencing]
pub struct RocksDbStorage {
	store: rocksdb::DB,
	#[borrows(store)]
	parent: &'this rocksdb::ColumnFamily,
	#[borrows(store)]
	child: &'this rocksdb::ColumnFamily,
	#[borrows(store)]
	sibling: &'this rocksdb::ColumnFamily,
	len: u64,
}

impl RocksDbStorage {
	pub fn from_path(path: impl AsRef<std::path::Path>) -> Self {
		let options = &mut Options::default();
		options.create_if_missing(true);
		let mut db = rocksdb::DB::open(options, path).expect("Failed to open RocksDB database");
		for name in ["parent", "child", "sibling"] {
			db.create_cf(name, &Options::default()).unwrap();
		}
		Self::new(
			db,
			|db| db.cf_handle("parent").unwrap(),
			|db| db.cf_handle("child").unwrap(),
			|db| db.cf_handle("sibling").unwrap(),
			0,
		)
	}
}

impl RocksDbStorage {
	fn get(&self, key: Key, cf: &rocksdb::ColumnFamily) -> Option<Key> {
		let bytes = self
			.borrow_store()
			.get_pinned_cf(cf, key.inner.to_le_bytes())
			.unwrap()
			.unwrap();
		let read = Key {
			inner: u64::from_le_bytes((&*bytes).try_into().unwrap()),
		};
		if read == key {
			None
		} else {
			Some(read)
		}
	}

	fn set(&self, key: Key, cf: &rocksdb::ColumnFamily, value: Key) {
		self.borrow_store()
			.put_cf(cf, key.inner.to_le_bytes(), value.inner.to_le_bytes())
			.unwrap();
	}
}

impl Storage for RocksDbStorage {
	fn set_parent(&mut self, key: Key, value: Key) {
		self.set(key, self.borrow_parent(), value);
	}

	fn set_sibling(&mut self, key: Key, value: Key) {
		self.set(key, self.borrow_sibling(), value);
	}

	fn swap_child(&mut self, key: Key, value: Key) -> Key {
		let old = self.get_child(key).unwrap();
		self.set(key, self.borrow_child(), value);
		old
	}

	fn get_parent(&self, key: Key) -> Option<Key> {
		self.get(key, self.borrow_parent())
	}

	fn get_sibling(&self, key: Key) -> Option<Key> {
		self.get(key, self.borrow_sibling())
	}

	fn get_child(&self, key: Key) -> Option<Key> {
		self.get(key, self.borrow_child())
	}

	fn add_node(&mut self, shard: usize) -> Key {
		let shard_specific_id = *self.borrow_len();
		let key = Key::new(shard, shard_specific_id as u64);
		self.with_len_mut(|l| *l += 1);

		let key_repr = key.inner.to_le_bytes();
		let mut batch = WriteBatchWithTransaction::<false>::default();

		batch.put_cf(self.borrow_parent(), key_repr, key_repr);
		batch.put_cf(self.borrow_child(), key_repr, key_repr);
		batch.put_cf(self.borrow_sibling(), key_repr, key_repr);

		self.borrow_store().write(batch).unwrap();
		key
	}
}
