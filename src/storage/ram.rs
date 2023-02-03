use crate::prelude::*;

pub struct NodeData {
	parent: Key,
	sibling: Key,
	child: Key,
}

#[derive(Default)]
pub struct RamStorage {
	store: Vec<NodeData>,
}

impl RamStorage {
	fn get(&self, key: Key, selector: impl Fn(&NodeData) -> Key) -> Option<Key> {
		let x = &self.store[key.thread_specific_id() as usize];
		let x = selector(x);
		if x == key {
			None
		} else {
			Some(x)
		}
	}

	fn set(&mut self, key: Key, selector: impl Fn(&mut NodeData) -> &mut Key, value: Key) -> Key {
		let x = &mut self.store[key.thread_specific_id() as usize];
		let x = selector(x);
		let old = *x;
		*x = value;
		old
	}
}

impl Storage for RamStorage {
	fn set_parent(&mut self, key: Key, value: Key) {
		self.set(key, |x| &mut x.parent, value);
	}

	fn set_sibling(&mut self, key: Key, value: Key) {
		self.set(key, |x| &mut x.sibling, value);
	}

	fn swap_child(&mut self, key: Key, value: Key) -> Key {
		self.set(key, |x| &mut x.child, value)
	}

	fn get_parent(&self, key: Key) -> Option<Key> {
		self.get(key, |x| x.parent)
	}

	fn get_sibling(&self, key: Key) -> Option<Key> {
		self.get(key, |x| x.sibling)
	}

	fn get_child(&self, key: Key) -> Option<Key> {
		self.get(key, |x| x.child)
	}

	fn add_node(&mut self, thread: usize) -> Key {
		let thread_specific_id = self.store.len();
		let key = Key::new(thread, thread_specific_id as u64);

		self.store.push(NodeData {
			parent: key,
			sibling: key,
			child: key,
		});
		key
	}
}
