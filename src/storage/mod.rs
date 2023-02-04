pub mod ram;

use crate::prelude::*;

pub(crate) trait Storage: Default {
	fn set_parent(&mut self, key: Key, value: Key);
	fn set_sibling(&mut self, key: Key, value: Key);
	fn swap_child(&mut self, key: Key, value: Key) -> Key;

	fn get_parent(&self, key: Key) -> Option<Key>;
	fn get_sibling(&self, key: Key) -> Option<Key>;
	fn get_child(&self, key: Key) -> Option<Key>;

	fn add_node(&mut self, shard: usize) -> Key;
}
