use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops;
use std::slice::Iter;

/// u32 should be enough
type ArenaIdType = u32;

/// ArenaId types are checked at runtime to match the type of the Arena (newtype idiom).
pub struct ArenaId<T>(ArenaIdType, PhantomData<T>);

impl<T> Clone for ArenaId<T> {
    fn clone(&self) -> Self {
        ArenaId(self.0 as ArenaIdType, PhantomData)
    }
}

impl<T> Copy for ArenaId<T> {}

impl<T> Debug for ArenaId<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<T> Display for ArenaId<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<T> Eq for ArenaId<T> {}

impl<T> Hash for ArenaId<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl<T> PartialEq for ArenaId<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

/// Id based arena for graph data structures.
pub struct Arena<T> {
    items: Vec<T>,
}

impl<T> Arena<T> {
    /// Add an item and return its id.
    pub fn alloc(&mut self, item: T) -> ArenaId<T> {
        let id = self.next_id();
        self.items.push(item);
        id
    }

    /// Provide an id for the next item, add the item and return the id.
    pub fn alloc_with_id(&mut self, f: impl FnOnce(ArenaId<T>) -> T) -> ArenaId<T> {
        let id = self.next_id();
        self.items.push(f(id));
        id
    }

    pub fn get(&self, id: ArenaId<T>) -> Option<&T> {
        self.items.get(id.0 as usize)
    }

    pub fn get_mut(&mut self, id: ArenaId<T>) -> Option<&mut T> {
        self.items.get_mut(id.0 as usize)
    }

    pub fn first_id(&self) -> ArenaId<T> {
        ArenaId(0 as ArenaIdType, PhantomData)
    }

    /// Iteration w/o borrowing, use first_id() as starting point
    pub fn get_and_inc_id(&self, id: &mut ArenaId<T>) -> Option<&T> {
        match self.items.get(id.0 as usize) {
            Some(x) => {
                id.0 += 1;
                Some(x)
            }
            _ => None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn iter(&self) -> Iter<'_, T> {
        self.items.iter()
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, T> {
        self.items.iter_mut()
    }

    fn next_id(&self) -> ArenaId<T> {
        ArenaId(self.items.len() as ArenaIdType, PhantomData)
    }
}

impl<T> Default for Arena<T> {
    fn default() -> Self {
        Self { items: vec![] }
    }
}

impl<T> ops::Index<ArenaId<T>> for Arena<T> {
    type Output = T;

    fn index(&self, id: ArenaId<T>) -> &T {
        &self.items[id.0 as usize]
    }
}

impl<T> ops::IndexMut<ArenaId<T>> for Arena<T> {
    fn index_mut(&mut self, id: ArenaId<T>) -> &mut T {
        &mut self.items[id.0 as usize]
    }
}
