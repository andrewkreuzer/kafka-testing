use std::ptr;
use std::slice::Iter;

pub struct Cushion<T> {
    buffer: Vec<T>,
    capacity: usize,
    size: usize,
    write_index: usize,
    read_index: usize,
}

impl<T> Cushion<T> {
    pub fn with_capacity(capacity: usize) -> Cushion<T> {
        Cushion {
            buffer: Vec::<T>::with_capacity(capacity),
            capacity,
            size: 0,
            write_index: 0,
            read_index: 0,
        }
    }

    pub fn push_back(&mut self, value: T) {
        // TODO: overwrite of raise error?
        // if self.size == self.capacity {}

        unsafe {
            ptr::write(self.buffer.as_mut_ptr().add(self.write_index), value);
            if self.buffer.len() < self.capacity {
                self.buffer.set_len(self.buffer.len() + 1);
            }
        }

        self.size += 1;
        self.write_index += 1;
        if self.write_index == self.capacity {
            self.write_index = 0;
        }
    }

    pub fn peek_back(&mut self) -> Option<T> {
        if self.size == 0 && self.write_index == 0 {
            return None;
        }
        let ret: T;
        unsafe {
            ret = ptr::read(self.buffer.as_ptr().add(self.write_index));
        }
        Some(ret)
    }

    pub fn pop_front(&mut self) -> Option<T> {
        if self.size == 0 {
            return None
        }
        let ret: T;
        unsafe {
            ret = ptr::read(self.buffer.as_ptr().add(self.read_index));
            self.buffer.set_len(self.buffer.len() - 1);
            if self.read_index == self.capacity {
                self.buffer.set_len(0);
            }
        }
        self.size -= 1;
        self.read_index += 1;
        if self.read_index == self.capacity {
            self.read_index = 0;
        }

        Some(ret)
    }

    pub fn peek_front(&mut self) -> Option<T> {
        let ret: T;
        unsafe {
            ret = ptr::read(self.buffer.as_ptr().add(self.read_index));
        }
        Some(ret)
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn iter(&mut self) -> Iter<'_, T> {
        self.buffer.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_push_pop() {
        let mut c: Cushion<u8> = Cushion::with_capacity(10);
        let a = 1;
        c.push_back(a);

        assert!(c.pop_front().unwrap() == a)
    }

    #[test]
    fn test_overflow() {
        let mut c: Cushion<u8> = Cushion::with_capacity(5);
        for i in 0..10 {
            c.push_back(i);
        }

        match c.peek_back() {
            Some(i) => {
                assert!(i == 5);
            }
            None => panic!("there is no last"),
        }

        assert!(c.pop_front().unwrap() == 5);
        match c.peek_front() {
            Some(i) => {
                assert!(i == 6)
            },
            None => panic!("there is no first"),
        }

    }

    #[test]
    fn test_underflow() {
        let mut c: Cushion<u8> = Cushion::with_capacity(5);
        for i in 0..6 {
            c.push_back(i);
            assert!(c.pop_front().unwrap() == i);
        }
    }
}
