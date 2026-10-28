//! A memory block flowing through the pump.

/// A data block is a slice of memory.
pub struct DataBlock {
    ptr: *mut u8,
    alloc_len: usize,
    len: usize,
}

unsafe impl Send for DataBlock {}
unsafe impl Sync for DataBlock {}

impl DataBlock {
    /// Creates a new data block.
    ///
    /// We allign the memory to 4096 bytes to ensure that the data is
    /// not split accross different pages.
    pub fn new(alloc_len: usize) -> Self {
        let ptr = unsafe { libc::aligned_alloc(4096, alloc_len) as *mut u8 };
        if ptr.is_null() {
            panic!("Failed to allocate memory");
        }
        Self {
            ptr,
            alloc_len,
            len: 0,
        }
    }

    pub fn set_len(&mut self, len: usize) {
        assert!(len <= self.alloc_len);
        self.len = len;
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.alloc_len) }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return a pointer to the data stored at `offset`.
    ///
    /// # Safety
    ///
    /// `offset` should be smaller than [self.len()](Self::len).
    pub unsafe fn get_unchecked(&self, offset: usize) -> *mut u8 {
        unsafe { self.ptr.add(offset) }
    }
}

impl Drop for DataBlock {
    fn drop(&mut self) {
        unsafe { libc::free(self.ptr as *mut libc::c_void) };
    }
}
