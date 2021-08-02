use crate::error::Result;

pub mod disk;

pub const PAGE_SIZE: usize = 8192;

pub trait DiskManager {
    fn write_page(&mut self, page_id: PageId, data: &[u8]) -> Result<()>;

    fn read_page(&mut self, page_id: PageId) -> Result<Vec<u8>>;

    fn new_page(&self) -> PageId;
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct PageId(pub usize);

#[derive(Copy, Clone, Debug)]
pub struct Page {
    pub id: PageId,
    pub data: [u8; PAGE_SIZE],
    pub is_dirty: bool,
    pub pin_count: u8,
}

impl Page {
    pub fn new(id: PageId, data: [u8; PAGE_SIZE]) -> Self {
        Page {
            id,
            data,
            is_dirty: false,
            pin_count: 0,
        }
    }
}
