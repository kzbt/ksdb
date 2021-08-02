use crate::error::Result;
use crate::storage::{Page, PageId};

pub mod buffer_pool;

pub struct FrameId(usize);

pub const POOL_SIZE: usize = 1024;

pub trait BufferPoolManager {
    fn pool_size(&self) -> usize;

    fn pages(&self) -> &[Page];

    fn new_page(&mut self) -> Option<Page>;

    fn fetch_page(&self, page_id: PageId) -> Result<Page>;

    fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> bool;

    fn flush_page(&self, page_id: PageId) -> bool;

    fn delete_page(&mut self, page_id: PageId) -> bool;

    fn flush_all(&self);
}

pub trait Replacer {
    fn remove(&mut self) -> Option<PageId>;

    fn pin(&mut self, page_id: PageId);

    fn unpin(&mut self, page_id: PageId);

    fn available(&self) -> usize;
}
