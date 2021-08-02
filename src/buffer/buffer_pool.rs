use std::collections::{BTreeSet, BinaryHeap, HashMap};

use crate::{
    buffer::POOL_SIZE,
    storage::{DiskManager, Page, PageId, PAGE_SIZE},
};

use super::{BufferPoolManager, FrameId, Replacer};

pub struct DefaultBufferPoolManager<DM, R> {
    pool_size: usize,
    pages: Vec<Page>,
    free_pages: Vec<FrameId>,
    page_table: HashMap<PageId, Page>,
    disk_manager: DM,
    replacer: R,
}

impl<DM: DiskManager, R: Replacer> DefaultBufferPoolManager<DM, R> {
    fn new(disk_manager: DM, replacer: R) -> Self {
        Self {
            pool_size: POOL_SIZE,
            pages: vec![],
            free_pages: vec![],
            page_table: HashMap::with_capacity(POOL_SIZE),
            disk_manager,
            replacer,
        }
    }
}

impl<DM: DiskManager, R: Replacer> BufferPoolManager for DefaultBufferPoolManager<DM, R> {
    fn pool_size(&self) -> usize {
        self.pool_size
    }

    fn pages(&self) -> &[Page] {
        &self.pages
    }

    fn new_page(&mut self) -> Option<Page> {
        if self.page_table.len() >= self.pool_size {
            let page_to_remove = self.replacer.remove()?;

            let removed = self.page_table.remove(&page_to_remove)?;
            if removed.is_dirty {
                self.disk_manager
                    .write_page(removed.id, &removed.data)
                    .ok()?;
            }
        }

        let next_id = self.disk_manager.new_page();
        let data = [0; PAGE_SIZE];
        let new_page = Page::new(next_id, data);
        self.page_table.insert(next_id, new_page);
        dbg!(&self.page_table.keys());

        Some(new_page)
    }

    fn fetch_page(&self, page_id: PageId) -> crate::error::Result<Page> {
        todo!()
    }

    fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> bool {
        todo!()
    }

    fn flush_page(&self, page_id: PageId) -> bool {
        todo!()
    }

    fn delete_page(&mut self, page_id: PageId) -> bool {
        todo!()
    }

    fn flush_all(&self) {
        todo!()
    }
}

#[derive(Debug, Eq, PartialEq)]
struct CachedPageItem {
    page_id: PageId,
    rank: usize,
}

impl Ord for CachedPageItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.rank.cmp(&other.rank)
    }
}

impl PartialOrd for CachedPageItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub struct LruReplacer {
    pages: BTreeSet<CachedPageItem>,
    count: usize,
}

impl LruReplacer {
    pub fn new(size: usize) -> Self {
        LruReplacer {
            pages: BTreeSet::new(),
            count: 0,
        }
    }
}

impl Replacer for LruReplacer {
    fn remove(&mut self) -> Option<PageId> {
        self.pages.pop_first().map(|item| item.page_id)
    }

    fn pin(&mut self, page_id: PageId) {
        self.pages.retain(|item| item.page_id != page_id)
    }

    fn unpin(&mut self, page_id: PageId) {
        self.pages.insert(CachedPageItem {
            page_id,
            rank: self.count,
        });
        self.count += 1;
    }

    fn available(&self) -> usize {
        self.pages.len()
    }
}

#[cfg(test)]
mod test {

    use crate::storage::disk::{self, DefaultDiskManager};

    use super::*;

    pub fn replacer(size: usize) -> LruReplacer {
        LruReplacer::new(size)
    }

    pub fn buffer_pool_manager(
    ) -> DefaultBufferPoolManager<DefaultDiskManager<'static>, LruReplacer> {
        DefaultBufferPoolManager::new(disk::should::disk_manager(), replacer(10))
    }

    #[test]
    fn create_new_page_if_space_available() {
        let mut bpm = buffer_pool_manager();
        let page = bpm.new_page();
        assert!(page.is_some());
        assert_eq!(page.unwrap().id, PageId(1))
    }

    #[test]
    fn fail_create_new_page_if_no_space() {
        let mut bpm = buffer_pool_manager();
        bpm.pool_size = 2;
        let _ = bpm.new_page();
        let _ = bpm.new_page();
        let page = bpm.new_page();
        assert!(page.is_none());
    }

    mod lru {
        use std::assert_matches::assert_matches;

        use super::*;

        #[test]
        fn remove_item_with_lowest_rank() {
            let mut replacer = replacer(5);
            replacer.unpin(PageId(5));
            replacer.unpin(PageId(2));
            replacer.unpin(PageId(4));
            replacer.unpin(PageId(8));
            replacer.unpin(PageId(1));

            let removed = replacer.remove();
            assert_matches!(removed, Some(PageId(5)));
            let removed = replacer.remove();
            assert_matches!(removed, Some(PageId(2)));
            let removed = replacer.remove();
            assert_matches!(removed, Some(PageId(4)));
            let removed = replacer.remove();
            assert_matches!(removed, Some(PageId(8)));
            let removed = replacer.remove();
            assert_matches!(removed, Some(PageId(1)));
            let removed = replacer.remove();
            assert_matches!(removed, None);
        }
    }
}
