use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    sync::atomic::{AtomicIsize, AtomicPtr, AtomicUsize, Ordering},
};

use crate::error::Result;

use super::{DiskManager, PageId, PAGE_SIZE};

pub struct DefaultDiskManager<'a> {
    db_file: &'a File,
    writer: BufWriter<&'a File>,
    reader: BufReader<&'a File>,
    next_page_id: AtomicUsize,
}

impl<'a> DefaultDiskManager<'a> {
    pub fn new(db_file: &'a File) -> Self {
        DefaultDiskManager {
            db_file: &db_file,
            writer: BufWriter::new(&db_file),
            reader: BufReader::new(&db_file),
            next_page_id: AtomicUsize::new(1),
        }
    }
}

impl<'a> DiskManager for DefaultDiskManager<'a> {
    fn write_page(&mut self, page_id: PageId, data: &[u8]) -> Result<()> {
        let offset = page_id.0 * PAGE_SIZE;
        self.db_file.seek(SeekFrom::Start(offset as u64))?;

        self.writer.write_all(&data)?;
        self.writer.flush()?;
        Ok(())
    }

    fn read_page(&mut self, page_id: PageId) -> Result<Vec<u8>> {
        let offset = page_id.0 * PAGE_SIZE;
        self.db_file.seek(SeekFrom::Start(offset as u64))?;

        let mut buf = [0; PAGE_SIZE as usize];
        let _ = self.reader.by_ref().take(PAGE_SIZE as u64).read(&mut buf);
        Ok(buf.to_vec())
    }

    fn new_page(&self) -> PageId {
        let next_id = self.next_page_id.fetch_add(1, Ordering::Relaxed);
        PageId(next_id)
    }
}

#[cfg(test)]
pub mod test {
    use std::fs::OpenOptions;

    use super::*;

    pub fn disk_manager() -> DefaultDiskManager<'static> {
        let db_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open("/tmp/db")
            .unwrap();
        DefaultDiskManager::new(Box::leak(Box::new(db_file)))
    }

    #[test]
    fn read_write_correctly() {
        let mut disk_manager = disk_manager();
        disk_manager
            .write_page(PageId(2), "some page content".as_bytes())
            .unwrap();
        disk_manager
            .write_page(PageId(5), "more page content".as_bytes())
            .unwrap();

        let data1 = disk_manager.read_page(PageId(1)).unwrap();
        let data2 = disk_manager.read_page(PageId(2)).unwrap();
        let data4 = disk_manager.read_page(PageId(4)).unwrap();
        let data5 = disk_manager.read_page(PageId(5)).unwrap();

        assert_eq!(data1, pad_zeros(&vec![], PAGE_SIZE));
        assert_eq!(data2, pad_zeros("some page content".as_bytes(), PAGE_SIZE));
        assert_eq!(data4, pad_zeros(&vec![], PAGE_SIZE));
        assert_eq!(data5, pad_zeros("more page content".as_bytes(), PAGE_SIZE));
    }

    #[test]
    fn increment_page_id_on_new() {
        let mut disk_manager = disk_manager();

        let next_page_id = disk_manager.new_page();
        assert_eq!(next_page_id, PageId(1));

        let next_page_id = disk_manager.new_page();
        assert_eq!(next_page_id, PageId(2));
    }

    fn pad_zeros(bytes: &[u8], len: usize) -> Vec<u8> {
        let mut padding = vec![0; len - bytes.len()];
        let mut result = bytes.to_vec();

        result.append(&mut padding);
        result
    }
}
