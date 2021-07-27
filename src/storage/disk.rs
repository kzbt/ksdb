use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
};

use crate::error::Result;

pub struct PageId(pub u64);

const PAGE_SIZE: u64 = 8192;

pub trait DiskManager {
    fn write_page(&mut self, page_id: PageId, data: Vec<u8>) -> Result<()>;

    fn read_page(&mut self, page_id: PageId) -> Result<Vec<u8>>;
}

pub struct DefaultDiskManager<'a> {
    db_file: &'a File,
    writer: BufWriter<&'a File>,
    reader: BufReader<&'a File>,
}

impl<'a> DefaultDiskManager<'a> {
    pub fn new(db_file: &'a File) -> Self {
        DefaultDiskManager {
            db_file: &db_file,
            writer: BufWriter::new(&db_file),
            reader: BufReader::new(&db_file),
        }
    }
}

impl<'a> DiskManager for DefaultDiskManager<'a> {
    fn write_page(&mut self, page_id: PageId, data: Vec<u8>) -> Result<()> {
        let offset = page_id.0 * PAGE_SIZE;
        self.db_file.seek(SeekFrom::Start(offset))?;

        self.writer.write_all(&data)?;
        self.writer.flush()?;
        Ok(())
    }

    fn read_page(&mut self, page_id: PageId) -> Result<Vec<u8>> {
        let offset = page_id.0 * PAGE_SIZE;
        self.db_file.seek(SeekFrom::Start(offset))?;

        let mut buf = [0; PAGE_SIZE as usize];
        let _ = self.reader.by_ref().take(PAGE_SIZE).read(&mut buf);
        Ok(buf.to_vec())
    }
}

#[cfg(test)]
mod test {
    use std::{fs::OpenOptions, ops::Range};

    use super::*;

    fn pad_zeros(bytes: &[u8], len: usize) -> Vec<u8> {
        let mut padding = vec![0; len - bytes.len()];
        let mut result = bytes.to_vec();

        result.append(&mut padding);
        result
    }

    #[test]
    fn should_read_write_correctly() {
        let db_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open("/tmp/db")
            .unwrap();
        let mut disk_manager = DefaultDiskManager::new(&db_file);
        disk_manager
            .write_page(PageId(2), "some page content".as_bytes().to_vec())
            .unwrap();
        disk_manager
            .write_page(PageId(5), "another page content".as_bytes().to_vec())
            .unwrap();

        let data1 = disk_manager.read_page(PageId(1)).unwrap();
        let data2 = disk_manager.read_page(PageId(2)).unwrap();
        let data4 = disk_manager.read_page(PageId(4)).unwrap();
        let data5 = disk_manager.read_page(PageId(5)).unwrap();

        assert_eq!(data1, pad_zeros(&vec![], PAGE_SIZE as usize));
        assert_eq!(
            data2,
            pad_zeros("some page content".as_bytes(), PAGE_SIZE as usize)
        );
        assert_eq!(data4, pad_zeros(&vec![], PAGE_SIZE as usize));
        assert_eq!(
            data5,
            pad_zeros("another page content".as_bytes(), PAGE_SIZE as usize)
        );
    }
}
