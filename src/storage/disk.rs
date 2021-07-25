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
