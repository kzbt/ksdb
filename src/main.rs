use std::fs::{File, OpenOptions};

use error::Result;
use storage::disk::{DefaultDiskManager, DiskManager, PageId};

mod error;
mod storage;

fn main() -> Result<()> {
    let db_file: File;

    db_file = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open("/var/local/ksdb/db")
        .expect("Not a valid database file path");
    let mut disk_manager = DefaultDiskManager::new(&db_file);
    disk_manager.write_page(PageId(2), "hello db bytes1".as_bytes().to_vec())?;
    disk_manager.write_page(PageId(5), "hello db bytes2".as_bytes().to_vec())?;

    let data1 = disk_manager.read_page(PageId(1))?;
    let data2 = disk_manager.read_page(PageId(2))?;
    let data3 = disk_manager.read_page(PageId(5))?;

    println!("1: {}", String::from_utf8(data1).unwrap());
    println!("2: {}", String::from_utf8(data2).unwrap());
    println!("3: {}", String::from_utf8(data3).unwrap());

    Ok(())
}
