use std::fs::{File, OpenOptions};

use error::Result;
use storage::disk::{DefaultDiskManager, DiskManager, PageId};

mod error;
mod storage;

fn main() -> Result<()> {
    Ok(())
}
