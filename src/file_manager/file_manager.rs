use std::cell::RefCell;
use std::collections::HashMap;

use std::collections::hash_map::Entry;
use std::fs::{metadata, File, OpenOptions};
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::rc::Rc;

use log::trace;

use super::page::Page;

pub const PAGE_SIZE: usize = 4096;
pub const INTGER_BYTES: usize = 4;

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct BlockId {
    pub filename: String,
    pub block_number: i32,
}

impl BlockId {
    fn new(filename: &str, block_number: i32) -> Self {
        BlockId {
            filename: filename.to_string(),
            block_number,
        }
    }
}

pub struct FileManager {
    pub directory: String,
    pub block_size: usize,
    pub open_files: Rc<RefCell<HashMap<String, File>>>,
}

impl FileManager {
    pub fn new(directory: String) -> Self {
        FileManager {
            directory,
            block_size: PAGE_SIZE,
            open_files: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    pub fn write(&mut self, block_id: &BlockId, page: &mut Page) -> io::Result<()> {
        let block_size = self.block_size as i32;
        let mut file = self.get_file(&block_id.filename)?;
        file.seek(SeekFrom::Start((block_id.block_number * block_size) as u64))?;
        file.write_all(page.contents())?;
        Ok(())
    }

    pub fn read(&mut self, block_id: &BlockId, page: &mut Page) -> io::Result<()> {
        let block_size = self.block_size as i32;
        let mut file = self.get_file(&block_id.filename)?;
        file.seek(SeekFrom::Start((block_id.block_number * block_size) as u64))?;
        file.read_to_end(page.contents())?;
        Ok(())
    }

    fn get_file(&mut self, filename: &String) -> io::Result<File> {
        let file = match self.open_files.borrow_mut().entry(filename.to_string()) {
            Entry::Occupied(o) => o.into_mut().try_clone()?,
            Entry::Vacant(v) => {
                let new_file = OpenOptions::new()
                    .write(true)
                    .read(true)
                    .create(true)
                    .open(format!("{}/{filename}", self.directory))?;
                v.insert(new_file).try_clone()?
            }
        };
        Ok(file)
    }

    pub fn length(&mut self, filename: &String) -> anyhow::Result<i32> {
        let _ = self.get_file(filename)?;
        let s = metadata(filename).unwrap();
        return Ok((s.len() / (self.block_size as u64)) as i32);
    }

    pub fn append_new_block(&mut self, filename: &String) -> io::Result<BlockId> {
        let block_size = self.block_size;

        let new_block_num = self.last_block_num(filename)?;
        let new_block = BlockId {
            filename: filename.to_string(),
            block_number: new_block_num as i32,
        };
        let buf: Vec<u8> = Vec::with_capacity(block_size);

        let mut file = self.get_file(filename)?;
        file.seek(SeekFrom::Start((new_block_num * block_size) as u64))?;
        file.write_all(&buf)?;
        Ok(new_block)
    }

    pub fn last_block_num(&mut self, filename: &String) -> io::Result<usize> {
        let file = self.get_file(filename)?;
        Ok(file.metadata().unwrap().len() as usize / self.block_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::Builder;

    #[test]
    fn disk() {
        // init logger for debug
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Debug)
            .is_test(true)
            .try_init();

        let directory = "./data";
        let tempfile = Builder::new().tempfile_in(directory).unwrap();
        let filename = tempfile.path().file_name().unwrap().to_str().unwrap();

        let str_sample = "abcdeg";
        let byte_sample = b"hijklmn";
        let int_sample = 345;

        let mut file_manager1 = FileManager::new(directory.to_string());
        let mut file_manager2 = FileManager::new(directory.to_string());

        let block_id = BlockId {
            filename: filename.to_string(),
            block_number: 2,
        };

        let block_id2 = BlockId {
            filename: filename.to_string(),
            block_number: 3,
        };

        let mut page1 = Page::new(file_manager1.block_size);
        let mut page2 = Page::new(file_manager2.block_size);
        let mut page3 = Page::new(file_manager2.block_size);

        let str_position: usize = 1025;
        let byte_position = str_position + Page::max_length(str_sample.len());
        let int_position = Page::max_length(byte_position + byte_sample.len());

        // set_string & get_string
        page1
            .set_string(str_position, str_sample.to_string())
            .unwrap();
        assert_eq!(
            page1.get_string(str_position).unwrap(),
            str_sample.to_string()
        );

        // set_bytes & get_bytes
        page1.set_bytes(byte_position, byte_sample).unwrap();
        assert_eq!(
            page1.get_bytes(byte_position).unwrap().to_vec(),
            byte_sample
        );

        // set_int & get_int
        page1.set_int(int_position, int_sample).unwrap();
        assert_eq!(page1.get_int(int_position).unwrap(), int_sample);

        // write file & read file
        file_manager1.write(&block_id, &mut page1).unwrap();
        file_manager2.read(&block_id, &mut page2).unwrap();
        assert_eq!(
            page2.get_string(str_position).unwrap(),
            str_sample.to_string()
        );

        file_manager2.read(&block_id2, &mut page3).unwrap();
        assert!(page3.get_string(str_position).is_err());

        drop(tempfile)
    }
}
