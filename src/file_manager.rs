use std::io::{Cursor, SeekFrom, Seek, Read, Write, self};
use std::fs::{File, OpenOptions};

pub const PAGE_SIZE: usize = 4096;
pub const INTGER_BYTES: usize = 4;

#[derive(Eq, PartialEq, Hash)]
pub struct BlockId {
    pub filename: String,
    pub block_number: usize,
}

pub struct Page {
    pub block_size: usize,
    cursor: Cursor<Vec<u8>>
}

impl Page {
    pub fn new(block_size: usize) -> Self {
        Page {
            block_size,
            cursor: Cursor::new(Vec::with_capacity(block_size))
        }
    }

    pub fn get_int(&mut self, offset: usize) -> io::Result<i32> {
        self.cursor.seek(SeekFrom::Start(offset as u64))?;
        let ret: &mut [u8; INTGER_BYTES] = &mut [0; INTGER_BYTES];
        self.cursor.read_exact(ret)?;
        Ok(i32::from_be_bytes(*ret))
    }

    pub fn set_int(&mut self, offset: usize, value: i32) -> io::Result<()> {
        self.cursor.seek(SeekFrom::Start(offset as u64))?;
        let data = i32::to_be_bytes(value);
        self.cursor.write_all(&data)?;
        Ok(())
    }

    pub fn get_bytes(&mut self, offset: usize) -> io::Result<Box<[u8]>> {
        let length = self.get_int(offset)?;
        let mut data = vec![0; length as usize].into_boxed_slice();
        self.cursor.read_exact(data.as_mut())?;
        Ok(data)
    }

    pub fn set_bytes(&mut self, offset: usize, value: &[u8]) -> io::Result<()>{
        self.cursor.seek(SeekFrom::Start(offset as u64))?;
        self.set_int(offset, value.len() as i32)?;
        self.cursor.write_all(value)?;
        Ok(())
    }
    
    pub fn get_string(&mut self, offset: usize) -> io::Result<String> {
        let data = self.get_bytes(offset)?;
        Ok(String::from_utf8(data.to_vec()).unwrap())
    }

    pub fn set_string(&mut self, offset: usize, value: String) -> io::Result<()> {
        self.set_bytes(offset, value.as_bytes())?;
        Ok(())
    }

    pub fn max_length(strlen: usize) -> usize {
        INTGER_BYTES + strlen
    }
    
    pub fn contents(&mut self) -> &mut Vec<u8> {
        self.cursor.get_mut()
    }
}

pub struct FileManager {
    pub directory: String,
    pub block_size: usize,
}

impl FileManager {
    fn new(directory: String) -> Self {
        FileManager {
            directory,
            block_size: PAGE_SIZE,
        }
    }

    fn write(&self, block_id: &BlockId, page: &mut Page) -> io::Result<()> {
        let mut file = self.get_file(&block_id.filename)?;
        file.seek(SeekFrom::Start((block_id.block_number * PAGE_SIZE) as u64))?;
        file.write_all(page.contents())?;
        Ok(())
    }

    fn read(&self, block_id: &BlockId, page: &mut Page) -> io::Result<()> {
        let mut file = self.get_file(&block_id.filename)?;
        file.seek(SeekFrom::Start((block_id.block_number * PAGE_SIZE) as u64))?;
        file.read_to_end(page.contents())?;
        Ok(())
    }

    fn get_file(&self, filename: &String) -> io::Result<File> {
        OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(format!("{}/{filename}", self.directory))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::Builder;

    #[test]
    fn disk() {
        let directory = "./data";
        let tempfile = Builder::new()
            .tempfile_in(directory)
            .unwrap();
        let filename = tempfile
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap();

        let str_sample = "abcdeg";
        let byte_sample = b"hijklmn";
        let int_sample = 345;


        let file_manager1 = FileManager::new(directory.to_string());
        let file_manager2 = FileManager::new(directory.to_string());
        
        let block_id = BlockId {
            filename: filename.to_string(),
            block_number: 2
        };

        let block_id2 = BlockId {
            filename: filename.to_string(),
            block_number: 3
        };

        let mut page1 = Page::new(file_manager1.block_size);
        let mut page2 = Page::new(file_manager2.block_size);
        let mut page3 = Page::new(file_manager2.block_size);

        let str_position: usize = 1025;
        let byte_position = str_position + Page::max_length(str_sample.len());
        let int_position = Page::max_length(byte_position + byte_sample.len());
    
        // set_string & get_string
        page1.set_string(str_position, str_sample.to_string()).unwrap();
        assert_eq!(page1.get_string(str_position).unwrap(), str_sample.to_string());

        // set_bytes & get_bytes
        page1.set_bytes(byte_position, byte_sample).unwrap();
        assert_eq!(page1.get_bytes(byte_position).unwrap().to_vec(), byte_sample);

        // set_int & get_int
        page1.set_int(int_position, int_sample).unwrap();
        assert_eq!(page1.get_int(int_position).unwrap(), int_sample);

        // write file & read file
        file_manager1.write(&block_id, &mut page1).unwrap();
        file_manager2.read(&block_id, &mut page2).unwrap();
        assert_eq!(page2.get_string(str_position).unwrap(), str_sample.to_string());
        
        file_manager2.read(&block_id2, &mut page3).unwrap();
        assert!(page3.get_string(str_position).is_err());
        
        drop(tempfile)
    }
}
