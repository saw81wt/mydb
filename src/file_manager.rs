use std::io::{Cursor, SeekFrom, Seek, Read, Write, self};

pub const PAGE_SIZE: usize = 4096;
pub const INTGER_BYTES: usize = 4;

#[derive(Eq, PartialEq, Hash)]
pub struct BlockId {
    pub filename: String,
    pub block_number: i32,
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
}

pub struct FileManager {
    pub directory: String,
    pub block_size: usize,
}

impl FileManager {
    fn new(directory: String) -> Self {
        FileManager {
            directory,
            block_size: 1024,
        }
    }

    fn write(&self, block_id: &BlockId, page: &Page) {

    }

    fn read(&self, block_id: &BlockId, page: &Page) {

    }

    fn get_file(&self, filename: String) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disk() {
        let file_manager = FileManager::new("simple_db".to_string());

        let block_id = BlockId {
            filename: "testfile".to_string(),
            block_number: 2
        };

        let mut page1 = Page::new(file_manager.block_size);

        let pos1: usize = 1025;
        page1.set_string(pos1, "abcdefg".to_string()).unwrap();
        assert_eq!(page1.get_string(pos1).unwrap(), "abcdefg".to_string());

        let size1: usize = Page::max_length("abcdefg".len());

        let pos2 = (pos1 + size1) as usize;
        page1.set_int(pos2, 345).unwrap();
        assert_eq!(page1.get_int(pos2).unwrap(), 345);

        let pos3 = Page::max_length(pos2);
        page1.set_bytes(pos3, b"hijklmn").unwrap();
        assert_eq!(page1.get_bytes(pos3).unwrap().to_vec(), b"hijklmn");        
        //file_manager.write(&block_id, &page1);

        //let mut page2 = Page::new(file_manager.block_size);
        //file_manager.read(&block_id, &page2);

        //assert_eq!(page1.get_string(pos1), page2.get_string(pos1));
        //assert_eq!(page1.get_int(pos2), page2.get_int(pos2));

        //assert_ne!(page1.get_string(pos1), page2.get_string(pos2))
    }
}
