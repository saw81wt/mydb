use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};

use super::file_manager::INTGER_BYTES;

#[derive(Debug)]
pub struct Page {
    cursor: Cursor<Vec<u8>>,
}

impl Page {
    pub fn new(block_size: usize) -> Self {
        Page {
            cursor: Cursor::new(Vec::with_capacity(block_size)),
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
        let read_length = self.cursor.read(data.as_mut())?;
        log::debug!("read_length: {}", read_length);
        Ok(data)
    }

    pub fn set_bytes(&mut self, offset: usize, value: &[u8]) -> io::Result<()> {
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

impl From<Box<[u8]>> for Page {
    fn from(buf: Box<[u8]>) -> Self {
        Page {
            cursor: Cursor::new(buf.to_vec()),
        }
    }
}
