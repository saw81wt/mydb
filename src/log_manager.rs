use std::{cell::RefCell, io, rc::Rc};

use crate::file_manager::{BlockId, FileManager, Page, INTGER_BYTES};

pub struct LogManager {
    file_manager: Rc<RefCell<FileManager>>,
    log_file: String,
    log_page: Page,
    current_block: BlockId,
    latest_log_sequence_number: i32,
    last_saved_log_sequence_number: i32,
}

impl LogManager {
    pub fn new(mut file_manager: FileManager, log_file: String) -> io::Result<Self> {
        let log_size = file_manager.length(&log_file)?;
        let mut log_page = Page::new(file_manager.block_size);

        let current_block = if log_size == 0 {
            let block_id = file_manager.append(&log_file)?;
            log_page.set_int(0, file_manager.block_size as i32)?;
            file_manager.write(&block_id, &mut log_page)?;
            block_id
        } else {
            BlockId {
                filename: log_file.clone(),
                block_number: (log_size - 1) as usize,
            }
        };
        Ok(LogManager {
            file_manager: Rc::new(RefCell::new(file_manager)),
            log_file,
            log_page,
            current_block,
            latest_log_sequence_number: 1,
            last_saved_log_sequence_number: 1,
        })
    }

    pub fn flush_with(&mut self, lsn: i32) -> io::Result<()> {
        if lsn > self.last_saved_log_sequence_number {
            self.flush()?;
        }
        Ok(())
    }

    fn iterator(&mut self) -> LogIterator {
        self.flush().unwrap();
        LogIterator::new(self.file_manager.clone(), self.current_block.clone())
    }

    fn append(&mut self, log_rec: &[u8]) -> io::Result<i32> {
        let mut boundary = self.log_page.get_int(0).expect("get_int");
        let rec_size = log_rec.len();
        let bytes_needed = rec_size + INTGER_BYTES;

        if boundary as usize - bytes_needed < INTGER_BYTES {
            self.flush()?;
            self.current_block = self.append_new_block()?;
            boundary = self.log_page.get_int(0).expect("get_int");
        }

        let rec_pos = boundary as usize - bytes_needed;
        self.log_page.set_bytes(rec_pos, log_rec).expect("set_byte");
        self.log_page.set_int(0, rec_pos as i32).expect("set_int");

        self.last_saved_log_sequence_number += 1;
        Ok(self.latest_log_sequence_number)
    }

    fn append_new_block(&mut self) -> io::Result<BlockId> {
        let block_id = self
            .file_manager
            .borrow_mut()
            .append(&self.log_file)
            .expect("append");
        self.log_page
            .set_int(0, self.file_manager.borrow_mut().block_size as i32)
            .expect("set_int");
        self.file_manager
            .borrow_mut()
            .write(&block_id, &mut self.log_page)
            .expect("write");
        Ok(block_id)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file_manager
            .borrow_mut()
            .write(&mut self.current_block, &mut self.log_page)?;
        self.last_saved_log_sequence_number = self.latest_log_sequence_number;
        Ok(())
    }
}

struct LogIterator {
    file_manager: Rc<RefCell<FileManager>>,
    block_id: BlockId,
    page: Page,
    current_pos: usize,
    boundary: usize,
}

impl LogIterator {
    pub fn new(file_manager: Rc<RefCell<FileManager>>, block_id: BlockId) -> Self {
        let buf: Vec<u8> = Vec::with_capacity(file_manager.borrow().block_size);
        let mut log_itertor = LogIterator {
            file_manager,
            block_id: block_id.clone(),
            page: Page::from(Box::from(buf)),
            current_pos: 0,
            boundary: 0,
        };

        log_itertor.move_to_block(&block_id).unwrap();
        log_itertor
    }

    fn move_to_block(&mut self, block_id: &BlockId) -> io::Result<()> {
        self.file_manager
            .borrow_mut()
            .read(block_id, &mut self.page)?;
        self.boundary = self.page.get_int(0)? as usize;
        self.current_pos = self.boundary;
        Ok(())
    }
}

impl Iterator for LogIterator {
    type Item = Box<[u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_pos >= self.file_manager.borrow().block_size
            && self.block_id.block_number <= 0
        {
            return None;
        }

        if self.current_pos == self.file_manager.borrow().block_size {
            let block_id = BlockId {
                filename: self.block_id.filename.clone(),
                block_number: self.block_id.block_number - 1,
            };
            self.move_to_block(&block_id).unwrap();
            self.block_id = block_id;
        }
        let rec = self.page.get_bytes(self.current_pos).unwrap();
        self.current_pos += INTGER_BYTES + rec.len();
        Some(rec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::Builder;

    #[test]
    fn log() {
        let directory = "./data";
        let tempfile = Builder::new().tempfile_in(directory).unwrap();
        let filename = tempfile.path().file_name().unwrap().to_str().unwrap();
        let file_manager = FileManager::new(directory.to_string());
        let mut log_manager = LogManager::new(file_manager, filename.to_string()).unwrap();

        for n in 0..35 {
            let mut buf = create_log_record(format!("record{}", n).to_string(), n);
            log_manager.append(buf.as_mut()).expect("here");
        }

        for record in log_manager.iterator() {
            let mut page = Page::from(record);
            let str = page.get_string(0).unwrap();
            let npos = Page::max_length(str.len());
            let val = page.get_int(npos).unwrap();

            assert_eq!(str.to_string(), format!("record{}", val).to_string());
        }

        fn create_log_record(str: String, n: i32) -> Vec<u8> {
            let npos = Page::max_length(str.len());
            let buf: Vec<u8> = Vec::with_capacity(npos + INTGER_BYTES);
            let mut page = Page::from(Box::from(buf));
            page.set_string(0, str).unwrap();
            page.set_int(npos, n).unwrap();
            page.contents().to_owned()
        }
    }
}
