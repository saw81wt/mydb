use std::{
    io,
    sync::{Arc, Mutex},
};

use crate::file_manager::file_manager::{BlockId, FileManager, Page, INTGER_BYTES};

pub struct LogManager {
    file_manager: Arc<Mutex<FileManager>>,
    log_file: String,
    log_page: Page,
    current_block: BlockId,
    latest_log_sequence_number: i32,
    last_saved_log_sequence_number: i32,
}

impl LogManager {
    pub fn new(mut file_manager: FileManager, log_file: String) -> io::Result<Self> {
        let log_size = file_manager.last_block_num(&log_file)?;
        let mut log_page = Page::new(file_manager.block_size);

        let current_block = if log_size == 0 {
            let block_id = file_manager.append_new_block(&log_file)?;
            log_page.set_int(0, file_manager.block_size as i32)?;
            file_manager.write(&block_id, &mut log_page)?;
            block_id
        } else {
            BlockId {
                filename: log_file.clone(),
                block_number: (log_size - 1) as i32,
            }
        };
        Ok(LogManager {
            file_manager: Arc::new(Mutex::new(file_manager)),
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

    pub fn iterator(&mut self) -> io::Result<LogIterator> {
        self.flush().unwrap();
        LogIterator::new(self.file_manager.clone(), self.current_block.clone())
    }

    pub fn append_record(&mut self, log_record: &[u8]) -> io::Result<i32> {
        //
        let mut boundary = self.get_boundary();
        let record_size = log_record.len();
        let bytes_needed = record_size + INTGER_BYTES;

        if (boundary - bytes_needed as i32) < (INTGER_BYTES as i32) {
            self.flush()?;
            self.current_block = self.append_new_block()?;
            boundary = self.get_boundary();
        }

        let record_pos = boundary as usize - bytes_needed;
        self.log_page.set_bytes(record_pos, log_record)?;
        self.log_page.set_int(0, record_pos as i32)?;

        self.last_saved_log_sequence_number += 1;
        Ok(self.last_saved_log_sequence_number)
    }

    fn append_new_block(&mut self) -> io::Result<BlockId> {
        self.log_page = Page::new(self.file_manager.lock().unwrap().block_size);
        let block_id = self
            .file_manager
            .lock()
            .unwrap()
            .append_new_block(&self.log_file)?;
        self.set_boundary();
        self.file_manager
            .lock()
            .unwrap()
            .write(&block_id, &mut self.log_page)?;
        Ok(block_id)
    }

    // log pageの最初(offset = 0)には境界のサイズが格納されている
    fn get_boundary(&mut self) -> i32 {
        self.log_page.get_int(0).expect("get boundary")
    }

    fn set_boundary(&mut self) {
        self.log_page
            .set_int(0, self.file_manager.lock().unwrap().block_size as i32)
            .expect("set boundary")
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file_manager
            .lock()
            .unwrap()
            .write(&mut self.current_block, &mut self.log_page)?;
        self.last_saved_log_sequence_number = self.latest_log_sequence_number;
        Ok(())
    }
}

pub struct LogIterator {
    file_manager: Arc<Mutex<FileManager>>,
    block_id: BlockId,
    page: Page,
    current_pos: usize,
    boundary: usize,
}

impl LogIterator {
    pub fn new(file_manager: Arc<Mutex<FileManager>>, block_id: BlockId) -> io::Result<Self> {
        let buf: Vec<u8> = Vec::with_capacity(file_manager.lock().unwrap().block_size);
        let mut log_itertor = LogIterator {
            file_manager,
            block_id: block_id.clone(),
            page: Page::from(Box::from(buf)),
            current_pos: 0,
            boundary: 0,
        };

        log_itertor.move_to_block(&block_id)?;
        Ok(log_itertor)
    }

    fn move_to_block(&mut self, block_id: &BlockId) -> io::Result<()> {
        self.page = Page::new(self.file_manager.lock().unwrap().block_size);
        self.file_manager
            .lock()
            .unwrap()
            .read(block_id, &mut self.page)?;
        self.boundary = self.page.get_int(0)? as usize;
        self.current_pos = self.boundary;
        Ok(())
    }
}

impl Iterator for LogIterator {
    type Item = Box<[u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_pos >= self.file_manager.lock().unwrap().block_size
            && self.block_id.block_number <= 0
        {
            return None;
        }

        if self.current_pos == self.file_manager.lock().unwrap().block_size {
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
            log_manager.append_record(buf.as_mut()).unwrap();
        }

        for record in log_manager.iterator().unwrap() {
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
