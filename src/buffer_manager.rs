use std::sync::{Arc, Mutex, RwLock};
use std::{cell::RefCell, rc::Rc};
use thiserror::Error;

use crate::{
    file_manager::{BlockId, FileManager, Page},
    log_manager::LogManager,
};

pub const MAX_TIME: i32 = 10000;

#[derive(Error, Debug)]
pub enum BufferAbortError {
    #[error("Buffer Abort Error")]
    BufferAbortError,
}

pub struct Buffer {
    contents: Page,
    block_id: Option<BlockId>,
    pins: i32,
    txnum: i32,
    last_save_numbder: i32,
}

impl Buffer {
    fn new(block_size: usize) -> Buffer {
        let contents = Page::new(block_size);
        Buffer {
            contents,
            block_id: None,
            pins: 0,
            txnum: -1,
            last_save_numbder: -1,
        }
    }

    pub fn contents(&self) -> &Page {
        &self.contents
    }

    pub fn block_id(&self) -> &Option<BlockId> {
        &self.block_id
    }

    pub fn set_modified(&mut self, txnum: i32, last_save_number: i32) {
        self.txnum = txnum;
        if last_save_number >= 0 {
            self.last_save_numbder = last_save_number
        }
    }

    pub fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    pub fn modifying_tx(&self) -> i32 {
        self.txnum
    }

    fn assign_to_back(&mut self, block_id: BlockId) {
        self.flush();
        self.block_id = Some(block_id);
        self.pins = 0;
    }

    fn flush(&mut self) {}

    fn pin(&mut self) {
        self.pins += 1;
    }

    fn unpin(&mut self) {
        self.pins -= 1;
    }
}

pub struct BufferManager {
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_pool: Mutex<Vec<Arc<RwLock<Buffer>>>>,
    num_available: i32,
}

impl BufferManager {
    pub fn new(
        file_manager: Arc<Mutex<FileManager>>,
        log_manager: Arc<Mutex<LogManager>>,
        num_buffers: i32,
    ) -> BufferManager {
        let block_size = file_manager.lock().unwrap().block_size;
        BufferManager {
            file_manager: Arc::clone(&file_manager),
            log_manager: Arc::clone(&log_manager),
            buffer_pool: Mutex::new(
                (0..num_buffers)
                    .map(|_| Arc::new(RwLock::new(Buffer::new(block_size))))
                    .collect(),
            ),
            num_available: num_buffers,
        }
    }

    pub fn available(&self) -> i32 {
        self.num_available
    }

    pub fn flush_all(&mut self, txnum: i32) {
        let locked_pool = self.buffer_pool.lock().unwrap();
        for buffer in locked_pool.iter() {
            let mut buffer = buffer.write().unwrap();
            if buffer.modifying_tx() == txnum {
                if buffer.txnum >= 0 {
                    self.log_manager
                        .lock()
                        .unwrap()
                        .flush_with(buffer.last_save_numbder)
                        .unwrap();
                    self.file_manager
                        .lock()
                        .unwrap()
                        .write(&buffer.block_id.clone().unwrap(), &mut buffer.contents)
                        .unwrap();
                    buffer.txnum -= 1;
                }
            }
        }
    }

    pub fn unpin(&mut self, buffer: Arc<RwLock<Buffer>>) {
        buffer.write().unwrap().unpin();
        if !buffer.write().unwrap().is_pinned() {
            self.num_available += 1;
        }
    }

    pub fn pin(&mut self, block_id: &BlockId) -> Result<Arc<RwLock<Buffer>>, BufferAbortError> {
        self.try_to_pin(block_id)
            .ok_or(BufferAbortError::BufferAbortError)
    }

    // TODO: fn wait_to_long(self) {}

    fn try_to_pin(&mut self, block_id: &BlockId) -> Option<Arc<RwLock<Buffer>>> {
        if let Some(buffer) = self.find_assignable_block(block_id) {
            if !buffer.write().unwrap().is_pinned() {
                self.num_available -= 1;
            }
            buffer.write().unwrap().pin();
            Some(buffer)
        } else {
            None
        }
    }

    fn find_assignable_block(&self, block_id: &BlockId) -> Option<Arc<RwLock<Buffer>>> {
        self.find_existing_buffer(block_id)
            .or_else(|| match self.choose_unpinned_buffer() {
                Some(buffer) => {
                    buffer.write().unwrap().assign_to_back(block_id.clone());
                    self.file_manager
                        .lock()
                        .unwrap()
                        .read(&block_id, &mut buffer.write().unwrap().contents)
                        .unwrap();
                    Some(buffer)
                }
                None => None,
            })
    }

    fn find_existing_buffer(&self, target_block_id: &BlockId) -> Option<Arc<RwLock<Buffer>>> {
        let locked_pool = self.buffer_pool.lock().unwrap();
        locked_pool
            .iter()
            .find(|buffer| {
                if let Some(block_id) = buffer.write().unwrap().block_id.clone() {
                    block_id.eq(target_block_id)
                } else {
                    false
                }
            })
            .and_then(|v| Some(v.clone()))
    }

    fn choose_unpinned_buffer(&self) -> Option<Arc<RwLock<Buffer>>> {
        self.buffer_pool
            .lock()
            .unwrap()
            .iter()
            .find(|buffer| !buffer.write().unwrap().is_pinned())
            .and_then(|v| Some(v.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::Builder;

    #[test]
    fn buffer_manager() {
        let directory = "./data";
        let log_tempfile = Builder::new().tempfile_in(directory.to_string()).unwrap();
        let log_filename = log_tempfile.path().file_name().unwrap().to_str().unwrap();
        let log_file_manager = FileManager::new(directory.to_string());
        let log_manager = Arc::new(Mutex::new(
            LogManager::new(log_file_manager, log_filename.to_string()).unwrap(),
        ));

        let tempfile = Builder::new().tempfile_in(directory.to_string()).unwrap();
        let filename = tempfile.path().file_name().unwrap().to_str().unwrap();
        let file_manager = Arc::new(Mutex::new(FileManager::new(directory.to_string())));

        let mut buffer_manager =
            BufferManager::new(Arc::clone(&file_manager), Arc::clone(&log_manager), 3);

        let mut buffer: Vec<Arc<RwLock<Buffer>>> = Vec::with_capacity(6);
        let block_id_0 = BlockId {
            filename: filename.to_string(),
            block_number: 0,
        };
        let block_id_1 = BlockId {
            filename: filename.to_string(),
            block_number: 1,
        };
        let block_id_2 = BlockId {
            filename: filename.to_string(),
            block_number: 2,
        };
        let block_id_3 = BlockId {
            filename: filename.to_string(),
            block_number: 3,
        };

        buffer.insert(0, buffer_manager.pin(&block_id_0).unwrap());
        buffer.insert(1, buffer_manager.pin(&block_id_1).unwrap());
        buffer.insert(2, buffer_manager.pin(&block_id_2).unwrap());

        buffer_manager.unpin(buffer.get(1).unwrap().clone());

        buffer.insert(3, buffer_manager.pin(&block_id_0).unwrap());
        buffer.insert(4, buffer_manager.pin(&block_id_1).unwrap());

        assert_eq!(buffer_manager.available(), 0);

        assert!(buffer_manager.pin(&block_id_3).is_err());

        buffer_manager.unpin(buffer.get(2).unwrap().clone());

        buffer.insert(5, buffer_manager.pin(&block_id_3).unwrap());
    }
}
