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
    file_manager: Rc<RefCell<FileManager>>,
    log_manager: Rc<RefCell<LogManager>>,
    contents: Page,
    block_id: Option<BlockId>,
    pins: i32,
    txnum: i32,
    last_save_numbder: i32,
}

impl Buffer {
    fn new(file_manager: Rc<RefCell<FileManager>>, log_manager: Rc<RefCell<LogManager>>) -> Buffer {
        let contents = Page::new(file_manager.borrow().block_size);
        Buffer {
            file_manager,
            log_manager,
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
        self.file_manager
            .borrow_mut()
            .read(&self.block_id.as_ref().unwrap(), &mut self.contents)
            .unwrap();
        self.pins = 0;
    }

    fn flush(&mut self) {
        if self.txnum >= 0 {
            self.log_manager
                .borrow_mut()
                .flush_with(self.last_save_numbder)
                .unwrap();
            self.file_manager
                .borrow_mut()
                .write(&self.block_id.as_ref().unwrap(), &mut self.contents)
                .unwrap();
            self.txnum -= 1;
        }
    }

    fn pin(&mut self) {
        self.pins += 1;
    }

    fn unpin(&mut self) {
        self.pins -= 1;
    }
}

pub struct BufferManager {
    buffer_pool: Vec<Rc<RefCell<Buffer>>>,
    num_available: i32,
}

impl BufferManager {
    pub fn new(
        file_manager: Rc<RefCell<FileManager>>,
        log_manager: Rc<RefCell<LogManager>>,
        num_buffers: i32,
    ) -> BufferManager {
        BufferManager {
            buffer_pool: (0..num_buffers)
                .map(|_| {
                    Rc::new(RefCell::new(Buffer::new(
                        file_manager.clone(),
                        log_manager.clone(),
                    )))
                })
                .collect(),
            num_available: num_buffers,
        }
    }

    pub fn available(&self) -> i32 {
        self.num_available
    }

    pub fn flush_all(&mut self, txnum: i32) {
        for buffer in self.buffer_pool.iter() {
            let mut buffer = buffer.borrow_mut();
            if buffer.modifying_tx() == txnum {
                buffer.flush()
            }
        }
    }

    pub fn unpin(&mut self, buffer: Rc<RefCell<Buffer>>) {
        buffer.borrow_mut().unpin();
        if !buffer.borrow().is_pinned() {
            self.num_available += 1;
        }
    }

    pub fn pin(&mut self, block_id: &BlockId) -> Result<Rc<RefCell<Buffer>>, BufferAbortError> {
        self.try_to_pin(block_id)
            .ok_or(BufferAbortError::BufferAbortError)
    }

    // TODO: fn wait_to_long(self) {}

    fn try_to_pin(&mut self, block_id: &BlockId) -> Option<Rc<RefCell<Buffer>>> {
        if let Some(buffer) = self.find_assignable_block(block_id) {
            if !buffer.borrow().is_pinned() {
                self.num_available -= 1;
            }
            buffer.borrow_mut().pin();
            Some(buffer)
        } else {
            None
        }
    }

    fn find_assignable_block(&self, block_id: &BlockId) -> Option<Rc<RefCell<Buffer>>> {
        self.find_existing_buffer(block_id)
            .or_else(|| match self.choose_unpinned_buffer() {
                Some(buffer) => {
                    buffer.borrow_mut().assign_to_back(block_id.clone());
                    Some(buffer)
                }
                None => None,
            })
    }

    fn find_existing_buffer(&self, target_block_id: &BlockId) -> Option<Rc<RefCell<Buffer>>> {
        self.buffer_pool
            .iter()
            .find(|buffer| {
                if let Some(block_id) = &buffer.borrow().block_id {
                    block_id.eq(target_block_id)
                } else {
                    false
                }
            })
            .and_then(|v| Some(v.clone()))
    }

    fn choose_unpinned_buffer(&self) -> Option<Rc<RefCell<Buffer>>> {
        self.buffer_pool
            .iter()
            .find(|buffer| !buffer.borrow().is_pinned())
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
        let log_manager = Rc::new(RefCell::new(
            LogManager::new(log_file_manager, log_filename.to_string()).unwrap(),
        ));

        let tempfile = Builder::new().tempfile_in(directory.to_string()).unwrap();
        let filename = tempfile.path().file_name().unwrap().to_str().unwrap();
        let file_manager = Rc::new(RefCell::new(FileManager::new(directory.to_string())));

        let mut buffer_manager = BufferManager::new(file_manager.clone(), log_manager.clone(), 3);

        let mut buffer: Vec<Rc<RefCell<Buffer>>> = Vec::with_capacity(6);
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
