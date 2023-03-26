use crate::{
    buffer_manager::BufferManager,
    file_manager::file_manager::FileManager,
    log_manager::LogManager,
    transaction::{lock_table::LockTable, transaction::Transaction},
};
use std::sync::{Arc, Mutex};

// todo: make this configurable
const FILE_DIR: &str = "data";
const LOG_FILE: &str = "logfile";

pub struct MyDb {
    name: String,
    block_size: usize,
    buffer_pool_size: usize,
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    lock_table: Arc<Mutex<LockTable>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
}

impl MyDb {
    pub fn new(name: String, block_size: usize, buffer_pool_size: usize) -> Self {
        let file_manager = Arc::new(Mutex::new(FileManager::new(FILE_DIR.to_string())));
        let log_file_manager = FileManager::new(FILE_DIR.to_string());
        let log_manager = Arc::new(Mutex::new(
            LogManager::new(log_file_manager, LOG_FILE.to_string()).unwrap(),
        ));
        let lock_table = Arc::new(Mutex::new(LockTable::new()));
        let buffer_manager = Arc::new(Mutex::new(BufferManager::new(
            file_manager.clone(),
            log_manager.clone(),
            buffer_pool_size as i32,
        )));

        Self {
            name,
            block_size,
            buffer_pool_size,
            file_manager,
            log_manager: Arc::clone(&log_manager),
            lock_table,
            buffer_manager,
        }
    }

    pub fn new_transaction(&self) -> Transaction {
        Transaction::new(
            Arc::clone(&self.file_manager),
            Arc::clone(&self.log_manager),
            Arc::clone(&self.buffer_manager),
            Arc::clone(&self.lock_table),
        )
    }
}
