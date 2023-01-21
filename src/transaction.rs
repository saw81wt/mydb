use std::sync::{Arc, Mutex};

use crate::buffer_manager::BufferManager;
use crate::file_manager::{BlockId, FileManager};
use crate::log_manager::LogManager;

struct Transaction {}

struct TransactionManager {
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
}

impl TransactionManager {
    pub fn new(
        file_manager: Arc<Mutex<FileManager>>,
        log_manager: Arc<Mutex<LogManager>>,
        buffer_manager: Arc<Mutex<BufferManager>>,
    ) -> Self {
        Self {
            file_manager,
            log_manager,
            buffer_manager,
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;

    #[test]
    fn transaction() {
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

        let mut buffer_manager = BufferManager::new(file_manager.clone(), log_manager.clone(), 3);
        let transaction_manager = TransactionManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            Arc::new(Mutex::new(buffer_manager)),
        );

        let tx1 = Transaction {};
        let block = BlockId {
            filename: filename.clone().to_string(),
            block_number: 1,
        };

        tx1.pin(block);
        tx1.setInt(block, 80, 1, false);
        tx1.setString(block, 40, "one", false);
        tx1.commit();
    }
}
