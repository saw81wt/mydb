use std::sync::{Arc, Mutex};

use crate::buffer_manager::BufferManager;
use crate::file_manager::{BlockId, FileManager};
use crate::log_manager::LogManager;

pub struct Transaction {
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
}

impl Transaction {
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

    fn pin(&self) {}
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
        let tx1 = Transaction::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            Arc::new(Mutex::new(buffer_manager)),
        );

        let block = BlockId {
            filename: filename.clone().to_string(),
            block_number: 1,
        };

        tx1.pin(block);
        tx1.setInt(block, 80, 1, false);
        tx1.setString(block, 40, "one", false);
        tx1.commit();

        let tx2 = Transaction::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            Arc::new(Mutex::new(buffer_manager)),
        );
        tx2.pin(block);
        let ival = tx2.getInt(block, 80);
        let sval = tx2.getString(block, 40);

        let new_ival = ival + 1;
        let new_sval = stal + "!";
        tx2.setInt(block, 80);
        tx2.setString(block, 40);
        tx2.commit();

        let tx3 = Transaction::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            Arc::new(Mutex::new(buffer_manager)),
        );
        tx3.pin();
        tx3.setInt(block, 80, 9999, true);
        tx3.rollback();

        let tx4 = Transaction::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            Arc::new(Mutex::new(buffer_manager)),
        );
        tx4.pin();
        let rval = tx4.getInt(block, 80);
        tx4.commit();
    }
}
