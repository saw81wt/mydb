use std::sync::{Arc, Mutex};

use crate::{buffer_manager::BufferManager, log_manager::LogManager};

use super::{log_record::LogRecord, transaction::Transaction};

struct RecoveryManager {
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
    transaction: Transaction,
    txnum: i32,
}

impl RecoveryManager {
    fn new(
        log_manager: Arc<Mutex<LogManager>>,
        buffer_manager: Arc<Mutex<BufferManager>>,
        transaction: Transaction,
        txnum: i32,
    ) -> Self {
        Self {
            log_manager,
            buffer_manager,
            transaction,
            txnum,
        }
    }

    fn commit(&self) {
        self.buffer_manager.lock().unwrap().flush_all(self.txnum);
        let record = LogRecord::create_commit_record(self.txnum);
        let lsm = record.write_to_log(Arc::clone(&self.log_manager));
        self.log_manager.lock().unwrap().flush_with(lsm).unwrap();
    }
}
