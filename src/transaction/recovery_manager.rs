use std::sync::{Arc, Mutex, RwLock};

use crate::{
    buffer_manager::{Buffer, BufferManager},
    file_manager::Page,
    log_manager::LogManager,
};

use super::{
    log_record::{LogRecord, LogRecordTrait},
    transaction::Transaction,
};

struct RecoveryManager {
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
    transaction: Transaction,
    txnum: i32,
}

impl RecoveryManager {
    pub fn new(
        log_manager: Arc<Mutex<LogManager>>,
        buffer_manager: Arc<Mutex<BufferManager>>,
        transaction: Transaction,
        txnum: i32,
    ) -> Self {
        let record = LogRecord::create_start_record(txnum);
        record.write_to_log(Arc::clone(&log_manager));
        Self {
            log_manager,
            buffer_manager,
            transaction,
            txnum,
        }
    }

    pub fn commit(&self) {
        self.buffer_manager.lock().unwrap().flush_all(self.txnum);
        let record = LogRecord::create_commit_record(self.txnum);
        let lsm = record.write_to_log(Arc::clone(&self.log_manager));
        self.log_manager.lock().unwrap().flush_with(lsm).unwrap();
    }

    pub fn rollback(&self) {
        self.do_rollback();
        self.buffer_manager.lock().unwrap().flush_all(self.txnum);
        let record = LogRecord::create_rollback_record(self.txnum);
        let lsm = record.write_to_log(Arc::clone(&self.log_manager));
        self.log_manager.lock().unwrap().flush_with(lsm).unwrap();
    }

    pub fn recover(&self) {
        self.do_recevery();
        self.buffer_manager.lock().unwrap().flush_all(self.txnum);
        let record = LogRecord::create_checkpoint_record(self.txnum);
        let lsm = record.write_to_log(Arc::clone(&self.log_manager));
        self.log_manager.lock().unwrap().flush_with(lsm).unwrap();
    }

    pub fn set_int(&self, buf: Arc<RwLock<Buffer>>, offset: i32) -> i32 {
        let mut locked_buffer = buf.write().unwrap();
        let old_value = locked_buffer.get_int(offset as usize).unwrap();
        let block_id = (*locked_buffer.block_id()).as_ref().unwrap();
        let record =
            LogRecord::create_set_int_record(self.txnum, offset, old_value, block_id.clone());
        record.write_to_log(Arc::clone(&self.log_manager))
    }

    pub fn set_string(&self, buf: Arc<RwLock<Buffer>>, offset: i32) -> i32 {
        let mut locked_buffer = buf.write().unwrap();
        let old_value = locked_buffer.get_string(offset as usize).unwrap();
        let block_id = (*locked_buffer.block_id()).as_ref().unwrap();
        let record =
            LogRecord::create_set_string_record(self.txnum, offset, old_value, block_id.clone());
        record.write_to_log(Arc::clone(&self.log_manager))
    }

    fn do_rollback(&self) {
        let iter = self.log_manager.lock().unwrap().iterator().unwrap();
        for record in iter {
            let mut page = Page::from(record);
            let log_record = LogRecord::from(&mut page);
            if log_record.get_txnum() == self.txnum {
                match log_record {
                    LogRecord::Start(_) => return,
                    _ => {
                        log_record.undo(&self.transaction);
                    }
                }
            }
        }
    }

    fn do_recevery(&self) {
        let mut finished_transactions: Vec<i32> = vec![];
        let iter = self.log_manager.lock().unwrap().iterator().unwrap();
        for record in iter {
            let mut page = Page::from(record);
            let log_record = LogRecord::from(&mut page);
            let txnum = log_record.get_txnum();
            match log_record {
                LogRecord::CheckPoint(_) => return,
                LogRecord::Commit(_) | LogRecord::Rollback(_) => finished_transactions.push(txnum),
                _ => {
                    if !finished_transactions.contains(&txnum) {
                        log_record.undo(&self.transaction)
                    }
                }
            }
        }
    }
}
