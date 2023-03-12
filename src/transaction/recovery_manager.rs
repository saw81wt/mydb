use std::sync::{Arc, Mutex, RwLock};

use crate::{
    buffer_manager::{Buffer, BufferManager},
    file_manager::Page,
    log_manager::LogManager,
};

use super::{
    log_record::{LogRecord, LogRecordTrait},
    transaction::{self, Transaction},
};

pub struct RecoveryManager {
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
    //transaction: Transaction,
    txnum: i32,
}

impl RecoveryManager {
    pub fn new(
        log_manager: Arc<Mutex<LogManager>>,
        buffer_manager: Arc<Mutex<BufferManager>>,
        //transaction: Transaction,
        txnum: i32,
    ) -> Self {
        let record = LogRecord::create_start_record(txnum);
        let mut page: Page = record.into();
        log_manager.lock().unwrap().append_record(page.contents()).unwrap();
        Self {
            log_manager,
            buffer_manager,
            //transaction,
            txnum,
        }
    }

    pub fn commit(&self) {
        self.buffer_manager.lock().unwrap().flush_all(self.txnum);
        let record = LogRecord::create_commit_record(self.txnum);
        let mut page: Page = record.into();
        let lsm = self.log_manager.lock().unwrap().append_record(page.contents()).unwrap();
        self.log_manager.lock().unwrap().flush_with(lsm).unwrap();
    }

    pub fn rollback(&self, transaction: &Transaction) {
        self.do_rollback(transaction);
        self.buffer_manager.lock().unwrap().flush_all(self.txnum);
        let record = LogRecord::create_rollback_record(self.txnum);
        let mut page: Page = record.into();
        let lsm = self.log_manager.lock().unwrap().append_record(page.contents()).unwrap();
        self.log_manager.lock().unwrap().flush_with(lsm).unwrap();
    }

    pub fn recover(&self, transaction: &Transaction) {
        self.do_recevery(transaction);
        self.buffer_manager.lock().unwrap().flush_all(self.txnum);
        let record = LogRecord::create_checkpoint_record(self.txnum);
        let mut page: Page = record.into();
        let lsm = self.log_manager.lock().unwrap().append_record(page.contents()).unwrap();
        self.log_manager.lock().unwrap().flush_with(lsm).unwrap();
    }

    pub fn set_int(&self, buf: Arc<RwLock<Buffer>>, offset: i32) -> i32 {
        let mut locked_buffer = buf.write().unwrap();
        let old_value = locked_buffer.get_int(offset as usize).unwrap();
        let block_id = (*locked_buffer.block_id()).as_ref().unwrap();
        let record =
            LogRecord::create_set_int_record(self.txnum, offset, old_value, block_id.clone());
        let mut page: Page = record.into();
        self.log_manager.lock().unwrap().append_record(page.contents()).unwrap()
    }

    pub fn set_string(&self, buf: Arc<RwLock<Buffer>>, offset: i32) -> i32 {
        let mut locked_buffer = buf.write().unwrap();
        let old_value = locked_buffer.get_string(offset as usize).unwrap();
        let block_id = (*locked_buffer.block_id()).as_ref().unwrap();
        let record =
            LogRecord::create_set_string_record(self.txnum, offset, old_value, block_id.clone());
        let mut page: Page = record.into();
        self.log_manager.lock().unwrap().append_record(page.contents()).unwrap()
    }

    fn do_rollback(&self, transaction: &Transaction) {
        let iter = self.log_manager.lock().unwrap().iterator().unwrap();
        for record in iter {
            let mut page = Page::from(record);
            let log_record = LogRecord::try_from(&mut page).unwrap();
            if log_record.get_txnum() == self.txnum {
                match log_record {
                    LogRecord::Start(_) => return,
                    _ => {
                        log_record.undo(transaction);
                    }
                }
            }
        }
    }

    fn do_recevery(&self, transaction: &Transaction) {
        let mut finished_transactions: Vec<i32> = vec![];
        let iter = self.log_manager.lock().unwrap().iterator().unwrap();
        for record in iter {
            let mut page = Page::from(record);
            let log_record = LogRecord::try_from(&mut page).unwrap();
            let txnum = log_record.get_txnum();
            match log_record {
                LogRecord::CheckPoint(_) => return,
                LogRecord::Commit(_) | LogRecord::Rollback(_) => finished_transactions.push(txnum),
                _ => {
                    if !finished_transactions.contains(&txnum) {
                        log_record.undo(transaction)
                    }
                }
            }
        }
    }
}
