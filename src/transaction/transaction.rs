use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Ok, Result};

use crate::buffer_manager::BufferManager;
use crate::file_manager::file_manager::{Page, BlockId, FileManager};
use crate::log_manager::LogManager;

use super::buffer_list::BufferList;
use super::lock_table::{ConcurrentManager, LockTable};
use super::log_record::{LogRecord, LogRecordTrait};
use super::recovery_manager::RecoveryManager;

static TXMUN: AtomicUsize = AtomicUsize::new(0);

pub struct Transaction {
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
    recovery_manager: RecoveryManager,
    concurrent_manager: ConcurrentManager,
    buffer_list: BufferList,
    txnum: usize,
}

impl Transaction {
    pub fn new(
        file_manager: Arc<Mutex<FileManager>>,
        log_manager: Arc<Mutex<LogManager>>,
        buffer_manager: Arc<Mutex<BufferManager>>,
        lock_table: Arc<Mutex<LockTable>>,
    ) -> Self {
        TXMUN.fetch_add(1, Ordering::SeqCst);
        let txnum = TXMUN.load(Ordering::SeqCst);
        let recovery_manager = RecoveryManager::new(
            Arc::clone(&log_manager),
            Arc::clone(&buffer_manager),
            txnum as i32,
        );
        Self {
            file_manager,
            log_manager,
            buffer_manager: Arc::clone(&buffer_manager),
            recovery_manager,
            concurrent_manager: ConcurrentManager::new(lock_table),
            buffer_list: BufferList::new(Arc::clone(&buffer_manager)),
            txnum,
        }
    }

    pub fn commit(&mut self) -> anyhow::Result<()> {
        self.recovery_manager.commit();
        self.concurrent_manager.release();
        self.buffer_list.unpin_all()?;
        Ok(())
    }

    pub fn rollback(&mut self) -> anyhow::Result<()> {
        let iter = self.log_manager.lock().unwrap().iterator().unwrap();

        for record in iter {
            let mut page = Page::from(record);
            let log_record = LogRecord::try_from(&mut page).unwrap();
            if log_record.get_txnum() == self.txnum as i32 {
                match log_record {
                    LogRecord::Start(_) => return Ok(()),
                    _ => {}
                }
                self.undo(log_record);
            }
        }
        self.recovery_manager.rollback();
        self.concurrent_manager.release();
        self.buffer_list.unpin_all()?;
        Ok(())
    }

    pub fn recover(&mut self) {
        {
            let mut locked_buffer_manager = self.buffer_manager.lock().unwrap();
            locked_buffer_manager.flush_all(self.txnum as i32);
        }

        let mut finished_transactions: Vec<i32> = vec![];
        let iter = self.log_manager.lock().unwrap().iterator().unwrap();
        for record in iter {
            let mut page = Page::from(record);
            let log_record = LogRecord::try_from(&mut page).unwrap();
            let txnum = log_record.get_txnum();
            match log_record {
                LogRecord::CheckPoint(_) => return,
                LogRecord::Commit(_) | LogRecord::Rollback(_) => finished_transactions.push(txnum),
                _ => {}
            }
            if !finished_transactions.contains(&txnum) {
                self.undo(log_record);
            }
        }
    }

    pub fn pin(&mut self, block_id: &BlockId) -> anyhow::Result<()> {
        self.buffer_list.pin(block_id)
    }

    pub fn unpin(&mut self, block_id: &BlockId) -> anyhow::Result<()> {
        self.buffer_list.unpin(block_id)
    }

    pub fn get_int(&mut self, block_id: &BlockId, offset: i32) -> anyhow::Result<i32> {
        self.concurrent_manager.slock(block_id)?;
        let buffer = match self.buffer_list.get_buffer(block_id) {
            Some(b) => Arc::clone(b),
            None => todo!(),
        };
        let mut locked_buffer = buffer.write().unwrap();
        locked_buffer.get_int(offset as usize).context("get int")
    }

    pub fn get_string(&mut self, block_id: &BlockId, offset: i32) -> anyhow::Result<String> {
        self.concurrent_manager.slock(block_id)?;
        let buffer = match self.buffer_list.get_buffer(block_id) {
            Some(b) => b,
            None => todo!(),
        };
        let mut locked_buffer = buffer.write().unwrap();
        locked_buffer
            .get_string(offset as usize)
            .context("get string")
    }

    pub fn set_int(
        &mut self,
        block_id: &BlockId,
        offset: i32,
        val: i32,
        ok_to_log: bool,
    ) -> anyhow::Result<()> {
        self.concurrent_manager.xlock(block_id)?;
        let buffer = self
            .buffer_list
            .get_buffer(block_id)
            .context("buffer none")?;
        let mut lsn = -1;
        if ok_to_log {
            lsn = self.recovery_manager.set_int(Arc::clone(buffer), offset);
        }
        let mut locked_buffer = buffer.write().unwrap();
        locked_buffer.set_int(offset as usize, val)?;
        locked_buffer.set_modified(self.txnum as i32, lsn);
        Ok(())
    }

    pub fn set_string(
        &mut self,
        block_id: &BlockId,
        offset: i32,
        val: String,
        ok_to_log: bool,
    ) -> anyhow::Result<()> {
        self.concurrent_manager.xlock(block_id)?;
        let buffer = self
            .buffer_list
            .get_buffer(block_id)
            .context("buffer none")?;
        let mut lsn = -1;
        if ok_to_log {
            lsn = self.recovery_manager.set_string(Arc::clone(buffer), offset);
        }
        let mut locked_buffer = buffer.write().unwrap();
        locked_buffer.set_string(offset as usize, val)?;
        locked_buffer.set_modified(self.txnum as i32, lsn);
        Ok(())
    }

    pub fn size(&mut self, filename: String) -> anyhow::Result<i32> {
        let dummy = BlockId {
            filename: filename.clone(),
            block_number: -1,
        };
        self.concurrent_manager.slock(&dummy)?;
        let mut locked_fm = self.file_manager.lock().unwrap();
        locked_fm.length(&filename)
    }

    pub fn append(&mut self, filename: String) -> Result<BlockId> {
        let dummy = BlockId {
            filename: filename.clone(),
            block_number: -1,
        };
        self.concurrent_manager.xlock(&dummy);
        let mut locked_filemanager = self.file_manager.lock().unwrap();
        locked_filemanager.append_new_block(&filename).context("append new block")
    }

    pub fn undo(&mut self, log_record: LogRecord) {
        match log_record {
            LogRecord::CheckPoint(record)
            | LogRecord::Commit(record)
            | LogRecord::Start(record)
            | LogRecord::Rollback(record) => {
                todo!()
            }
            LogRecord::SetInt(record) => {
                self.pin(&record.block_id);
                self.set_int(&record.block_id, record.offset, record.value, false);
                self.unpin(&record.block_id);
            }
            LogRecord::SetString(record) => {
                self.pin(&record.block_id);
                self.set_string(&record.block_id, record.offset, record.value, false);
                self.unpin(&record.block_id);
            }
        }
    }

    pub fn block_size(&self) -> usize {
        self.file_manager.lock().unwrap().block_size
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

        let buffer_manager = Arc::new(Mutex::new(BufferManager::new(
            file_manager.clone(),
            log_manager.clone(),
            3,
        )));

        let lock_table = Arc::new(Mutex::new(LockTable::new()));

        let mut tx1 = Transaction::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            Arc::clone(&buffer_manager),
            Arc::clone(&lock_table),
        );

        let block = BlockId {
            filename: filename.clone().to_string(),
            block_number: 1,
        };

        tx1.pin(&block);
        tx1.set_int(&block, 80, 1, false);
        tx1.set_string(&block, 40, "one".to_string(), false);
        tx1.commit();

        let mut tx2 = Transaction::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            Arc::clone(&buffer_manager),
            Arc::clone(&lock_table),
        );
        tx2.pin(&block);
        let ival = tx2.get_int(&block, 80).unwrap();
        let sval = tx2.get_string(&block, 40).unwrap();

        let new_ival = ival + 1;
        let new_sval = sval + "!";
        tx2.set_int(&block, 80, new_ival, false);
        tx2.set_string(&block, 40, new_sval.to_string(), false);
        tx2.commit();

        let mut tx3 = Transaction::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            Arc::clone(&buffer_manager),
            Arc::clone(&lock_table),
        );
        tx3.pin(&block);
        tx3.set_int(&block, 80, 9999, false);
        tx3.rollback();

        let mut tx4 = Transaction::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            Arc::clone(&buffer_manager),
            Arc::clone(&lock_table),
        );
        tx4.pin(&block);
        let rval = tx4.get_int(&block, 80);
        tx4.commit();
    }
}
