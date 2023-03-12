use std::collections::HashMap;
use std::sync::{Arc, RwLock, Mutex};
use std::time::{Duration, Instant};

use crate::{error::LockAbortError, file_manager::BlockId};

const MAX_TIME: Duration = Duration::from_secs(10);

pub struct LockTable {
    table: HashMap<BlockId, i32>,
}

impl Default for LockTable {
    fn default() -> Self {
        Self {
            table: HashMap::new(),
        }
    }
}

impl LockTable {
    pub fn new() -> Self {
        Self::default()
    }

    fn slock(&mut self, block_id: &BlockId) -> Result<(), LockAbortError> {
        let start = Instant::now();
        while self.has_xlock(block_id) {
            if start.elapsed() > MAX_TIME {
                return Err(anyhow::anyhow!(format!("{:?}はlockされています", block_id)).into());
            }
        }

        let val = self.get_lock_val(block_id);

        self.table.insert(block_id.clone(), val + 1);
        Ok(())
    }

    fn xlock(&mut self, block_id: &BlockId) -> Result<(), LockAbortError> {
        let start = Instant::now();
        while self.has_other_slocks(block_id) {
            if start.elapsed() > MAX_TIME {
                return Err(anyhow::anyhow!(format!("{:?}はlockされています", block_id)).into());
            }
        }

        self.table.insert(block_id.clone(), -1);
        Ok(())
    }

    fn unlock(&mut self, block_id: &BlockId) {
        let ival = self.get_lock_val(block_id);
        if ival > 1 {
            self.table.insert(block_id.clone(), ival - 1);
        } else {
            self.table.remove(block_id);
        }
    }

    fn has_xlock(&self, block_id: &BlockId) -> bool {
        self.get_lock_val(block_id) < 0
    }

    fn has_other_slocks(&self, block_id: &BlockId) -> bool {
        self.get_lock_val(block_id) > 1
    }

    fn get_lock_val(&self, block_id: &BlockId) -> i32 {
        match self.table.get(block_id) {
            Some(v) => *v,
            None => 0,
        }
    }
}

pub struct ConcurrentManager {
    lock_table: Arc<Mutex<LockTable>>,
    table: HashMap<BlockId, String>,
}

impl ConcurrentManager {
    pub fn new(lock_table: Arc<Mutex<LockTable>>) -> Self {
        let table = HashMap::new();
        Self { lock_table, table }
    }

    pub fn slock(&mut self, block_id: &BlockId) -> anyhow::Result<()> {
        if self.table.get(block_id) != None {
            self.lock_table.lock().unwrap().slock(block_id)?;
            self.table.insert(block_id.clone(), "S".to_string());
        }
        Ok(())
    }

    pub fn xlock(&mut self, block_id: &BlockId) -> anyhow::Result<()> {
        if self.has_lock(block_id) {
            self.slock(block_id)?;
            self.lock_table.lock().unwrap().xlock(block_id)?;

            self.table.insert(block_id.clone(), "X".to_string());
        }
        Ok(())
    }

    pub fn release(&mut self) {
        for block_id in self.table.keys() {
            self.lock_table.lock().unwrap().unlock(block_id);
        }
        self.table.clear();
    }

    fn has_lock(&self, block_id: &BlockId) -> bool {
        match self.table.get(block_id) {
            Some(v) => v == "X",
            None => false,
        }
    }
}
