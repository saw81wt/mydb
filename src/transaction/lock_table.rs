use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::{error::LockAbortError, file_manager::BlockId};

const MAX_TIME: Duration = Duration::from_secs(10);

struct LockTable {
    table: Arc<Mutex<HashMap<BlockId, i32>>>,
}

impl Default for LockTable {
    fn default() -> Self {
        Self {
            table: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl LockTable {
    fn new() -> Self {
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

        let mut locked_table = self.table.lock().unwrap();
        locked_table.insert(block_id.clone(), val + 1);
        Ok(())
    }

    fn xlock(&mut self, block_id: &BlockId) -> Result<(), LockAbortError> {
        let start = Instant::now();
        while self.has_other_slocks(block_id) {
            if start.elapsed() > MAX_TIME {
                return Err(anyhow::anyhow!(format!("{:?}はlockされています", block_id)).into());
            }
        }

        let mut locked_table = self.table.lock().unwrap();
        locked_table.insert(block_id.clone(), -1);
        Ok(())
    }

    fn unlock(&mut self, block_id: &BlockId) {
        let ival = self.get_lock_val(block_id);
        let mut locked_table = self.table.lock().unwrap();
        if ival > 1 {
            locked_table.insert(block_id.clone(), ival - 1);
        } else {
            locked_table.remove(block_id);
        }
    }

    fn has_xlock(&self, block_id: &BlockId) -> bool {
        self.get_lock_val(block_id) < 0
    }

    fn has_other_slocks(&self, block_id: &BlockId) -> bool {
        self.get_lock_val(block_id) > 1
    }

    fn get_lock_val(&self, block_id: &BlockId) -> i32 {
        let locked_table = self.table.lock().unwrap();
        match locked_table.get(block_id) {
            Some(v) => *v,
            None => 0,
        }
    }
}

struct ConcurrentManager {
    lock_table: LockTable,
    table: Arc<Mutex<HashMap<BlockId, String>>>,
}

impl ConcurrentManager {
    fn slock(&mut self, block_id: &BlockId) -> anyhow::Result<()> {
        let mut locked_table = self.table.lock().unwrap();
        if locked_table.get(block_id) != None {
            self.lock_table.slock(block_id)?;
            locked_table.insert(block_id.clone(), "S".to_string());
        }
        Ok(())
    }

    fn xlock(&mut self, block_id: &BlockId) -> anyhow::Result<()> {
        if self.has_lock(block_id) {
            self.slock(block_id)?;
            self.lock_table.xlock(block_id)?;

            let mut locked_table = self.table.lock().unwrap();
            locked_table.insert(block_id.clone(), "X".to_string());
        }
        Ok(())
    }

    fn release(&mut self) {
        let mut locked_table = self.table.lock().unwrap();
        for block_id in locked_table.keys() {
            self.lock_table.unlock(block_id);
        }
        locked_table.clear();
    }

    fn has_lock(&self, block_id: &BlockId) -> bool {
        let locked_table = self.table.lock().unwrap();
        match locked_table.get(block_id) {
            Some(v) => v == "X",
            None => false,
        }
    }
}
