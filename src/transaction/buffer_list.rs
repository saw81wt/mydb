use crate::buffer_manager::{Buffer, BufferManager};
use crate::file_manager::BlockId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

pub struct BufferList {
    buffers: HashMap<BlockId, Arc<RwLock<Buffer>>>,
    pins: HashMap<BlockId, i32>,
    buffer_manager: Arc<Mutex<BufferManager>>,
}

impl BufferList {
    pub fn new(buffer_manager: Arc<Mutex<BufferManager>>) -> Self {
        let buffers = HashMap::new();
        let pins = HashMap::new();
        Self {
            buffers,
            pins,
            buffer_manager,
        }
    }

    pub fn get_buffer(&self, block_id: &BlockId) -> Option<&Arc<RwLock<Buffer>>> {
        return self.buffers.get(block_id);
    }

    pub fn pin(&mut self, block_id: &BlockId) -> anyhow::Result<()> {
        let mut locked_buffer_manager = self.buffer_manager.lock().unwrap();
        let buffer = locked_buffer_manager.pin(block_id)?;
        self.buffers.insert(block_id.clone(), Arc::clone(&buffer));
        let val = match self.pins.get(block_id) {
            Some(v) => *v,
            None => 0,
        };
        self.pins.insert(block_id.clone(), val + 1);
        Ok(())
    }

    pub fn unpin(&mut self, block_id: &BlockId) -> anyhow::Result<()> {
        let buffer = self.buffers.get(block_id).unwrap();

        let mut locked_buffer_manager = self.buffer_manager.lock().unwrap();
        locked_buffer_manager.unpin(Arc::clone(buffer));

        let val = match self.pins.get(block_id) {
            Some(v) => *v,
            None => return Err(anyhow::anyhow!("Unpin Error").into()),
        };

        if val != 1 {
            self.buffers.remove(block_id);
            self.pins.remove(block_id);
        } else {
            self.pins.insert(block_id.clone(), val - 1);
        }

        Ok(())
    }

    pub fn unpin_all(&mut self) -> anyhow::Result<()> {
        for block in self.pins.keys() {
            match self.buffers.get(block) {
                Some(buffer) => {
                    let mut locked_buffer_manager = self.buffer_manager.lock().unwrap();
                    locked_buffer_manager.unpin(Arc::clone(buffer));
                }
                None => {}
            }
        }
        self.buffers.clear();
        self.pins.clear();
        Ok(())
    }
}
