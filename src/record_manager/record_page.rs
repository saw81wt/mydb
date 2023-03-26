use crate::transaction::transaction::Transaction;
use crate::{file_manager::file_manager::BlockId, record_manager::layout::Layout};
use anyhow::{Context, Result};

// if the slot is empty, the first 4 bytes will be 0
const EMPTY_FLAG: i32 = 0;
// if the slot is used, the first 4 bytes will be 1
const USED_FLAG: i32 = 1;

pub struct RecordPage {
    pub layout: Layout,
    pub transaction: Transaction,
    block_id: BlockId,
}

impl RecordPage {
    pub fn new(layout: Layout, transaction: Transaction, block_id: BlockId) -> Self {
        // transaction.pin(&block_id);
        Self {
            layout,
            transaction,
            block_id,
        }
    }

    pub fn get_int(&mut self, slot_id: usize, field_name: String) -> Result<i32> {
        let field_pos = self.field_pos(slot_id, field_name);
        self.transaction.get_int(&self.block_id, field_pos as i32)
    }

    pub fn get_string(&mut self, slot_id: usize, field_name: String) -> Result<String> {
        let field_pos = self.field_pos(slot_id, field_name);
        self.transaction
            .get_string(&self.block_id, field_pos as i32)
    }

    pub fn set_int(&mut self, slot_id: usize, field_name: String, value: i32) -> Result<()> {
        let field_pos = self.field_pos(slot_id, field_name);
        self.transaction
            .set_int(&self.block_id, field_pos as i32, value, true)
    }

    pub fn set_string(&mut self, slot_id: usize, field_name: String, value: String) -> Result<()> {
        let field_pos = self.field_pos(slot_id, field_name);
        self.transaction
            .set_string(&self.block_id, field_pos as i32, value, true)
    }

    fn field_pos(&self, slot_id: usize, field_name: String) -> usize {
        self.layout.get_offset(&field_name) + self.slot_offset(slot_id)
    }

    fn slot_offset(&self, slot_id: usize) -> usize {
        self.layout.slot_size * slot_id
    }

    pub fn delete_record(&mut self, slot_id: usize) -> Result<()> {
        let slot_offset = self.slot_offset(slot_id);
        self.transaction
            .set_int(&self.block_id, slot_offset as i32, EMPTY_FLAG, true)
    }

    pub fn intert_after(&mut self, slot_id: usize) -> Result<usize> {
        let empty_slot_id = self.search_empty_slot(slot_id).context("no empty slot")?;
        let empty_slot_offset = self.slot_offset(empty_slot_id);
        self.transaction
            .set_int(&self.block_id, empty_slot_offset as i32, USED_FLAG, true)?;
        Ok(empty_slot_id)
    }

    fn search_empty_slot(&mut self, slot_id: usize) -> Option<usize> {
        let mut slot_id = slot_id + 1;
        while self.slot_offset(slot_id + 1) <= self.transaction.block_size() {
            let flag = self
                .transaction
                .get_int(&self.block_id, slot_id as i32)
                .unwrap();
            if flag == 0 {
                return Some(slot_id);
            }
            slot_id += 1;
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        mydb::MyDb,
        record_manager::{record_page, schema::Schema},
    };

    use super::*;

    #[test]
    fn test_record_page() {
        let mydb = MyDb::new("test".to_string(), 1000, 8);
        let mut transaction = mydb.new_transaction();

        let mut schema = Schema::new();
        schema.add_int_field("id".to_string());
        schema.add_string_field("name".to_string(), 10);
        schema.add_int_field("age".to_string());

        let layout = Layout::from(schema);
        let new_block_id = transaction.append("test".to_string()).unwrap();
        transaction.pin(&new_block_id);
        let mut record_page = RecordPage::new(layout, transaction, new_block_id);

        let slot_id = record_page.intert_after(0).unwrap();
        record_page.set_int(slot_id, "id".to_string(), 1);
        record_page.set_string(slot_id, "name".to_string(), "John".to_string());
        record_page.set_int(slot_id, "age".to_string(), 23);

        let id = record_page.get_int(slot_id, "id".to_string()).unwrap();
        assert_eq!(id, 1);
    }
}
