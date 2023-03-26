use crate::file_manager::file_manager::INTGER_BYTES;
use std::collections::HashMap;

use super::schema::Schema;

#[derive(Debug)]
pub struct Layout {
    pub schema: Schema,
    pub offsets: HashMap<String, usize>,
    pub slot_size: usize,
}

impl Layout {
    pub fn new(schema: Schema, offsets: HashMap<String, usize>, slot_size: usize) -> Self {
        Self {
            schema,
            offsets,
            slot_size,
        }
    }

    pub fn get_offset(&self, field_name: &str) -> usize {
        *self.offsets.get(field_name).unwrap()
    }
}

impl From<Schema> for Layout {
    fn from(schema: Schema) -> Self {
        let mut offsets = HashMap::new();
        // space for the empty/inuse flag
        let mut offset = INTGER_BYTES;
        for (field_name, field_info) in &schema.field_info {
            offsets.insert(field_name.clone(), offset);
            offset += field_info.bytes_length();
        }
        Self {
            schema,
            offsets,
            slot_size: offset,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_layout() {
        let mut schema = Schema::new();
        schema.add_int_field("id".to_string());
        schema.add_string_field("name".to_string(), 10);

        let layout = Layout::from(schema);

        assert_eq!(layout.slot_size, 22);
        assert_eq!(layout.get_offset("id"), 4);
        assert_eq!(layout.get_offset("name"), 8);
    }

    #[test]
    fn test_layout_get_offset() {
        let mut schema = Schema::new();
        schema.add_int_field("id".to_string());
        schema.add_string_field("name".to_string(), 10);
        schema.add_int_field("age".to_string());

        let layout = Layout::from(schema);

        assert_eq!(layout.get_offset("id"), 4);
        assert_eq!(layout.get_offset("name"), 8);
        assert_eq!(layout.get_offset("age"), 22);
    }
}
