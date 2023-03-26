use linked_hash_map::LinkedHashMap;

use crate::file_manager::{file_manager::INTGER_BYTES, page::Page};

#[derive(Debug)]
pub struct Schema {
    pub fields: Vec<String>,
    pub field_info: LinkedHashMap<String, FieldInfo>,
}

impl Schema {
    pub fn new() -> Self {
        let fields = vec![];
        let field_info = LinkedHashMap::new();
        Self { fields, field_info }
    }

    pub fn add_string_field(&mut self, field: String, length: usize) {
        self.fields.push(field.clone());
        self.field_info
            .insert(field, FieldInfo::StringField(StringField { length }));
    }

    pub fn add_int_field(&mut self, field: String) {
        self.fields.push(field.clone());
        self.field_info.insert(field, FieldInfo::IntField(IntField));
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum FieldInfo {
    IntField(IntField),
    StringField(StringField),
}

#[derive(Debug, PartialEq, Eq)]
struct StringField {
    pub length: usize,
}

#[derive(Debug, PartialEq, Eq)]
struct IntField;

impl FieldInfo {
    pub fn bytes_length(&self) -> usize {
        match self {
            FieldInfo::IntField(_) => INTGER_BYTES,
            FieldInfo::StringField(string_field) => Page::max_length(string_field.length),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_fields() {
        let mut schema = Schema::new();

        schema.add_int_field("id".to_string());
        schema.add_string_field("name".to_string(), 10);

        assert_eq!(
            schema.fields,
            vec![String::from("id"), String::from("name")]
        );
        // field info has id and name
        assert_eq!(schema.field_info.len(), 2);
        // id is int field
        assert_eq!(
            schema.field_info.get("id").unwrap(),
            &FieldInfo::IntField(IntField)
        );
        // name is string field
        assert_eq!(
            schema.field_info.get("name").unwrap(),
            &FieldInfo::StringField(StringField { length: 10 })
        );
    }
}
