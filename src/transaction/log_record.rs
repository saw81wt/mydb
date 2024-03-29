use std::sync::{Arc, Mutex};

use crate::{
    file_manager::{BlockId, Page, INTGER_BYTES},
    log_manager::LogManager,
};

use super::transaction::Transaction;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogRecordType {
    CheckPoint = 0,
    Start,
    Commit,
    Rollback,
    SetInt,
    SetString,
}

impl From<i32> for LogRecordType {
    fn from(v: i32) -> Self {
        match v {
            0 => LogRecordType::CheckPoint,
            1 => LogRecordType::Start,
            2 => LogRecordType::Commit,
            3 => LogRecordType::Rollback,
            4 => LogRecordType::SetInt,
            5 => LogRecordType::SetString,
            _ => todo!(),
        }
    }
}

impl From<LogRecordType> for i32 {
    fn from(l: LogRecordType) -> Self {
        match l {
            LogRecordType::CheckPoint => 0,
            LogRecordType::Start => 1,
            LogRecordType::Commit => 2,
            LogRecordType::Rollback => 3,
            LogRecordType::SetInt => 4,
            LogRecordType::SetString => 5,
        }
    }
}

pub enum LogRecord {
    CheckPoint(TransactionRecord),
    Start(TransactionRecord),
    Commit(TransactionRecord),
    Rollback(TransactionRecord),
    SetInt(UpdateRecord<i32>),
    SetString(UpdateRecord<String>),
}

impl LogRecord {
    pub fn create_checkpoint_record(txnum: i32) -> Self {
        LogRecord::CheckPoint(TransactionRecord {
            record_type: LogRecordType::CheckPoint,
            txnum,
        })
    }

    pub fn create_start_record(txnum: i32) -> Self {
        LogRecord::Start(TransactionRecord {
            record_type: LogRecordType::Start,
            txnum,
        })
    }

    pub fn create_commit_record(txnum: i32) -> Self {
        LogRecord::Commit(TransactionRecord {
            record_type: LogRecordType::Commit,
            txnum,
        })
    }

    pub fn create_rollback_record(txnum: i32) -> Self {
        LogRecord::Rollback(TransactionRecord {
            record_type: LogRecordType::Rollback,
            txnum,
        })
    }

    pub fn create_set_int_record(txnum: i32, offset: i32, value: i32, block_id: BlockId) -> Self {
        LogRecord::SetInt(UpdateRecord {
            record_type: LogRecordType::SetInt,
            txnum,
            offset,
            value,
            block_id,
        })
    }

    pub fn create_set_string_record(
        txnum: i32,
        offset: i32,
        value: String,
        block_id: BlockId,
    ) -> Self {
        LogRecord::SetString(UpdateRecord {
            record_type: LogRecordType::SetString,
            txnum,
            offset,
            value,
            block_id,
        })
    }
}

impl LogRecordTrait for LogRecord {
    fn get_txnum(&self) -> i32 {
        match self {
            Self::CheckPoint(record)
            | Self::Commit(record)
            | Self::Start(record)
            | Self::Rollback(record) => record.txnum,
            Self::SetInt(record) => record.txnum,
            Self::SetString(record) => record.txnum,
        }
    }
}

pub trait LogRecordTrait {
    fn get_txnum(&self) -> i32;
}

pub struct TransactionRecord {
    record_type: LogRecordType,
    txnum: i32,
}

pub struct UpdateRecord<T> {
    record_type: LogRecordType,
    pub txnum: i32,
    pub offset: i32,
    pub value: T,
    pub block_id: BlockId,
}

impl TryFrom<&mut Page> for LogRecord {
    type Error = anyhow::Error;
    fn try_from(page: &mut Page) -> anyhow::Result<Self> {
        let record_type: LogRecordType = page.get_int(0)?.into();
        match record_type {
            LogRecordType::CheckPoint => {
                let tpos = INTGER_BYTES;
                let txnum = page.get_int(tpos)?;

                Ok(LogRecord::create_checkpoint_record(txnum))
            }
            LogRecordType::Start => {
                let tpos = INTGER_BYTES;
                let txnum = page.get_int(tpos)?;

                Ok(LogRecord::create_start_record(txnum))
            }
            LogRecordType::Commit => {
                let tpos = INTGER_BYTES;
                let txnum = page.get_int(tpos)?;

                Ok(LogRecord::create_commit_record(txnum))
            }
            LogRecordType::Rollback => {
                let tpos = INTGER_BYTES;
                let txnum = page.get_int(tpos)?;

                Ok(LogRecord::create_rollback_record(txnum))
            }
            LogRecordType::SetInt => {
                let tpos = INTGER_BYTES;
                let txnum = page.get_int(tpos)?;

                let fpos = tpos + INTGER_BYTES;
                let filename = page.get_string(fpos)?;

                let bpos = fpos + Page::max_length(filename.len());
                let block_number = page.get_int(bpos)?;

                let opos = bpos + INTGER_BYTES;
                let offset = page.get_int(opos)?;

                let vpos = opos + INTGER_BYTES;
                let value = page.get_int(vpos)?;

                Ok(LogRecord::create_set_int_record(
                    txnum,
                    offset,
                    value,
                    BlockId {
                        filename,
                        block_number,
                    },
                ))
            }
            LogRecordType::SetString => {
                let tpos = INTGER_BYTES;
                let txnum = page.get_int(tpos)?;

                let fpos = tpos + INTGER_BYTES;
                let filename = page.get_string(fpos)?;

                let bpos = fpos + Page::max_length(filename.len());
                let block_number = page.get_int(bpos)?;

                let opos = bpos + INTGER_BYTES;
                let offset = page.get_int(opos)?;

                let vpos = opos + INTGER_BYTES;
                let value = page.get_string(vpos).unwrap();

                Ok(LogRecord::create_set_string_record(
                    txnum,
                    offset,
                    value,
                    BlockId {
                        filename,
                        block_number,
                    },
                ))
            }
            _ => {
                todo!()
            }
        }
    }
}

impl From<LogRecord> for Page {
    fn from(log_record: LogRecord) -> Page {
        match log_record {
            LogRecord::CheckPoint(record)
            | LogRecord::Commit(record)
            | LogRecord::Start(record)
            | LogRecord::Rollback(record) => {
                let tpos = INTGER_BYTES;
                let record_len = tpos + INTGER_BYTES;

                let buf = Vec::with_capacity(record_len);
                let mut page = Page::from(Box::from(buf));

                page.set_int(0, record.record_type.into()).unwrap();
                page.set_int(tpos, record.txnum).unwrap();
                page
            }
            LogRecord::SetInt(record) => {
                let tpos = INTGER_BYTES;
                let fpos = tpos + INTGER_BYTES;
                let bpos = fpos + Page::max_length(record.block_id.filename.len());
                let opos = bpos + INTGER_BYTES;
                let vpos = opos + INTGER_BYTES;
                let reclen = vpos + INTGER_BYTES;

                let buf = Vec::with_capacity(reclen);
                let mut page = Page::from(Box::from(buf));
                page.set_int(0, LogRecordType::SetString.into()).unwrap();
                page.set_int(tpos, record.txnum).unwrap();
                page.set_string(fpos, record.block_id.filename.to_owned())
                    .unwrap();
                page.set_int(bpos, record.offset).unwrap();
                page.set_int(vpos, record.value).unwrap();
                page
            }
            LogRecord::SetString(record) => {
                let tpos = INTGER_BYTES;
                let fpos = tpos + INTGER_BYTES;
                let bpos = fpos + Page::max_length(record.block_id.filename.len());
                let opos = bpos + INTGER_BYTES;
                let vpos = opos + INTGER_BYTES;
                let reclen = vpos + Page::max_length(record.value.len());

                let buf = Vec::with_capacity(reclen);
                let mut page = Page::from(Box::from(buf));
                page.set_int(0, LogRecordType::SetString.into()).unwrap();
                page.set_int(tpos, record.txnum).unwrap();
                page.set_string(fpos, record.block_id.filename.to_owned())
                    .unwrap();
                page.set_int(bpos, record.block_id.block_number).unwrap();
                page.set_int(opos, record.offset).unwrap();
                page.set_string(vpos, record.value.to_owned()).unwrap();
                page
            }
            _ => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_string() {
        let block_id = BlockId {
            filename: "test.txt".to_string(),
            block_number: 1,
        };
        let record =
            LogRecord::create_set_string_record(1, 0, "test".to_string(), block_id.clone());
        let log_record = LogRecord::try_from(&mut record.into()).unwrap();

        match log_record {
            LogRecord::SetString(update_record) => {
                assert_eq!(update_record.txnum, 1);
                assert_eq!(update_record.block_id, block_id);
                assert_eq!(update_record.offset, 0);
                assert_eq!(update_record.value, "test".to_string());
            }
            _ => panic!("Invalid log record type."),
        }
    }

    fn test_set_int() {
        let block_id = BlockId {
            filename: "test.txt".to_string(),
            block_number: 1,
        };
        let record = LogRecord::create_set_int_record(1, 0, 20, block_id.clone());
        let log_record = LogRecord::try_from(&mut record.into()).unwrap();

        match log_record {
            LogRecord::SetInt(update_record) => {
                assert_eq!(update_record.txnum, 1);
                assert_eq!(update_record.block_id, block_id);
                assert_eq!(update_record.offset, 0);
                assert_eq!(update_record.value, 20);
            }
            _ => panic!("Invalid log record type."),
        }
    }
}
