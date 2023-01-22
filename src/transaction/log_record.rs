use std::sync::{Arc, Mutex};

use crate::{
    file_manager::{BlockId, Page, INTGER_BYTES},
    log_manager::LogManager,
};

enum LogRecordType {
    CheckPoint = 0,
    StartRecord,
    CommitRecord,
    Rollback,
    SetInt,
    SetString,
}

impl From<i32> for LogRecordType {
    fn from(v: i32) -> Self {
        match v {
            5 => LogRecordType::SetString,
            _ => todo!(),
        }
    }
}

enum LogRecord {
    CheckPoint,
    StartRecord,
    CommitRecord,
    Rollback,
    SetInt,
    SetString(SetStringRecord),
}

struct SetStringRecord {
    record_type: LogRecordType,
    txnum: i32,
    offset: i32,
    value: String,
    block_id: BlockId,
}

impl From<&mut Page> for LogRecord {
    fn from(page: &mut Page) -> Self {
        let record_type: LogRecordType = page.get_int(0).unwrap().into();
        match record_type {
            LogRecordType::SetString => {
                let tpos = INTGER_BYTES;
                let txnum = page.get_int(tpos).unwrap();

                let fpos = tpos + INTGER_BYTES;
                let filename = page.get_string(fpos).unwrap();

                let bpos = fpos + Page::max_length(filename.len());
                let block_number = page.get_int(bpos).unwrap();

                let opos = bpos + INTGER_BYTES;
                let offset = page.get_int(opos).unwrap();

                let vpos = opos + INTGER_BYTES;
                let value = page.get_string(vpos).unwrap();

                LogRecord::SetString(SetStringRecord {
                    record_type: LogRecordType::SetString,
                    txnum,
                    offset,
                    value,
                    block_id: BlockId {
                        filename,
                        block_number: block_number as usize,
                    },
                })
            }
            _ => {
                todo!()
            }
        }
    }
}

impl LogRecord {
    fn write_to_log(&self, log_manager: Arc<Mutex<LogManager>>) -> i32 {
        match self {
            Self::SetString(record) => {
                let tpos = INTGER_BYTES;
                let fpos = tpos + INTGER_BYTES;
                let bpos = fpos + Page::max_length(record.block_id.filename.len());
                let opos = bpos + INTGER_BYTES;
                let vpos = opos + INTGER_BYTES;
                let reclen = vpos + Page::max_length(record.value.len());

                let buf = Vec::with_capacity(reclen);
                let mut page = Page::from(Box::from(buf));
                page.set_int(0, 5).unwrap();
                page.set_int(tpos, record.txnum).unwrap();
                page.set_string(fpos, record.block_id.filename.to_owned())
                    .unwrap();
                page.set_int(bpos, record.offset).unwrap();
                page.set_string(vpos, record.value.to_owned()).unwrap();
                log_manager
                    .lock()
                    .unwrap()
                    .append_record(&page.contents())
                    .unwrap()
            }
            _ => 0,
        }
    }
}
