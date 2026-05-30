use std::time::Instant;

// A record in our database
#[derive(Debug)]
pub enum RecordDataType {
    String(String),
    List(Vec<String>),
}

#[derive(Debug)]
pub struct Record {
    pub data: RecordDataType,
    pub expiration_time: Option<Instant>,
}
