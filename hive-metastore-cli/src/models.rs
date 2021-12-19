use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Table {
    pub tbl_name: String,
    pub db_name: String,
    pub columns: Vec<Column>,
    pub partitions: Vec<Partition>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub ty: String,
    pub comment: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Partition {
    pub name: String,
    pub ty: String,
    pub comment: String,
}
