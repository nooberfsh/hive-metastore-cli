use std::net::ToSocketAddrs;

use thiserror::Error;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{ReadHalf, TBufferedReadTransport, TBufferedWriteTransport, TIoChannel, TTcpChannel, WriteHalf};
use thrift::Error as ThriftError;

use hive_metastore::{ThriftHiveMetastoreSyncClient};

#[allow(unused)]
pub (crate) mod fb303;
#[allow(unused)]
pub (crate) mod hive_metastore;
mod models;
pub use models::*;
use crate::hive_metastore::{FieldSchema, TThriftHiveMetastoreSyncClient};


pub struct HiveMetastoreCli {
    client: ThriftHiveMetastoreSyncClient<TBinaryInputProtocol<TBufferedReadTransport<ReadHalf<TTcpChannel>>>, TBinaryOutputProtocol<TBufferedWriteTransport<WriteHalf<TTcpChannel>>>>,
}

#[derive(Error, Debug)]
pub enum HiveMetastoreError {
    #[error("thrift error: {0}")]
    ThriftError(String),
    #[error("request table, bug the target is a view: {0}.{1}")]
    ViewInsteadOfTable(String, String),
}

type Result<T> = std::result::Result<T, HiveMetastoreError>;

impl HiveMetastoreCli {
    pub async fn new(addr: impl ToSocketAddrs) -> Result<Self> {
        // build our client and connect to the host:port
        let mut c = TTcpChannel::new();
        c.open(addr)?;
        let (i_chan, o_chan) = c.split()?;

        // build the input/output protocol
        let i_prot = TBinaryInputProtocol::new(TBufferedReadTransport::new(i_chan), true);
        let o_prot = TBinaryOutputProtocol::new(TBufferedWriteTransport::new(o_chan), true);

        // use the input/output protocol to create a Thrift client
        let client = ThriftHiveMetastoreSyncClient::new(i_prot, o_prot);
        Ok(HiveMetastoreCli{client})
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// API

impl HiveMetastoreCli {
    pub async fn get_table(&mut self, db: &str, tbl: &str) -> Result<Table> {
        let table = self.client.get_table(db.to_string(), tbl.to_string())?;
        if !table.view_original_text.unwrap_or_else(||String::new()).trim().is_empty() {
            return Err(HiveMetastoreError::ViewInsteadOfTable(db.to_string(), tbl.to_string()));
        }
        let db_name = table.db_name.unwrap();
        let tbl_name = table.table_name.unwrap();
        let columns = table.sd.unwrap().cols.unwrap().into_iter().map(|t| t.into()).collect();
        let partitions = table.partition_keys.unwrap_or_else(|| vec![]).into_iter().map(|t| t.into()).collect();
        Ok(Table {db_name, tbl_name, columns, partitions})
    }

    pub async fn get_all_tables(&mut self, db: &str) -> Result<Vec<String>> {
        let tables = self.client.get_all_tables(db.to_string())?;
        Ok(tables)
    }

    pub async fn get_all_databases(&mut self) -> Result<Vec<String>> {
        let dbs = self.client.get_all_databases()?;
        Ok(dbs)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// helpers
impl From<ThriftError> for HiveMetastoreError {
    fn from(e: ThriftError) -> Self {
        HiveMetastoreError::ThriftError(e.to_string())
    }
}

impl From<FieldSchema> for Column {
    fn from(f: FieldSchema) -> Self {
        Column {
            name: f.name.unwrap(),
            ty: f.type_.unwrap(),
            comment: f.comment.unwrap_or_else(||String::new()),
        }
    }
}

impl From<FieldSchema> for Partition {
    fn from(f: FieldSchema) -> Self {
        Partition {
            name: f.name.unwrap(),
            ty: f.type_.unwrap(),
            comment: f.comment.unwrap_or_else(||String::new()),
        }
    }
}
