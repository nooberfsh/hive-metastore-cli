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
use crate::hive_metastore::TThriftHiveMetastoreSyncClient;


pub struct HiveMetastoreCli {
    client: ThriftHiveMetastoreSyncClient<TBinaryInputProtocol<TBufferedReadTransport<ReadHalf<TTcpChannel>>>, TBinaryOutputProtocol<TBufferedWriteTransport<WriteHalf<TTcpChannel>>>>,
}

#[derive(Error, Debug)]
pub enum HiveMetastoreError {
    #[error("thrift error")]
    ThriftError(#[from] ThriftError)
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
    pub async fn get_table_info(&mut self, db: &str, tbl: &str) -> Result<TableInfo> {
        let tbl = self.client.get_table(db.to_string(), tbl.to_string())?;
        println!("{:?}", tbl);
        Ok(TableInfo{})
    }
}
