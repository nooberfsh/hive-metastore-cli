use std::net::ToSocketAddrs;

use thiserror::Error;
use thrift::protocol::{TCompactInputProtocol, TCompactOutputProtocol};
use thrift::transport::{ReadHalf, TFramedReadTransport, TFramedWriteTransport, TIoChannel, TTcpChannel, WriteHalf};
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
    client: ThriftHiveMetastoreSyncClient<TCompactInputProtocol<TFramedReadTransport<ReadHalf<TTcpChannel>>>, TCompactOutputProtocol<TFramedWriteTransport<WriteHalf<TTcpChannel>>>>,
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
        let i_prot = TCompactInputProtocol::new(TFramedReadTransport::new(i_chan));
        let o_prot = TCompactOutputProtocol::new(TFramedWriteTransport::new(o_chan));

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
