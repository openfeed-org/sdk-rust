use thiserror::Error;

#[derive(Error, Debug)]
pub enum OpenfeedError {
    #[error("{0} of: invalid login")]
    InvalidLogin(String),

    #[error("of: stream closed")]
    StreamClosed(),

    #[error("of: network read")]
    NetworkRead(),

    #[error("of: server: {0}")]
    Server(String),

    #[error("of: deserialize error")]
    DeserializeError(),

    #[error("client(blocking): attempting to write while reading feed")]
    FeedActive(),

    #[error("tungstenite: {0}")]
    Tungstenite(#[from] tungstenite::Error),

    #[error("decode: {0}")]
    DecodeError(#[from] prost::DecodeError),
}

pub type OpenfeedResult<T> = std::result::Result<T, OpenfeedError>;
