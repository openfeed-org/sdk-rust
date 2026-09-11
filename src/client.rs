#[cfg(not(feature = "blocking"))]
use futures::StreamExt;

use prost::Message;
use std::default::Default;
use tungstenite::Bytes;

use crate::{
    connection::{Connection, ConnectionType, Feed},
    error::{OpenfeedError, OpenfeedResult},
    openfeed::{
        ExchangeRequest, InstrumentRequest, LoginRequest, LogoutRequest, OpenfeedGatewayMessage,
        OpenfeedGatewayRequest, Result, Service, SubscriptionRequest, SubscriptionType,
        instrument_definition::InstrumentType,
        instrument_request::Request as DefRequest,
        openfeed_gateway_message::Data::{LoginResponse, LogoutResponse},
        openfeed_gateway_request::Data::{
            ExchangeRequest as ExchangeRequestData, InstrumentRequest as InstrumentRequestData,
            LoginRequest as LoginRequestData, LogoutRequest as LogoutRequestData,
            SubscriptionRequest as SubscriptionRequestData,
        },
        subscription_request::{Request as SubRequest, request::Data as SubRequestData},
    },
};

/// Settings required for establishing a connection.
/// Server defaults to "openfeed.aws.barchart.com"
/// ```
/// let config = OpenfeedConfig::default()
///     .username("<user>")
///     .password("<pass>");
/// ```
#[derive(Debug)]
pub struct OpenfeedConfig {
    pub username: String,
    pub password: String,
    pub server: String,
}

impl Default for OpenfeedConfig {
    fn default() -> Self {
        OpenfeedConfig {
            username: String::new(),
            password: String::new(),
            server: "openfeed.aws.barchart.com".to_string(),
        }
    }
}

impl OpenfeedConfig {
    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.username = username.into();
        self
    }
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = password.into();
        self
    }
    pub fn server(mut self, server: impl Into<String>) -> Self {
        self.server = server.into();
        self
    }
}

/// A client for interacting with the Barchart Openfeed protocol.
/// Utilizes a synchronous connection when feature `blocking` is set,
/// or a non-blocking async connection when one (and only one) of
/// the available async runtime features
/// `tokio-runtime` or `smol-runtime` is set.
pub struct OpenfeedClient {
    config: OpenfeedConfig,
    connection: Option<ConnectionType>,
    token: Option<String>,
}

impl OpenfeedClient {
    pub fn new(config: OpenfeedConfig) -> Self {
        OpenfeedClient {
            config,
            connection: None,
            token: None,
        }
    }

    /// Creates a new, authenticated connection using the client config
    #[maybe_async::maybe_async]
    pub async fn connect(&mut self) -> OpenfeedResult<()> {
        self.connection =
            Some(ConnectionType::new(format!("ws://{}/ws", self.config.server)).await?);

        let login_request = OpenfeedGatewayRequest {
            data: Some(LoginRequestData(LoginRequest {
                username: self.config.username.clone(),
                password: self.config.password.clone(),
                client_version: "rust-sdk v0".to_string(),
                protocol_version: 1,
                ..Default::default()
            })),
        };
        self.send_message(login_request).await?;

        let gateway_response = self
            .read_messages()
            .await
            .next()
            .await
            .ok_or(OpenfeedError::StreamClosed())??;

        let (status, token) = match gateway_response.data {
            Some(LoginResponse(res)) => Some(res.status.zip(Some(res.token))),
            _ => None,
        }
        .flatten()
        .ok_or(OpenfeedError::DeserializeError())?;

        match status.result() {
            Result::Success => Ok(self.token = Some(token)),
            _ => Err(OpenfeedError::InvalidLogin(status.message)),
        }
    }

    /// Safely logs out and closes connection
    #[maybe_async::maybe_async]
    pub async fn disconnect(&self) -> OpenfeedResult<()> {
        let logout_request = OpenfeedGatewayRequest {
            data: Some(LogoutRequestData(LogoutRequest {
                token: self.token(),
                ..Default::default()
            })),
        };
        self.send_message(logout_request).await?;
        self.connection().close().await?;

        while let Some(gateway_response) = self.read_messages().await.next().await {
            match gateway_response?.data {
                Some(LogoutResponse(res)) => {
                    let status = res.status.ok_or(OpenfeedError::DeserializeError())?;
                    match status.result() {
                        Result::Success => { /* successfully logged out */ }
                        _ => return Err(OpenfeedError::InvalidLogin(status.message)),
                    }
                }
                _ => { /* drain feed until server closed conn */ }
            }
        }
        Ok(())
    }

    /// Subscribes to quotes for the given symbols.
    #[maybe_async::maybe_async]
    pub async fn subscribe_symbols(
        &self,
        symbols: impl IntoIterator<Item = impl Into<String>>,
        subscription_types: &[SubscriptionType],
        service: Service,
    ) -> OpenfeedResult<()> {
        self.create_subscription_request(
            symbols
                .into_iter()
                .map(|s| SubRequestData::Symbol(s.into())),
            subscription_types,
            &[],
            service,
        )
        .await
    }

    /// Subscribes to quotes for every instrument on the given exchanges.
    #[maybe_async::maybe_async]
    pub async fn subscribe_exchanges(
        &self,
        exchanges: impl IntoIterator<Item = impl Into<String>>,
        subscription_types: &[SubscriptionType],
        instrument_types: &[InstrumentType],
        service: Service,
    ) -> OpenfeedResult<()> {
        self.create_subscription_request(
            exchanges
                .into_iter()
                .map(|s| SubRequestData::Exchange(s.into())),
            subscription_types,
            instrument_types,
            service,
        )
        .await
    }

    /// Request an instrument definitions for a symbol.
    #[maybe_async::maybe_async]
    pub async fn request_instrument(&self, symbol: impl Into<String>) -> OpenfeedResult<()> {
        self.create_instrument_request(DefRequest::Symbol(symbol.into()))
            .await
    }

    /// Request all instrument definitions for an exchange.
    #[maybe_async::maybe_async]
    pub async fn request_instruments_for_exchange(
        &self,
        exchange: impl Into<String>,
    ) -> OpenfeedResult<()> {
        self.create_instrument_request(DefRequest::Exchange(exchange.into()))
            .await
    }

    /// Request available exchanges.
    #[maybe_async::maybe_async]
    pub async fn request_exchanges(&self) -> OpenfeedResult<()> {
        self.send_message(OpenfeedGatewayRequest {
            data: Some(ExchangeRequestData(ExchangeRequest {
                token: self.token(),
                ..Default::default()
            })),
        })
        .await
    }

    /// Returns a `Feed` of `OpenfeedGatewayMessage` results. A `Feed` is either
    /// an `Iterator` when using the blocking client
    /// or a `Stream` when using the nonblocking client.
    ///
    /// ```
    /// use openfeed_gateway_message::Data::*,
    ///
    /// for message in client.read_messages() {
    ///     match message?.data {
    ///         Some(LoginResponse(_)) => todo!(),
    ///         Some(LogoutResponse(_)) => todo!(),
    ///         Some(ListSubscriptionsResponse(_))=>todo!(),
    ///         Some(MarketStatus(_)) => todo!(),
    ///         Some(InstrumentResponse(_)) => todo!(),
    ///         Some(SubscriptionResponse(_)) => todo!(),
    ///         Some(InstrumentReferenceResponse(_))=>todo!(),
    ///         Some(InstrumentDefinition(_)) => todo!(),
    ///         Some(MarketSnapshot(_))=>todo!(),
    ///         Some(MarketUpdate(_))=>todo!(),
    ///         Some(VolumeAtPrice(_))=>todo!(),
    ///         Some(Ohlc(_))=>todo!(),
    ///         Some(InstrumentAction(_))=>todo!(),
    ///         Some(ExchangeResponse(_))=>todo!(),
    ///         Some(HeartBeat(_)) => todo!(),
    ///         None => todo!(),
    ///     }
    /// }
    /// ```
    #[maybe_async::maybe_async]
    pub async fn read_messages(&self) -> impl Feed<Item = OpenfeedResult<OpenfeedGatewayMessage>> {
        self.read_bytes().await.map(|res| match res {
            Ok(bytes) => OpenfeedGatewayMessage::decode(bytes).map_err(OpenfeedError::from),
            Err(err) => Err(err),
        })
    }

    /// Returns a `Feed` of raw bytes making up `OpenfeedGatewayMessage` results.
    /// A `Feed` is either
    /// an `Iterator` when using the blocking client
    /// or a `Stream` when using the nonblocking client.
    #[maybe_async::maybe_async]
    pub async fn read_bytes(&self) -> impl Feed<Item = OpenfeedResult<Bytes>> {
        self.connection().recv().await
    }

    #[maybe_async::maybe_async]
    async fn create_subscription_request(
        &self,
        requests: impl IntoIterator<Item = SubRequestData>,
        subscription_types: &[SubscriptionType],
        instrument_types: &[InstrumentType],
        service: Service,
    ) -> OpenfeedResult<()> {
        self.send_message(OpenfeedGatewayRequest {
            data: Some(SubscriptionRequestData(SubscriptionRequest {
                token: self.token(),
                service: service as i32,
                requests: requests
                    .into_iter()
                    .map(|data| SubRequest {
                        data: Some(data),
                        subscription_type: subscription_types.iter().map(|t| *t as i32).collect(),
                        instrument_type: instrument_types.iter().map(|t| *t as i32).collect(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })),
        })
        .await
    }

    #[maybe_async::maybe_async]
    async fn create_instrument_request(&self, request: DefRequest) -> OpenfeedResult<()> {
        self.send_message(OpenfeedGatewayRequest {
            data: Some(InstrumentRequestData(InstrumentRequest {
                token: self.token(),
                request: Some(request),
                ..Default::default()
            })),
        })
        .await
    }

    #[maybe_async::maybe_async]
    async fn send_message<T: Message>(&self, msg: T) -> OpenfeedResult<()> {
        self.connection().send(msg.encode_to_vec().into()).await
    }

    fn connection(&self) -> &ConnectionType {
        self.connection
            .as_ref()
            .expect("connection not established")
    }

    fn token(&self) -> String {
        self.token
            .clone()
            .expect("requires token: establish connection first")
    }
}
