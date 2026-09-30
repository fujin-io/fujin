//! `NATS` `JetStream` connector for Fujin. Streams and durable pull consumers are provisioned externally.

mod config;
mod reader;
#[cfg(test)]
mod tests;
mod writer;

use std::sync::Arc;

use async_nats::jetstream;
use fujin_connector::{
    BoxFuture, CompletionSink, ConnectorRuntime, Reader, ReaderEventSink, Writer,
};
use fujin_error::{CoreError, Result};
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

pub use config::NatsDescriptor;
use config::{BROKER_TIMEOUT, NatsConfig};

#[derive(Debug)]
struct NatsRuntime {
    config: NatsConfig,
    connection: Arc<OnceCell<jetstream::Context>>,
    shutdown: CancellationToken,
}

#[derive(Debug)]
struct Shared {
    common: config::CommonConfig,
    connection: Arc<OnceCell<jetstream::Context>>,
    shutdown: CancellationToken,
}

impl Shared {
    async fn context(&self) -> Result<jetstream::Context> {
        let context =
            self.connection
                .get_or_try_init(|| async {
                    if self.shutdown.is_cancelled() {
                        return Err(CoreError::Closed);
                    }
                    let mut options = async_nats::ConnectOptions::new()
                        .connection_timeout(BROKER_TIMEOUT)
                        .max_reconnects(3);
                    if let Some(file) = &self.common.credentials_file {
                        options = options.credentials_file(file).await.map_err(|e| {
                            CoreError::InvalidConfig(format!("NATS credentials: {e}"))
                        })?;
                    }
                    let servers = self.common.servers.join(",");
                    let client = tokio::time::timeout(BROKER_TIMEOUT, options.connect(servers))
                        .await
                        .map_err(|_| CoreError::Unavailable("NATS connection timed out".into()))?
                        .map_err(|e| CoreError::Unavailable(format!("NATS connection: {e}")))?;
                    let mut context = jetstream::new(client);
                    context.set_timeout(BROKER_TIMEOUT);
                    Ok(context)
                })
                .await?;
        Ok(context.clone())
    }
}

impl ConnectorRuntime for NatsRuntime {
    fn open_reader(
        &self,
        route: &str,
        auto_settle: bool,
        events: Arc<dyn ReaderEventSink>,
    ) -> Result<Arc<dyn Reader>> {
        if self.shutdown.is_cancelled() {
            return Err(CoreError::Closed);
        }
        let config = self
            .config
            .routes
            .get(route)
            .ok_or_else(|| CoreError::RouteNotFound(route.into()))?;
        if config.stream.is_none() {
            return Err(CoreError::OperationUnsupported);
        }
        Ok(reader::NatsReader::new(
            self.shared(),
            config.clone(),
            auto_settle,
            events,
        ))
    }

    fn open_writer(
        &self,
        route: &str,
        completions: Arc<dyn CompletionSink>,
    ) -> Result<Arc<dyn Writer>> {
        if self.shutdown.is_cancelled() {
            return Err(CoreError::Closed);
        }
        let config = self
            .config
            .routes
            .get(route)
            .ok_or_else(|| CoreError::RouteNotFound(route.into()))?;
        let subject = config
            .publish_subject
            .clone()
            .ok_or(CoreError::OperationUnsupported)?;
        Ok(writer::NatsWriter::new(self.shared(), subject, completions))
    }

    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            self.shutdown.cancel();
            if let Some(context) = self.connection.get() {
                tokio::time::timeout(BROKER_TIMEOUT, context.client().drain())
                    .await
                    .map_err(|_| CoreError::Unavailable("NATS drain timed out".into()))?
                    .map_err(|e| CoreError::Unavailable(format!("NATS drain: {e}")))?;
            }
            Ok(())
        })
    }
}

impl NatsRuntime {
    fn shared(&self) -> Arc<Shared> {
        Arc::new(Shared {
            common: self.config.common.clone(),
            connection: Arc::clone(&self.connection),
            shutdown: self.shutdown.clone(),
        })
    }
}

#[must_use]
pub fn plugin() -> fujin_connector::ConnectorPlugin {
    fujin_connector::ConnectorPlugin::new("nats", NatsDescriptor)
}
