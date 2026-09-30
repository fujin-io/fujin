use std::{collections::BTreeMap, sync::Arc, time::Duration};

use fujin_connector::{
    AcceptanceGuarantee, AckGranularity, Capabilities, CompiledConnector, ConnectorDescriptor,
    NackEffect, RouteProfile, SettlementProfile,
};
use fujin_error::{CoreError, Result};
use serde::Deserialize;
use tokio::sync::OnceCell;

use crate::NatsRuntime;

pub(crate) const BROKER_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommonConfig {
    pub servers: Vec<String>,
    pub credentials_file: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RouteConfig {
    pub publish_subject: Option<String>,
    pub stream: Option<String>,
    pub consumer: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NatsConfig {
    pub common: CommonConfig,
    pub routes: BTreeMap<String, RouteConfig>,
}

#[derive(Debug, Default)]
pub struct NatsDescriptor;

impl ConnectorDescriptor for NatsDescriptor {
    fn compile(&self, settings: &serde_json::Value) -> Result<Arc<dyn CompiledConnector>> {
        let config: NatsConfig = serde_json::from_value(settings.clone())
            .map_err(|error| CoreError::InvalidConfig(format!("NATS configuration: {error}")))?;
        if config.common.servers.is_empty()
            || config.common.servers.iter().any(|server| {
                !(server.starts_with("nats://") || server.starts_with("tls://"))
                    || server.parse::<async_nats::ServerAddr>().is_err()
            })
        {
            return Err(CoreError::InvalidConfig(
                "NATS common.servers requires valid nats:// or tls:// URLs".into(),
            ));
        }
        if config
            .common
            .credentials_file
            .as_ref()
            .is_some_and(String::is_empty)
        {
            return Err(CoreError::InvalidConfig(
                "NATS credentials_file is empty".into(),
            ));
        }
        if config.routes.is_empty() {
            return Err(CoreError::InvalidConfig("NATS routes are empty".into()));
        }
        let mut profiles = BTreeMap::new();
        for (name, route) in &config.routes {
            if name.is_empty() {
                return Err(CoreError::InvalidConfig("NATS route name is empty".into()));
            }
            if route.stream.is_some() != route.consumer.is_some() {
                return Err(CoreError::InvalidConfig(format!(
                    "NATS route {name:?} requires both stream and consumer"
                )));
            }
            if route.publish_subject.is_none() && route.stream.is_none() {
                return Err(CoreError::InvalidConfig(format!(
                    "NATS route {name:?} has no publish or read operation"
                )));
            }
            if route.publish_subject.as_ref().is_some_and(|subject| {
                subject.is_empty()
                    || subject.split('.').any(|segment| {
                        segment.is_empty()
                            || segment == "*"
                            || segment == ">"
                            || segment.chars().any(char::is_whitespace)
                    })
            }) || route
                .stream
                .as_ref()
                .is_some_and(|s| s.is_empty() || s.chars().any(char::is_whitespace))
                || route
                    .consumer
                    .as_ref()
                    .is_some_and(|s| s.is_empty() || s.chars().any(char::is_whitespace))
            {
                return Err(CoreError::InvalidConfig(format!(
                    "NATS route {name:?} contains an invalid subject, stream, or consumer"
                )));
            }
            let mut capabilities = Capabilities::default();
            let mut produce_guarantee = AcceptanceGuarantee::Unspecified;
            let mut settlement = SettlementProfile::default();
            if route.publish_subject.is_some() {
                capabilities = capabilities.union(Capabilities::PRODUCE);
                produce_guarantee = AcceptanceGuarantee::Peer;
            }
            if route.stream.is_some() {
                capabilities = capabilities
                    .union(Capabilities::FETCH)
                    .union(Capabilities::SUBSCRIBE)
                    .union(Capabilities::MANUAL_SETTLEMENT);
                settlement = SettlementProfile {
                    ack: AckGranularity::Single,
                    nack: NackEffect::Requeue,
                };
            }
            let profile = RouteProfile {
                capabilities,
                produce_guarantee,
                settlement,
            };
            profile.validate(name)?;
            profiles.insert(name.clone(), profile);
        }
        Ok(Arc::new(NatsCompiled { config, profiles }))
    }
}

struct NatsCompiled {
    config: NatsConfig,
    profiles: BTreeMap<String, RouteProfile>,
}

impl CompiledConnector for NatsCompiled {
    fn routes(&self) -> &BTreeMap<String, RouteProfile> {
        &self.profiles
    }

    fn open_runtime(&self) -> Result<Arc<dyn fujin_connector::ConnectorRuntime>> {
        Ok(Arc::new(NatsRuntime {
            config: self.config.clone(),
            connection: Arc::new(OnceCell::new()),
            shutdown: tokio_util::sync::CancellationToken::new(),
        }))
    }
}
