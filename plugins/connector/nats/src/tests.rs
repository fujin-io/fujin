use fujin_connector::{
    AcceptanceGuarantee, AckGranularity, Capabilities, ConnectorDescriptor, NackEffect,
};
use serde_json::json;

use crate::NatsDescriptor;

#[test]
fn invalid_route_rejected_without_broker() {
    let descriptor = NatsDescriptor;
    for routes in [
        json!({"invalid": {}}),
        json!({"invalid": {"stream": "EVENTS"}}),
    ] {
        let config = json!({"common": {"servers": ["nats://127.0.0.1:1"]}, "routes": routes});
        assert!(descriptor.compile(&config).is_err());
    }
}

#[test]
fn nats_route_profile_excludes_headers_and_transactions() {
    let descriptor = NatsDescriptor;
    let config = json!({"common": {"servers": ["nats://127.0.0.1:1"]}, "routes": {
        "events": {"publish_subject": "events.created", "stream": "EVENTS", "consumer": "worker"},
        "pub": {"publish_subject": "events.created"}
    }});
    let compiled = descriptor
        .compile(&config)
        .expect("local compile without broker");
    let profile = compiled.routes()["events"];
    assert!(profile.capabilities.contains(Capabilities::PRODUCE));
    assert!(profile.capabilities.contains(Capabilities::FETCH));
    assert!(profile.capabilities.contains(Capabilities::SUBSCRIBE));
    assert!(
        profile
            .capabilities
            .contains(Capabilities::MANUAL_SETTLEMENT)
    );
    assert!(!profile.capabilities.contains(Capabilities::HEADERS));
    assert!(!profile.capabilities.contains(Capabilities::TRANSACTIONS));
    assert_eq!(profile.produce_guarantee, AcceptanceGuarantee::Peer);
    assert_eq!(profile.settlement.ack, AckGranularity::Single);
    assert_eq!(profile.settlement.nack, NackEffect::Requeue);
    assert!(
        !compiled.routes()["pub"]
            .capabilities
            .contains(Capabilities::FETCH)
    );
}
