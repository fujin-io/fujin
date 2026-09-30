#![cfg(feature = "grpc")]

use fujin_connector::{Catalog, ConnectorConfig, ConnectorRegistry, GenerationCompiler};
use fujin_grpc_proto::fujin::v1 as pb;
use fujin_middleware::NoBindMiddleware;
use fujin_native::{RequestCode, ResponseCode};
use fujin_runtime::GrpcService;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_stream::wrappers::{TcpListenerStream, UnboundedReceiverStream};
use tonic::transport::Server;

fn append_bytes(buffer: &mut Vec<u8>, value: &[u8]) {
    buffer.extend_from_slice(&(value.len() as u32).to_be_bytes());
    buffer.extend_from_slice(value);
}

async fn read_bytes(stream: &mut tokio::io::DuplexStream) -> Vec<u8> {
    let mut size = [0; 4];
    stream.read_exact(&mut size).await.expect("length");
    let mut bytes = vec![0; u32::from_be_bytes(size) as usize];
    stream.read_exact(&mut bytes).await.expect("bytes");
    bytes
}

async fn read_failure(stream: &mut tokio::io::DuplexStream, expected: ResponseCode, id: u32) -> u8 {
    let mut prefix = [0; 6];
    stream
        .read_exact(&mut prefix)
        .await
        .expect("failure prefix");
    assert_eq!(prefix[0], expected as u8);
    assert_eq!(
        u32::from_be_bytes(prefix[1..5].try_into().expect("correlation id")),
        id
    );
    assert_ne!(prefix[5], 0);
    let mut outcome = [0];
    stream.read_exact(&mut outcome).await.expect("outcome");
    assert_eq!(outcome, [1]);
    for _ in 0..2 {
        read_bytes(stream).await;
    }
    let mut details = [0; 2];
    stream
        .read_exact(&mut details)
        .await
        .expect("details count");
    for _ in 0..u16::from_be_bytes(details) {
        read_bytes(stream).await;
        read_bytes(stream).await;
    }
    prefix[5]
}

#[tokio::test]
async fn nats_native_and_grpc_bind_profiles_match() {
    let registry = Arc::new(ConnectorRegistry::default());
    registry
        .register("nats", Arc::new(fujin_connector_nats::NatsDescriptor))
        .expect("register NATS");
    let configs = BTreeMap::from([(
        "events".into(),
        ConnectorConfig {
            connector_type: "nats".into(),
            overridable: vec![],
            bind_middlewares: vec![],
            connector_middlewares: vec![],
            settings: serde_json::json!({"common": {"servers": ["nats://127.0.0.1:1"]}, "routes": {
                "route": {"publish_subject": "events.created", "stream": "EVENTS", "consumer": "worker"}
            }}),
        },
    )]);
    let catalog = Arc::new(
        Catalog::compile(
            &configs,
            Arc::new(GenerationCompiler::without_middlewares(registry)),
        )
        .await
        .expect("compile catalog without NATS I/O"),
    );
    let (mut native, server) = tokio::io::duplex(4096);
    let native_catalog = Arc::clone(&catalog);
    let native_task = tokio::spawn(async move {
        fujin_native::run(
            server,
            native_catalog,
            Arc::new(NoBindMiddleware),
            "nats-test",
        )
        .await
    });
    let mut hello = vec![RequestCode::Hello as u8, 1, 1, 1];
    append_bytes(&mut hello, b"client");
    append_bytes(&mut hello, b"build");
    native.write_all(&hello).await.expect("HELLO");
    let mut hello_response = [0; 4];
    native
        .read_exact(&mut hello_response)
        .await
        .expect("HELLO response");
    assert_eq!(hello_response, [ResponseCode::Hello as u8, 0, 1, 1]);
    read_bytes(&mut native).await;
    let mut bind = vec![RequestCode::Bind as u8];
    append_bytes(&mut bind, b"events");
    bind.extend_from_slice(&[0; 4]);
    native.write_all(&bind).await.expect("BIND");
    let mut header = [0; 6];
    native.read_exact(&mut header).await.expect("BIND response");
    assert_eq!(&header, &[ResponseCode::Bind as u8, 0, 0, 0, 0, 1]);
    assert_eq!(read_bytes(&mut native).await, b"route");
    let mut native_profile = [0; 4];
    native
        .read_exact(&mut native_profile)
        .await
        .expect("profile");
    assert_eq!(native_profile, [0x39, 2, 1, 1]);
    let mut hproduce = vec![RequestCode::HProduce as u8];
    hproduce.extend_from_slice(&1_u32.to_be_bytes());
    append_bytes(&mut hproduce, b"route");
    hproduce.extend_from_slice(&0_u16.to_be_bytes());
    append_bytes(&mut hproduce, b"payload");
    native.write_all(&hproduce).await.expect("HPRODUCE");
    let native_header_status = read_failure(&mut native, ResponseCode::HProduce, 1).await;
    let mut begin = vec![RequestCode::BeginTransaction as u8];
    begin.extend_from_slice(&2_u32.to_be_bytes());
    append_bytes(&mut begin, b"route");
    native.write_all(&begin).await.expect("BEGIN TX");
    let native_tx_status = read_failure(&mut native, ResponseCode::BeginTransaction, 2).await;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listen");
    let address = listener.local_addr().expect("address");
    let service = GrpcService::new(Arc::clone(&catalog), Arc::new(NoBindMiddleware));
    let (stop, done) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        Server::builder()
            .add_service(pb::fujin_service_server::FujinServiceServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = done.await;
            })
            .await
    });
    let mut client =
        pb::fujin_service_client::FujinServiceClient::connect(format!("http://{address}"))
            .await
            .expect("gRPC connect");
    let (send, recv) = mpsc::unbounded_channel();
    let mut stream = client
        .stream(UnboundedReceiverStream::new(recv))
        .await
        .expect("gRPC stream")
        .into_inner();
    send.send(pb::FujinRequest {
        request: Some(pb::fujin_request::Request::Bind(pb::BindRequest {
            connector: "events".into(),
            meta: HashMap::new(),
            config_overrides: HashMap::new(),
        })),
    })
    .expect("BIND");
    let bind = timeout(Duration::from_secs(2), stream.message())
        .await
        .expect("BIND deadline")
        .expect("stream")
        .expect("BIND message");
    let Some(pb::fujin_response::Response::Bind(bind)) = bind.response else {
        panic!("expected bind response")
    };
    assert!(bind.error.is_none());
    let profile = &bind.routes["route"];
    assert!(profile.produce && profile.fetch && profile.subscribe && profile.manual_settlement);
    assert!(!profile.headers && !profile.transactions);
    assert_eq!(
        profile.produce_guarantee,
        pb::ProduceGuarantee::PeerAccept as i32
    );
    assert_eq!(native_profile[0] & 0x01 != 0, profile.produce);
    assert_eq!(native_profile[0] & 0x02 != 0, profile.headers);
    assert_eq!(native_profile[0] & 0x04 != 0, profile.transactions);
    send.send(pb::FujinRequest {
        request: Some(pb::fujin_request::Request::Hproduce(pb::HProduceRequest {
            correlation_id: 1,
            route: "route".into(),
            headers: vec![],
            message: b"payload".to_vec(),
        })),
    })
    .expect("HPRODUCE");
    let response = timeout(Duration::from_secs(2), stream.message())
        .await
        .expect("HPRODUCE deadline")
        .expect("stream")
        .expect("response");
    let Some(pb::fujin_response::Response::Hproduce(response)) = response.response else {
        panic!("expected HPRODUCE response")
    };
    assert_eq!(
        response.error.expect("unsupported header").code,
        i32::from(native_header_status)
    );
    send.send(pb::FujinRequest {
        request: Some(pb::fujin_request::Request::BeginTx(pb::BeginTxRequest {
            correlation_id: 2,
            route: "route".into(),
        })),
    })
    .expect("BEGIN TX");
    let response = timeout(Duration::from_secs(2), stream.message())
        .await
        .expect("BEGIN deadline")
        .expect("stream")
        .expect("response");
    let Some(pb::fujin_response::Response::BeginTx(response)) = response.response else {
        panic!("expected BEGIN response")
    };
    assert_eq!(
        response.error.expect("unsupported transaction").code,
        i32::from(native_tx_status)
    );
    drop(send);
    drop(stream);
    drop(client);
    drop(native);
    timeout(Duration::from_secs(2), native_task)
        .await
        .expect("native shutdown")
        .expect("native task")
        .expect("native session");
    stop.send(()).expect("stop gRPC");
    timeout(Duration::from_secs(2), server_task)
        .await
        .expect("gRPC shutdown")
        .expect("gRPC task")
        .expect("gRPC server");
    catalog.close().await.expect("close catalog");
}
