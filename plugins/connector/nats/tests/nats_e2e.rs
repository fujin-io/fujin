use std::{
    collections::BTreeMap,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_nats::jetstream::{
    self,
    consumer::{AckPolicy, PullConsumer, PushConsumer, pull, push},
    stream::{Config as StreamConfig, StorageType},
};
use bytes::Bytes;
use fujin_connector::{
    Binding, Catalog, Completion, CompletionSink, ConnectorConfig, ConnectorRegistry, Delivery,
    GenerationCompiler, Message, OperationToken, Reader, ReaderEvent, ReaderEventSink,
    SettlementKind, SettlementResult,
};
use fujin_error::CoreError;
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};

const SERVER: &str = "nats://127.0.0.1:4222";
const BROKER_DEADLINE: Duration = Duration::from_secs(8);
const QUIET_PERIOD: Duration = Duration::from_millis(200);
const INSTANCE: &str = "nats-e2e";
const ROUTE: &str = "events";

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static E2E_LOCK: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(|| tokio::sync::Mutex::new(()));

#[derive(Clone, Debug)]
struct ResourceNames {
    stream: String,
    consumer: String,
    subject: String,
}

struct BrokerFixture {
    context: jetstream::Context,
    unique: String,
    streams: Vec<String>,
}

impl BrokerFixture {
    async fn connect() -> Self {
        let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let unique = format!("{}_{}_{}", std::process::id(), nanos, sequence);
        let client = tokio::time::timeout(
            BROKER_DEADLINE,
            async_nats::ConnectOptions::new()
                .connection_timeout(BROKER_DEADLINE)
                .max_reconnects(1)
                .connect(SERVER),
        )
        .await
        .expect("NATS connection deadline")
        .expect("connect to the FUJIN_NATS_E2E broker");
        let mut context = jetstream::new(client);
        context.set_timeout(BROKER_DEADLINE);
        Self {
            context,
            unique,
            streams: Vec::new(),
        }
    }

    async fn create_stream(&mut self, label: &str) -> ResourceNames {
        let stream = format!("FJ_{}_{}", self.unique, label).to_ascii_uppercase();
        let consumer = format!("C_{}_{}", self.unique, label).to_ascii_uppercase();
        let subject = format!("fujin_e2e.{}.{}.events", self.unique, label);
        tokio::time::timeout(
            BROKER_DEADLINE,
            self.context.create_stream(StreamConfig {
                name: stream.clone(),
                subjects: vec![subject.clone()],
                storage: StorageType::Memory,
                allow_direct: true,
                max_age: Duration::from_secs(120),
                ..StreamConfig::default()
            }),
        )
        .await
        .expect("create stream deadline")
        .expect("create isolated JetStream stream");
        self.streams.push(stream.clone());
        ResourceNames {
            stream,
            consumer,
            subject,
        }
    }

    async fn create_pull_stream(&mut self, label: &str) -> ResourceNames {
        let names = self.create_stream(label).await;
        let stream = self
            .context
            .get_stream(&names.stream)
            .await
            .expect("look up isolated stream");
        let _: PullConsumer = stream
            .create_consumer(pull::Config {
                durable_name: Some(names.consumer.clone()),
                ack_policy: AckPolicy::Explicit,
                ack_wait: Duration::from_secs(30),
                max_ack_pending: 128,
                memory_storage: true,
                ..pull::Config::default()
            })
            .await
            .expect("create isolated durable pull consumer");
        names
    }

    async fn create_push_consumer(&self, names: &ResourceNames) {
        let stream = self
            .context
            .get_stream(&names.stream)
            .await
            .expect("look up isolated stream");
        let _: PushConsumer = stream
            .create_consumer(push::Config {
                deliver_subject: format!("fujin_e2e.{}.{}.push", self.unique, names.consumer),
                durable_name: Some(names.consumer.clone()),
                ack_policy: AckPolicy::Explicit,
                memory_storage: true,
                ..push::Config::default()
            })
            .await
            .expect("create incompatible durable push consumer");
    }

    async fn publish(&self, subject: &str, payload: &'static [u8]) {
        let acknowledgment = tokio::time::timeout(
            BROKER_DEADLINE,
            self.context
                .publish(subject.to_owned(), Bytes::from_static(payload)),
        )
        .await
        .expect("publish request deadline")
        .expect("submit fixture publication");
        tokio::time::timeout(BROKER_DEADLINE, acknowledgment)
            .await
            .expect("publication acknowledgment deadline")
            .expect("fixture publication acknowledgment");
    }

    async fn stream_message(&self, names: &ResourceNames, sequence: u64) -> Bytes {
        self.context
            .get_stream(&names.stream)
            .await
            .expect("look up stream")
            .direct_get(sequence)
            .await
            .expect("read stored stream message")
            .payload
    }

    async fn stream_message_count(&self, names: &ResourceNames) -> u64 {
        self.context
            .get_stream(&names.stream)
            .await
            .expect("look up stream")
            .cached_info()
            .state
            .messages
    }

    async fn consumer_info(&self, names: &ResourceNames) -> async_nats::jetstream::consumer::Info {
        self.context
            .get_stream(&names.stream)
            .await
            .expect("look up stream")
            .consumer_info(&names.consumer)
            .await
            .expect("look up consumer")
    }

    async fn wait_for_consumer<F>(
        &self,
        names: &ResourceNames,
        description: &str,
        predicate: F,
    ) -> async_nats::jetstream::consumer::Info
    where
        F: Fn(&async_nats::jetstream::consumer::Info) -> bool,
    {
        let deadline = Instant::now() + BROKER_DEADLINE;
        loop {
            let info = self.consumer_info(names).await;
            if predicate(&info) {
                return info;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {description}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn cleanup(mut self) {
        for stream in self.streams.drain(..).rev() {
            tokio::time::timeout(BROKER_DEADLINE, self.context.delete_stream(stream))
                .await
                .expect("delete stream deadline")
                .expect("delete isolated JetStream stream");
        }
        tokio::time::timeout(BROKER_DEADLINE, self.context.client().drain())
            .await
            .expect("drain fixture connection deadline")
            .expect("drain fixture NATS connection");
    }
}

#[derive(Debug)]
struct CompletionChannel(mpsc::UnboundedSender<Completion>);

impl CompletionSink for CompletionChannel {
    fn complete(&self, completion: Completion) {
        let _ = self.0.send(completion);
    }
}

#[derive(Debug)]
struct EventChannel(mpsc::UnboundedSender<ReaderEvent>);

impl ReaderEventSink for EventChannel {
    fn emit(&self, event: ReaderEvent) {
        let _ = self.0.send(event);
    }
}

#[derive(Debug)]
enum SubscriptionObservation {
    Ready(fujin_connector::Result<()>),
    Event(ReaderEvent),
}

#[derive(Debug)]
struct ObservationChannel(mpsc::UnboundedSender<SubscriptionObservation>);

impl ReaderEventSink for ObservationChannel {
    fn emit(&self, event: ReaderEvent) {
        let _ = self.0.send(SubscriptionObservation::Event(event));
    }
}

fn enabled() -> bool {
    std::env::var("FUJIN_NATS_E2E").is_ok_and(|value| value == "1")
}

async fn serial_test() -> tokio::sync::MutexGuard<'static, ()> {
    E2E_LOCK.lock().await
}

fn operation(value: u64) -> OperationToken {
    OperationToken::external(value).expect("external operation token")
}

fn connector_configs(routes: Value) -> BTreeMap<String, ConnectorConfig> {
    BTreeMap::from([(
        INSTANCE.to_owned(),
        ConnectorConfig {
            connector_type: "nats".to_owned(),
            overridable: Vec::new(),
            bind_middlewares: Vec::new(),
            connector_middlewares: Vec::new(),
            settings: json!({
                "common": { "servers": [SERVER] },
                "routes": routes,
            }),
        },
    )])
}

async fn compile_catalog(routes: Value) -> Arc<Catalog> {
    let registry = Arc::new(ConnectorRegistry::default());
    registry
        .register_plugin(fujin_connector_nats::plugin())
        .expect("register NATS connector plugin");
    let compiler = Arc::new(GenerationCompiler::without_middlewares(registry));
    Arc::new(
        Catalog::compile(&connector_configs(routes), compiler)
            .await
            .expect("compile NATS connector catalog"),
    )
}

fn acquire(catalog: &Arc<Catalog>) -> Binding {
    catalog
        .current()
        .expect("published connector generation")
        .acquire(INSTANCE)
        .expect("bind NATS connector generation")
}

fn open_writer(
    binding: &Binding,
) -> (
    Arc<dyn fujin_connector::Writer>,
    mpsc::UnboundedReceiver<Completion>,
) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let sink: Arc<dyn CompletionSink> = Arc::new(CompletionChannel(sender));
    let writer = binding.open_writer(ROUTE, sink).expect("open NATS writer");
    (writer, receiver)
}

fn open_reader(
    binding: &Binding,
    auto_settle: bool,
) -> (Arc<dyn Reader>, mpsc::UnboundedReceiver<ReaderEvent>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let sink: Arc<dyn ReaderEventSink> = Arc::new(EventChannel(sender));
    let reader = binding
        .open_reader(ROUTE, auto_settle, sink)
        .expect("open NATS reader");
    (reader, receiver)
}

async fn next_completion(receiver: &mut mpsc::UnboundedReceiver<Completion>) -> Completion {
    tokio::time::timeout(BROKER_DEADLINE, receiver.recv())
        .await
        .expect("completion deadline")
        .expect("completion channel closed")
}

async fn next_event(receiver: &mut mpsc::UnboundedReceiver<ReaderEvent>) -> ReaderEvent {
    tokio::time::timeout(BROKER_DEADLINE, receiver.recv())
        .await
        .expect("reader event deadline")
        .expect("reader event channel closed")
}

async fn fetch(
    reader: &Arc<dyn Reader>,
    receiver: &mut mpsc::UnboundedReceiver<ReaderEvent>,
    token: u64,
    maximum: u32,
) -> Vec<Delivery> {
    reader
        .fetch(operation(token), maximum, false)
        .expect("accept NATS fetch");
    match next_event(receiver).await {
        ReaderEvent::FetchComplete {
            token: actual,
            reported_count,
            messages,
            result,
        } => {
            assert_eq!(actual, operation(token));
            result.expect("NATS fetch result");
            assert_eq!(reported_count as usize, messages.len());
            assert!(reported_count <= maximum);
            messages
        }
        other => panic!("unexpected reader event during fetch: {other:?}"),
    }
}

async fn settle(
    reader: &Arc<dyn Reader>,
    receiver: &mut mpsc::UnboundedReceiver<ReaderEvent>,
    token: u64,
    kind: SettlementKind,
    message_ids: Vec<Bytes>,
) -> Vec<SettlementResult> {
    let request = message_ids
        .into_iter()
        .map(|message_id| SettlementResult {
            message_id,
            result: Ok(()),
        })
        .collect();
    reader
        .settle(operation(token), kind, request)
        .expect("accept NATS settlement");
    match next_event(receiver).await {
        ReaderEvent::SettlementComplete {
            token: actual,
            result,
            messages,
        } => {
            assert_eq!(actual, operation(token));
            result.expect("NATS settlement batch result");
            for message in &messages {
                message.result.clone().expect("NATS message settlement");
            }
            messages
        }
        other => panic!("unexpected reader event during settlement: {other:?}"),
    }
}

async fn close_catalog(catalog: &Arc<Catalog>, binding: Binding) {
    drop(binding);
    tokio::time::timeout(BROKER_DEADLINE, catalog.close())
        .await
        .expect("catalog close deadline")
        .expect("close NATS catalog");
}

fn route(names: &ResourceNames) -> Value {
    json!({
        "events": {
            "publish_subject": names.subject,
            "stream": names.stream,
            "consumer": names.consumer,
        }
    })
}

#[tokio::test]
async fn publish_waits_for_jetstream_puback() {
    if !enabled() {
        return;
    }
    let _serial = serial_test().await;
    let mut fixture = BrokerFixture::connect().await;
    let names = fixture.create_pull_stream("PUBACK").await;
    let catalog = compile_catalog(route(&names)).await;
    let binding = acquire(&catalog);
    let (writer, mut completions) = open_writer(&binding);

    writer
        .produce(
            operation(1),
            Message::new(Bytes::from_static(b"persisted-before-completion")),
        )
        .expect("accept publication");
    let completion = next_completion(&mut completions).await;
    assert_eq!(completion.token, operation(1));
    completion
        .result
        .expect("JetStream publication acknowledgment");
    assert_eq!(fixture.stream_message_count(&names).await, 1);
    assert_eq!(
        fixture.stream_message(&names, 1).await,
        Bytes::from_static(b"persisted-before-completion")
    );

    Arc::clone(&writer).close().await.expect("close writer");
    drop(writer);
    close_catalog(&catalog, binding).await;
    fixture.cleanup().await;
}

#[tokio::test]
async fn publish_broker_rejection_reports_failure_once() {
    if !enabled() {
        return;
    }
    let _serial = serial_test().await;
    let mut fixture = BrokerFixture::connect().await;
    let names = fixture.create_pull_stream("REJECT").await;
    let rejected_subject = format!("{}.no_matching_stream", names.subject);
    let catalog = compile_catalog(json!({
        "events": { "publish_subject": rejected_subject }
    }))
    .await;
    let binding = acquire(&catalog);
    let (writer, mut completions) = open_writer(&binding);

    writer
        .produce(operation(7), Message::new(Bytes::from_static(b"reject-me")))
        .expect("connector accepts responsibility for publication");
    let completion = next_completion(&mut completions).await;
    assert_eq!(completion.token, operation(7));
    assert!(
        completion.result.is_err(),
        "broker rejection must not become success"
    );
    tokio::time::sleep(QUIET_PERIOD).await;
    assert!(
        matches!(
            completions.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ),
        "rejected publication completed more than once"
    );
    assert_eq!(fixture.stream_message_count(&names).await, 0);

    Arc::clone(&writer).close().await.expect("close writer");
    drop(writer);
    close_catalog(&catalog, binding).await;
    fixture.cleanup().await;
}

#[tokio::test]
async fn broker_disconnect_fails_publication_once() {
    if !enabled() {
        return;
    }
    let registry = Arc::new(ConnectorRegistry::default());
    registry
        .register_plugin(fujin_connector_nats::plugin())
        .expect("register NATS");
    let mut configs =
        connector_configs(json!({"events": {"publish_subject": "events.disconnected"}}));
    configs.get_mut(INSTANCE).expect("connector").settings["common"]["servers"] =
        json!(["nats://127.0.0.1:1"]);
    let catalog = Arc::new(
        Catalog::compile(
            &configs,
            Arc::new(GenerationCompiler::without_middlewares(registry)),
        )
        .await
        .expect("compile disconnected broker route without I/O"),
    );
    let binding = acquire(&catalog);
    let (writer, mut completions) = open_writer(&binding);
    writer
        .produce(
            operation(1),
            Message::new(Bytes::from_static(b"cannot-connect")),
        )
        .expect("accept publication while disconnected");
    let completed = next_completion(&mut completions).await;
    assert_eq!(completed.token, operation(1));
    assert!(matches!(completed.result, Err(CoreError::Unavailable(_))));
    Arc::clone(&writer)
        .close()
        .await
        .expect("close disconnected writer");
    assert!(matches!(
        completions.try_recv(),
        Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
    ));
    drop(writer);
    close_catalog(&catalog, binding).await;
}

#[tokio::test]
async fn flush_is_snapshot_barrier() {
    if !enabled() {
        return;
    }
    let _serial = serial_test().await;
    let mut fixture = BrokerFixture::connect().await;
    let names = fixture.create_pull_stream("FLUSH").await;
    let catalog = compile_catalog(route(&names)).await;
    let binding = acquire(&catalog);
    let (writer, mut completions) = open_writer(&binding);

    writer
        .produce(operation(1), Message::new(Bytes::from_static(b"before")))
        .expect("accept publication before flush");
    writer.flush(operation(2)).expect("accept flush barrier");
    writer
        .produce(operation(3), Message::new(Bytes::from_static(b"after")))
        .expect("accept publication after flush");

    let first = next_completion(&mut completions).await;
    let barrier = next_completion(&mut completions).await;
    assert_eq!(first.token, operation(1));
    first.result.expect("publication before barrier");
    assert_eq!(barrier.token, operation(2));
    barrier.result.expect("snapshot flush barrier");
    let later = next_completion(&mut completions).await;
    assert_eq!(later.token, operation(3));
    later.result.expect("publication after barrier");
    assert_eq!(fixture.stream_message_count(&names).await, 2);

    Arc::clone(&writer).close().await.expect("close writer");
    drop(writer);
    close_catalog(&catalog, binding).await;
    fixture.cleanup().await;
}

#[tokio::test]
async fn missing_or_incompatible_consumer_is_rejected() {
    if !enabled() {
        return;
    }
    let _serial = serial_test().await;
    let mut fixture = BrokerFixture::connect().await;
    let missing = fixture.create_stream("MISSING").await;
    let incompatible = fixture.create_stream("PUSH").await;
    fixture.create_push_consumer(&incompatible).await;
    let catalog = compile_catalog(json!({
        "missing": {
            "stream": missing.stream,
            "consumer": missing.consumer,
        },
        "incompatible": {
            "stream": incompatible.stream,
            "consumer": incompatible.consumer,
        }
    }))
    .await;
    let binding = acquire(&catalog);

    let (missing_events_sender, mut missing_events) = mpsc::unbounded_channel();
    let missing_sink: Arc<dyn ReaderEventSink> = Arc::new(EventChannel(missing_events_sender));
    let missing_reader = binding
        .open_reader("missing", false, missing_sink)
        .expect("open reader before asynchronous broker validation");
    let (ready_sender, ready_receiver) = oneshot::channel();
    missing_reader
        .subscribe(
            false,
            Box::new(move |result| {
                let _ = ready_sender.send(result.clone());
                result
            }),
        )
        .expect("accept subscription startup");
    let readiness = tokio::time::timeout(BROKER_DEADLINE, ready_receiver)
        .await
        .expect("missing-consumer readiness deadline")
        .expect("missing-consumer readiness callback");
    assert!(
        matches!(readiness, Err(CoreError::Unavailable(_))),
        "missing consumer must reject readiness: {readiness:?}"
    );
    assert!(
        matches!(
            missing_events.try_recv(),
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
        ),
        "missing consumer delivered an event after rejected readiness"
    );

    let (incompatible_events_sender, mut incompatible_events) = mpsc::unbounded_channel();
    let incompatible_sink: Arc<dyn ReaderEventSink> =
        Arc::new(EventChannel(incompatible_events_sender));
    let incompatible_reader = binding
        .open_reader("incompatible", false, incompatible_sink)
        .expect("open reader before asynchronous broker validation");
    incompatible_reader
        .fetch(operation(11), 1, false)
        .expect("accept first fetch");
    match next_event(&mut incompatible_events).await {
        ReaderEvent::FetchComplete {
            token,
            reported_count,
            messages,
            result,
        } => {
            assert_eq!(token, operation(11));
            assert_eq!(reported_count, 0);
            assert!(messages.is_empty());
            assert!(
                matches!(result, Err(CoreError::Unavailable(_))),
                "push consumer must fail the first fetch: {result:?}"
            );
        }
        other => panic!("unexpected incompatible-consumer event: {other:?}"),
    }

    let missing_info = fixture
        .context
        .get_stream(&missing.stream)
        .await
        .expect("look up missing-consumer stream");
    assert_eq!(missing_info.cached_info().state.consumer_count, 0);
    let incompatible_info = fixture.consumer_info(&incompatible).await;
    assert_eq!(
        incompatible_info.config.durable_name.as_deref(),
        Some(incompatible.consumer.as_str())
    );
    assert!(incompatible_info.config.deliver_subject.is_some());
    assert_eq!(incompatible_info.config.ack_policy, AckPolicy::Explicit);

    Arc::clone(&missing_reader)
        .close()
        .await
        .expect("close missing reader");
    Arc::clone(&incompatible_reader)
        .close()
        .await
        .expect("close incompatible reader");
    drop((missing_reader, incompatible_reader));
    close_catalog(&catalog, binding).await;
    fixture.cleanup().await;
}

#[tokio::test]
async fn fetch_returns_at_most_requested_count() {
    if !enabled() {
        return;
    }
    let _serial = serial_test().await;
    let mut fixture = BrokerFixture::connect().await;
    let names = fixture.create_pull_stream("FETCH").await;
    let catalog = compile_catalog(route(&names)).await;
    let binding = acquire(&catalog);
    let (reader, mut events) = open_reader(&binding, false);

    let empty_started = Instant::now();
    let empty = fetch(&reader, &mut events, 1, 3).await;
    assert!(
        empty.is_empty(),
        "empty consumer must return an empty batch"
    );
    assert!(
        empty_started.elapsed() >= Duration::from_secs(4),
        "empty fetch returned before the configured broker wait"
    );

    fixture.publish(&names.subject, b"one").await;
    fixture.publish(&names.subject, b"two").await;
    fixture.publish(&names.subject, b"three").await;
    let messages = fetch(&reader, &mut events, 2, 2).await;
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].payload, Bytes::from_static(b"one"));
    assert_eq!(messages[1].payload, Bytes::from_static(b"two"));
    let ids = messages
        .into_iter()
        .map(|message| message.message_id.expect("manual message ID"))
        .collect();
    settle(&reader, &mut events, 3, SettlementKind::Ack, ids).await;

    Arc::clone(&reader).close().await.expect("close reader");
    drop(reader);
    close_catalog(&catalog, binding).await;
    fixture.cleanup().await;
}

#[tokio::test]
async fn subscribe_ready_precedes_first_message() {
    if !enabled() {
        return;
    }
    let _serial = serial_test().await;
    let mut fixture = BrokerFixture::connect().await;
    let names = fixture.create_pull_stream("READY").await;
    fixture
        .publish(&names.subject, b"queued-before-subscribe")
        .await;
    let catalog = compile_catalog(route(&names)).await;
    let binding = acquire(&catalog);
    let (sender, mut observations) = mpsc::unbounded_channel();
    let sink: Arc<dyn ReaderEventSink> = Arc::new(ObservationChannel(sender.clone()));
    let reader = binding
        .open_reader(ROUTE, false, sink)
        .expect("open NATS subscription reader");
    reader
        .subscribe(
            false,
            Box::new(move |result| {
                let _ = sender.send(SubscriptionObservation::Ready(result.clone()));
                result
            }),
        )
        .expect("accept subscription");

    let first = tokio::time::timeout(BROKER_DEADLINE, observations.recv())
        .await
        .expect("subscription readiness deadline")
        .expect("subscription observation");
    match first {
        SubscriptionObservation::Ready(result) => result.expect("subscription readiness"),
        other => panic!("delivery preceded readiness: {other:?}"),
    }
    let second = tokio::time::timeout(BROKER_DEADLINE, observations.recv())
        .await
        .expect("subscription delivery deadline")
        .expect("subscription observation");
    match second {
        SubscriptionObservation::Event(ReaderEvent::Message(delivery)) => {
            assert_eq!(
                delivery.payload,
                Bytes::from_static(b"queued-before-subscribe")
            );
            assert!(delivery.message_id.is_some());
        }
        other => panic!("expected first delivery after readiness, got {other:?}"),
    }

    Arc::clone(&reader).close().await.expect("close reader");
    drop(reader);
    close_catalog(&catalog, binding).await;
    fixture.cleanup().await;
}

#[tokio::test]
async fn subscription_reports_terminal_after_broker_resource_removal() {
    if !enabled() {
        return;
    }
    let _serial = serial_test().await;
    let mut fixture = BrokerFixture::connect().await;
    let names = fixture.create_pull_stream("TERMINAL").await;
    let catalog = compile_catalog(route(&names)).await;
    let binding = acquire(&catalog);
    let (sender, mut observations) = mpsc::unbounded_channel();
    let sink: Arc<dyn ReaderEventSink> = Arc::new(ObservationChannel(sender.clone()));
    let reader = binding
        .open_reader(ROUTE, false, sink)
        .expect("open subscription reader");
    reader
        .subscribe(
            false,
            Box::new(move |result| {
                let _ = sender.send(SubscriptionObservation::Ready(result.clone()));
                result
            }),
        )
        .expect("accept subscription");
    match tokio::time::timeout(BROKER_DEADLINE, observations.recv())
        .await
        .expect("readiness deadline")
        .expect("readiness")
    {
        SubscriptionObservation::Ready(Ok(())) => {}
        other => panic!("subscription did not become ready: {other:?}"),
    }
    fixture
        .context
        .delete_stream(&names.stream)
        .await
        .expect("remove backing stream");
    fixture.streams.retain(|name| name != &names.stream);
    match tokio::time::timeout(BROKER_DEADLINE, observations.recv())
        .await
        .expect("terminal deadline")
        .expect("terminal")
    {
        SubscriptionObservation::Event(ReaderEvent::Terminal(Err(CoreError::Unavailable(_)))) => {}
        other => panic!("missing terminal failure after stream removal: {other:?}"),
    }
    Arc::clone(&reader)
        .close()
        .await
        .expect("close terminated reader");
    drop(reader);
    close_catalog(&catalog, binding).await;
    fixture.cleanup().await;
}

#[tokio::test]
async fn ack_confirms_only_selected_message() {
    if !enabled() {
        return;
    }
    let _serial = serial_test().await;
    let mut fixture = BrokerFixture::connect().await;
    let names = fixture.create_pull_stream("ACK_ONE").await;
    fixture.publish(&names.subject, b"selected").await;
    fixture.publish(&names.subject, b"outstanding").await;
    let catalog = compile_catalog(route(&names)).await;
    let binding = acquire(&catalog);
    let (reader, mut events) = open_reader(&binding, false);

    let messages = fetch(&reader, &mut events, 1, 2).await;
    assert_eq!(messages.len(), 2);
    let selected = messages[0].message_id.clone().expect("selected message ID");
    let outstanding = messages[1]
        .message_id
        .clone()
        .expect("outstanding message ID");
    let (other_reader, mut other_events) = open_reader(&binding, false);
    other_reader
        .settle(
            operation(4),
            SettlementKind::Ack,
            vec![SettlementResult {
                message_id: selected.clone(),
                result: Ok(()),
            }],
        )
        .expect("accept cross-reader settlement for individual result");
    match next_event(&mut other_events).await {
        ReaderEvent::SettlementComplete { messages, .. } => {
            assert!(matches!(
                messages.as_slice(),
                [SettlementResult {
                    result: Err(CoreError::InvalidMessageId(_)),
                    ..
                }]
            ));
        }
        other => panic!("expected cross-reader settlement failure: {other:?}"),
    }
    let results = settle(
        &reader,
        &mut events,
        2,
        SettlementKind::Ack,
        vec![selected.clone()],
    )
    .await;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message_id, selected);
    reader
        .settle(
            operation(5),
            SettlementKind::Ack,
            vec![SettlementResult {
                message_id: selected.clone(),
                result: Ok(()),
            }],
        )
        .expect("accept stale settlement for individual result");
    match next_event(&mut events).await {
        ReaderEvent::SettlementComplete { messages, .. } => {
            assert!(matches!(
                messages.as_slice(),
                [SettlementResult {
                    result: Err(CoreError::InvalidMessageId(_)),
                    ..
                }]
            ));
        }
        other => panic!("expected stale settlement failure: {other:?}"),
    }

    let info = fixture.consumer_info(&names).await;
    assert_eq!(info.ack_floor.consumer_sequence, 1);
    assert_eq!(info.delivered.consumer_sequence, 2);
    assert_eq!(
        info.num_ack_pending, 1,
        "unselected delivery was also acknowledged"
    );
    settle(
        &reader,
        &mut events,
        3,
        SettlementKind::Ack,
        vec![outstanding],
    )
    .await;

    Arc::clone(&reader).close().await.expect("close reader");
    Arc::clone(&other_reader)
        .close()
        .await
        .expect("close other reader");
    drop(other_reader);
    drop(reader);
    close_catalog(&catalog, binding).await;
    fixture.cleanup().await;
}

#[tokio::test]
async fn nack_requeues_delivery() {
    if !enabled() {
        return;
    }
    let _serial = serial_test().await;
    let mut fixture = BrokerFixture::connect().await;
    let names = fixture.create_pull_stream("NACK").await;
    fixture.publish(&names.subject, b"redeliver-me").await;
    let catalog = compile_catalog(route(&names)).await;
    let binding = acquire(&catalog);
    let (reader, mut events) = open_reader(&binding, false);

    let first = fetch(&reader, &mut events, 1, 1).await;
    let first_id = first[0].message_id.clone().expect("first delivery ID");
    settle(
        &reader,
        &mut events,
        2,
        SettlementKind::Nack,
        vec![first_id.clone()],
    )
    .await;
    let redelivered = fetch(&reader, &mut events, 3, 1).await;
    assert_eq!(redelivered.len(), 1);
    assert_eq!(redelivered[0].payload, Bytes::from_static(b"redeliver-me"));
    let redelivered_id = redelivered[0]
        .message_id
        .clone()
        .expect("redelivery message ID");
    assert_ne!(redelivered_id, first_id);
    let info = fixture.consumer_info(&names).await;
    assert!(
        info.num_redelivered >= 1,
        "broker did not record a redelivery"
    );
    settle(
        &reader,
        &mut events,
        4,
        SettlementKind::Ack,
        vec![redelivered_id],
    )
    .await;

    Arc::clone(&reader).close().await.expect("close reader");
    drop(reader);
    close_catalog(&catalog, binding).await;
    fixture.cleanup().await;
}

#[tokio::test]
async fn auto_settle_acknowledges_before_delivery() {
    if !enabled() {
        return;
    }
    let _serial = serial_test().await;
    let mut fixture = BrokerFixture::connect().await;
    let names = fixture.create_pull_stream("AUTO_ACK").await;
    fixture.publish(&names.subject, b"auto-settled").await;
    let catalog = compile_catalog(route(&names)).await;
    let binding = acquire(&catalog);
    let (sender, mut observations) = mpsc::unbounded_channel();
    let sink: Arc<dyn ReaderEventSink> = Arc::new(ObservationChannel(sender.clone()));
    let reader = binding
        .open_reader(ROUTE, true, sink)
        .expect("open auto-settle reader");
    reader
        .subscribe(
            false,
            Box::new(move |result| {
                let _ = sender.send(SubscriptionObservation::Ready(result.clone()));
                result
            }),
        )
        .expect("accept auto-settle subscription");

    match tokio::time::timeout(BROKER_DEADLINE, observations.recv())
        .await
        .expect("auto-settle readiness deadline")
        .expect("auto-settle readiness observation")
    {
        SubscriptionObservation::Ready(result) => result.expect("auto-settle readiness"),
        other => panic!("delivery preceded auto-settle readiness: {other:?}"),
    }
    let delivery = match tokio::time::timeout(BROKER_DEADLINE, observations.recv())
        .await
        .expect("auto-settle delivery deadline")
        .expect("auto-settle delivery observation")
    {
        SubscriptionObservation::Event(ReaderEvent::Message(delivery)) => delivery,
        other => panic!("expected auto-settle delivery, got {other:?}"),
    };
    assert_eq!(delivery.payload, Bytes::from_static(b"auto-settled"));
    assert!(
        delivery.message_id.is_none(),
        "auto-settled delivery exposed a settleable ID"
    );
    let info = fixture.consumer_info(&names).await;
    assert_eq!(info.ack_floor.consumer_sequence, 1);
    assert_eq!(info.ack_floor.stream_sequence, 1);
    assert_eq!(
        info.num_ack_pending, 0,
        "delivery was emitted before broker ACK confirmation"
    );

    Arc::clone(&reader).close().await.expect("close reader");
    drop(reader);
    close_catalog(&catalog, binding).await;
    fixture.cleanup().await;
}

#[tokio::test]
async fn close_resolves_pending_work_and_releases_resources() {
    if !enabled() {
        return;
    }
    let _serial = serial_test().await;
    let mut fixture = BrokerFixture::connect().await;
    let names = fixture.create_pull_stream("CLOSE").await;
    let rejected_subject = format!("{}.not_captured", names.subject);
    let catalog = compile_catalog(json!({
        "events": {
            "publish_subject": rejected_subject,
            "stream": names.stream,
            "consumer": names.consumer,
        }
    }))
    .await;
    let binding = acquire(&catalog);
    let (writer, mut completions) = open_writer(&binding);
    let (reader, mut events) = open_reader(&binding, false);

    writer
        .produce(
            operation(1),
            Message::new(Bytes::from_static(b"pending-on-close")),
        )
        .expect("accept publication before close");
    reader
        .fetch(operation(2), 1, false)
        .expect("accept empty pull before close");
    fixture
        .wait_for_consumer(&names, "pending pull request", |info| info.num_waiting > 0)
        .await;

    tokio::time::timeout(BROKER_DEADLINE, Arc::clone(&reader).close())
        .await
        .expect("reader close deadline")
        .expect("close reader with pending pull");
    tokio::time::timeout(BROKER_DEADLINE, Arc::clone(&writer).close())
        .await
        .expect("writer close deadline")
        .expect("close writer with pending publication");

    let publication = next_completion(&mut completions).await;
    assert_eq!(publication.token, operation(1));
    assert!(publication.result.is_err());
    match next_event(&mut events).await {
        ReaderEvent::FetchComplete {
            token,
            reported_count,
            messages,
            result,
        } => {
            assert_eq!(token, operation(2));
            assert_eq!(reported_count, 0);
            assert!(messages.is_empty());
            assert_eq!(result, Err(CoreError::Closed));
        }
        other => panic!("unexpected close completion event: {other:?}"),
    }
    assert_eq!(
        writer.produce(operation(3), Message::new(Bytes::new())),
        Err(CoreError::Closed)
    );
    assert_eq!(reader.fetch(operation(4), 1, false), Err(CoreError::Closed));
    tokio::time::sleep(QUIET_PERIOD).await;
    assert!(
        matches!(
            completions.try_recv(),
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
        ),
        "accepted publication completed more than once"
    );
    fixture
        .wait_for_consumer(&names, "pending pull release", |info| info.num_waiting == 0)
        .await;

    drop((writer, reader));
    close_catalog(&catalog, binding).await;
    fixture.cleanup().await;
}

#[tokio::test]
async fn nats_replacement_preserves_bound_generation() {
    if !enabled() {
        return;
    }
    let _serial = serial_test().await;
    let mut fixture = BrokerFixture::connect().await;
    let original = fixture.create_pull_stream("GEN_OLD").await;
    let replacement = fixture.create_pull_stream("GEN_NEW").await;
    let catalog = compile_catalog(route(&original)).await;
    let original_generation = catalog.current().expect("original generation");
    let original_generation_id = original_generation.id();
    let original_binding = original_generation
        .acquire(INSTANCE)
        .expect("bind original generation");

    let replacement_generation = catalog
        .reload(&connector_configs(route(&replacement)))
        .await
        .expect("publish replacement NATS generation");
    assert_ne!(replacement_generation.id(), original_generation_id);
    assert!(
        catalog
            .status()
            .draining
            .iter()
            .any(|status| { status.id == original_generation_id && status.bindings == 1 }),
        "original bound generation was not retained while draining"
    );
    let replacement_binding = replacement_generation
        .acquire(INSTANCE)
        .expect("bind replacement generation");

    let (original_writer, mut original_completions) = open_writer(&original_binding);
    let (replacement_writer, mut replacement_completions) = open_writer(&replacement_binding);
    original_writer
        .produce(
            operation(1),
            Message::new(Bytes::from_static(b"old-generation")),
        )
        .expect("publish through original binding after replacement");
    replacement_writer
        .produce(
            operation(2),
            Message::new(Bytes::from_static(b"new-generation")),
        )
        .expect("publish through replacement binding");
    next_completion(&mut original_completions)
        .await
        .result
        .expect("original generation publication");
    next_completion(&mut replacement_completions)
        .await
        .result
        .expect("replacement generation publication");
    assert_eq!(
        fixture.stream_message(&original, 1).await,
        Bytes::from_static(b"old-generation")
    );
    assert_eq!(
        fixture.stream_message(&replacement, 1).await,
        Bytes::from_static(b"new-generation")
    );

    Arc::clone(&original_writer)
        .close()
        .await
        .expect("close original writer");
    Arc::clone(&replacement_writer)
        .close()
        .await
        .expect("close replacement writer");
    drop((original_writer, replacement_writer, original_binding));
    tokio::time::timeout(BROKER_DEADLINE, original_generation.wait_closed())
        .await
        .expect("original generation drain deadline")
        .expect("retire original generation");
    close_catalog(&catalog, replacement_binding).await;
    fixture.cleanup().await;
}
