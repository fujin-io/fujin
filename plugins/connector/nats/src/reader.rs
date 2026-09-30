use std::{
    collections::HashMap,
    fmt,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use async_nats::jetstream::{
    self, AckKind,
    consumer::{AckPolicy, PullConsumer, pull},
};
use bytes::Bytes;
use fujin_connector::{
    BoxFuture, Delivery, OperationToken, Reader, ReaderEvent, ReaderEventSink, ReadyCallback,
    SettlementKind, SettlementResult,
};
use fujin_error::{CoreError, Result};
use futures_util::StreamExt;
use parking_lot::Mutex;
use tokio::{sync::mpsc, task::JoinHandle, time};
use tokio_util::sync::CancellationToken;

use crate::{
    Shared,
    config::{BROKER_TIMEOUT, RouteConfig},
};

const COMMAND_CAPACITY: usize = 64;
const SUBSCRIPTION_BATCH_SIZE: usize = 64;
const MAX_FETCH_MESSAGES: usize = 1_024;
const MAX_OUTSTANDING_MESSAGES: usize = 4_096;
const MESSAGE_ID_LEN: usize = size_of::<u64>() * 2;
static NEXT_READER_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) struct NatsReader {
    auto_settle: bool,
    commands: mpsc::Sender<ReaderCommand>,
    subscription_started: AtomicBool,
    shutdown: CancellationToken,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl fmt::Debug for NatsReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NatsReader")
            .field("auto_settle", &self.auto_settle)
            .finish_non_exhaustive()
    }
}

enum ReaderCommand {
    Subscribe {
        ready: ReadyCallback,
    },
    Fetch {
        token: OperationToken,
        maximum: u32,
    },
    Settle {
        token: OperationToken,
        kind: SettlementKind,
        settlements: Vec<SettlementResult>,
    },
}

impl NatsReader {
    pub(crate) fn new(
        shared: Arc<Shared>,
        config: RouteConfig,
        auto_settle: bool,
        events: Arc<dyn ReaderEventSink>,
    ) -> Arc<Self> {
        let (commands, receiver) = mpsc::channel(COMMAND_CAPACITY);
        let shutdown = shared.shutdown.child_token();
        let reader_id = NEXT_READER_ID.fetch_add(1, Ordering::Relaxed);
        let worker = tokio::spawn(reader_worker(
            shared,
            config,
            auto_settle,
            reader_id,
            events,
            receiver,
            shutdown.clone(),
        ));
        Arc::new(Self {
            auto_settle,
            commands,
            subscription_started: AtomicBool::new(false),
            shutdown,
            worker: Mutex::new(Some(worker)),
        })
    }

    fn send(&self, command: ReaderCommand) -> Result<()> {
        if self.shutdown.is_cancelled() {
            return Err(CoreError::Closed);
        }
        self.commands
            .try_send(command)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    CoreError::ResourceExhausted("NATS reader command queue is full".into())
                }
                mpsc::error::TrySendError::Closed(_) => CoreError::Closed,
            })
    }
}

impl Reader for NatsReader {
    fn subscribe(&self, with_headers: bool, ready: ReadyCallback) -> Result<()> {
        if with_headers {
            return Err(CoreError::OperationUnsupported);
        }
        if self
            .subscription_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(CoreError::OperationUnsupported);
        }
        if let Err(error) = self.send(ReaderCommand::Subscribe { ready }) {
            self.subscription_started.store(false, Ordering::Release);
            return Err(error);
        }
        Ok(())
    }

    fn fetch(&self, token: OperationToken, maximum: u32, with_headers: bool) -> Result<()> {
        if with_headers {
            return Err(CoreError::OperationUnsupported);
        }
        if maximum == 0 {
            return Err(CoreError::InvalidBatchSize);
        }
        self.send(ReaderCommand::Fetch { token, maximum })
    }

    fn settle(
        &self,
        token: OperationToken,
        kind: SettlementKind,
        settlements: Vec<SettlementResult>,
    ) -> Result<()> {
        if self.auto_settle {
            return Err(CoreError::OperationUnsupported);
        }
        self.send(ReaderCommand::Settle {
            token,
            kind,
            settlements,
        })
    }

    fn adapter_message_id_prefix_len(&self) -> usize {
        MESSAGE_ID_LEN
    }

    fn auto_settle(&self) -> bool {
        self.auto_settle
    }

    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            self.shutdown.cancel();
            let worker = self.worker.lock().take();
            if let Some(worker) = worker {
                worker.await.map_err(|error| {
                    CoreError::Internal(format!("join NATS reader worker: {error}"))
                })?;
            }
            Ok(())
        })
    }
}

async fn reader_worker(
    shared: Arc<Shared>,
    config: RouteConfig,
    auto_settle: bool,
    reader_id: u64,
    events: Arc<dyn ReaderEventSink>,
    mut commands: mpsc::Receiver<ReaderCommand>,
    shutdown: CancellationToken,
) {
    let mut consumer = None;
    let mut subscribed = false;
    let mut subscription_batch = None;
    let mut outstanding = HashMap::new();
    let mut next_message_id = 1_u64;

    loop {
        if shutdown.is_cancelled() {
            close_pending(&mut commands, events.as_ref());
            return;
        }

        let subscription_capacity = if auto_settle {
            SUBSCRIPTION_BATCH_SIZE
        } else {
            MAX_OUTSTANDING_MESSAGES
                .saturating_sub(outstanding.len())
                .min(SUBSCRIPTION_BATCH_SIZE)
        };
        if subscribed && subscription_batch.is_none() && subscription_capacity > 0 {
            let attached = consumer.as_ref().expect("subscription consumer attached");
            match create_batch(attached, subscription_capacity, &shutdown).await {
                Ok(batch) => subscription_batch = Some(batch),
                Err(CoreError::Closed) if shutdown.is_cancelled() => {
                    close_pending(&mut commands, events.as_ref());
                    return;
                }
                Err(error) => {
                    events.emit(ReaderEvent::Terminal(Err(error)));
                    close_pending(&mut commands, events.as_ref());
                    return;
                }
            }
        }

        if let Some(mut batch) = subscription_batch.take() {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    close_pending(&mut commands, events.as_ref());
                    return;
                }
                command = commands.recv() => {
                    let Some(command) = command else { return; };
                    if !handle_command(
                        command,
                        &shared,
                        &config,
                        auto_settle,
                        reader_id,
                        events.as_ref(),
                        &mut consumer,
                        &mut subscribed,
                        &mut outstanding,
                        &mut next_message_id,
                        &shutdown,
                    ).await {
                        close_pending(&mut commands, events.as_ref());
                        return;
                    }
                    subscription_batch = Some(batch);
                }
                message = batch.next() => {
                    match message {
                        Some(Ok(message)) => match delivery(
                            message,
                            reader_id,
                            auto_settle,
                            &mut outstanding,
                            &mut next_message_id,
                            &shutdown,
                        ).await {
                            Ok(message) => {
                                events.emit(ReaderEvent::Message(message));
                                subscription_batch = Some(batch);
                            }
                            Err(CoreError::Closed) if shutdown.is_cancelled() => {
                                close_pending(&mut commands, events.as_ref());
                                return;
                            }
                            Err(error) => {
                                events.emit(ReaderEvent::Terminal(Err(error)));
                                close_pending(&mut commands, events.as_ref());
                                return;
                            }
                        },
                        Some(Err(error)) => {
                            events.emit(ReaderEvent::Terminal(Err(nats_error(
                                "subscription pull",
                                error,
                            ))));
                            close_pending(&mut commands, events.as_ref());
                            return;
                        }
                        None => {}
                    }
                }
            }
        } else {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    close_pending(&mut commands, events.as_ref());
                    return;
                }
                command = commands.recv() => {
                    let Some(command) = command else { return; };
                    if !handle_command(
                        command,
                        &shared,
                        &config,
                        auto_settle,
                        reader_id,
                        events.as_ref(),
                        &mut consumer,
                        &mut subscribed,
                        &mut outstanding,
                        &mut next_message_id,
                        &shutdown,
                    ).await {
                        close_pending(&mut commands, events.as_ref());
                        return;
                    }
                }
            }
        }
    }
}

async fn create_batch(
    consumer: &PullConsumer,
    batch_size: usize,
    shutdown: &CancellationToken,
) -> Result<pull::Batch> {
    broker_call(
        shutdown,
        "start subscription pull",
        consumer
            .batch()
            .max_messages(batch_size)
            .expires(BROKER_TIMEOUT)
            .messages(),
    )
    .await
}

async fn handle_command(
    command: ReaderCommand,
    shared: &Arc<Shared>,
    config: &RouteConfig,
    auto_settle: bool,
    reader_id: u64,
    events: &dyn ReaderEventSink,
    consumer: &mut Option<PullConsumer>,
    subscribed: &mut bool,
    outstanding: &mut HashMap<u64, jetstream::Message>,
    next_message_id: &mut u64,
    shutdown: &CancellationToken,
) -> bool {
    match command {
        ReaderCommand::Subscribe { ready } => {
            match ensure_consumer(shared, config, consumer, shutdown).await {
                Ok(()) => {
                    if shutdown.is_cancelled() {
                        let _ = ready(Err(CoreError::Closed));
                        return false;
                    }
                    if ready(Ok(())).is_err() {
                        return false;
                    }
                    *subscribed = true;
                }
                Err(error) => {
                    let _ = ready(Err(error));
                    return false;
                }
            }
        }
        ReaderCommand::Fetch { token, maximum } => {
            let result = async {
                ensure_consumer(shared, config, consumer, shutdown).await?;
                let maximum = if auto_settle {
                    maximum
                } else {
                    let available = MAX_OUTSTANDING_MESSAGES.saturating_sub(outstanding.len());
                    maximum.min(u32::try_from(available).unwrap_or(u32::MAX))
                };
                if maximum == 0 {
                    return Ok(Vec::new());
                }
                let messages = fetch_messages(
                    consumer.as_ref().expect("consumer was attached"),
                    maximum,
                    shutdown,
                )
                .await?;
                let mut deliveries = Vec::with_capacity(messages.len());
                for message in messages {
                    deliveries.push(
                        delivery(
                            message,
                            reader_id,
                            auto_settle,
                            outstanding,
                            next_message_id,
                            shutdown,
                        )
                        .await?,
                    );
                }
                Ok(deliveries)
            }
            .await;
            match result {
                Ok(messages) => events.emit(ReaderEvent::FetchComplete {
                    token,
                    reported_count: u32::try_from(messages.len()).unwrap_or(maximum),
                    messages,
                    result: Ok(()),
                }),
                Err(error) => events.emit(ReaderEvent::FetchComplete {
                    token,
                    reported_count: 0,
                    messages: Vec::new(),
                    result: Err(error),
                }),
            }
        }
        ReaderCommand::Settle {
            token,
            kind,
            mut settlements,
        } => {
            for settlement in &mut settlements {
                settlement.result = settle_message(
                    kind,
                    reader_id,
                    &settlement.message_id,
                    outstanding,
                    shutdown,
                )
                .await;
            }
            events.emit(ReaderEvent::SettlementComplete {
                token,
                result: Ok(()),
                messages: settlements,
            });
        }
    }
    true
}

async fn ensure_consumer(
    shared: &Arc<Shared>,
    config: &RouteConfig,
    consumer: &mut Option<PullConsumer>,
    shutdown: &CancellationToken,
) -> Result<()> {
    if consumer.is_some() {
        return Ok(());
    }
    let stream_name = config
        .stream
        .as_deref()
        .ok_or(CoreError::OperationUnsupported)?;
    let consumer_name = config
        .consumer
        .as_deref()
        .ok_or(CoreError::OperationUnsupported)?;

    let context = tokio::select! {
        biased;
        () = shutdown.cancelled() => return Err(CoreError::Closed),
        result = shared.context() => result?,
    };
    let stream = broker_call(shutdown, "look up stream", context.get_stream(stream_name)).await?;
    let attached: PullConsumer = broker_call(
        shutdown,
        "look up consumer",
        stream.get_consumer(consumer_name),
    )
    .await?;
    let info = attached.cached_info();
    if info.config.durable_name.is_none() {
        return Err(CoreError::InvalidConfig(format!(
            "NATS consumer {stream_name}/{consumer_name} is not durable"
        )));
    }
    if info.config.ack_policy != AckPolicy::Explicit {
        return Err(CoreError::InvalidConfig(format!(
            "NATS consumer {stream_name}/{consumer_name} does not use explicit acknowledgments"
        )));
    }
    *consumer = Some(attached);
    Ok(())
}

async fn fetch_messages(
    consumer: &PullConsumer,
    maximum: u32,
    shutdown: &CancellationToken,
) -> Result<Vec<jetstream::Message>> {
    let maximum = usize::try_from(maximum)
        .unwrap_or(usize::MAX)
        .min(MAX_FETCH_MESSAGES);
    let mut batch = broker_call(
        shutdown,
        "start fetch",
        consumer
            .batch()
            .max_messages(maximum)
            .expires(BROKER_TIMEOUT)
            .messages(),
    )
    .await?;
    let deadline = time::sleep(BROKER_TIMEOUT);
    tokio::pin!(deadline);
    let mut messages = Vec::new();
    while messages.len() < maximum {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return Err(CoreError::Closed),
            () = &mut deadline => break,
            message = batch.next() => match message {
                Some(Ok(message)) => messages.push(message),
                Some(Err(error)) => return Err(nats_error("fetch", error)),
                None => break,
            },
        }
    }
    Ok(messages)
}

async fn delivery(
    message: jetstream::Message,
    reader_id: u64,
    auto_settle: bool,
    outstanding: &mut HashMap<u64, jetstream::Message>,
    next_message_id: &mut u64,
    shutdown: &CancellationToken,
) -> Result<Delivery> {
    let payload = message.payload.clone();
    let message_id = if auto_settle {
        broker_call(shutdown, "acknowledge message", message.double_ack()).await?;
        None
    } else {
        let id = *next_message_id;
        *next_message_id = next_message_id
            .checked_add(1)
            .ok_or(CoreError::SubscriptionIdsExhausted)?;
        outstanding.insert(id, message);
        let mut encoded = [0_u8; MESSAGE_ID_LEN];
        encoded[..size_of::<u64>()].copy_from_slice(&reader_id.to_be_bytes());
        encoded[size_of::<u64>()..].copy_from_slice(&id.to_be_bytes());
        Some(Bytes::copy_from_slice(&encoded))
    };
    Ok(Delivery {
        message_id,
        headers: None,
        payload,
    })
}

async fn settle_message(
    kind: SettlementKind,
    reader_id: u64,
    message_id: &Bytes,
    outstanding: &mut HashMap<u64, jetstream::Message>,
    shutdown: &CancellationToken,
) -> Result<()> {
    let encoded: [u8; MESSAGE_ID_LEN] = message_id
        .as_ref()
        .try_into()
        .map_err(|_| CoreError::InvalidMessageId("NATS message ID has an invalid length".into()))?;
    let encoded_reader = u64::from_be_bytes(
        encoded[..size_of::<u64>()]
            .try_into()
            .expect("fixed reader ID length"),
    );
    if encoded_reader != reader_id {
        return Err(CoreError::InvalidMessageId(
            "NATS delivery belongs to another reader".into(),
        ));
    }
    let id = u64::from_be_bytes(
        encoded[size_of::<u64>()..]
            .try_into()
            .expect("fixed delivery ID length"),
    );
    let message = outstanding
        .remove(&id)
        .ok_or_else(|| CoreError::InvalidMessageId("unknown NATS delivery".into()))?;
    let result = match kind {
        SettlementKind::Ack => {
            broker_call(shutdown, "acknowledge message", message.double_ack()).await
        }
        SettlementKind::Nack => {
            broker_call(
                shutdown,
                "requeue message",
                message.ack_with(AckKind::Nak(None)),
            )
            .await
        }
    };
    if let Err(error) = result {
        outstanding.insert(id, message);
        return Err(error);
    }
    Ok(())
}

async fn broker_call<T, E, F>(
    shutdown: &CancellationToken,
    action: &'static str,
    future: F,
) -> Result<T>
where
    E: fmt::Display,
    F: Future<Output = std::result::Result<T, E>>,
{
    tokio::select! {
        biased;
        () = shutdown.cancelled() => Err(CoreError::Closed),
        result = time::timeout(BROKER_TIMEOUT, future) => match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(nats_error(action, error)),
            Err(_) => Err(CoreError::Unavailable(format!("NATS {action} timed out"))),
        }
    }
}

fn close_pending(commands: &mut mpsc::Receiver<ReaderCommand>, events: &dyn ReaderEventSink) {
    while let Ok(command) = commands.try_recv() {
        match command {
            ReaderCommand::Subscribe { ready } => {
                let _ = ready(Err(CoreError::Closed));
            }
            ReaderCommand::Fetch { token, .. } => events.emit(ReaderEvent::FetchComplete {
                token,
                reported_count: 0,
                messages: Vec::new(),
                result: Err(CoreError::Closed),
            }),
            ReaderCommand::Settle {
                token,
                mut settlements,
                ..
            } => {
                for settlement in &mut settlements {
                    settlement.result = Err(CoreError::Closed);
                }
                events.emit(ReaderEvent::SettlementComplete {
                    token,
                    result: Ok(()),
                    messages: settlements,
                });
            }
        }
    }
}

fn nats_error(action: &str, error: impl fmt::Display) -> CoreError {
    CoreError::Unavailable(format!("NATS {action}: {error}"))
}
