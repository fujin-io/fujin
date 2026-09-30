use std::{fmt, sync::Arc};

use fujin_connector::{BoxFuture, Completion, CompletionSink, Message, OperationToken, Writer};
use fujin_error::{CoreError, Result};
use parking_lot::Mutex;
use tokio::{
    sync::{mpsc, watch},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use crate::{Shared, config::BROKER_TIMEOUT};

const PENDING_OPERATION_LIMIT: usize = 256;

enum Command {
    Publish {
        token: OperationToken,
        payload: bytes::Bytes,
    },
    Flush {
        token: OperationToken,
    },
}

impl Command {
    fn token(&self) -> OperationToken {
        match self {
            Self::Publish { token, .. } | Self::Flush { token } => *token,
        }
    }
}

struct WriterState {
    commands: Option<mpsc::Sender<Command>>,
}

pub(crate) struct NatsWriter {
    state: Mutex<WriterState>,
    shutdown: CancellationToken,
    worker_result: watch::Receiver<Option<Result<()>>>,
}

impl fmt::Debug for NatsWriter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("NatsWriter").finish_non_exhaustive()
    }
}

impl NatsWriter {
    pub(crate) fn new(
        shared: Arc<Shared>,
        subject: String,
        completions: Arc<dyn CompletionSink>,
    ) -> Arc<Self> {
        let shutdown = shared.shutdown.child_token();
        let worker_shutdown = shutdown.clone();
        let (commands, receiver) = mpsc::channel(PENDING_OPERATION_LIMIT);
        let (worker_result, result_receiver) = watch::channel(None);

        tokio::spawn(async move {
            let result = run_worker(shared, subject, completions, receiver, worker_shutdown).await;
            let _ = worker_result.send(Some(result));
        });

        Arc::new(Self {
            state: Mutex::new(WriterState {
                commands: Some(commands),
            }),
            shutdown,
            worker_result: result_receiver,
        })
    }

    fn submit(&self, command: Command) -> Result<()> {
        if self.shutdown.is_cancelled() {
            return Err(CoreError::Closed);
        }

        let state = self.state.lock();
        if self.shutdown.is_cancelled() {
            return Err(CoreError::Closed);
        }
        let commands = state.commands.as_ref().ok_or(CoreError::Closed)?;
        commands.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => {
                CoreError::ResourceExhausted("NATS writer pending-operation limit reached".into())
            }
            mpsc::error::TrySendError::Closed(_) => CoreError::Closed,
        })
    }
}

impl Writer for NatsWriter {
    fn produce(&self, token: OperationToken, message: Message) -> Result<()> {
        if message.headers.is_some() {
            return Err(CoreError::OperationUnsupported);
        }
        self.submit(Command::Publish {
            token,
            payload: message.payload,
        })
    }

    fn flush(&self, token: OperationToken) -> Result<()> {
        self.submit(Command::Flush { token })
    }

    fn begin_transaction(&self, _token: OperationToken) -> Result<()> {
        Err(CoreError::OperationUnsupported)
    }

    fn commit_transaction(&self, _token: OperationToken) -> Result<()> {
        Err(CoreError::OperationUnsupported)
    }

    fn rollback_transaction(&self, _token: OperationToken) -> Result<()> {
        Err(CoreError::OperationUnsupported)
    }

    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
        {
            let mut state = self.state.lock();
            state.commands.take();
            self.shutdown.cancel();
        }
        let mut worker_result = self.worker_result.clone();

        Box::pin(async move {
            if let Some(result) = worker_result.borrow().clone() {
                return result;
            }
            worker_result.changed().await.map_err(|_| {
                CoreError::Internal("NATS writer worker exited without a result".into())
            })?;
            worker_result.borrow().clone().ok_or_else(|| {
                CoreError::Internal("NATS writer worker returned no result".into())
            })?
        })
    }

    fn writer_contract_compliant(&self) -> bool {
        true
    }
}

impl Drop for NatsWriter {
    fn drop(&mut self) {
        self.state.get_mut().commands.take();
        self.shutdown.cancel();
    }
}

async fn run_worker(
    shared: Arc<Shared>,
    subject: String,
    completions: Arc<dyn CompletionSink>,
    mut commands: mpsc::Receiver<Command>,
    shutdown: CancellationToken,
) -> Result<()> {
    let subject = async_nats::Subject::from(subject);
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            command = commands.recv() => {
                let Some(command) = command else {
                    return Ok(());
                };
                match command {
                    Command::Publish { token, payload } => {
                        let result = publish(&shared, subject.clone(), payload, &shutdown).await;
                        completions.complete(Completion { token, result });
                    }
                    Command::Flush { token } => {
                        completions.complete(Completion {
                            token,
                            result: Ok(()),
                        });
                    }
                }
            }
        }
    }

    commands.close();
    while let Some(command) = commands.recv().await {
        completions.complete(Completion {
            token: command.token(),
            result: Err(CoreError::Closed),
        });
    }
    Ok(())
}

async fn publish(
    shared: &Shared,
    subject: async_nats::Subject,
    payload: bytes::Bytes,
    shutdown: &CancellationToken,
) -> Result<()> {
    let operation = async {
        let context = shared.context().await?;
        let acknowledgment = context
            .publish(subject, payload)
            .await
            .map_err(|error| CoreError::Unavailable(format!("NATS publish: {error}")))?;
        acknowledgment.await.map_err(|error| {
            CoreError::Unavailable(format!("NATS publish acknowledgment: {error}"))
        })?;
        Ok(())
    };

    tokio::select! {
        biased;
        () = shutdown.cancelled() => Err(CoreError::Unavailable(
            "NATS writer closed before publication acknowledgment".into(),
        )),
        result = timeout(BROKER_TIMEOUT, operation) => result.map_err(|_| {
            CoreError::Unavailable("NATS publication acknowledgment timed out".into())
        })?,
    }
}
