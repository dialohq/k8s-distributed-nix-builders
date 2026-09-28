//! The ARC pod owns one attachment; it never retries after sending JIT credentials.
use crate::online_rpc::{
    Config,
    wire::{runner_pool_client::RunnerPoolClient, *},
};
use anyhow::{Context, Result};
use serde::Deserialize;
use std::{fs, io::Write, path::PathBuf, time::Duration};
use tokio::{
    signal::unix::{SignalKind, signal},
    sync::mpsc,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Status, transport::Endpoint};

type Reservation = (mpsc::Sender<RunnerInput>, tonic::Streaming<RunnerEvent>);

async fn reserve(
    address: &str,
    id: &str,
    token: tonic::metadata::MetadataValue<tonic::metadata::Ascii>,
) -> Result<Reservation, Status> {
    let endpoint = Endpoint::from_shared(format!("http://{address}"))
        .map_err(|e| Status::invalid_argument(e.to_string()))?
        .connect_timeout(Duration::from_secs(5))
        .http2_keep_alive_interval(Duration::from_secs(20))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true);
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| Status::unavailable(e.to_string()))?;
    let mut client = RunnerPoolClient::new(channel);
    let (sender, receiver) = mpsc::channel(2);
    sender
        .try_send(RunnerInput {
            message: Some(runner_input::Message::Claim(RunnerClaim { id: id.into() })),
        })
        .map_err(|e| Status::internal(e.to_string()))?;
    let mut request = Request::new(ReceiverStream::new(receiver));
    request.metadata_mut().insert("authorization", token);
    let mut response = client.attach(request).await?.into_inner();
    if !response
        .message()
        .await?
        .is_some_and(|message| message.kind == runner_event::Kind::Reserved as i32)
    {
        return Err(Status::failed_precondition(
            "builder did not reserve a slot",
        ));
    }
    Ok((sender, response))
}

#[derive(Deserialize)]
struct Pool {
    builders: Vec<String>,
    token_file: PathBuf,
}
struct Ready(PathBuf);
impl Drop for Ready {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

pub async fn run() -> Result<i32> {
    let file = std::env::var("DISTRIBUTED_NIX_ARC_CONFIG")
        .unwrap_or_else(|_| "/etc/distributed-nix/arc-client.json".into());
    let pool: Pool = serde_json::from_slice(&fs::read(file)?)?;
    let config = Config {
        nodes: pool.builders,
        index: 0,
        token_file: pool.token_file,
        pod_uid: None,
    };
    config.validate()?;
    let token: tonic::metadata::MetadataValue<_> = format!("Bearer {}", config.token()?).parse()?;
    let id = std::env::var("DISTRIBUTED_NIX_POD_UID")?;
    let jit = std::env::var("ACTIONS_RUNNER_INPUT_JITCONFIG")
        .context("ARC did not supply JIT configuration")?;
    let agent = std::env::var("GITHUB_ACTIONS_RUNNER_EXTRA_USER_AGENT").unwrap_or_default();
    let ready = Ready("/run/distributed-nix-arc-client/ready".into());
    fs::create_dir_all(ready.0.parent().unwrap())?;
    let _ = fs::remove_file(&ready.0);
    let offset = id.bytes().fold(0usize, |hash, b| {
        hash.wrapping_mul(31).wrapping_add(b as usize)
    }) % config.nodes.len();
    let mut terminate = signal(SignalKind::terminate())?;
    let stopping = async {
        tokio::select! { _ = tokio::signal::ctrl_c() => (), _ = terminate.recv() => () }
    };
    tokio::pin!(stopping);
    eprintln!("Waiting for a warm Nix builder");
    let mut last_wait = Vec::new();
    loop {
        let mut waiting = Vec::new();
        for i in 0..config.nodes.len() {
            let address = &config.nodes[(offset + i) % config.nodes.len()];
            let reservation = tokio::select! {
                _ = &mut stopping => return Ok(143),
                result = tokio::time::timeout(Duration::from_secs(5), reserve(address, &id, token.clone())) => {
                    result.unwrap_or_else(|_| Err(Status::deadline_exceeded("reservation timed out")))
                },
            };
            let (sender, mut response) = match reservation {
                Ok(reservation) => reservation,
                Err(status)
                    if matches!(
                        status.code(),
                        tonic::Code::Unavailable
                            | tonic::Code::ResourceExhausted
                            | tonic::Code::Unimplemented
                            | tonic::Code::DeadlineExceeded
                    ) =>
                {
                    waiting.push(format!("{address}: {}", status.message()));
                    continue;
                }
                Err(status) => return Err(status.into()),
            };
            sender
                .send(RunnerInput {
                    message: Some(runner_input::Message::Start(RunnerStart {
                        jit_config: jit.clone(),
                        user_agent: agent.clone(),
                    })),
                })
                .await?;
            eprintln!("Attached to {address}");
            loop {
                let message = tokio::select! {
                    _ = &mut stopping => return Ok(143),
                    message = response.message() => message?.context("builder disconnected before reporting completion")?,
                };
                match runner_event::Kind::try_from(message.kind)? {
                    runner_event::Kind::Started => fs::write(&ready.0, b"")?,
                    runner_event::Kind::Output => std::io::stdout().write_all(&message.output)?,
                    runner_event::Kind::Exited => return Ok(message.exit_code),
                    runner_event::Kind::Reserved => anyhow::bail!("duplicate reservation"),
                }
            }
        }
        if waiting != last_wait {
            eprintln!("Builder availability: {}", waiting.join("; "));
            last_wait = waiting;
        }
        tokio::select! { _ = &mut stopping => return Ok(143), _ = tokio::time::sleep(Duration::from_secs(1)) => () }
    }
}
