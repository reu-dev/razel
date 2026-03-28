use crate::webui_types::Stats;
use leptos::prelude::*;
use server_fn::{BoxedStream, ServerFnError, Websocket, codec::JsonEncoding};

#[derive(Clone, Debug, PartialEq)]
pub enum WsStatus {
    Connecting,
    #[allow(dead_code)]
    Connected,
    #[allow(dead_code)]
    Disconnected,
}

/// Server function that streams Stats over a WebSocket connection.
/// The client→server direction is unused; all traffic flows server→client.
#[server(protocol = Websocket<JsonEncoding, JsonEncoding>)]
pub async fn stats_ws(
    _input: BoxedStream<String, ServerFnError>,
) -> Result<BoxedStream<Stats, ServerFnError>, ServerFnError> {
    use axum::Extension;
    use futures::{SinkExt, channel::mpsc};
    use tokio::sync::watch;

    let Extension(stats_rx): Extension<watch::Receiver<Stats>> = leptos_axum::extract()
        .await
        .map_err(|e| -> ServerFnError { ServerFnError::ServerError(format!("{e}")) })?;
    // Each WS client gets its own clone so they track changes independently
    let mut rx = stats_rx;
    let (mut tx, outgoing) = mpsc::channel(1);
    tokio::spawn(async move {
        // forward every change as it arrives
        loop {
            if rx.changed().await.is_err() {
                break;
            }
            let stats = rx.borrow_and_update().clone();
            if tx.send(Ok(stats)).await.is_err() {
                break;
            }
        }
    });
    Ok(outgoing.into())
}

/// Connects to the stats WebSocket and loops forever, reconnecting with a
/// 2-second back-off whenever the server drops the connection.
#[cfg(feature = "hydrate")]
pub async fn ws_loop(stats: RwSignal<Stats>, ws_status: RwSignal<WsStatus>) {
    use futures::{StreamExt, channel::mpsc, channel::oneshot};
    use std::time::Duration;

    loop {
        ws_status.set(WsStatus::Connecting);
        let (_tx, rx) = mpsc::channel::<Result<String, ServerFnError>>(1);
        match stats_ws(rx.into()).await {
            Ok(mut stream) => {
                while let Some(msg) = stream.next().await {
                    match msg {
                        Ok(s) => {
                            ws_status.set(WsStatus::Connected);
                            stats.set(s);
                        }
                        Err(e) => leptos::logging::warn!("stats_ws error: {e}"),
                    }
                }
            }
            Err(e) => {
                leptos::logging::warn!("stats_ws connect error: {e}");
            }
        }
        ws_status.set(WsStatus::Disconnected);
        // Wait before attempting to reconnect so we don't hammer the server.
        let (tx, rx) = oneshot::channel::<()>();
        leptos::prelude::set_timeout(
            move || {
                let _ = tx.send(());
            },
            Duration::from_millis(2_000),
        );
        rx.await.ok();
    }
}
