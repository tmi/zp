
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use futures::{sink::SinkExt, stream::StreamExt};
use std::sync::Arc;
use tracing::error;

use crate::common::AppState;

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let mut rx = state.tx.subscribe();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                // Handle messages from the websocket client
                ws_msg = receiver.next() => {
                    match ws_msg {
                        Some(Ok(msg)) => {
                            if let Message::Ping(buf) = msg {
                                if sender.send(Message::Pong(buf)).await.is_err() {
                                    error!("Failed to send pong, client disconnected");
                                    break;
                                }
                            }
                            // Ignore other messages from the client for now
                        },
                        Some(Err(e)) => {
                            error!("WebSocket receive error: {:?}", e);
                            break;
                        },
                        None => {
                            // Client disconnected
                            break;
                        },
                    }
                }
                // Handle messages from the broadcast channel
                bc_msg = rx.recv() => {
                    match bc_msg {
                        Ok((timestamp, value)) => {
                            let message = format!("{}=>{}", timestamp, value);
                            if sender.send(Message::Text(message)).await.is_err() {
                                error!("Failed to send broadcast message, client disconnected");
                                break;
                            }
                        },
                        Err(e) => {
                            error!("Broadcast receive error: {:?}", e);
                            // Depending on the error, we might want to continue or break.
                            // For RecvError::Lagged, we might want to log and continue.
                            // For others, it might indicate a serious issue with the broadcast channel.
                        }
                    }
                }
            }
        }
    });
}
