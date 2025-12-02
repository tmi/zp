This microservice consumes kafka messages, and retains an aggregation over the last 10 minutes.
It will exposes two endpoints:
 /fetch which returns the aggregation as a json
 /update which is a websocket that pushes updates to this aggregation

It is based on tokio and axum.
