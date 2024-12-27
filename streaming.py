import websocket
import json
from data_store.streaming_data_spark import write_to_parquet
from datetime import datetime

batch = []


def on_message(ws, message):
    global batch
    data = json.loads(message)

    if "result" in data and data["result"] is None:
        print("Ignoring subscription message.")
        return

    batch.append(data)

    if len(batch) >= 100:
        write_to_parquet(batch, "streaming_data/streaming_data.parquet")
        batch = []


def on_error(ws, error):
    print(f"Error: {error}")


def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")


def on_open(ws):
    payload = {"method": "SUBSCRIBE", "params": ["xrpusdt@trade"], "id": 1}
    ws.send(json.dumps(payload))
    print("Subscription message sent!")


if __name__ == "__main__":
    url = "wss://stream.binance.com:9443/ws"
    ws = websocket.WebSocketApp(
        url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open,
    )
    ws.run_forever()
