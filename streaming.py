import websocket
import json


def on_message(ws, message):
    data = json.loads(message)
    print(f"Received message: {data}")


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
