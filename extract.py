import json
import os
from websocket import WebSocketApp, WebSocket
from dotenv import load_dotenv
from google.cloud import pubsub

# # around 10 a second
# def on_message(ws: WebSocket, message: str) -> None:
#     json_data = json.loads(message)
#     print(json_data)
    

def on_error(ws: WebSocket, error: Exception) -> None:
    print(error)

def on_close(ws: WebSocket, close_status_code: int, close_msg: str) -> None:
    print(f"Closed with status {close_status_code}; close_msg: {close_msg}")

def on_open(ws: WebSocket):
    ws.send('{"type":"subscribe","symbol":"AAPL"}')
    ws.send('{"type":"subscribe","symbol":"AMZN"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')
    ws.send('{"type":"subscribe","symbol":"IC MARKETS:1"}')
    ws.send('{"type":"subscribe","symbol":"GOOGL"}')
    ws.send('{"type":"subscribe","symbol":"MSFT"}')
    ws.send('{"type":"subscribe","symbol":"TSLA"}')
    ws.send('{"type":"subscribe","symbol":"FB"}')
    ws.send('{"type":"subscribe","symbol":"NFLX"}')
    ws.send('{"type":"subscribe","symbol":"NVDA"}')


def publish_data(ws: WebSocket, message: str, publisher, topic_path: str) -> None:
    data = json.dumps(message).encode("utf-8")
    future = publisher.publish(topic_path, data, origin="finnhub-extractor", username="kronosmichall62")
    print(future.result())
    
if __name__ == "__main__":
    load_dotenv()
    FINNHUB_TOKEN = os.getenv("FINNHUB_KEY")
    PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
    TOPIC_ID = os.getenv("GCP_PUB_SUB_TOPIC")
    
    publisher = pubsub.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    
    on_message = lambda ws, message: publish_data(ws, message, publisher, topic_path)
    
    ws = WebSocketApp(f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}",
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()