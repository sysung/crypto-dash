import os
import json
import time
import sys
from kafka import KafkaProducer
from websocket import WebSocketApp

# If the KAFKA_BROKER environment variable isn't set, default to internal docker address
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = "raw_crypto_trades"

# 1. Target Coinbase Market Pairs
COINBASE_PRODUCTS = [
    "BTC-USD", "ETH-USD", "USDT-USD", "BNB-USD", 
    "XRP-USD", "USDC-USD", "SOL-USD", "DOGE-USD"
]

print("🔄 Establishing connection to local Apache Kafka broker...", flush=True)
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks=1
    )
except Exception as e:
    print(f"❌ Failed to reach Kafka broker at {KAFKA_BROKER}. Ensure Docker is up! Error: {e}")
    sys.exit(1)

def on_open(ws):
    print("🔌 WebSocket Connected to Coinbase. Sending subscription payload...", flush=True)
    
    # The Mandatory Coinbase Handshake payload
    subscription_payload = {
        "type": "subscribe",
        "product_ids": COINBASE_PRODUCTS,
        "channels": ["ticker"]
    }
    # Send the request to Coinbase over the open pipe
    ws.send(json.dumps(subscription_payload))

def on_message(ws, message):
    raw_data = json.loads(message)
    
    # Coinbase sends a confirmation message back first. We only want 'ticker' events.
    if raw_data.get("type") == "ticker":
        
        # Parse out fields. Coinbase names the single trade volume 'last_size'
        payload = {
            "symbol": raw_data.get("product_id"), # e.g., 'BTC-USD'
            "price": float(raw_data.get("price", 0)),
            "volume": float(raw_data.get("last_size", 0)), # Size of this exact transaction
            "timestamp": int(time.time() * 1000)
        }
        
        # Ship our clean data packet straight into Kafka
        producer.send(KAFKA_TOPIC, value=payload)
        print(f"📡 Coinbase Ingest -> {payload['symbol']} | Price: ${payload['price']:.4f} | Vol: {payload['volume']}", flush=True)

def on_error(ws, error):
    print(f"⚠️ Coinbase Stream Error: {error}", flush=True)

def on_close(ws, close_status_code, close_msg):
    print("🔌 Connection closed. Re-establishing link in 5 seconds...", flush=True)
    time.sleep(5)
    start_coinbase_stream()

def start_coinbase_stream():
    # Official Coinbase Public Exchange Feed URL
    coinbase_ws_url = "wss://ws-feed.exchange.coinbase.com"
    
    ws = WebSocketApp(
        coinbase_ws_url,
        on_open=on_open,         # Triggers our subscription handshake
        on_message=on_message,   # Triggers when a trade event lands
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()

if __name__ == "__main__":
    print("🚀 Starting Coinbase Real-Time Exchange Producer Stack...", flush=True)
    start_coinbase_stream()