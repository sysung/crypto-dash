import os
import json
import time
import sys
import queue
import logging
import threading
from typing import Any, Dict, List
from kafka import KafkaProducer
from websocket import WebSocketApp

# Configure logging to standard output with readable timestamps
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Constants and Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = "raw_crypto_trades"
COINBASE_PRODUCTS = [
    "BTC-USD", "ETH-USD", "USDT-USD", "BNB-USD", 
    "XRP-USD", "USDC-USD", "SOL-USD", "DOGE-USD"
]

class CoinbaseKafkaPipeline:
    """
    Manages the lifecycle of a real-time cryptocurrency tick data pipeline.
    Connects to the Coinbase WebSocket exchange feed, buffers parsed ticks in-memory 
    via a thread-safe queue, and publishes them to an Apache Kafka topic asynchronously 
    using a background worker thread.
    """
    def __init__(self, broker: str, topic: str, products: List[str]):
        self.broker = broker
        self.topic = topic
        self.products = products
        
        # Thread-safe queue for buffering incoming tick items
        self.msg_queue: queue.Queue[Dict[str, Any]] = queue.Queue(maxsize=10000)
        
        # Bootstrap connection to Kafka with progressive retries
        self.producer = self._initialize_producer()
        
        # Initialize and start background worker
        self._start_sender_thread()

    def _initialize_producer(self) -> KafkaProducer:
        """
        Attempts to establish a robust connection to the Kafka broker.
        Retries up to 10 times with progressive delays to handle boot-order synchronization.
        """
        logger.info(f"Establishing connection to Apache Kafka broker at {self.broker}...")
        for attempt in range(1, 11):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=[self.broker],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks=1,
                    compression_type='lz4'  # High-performance LZ4 compression
                )
                logger.info(f"✅ Successfully connected to Kafka broker on attempt {attempt}!")
                return producer
            except Exception as e:
                wait_seconds = min(attempt * 2, 20)
                logger.warning(
                    f"🔄 [Attempt {attempt}/10] Kafka not ready. Retrying in {wait_seconds}s... (Error: {e})"
                )
                time.sleep(wait_seconds)
        
        logger.critical(f"❌ Could not reach Kafka broker at {self.broker} after 10 attempts. Terminating.")
        sys.exit(1)

    def _start_sender_thread(self) -> None:
        """
        Spawns a background sender worker thread as a Daemon so that it automatically
        terminates when the main pipeline process stops.
        """
        sender_thread = threading.Thread(target=self._batch_sender_worker, daemon=True)
        sender_thread.start()

    def _batch_sender_worker(self) -> None:
        """
        Background worker loop that continuously drains structural trade payloads 
        from the in-memory queue and publishes them to the Kafka topic.
        This keeps the WebSocket thread completely non-blocking.
        """
        logger.info("👷 Background Kafka sender worker thread initialized and active.")
        while True:
            try:
                payload = self.msg_queue.get()
                self.producer.send(self.topic, value=payload)
                self.msg_queue.task_done()
            except Exception as e:
                logger.error(f"⚠️ Error inside background sender worker: {e}")
                time.sleep(1)

    def on_open(self, ws: WebSocketApp) -> None:
        """
        Callback triggered upon successful WebSocket connection.
        Sends the Coinbase subscription handshake payload over the socket.
        """
        logger.info("🔌 WebSocket Connected to Coinbase. Sending subscription payload...")
        subscription_payload = {
            "type": "subscribe",
            "product_ids": self.products,
            "channels": ["ticker"]
        }
        ws.send(json.dumps(subscription_payload))

    def on_message(self, ws: WebSocketApp, message: str) -> None:
        """
        Callback triggered whenever a raw text frame is received from the WebSocket.
        Parses ticker trade details, maps them to the data schema, and buffers them.
        """
        try:
            raw_data = json.loads(message)
            
            # Filter for ticker events only (skips handshake confirmation events)
            if raw_data.get("type") == "ticker":
                payload = {
                    "symbol": raw_data.get("product_id"),
                    "price": float(raw_data.get("price", 0)),
                    "volume": float(raw_data.get("last_size", 0)),
                    "timestamp": int(time.time() * 1000)
                }
                
                # Append parsed payload to buffering queue
                try:
                    self.msg_queue.put_nowait(payload)
                    logger.info(
                        f"📡 Coinbase Ingest -> {payload['symbol']} | "
                        f"Price: ${payload['price']:.4f} | Vol: {payload['volume']} "
                        f"(Queue Size: {self.msg_queue.qsize()})"
                    )
                except queue.Full:
                    logger.error(
                        "🚨 Alert: Queue is completely full! Dropping incoming message to prevent thread lock."
                    )
        except Exception as e:
            logger.error(f"⚠️ Failed to parse incoming WebSocket message: {e}")

    def on_error(self, ws: WebSocketApp, error: Any) -> None:
        """
        Callback triggered when a WebSocket network or client error occurs.
        """
        logger.error(f"⚠️ Coinbase Stream Error: {error}")

    def on_close(self, ws: WebSocketApp, close_status_code: Any, close_msg: Any) -> None:
        """
        Callback triggered when the WebSocket connection is closed/severed.
        """
        logger.warning(
            f"🔌 Connection severed (Code: {close_status_code}, Msg: {close_msg})."
        )

    def start_stream(self) -> None:
        """
        Opens a persistent connection to the Coinbase public exchange feed with reconnection logic.
        """
        coinbase_ws_url = "wss://ws-feed.exchange.coinbase.com"
        while True:
            try:
                ws = WebSocketApp(
                    coinbase_ws_url,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close
                )
                logger.info("🔌 Starting persistent WebSocket connection run loop...")
                ws.run_forever()
            except Exception as e:
                logger.error(f"⚠️ WebSocket run loop encountered an exception: {e}")
            
            logger.info("🔄 Re-establishing link in 5 seconds...")
            time.sleep(5)

def main() -> None:
    logger.info("🚀 Starting Coinbase Real-Time Exchange Producer Pipeline...")
    pipeline = CoinbaseKafkaPipeline(
        broker=KAFKA_BROKER,
        topic=KAFKA_TOPIC,
        products=COINBASE_PRODUCTS
    )
    pipeline.start_stream()

if __name__ == "__main__":
    main()