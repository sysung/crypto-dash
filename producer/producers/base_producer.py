import os
import json
import time
import sys
import queue
import logging
import threading
from abc import ABC, abstractmethod
from typing import Any, Dict, List
from kafka import KafkaProducer
from websocket import WebSocketApp

logger = logging.getLogger(__name__)

class BaseCoinbaseProducer(ABC):
    """
    Abstract base class managing the lifecycle of a real-time Coinbase WebSocket ingestion pipeline.
    Handles persistent connection loop, thread-safe buffering queue, progressive Kafka broker
    connection retries, and asynchronous thread-safe background workers.
    """
    def __init__(self, broker: str, topic: str, products: List[str]):
        self.broker = broker
        self.topic = topic
        self.products = products
        
        # Thread-safe queue for buffering parsed tick items
        self.msg_queue: queue.Queue[Dict[str, Any]] = queue.Queue(maxsize=20000)
        
        # Establish connection to Kafka with progressive retry delays
        self.producer = self._initialize_producer()
        
        # Start background sender worker thread
        self._start_sender_thread()

    def _initialize_producer(self) -> KafkaProducer:
        """
        Attempts to connect to the Apache Kafka broker.
        Retries up to 10 times with progressive delays to manage container boot synchronizations.
        """
        logger.info(f"[{self.__class__.__name__}] Connecting to Kafka broker at {self.broker}...")
        for attempt in range(1, 11):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=[self.broker],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks=1,
                    compression_type='lz4'  # High-performance compression
                )
                logger.info(f"✅ [{self.__class__.__name__}] Connected to Kafka broker on attempt {attempt}!")
                return producer
            except Exception as e:
                wait_seconds = min(attempt * 2, 20)
                logger.warning(
                    f"🔄 [{self.__class__.__name__}] [Attempt {attempt}/10] Kafka not ready. "
                    f"Retrying in {wait_seconds}s... (Error: {e})"
                )
                time.sleep(wait_seconds)
        
        logger.critical(f"❌ [{self.__class__.__name__}] Failed to connect to Kafka at {self.broker}. Exiting.")
        sys.exit(1)

    def _start_sender_thread(self) -> None:
        """
        Spawns a background worker thread as a Daemon so that it terminates automatically
        when the main process stops.
        """
        sender_thread = threading.Thread(target=self._batch_sender_worker, daemon=True)
        sender_thread.start()

    def _batch_sender_worker(self) -> None:
        """
        Drains parsed payloads from the thread-safe queue and publishes them to the Kafka topic.
        This keeps the WebSocket receiving thread completely non-blocking.
        """
        logger.info(f"👷 [{self.__class__.__name__}] Background Kafka sender worker thread active.")
        while True:
            try:
                payload = self.msg_queue.get()
                symbol = payload.get("symbol")
                key = symbol.encode("utf-8") if symbol else None
                
                # Map each symbol 1-to-1 to a partition index to avoid hashing collisions
                partition = None
                if symbol and symbol in self.products:
                    # Map symbol to one of the 8 partitions deterministically based on list order
                    partition = self.products.index(symbol) % 8
                
                self.producer.send(self.topic, key=key, partition=partition, value=payload)
                self.msg_queue.task_done()
            except Exception as e:
                logger.error(f"⚠️ [{self.__class__.__name__}] Error in Kafka sender worker: {e}")
                time.sleep(1)

    def on_open(self, ws: WebSocketApp) -> None:
        """
        Triggered upon successful WebSocket connection. Sends subscription payload.
        """
        logger.info(f"🔌 [{self.__class__.__name__}] WebSocket Connected to Coinbase. Sending subscription...")
        payload = self.get_subscription_payload()
        ws.send(json.dumps(payload))
        logger.info(f"✅ [{self.__class__.__name__}] Sent subscription request: {json.dumps(payload)}")

    def on_message(self, ws: WebSocketApp, message: str) -> None:
        """
        Triggered when a raw message frame is received. Parses JSON and delegates to process_message.
        """
        try:
            raw_data = json.loads(message)
            self.process_message(raw_data)
        except Exception as e:
            logger.error(f"⚠️ [{self.__class__.__name__}] Failed to parse WebSocket frame: {e}")

    def on_error(self, ws: WebSocketApp, error: Any) -> None:
        """
        Triggered when a WebSocket client or network error occurs.
        """
        logger.error(f"⚠️ [{self.__class__.__name__}] WebSocket Stream Error: {error}")

    def on_close(self, ws: WebSocketApp, close_status_code: Any, close_msg: Any) -> None:
        """
        Triggered when the WebSocket connection is severed.
        """
        logger.warning(
            f"🔌 [{self.__class__.__name__}] Connection severed (Code: {close_status_code}, Msg: {close_msg})."
        )

    @abstractmethod
    def get_subscription_payload(self) -> Dict[str, Any]:
        """
        Returns subscription handshake JSON payload for this channel.
        """
        pass

    @abstractmethod
    def process_message(self, raw_data: Dict[str, Any]) -> None:
        """
        Parses the specific channel data structure and enqueues payload(s) to the msg_queue.
        """
        pass

    def start_stream(self) -> None:
        """
        Runs a persistent connection loop to Coinbase WebSocket feed.
        Enforces a loop-based retry structure to completely avoid stack overflow issues.
        """
        coinbase_ws_url = "wss://advanced-trade-ws.coinbase.com"
        while True:
            try:
                ws = WebSocketApp(
                    coinbase_ws_url,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close
                )
                logger.info(f"🚀 [{self.__class__.__name__}] Starting persistent run loop...")
                ws.run_forever()
            except Exception as e:
                logger.error(f"⚠️ [{self.__class__.__name__}] Run loop encountered exception: {e}")
            
            logger.info(f"🔄 [{self.__class__.__name__}] Re-establishing connection in 5 seconds...")
            time.sleep(5)
