import queue
import logging
from typing import Any, Dict, List
from base_producer import BaseCoinbaseProducer

logger = logging.getLogger(__name__)

class TradesProducer(BaseCoinbaseProducer):
    """
    Ingests real-time cryptocurrency trade ticks (the 'ticker' channel) from Coinbase Advanced Trade
    and publishes them to a dedicated Kafka topic.
    """
    def __init__(self, broker: str, topic: str, products: List[str]):
        super().__init__(broker=broker, topic=topic, products=products)

    def get_subscription_payload(self) -> Dict[str, Any]:
        """
        Returns subscription handshake JSON payload for the ticker channel.
        """
        return {
            "type": "subscribe",
            "product_ids": self.products,
            "channel": "ticker"
        }

    def process_message(self, raw_data: Dict[str, Any]) -> None:
        """
        Processes real-time ticker update events and publishes all fields.
        """
        # Ensure we only process messages for the ticker channel
        if raw_data.get("channel") != "ticker":
            return

        events = raw_data.get("events", [])
        sequence_num = int(raw_data.get("sequence_num", 0))
        server_timestamp = raw_data.get("timestamp")

        for event in events:
            # We are interested in ticker updates
            if event.get("type") != "update":
                continue

            tickers = event.get("tickers", [])
            for ticker in tickers:
                # Map and extract ALL fields delivered by Advanced Trade WebSocket
                payload = {
                    "symbol": ticker.get("product_id"),
                    "price": float(ticker.get("price", 0)),
                    "volume_24h": float(ticker.get("volume_24_h", 0)),
                    "low_24h": float(ticker.get("low_24_h", 0)),
                    "high_24h": float(ticker.get("high_24_h", 0)),
                    "low_52w": float(ticker.get("low_52_w", 0)),
                    "high_52w": float(ticker.get("high_52_w", 0)),
                    "price_percent_chg_24h": float(ticker.get("price_percent_chg_24_h", 0)),
                    "best_bid": float(ticker.get("best_bid", 0)),
                    "best_ask": float(ticker.get("best_ask", 0)),
                    "best_bid_quantity": float(ticker.get("best_bid_quantity", 0)),
                    "best_ask_quantity": float(ticker.get("best_ask_quantity", 0)),
                    "sequence_num": sequence_num,
                    "timestamp": server_timestamp
                }

                # Enqueue the parsed payload
                try:
                    self.msg_queue.put_nowait(payload)
                    logger.info(
                        f"📡 [Trades] Ingest -> {payload['symbol']} | Price: ${payload['price']:.2f} | "
                        f"24h Vol: {payload['volume_24h']:.2f} | Ask: {payload['best_ask']} (Queue: {self.msg_queue.qsize()})"
                    )
                except queue.Full:
                    logger.error(
                        f"🚨 [Trades] Queue full! Dropping ticker update for {payload['symbol']}."
                    )
