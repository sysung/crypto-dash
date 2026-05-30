import queue
import logging
from typing import Any, Dict, List
from base_producer import BaseCoinbaseProducer

logger = logging.getLogger(__name__)

class L2Producer(BaseCoinbaseProducer):
    """
    Ingests real-time cryptocurrency Order Book Level 2 depth data (the 'level2' channel)
    from Coinbase Advanced Trade, flattens individual price level updates, and publishes them
    to a dedicated Kafka topic.
    """
    def __init__(self, broker: str, topic: str, products: List[str]):
        super().__init__(broker=broker, topic=topic, products=products)

    def get_subscription_payload(self) -> Dict[str, Any]:
        """
        Returns subscription handshake JSON payload for the level2 channel.
        """
        return {
            "type": "subscribe",
            "product_ids": self.products,
            "channel": "level2"
        }

    def process_message(self, raw_data: Dict[str, Any]) -> None:
        """
        Processes real-time L2 order book updates. Note that even though we subscribe to 'level2',
        the Coinbase API returns the channel name as 'l2_data' in message frames.
        """
        # Ensure we only process messages for the l2_data channel
        if raw_data.get("channel") != "l2_data":
            return

        events = raw_data.get("events", [])
        sequence_num = int(raw_data.get("sequence_num", 0))
        server_timestamp = raw_data.get("timestamp")

        for event in events:
            event_type = event.get("type")  # "snapshot" or "update"
            symbol = event.get("product_id")
            updates = event.get("updates", [])

            for update in updates:
                # Flatten the order book update to a clean flat schema
                payload = {
                    "event_type": event_type,
                    "symbol": symbol,
                    "side": update.get("side"),               # "bid" or "offer"
                    "price": float(update.get("price_level", 0)),
                    "volume": float(update.get("new_quantity", 0)),
                    "event_time": update.get("event_time"),   # ISO string format
                    "sequence_num": sequence_num,
                    "timestamp": server_timestamp
                }

                # Enqueue the parsed payload
                try:
                    self.msg_queue.put_nowait(payload)
                    logger.info(
                        f"📊 [L2 Depth] Ingest -> {payload['symbol']} | {payload['side'].upper()} | "
                        f"Price: ${payload['price']:.2f} | Vol: {payload['volume']:.6f} | "
                        f"Type: {payload['event_type']} (Queue: {self.msg_queue.qsize()})"
                    )
                except queue.Full:
                    logger.error(
                        f"🚨 [L2 Depth] Queue full! Dropping L2 update for {payload['symbol']}."
                    )
