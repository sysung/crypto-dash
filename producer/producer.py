import os
import sys
import time
import logging
import threading
from typing import List

# Configure logging to standard output with readable timestamps
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Add current folder to sys.path to enable clean relative imports
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from trades_producer import TradesProducer
from l2_producer import L2Producer

# Constants and Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC_TRADES = "raw_crypto_trades"
KAFKA_TOPIC_L2 = "raw_crypto_l2"

COINBASE_PRODUCTS: List[str] = [
    "BTC-USD", "ETH-USD", "USDT-USD", "BNB-USD", 
    "XRP-USD", "USDC-USD", "SOL-USD", "DOGE-USD"
]

def run_trades_producer():
    """Runs the trade execution tick stream in a dedicated loop."""
    logger.info("🎬 Initializing Trades (Ticker) Ingestion Pipeline...")
    trades_pipeline = TradesProducer(
        broker=KAFKA_BROKER,
        topic=KAFKA_TOPIC_TRADES,
        products=COINBASE_PRODUCTS
    )
    trades_pipeline.start_stream()

def run_l2_producer():
    """Runs the L2 depth order book stream in a dedicated loop."""
    logger.info("🎬 Initializing L2 Depth (Order Book) Ingestion Pipeline...")
    l2_pipeline = L2Producer(
        broker=KAFKA_BROKER,
        topic=KAFKA_TOPIC_L2,
        products=COINBASE_PRODUCTS
    )
    l2_pipeline.start_stream()

def main() -> None:
    logger.info("🚀 Starting Coinbase Real-Time Multi-Stream Ingestion System...")
    
    # Spawn the Trades Producer in a dedicated thread
    trades_thread = threading.Thread(target=run_trades_producer, name="TradesProducerThread", daemon=True)
    trades_thread.start()
    logger.info("🟢 Trades Ingestion Thread spawned and started.")
    
    # Spawn the L2 Producer in a dedicated thread
    l2_thread = threading.Thread(target=run_l2_producer, name="L2ProducerThread", daemon=True)
    l2_thread.start()
    logger.info("🟢 L2 Depth Ingestion Thread spawned and started.")
    
    # Keep the main orchestrator thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("🛑 Keyboard interrupt received. Shutting down multi-stream system...")

if __name__ == "__main__":
    main()