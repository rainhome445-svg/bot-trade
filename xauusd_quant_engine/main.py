import asyncio
import logging
import signal
import sys
from concurrent.futures import ThreadPoolExecutor

from src.core.mt5_engine import MT5Engine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("SystemOrchestrator")

class TradingSystem:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.is_running = False

        # Core Engine
        self.mt5 = MT5Engine(symbol=self.symbol)

        # ThreadPool untuk menjalankan fungsi MT5 yang blocking agar tidak membekukan async loop
        self.executor = ThreadPoolExecutor(max_workers=4)

    async def initialize(self) -> bool:
        logger.info("Booting up XAUUSD Quant Engine...")
        if not self.mt5.initialize():
            logger.critical("Failed to initialize MT5 Engine. Aborting boot.")
            return False

        logger.info("All agents initialized and warmed up.")
        return True

    async def shutdown(self):
        logger.info("Initiating graceful shutdown sequence...")
        self.is_running = False

        self.mt5.shutdown()
        self.executor.shutdown(wait=True)
        logger.info("System gracefully terminated. No trades compromised.")

    async def run_loop(self):
        self.is_running = True
        logger.info("Starting Main Event Loop...")

        while self.is_running:
            try:
                # 1. Heartbeat Check
                if not self.mt5.check_connection():
                    logger.warning("Connection lost. Retrying in 5 seconds...")
                    await asyncio.sleep(5)
                    continue

                # 2. Data Ingestion (Contoh memanggil fungsi blocking MT5 via executor)
                loop = asyncio.get_running_loop()
                current_tick = await loop.run_in_executor(self.executor, self.mt5.get_current_tick)

                # Nanti akan diteruskan ke agent-agent lainnya

                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                await asyncio.sleep(1)


def handle_exit(sig, frame, system: TradingSystem, loop: asyncio.AbstractEventLoop):
    logger.warning("\nInterrupt signal received! Forcing shutdown...")
    asyncio.create_task(system.shutdown())
    loop.call_soon_threadsafe(loop.stop)


if __name__ == "__main__":
    TARGET_SYMBOL = "XAUUSD"

    system = TradingSystem(symbol=TARGET_SYMBOL)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Note: On Windows, signal.add_signal_handler might not work for SIGINT/SIGTERM properly
    # Using a fallback for cross-platform compatibility
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(system.shutdown()))
        except NotImplementedError:
            signal.signal(sig, lambda s, f: handle_exit(s, f, system, loop))

    try:
        if loop.run_until_complete(system.initialize()):
            loop.run_until_complete(system.run_loop())
    except KeyboardInterrupt:
        logger.info("Application interrupted by user.")
    finally:
        if system.is_running:
            loop.run_until_complete(system.shutdown())
        loop.close()
        sys.exit(0)
