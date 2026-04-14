import MetaTrader5 as mt5
import logging

logger = logging.getLogger(__name__)

class MT5Engine:
    """
    Kelas inti untuk mengelola koneksi dan operasi tingkat rendah ke terminal MetaTrader 5.
    Menerapkan strict OOP dan fault tolerance.
    """

    def __init__(self, symbol: str = "XAUUSD", magic_number: int = 777777):
        self.symbol = symbol
        self.magic_number = magic_number
        self.connected = False

    def initialize(self) -> bool:
        """Menginisialisasi koneksi ke terminal MT5."""
        logger.info("Initializing MetaTrader 5 engine...")

        # Initialize MT5
        if not mt5.initialize():
            logger.critical(f"MT5 Initialization failed. Error code: {mt5.last_error()}")
            return False

        if not self._prepare_symbol():
            return False

        self.connected = True
        logger.info(f"Successfully connected to MT5. Terminal Info: {mt5.terminal_info().name}")
        return True

    def _prepare_symbol(self) -> bool:
        """Memastikan simbol yang ditargetkan tersedia dan terlihat di Market Watch."""
        symbol_info = mt5.symbol_info(self.symbol)
        if symbol_info is None:
            logger.error(f"Symbol {self.symbol} not found in MT5.")
            mt5.shutdown()
            return False

        if not symbol_info.visible:
            logger.info(f"Symbol {self.symbol} is not visible, trying to select it...")
            if not mt5.symbol_select(self.symbol, True):
                logger.error(f"Failed to select symbol {self.symbol}. Error: {mt5.last_error()}")
                mt5.shutdown()
                return False

        logger.info(f"Symbol {self.symbol} selected successfully.")
        return True

    def shutdown(self):
        """Menutup koneksi dengan aman."""
        if self.connected:
            mt5.shutdown()
            self.connected = False
            logger.info("MT5 Engine shutdown complete.")

    def get_current_tick(self):
        """Mengambil data tick terbaru."""
        if not self.connected:
            return None
        tick = mt5.symbol_info_tick(self.symbol)
        if tick is None:
            logger.warning(f"Failed to retrieve tick for {self.symbol}. Error: {mt5.last_error()}")
        return tick

    def check_connection(self) -> bool:
        """Heartbeat check untuk memastikan terminal masih responsif."""
        terminal_info = mt5.terminal_info()
        if terminal_info is None or not terminal_info.connected:
            logger.warning("MT5 Terminal disconnected. Attempting reconnect...")
            self.connected = False
            return self.initialize()
        return True
