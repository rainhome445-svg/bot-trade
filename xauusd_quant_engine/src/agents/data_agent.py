import asyncio
import logging
from collections import deque
from typing import Dict, Deque, Any, Optional
import MetaTrader5 as mt5
import pandas as pd

logger = logging.getLogger("DataAgent")

class EventBus:
    """
    Sistem Pub/Sub internal untuk mendistribusikan data ke agent analitik tanpa blocking.
    Menggunakan asyncio.Queue dengan batasan ukuran agar tidak memakan memori berlebih.
    """
    def __init__(self, queue_size: int = 100):
        self.tick_queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
        self.ohlcv_queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)

        # Event flag sebagai notifikasi instan
        self.new_tick_event: asyncio.Event = asyncio.Event()
        self.new_candle_event: asyncio.Event = asyncio.Event()

    async def publish_tick(self, tick_data: Dict[str, Any]) -> None:
        """Mempublikasikan data tick terbaru. Jika antrean penuh, buang data terlama."""
        if self.tick_queue.full():
            try:
                # Membuang data terlama jika antrean penuh (mencegah bottleneck)
                self.tick_queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        await self.tick_queue.put(tick_data)
        self.new_tick_event.set()

    async def publish_ohlcv(self, timeframe: str, data: pd.DataFrame) -> None:
        """Mempublikasikan data candle terbaru. Jika antrean penuh, buang data terlama."""
        payload = {"timeframe": timeframe, "data": data}
        if self.ohlcv_queue.full():
            try:
                self.ohlcv_queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        await self.ohlcv_queue.put(payload)
        self.new_candle_event.set()


class DataAgent:
    """
    Agent yang bertanggung jawab untuk mengambil data dari MT5 (Ingestion),
    mengelolanya dengan memori terbatas (Bounded Memory), dan mempublikasikannya (Event Bus).
    """
    def __init__(self, symbol: str = "XAUUSD", executor: Any = None):
        self.symbol: str = symbol
        self.executor = executor
        self.is_running: bool = False

        # Event Bus
        self.bus: EventBus = EventBus(queue_size=100)

        # Bounded Memory Structures
        # maxlen=10000 tick (sekitar beberapa jam pergerakan aktif)
        self.tick_history: Deque[Dict[str, float]] = deque(maxlen=10000)

        # OHLCV History, maxlen=1000 bars per timeframe
        self.ohlcv_history: Dict[str, pd.DataFrame] = {
            "M1": pd.DataFrame(),
            "M5": pd.DataFrame(),
            "M15": pd.DataFrame()
        }

        # Mapping timeframe string to MT5 timeframe enum
        self.tf_map: Dict[str, int] = {
            "M1": mt5.TIMEFRAME_M1,
            "M5": mt5.TIMEFRAME_M5,
            "M15": mt5.TIMEFRAME_M15
        }

        # Track last processed times to avoid redundant processing
        self.last_tick_time_ms: int = 0
        self.last_bar_time: Dict[str, int] = {"M1": 0, "M5": 0, "M15": 0}

    async def _fetch_tick(self) -> Optional[Any]:
        """Wrapper untuk memanggil fungsi MT5 secara non-blocking via executor"""
        loop = asyncio.get_running_loop()
        # symbol_info_tick is fast, but wrapping it in executor ensures thread safety if MT5 calls block
        if self.executor:
            tick = await loop.run_in_executor(self.executor, mt5.symbol_info_tick, self.symbol)
        else:
            tick = mt5.symbol_info_tick(self.symbol)
        return tick

    async def _fetch_rates(self, tf_enum: int, count: int = 1000) -> Optional[Any]:
        loop = asyncio.get_running_loop()
        if self.executor:
            rates = await loop.run_in_executor(self.executor, mt5.copy_rates_from_pos, self.symbol, tf_enum, 0, count)
        else:
            rates = mt5.copy_rates_from_pos(self.symbol, tf_enum, 0, count)
        return rates

    async def tick_listener(self) -> None:
        """Task asinkron untuk menarik data tick secara konstan."""
        logger.info("Tick Listener started.")
        while self.is_running:
            try:
                tick = await self._fetch_tick()
                if tick is not None:
                    # Tick MT5 time_msc
                    if getattr(tick, 'time_msc', 0) > self.last_tick_time_ms:
                        self.last_tick_time_ms = tick.time_msc

                        tick_dict: Dict[str, float] = {
                            "time_msc": float(tick.time_msc),
                            "bid": float(tick.bid),
                            "ask": float(tick.ask),
                            "volume": float(tick.volume)
                        }

                        # Store in bounded memory
                        self.tick_history.append(tick_dict)

                        # Broadcast
                        await self.bus.publish_tick(tick_dict)

                # Delay kecil (misal 10ms) untuk mencegah rate-limiting CPU & MT5 (throttle)
                await asyncio.sleep(0.01)
            except Exception as e:
                logger.error(f"Error in Tick Listener: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def ohlcv_poller(self) -> None:
        """Task asinkron untuk menarik OHLCV data pada multiple timeframe."""
        logger.info("OHLCV Poller started.")
        while self.is_running:
            try:
                for tf_str, tf_enum in self.tf_map.items():
                    rates = await self._fetch_rates(tf_enum, count=1000)
                    if rates is not None and len(rates) > 0:
                        df = pd.DataFrame(rates)
                        df['time'] = pd.to_datetime(df['time'], unit='s')

                        # Set data to bounded memory equivalent (we only store the latest 1000)
                        self.ohlcv_history[tf_str] = df

                        last_time = int(rates[-1]['time'])
                        # Jika ada bar baru (closed/open), broadcast
                        if last_time > self.last_bar_time[tf_str]:
                            self.last_bar_time[tf_str] = last_time
                            await self.bus.publish_ohlcv(tf_str, df)

                # Polling setiap 1 detik sudah cukup responsif untuk data candle
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error in OHLCV Poller: {e}", exc_info=True)
                await asyncio.sleep(5)

    def start(self) -> None:
        """Memulai agen."""
        self.is_running = True
        # Run background tasks
        asyncio.create_task(self.tick_listener())
        asyncio.create_task(self.ohlcv_poller())

    def stop(self) -> None:
        """Menghentikan agen."""
        self.is_running = False
        logger.info("Data Agent stopped.")

"""
===================================================================================
PENJELASAN ALOKASI RAM & MEKANISME ANTRIAN (QUEUE BOTTLENECK PREVENTION)
===================================================================================

1. Estimasi Alokasi RAM (Bounded Memory):
   - tick_history (Deque maxlen=10000):
     Setiap item adalah dictionary dengan 4 key (time_msc, bid, ask, volume).
     Satu dictionary Python kecil mengambil ~240 bytes.
     10,000 dicts * 240 bytes = ~2.4 MB. Sangat ringan dan otomatis menghapus (FIFO)
     tick paling lama tanpa invoking heavy Garbage Collection.
   - ohlcv_history (3 Timeframes, masing-masing 1000 rows):
     Satu row dalam pandas DataFrame dengan kolom numerik dasar (~8 kolom) memakan
     sekitar 64 bytes (8 * 8 byte float64).
     1000 rows * 64 bytes = ~64 KB per timeframe.
     3 timeframe (M1, M5, M15) = ~192 KB.
   Total memori state di Agen 1 sangat statis di angka < 5 MB, jauh dari batas
   4.6 GB RAM.

2. Pencegahan Bottleneck Antrean (Queue Drop-Tail / LIFO-style override):
   - Pada kelas `EventBus`, parameter `maxsize=100` digunakan untuk membatasi
     buffer broadcast.
   - Jika Agent analis lambat dan antrean penuh (`self.tick_queue.full()`), metode
     `publish_tick` dan `publish_ohlcv` akan secara eksplisit membuang (drop) event
     tertua dengan memanggil `get_nowait()`.
   - Hal ini memastikan bahwa Decision/Master Agent selalu mengevaluasi data
     "paling mutakhir" (Low-latency/real-time) dan engine tidak terhenti (lag/memory leak)
     akibat penumpukan objek antrean yang belum diproses.
===================================================================================
"""
