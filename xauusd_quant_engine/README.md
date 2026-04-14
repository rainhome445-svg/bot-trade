# XAUUSD Quant Engine

Sebuah trading engine kuantitatif murni (CLI-based) yang terhubung ke MetaTrader 5 via library MetaTrader5. Engine ini dirancang khusus untuk memperdagangkan instrumen XAUUSD (Gold) menggunakan pola desain Multi-Agent Micro-architecture.

## Arsitektur (Multi-Agent System)

1. **Data Ingestion Agent**: Bertugas menarik data tick dan OHLCV dari MT5 secara real-time.
2. **Market State Agent**: Menganalisis kondisi makro XAUUSD (volatilitas dan tren).
3. **Momentum & Reversion Agent**: Mencari titik jenuh (Overbought/Oversold).
4. **Order Block / Price Action Agent**: Mendeteksi support/resistance dinamis.
5. **The Master Node (Decision Engine)**: Memvalidasi konfluens dari Agent 2, 3, dan 4 untuk eksekusi trade.
6. **Risk Management & Execution Agent**: Manajemen risiko (position sizing, Stop Loss, Trailing Stop) dan eksekusi order.

## Syarat

- Python 3.9+
- Windows OS (karena library MetaTrader5 hanya berjalan di Windows)
- MetaTrader 5 Terminal terinstal dan berjalan
