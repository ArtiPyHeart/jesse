import json

import jesse.helpers as jh
from jesse.services import heartbeat


def test_account_heartbeat_written(monkeypatch, tmp_path):
    monkeypatch.setenv("JESSE_HEALTH_DIR", str(tmp_path))
    monkeypatch.setattr(jh, "is_live", lambda: True)

    heartbeat.record_account_heartbeat(
        exchange="Binance Perpetual Futures",
        available_margin=123.45,
        wallet_balance=678.9,
    )

    path = tmp_path / "account_heartbeat.json"
    assert path.exists()
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["exchange"] == "Binance Perpetual Futures"
    assert data["available_margin"] == 123.45
    assert data["wallet_balance"] == 678.9


def test_candle_heartbeat_history_trim(monkeypatch, tmp_path):
    monkeypatch.setenv("JESSE_HEALTH_DIR", str(tmp_path))
    monkeypatch.setattr(jh, "is_live", lambda: True)
    monkeypatch.setattr(heartbeat, "_CANDLE_HISTORY_LIMIT", 3)
    monkeypatch.setattr(heartbeat, "_CANDLE_MIN_INTERVAL_MS", 0)
    heartbeat._last_candle_write_ts = 0

    for i in range(5):
        heartbeat.record_candle_heartbeat(
            exchange="Binance Perpetual Futures",
            symbol="BTC-USDT",
            timeframe="1m",
            candle_ts=i,
            close_price=100 + i,
        )

    path = tmp_path / "candle_heartbeat.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["last"]["candle_ts"] == 4
    assert len(data["history"]) == 3
    assert data["history"][0]["candle_ts"] == 2
