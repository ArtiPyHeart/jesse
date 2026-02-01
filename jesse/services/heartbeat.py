from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

import jesse.helpers as jh
import jesse.services.logger as logger

_CANDLE_HISTORY_LIMIT = 120
_CANDLE_MIN_INTERVAL_MS = 5_000
_last_candle_write_ts = 0


def _health_dir() -> Path:
    env_path = os.getenv("JESSE_HEALTH_DIR")
    if env_path:
        return Path(env_path)
    return Path("storage/health")


def _atomic_write(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(payload, separators=(",", ":"), sort_keys=True), encoding="utf-8"
    )
    tmp_path.replace(path)


def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.error(f"Failed to read heartbeat file: {path} error={exc}")
        return None


def record_account_heartbeat(
    exchange: str, available_margin: float, wallet_balance: float
) -> None:
    if not jh.is_live():
        return

    payload = {
        "ts": jh.now_to_timestamp(force_fresh=True),
        "exchange": exchange,
        "available_margin": float(available_margin),
        "wallet_balance": float(wallet_balance),
    }

    try:
        _atomic_write(_health_dir() / "account_heartbeat.json", payload)
    except Exception as exc:
        logger.error(f"Failed to write account heartbeat: {exc}")


def record_candle_heartbeat(
    exchange: str,
    symbol: str,
    timeframe: str,
    candle_ts: int,
    close_price: float,
) -> None:
    if not jh.is_live():
        return

    global _last_candle_write_ts
    now_ts = jh.now_to_timestamp(force_fresh=True)
    if now_ts - _last_candle_write_ts < _CANDLE_MIN_INTERVAL_MS:
        return

    payload = {
        "ts": now_ts,
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": timeframe,
        "candle_ts": int(candle_ts),
        "close": float(close_price),
    }

    path = _health_dir() / "candle_heartbeat.json"
    try:
        existing = _read_json(path)
        history = []
        if isinstance(existing, dict):
            history = existing.get("history", [])
            if not isinstance(history, list):
                history = []
        history.append(payload)
        if len(history) > _CANDLE_HISTORY_LIMIT:
            history = history[-_CANDLE_HISTORY_LIMIT:]
        data = {"last": payload, "history": history}
        _atomic_write(path, data)
        _last_candle_write_ts = now_ts
    except Exception as exc:
        logger.error(f"Failed to write candle heartbeat: {exc}")
