import peewee
from jesse.services.db import database
import jesse.helpers as jh
from jesse.services import logger
import numpy as np


if database.is_closed():
    database.open_connection()


class Candle(peewee.Model):
    id = peewee.UUIDField(primary_key=True)
    timestamp = peewee.BigIntegerField()
    open = peewee.FloatField()
    close = peewee.FloatField()
    high = peewee.FloatField()
    low = peewee.FloatField()
    volume = peewee.FloatField()
    exchange = peewee.CharField()
    symbol = peewee.CharField()
    timeframe = peewee.CharField()

    # partial candles: 5 * 1m candle = 5m candle while 1m == partial candle
    is_partial = True

    class Meta:
        from jesse.services.db import database

        database = database.db
        indexes = (
            (('exchange', 'symbol', 'timeframe', 'timestamp'), True),
        )

    def __init__(self, attributes: dict = None, **kwargs) -> None:
        peewee.Model.__init__(self, attributes=attributes, **kwargs)

        if attributes is None:
            attributes = {}

        for a, value in attributes.items():
            setattr(self, a, value)


# if database is open, create the table
if database.is_open():
    Candle.create_table()


# # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # DB FUNCTIONS # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # 


def store_candle_into_db(exchange: str, symbol: str, timeframe: str, candle: np.ndarray, on_conflict='ignore') -> None:
    d = {
        'id': jh.generate_unique_id(),
        'exchange': exchange,
        'symbol': symbol,
        'timeframe': timeframe,
        'timestamp': candle[0],
        'open': candle[1],
        'high': candle[3],
        'low': candle[4],
        'close': candle[2],
        'volume': candle[5]
    }

    def _exec():
        if on_conflict == 'ignore':
            return Candle.insert(**d).on_conflict_ignore().execute()
        if on_conflict == 'replace':
            return Candle.insert(**d).on_conflict(
                conflict_target=['exchange', 'symbol', 'timeframe', 'timestamp'],
                preserve=(Candle.open, Candle.high, Candle.low, Candle.close, Candle.volume),
            ).execute()
        if on_conflict == 'error':
            return Candle.insert(**d).execute()
        raise Exception(f'Unknown on_conflict value: {on_conflict}')

    try:
        _exec()
    except (peewee.OperationalError, peewee.InterfaceError) as exc:
        # Force a reconnect once, then retry the write.
        logger.error(f"Candle DB write failed, retry after reconnect: {exc}")
        database.reconnect()
        _exec()


def store_candles_into_db(exchange: str, symbol: str, timeframe: str, candles: np.ndarray, on_conflict='ignore') -> None:
    # make sure the number of candles is more than 0
    if len(candles) == 0:
        raise Exception(f'No candles to store for {exchange}-{symbol}-{timeframe}')

    # convert candles to list of dicts
    candles_list = []
    for candle in candles:
        d = {
            'id': jh.generate_unique_id(),
            'symbol': symbol,
            'exchange': exchange,
            'timestamp': candle[0],
            'open': candle[1],
            'high': candle[3],
            'low': candle[4],
            'close': candle[2],
            'volume': candle[5],
            'timeframe': timeframe,
        }
        candles_list.append(d)

    def _exec():
        if on_conflict == 'ignore':
            return Candle.insert_many(candles_list).on_conflict_ignore().execute()
        if on_conflict == 'replace':
            return Candle.insert_many(candles_list).on_conflict(
                conflict_target=['exchange', 'symbol', 'timeframe', 'timestamp'],
                preserve=(Candle.open, Candle.high, Candle.low, Candle.close, Candle.volume),
            ).execute()
        if on_conflict == 'error':
            return Candle.insert_many(candles_list).execute()
        raise Exception(f'Unknown on_conflict value: {on_conflict}')

    try:
        _exec()
    except (peewee.OperationalError, peewee.InterfaceError) as exc:
        logger.error(f"Bulk candle DB write failed, retry after reconnect: {exc}")
        database.reconnect()
        _exec()


def fetch_candles_from_db(exchange: str, symbol: str, timeframe: str, start_date: int, finish_date: int) -> tuple:
    res = tuple(
        Candle.select(
            Candle.timestamp, Candle.open, Candle.close, Candle.high, Candle.low,
            Candle.volume
        ).where(
            Candle.exchange == exchange,
            Candle.symbol == symbol,
            Candle.timeframe == timeframe,
            Candle.timestamp.between(start_date, finish_date)
        ).order_by(Candle.timestamp.asc()).tuples()
    )

    return res
