import asyncio
import os
import zlib
from dataclasses import dataclass
from datetime import datetime
from warnings import warn as log_warn

from cachetools import TTLCache
from cryptography.fernet import Fernet


@dataclass
class Message:
    """Represents a message with sequence number, client identifier, data, version, and x value."""
    num: int
    client: str
    data: bytes
    v: bytes
    x: int


@dataclass
class Chunk:
    """Chunk with messages"""
    m: list[Message]
    i: int


def sort_message(msg: Message) -> int:
    return msg.num


def read_env_str(name: str, default: str = "") -> str:
    """Read an environment variable and return its string value."""
    return str(os.environ.get(name, default))


def read_env_bool(name: str, default: bool = False) -> bool:
    """Read an environment variable and convert its value to boolean."""
    value = os.environ.get(name, f"{default}").lower()
    return value in ("true", "on", "ok", "1", "yes")


def read_env_float(name: str, default: float = 0.0) -> float:
    """Read an environment variable and convert its value to float."""
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


def read_env_int(name: str, default: int = 0) -> int:
    """Read an environment variable and convert its value to integer."""
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


def read_env_list(name: str, split_with: str = ";", default: list = []) -> list[str]:
    """Read an environment variable and return its value as a list."""
    result = []
    values = read_env_str(name)
    if values:
        result.extend(values.split(split_with))
    else:
        result.extend(default)

    return result


traffic_stats = TTLCache(maxsize=1000, ttl=3600 * 24)
stat_lock = asyncio.Lock()


async def traffic_stats_inc(key: str, val: int = 1):
    """Increment traffic statistics for a given key."""
    async with stat_lock:
        if key in traffic_stats:
            traffic_stats[key] = traffic_stats[key] + val
        else:
            traffic_stats[key] = val


async def set_stats_value(key: str, val: float):
    """Increment traffic statistics for a given key."""
    async with stat_lock:
        traffic_stats[key] = val


def get_current_stat() -> dict:
    traffic_stats_dict = dict(traffic_stats)
    traffic_stats_dict["updated"] = datetime.now().isoformat()
    return traffic_stats_dict


LOG_LEVEL = read_env_str("LOG_LEVEL") or "info"
BROKER_HOST = read_env_str("BROKER_HOST", "localhost")
BROKER_USER = read_env_str("BROKER_USER")
BROKER_PASSWORD = read_env_str("BROKER_PASSWORD")
BROKER_PORT = read_env_int("BROKER_PORT", 1883)
STOP_W = "quit"
BUFFER_SIZE = read_env_int("READ_BUFFER_SIZE", 1024 * 8)
WORKERS = read_env_int("WORKERS")
uvloop = read_env_bool("UVLOOP", True)
use_compress = read_env_bool("COMPRESS", False)
CRYPTO_KEY = read_env_str("CRYPTO_KEY")
CRYPTO_ALG = Fernet(CRYPTO_KEY) if CRYPTO_KEY else None
qos = read_env_int("QOS_LEVEL", 0)
CONNECTION_IDLE_LIMIT = read_env_int("CONNECTION_IDLE_LIMIT", 300)
STAT_FILE = read_env_str("STAT_FILE")
CHUNK_COLLECT_DELAY = read_env_float("CHUNK_COLLECT_DELAY", 0.065)


if uvloop:
    try:
        import uvloop
        uvloop.install()
    except Exception as err:
        log_warn(err)


def make_convert_data(data: bytes) -> bytes:
    """Compress and encrypt data for transmission."""
    raw = CRYPTO_ALG.encrypt(data) if CRYPTO_ALG else data
    return zlib.compress(raw, 9) if use_compress else raw


def make_restore_data(data: bytes) -> bytes:
    """Decompress and decrypt data from transmission."""
    src_data = zlib.decompress(data) if use_compress else data
    if CRYPTO_ALG:
        return CRYPTO_ALG.decrypt(src_data)
    else:
        return src_data


class IndexManager:
    _value = 0
    _limit = 2 ** 32 - 1

    def __init__(self):
        self._value = 0

    def get(self) -> int:
        self._value += 1
        if self._value >= self._limit:
            self._value = 0

        return self._value
