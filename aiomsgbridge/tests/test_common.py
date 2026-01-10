import logging
import os
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from aiomsgbridge.common import Chunk
from aiomsgbridge.common import IndexManager
from aiomsgbridge.common import Message
from aiomsgbridge.common import get_current_stat
from aiomsgbridge.common import make_convert_data
from aiomsgbridge.common import make_restore_data
from aiomsgbridge.common import read_env_bool
from aiomsgbridge.common import read_env_float
from aiomsgbridge.common import read_env_int
from aiomsgbridge.common import read_env_list
from aiomsgbridge.common import read_env_str
from aiomsgbridge.common import save_stat
from aiomsgbridge.common import sort_message
from aiomsgbridge.common import traffic_stats
from aiomsgbridge.common import traffic_stats_inc


# Test Message dataclass
def test_message_creation():
    msg = Message(num=1, client="test", data=b"data", v=b"v1", x=10)
    assert msg.num == 1
    assert msg.client == "test"
    assert msg.data == b"data"
    assert msg.v == b"v1"
    assert msg.x == 10


# Test Chunk dataclass
def test_chunk_creation():
    messages = [Message(1, "c1", b"d1", b"v1", 1)]
    chunk = Chunk(m=messages, i=1)
    assert chunk.m == messages
    assert chunk.i == 1


# Test sort_message function
def test_sort_message():
    msg = Message(5, "test", b"data", b"v1", 10)
    assert sort_message(msg) == 5


# Test environment reading functions
def test_read_env_str():
    with patch.dict(os.environ, {"TEST_VAR": "value"}):
        assert read_env_str("TEST_VAR") == "value"
    assert read_env_str("NON_EXISTENT") == ""
    assert read_env_str("NON_EXISTENT", "default") == "default"


def test_read_env_bool():
    with patch.dict(os.environ, {"TEST_BOOL": "true"}):
        assert read_env_bool("TEST_BOOL") is True
    with patch.dict(os.environ, {"TEST_BOOL": "false"}):
        assert read_env_bool("TEST_BOOL") is False
    assert read_env_bool("NON_EXISTENT") is False
    assert read_env_bool("NON_EXISTENT", True) is True


def test_read_env_float():
    with patch.dict(os.environ, {"TEST_FLOAT": "3.14"}):
        assert read_env_float("TEST_FLOAT") == 3.14
    assert read_env_float("NON_EXISTENT") == 0.0
    assert read_env_float("NON_EXISTENT", 1.5) == 1.5


def test_read_env_int():
    with patch.dict(os.environ, {"TEST_INT": "42"}):
        assert read_env_int("TEST_INT") == 42
    assert read_env_int("NON_EXISTENT") == 0
    assert read_env_int("NON_EXISTENT", 10) == 10


def test_read_env_list():
    with patch.dict(os.environ, {"TEST_LIST": "a;b;c"}):
        assert read_env_list("TEST_LIST") == ["a", "b", "c"]
    assert read_env_list("NON_EXISTENT") == []
    assert read_env_list("NON_EXISTENT", ["x", "y"]) == ["x", "y"]


@pytest.mark.asyncio
async def test_get_current_stat():
    stats = get_current_stat()
    assert "updated" in stats
    assert "coroutines" in stats
    assert isinstance(stats["coroutines"], int)


@pytest.mark.asyncio
async def test_save_stat():
    logger = logging.getLogger("test")
    with patch("aiofiles.open", new_callable=MagicMock) as mock_file:
        await save_stat(logger)
        mock_file.assert_not_called()


# Test data conversion functions
def test_make_convert_data():
    data = b"test data"
    converted = make_convert_data(data)
    assert isinstance(converted, bytes)


def test_make_restore_data():
    data = b"test data"
    converted = make_convert_data(data)
    restored = make_restore_data(converted)
    assert restored == data


# Test IndexManager class
def test_index_manager():
    manager = IndexManager()
    assert manager.get() == 1
    assert manager.get() == 2
    # Test overflow protection
    manager._value = 2 ** 32 - 5
    values = [manager.get() for _ in range(10)]
    assert values == [4294967292, 4294967293, 4294967294, 1, 2, 3, 4, 5, 6, 7]


# Test traffic statistics functions
@pytest.mark.asyncio
async def test_traffic_stats_inc():
    initial = traffic_stats.get("test_key", 0)
    await traffic_stats_inc("test_key")
    assert traffic_stats["test_key"] == initial + 1
