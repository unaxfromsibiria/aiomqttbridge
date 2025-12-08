import asyncio
import hashlib
import json
import logging
import os
import pickle
import uuid
import zlib
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from aiomqtt import Client
from aiomqtt import ProtocolVersion
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


def sort_mgs(msg: Message) -> int:
    """Sort messages by their sequence number."""
    return msg.num


async def read_queue(buffer: list, quit_cmd: list, queue: asyncio.Queue):
    """Read a message from queue and add to buffer or mark quit command."""
    msg: Message = await queue.get()
    if isinstance(msg, str) and msg == STOP_W:
        quit_cmd.append(1)
    else:
        buffer.append(msg)


def read_env_str(name: str, default: str = "") -> str:
    """Reads an environment variable by name and return value."""
    return str(os.environ.get(name, default))


def read_env_bool(name: str, default: bool = False) -> bool:
    """Reads an environment variable by name and converts value as boolean."""
    value = os.environ.get(name, f"{default}").lower()
    return value in ("true", "on", "ok", "1", "yes")


def read_env_float(name: str, default: float = 0.0) -> float:
    """Reads an environment variable by name and converts value to float."""
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


def read_env_int(name: str, default: int = 0) -> int:
    """Reads an environment variable by name and converts value to integer."""
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


LOG_LEVEL = read_env_str("LOG_LEVEL") or "info"
SERVER_TARGET = read_env_str("SERVER_TARGET")
BROKER_HOST = read_env_str("BROKER_HOST", "localhost")
BROKER_USER = read_env_str("BROKER_USER")
BROKER_PASSWORD = read_env_str("BROKER_PASSWORD")
BROKER_PORT = read_env_int("BROKER_PORT", 1883)
STOP_W = "quit"
RECONNECT_TIMEOUT = read_env_float("RECONNECT_TIMEOUT", 5)
RECONNECT_ATTEMPT = read_env_int("RECONNECT_ATTEMPT", 30)
BUFFER_SIZE = read_env_int("READ_BUFFER_SIZE", 1024 * 4)
WORKERS = read_env_int("WORKERS")
CONNECTION_IDLE_LIMIT = read_env_int("CONNECTION_IDLE_LIMIT", 300)
uvloop = read_env_bool("UVLOOP", True)
use_compress = read_env_bool("COMPRESS", False)
STAT_FILE = read_env_str("STAT_FILE")
CRYPTO_KEY = read_env_str("CRYPTO_KEY")
CRYPTO_ALG = Fernet(CRYPTO_KEY) if CRYPTO_KEY else None
qos = read_env_int("QOS_LEVEL", 1)


def _convert_data(data: bytes) -> bytes:
    """Compress and encrypt data for transmission."""
    raw = CRYPTO_ALG.encrypt(data) if CRYPTO_ALG else data
    return zlib.compress(raw, 9) if use_compress else raw


def _restore_data(data: bytes) -> bytes:
    """Decompress and decrypt data from transmission."""
    src_data = zlib.decompress(data) if use_compress else data
    if CRYPTO_ALG:
        return CRYPTO_ALG.decrypt(src_data)
    else:
        return src_data

logger = logging.getLogger(__name__)
traffic_stats = TTLCache(maxsize=1000, ttl=3600 * 24)
stat_lock = asyncio.Lock()


async def traffic_stats_inc(key: str, val: int = 1):
    """Increment traffic statistics for a given key."""
    async with stat_lock:
        if key in traffic_stats:
            traffic_stats[key] = traffic_stats[key] + val
        else:
            traffic_stats[key] = val


level = getattr(logging, LOG_LEVEL.upper(), logging.WARNING)
logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")
if uvloop:
    try:
        import uvloop
        uvloop.install()
    except Exception as err:
        logger.critical(err)


class OutConnectionServer:
    """
    TCP proxy server that forwards data between TCP targets and MQTT topics.
    This server manages connections to multiple TCP targets and routes data
    through MQTT topics. It handles connection establishment, data forwarding,
    and cleanup of idle connections.
    """

    _out_routes: Dict[tuple, asyncio.Queue] = {}
    _wait_iteration_time: float = 0.0045
    _iter_timeout: float = 0.0012
    _mqtt_client = None
    _topics: List[str]
    _out_topic_tpl: str
    _executor: ProcessPoolExecutor = None
    _latest_message: Dict[tuple, datetime] = {}
    tcp_targets: Dict[str, tuple] = {}
    service_check_interval: float = 3.0
    connections: Dict[tuple, Any] = {}

    def __init__(self):
        """Initialize the proxy server with target configurations."""
        self.server = None
        self._out_routes = {}
        self._latest_message = {}
        self.tcp_targets = {}
        self.connections = {}
        self._topics = []
        # Parse SERVER_TARGET
        if not SERVER_TARGET:
            raise ValueError("SERVER_TARGET environment variable is required")

        self._parse_targets()
        self._out_topic_tpl = "data/{}"
        for target_code in self.tcp_targets:
            self._topics.append(f"func/{target_code}")

    async def _process_reader(self, reader: asyncio.StreamReader, target: str, connection_id: str):
        """
        Process data received from TCP target and publish to MQTT.
        Reads data from TCP connection, converts it, and publishes to MQTT topic.
        Handles errors and connection closure gracefully.
        """
        loop = asyncio.get_event_loop()
        index = 1
        wait = True
        topic = self._out_topic_tpl.format(target)
        while wait:
            try:
                data = await reader.read(BUFFER_SIZE)
                if not data:
                    wait = False
                    continue

                if self._executor:
                    c_data = await loop.run_in_executor(self._executor, _convert_data, data)
                else:
                    c_data = _convert_data(data)

                msg_content = pickle.dumps(
                    Message(index, connection_id, c_data, uuid.uuid4().bytes, 0)
                )
                index += 1
                await traffic_stats_inc("tcp_in", len(data))
            except Exception as err:
                logger.error(f"Error in handle read connection: {err}")
                msg_content = pickle.dumps(Message(index, connection_id, b"", b"", 1))

            try:
                await self._mqtt_client.publish(topic, payload=msg_content, qos=qos)
            except Exception as err:
                wait = False
                logger.error(f"Error to send in {topic} from connection {connection_id}: {err}")
            else:
                await traffic_stats_inc("mqtt_out", len(msg_content))
                self._latest_message[(target, connection_id)] = datetime.now()

    async def _process_writer(self, writer: asyncio.StreamWriter, queue: asyncio.Queue):
        """
        Process data from MQTT and write to TCP target.
        Reads messages from queue and writes them to TCP connection.
        Handles connection errors and message sorting.
        """
        quit_cmd = []
        parts = []
        iter_time = self._wait_iteration_time
        while not quit_cmd:
            parts.clear()
            try:
                await asyncio.wait_for(read_queue(parts, quit_cmd, queue), timeout=iter_time)
            except asyncio.TimeoutError:
                ...

            if parts:
                self._latest_message[parts[0].client] = datetime.now()
                parts.sort(key=sort_mgs)
                try:
                    for msg in parts:
                        writer.write(msg.data)
                        await writer.drain()
                        await traffic_stats_inc("tcp_out", len(msg.data))
                except Exception as err:
                    logger.error(f"Error in handle read connection: {err}")
                    quit_cmd.append(1)
            else:
                await asyncio.sleep(self._iter_timeout)

    def _parse_targets(self):
        """
        Parse SERVER_TARGET environment variable into target configurations.
        Converts target strings into structured configurations with unique codes
        and validates port numbers and host addresses.
        """
        tcp_targets = {}
        sockets = set()
        
        for target_socket in SERVER_TARGET.split(";"):
            parts = target_socket.split(":")
            try:
                target_code, t_host, t_port = parts
                assert target_code
                assert t_host
                t_port = int(t_port)
                assert 1 <= t_port <= 2 ** 16
                t_name = target_code
                target_code = hashlib.sha1(target_code.encode()).hexdigest()[:8]
            except Exception as err:
                raise ValueError(
                    f"Incorrect targets in env 'SERVER_TARGET': {parts} ({err})"
                    " format:  'target_code1:host:port;target_code2:host:port'"
                )
            else:
                if (t_host, t_port) in sockets:
                    raise ValueError(
                        f"Duplicate socket detected: {t_host}:{t_port}. "
                        "Each target must have a unique socket."
                    )
                tcp_targets[target_code] = (t_name, t_host, t_port)
                sockets.add((t_host, t_port))
                logger.info(f"Target '{t_name}' in {t_host}:{t_port} with code: {target_code}")
        
        self.tcp_targets = tcp_targets

    async def create_target_connection(self, target: str, connection_id: str, queue: asyncio.Queue):
        """
        Establish and manage connection to TCP target.
        Creates TCP connection to target and manages reader/writer tasks.
        Handles reconnection attempts and task cleanup.
        """
        wait = True
        t_name, target_host, target_port = self.tcp_targets[target]
        reconnect = False
        tasks = None
        attempt_count = 0
        loop = asyncio.get_event_loop()
        while wait:
            if reconnect:
                attempt_count += 1
                if attempt_count > RECONNECT_ATTEMPT:
                    logger.error(f"Max reconnect attempts ({RECONNECT_ATTEMPT}) exceeded for '{t_name}'")
                    wait = False
                    break
                logger.warning(f"Reconnecting to '{t_name}' (attempt {attempt_count}/{RECONNECT_ATTEMPT})...")
                await asyncio.sleep(RECONNECT_TIMEOUT)
            try:
                target_reader, target_writer = await asyncio.open_connection(target_host, target_port)
            except Exception as err:
                logger.error(f"Failed to connect to '{t_name}': {err}")
                reconnect = True
                continue
            else:
                logger.info(f"Connected to '{t_name}' ({target_host}:{target_port})")
                reconnect = False
                attempt_count = 0  # Reset attempt count on successful connection
                tasks = [
                    loop.create_task(self._process_reader(target_reader, target, connection_id)),
                    loop.create_task(self._process_writer(target_writer, queue)),
                ]

            if tasks:
                _, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                # Cancel remaining tasks
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

                logger.info(f"Connection to '{t_name}' closed")

    async def wait_messages(self):
        """
        Process incoming MQTT messages and route to TCP targets.
        Listens for messages on MQTT topics and forwards them to appropriate
        TCP connections. Handles connection creation and message deserialization.
        """
        loop = asyncio.get_event_loop()
        for topic in self._topics:
            try:
                await self._mqtt_client.subscribe(topic, qos=qos)
            except Exception as err:
                logger.error(f"Failed to subscribe to topic '{topic}': {err}")
                return
            else:
                logger.info(f"Listen {topic}")

        async for message in self._mqtt_client.messages:
            target = next(code for code in self.tcp_targets if code in message.topic.value)
            if message.payload:
                try:
                    msg: Message = pickle.loads(message.payload)
                except Exception as err:
                    logger.error(f"Failed to deserialize message: {err}")
                    continue
                else:
                    await traffic_stats_inc("mqtt_in", len(message.payload))

                route_key = (target, msg.client)
                self._latest_message[route_key] = datetime.now()

                queue = None
                if msg.x:
                    logger.warning(f"Client {msg.client} requests closing connection in {target}")
                    if route_key in self._out_routes:
                        queue = self._out_routes[route_key]
                        # queue.put_nowait(STOP_W)
                    continue

                if route_key not in self._out_routes:
                    queue = self._out_routes[route_key] = asyncio.Queue()
                    task = loop.create_task(self.create_target_connection(target, msg.client, queue))
                    self.connections[route_key] = task
                else:
                    queue = self._out_routes[route_key]

                try:
                    if self._executor:
                        data = await loop.run_in_executor(self._executor, _restore_data, msg.data)
                    else:
                        data = _restore_data(msg.data)
                except Exception as err:
                    logger.error(f"Failed to restore data for client {msg.client} in topic {target}: {err}")
                    continue
                else:
                    msg.data = data
                    queue.put_nowait(msg)
            else:
                await asyncio.sleep(self._iter_timeout)

        logger.warning("Closed subscribers")

    async def service_process(self):
        """
        Monitor and cleanup idle connections.
        Checks for idle connections based on timeout limits and cleans them up
        by canceling tasks and removing references.
        """
        idle_limit = CONNECTION_IDLE_LIMIT
        wait = True
        while wait:
            await asyncio.sleep(self.service_check_interval)
            connections_to_remove = []
            current_time = datetime.now()
            for (conn_code, connection_id), _ in list(self._out_routes.items()):
                # Check if connection is idle
                connection_key = (conn_code, connection_id)
                if connection_key in self._latest_message:
                    last_message_time: datetime = self._latest_message.get(connection_key, current_time)
                    if (current_time - last_message_time).total_seconds() > idle_limit:
                        # Connection is idle, mark for cleanup
                        connections_to_remove.append(connection_key)

            if connections_to_remove:
                con_list = ", ".join(k for _, k in connections_to_remove)
                logger.warning(f"Closing {len(connections_to_remove)} '{conn_code}' connections: {con_list}")
                await traffic_stats_inc("idle_connections", len(connections_to_remove))

            for connection_key in connections_to_remove:
                # Cancel the task if it exists
                if connection_key in self.connections:
                    task = self.connections[connection_key]
                    if not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                        except Exception as err:
                            logger.error(f"Error canceling task for connection {connection_key}: {err}")
                # Remove from queues
                if connection_key in self._out_routes:
                    try:
                        del self._out_routes[connection_key]
                    except Exception as err:
                        logger.error(f"Error removing queue for connection {connection_key}: {err}")
                # Remove from connections dict
                if connection_key in self.connections:
                    try:
                        del self.connections[connection_key]
                    except Exception as err:
                        logger.error(f"Error removing connection from dict for {connection_key}: {err}")
                    else:
                        await traffic_stats_inc("clear_connections")
                # Remove from latest_message
                if connection_key in self._latest_message:
                    try:
                        del self._latest_message[connection_key]
                    except Exception as err:
                        logger.error(f"Error removing latest_message for connection {connection_key}: {err}")
            # save satistic
            if STAT_FILE:
                try:
                    with open(STAT_FILE, "w") as json_file:
                        traffic_stats_dict = dict(traffic_stats)
                        traffic_stats_dict["updated"] = datetime.now().isoformat()
                        json.dump(traffic_stats_dict, json_file, indent=2)
                except Exception as err:
                    logger.error(f"Problem {err} to save statistic into '{STAT_FILE}'")

    async def start_server(self, executor: Optional[ProcessPoolExecutor] = None):
        """
        Start the proxy server and begin processing connections.
        Initializes MQTT connection and starts message processing and service
        monitoring tasks.
        """
        self._executor = executor
        loop = asyncio.get_event_loop()
        tasks = []
        async with Client(
            hostname=BROKER_HOST,
            port=BROKER_PORT,
            username=BROKER_USER,
            password=BROKER_PASSWORD,
            protocol=ProtocolVersion.V5,
        ) as client:
            self._mqtt_client = client
            logger.info(f"Connection to broker {BROKER_HOST}")
            tasks.append(loop.create_task(self.wait_messages()))
            tasks.append(loop.create_task(self.service_process()))
            _, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)            
            # Cancel remaining tasks
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass


async def main():
    """Main entry point for the proxy server."""
    proxy = OutConnectionServer()
    if WORKERS > 0:
        with ProcessPoolExecutor(max_workers=WORKERS) as executor:
            await proxy.start_server(executor)
    else:
        await proxy.start_server()


if __name__ == "__main__":
    asyncio.run(main())
