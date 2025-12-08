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


LOG_LEVEL = read_env_str("LOG_LEVEL") or "info"
BROKER_HOST = read_env_str("BROKER_HOST", "localhost")
BROKER_USER = read_env_str("BROKER_USER")
BROKER_PASSWORD = read_env_str("BROKER_PASSWORD")
BROKER_PORT = read_env_int("BROKER_PORT", 1883)
STOP_W = "quit"
SOCKETS = read_env_str("SOCKETS")
# eg SOCKETS=ssh:localhost:22;web:0.0.0.0:80;postgres:127.0.0.1:5432
STAT_FILE = read_env_str("STAT_FILE")
BUFFER_SIZE = read_env_int("READ_BUFFER_SIZE", 1024 * 8)
WORKERS = read_env_int("WORKERS")
CONNECTION_IDLE_LIMIT = read_env_int("CONNECTION_IDLE_LIMIT", 300)
uvloop = read_env_bool("UVLOOP", True)
use_compress = read_env_bool("COMPRESS", False)
CRYPTO_KEY = read_env_str("CRYPTO_KEY")
CRYPTO_ALG = Fernet(CRYPTO_KEY) if CRYPTO_KEY else None
qos = read_env_int("QOS_LEVEL", 0)


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


class SocketServer:
    """
    Handles TCP to MQTT proxy server functionality.
    This class manages bidirectional data transfer between TCP clients and MQTT brokers,
    routing data through MQTT topics for communication between different services.
    """
    
    _out_routes: Dict[str, asyncio.Queue] = {}
    _wait_iteration_time: float = 0.0045
    _iter_timeout: float = 0.0012
    _mqtt_client = None
    _topoc: str
    res_topic: str
    _latest_message: Dict[tuple, datetime] = {}
    _executor: ProcessPoolExecutor = None
    # target
    _target: str = ""
    _port: int = 0
    _host: str = ""

    def __init__(
        self,
        mqtt_client: Client,
        executor: Optional[ProcessPoolExecutor],
        target: str,
        host: str,
        port: int
    ):
        """Initialize the proxy server with MQTT client and connection parameters."""
        self.server = None
        self._out_routes = {}
        self._latest_message = {}
        self._target = target
        self._mqtt_client = mqtt_client
        self._executor = executor
        self._host = host
        self._port = port
        t_part = hashlib.sha1(target.encode()).hexdigest()[:8]
        self._topic = f"func/{t_part}"
        self.res_topic = f"data/{t_part}"

    async def send_to_server(self, client: str, reader: asyncio.StreamReader):
        """Send data from client to server via MQTT."""
        index = 1
        wait = True
        loop = asyncio.get_event_loop()

        while wait:

            try:
                data = await reader.read(BUFFER_SIZE)
            except Exception as err:
                logger.warning(f"Broken reading for '{client}': {err}")
                wait = False
                continue

            if not data:
                wait = False
                continue

            await traffic_stats_inc(f"tcp_in_{self._target}", len(data))
            if self._executor:
                c_data = await loop.run_in_executor(self._executor, _convert_data, data)
            else:
                c_data = _convert_data(data)

            self._latest_message[client] = datetime.now()
            msg_content = pickle.dumps(Message(index, client, c_data, uuid.uuid4().bytes, 0))
            try:
                await self._mqtt_client.publish(self._topic, payload=msg_content, qos=qos)
            except Exception as err:
                logger.error(f"Client {client} lost data in {index}: {err}")
            else:
                await traffic_stats_inc(f"mqtt_out_{self._target}", len(msg_content))
                index += 1

    async def send_to_client(self, queue: asyncio.Queue, writer: asyncio.StreamWriter):
        """Send data from server to client."""
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
                        await traffic_stats_inc(f"tcp_out_{self._target}", len(msg.data))
                except Exception as err:
                    logger.error(f"Error in handle read connection: {err}")
                    quit_cmd.append(1)
            else:
                await asyncio.sleep(self._iter_timeout)

    async def claen_up(self):
        """
        Clean up idle connections.
        Remove connections that have been idle beyond the configured limit.
        """
        idle_limit = CONNECTION_IDLE_LIMIT
        connections_to_remove = []
        current_time = datetime.now()
        for connection_id, _ in list(self._out_routes.items()):
            if connection_id in self._latest_message:
                last_message_time: datetime = self._latest_message.get(connection_id, current_time)
                if (current_time - last_message_time).total_seconds() > idle_limit:
                    # Connection is idle, mark for cleanup
                    connections_to_remove.append(connection_id)

        if connections_to_remove:
            await traffic_stats_inc(f"idle_connections_{self._target}", len(connections_to_remove))
            for connection_id in connections_to_remove:
                if connection_id in self._out_routes:
                    self._out_routes[connection_id].put_nowait(STOP_W)
                    await asyncio.sleep(self._iter_timeout)
                    try:
                        del self._out_routes[connection_id]
                    except Exception as err:
                        logger.error(f"Error removing queue for connection {connection_id}: {err}")
                # Remove from _latest_message
                if connection_id in self._latest_message:
                    try:
                        del self._latest_message[connection_id]
                    except Exception as err:
                        logger.error(f"Error removing _latest_message for connection {connection_id}: {err}")

    def has_client(self, client: str) -> bool:
        """Check if a client exists in the routes."""
        return client and client in self._out_routes

    def put_client_message(self, client: str, msg: Any) -> bool:
        """Put a message into the client's queue."""
        self._out_routes[client].put_nowait(msg)
    
    def del_client_route(self, client: str):
        """Remove a client route from the server."""
        if client in self._out_routes:
            del self._out_routes[client]

    async def handle_client(self, client_reader, client_writer):
        """Handle client connection and bidirectional data transfer."""
        client = hashlib.sha1(f"client:{id(client_writer)}".encode()).hexdigest()
        await traffic_stats_inc(f"new_connection_{self._target}")
        try:
            c_host = client_writer.get_extra_info("peername")
            logger.info(f"Client connected from {c_host} id: {client}")

            if client in self._out_routes:
                queue = self._out_routes[client]
            else:
                self._out_routes[client] = queue = asyncio.Queue()

            # Create tasks to handle bidirectional data transfer
            tasks = [
                asyncio.create_task(self.send_to_client(queue, client_writer)),
                asyncio.create_task(self.send_to_server(client, client_reader)),
            ]
            # Wait for both tasks to complete
            _, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            # Cancel remaining tasks
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        except Exception as err:
            logger.error(f"Error handling client: {err}")
            await traffic_stats_inc(f"error_connection_{self._target}")
        finally:
            await traffic_stats_inc(f"closed_connection_{self._target}")
            client_writer.close()

        try:
            await asyncio.sleep(self._iter_timeout)
        except Exception as err:
            logger.warning(f"Closing {client} error: {err}")


class TcpConnectionServer:
    """
    TCP connection server that manages proxy connections to target hosts.
    This server handles TCP connections, forwards traffic through MQTT,
    and maintains connection statistics.
    """
    
    sockets: Dict[str, SocketServer] = {}
    service_check_interval: float = 3.0
    _iter_timeout: float = 0.0012

    async def make_server(self, new_server: SocketServer):
        """
        Start a TCP server for the given SocketServer instance.
        """
        server = await asyncio.start_server(new_server.handle_client, new_server._host, new_server._port)
        logger.info(f"Server started on {new_server._host}:{new_server._port} connection to {BROKER_HOST}")
        async with server:
            new_server.server = server
            await server.serve_forever()

    async def start_server(self, executor: Optional[ProcessPoolExecutor] = None):
        """
        Start the proxy server with configured target sockets.
        """
        self._executor = executor
        self.sockets = {}
        handlers = []
        async with Client(
            hostname=BROKER_HOST,
            port=BROKER_PORT,
            username=BROKER_USER,
            password=BROKER_PASSWORD,
            protocol=ProtocolVersion.V5,
        ) as client:
            self._mqtt_client = client

            for target_socket in SOCKETS.split(";"):
                parts = target_socket.split(":")
                try:
                    target_code, t_host, t_port = parts
                    assert target_code
                    assert t_host
                    t_port = int(t_port)
                    assert 1 <= t_port <= 2 ** 16
                except Exception as err:
                    raise ValueError(
                        f"Incorrect targets in env 'SOCKETS': {parts} ({err})"
                        " format:  'target_code1:host:port;target_code2:host:port'"
                    )
                    continue

                server = SocketServer(client, executor, target_code, t_host, t_port)
                self.sockets[target_code] = server
                handlers.append(self.make_server(server))

            if handlers:
                handlers.append(self.wait_messages())
                handlers.append(self.service_process())
                await asyncio.gather(*handlers)

    async def service_process(self):
        """
        Periodically clean up connections and save traffic statistics.
        """
        wait = True
        while wait:
            await asyncio.sleep(self.service_check_interval)
            for _, server in self.sockets.items():
                await server.claen_up()  # Note: Typo in original code - should be "clean_up"

            # save satistic
            if STAT_FILE:
                try:
                    with open(STAT_FILE, "w") as json_file:
                        traffic_stats_dict = dict(traffic_stats)
                        traffic_stats_dict["updated"] = datetime.now().isoformat()
                        json.dump(traffic_stats_dict, json_file, indent=2)
                except Exception as err:
                    logger.error(f"Problem {err} ti save statistic into '{STAT_FILE}'")

    async def wait_messages(self):
        """
        Wait for and process incoming MQTT messages.
        """
        loop = asyncio.get_event_loop()

        up_topics = 0
        for target, server in self.sockets.items():
            try:
                await self._mqtt_client.subscribe(server.res_topic, qos=qos)
            except Exception as err:
                logger.error(f"Failed to subscribe to topic '{self._res_topic} in {target}': {err}")
                break
            else:
                up_topics += 1

        if up_topics < 1:
            return

        async for message in self._mqtt_client.messages:
            if message.payload:
                try:
                    msg: Message = pickle.loads(message.payload)
                except Exception as err:
                    logger.error(f"Failed to deserialize message: {err}")
                    continue

                sock_server = None
                topic = message.topic.value
                for target, server in self.sockets.items():
                    if server.res_topic == topic or server.res_topic in topic:
                        sock_server = server
                        break

                if msg.x:
                    logger.warning(f"Client {msg.client} closing connection")
                await traffic_stats_inc(f"mqtt_in_{target}", len(message.payload))
                if sock_server.has_client(msg.client):
                    if msg.x:
                        try:
                            sock_server.put_client_message(msg.client, STOP_W)
                            # self._closig[msg.client] = datetime.now()
                        except Exception as err:
                            logger.error(f"Failed to send STOP_W to client {msg.client}: {err}")
                        else:
                            logger.warning(f"Sending quit to {msg.client}")

                        continue

                    try:
                        if self._executor:
                            data = await loop.run_in_executor(self._executor, _restore_data, msg.data)
                        else:
                            data = _restore_data(msg.data)
                        msg.data = data
                    except Exception as err:
                        logger.error(f"Failed to restore data for client {msg.client}: {err}")
                        continue

                    try:
                        sock_server.put_client_message(msg.client, msg)
                    except Exception as err:
                        logger.error(f"Failed to put message in queue for client {msg.client}: {err}")
                        # Remove the client from routes if queue is broken
                        sock_server.del_client_route(msg.client)
            else:
                await asyncio.sleep(self._iter_timeout)

        logger.warning("Closed topics")


async def main():
    """
    Main entry point for the proxy server.
    Starts the TCP connection server with optional process pool execution.
    """
    server = TcpConnectionServer()
    if WORKERS > 0:
        with ProcessPoolExecutor(max_workers=WORKERS) as executor:
            await server.start_server(executor)
    else:
        await server.start_server()


if __name__ == "__main__":
    asyncio.run(main())
