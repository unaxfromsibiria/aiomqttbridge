import asyncio
import hashlib
import json
import logging
import os
import pickle
import uuid
import zlib
from asyncio.transports import DatagramTransport
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


LOG_LEVEL = read_env_str("LOG_LEVEL") or "info"
BROKER_HOST = read_env_str("BROKER_HOST", "localhost")
BROKER_USER = read_env_str("BROKER_USER")
BROKER_PASSWORD = read_env_str("BROKER_PASSWORD")
BROKER_PORT = read_env_int("BROKER_PORT", 1883)
STOP_W = "quit"
CLIENT = read_env_str("CLIENT", "client")
TCP_SOCKETS = read_env_list("TCP_SOCKETS")
UDP_SOCKETS = read_env_list("UDP_SOCKETS")
# eg TCP_SOCKETS=ssh:localhost:22;web:0.0.0.0:80;postgres:127.0.0.1:5432
# UDP_SOCKETS=dns:localhost:53;
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


class BaseSocketServer:
    """
    Base class for socket server implementations.
    Provides common functionality for TCP and UDP socket servers including
    message routing, connection management, and MQTT communication.
    """

    _out_routes: Dict[str, asyncio.Queue] = {}
    _wait_iteration_time: float = 0.08
    _iter_timeout: float = 0.1
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
        self._out_routes = {}
        self._latest_message = {}
        self._target = target
        self._mqtt_client = mqtt_client
        self._executor = executor
        self._host = host
        self._port = port
        t_part = hashlib.sha1(target.encode() + CLIENT.encode()).hexdigest()[:8]
        self._topic = f"func/{t_part}"
        self.res_topic = f"data/{t_part}"

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


class UdpSocketServer(BaseSocketServer):
    """
    UDP socket server implementation for proxy functionality.    
    Handles UDP connections and forwards data between clients and target services
    through MQTT broker.
    """

    index: int = 0
    loop: Optional[asyncio.AbstractEventLoop] = None

    def add_client(self, client: str, addr: tuple[str, int], transport: DatagramTransport):
        """Add a new UDP client to the server."""
        if self.loop is None:
            self.loop = asyncio.get_event_loop()
        if client in self._out_routes:
            return

        self._out_routes[client] = queue = asyncio.Queue()
        self.loop.create_task(self.send_to_client(client, queue, addr, transport))

    async def send_to_client(self, client: str, queue: asyncio.Queue, addr: tuple[str, int], transport: DatagramTransport):
        """Send data from server to UDP client."""
        await traffic_stats_inc(f"new_connection_{self._target}")
        logger.info(f"UDP client from {addr} id: {client} transport: {id(transport)}")
        quit_cmd = False
        iter_time = self._wait_iteration_time
        addr_info = ":".join(map(str, addr))
        while not quit_cmd:
            try:
                msg: Message = await asyncio.wait_for(queue.get(), timeout=iter_time)
            except asyncio.TimeoutError:
                msg = None

            if msg:
                if isinstance(msg, str) and msg == STOP_W:
                    quit_cmd = True
                    logger.info(f"Closed {addr_info} from client {client}")
                    continue

                self._latest_message[msg.client] = datetime.now()
                try:
                    transport.sendto(msg.data, addr)
                    await traffic_stats_inc(f"udp_out_{self._target}", len(msg.data))
                except Exception as err:
                    logger.error(f"Error in handle read connection to ({addr_info} for {client}): {err}")
                    quit_cmd = True
            else:
                await asyncio.sleep(self._iter_timeout)

    def add_data(self, client: str, data: bytes, client_addr: str):
        """Add data from a client to be sent to the target server."""
        self.index += 1
        self.loop.create_task(self.send_to_server(client, data, client_addr))

    async def send_to_server(self, client: str, data: bytes, client_addr: str):
        """Handle data received from client and forward to target"""
        try:
            await traffic_stats_inc(f"udp_in_{self._target}", len(data))
            if self._executor:
                c_data = await self.loop.run_in_executor(self._executor, _convert_data, data)
            else:
                c_data = _convert_data(data)

            index = self.index
            self._latest_message[client] = datetime.now()
            msg_content = pickle.dumps(Message(index, client, c_data, uuid.uuid4().bytes, 0))
            try:
                await self._mqtt_client.publish(self._topic, payload=msg_content, qos=qos)
            except Exception as err:
                logger.error(f"Client {client} lost data in {index}: {err}")
            else:
                await traffic_stats_inc(f"mqtt_out_{self._target}", len(msg_content))
                logger.debug(f"Topic '{self._topic}' got {len(msg_content)} bytes")
        except Exception as err:
            logger.error(f"Error forwarding data from {client_addr}: {err}")


class TcpSocketServer(BaseSocketServer):
    """
    TCP socket server implementation for proxy functionality.
    Handles TCP connections and forwards data between clients and target services
    through MQTT broker with bidirectional communication.
    """

    server = None

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
        quit_cmd = False
        iter_time = self._wait_iteration_time
        while not quit_cmd:
            try:
                msg = await asyncio.wait_for(queue.get(), timeout=iter_time)
            except asyncio.TimeoutError:
                msg = None

            if msg:
                if isinstance(msg, str) and msg == STOP_W:
                    quit_cmd = True
                    continue

                self._latest_message[msg.client] = datetime.now()
                try:
                    writer.write(msg.data)
                    await writer.drain()
                    await traffic_stats_inc(f"tcp_out_{self._target}", len(msg.data))
                except Exception as err:
                    logger.error(f"Error in handle read connection: {err}")
                    quit_cmd = True
            else:
                await asyncio.sleep(self._iter_timeout)

    async def handle_client(self, client_reader, client_writer):
        """Handle client connection and bidirectional data transfer."""
        client = hashlib.sha1(f"client:{id(client_writer)}".encode()).hexdigest()
        await traffic_stats_inc(f"new_connection_{self._target}")
        try:
            c_host = client_writer.get_extra_info("peername")
            logger.info(f"Client tcp-connected from {c_host} id: {client}")

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


class UdpProxyProtocol(asyncio.DatagramProtocol):
    """Protocol for handling UDP proxy connections"""

    transport: Optional[DatagramTransport] = None

    def connection_made(self, transport: DatagramTransport):
        self.transport = transport

    def __init__(self, server: UdpSocketServer):
        self.server = server

    def datagram_received(self, data: bytes, addr_info: tuple[str, int]):
        """Handle incoming UDP datagrams"""
        addr, _ = addr_info
        client = hashlib.sha1(f"client:{id(self.transport)}{addr_info}".encode()).hexdigest()
        self.server.add_client(client, addr_info, self.transport)
        self.server.add_data(client, data, addr)

    def error_received(self, exc):
        logger.warning(f"Error for closing {exc}")

    def connection_lost(self, exc):
        logger.info("Connection udp lost")


class LocalConnectionServer:
    """
    TCP/UDP connection server that manages proxy connections to target hosts.
    This server handles TCP/UDP connections, forwards traffic through MQTT,
    and maintains connection statistics.
    """

    sockets: Dict[str, BaseSocketServer] = {}
    service_check_interval: float = 3.0
    _iter_timeout: float = 0.0050

    async def make_udp_server(self, new_server: UdpSocketServer):
        """
        Starts a UDP server to listen for incoming connections.
        This method creates a UDP transport socket on the specified host and port,
        which forwards incoming traffic through an MQTT broker.
        The server runs in a loop until it is manually stopped.
        """
        loop = asyncio.get_event_loop()
        transport = None
        wait = True

        def new_client(*args, **kwargs) -> UdpProxyProtocol:
            return UdpProxyProtocol(new_server)

        try:
            transport, _ = await loop.create_datagram_endpoint(
                new_client,
                local_addr=(new_server._host, new_server._port)
            )
            logger.info(
                f"Server started on UDP {new_server._host}:{new_server._port} "
                f"transport: {id(transport)} connection to {BROKER_HOST}"
            )
            try:
                while wait:
                    await asyncio.sleep(self.service_check_interval)
            except KeyboardInterrupt:
                logger.info("Server UDP stopped")
                if transport:
                    transport.close()

        except Exception as err:
            wait = False
            logger.error(f"Error starting server: {err}")

    async def make_tcp_server(self, new_server: TcpSocketServer):
        """Start a TCP server for the given SocketServer instance."""
        server = await asyncio.start_server(new_server.handle_client, new_server._host, new_server._port)
        logger.info(f"Server started on TCP {new_server._host}:{new_server._port} connection to {BROKER_HOST}")
        async with server:
            new_server.server = server
            await server.serve_forever()

    async def start_server(self, executor: Optional[ProcessPoolExecutor] = None):
        """Start the proxy server with configured target sockets."""
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

            all_codes = set()
            for target_socket in TCP_SOCKETS:
                parts = target_socket.split(":")
                try:
                    target_code, t_host, t_port = parts
                    assert target_code
                    all_codes.add(target_code)
                    assert t_host
                    t_port = int(t_port)
                    assert 1 <= t_port <= 2 ** 16
                except Exception as err:
                    raise ValueError(
                        f"Incorrect targets in env 'TCP_SOCKETS': {parts} ({err})"
                        " format: 'target_code1:host:port;target_code2:host:port'"
                    )

                server = TcpSocketServer(client, executor, target_code, t_host, t_port)
                self.sockets[target_code] = server
                handlers.append(self.make_tcp_server(server))

            for target_socket in UDP_SOCKETS:
                parts = target_socket.split(":")
                try:
                    target_code, t_host, t_port = parts
                    assert target_code
                    assert t_host
                    t_port = int(t_port)
                    assert 1 <= t_port <= 2 ** 16
                except Exception as err:
                    raise ValueError(
                        f"Incorrect targets in env 'UDP_SOCKETS': {parts} ({err})"
                        " format: 'target_code1:host:port;target_code2:host:port'"
                    )

                if target_code in all_codes:
                    raise ValueError(
                        f"Use different service codes for TCP and UDP targets ({target_code} in tcp-list)"
                    )

                server = UdpSocketServer(client, executor, target_code, t_host, t_port)
                self.sockets[target_code] = server
                handlers.append(self.make_udp_server(server))

            if handlers:
                handlers.append(self.wait_messages())
                handlers.append(self.service_process())
                await asyncio.gather(*handlers)

    async def service_process(self):
        """Periodically clean up connections and save traffic statistics."""
        wait = True
        while wait:
            await asyncio.sleep(self.service_check_interval)
            for _, server in self.sockets.items():
                await server.claen_up()

            # save satistic
            if not STAT_FILE:
                continue

            try:
                with open(STAT_FILE, "w") as json_file:
                    traffic_stats_dict = dict(traffic_stats)
                    traffic_stats_dict["updated"] = datetime.now().isoformat()
                    json.dump(traffic_stats_dict, json_file, indent=2)
            except Exception as err:
                logger.error(f"Problem {err} in saving statistic into '{STAT_FILE}'")

    async def wait_messages(self):
        """Wait for and process incoming MQTT messages."""
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
    Starts the TCP/UDP connection server with optional process pool execution.
    """
    server = LocalConnectionServer()
    if WORKERS > 0:
        with ProcessPoolExecutor(max_workers=WORKERS) as executor:
            await server.start_server(executor)
    else:
        await server.start_server()


if __name__ == "__main__":
    asyncio.run(main())
