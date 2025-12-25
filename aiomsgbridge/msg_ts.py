import asyncio
import hashlib
import json
import logging
import pickle
import uuid
from asyncio.transports import DatagramTransport
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from aiomqtt import Client
from aiomqtt import ProtocolVersion
from cachetools import TTLCache

from .common import BROKER_HOST
from .common import BROKER_PASSWORD
from .common import BROKER_PORT
from .common import BROKER_USER
from .common import BUFFER_SIZE
from .common import CONNECTION_IDLE_LIMIT
from .common import LOG_LEVEL
from .common import STAT_FILE
from .common import STOP_W
from .common import WORKERS
from .common import Chunk
from .common import Message
from .common import make_convert_data
from .common import make_restore_data
from .common import qos
from .common import read_env_float
from .common import read_env_int
from .common import read_env_list
from .common import read_env_str
from .common import sort_message

SERVER_TCP_TARGET = read_env_list("SERVER_TCP_TARGET")
SERVER_UDP_TARGET = read_env_list("SERVER_UDP_TARGET")

CLIENTS = read_env_list("CLIENTS", default=["client"])
RECONNECT_TIMEOUT = read_env_float("RECONNECT_TIMEOUT", 5)
RECONNECT_ATTEMPT = read_env_int("RECONNECT_ATTEMPT", 30)

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


class UdpProxyProtocol(asyncio.DatagramProtocol):
    """Protocol for handling UDP proxy connections"""

    transport: Optional[DatagramTransport] = None
    server: Optional["OutConnectionServer"] = None
    _queue: Optional[asyncio.Queue] = None

    def connection_made(self, transport: DatagramTransport):
        self.transport = transport

    def __init__(self, target: str, connection_id: str, server: Any, client_queue: asyncio.Queue):
        self.target = target
        self.server = server
        self.index = 0
        self.connection_id = connection_id
        self._queue = client_queue

    async def send_to_client(self, data: bytes, addr: str):
        """
        Send data to a connected client via MQTT topic.
        This method serializes the data into a Message object and publishes it 
        to an MQTT topic specific to the target and connection ID. It handles 
        data conversion and includes error handling for both serialization and 
        MQTT publishing operations.
        """
        loop = asyncio.get_event_loop()
        self.index += 1
        server = self.server
        executor = self.server._executor
        topic = self.server._out_topic_tpl.format(self.target)
        logger.info(f"Data for topic '{topic}' to {len(data)} bytes client {self.connection_id}")
        try:
            if executor:
                c_data = await loop.run_in_executor(executor, make_convert_data, data)
            else:
                c_data = make_convert_data(data)

            msg_content = pickle.dumps(
                Message(self.index, self.connection_id, c_data, uuid.uuid4().bytes, 0)
            )
            await traffic_stats_inc("udp_in", len(data))
        except Exception as err:
            logger.error(f"Error in handle read connection {addr}: {err}")
            msg_content = pickle.dumps(Message(self.index, self.connection_id, b"", b"", 1))

        try:
            await server._mqtt_client.publish(topic, payload=msg_content, qos=qos)
        except Exception as err:
            logger.error(f"Error to send in {topic} from connection {self.connection_id} {addr}: {err}")
        else:
            await traffic_stats_inc("mqtt_out", len(msg_content))
            server._latest_message[(self.target, self.connection_id)] = datetime.now()

    def datagram_received(self, data: bytes, addr_info: tuple[str, int]):
        """Handle incoming UDP datagrams"""
        addr, _ = addr_info
        logger.info(f"Data {len(data)} from {addr}")
        asyncio.create_task(self.send_to_client(data, addr))

    def error_received(self, exc):
        logger.warning(f"Error for closing {exc} for {self.connection_id}")

    def connection_lost(self, exc):
        logger.info(f"Connection {self.connection_id} lost")
        if self._queue:
            try:
                self._queue.put_nowait(STOP_W)
            except asyncio.QueueFull:
                ...


class OutConnectionServer:
    """
    TCP proxy server that forwards data between TCP targets and MQTT topics.
    This server manages connections to multiple TCP targets and routes data
    through MQTT topics. It handles connection establishment, data forwarding,
    and cleanup of idle connections.
    """

    _out_routes: Dict[tuple, asyncio.Queue] = {}
    _wait_iteration_time: float = 0.08
    _iter_timeout: float = 0.15
    _mqtt_client = None
    _topics: List[str]
    _out_topic_tpl: str
    _executor: ProcessPoolExecutor = None
    _latest_message: Dict[tuple, datetime] = {}
    tcp_targets: Dict[str, tuple] = {}
    udp_targets: Dict[str, tuple] = {}
    service_check_interval: float = 3.0
    connections: Dict[tuple, Any] = {}
    short_buffer_size: int = 64

    def __init__(self):
        """Initialize the proxy server with target configurations."""
        self.server = None
        self._out_routes = {}
        self._latest_message = {}
        self.tcp_targets = {}
        self.udp_targets = {}
        self.connections = {}
        self._topics = []
        if not (SERVER_TCP_TARGET or SERVER_UDP_TARGET):
            raise ValueError("SERVER_TCP_TARGET or/and SERVER_UDP_TARGET environment variable is required")

        self._parse_targets()
        self._out_topic_tpl = "data/{}"
        for target_code in self.tcp_targets:
            self._topics.append(f"func/{target_code}")
        for target_code in self.udp_targets:
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
                    c_data = await loop.run_in_executor(self._executor, make_convert_data, data)
                else:
                    c_data = make_convert_data(data)

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
                    await traffic_stats_inc("tcp_out", len(msg.data))
                except Exception as err:
                    logger.error(f"Error in handle read connection: {err}")
                    quit_cmd = True
            else:
                await asyncio.sleep(self._iter_timeout)

    def _parse_targets(self):
        """
        Parse SERVER_TCP_TARGET/SERVER_UDP_TARGET environment variable into target configurations.
        Converts target strings into structured configurations with unique codes
        and validates port numbers and host addresses.
        """
        tcp_targets = {}
        udp_targets = {}
        service_names = set()
        sockets = set()
        for target_socket in SERVER_TCP_TARGET:
            parts = target_socket.split(":")
            try:
                target_code, t_host, t_port = parts
                assert target_code
                assert t_host
                t_port = int(t_port)
                assert 1 <= t_port <= 2 ** 16
                t_name = target_code
                service_names.add(t_name)
                target_codes = [
                    hashlib.sha1(target_code.encode() + client.encode()).hexdigest()[:8]
                    for client in CLIENTS
                ]
            except Exception as err:
                raise ValueError(
                    f"Incorrect targets in env 'SERVER_TCP_TARGET': {parts} ({err})"
                    " format: 'target_code1:host:port;target_code2:host:port'"
                )
            else:
                if (t_host, t_port, "tcp") in sockets:
                    raise ValueError(
                        f"Duplicate socket detected: {t_host}:{t_port}. "
                        "Each target must have a unique socket."
                    )
                sockets.add((t_host, t_port, "tcp"))
                for target_code in target_codes:
                    tcp_targets[target_code] = (t_name, t_host, t_port)
                    logger.info(f"Target '{t_name}' in {t_host}:{t_port} with code: {target_code}")

        for target_socket in SERVER_UDP_TARGET:
            parts = target_socket.split(":")
            try:
                target_code, t_host, t_port = parts
                assert target_code
                assert t_host
                t_port = int(t_port)
                assert 1 <= t_port <= 2 ** 16
                t_name = target_code
                target_codes = [
                    hashlib.sha1(target_code.encode() + client.encode()).hexdigest()[:8]
                    for client in CLIENTS
                ]
            except Exception as err:
                raise ValueError(
                    f"Incorrect targets in env 'SERVER_UDP_TARGET': {parts} ({err})"
                    " format: 'target_code1:host:port;target_code2:host:port'"
                )
            else:
                logger.info(f"Target UDP service '{target_code}' {t_host}:{t_port}")
                if t_name in service_names:
                    raise ValueError(
                        f"UDP service code '{t_name}' uses in TCP targets"
                    )
                if (t_host, t_port, "udp") in sockets:
                    raise ValueError(
                        f"Duplicate socket detected: {t_host}:{t_port}. "
                        "Each target must have a unique socket."
                    )
                sockets.add((t_host, t_port, "udp"))
                for target_code in target_codes:
                    udp_targets[target_code] = (t_name, t_host, t_port)
                    logger.info(f"Target '{t_name}' in {t_host}:{t_port} with code: {target_code}")

        self.tcp_targets = tcp_targets
        self.udp_targets = udp_targets

    async def create_target_udp_connection(self, target: str, connection_id: str, queue: asyncio.Queue):
        """
        Create and manage a UDP connection to a target endpoint.
        This method establishes a UDP datagram endpoint to the specified target and 
        handles message forwarding between the queue and the UDP socket. It continuously
        monitors the queue for incoming messages and sends them through the UDP connection.
        """
        t_name, target_host, target_port = self.udp_targets[target]
        loop = asyncio.get_event_loop()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: UdpProxyProtocol(target, connection_id, self, queue),
            remote_addr=(target_host, target_port)
        )
        quit_cmd = False
        iter_time = self._wait_iteration_time
        while not quit_cmd:
            try:
                msg: Message = await asyncio.wait_for(queue.get(), timeout=iter_time)
            except asyncio.TimeoutError:
                msg = None

            if msg:
                if isinstance(msg, str) and msg == STOP_W:
                    quit_cmd = True
                    continue

                self._latest_message[msg.client] = datetime.now()
                try:
                    transport.sendto(msg.data)
                    await traffic_stats_inc("udp_out", len(msg.data))
                except Exception as err:
                    logger.error(f"Error in handle read connection: {err}")
                    quit_cmd = True
                else:
                    logger.debug(f"Sending {len(msg.data)} bytes to {target_host}:{target_port}")
            else:
                await asyncio.sleep(self._iter_timeout)

        if transport:
            try:
                logger.info(f"Closing udp {target} {connection_id}")
                transport.close()
            except Exception as err:
                logger.error(f"UDP {target_host}:{target_port}:{t_name} close error {err}")

    async def create_target_tcp_connection(self, target: str, connection_id: str, queue: asyncio.Queue):
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
            target = proto = None
            for code in self.tcp_targets:
                if code in message.topic.value:
                    proto = "tcp"
                    target = code
                    break

            if not proto:
                for code in self.udp_targets:
                    if code in message.topic.value:
                        proto = "udp"
                        target = code
                        break

            if message.payload:
                try:
                    chunk: Chunk = pickle.loads(message.payload)
                except Exception as err:
                    logger.error(f"Failed to deserialize message: {err}")
                    continue
                else:
                    await traffic_stats_inc("mqtt_in", len(message.payload))

                chunk.m.sort(key=sort_message)
                for msg in chunk.m:
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
                        queue = self._out_routes[route_key] = asyncio.Queue(maxsize=self.short_buffer_size)
                        task = loop.create_task(
                            (
                                self.create_target_tcp_connection(target, msg.client, queue)
                            ) if proto == "tcp" else (
                                self.create_target_udp_connection(target, msg.client, queue)
                            )
                        )
                        self.connections[route_key] = task
                    else:
                        queue = self._out_routes[route_key]

                    try:
                        if self._executor:
                            data = await loop.run_in_executor(self._executor, make_restore_data, msg.data)
                        else:
                            data = make_restore_data(msg.data)

                        msg.data = data
                        queue.put_nowait(msg)
                    except asyncio.QueueFull:
                        logger.warning(f"Failed to process connection for {msg.client} in topic {target}")
                        break
                    except Exception as err:
                        logger.error(f"Failed to restore data for client {msg.client} in topic {target}: {err}")
                        continue
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
