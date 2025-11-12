# Forwarding TCP connections via MQTT broker

This project enables you to create a local server for forwarding TCP connections. On the server side, it establishes persistent connections to remote destinations, supporting encryption. The solution delivers good performance and reliability. A similar project was previously published by me using HTTP/3 transport [here](https://github.com/unaxfromsibiria/httpwood-totcp).

You can forward multiple TCP connections through a message broker, launch several infrastructure services without direct access, and define named enumerations for each service: `SOCKETS='redis:127.0.0.1:6379;db:127.0.0.1:5432;dev-api:127.0.0.1:8080;rabbit:127.0.0.1:5672'`

On the client side, all these sockets are accessible locally. On the server side, connections are established to the appropriate services based on the target configuration, e.g.: `SERVER_TARGET='redis:host-in-cloud-1:6379;db:host-in-cloud-2:5432;dev-api:host-in-cloud-3:8080;rabbit:host-in-cloud-4:5672'`

## Example using Docker

To set up the server side:

```bash
make example_server
cd server
# edit compose file to set env variables
docker compose up -d --build
make mqttpassword
docker compose restart tcp-server
```

and client side (local docker maybe)

```bash
make example_client
cd client
# edit settings in section local-tcp-server
docker compose up -d --build
```

### Check the example

Using example socket 1:

```bash
iperf -c 127.0.0.1 -p 9091
```

and socket 2:

```bash
curl -x http://127.0.0.1:9090 https://api.myip.com/
```

To use it outside of the example, you will need a more reliable MQTT broker configuration, but you can use this one as a basis.
