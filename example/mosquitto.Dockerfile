FROM eclipse-mosquitto

RUN echo "listener 1883 0.0.0.0" > /mosquitto/config/mosquitto.conf && \
    echo "persistence false" >> /mosquitto/config/mosquitto.conf && \
    echo "log_dest file /mosquitto/log/mosquitto.log" >> /mosquitto/config/mosquitto.conf && \
    echo "log_dest syslog" >> /mosquitto/config/mosquitto.conf && \
    echo "log_dest stdout" >> /mosquitto/config/mosquitto.conf && \
    echo "log_dest topic" >> /mosquitto/config/mosquitto.conf && \
    echo "log_type error" >> /mosquitto/config/mosquitto.conf && \
    echo "log_type warning" >> /mosquitto/config/mosquitto.conf && \
    echo "log_type notice" >> /mosquitto/config/mosquitto.conf && \
    echo "log_type information" >> /mosquitto/config/mosquitto.conf && \
    echo "connection_messages true" >> /mosquitto/config/mosquitto.conf && \
    echo "log_timestamp true" >> /mosquitto/config/mosquitto.conf && \
    echo "allow_anonymous false" >> /mosquitto/config/mosquitto.conf && \
    echo "password_file /mosquitto/config/host/password.txt" >> /mosquitto/config/mosquitto.conf

RUN apk add --no-cache bash
RUN mkdir -p /mosquitto/config/host/
RUN touch /mosquitto/config/host/password.txt && chmod 700 /mosquitto/config/host/password.txt
RUN echo '#!/bin/bash' >> /opt/make_user.sh && chmod 770 /opt/make_user.sh
RUN echo 'export NEW_PWD=$(tr -dc A-Za-z0-9 < /dev/urandom | head -c32) && echo "You can use this: $NEW_PWD"' >> /opt/make_user.sh
RUN echo 'mosquitto_passwd -c /mosquitto/config/host/password.txt $1' >> /opt/make_user.sh
RUN echo 'sleep 0.1 && ps ax| grep mosquitto | grep -v grep |  cut -d m -f1 | xargs kill -HUP' >> /opt/make_user.sh
