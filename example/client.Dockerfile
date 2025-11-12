FROM pypy:3.11-slim-bookworm

RUN apt update
RUN apt install -y openssl build-essential libssl-dev curl iperf

COPY aiomsgbridge /opt/aiomsgbridge
COPY requirements.txt /opt/aiomsgbridge/requirements.txt

WORKDIR /opt/
RUN pypy -m pip install -U pip && pypy -m pip install -r aiomsgbridge/requirements.txt

RUN echo "pypy -m aiomsgbridge.msg_tc" > /opt/run.sh && chmod 770 /opt/run.sh

RUN apt clean && rm -rf /var/lib/apt/lists/* && pypy -m pip cache purge

EXPOSE 9090
EXPOSE 9091

CMD ["bash", "-c", "/opt/run.sh"]
