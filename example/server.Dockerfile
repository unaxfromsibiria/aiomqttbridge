FROM python:3.14-trixie

RUN apt update
RUN apt install -y openssl build-essential libssl-dev curl iperf

COPY aiomsgbridge /opt/aiomsgbridge
COPY requirements.txt /opt/aiomsgbridge/requirements.txt

WORKDIR /opt/
RUN python -m pip install -U pip && python -m pip install -r aiomsgbridge/requirements.txt

RUN echo "python -m aiomsgbridge.msg_ts & iperf -s 0.0.0.0 -p 9091 & iperf -u -s 0.0.0.0 -p 9092" > /opt/run.sh && chmod 770 /opt/run.sh

RUN apt clean && rm -rf /var/lib/apt/lists/* && python -m pip cache purge

CMD ["bash", "-c", "/opt/run.sh"]
