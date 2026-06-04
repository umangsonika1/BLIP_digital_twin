import json
import time
import logging
from datetime import datetime
import os
from dotenv import load_dotenv
import boto3
import paho.mqtt.client as mqtt
from botocore.exceptions import ClientError

# ====================================================
# AWS IoT MQTT CONFIG
# ====================================================

load_dotenv()

MQTT_ENDPOINT = "a1weajnxl45qa6-ats.iot.ap-south-1.amazonaws.com"
MQTT_PORT = 8883
MQTT_TOPIC = "hardwareIncoming"

ROOT_CA = "AmazonRootCA1.pem"
CERT = "391bd44f0161140fefc267db93282533291053178ec143e343e7ecbec7e9f072-certificate.pem.crt"
PRIVATE_KEY = "391bd44f0161140fefc267db93282533291053178ec143e343e7ecbec7e9f072-private.pem.key"

CLIENT_ID = "mqtt-kinesis-bridge"

# ====================================================
# KINESIS CONFIG
# ====================================================
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "ap-south-1"
STREAM_NAME = "telemetry-events"

# kinesis = boto3.client(
#     "kinesis",
#     region_name=AWS_REGION
# )
kinesis = boto3.client(
    "kinesis",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)
# ====================================================
# LOGGING
# ====================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ====================================================
# SEND TO KINESIS
# ====================================================

def publish_to_kinesis(payload):

    try:

        if isinstance(payload, bytes):
            payload = payload.decode()

        try:
            data = json.loads(payload)
        except Exception:
            data = {"raw_message": payload}

        # record = {
        #     "received_time": datetime.utcnow().isoformat(),
        #     "payload": data
        # }
        record = data

        partition_key = (
            str(data.get("pack_id"))
            if isinstance(data, dict)
            else str(time.time())
        )

        response = kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(record),
            PartitionKey=partition_key
        )

        logging.info(
            f"Published → Shard={response['ShardId']}"
        )

    except ClientError as e:
        logging.error(f"Kinesis Error: {e}")

    except Exception as e:
        logging.exception(e)


# ====================================================
# MQTT CALLBACKS
# ====================================================

def on_connect(client, userdata, flags, rc):

    if rc == 0:
        logging.info("Connected to AWS IoT")

        client.subscribe(MQTT_TOPIC)

        logging.info(
            f"Subscribed → {MQTT_TOPIC}"
        )

    else:
        logging.error(f"Connection failed: {rc}")


# def on_message(client, userdata, msg):

#     logging.info(
#         f"Message received ({msg.topic})"
#     )

#     publish_to_kinesis(msg.payload)

import json

def on_message(client, userdata, msg):

    try:
        payload = msg.payload.decode("utf-8")

        print("\n========== MQTT MESSAGE ==========")
        print(f"Topic : {msg.topic}")

        try:
            obj = json.loads(payload)

            print(
                json.dumps(
                    obj,
                    indent=2
                )
            )

        except:
            print(payload)

        print("==================================\n")

        publish_to_kinesis(payload)

    except Exception as e:
        logging.exception(e)


def on_disconnect(client, userdata, rc):

    logging.warning(
        f"Disconnected rc={rc}"
    )

    while True:
        try:
            client.reconnect()
            break

        except Exception:
            time.sleep(5)


# ====================================================
# MQTT CLIENT
# ====================================================

client = mqtt.Client(
    client_id=CLIENT_ID
)

client.tls_set(
    ca_certs=ROOT_CA,
    certfile=CERT,
    keyfile=PRIVATE_KEY
)

client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

# ====================================================
# RUN
# ====================================================

logging.info("Connecting...")

client.connect(
    MQTT_ENDPOINT,
    MQTT_PORT,
    keepalive=60
)

client.loop_forever()