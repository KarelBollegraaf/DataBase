import database
import json
import paho.mqtt.client as mqtt
import settings
settings.init()
from datetime import datetime


def StartMQTT(MQTT_client):
    MQTT_client.username_pw_set(settings.MQTT_USER, settings.MQTT_PASSWORD)
    MQTT_client.connect(settings.MQTT_BROKER, settings.MQTT_PORT)
    MQTT_client.subscribe(settings.MQTT_MB_TOPIC)
    MQTT_client.on_message = on_message
    MQTT_client.loop_forever()
    return MQTT_client

def fixtime(DataIn):
    timestamp_dict = DataIn["fields"]["timestamp"]
    if timestamp_dict["year"] <= 0:
        timestamp_dict["year"] = 2000
        timestamp_dict["month"] = 1
        timestamp_dict["day"] = 1
    Datetimex = datetime(
        year=timestamp_dict["year"],
        month=timestamp_dict["month"],
        day=timestamp_dict["day"],
        hour=timestamp_dict["hour"],
        minute=timestamp_dict["minute"],
        second=timestamp_dict["second"])
    DataIn["fields"]["timestamp"] = Datetimex.strftime('%Y-%m-%d %H:%M:%S')
    return DataIn["fields"]["timestamp"]

def on_message(MQTT_client, userdata, message):
    DataIn = json.loads(message.payload.decode("utf-8"))
    fixtime(DataIn)
    fields = DataIn["fields"] 
    cycles = DataIn["cycles"]
    pressure = DataIn["pressure"]
    print(fields, "\n")
    print(cycles, "\n")
    print(pressure, "\n")
    database.store_bale_data(fields, cycles, pressure)