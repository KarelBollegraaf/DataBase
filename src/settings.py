import json

def init():
    with open("cfg-data/Param.JSON", "r") as f:
        global SQLALCHEMY_DATABASE_URI, MQTT_BROKER, MQTT_PORT, MQTT_USER, MQTT_PASSWORD, MQTT_MB_TOPIC
        dParam = json.load(f)
        SQLALCHEMY_DATABASE_URI = dParam["SQLALCHEMY_DATABASE_URI"]
        MQTT_BROKER = dParam["MQTT_BROKER"]
        MQTT_PORT = dParam["MQTT_PORT"]
        MQTT_USER = dParam["MQTT_USER"]
        MQTT_PASSWORD = dParam["MQTT_PASSWORD"]
        MQTT_MB_TOPIC = dParam["MQTT_MB_TOPIC"]