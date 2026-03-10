import paho.mqtt.client as mqtt
import dbscheme
from sqlalchemy import create_engine
import settings
from MQTTIn import StartMQTT



settings.init()

def main():
    if not dbscheme.database_exists(settings.SQLALCHEMY_DATABASE_URI):
        print("does not exist, creating")
        dbscheme.create_database(settings.SQLALCHEMY_DATABASE_URI)

    engine = create_engine(settings.SQLALCHEMY_DATABASE_URI, echo=False)
    dbscheme.create_tables(engine)
    MQTT_client = mqtt.Client(client_id = "BalerDatabaseClient")
    StartMQTT(MQTT_client)

if __name__ == "__main__":
    main()