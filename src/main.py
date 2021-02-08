import ssl
import os
import time

import paho.mqtt.client as mqtt
from loguru import logger as log
from pony import orm

MQTT_BROKER = os.environ.get('MQTT_BROKER')
MQTT_PORT = os.environ.get('MQTT_PORT') or 8883
MQTT_USER = os.environ.get('MQTT_USER')
MQTT_PASS = os.environ.get('MQTT_PASS')
DB_FILE = '/data/db.sqlite'

if not(MQTT_BROKER and MQTT_USER and MQTT_PASS):
    exit("No MQTT credentials given!")

db = orm.Database()

class Entry(db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    timestamp = orm.Required(int, index=True)
    key = orm.Required(str, index=True)
    value = orm.Required(float)

db.bind(provider='sqlite', filename=DB_FILE, create_db=True)
db.generate_mapping(create_tables=True)

@orm.db_session
def on_mqtt_message(client, userdata, msg):
    log.info(f'Received: {msg.topic} -> {msg.payload}')
    e = Entry(
        timestamp=int(time.time()),
        key=msg.topic,
        value=msg.payload.decode()
    )
    db.commit()

def on_mqtt_connect(client, userdata, flags, rc):
    log.debug('Subscribing to home/#')
    client.subscribe('home/#')

def main():
    client = mqtt.Client()
    client.on_connect = on_mqtt_connect
    client.on_message = on_mqtt_message
    client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS)
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    log.debug(f'Connecting to {MQTT_BROKER}:{MQTT_USER}')
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever(timeout=1.0)


if __name__ == '__main__':
    main()
