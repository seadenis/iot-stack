#!/usr/bin/python3
import argparse
import math

import paho.mqtt.client

import time
import random
import sys

from influxdb import InfluxDBClient
from threading import Thread
from collections import deque

import logging
logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)

class DBWriterThread(Thread):
    def __init__(self, influx_client, *args, **kwargs):
        self.influx_client = influx_client
        self.data_queue = deque()

        super(DBWriterThread, self).__init__(*args, **kwargs)

    def schedule_item(self, client, device_id, control_id, value):
        item = (client, device_id, control_id, value)
        self.data_queue.append(item)

    def get_items(self, mininterval, maxitems):
        """ This will collect items from queue until either 'mininterval'
        is over or 'maxitems' items are collected """
        started = time.time()
        items = []

        while (time.time() - started < mininterval) and (len(items) < maxitems):
            try:
                item = self.data_queue.popleft()
            except IndexError:
                time.sleep(mininterval * 0.1)
            else:
                items.append(item)

        return items

    def run(self):
        while True:
            items = self.get_items(mininterval=0.05, maxitems=200)
            db_req_body = []
            stat_clients = set()
            for client, device_id, control_id, value in items:
                ser_item = self.serialize_data_item(client, device_id, control_id, value)
                if ser_item:
                    db_req_body.append(ser_item)
                    stat_clients.add(client)

            if db_req_body:
                logging.info("Write %d items for %d clients" % (len(items), len(stat_clients)))
                time.sleep(10)
                try:
                    self.influx_client.write_points(db_req_body)
                except:
                    logging.info ("Exception during writing points")

#        time.sleep(0.01)

    def serialize_data_item(self, client, device_id, control_id, value):
        value = value.replace('\n', ' ')
        if not value:
            return


        fields = {}
        try:
            value_f = float(value)
            if not math.isnan(value_f):
                fields["value_f"] = value_f
        except ValueError:
            pass
        if "value_f" not in fields:
            fields["value_s"] = value

        item = {
            'measurement': 'mqtt_data',
            'tags' : {
                'client' : client,
                "channel" : '%s/%s' % (device_id, control_id),
            },
            "fields" : fields
        }

        return item

#db_writer = None
#db_writer_1 = None
db_writer_94 = None

def on_mqtt_message(arg0, arg1, arg2=None):
    if arg2 is None:
        msg = arg1
    else:
        msg = arg2

    if msg.retain:
        return

    if (msg.topic.find('bridge') != -1):
        return
    if (msg.topic.find('homeassistant_1') != -1):
        logging.info ("Zigbee: topic="+msg.topic+", value="+msg.payload.decode('utf8'))
#        logging.info (msg.topic[msg.topic.find('/')+1:])
        device1=msg.topic[msg.topic.find('/')+1:]
#        logging.info (msg.payload.decode('utf8'))
        msg1=msg.payload.decode('utf8')[1:-1]
#        logging.info (msg1)
        parts1=msg1.split(',')
        for part1 in parts1:
#            logging.info (part1)
#            logging.info (part1[1:part1.find(':')-1])
            param1=part1[1:part1.find(':')-1]
#            logging.info (part1[part1.find(':')+1:])
            value1=part1[part1.find(':')+1:]
#            try:
#                db_writer.schedule_item('zigbee', device1, param1, value1)
#                logging.info ('zigbee device: '+device1+', param: '+param1+', value: '+value1+' has been successfully written to db')
#            except:
#                logging.info ("Exception during schedule zigbee item 75")
#            try:
#                db_writer_1.schedule_item('zigbee', device1, param1, value1)
#                logging.info ('zigbee device: '+device1+', param: '+param1+', value: '+value1+' has been successfully written to db1')
#            except:
#                logging.info ("Exception during schedule zigbee item 42")
            try:
                db_writer_94.schedule_item('zigbee', device1, param1, value1)
#                logging.info ('zigbee device: '+device1+', param: '+param1+', value: '+value1+' has been successfully written to db94')
            except:
                logging.info ("Exception during schedule zigbee item 94")
        return

    parts = msg.topic.split('/')
    client = None
    if len(parts) < 4:
        return

    if (parts[1] == 'client'):
        client = parts[2]
        parts = parts[3:]

    if len(parts) != 4:
        return

    device_id = parts[1]
    control_id = parts[3]
    try:
        value = msg.payload.decode('utf8')
    except:
        value = "Error during decoding"
    logging.info ("WB: topic="+msg.topic+", value="+value)

#    logging.info (client)
#    logging.info (device_id)
#    logging.info (control_id)
#    logging.info (value)

    if (device_id == 'wb-adc'):
        return

#    db_writer.schedule_item(client, device_id, control_id, value)
#    db_writer_1.schedule_item(client, device_id, control_id, value)
    db_writer_94.schedule_item(client, device_id, control_id, value)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MQTT retained message deleter', add_help=False)

    parser.add_argument('-h', '--host', dest='host', type=str,
                        help='MQTT host', default='192.168.1.105')

    parser.add_argument('-u', '--username', dest='username', type=str,
                        help='MQTT username', default='mosquitto')

    parser.add_argument('-P', '--password', dest='password', type=str,
                        help='MQTT password', default='password')

    parser.add_argument('-p', '--port', dest='port', type=int,
                        help='MQTT port', default='1883')

    mqtt_device_id = str(time.time()) + str(random.randint(0, 100000))

#    parser.add_argument('topic',  type=str,
#                        help='Topic mask to unpublish retained messages from. For example: "/devices/my-device/#"', default='#')

    args = parser.parse_args()

    client = paho.mqtt.client.Client(client_id=None, clean_session=True, protocol=paho.mqtt.client.MQTTv31)

    if args.username:
        client.username_pw_set(args.username, args.password)

    client.connect(args.host, args.port)
    client.on_message = on_mqtt_message

#    client.subscribe(args.topic)
    client.subscribe("#")

#    influx_client = InfluxDBClient('192.168.1.75', 8086, database='mqtt_data')
#    influx_client_1 = InfluxDBClient('192.168.1.42', 8086, database='mqtt_data')
    influx_client_94 = InfluxDBClient('192.168.1.94', 8086, database='mqtt_data')
#    db_writer =  DBWriterThread(influx_client, daemon=True)
#    db_writer_1 =  DBWriterThread(influx_client_1, daemon=True)
    db_writer_94 =  DBWriterThread(influx_client_94, daemon=True)
#    db_writer.start()
#    db_writer_1.start()
    db_writer_94.start()


    while 1:
        rc = client.loop()
        if rc != 0:
            break
