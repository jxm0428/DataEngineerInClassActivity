#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer
import json
import ccloud_lib
import time
import psycopg2
import datetime

DBname = 'bcdatabase'
DBuser = 'bcuser'
DBpwd = 'bcdatabase'

# connect to the database


def dbconnect():
    connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
    )
    connection.autocommit = True
    return connection


def createTable(conn):

    with conn.cursor() as cursor:
        cursor.execute(f"""

    drop table if exists BreadCrumb;
    drop table if exists Trip;
    drop type if exists service_type;
    drop type if exists tripdir_type;

    create type service_type as enum ('Weekday', 'Saturday', 'Sunday');
    create type tripdir_type as enum ('Out', 'Back');

    create table Trip (
            trip_id integer,
            route_id integer,
            vehicle_id integer,
            service_key service_type,
            direction tripdir_type,
            PRIMARY KEY (trip_id)
    );

    create table BreadCrumb (
            tstamp timestamp,
            latitude float,
            longitude float,
            direction integer,
            speed float,
            trip_id integer,
            FOREIGN KEY (trip_id) REFERENCES Trip
    );

        """)


def getSQLcmndsBC(data):
    tm = time.strftime('%H:%M:%S', time.gmtime(int(data['ACT_TIME'])))
    time_str = data['OPD_DATE'] + ' ' + tm
    dt = time.strptime(time_str, '%d-%b-%y %H:%M:%S')
    tstamp = time.strftime('%Y-%m-%d %H:%M:%S', dt)

    # ttime = time.strftime('%H:%M:%S', time.gmtime(int(data['ACT_TIME'])))
    # tstamp = datetime.datetime.strptime(data['OPD_DATE'] +' '+ ttime,
    # '%d-%b-%y %H:%M:%S')

    latitude = data['GPS_LATITUDE']
    longitude = data['GPS_LONGITUDE']
    direction = data['DIRECTION']
    speed = float(data['VELOCITY']) * 2.236936
    trip_id = data['EVENT_NO_TRIP']

    ret = f"""
    '{tstamp}',
    {latitude},
    {longitude},
    {direction},
    {speed},
    {trip_id}
    """

    cmd = f"INSERT INTO BreadCrumb VALUES ({ret});"

    # print("BC: ",tstamp,latitude,longitude,direction,speed,trip_id)
    return cmd


def getSQLcmndsTrip(data):
    trip_id = data['EVENT_NO_TRIP']
    route_id = '0'
    vehicle_id = data['VEHICLE_ID']
    service_key = 'Weekday'
    direction = 'Out'

    ret = f"""
    {trip_id},
    {vehicle_id},
    {route_id},
    '{service_key}',
    '{direction}'
    """
    cmd = f"INSERT INTO Trip VALUES ({ret});"
    # print("Trip: ",trip_id,route_id,vehicle_id,service_key,direction)
    return cmd


def is_stop_data_valid(data):
    for _ in data:
        if _ == "" or _ is None:
            return False
    return True


def getSQLcmndsStop(data):
    trip_id = data['trip_id']
    route_id = data['route_number']


    if data['service_key'] == 'W':
        service_key = 'Weekday'
    elif data['service_key'] == 'S':
        service_key = 'Saturday'
    else:
        service_key = 'Sunday'

    if data['direction'] == '1':
        direction = 'Out'
    else:
        direction = 'Back'


    ret = f"""
    {trip_id},
    {route_id},
    '{service_key}',
    '{direction}'
    """

    cmd = f"UPDATE Trip SET route_id = {route_id},service_key = '{service_key}' ,direction = '{direction}' WHERE trip_id = {trip_id};"
    return cmd


def load(conn, cmd):
    with conn.cursor() as cursor:
        try:
            cursor.execute(cmd)
        except psycopg2.IntegrityError:
            conn.rollback()
        else:
            conn.commit()


def dataValidation(data):
    # Check data exist
    if data['ACT_TIME'] == "":
        return False

    if data['OPD_DATE'] == "":
        return False

    if data['GPS_LATITUDE'] == "":
        return False

    if data['GPS_LONGITUDE'] == "":
        return False

    if data['DIRECTION'] == "":
        return False

    if data['VELOCITY'] == "":
        return False

    if data['EVENT_NO_TRIP'] == "":
        return False

    if data['VEHICLE_ID'] == "":
        return False

    if data['DIRECTION'] == "":
        return False

    # Check data range
    if float(data['GPS_LATITUDE']) < 45 or float(data['GPS_LATITUDE']) > 46:
        return False

    if float(data['GPS_LONGITUDE']) > - \
            122 or float(data['GPS_LONGITUDE']) < -123:
        return False

    return True


def trip_data(data, conn):
    if dataValidation(data):
        print(data)
        cmd_trip = getSQLcmndsTrip(data)
        cmd_bc = getSQLcmndsBC(data)
        load(conn, cmd_trip)
        load(conn, cmd_bc)


def stop_event_data(data, conn):
    cmd_stop = getSQLcmndsStop(data)
    load(conn, cmd_stop)


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'python_example_group_1',
        'auto.offset.reset': 'earliest',
    })

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    total_count = 0

    conn = dbconnect()
    createTable(conn)
    trip_count = 0
    stop_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                if 'trip' in str(record_key):
                    if dataValidation(data):
                        cmd_trip = getSQLcmndsTrip(data)
                        cmd_bc = getSQLcmndsBC(data)
                        load(conn, cmd_trip)
                        load(conn, cmd_bc)
                        trip_count += 1


#                    trip_data(data,conn)
                if 'stop' in str(record_key):
                    if is_stop_data_valid(data):
                        cmd_stop = getSQLcmndsStop(data)
                        load(conn, cmd_stop)
                        stop_count += 1
                   # stop_event_data(data,conn)
                total_count += 1
                # print("Consumed record with key {} and value {}, \
          #   and updated total count to {}"
          #   .format(record_key, record_value, total_count))

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        print("Total Trip data", trip_count)
        print("Total Stop data", stop_count)
        consumer.close()
