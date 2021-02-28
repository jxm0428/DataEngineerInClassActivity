import json
import psycopg2
from geojson import Feature, FeatureCollection, Point,dump

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

def select_bc_data(conn):
    cur = conn.cursor()
    #cur.execute("SELECT * FROM breadcrumb WHERE (latitude > 45.586158 AND longitude > -122.550711) AND (latitude < 45.592404 AND longitude < -122.541270 );")
    #cur.execute("SELECT * FROM breadcrumb where trip_id = 170579808")
    #cur.execute("select * from breadcrumb B,trip T where B.trip_id = T.trip_id and T.route_id = 65 and b.tstamp::time>'09:00:00' and b.tstamp::time < '11:00:00';")

    cur.execute("select * from breadcrumb B,trip T where B.trip_id = T.trip_id and T.route_id >80 and b.tstamp::time>'16:00:00' and b.tstamp::time < '18:00:00';")

    rows = cur.fetchall()

    return rows


if __name__ == '__main__':
    conn = dbconnect()
    bc_rows = select_bc_data(conn)
    features = []
    for row in bc_rows:

        lat = row[1]
        lon = row[2]
        speed = row[4]
        print(lat,lon,speed)

        if speed is None or speed == "":
            continue

        try:
            latitude, longitude = map(float, (lat, lon))
            point = Point((longitude,latitude))
            features.append(Feature(geometry = point,properties = {'speed': (int(speed))}))
        except ValueError:
            continue
      
    collection = FeatureCollection(features)
    with open("visualization5c.geojson","w") as f:
        #f.write('%s' % collection)
        dump(collection,f)


