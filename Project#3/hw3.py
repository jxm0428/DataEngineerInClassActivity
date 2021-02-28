#!/home/jiang6/confluent-exercise/bin/python3
import json
import re
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen

url = 'http://rbi.ddns.net/getStopEvents'
html = urlopen(url)
soup = BeautifulSoup(html,'html.parser')

print(soup.find_all('h3')[0])
tables = soup.find_all('table')
#print(table[:1])


def get_trip_id():
    tripId = soup.find_all('h3')
    id_list = []
    for everyId in tripId:
        str_tripId = str(everyId)
        clean_tripId = BeautifulSoup(str_tripId,'lxml').get_text()
        num = re.findall(r'\d+',clean_tripId)
        num = str(num)
        new_str = num[2:-2]
        id_list.append(new_str)

    return id_list
    
data = []
trip_ids = get_trip_id()
i = 0
for table in tables[:3]:
    trs = table.find_all('tr')
    trip_id = trip_ids[i] 
    i += 1
    for tr in trs[1:]:
        element = '{"trip_id":"","direction":"","route_number":"","service_key":""}'
        obj = json.loads(element)
        tds = tr.find_all('td')
        obj['trip_id'] = trip_id 
        obj['direction'] = str(tds[4])[4:-5]
        obj['route_number'] = str(tds[5])[4:-5]
        obj['service_key'] = str(tds[6])[4:-5]

        data.append(obj)

print(data)

