import json
import pymysql

# load the JSON data
f = open('/data/rnv_big-data_mining/data/general/stations_all_2023-12-10_20-43-46.json')
data  = json.load(f)
f.close()


# create a database connection
db = pymysql.connect(host="localhost", user="rnv_importer", password="rnv_importer", database="rnv_big_data_mining")

# create a db cursor
cursor = db.cursor()

# iterate over the stations and insert them into the database
for station in data['data']['stations']['elements']:
    cursor.execute("""
        INSERT INTO stations 
        (api_id, api_hafasID, api_globalID, api_name, api_longName, api_place, api_location_lat, api_location_long, api_location_hash, api_hasVRNStops) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        station['id'], 
        station['hafasID'], 
        station['globalID'], 
        station['name'], 
        station['longName'], 
        station['place'], 
        station['location']['lat'], 
        station['location']['long'], 
        station['location']['hash'], 
        station['hasVRNStops']
    ))

# commit the changes
db.commit()

# close the db connection
db.close()


# example station for json structure reference
# this station is not outputted in the station_all mining but appears in the line_monitoring mining
# manuell direct query of the hafasID to get the full station data

# "station": {
#             "hafasID": "8147",
#             "longName": "Waldorfschule Mannheim",
#             "shortName": "NRWA",
#             "location": {
#                 "lat": 49.4493147,
#                 "long": 8.4730328,
#                 "hash": "u0y12ccz5urh"
#             },
#             "hasVRNStops": false,
#             "name": "Waldorfschule MA"
#         }