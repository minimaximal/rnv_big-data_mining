import json
import pymysql

# JSON-Daten laden
f = open('/data/rnv_big-data_mining/data/general/stations_all_2023-12-03_13-42-30.json')
data  = json.load(f)
f.close()


# Verbindung zur MariaDB-Datenbank herstellen
db = pymysql.connect(host="localhost", user="rnv_importer", password="rnv_importer", database="rnv_big_data_mining")

# Cursor erstellen
cursor = db.cursor()

# Über die JSON-Daten iterieren und jede Station in die Datenbank einfügen
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

# Änderungen speichern
db.commit()

# Verbindung schließen
db.close()