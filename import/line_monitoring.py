import json
import pymysql
from datetime import datetime, timezone

# JSON-Datei laden
f = open('/data/rnv_big-data_mining/data/general/line_monitoring_all_20231126172001_0.json')
data  = json.load(f)
f.close()



# Verbindung zur Datenbank herstellen
db = pymysql.connect(host="localhost", user="rnv_importer", password="rnv_importer", database="rnv_big_data_mining")

# Cursor erstellen
cursor = db.cursor()





# Durch jede Linie in den Daten gehen
for line in data['data']['lines']['elements']:
    
    if len(line['journeys']['elements']) == 0:
      cursor.execute("SELECT id FROM `lines` WHERE api_id = %s", (line['id']))
      result = cursor.fetchone()

      if result is None:
        # Wenn die Linie nicht existiert, erstellen Sie eine neue
        cursor.execute("INSERT INTO `lines` (api_id) VALUES (%s)", (line['id']))
    
    else:
         # Durch jede Reise in der Linie gehen
         for journey in line['journeys']['elements']:
            # Überprüfen, ob die Linie bereits existiert
            cursor.execute("SELECT id FROM `lines` WHERE (api_id = %s) AND (api_destinationLabel = %s)", (line['id'], journey['stops'][0]['destinationLabel']))
            result = cursor.fetchone()

            if result is None:
               # Wenn die Linie nicht existiert, erstellen Sie eine neue
               cursor.execute("INSERT INTO `lines` (api_id, api_destinationLabel) VALUES (%s, %s)", (line['id'], journey['stops'][0]['destinationLabel']))
               line_id = cursor.lastrowid
            else:
               # Wenn die Linie existiert, speichern Sie die ID
               line_id = result[0]
        
      
            # Erstellen Sie eine neue Reise
            cursor.execute("INSERT INTO journeys (api_line, canceled) VALUES (%s, %s)", (line_id, journey['canceled']))
            journey_id = cursor.lastrowid

            # Durch jeden Stopp in der Reise gehen
            for stop in journey['stops']:
                  # Überprüfen, ob der Stopp bereits existiert
                  cursor.execute("SELECT id FROM stops WHERE (api_line = %s) AND (api_station = %s) AND (api_journey = %s)", (line_id, stop['station']['id'], journey_id))
                  result = cursor.fetchone()

                  pd = stop['plannedDeparture']['isoString'] 
                  rd = stop['realtimeDeparture']['isoString']

                  if pd is None:
                     pd = None
                  else: pd = int(datetime.fromisoformat(stop['plannedDeparture']['isoString'].replace("Z", "+00:00")).replace(tzinfo=timezone.utc).timestamp())
                  
                  if rd is None:
                     rd = None
                  else: rd = int(datetime.fromisoformat(stop['realtimeDeparture']['isoString'].replace("Z", "+00:00")).replace(tzinfo=timezone.utc).timestamp())

                  if result is None:
                     # Wenn der Stopp nicht existiert, erstellen Sie einen neuen
                     cursor.execute("INSERT INTO stops (api_line, api_station, api_journey, api_plannedDeparture, api_realtimeDeparture) VALUES (%s, %s, %s, FROM_UNIXTIME(%s), FROM_UNIXTIME(%s))", (line_id, stop['station']['id'], journey_id, pd, rd))
                  else:
                     # Wenn der Stopp existiert, aktualisieren Sie die Echtzeit-Abfahrt
                     cursor.execute("UPDATE stops SET api_realtimeDeparture = FROM_UNIXTIME(%s) WHERE id = %s", (rd, result[0]))

# Commit nach jeder Datei
db.commit()

# Schließen Sie die Verbindung zur Datenbank
db.close()