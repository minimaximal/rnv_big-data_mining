import json
import traceback
import pymysql
import os
from datetime import datetime, timezone
import shutil


def sorted_directory_listing_with_os_listdir(directory):
    items = os.listdir(directory)
    sorted_items = sorted(items)
    return sorted_items

"""
Imports line monitoring data from JSON files into a database.

This function connects to a MySQL database, reads JSON files from a specified folder,
and imports the data into the database. It logs the import process and handles errors.

Args:
   None

Returns:
   None
"""



# connect to database
db = pymysql.connect(host="localhost", user="rnv_importer", password="rnv_importer", database="rnv_big_data_mining")

# create cursor
cursor = db.cursor()

# folder with files to be imported
folder_path_2beimported = '/data/rnv_big-data_mining/data/line_monitoring/to_be_imported'

# folder with imported files
folder_path_imported = '/data/rnv_big-data_mining/data/line_monitoring/imported'

# folder with failed files
folder_path_failed = '/data/rnv_big-data_mining/data/line_monitoring/import_failed'

now = datetime.now()

with open('/data/rnv_big-data_mining/data/line_monitoring/import.log', 'a') as f:
   f.write(f"\n \n")
   f.write(f"{format(now.strftime('%Y-%m-%d_%H-%M-%S'))}\n")
   f.write(f"filename; state; lines_total; lines_updated; lines_new; journeys_total; journeys_updated; journeys_new; stops_total; stops_updated; stops_new \n")   
   f.close()

# loop through every file in the folder
for filename in sorted_directory_listing_with_os_listdir(folder_path_2beimported):
   tempvar = ""

   # check if file is a json file
   if filename.endswith('.json'):
      # get the full path of the file
      file_path = os.path.join(folder_path_2beimported, filename)

      try:
            # open the file
         with open(file_path) as f:
            data = json.load(f)

         # debug variables
         debug_lines_total = 0
         debug_lines_updated = 0
         debug_lines_new = 0

         debug_journeys_total = 0
         debug_journeys_updated = 0
         debug_journeys_new = 0

         debug_stops_total = 0
         debug_stops_updated = 0
         debug_stops_new = 0

         # loop through every line in the dataset
         for line in data['data']['lines']['elements']:
            debug_lines_total += 1

            if line['journeys'] is not None:

               if len(line['journeys']['elements']) == 0:
                  cursor.execute("SELECT id FROM `lines` WHERE api_id = %s", (line['id']))
                  result = cursor.fetchone()

                  if result is None:
                     # if line does not exist, create a new one
                     cursor.execute("INSERT INTO `lines` (api_id) VALUES (%s)", (line['id']))
                     debug_lines_new += 1

               else:
               # loop through every journey in the line
                  for journey in line['journeys']['elements']:
                     debug_journeys_total += 1
                     # check if line exists
                     cursor.execute("SELECT id FROM `lines` WHERE (api_id = %s) AND (api_destinationLabel = %s)", (line['id'], journey['stops'][0]['destinationLabel']))
                     result = cursor.fetchone()

                     if result is None:
                        # check if line exists without destinationLabel
                        cursor.execute("SELECT id FROM `lines` WHERE api_id = %s AND api_destinationLabel IS NULL", (line['id']))
                        result = cursor.fetchone()
                        if result is not None:
                           cursor.execute("UPDATE `lines` SET api_destinationLabel = %s WHERE id = %s", (journey['stops'][0]['destinationLabel'], result[0]))
                           line_id = result[0]
                           debug_lines_updated += 1
                        else:
                           # if line does not exist, create a new one
                           cursor.execute("INSERT INTO `lines` (api_id, api_destinationLabel) VALUES (%s, %s)", (line['id'], journey['stops'][0]['destinationLabel']))
                           line_id = cursor.lastrowid
                           debug_lines_new += 1

                     else:
                        # if line exists, save the id
                        line_id = result[0]

                  # prepare fistStop_plannedDeparture
                  fistStop_plannedDeparture = int(datetime.fromisoformat(journey['stops'][0]['plannedDeparture']['isoString'].replace("Z", "+00:00")).replace(tzinfo=timezone.utc).timestamp())

                  # check if journey exists
                  cursor.execute("SELECT id FROM `journeys` WHERE (api_line = %s) AND (api_fistStop_plannedDeparture = FROM_UNIXTIME(%s))", (line_id, fistStop_plannedDeparture))
                  result = cursor.fetchone()

                  if result is not None:
                     # if journey exists, save the id
                     journey_id = result[0]
                     # check if state of journey cancelation has changed
                     cursor.execute("SELECT api_canceled FROM `journeys` WHERE id = %s", (journey_id))
                     result = cursor.fetchone()
                     if result[0] != journey['canceled']:
                        cursor.execute("UPDATE journeys SET api_canceled = %s WHERE id = %s", (journey['canceled'], journey_id))
                        debug_journeys_updated += 1
                  else:
                     # if journey does not exist, create a new one
                     cursor.execute("INSERT INTO journeys (api_line, api_fistStop_plannedDeparture, api_canceled) VALUES (%s, FROM_UNIXTIME(%s), %s)", (line_id, fistStop_plannedDeparture, journey['canceled']))
                     journey_id = cursor.lastrowid
                     debug_journeys_new += 1

                  # loop through every stop in the journey
                  for stop in journey['stops']:
                     debug_stops_total += 1

                     pd = stop['plannedDeparture']['isoString']
                     rd = stop['realtimeDeparture']['isoString']

                     # prepare plannedDeparture and realtimeDeparture
                     if pd is not None:
                        pd = int(datetime.fromisoformat(stop['plannedDeparture']['isoString'].replace("Z", "+00:00")).replace(tzinfo=timezone.utc).timestamp())

                     if rd is not None:
                        rd = int(datetime.fromisoformat(stop['realtimeDeparture']['isoString'].replace("Z", "+00:00")).replace(tzinfo=timezone.utc).timestamp())


                     # check if stop exists
                     cursor.execute("SELECT id FROM stops WHERE (api_line = %s) AND (api_station = %s) AND (api_journey = %s)", (line_id, stop['station']['id'], journey_id))
                     result = cursor.fetchone()

                     if result is not None:
                        # check if realtimeDeparture has changed
                        cursor.execute("SELECT api_realtimeDeparture FROM `stops` WHERE id = %s", (result[0]))
                        result = cursor.fetchone()
                        if result[0] != rd:
                           cursor.execute("UPDATE stops SET api_realtimeDeparture = FROM_UNIXTIME(%s) WHERE id = %s", (rd, result[0]))
                           debug_stops_updated += 1
                     else:
                        tempvar = line_id, journey_id, stop['plannedDeparture']['isoString'], stop['realtimeDeparture']['isoString'], stop['station']['id']

                        # check if station exists
                        cursor.execute("SELECT api_hafasID FROM stations WHERE api_hafasID = %s", stop['station']['id'])
                        result = cursor.fetchone()
                        if result is None:
                           # if station does not exist, create a new one   
                           station_name = "!!! SONDER-STATION : " + str(stop['destinationLabel']) + " !!!"
                           cursor.execute("INSERT INTO stations (api_id, api_hafasID, api_name, api_longName, api_location_lat, api_location_long, api_location_hash) VALUES (%s, %s, %s, %s, %s, %s, %s)", (stop['station']['id'], stop['station']['id'], station_name, station_name, 0, 0, "0"))

                        # if stop does not exist, create a new one
                        cursor.execute("INSERT INTO stops (api_line, api_station, api_journey, api_plannedDeparture, api_realtimeDeparture) VALUES (%s, %s, %s, FROM_UNIXTIME(%s), FROM_UNIXTIME(%s))", (line_id, stop['station']['id'], journey_id, pd, rd))
                        debug_stops_new += 1

         # commit changes to database
         db.commit()

         shutil.move(file_path, os.path.join(folder_path_imported, filename))

         # write debug information to log file
         with open('/data/rnv_big-data_mining/data/line_monitoring/import.log', 'a') as f:
            f.write(f"{filename}; imported; {debug_lines_total}; {debug_lines_updated}; {debug_lines_new}; {debug_journeys_total}; {debug_journeys_updated}; {debug_journeys_new}; {debug_stops_total}; {debug_stops_updated}; {debug_stops_new}\n")   

      except:
         # error handling
         shutil.move(file_path, os.path.join(folder_path_failed, filename))

         # write traceback to error file
         f = open(os.path.join(folder_path_failed, filename + ".error"), "w")
         traceback.print_exc(file=f)
         f.close()

         # rollback changes to database after error
         db.rollback()

         # write debug information to log file
         with open('/data/rnv_big-data_mining/data/line_monitoring/import.log', 'a') as f:
            f.write(f"{filename}; failed\n")   

         print(tempvar)

# close database connection
db.close()