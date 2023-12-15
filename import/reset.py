import os
import shutil
import subprocess
import pymysql

# create a database connection
db = pymysql.connect(host="localhost", user="rnv_importer", password="rnv_importer", database="rnv_big_data_mining")
cursor = db.cursor()

# list of tables to be emptied
tables = ["stops", "journeys", "`lines`", "stations"]

# clear all the listed tables
for table in tables:
    cursor.execute(f"DELETE FROM {table}")
db.commit()

# close the db connection
db.close()


# run the stations_all.py script to reimport the stations
subprocess.run(["python3", "/data/rnv_big-data_mining/import/stations_all.py"])

# list of folders to be cleared
folder_paths = ['/data/rnv_big-data_mining/data/line_monitoring/imported', '/data/rnv_big-data_mining/data/line_monitoring/import_failed']

# destination folder for the files to be moved
destination_folder = '/data/rnv_big-data_mining/data/line_monitoring/to_be_imported'

# clear / move  all the data in the listed folders
for folder_path in folder_paths:
    # Durch jede Datei im Ordner gehen
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)

        # Wenn die Datei mit .error endet, löschen Sie sie
        if filename.endswith('.error'):
            os.remove(file_path)
        else:
            # Verschieben Sie alle anderen Dateien in den Zielordner
            shutil.move(file_path, os.path.join(destination_folder, filename))

