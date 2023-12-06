import os
import shutil
import subprocess
import pymysql

# Verbindung zur Datenbank herstellen
db = pymysql.connect(host="localhost", user="rnv_importer", password="rnv_importer", database="rnv_big_data_mining")
cursor = db.cursor()

# Liste der Tabellen in Ihrer Datenbank
tables = ["stops", "journeys", "`lines`", "stations"]

# Jede Tabelle leeren
for table in tables:
    cursor.execute(f"DELETE FROM {table}")
db.commit()

# Schließen Sie die Verbindung zur Datenbank
db.close()


# Anderes Python-Skript ausführen
subprocess.run(["python3", "/data/rnv_big-data_mining/import/stations_all.py"])

# Ordner mit den zu löschenden/verschiebenden Dateien
folder_paths = ['/data/rnv_big-data_mining/data/line_monitoring/imported', '/data/rnv_big-data_mining/data/line_monitoring/import_failed']

# Ordner, in den die Dateien verschoben werden sollen
destination_folder = '/data/rnv_big-data_mining/data/line_monitoring/to_be_imported'

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

