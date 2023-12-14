import re
import pandas as pd



def dms_to_dd(dms):
    match = re.match(r"(\d+)°(\d+)'(\d+\.\d+)\"([NSEW])", dms)
    if match:
        degrees, minutes, seconds, direction = match.groups()
        dd = float(degrees) + float(minutes)/60 + float(seconds)/3600;
        if direction in ['S','W']:
            dd*= -1
        return dd
    return None


def convert_coordinates(csv_file):
    # CSV-Datei lesen
    df = pd.read_csv(csv_file)

    # Koordinaten in Dezimalgrad umwandeln
    df['Latitude'] = df['Latitude'].apply(dms_to_dd)
    df['Longitude'] = df['Longitude'].apply(dms_to_dd)

    # sort nach latitude
    # df = df.sort_values(by=['Latitude'])
    df = df.sort_values(by=['Longitude'])

    # Datenrahmen in ein Array von Tupeln umwandeln
    coordinates = list(df[['Longitude', 'Latitude']].itertuples(index=False, name=None))

    return coordinates

# Testen Sie die Funktion mit einer CSV-Datei
coordinates = convert_coordinates('/data/rnv_big-data_mining/analytics/Geodata/coordinates.csv')
print(coordinates)