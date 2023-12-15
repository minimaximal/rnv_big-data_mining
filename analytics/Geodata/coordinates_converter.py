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
    # read csv file
    df = pd.read_csv(csv_file)

    # convert coordinates from dms to dd
    df['Latitude'] = df['Latitude'].apply(dms_to_dd)
    df['Longitude'] = df['Longitude'].apply(dms_to_dd)

    # sort by latitude
    # df = df.sort_values(by=['Latitude'])

    # sort by longitude
    df = df.sort_values(by=['Longitude'])

    # convert to list of tuples
    coordinates = list(df[['Longitude', 'Latitude']].itertuples(index=False, name=None))

    return coordinates


# main script call
coordinates = convert_coordinates('/data/rnv_big-data_mining/analytics/Geodata/coordinates.csv')
print(coordinates)