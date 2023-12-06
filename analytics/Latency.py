import numpy as np
import pandas as pd
import matplotlib as mp
import pymysql

db = pymysql.connect(host="localhost", user="rnv_importer", password="rnv_importer", database="rnv_big_data_mining")
cursor = db.cursor()
cursor.execute("SELECT COUNT(*) from stops where (api_plannedDeparture = api_realtimeDeparture) AND (api_plannedDeparture is not null) AND (api_realtimeDeparture is not null)")
accurate = cursor.fetchone()[0]
print(accurate)

cursor.execute("SELECT COUNT(*) from stops where (api_plannedDeparture != api_realtimeDeparture) AND (api_plannedDeparture is not null) AND (api_realtimeDeparture is not null)")
inaccurate = cursor.fetchone()[0]
print(inaccurate)


df = pd.DataFrame({'Anzahl': [accurate, inaccurate]},
                   index=['pünktlich', 'außerplanmäßig'])
plot = df.plot.pie(y='Anzahl', figsize=(5, 5))