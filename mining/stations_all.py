import requests
import json
import datetime

url = "https://graphql-sandbox-dds.rnv-online.de/"

payload = "{\"query\":\"{\\n  stations(first: 5000 lat:49.487758 long:8.466272 distance:99) { totalCount\\n    elements {\\n      ... on Station {\\n          id\\n          hafasID\\n          globalID\\n          name\\n          longName\\n          place\\n          location {\\n            lat\\n            long\\n            hash\\n         }\\n        hasVRNStops  \\n        # journeys\\n        # lines\\n        # platforms\\n        # poles \\n      }\\n    }\\n  }\\n}\",\"variables\":{}}"

f = open('/data/rnv_big-data_mining/data/general/auth.json')
auth = json.load(f)
f.close()


headers = {
  'Authorization': auth['token_type'] + ' ' + auth['access_token'],
  'Content-Type': 'application/json'
}

# get the current timestamp
now = datetime.datetime.now()

response = requests.request("POST", url, headers=headers, data=payload)

filename = 'stations_all_{}.json'.format(now.strftime('%Y-%m-%d_%H-%M-%S'))

# print(response.text)
with open('/data/rnv_big-data_mining/data/general/' + filename, 'w') as f:
    f.write(response.text)