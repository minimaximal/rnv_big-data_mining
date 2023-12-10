import requests
import json
import datetime

url = "https://graphql-sandbox-dds.rnv-online.de/"



f = open('/data/rnv_big-data_mining/data/general/auth.json')
auth = json.load(f)
f.close()

headers = {
  'Authorization': auth['token_type'] + ' ' + auth['access_token'],
  'Content-Type': 'application/json'
}

# Get the current timestamp
now = datetime.datetime.now()

cursor = "0"
while cursor != "999-X" and cursor != "null":
   payload = "{\"query\":\"{lines (first: 50 after:\\\"" + cursor + "\\\")\\n    {totalCount elements  {\\n        ... on Line\\n        {\\n            id  \\n\\n            journeys { elements {\\n              ... on Journey {\\n                          canceled\\n    \\n          stops    {\\n            \\n            destinationLabel\\n\\n            station {\\n                id\\n            }\\n\\n            plannedDeparture {\\n              isoString\\n            }\\n            \\n            realtimeDeparture {\\n              isoString\\n                }\\n              }\\n            }\\n          }\\n        }\\n      }\\n    }\\n  cursor\\n  }\\n}\",\"variables\":{}}"
   response = requests.request("POST", url, headers=headers, data=payload)
   filename = 'line_monitoring_all_{}_'.format(now.strftime('%Y%m%d%H%M%S')) + cursor + '.json'
   
   # print(response.text)
   with open('/data/rnv_big-data_mining/data/line_monitoring/mined/' + filename, 'w') as f:
      f.write(response.text)

   with open('/data/rnv_big-data_mining/data/line_monitoring/to_be_imported/' + filename, 'w') as f:
      f.write(response.text)

   cursor = json.loads(response.text)['data']['lines']['cursor']