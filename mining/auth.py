import requests

url = "https://login.microsoftonline.com/87cd3c4f-1e0a-4350-889e-3969cd4616c9/oauth2/token"

payload = ''
headers = {
  'Content-Type': 'application/x-www-form-urlencoded',
  'Cookie': 'fpc=Aif7Uh2bwK5JnMAIeCpEfKezsbt_AQAAAOo99dwOAAAA; stsservicecookie=estsfd; x-ms-gateway-slice=estsfd'
}

response = requests.request("POST", url, headers=headers, data=payload)

# print(response.text)
with open('/data/rnv_big-data_mining/data/general/auth.json', 'w') as f:
    f.write(response.text)