import requests
import json
import os
from dotenv import load_dotenv
load_dotenv()

Auth=os.getenv('TOKEN')
url = "https://greenestep.giftai.co.in/api/v1/csv"

headers = {
  'Cookie': 'ticket=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImRldmFyYWpAaWJhY3VzdGVjaGxhYnMuaW4iLCJpZCI6NCwidHlwZSI6IkFETUlOIiwiaWF0IjoxNzQ2NzAyMDk1LCJleHAiOjE3NDY3NDUyOTV9.DdDTi3weT9nz3XzT4luufRlSD6ILdJehMc_4AmBakCg',
  'Content-Type': 'application/json',
  'Authorization': f'Bearer {Auth}'
}

Collections=[
  {
  "collection_description": "Careersheet_dashboard",
  "collection_name": "collegeadmin by TotalLogins",
  "collection_permission": "READ",
  "collection_type": "PUBLIC"
  }
]

for collection in Collections:
  payload = json.dumps(collection)
  response = requests.request("POST", url, headers=headers, data=payload)
  print(response.text)