import os
from pymongo import MongoClient
import pandas as pd
import requests
from dotenv import load_dotenv

# Load .env values
load_dotenv(dotenv_path=r"C:\Users\devar\Documents\Code\GIFT\.env")
load_dotenv()
# MongoDB setup
MONGO_URI = os.getenv("MONGO_URL")
client = MongoClient(MONGO_URI)
db = client["app"]

# Cookie for uploading
cookie = os.getenv("IBT_COOKIE")  

# Queries
queries = {
    1: db.applications.find({}, {"company": 1, "Count_of_application": {"$literal": 1}, "createdAt": 1, "_id": 0}),
    2: db.applications.find({}, {"whereApply": 1, "Count_of_application": {"$literal": 1}, "createdAt": 1, "_id": 0}),
    3: db.jobs.aggregate([
        {
            "$project": {
                "_id": 0,
                "roleName": 1,
                "createdAt": 1,
                "appliedUserCount": {"$size": {"$ifNull": ["$appliedUsers", []]}}
            }
        }
    ]),
    4: db.users.aggregate([
  {
    "$match": { "role": "user" }
  },
  {
    "$lookup": {
      "from": "personals",
      "localField": "_id",
      "foreignField": "user",
      "as": "personalInfo"
    }
  },
  {
    "$unwind": "$personalInfo"
  },
  {
    "$addFields": {
      "orderedFields": {
        "hometown": "$personalInfo.hometown",
        "createdAt": "$createdAt",
        "count": { "$literal": 1 }
      }
    }
  },
  {
    "$project": {
      "_id": 0,
      "hometown": "$orderedFields.hometown",
      "createdAt": "$orderedFields.createdAt",
      "Totalstudent": "$orderedFields.count"
    }
  }
]),
    5: db.jobs.aggregate([
      {
        "$lookup": {
          "from":"jobshistories",
          "localField":"_id",
          "foreignField":"job_id",
          "as":"job_info"
        }
      },
        {"$unwind": "$job_info"},
        {
          "$project": {
            "_id": 0,
            "roleName": 1,
            "createdAt": "$job_info.createdAt",
            "Total_views":{"$literal": 1}
          }
        }
    ]),
    6:db.jobs.find({},{"companyName":1,"_id":0,"createdAt":1,"TotalJobs":{"$literal":1}}),
    7:db.users.find({},{"role":1,"_id":0,"createdAt":1,"TotalUsers":{"$literal":1}}),
    8:db.certifications.aggregate([
      {"$lookup": {
        "from":"certificationproviders",
        "localField":"issuedBy",
        "foreignField":"_id",
        "as":"certification_provider"
      }},
      {"$unwind":"$certification_provider"},
      {"$project":{
        "_id":0,
        "ProvidedName":"$certification_provider.ProviderName",
        "createdAt":"$certificateIssuedDate",
        "TotalCertifications":{"$literal":1}
      }}
    ]),
    9:db.users.aggregate([
      {"$match": { 
        "role": "collegeadmin",
        "name":{"$nin":["Amal Das","Swetha S","Krishna Murthy","dev s "]}} },
      {
        "$lookup":{
          "from":"loginhistories",
          "localField":"_id",
          "foreignField":"user_id",
          "as":"login_info"
        }
      },
      {"$unwind":"$login_info"},
      {
        "$project": {
          "_id": 0,
          "name": 1,
          "createdAt": "$login_info.createdAt",
          "TotalLogins": {"$literal": 1}
        }
      }
    ])
}

# Upload payloads
payloads = {
    1: {'collection_id': '132', 'type': 'Replace', 'fieldMapped': 'Object'},
    2: {'collection_id': '133', 'type': 'Replace', 'fieldMapped': 'Object'},
    3: {'collection_id': '134', 'type': 'Replace', 'fieldMapped': 'Object'},
    4: {'collection_id': '136', 'type': 'Replace', 'fieldMapped': 'Object'},
    5: {'collection_id': '146', 'type': 'Replace', 'fieldMapped': 'Object'},
    6: {'collection_id': '147', 'type': 'Replace', 'fieldMapped': 'Object'},
    7: {'collection_id': '148', 'type': 'Replace', 'fieldMapped': 'Object'},
    8: {'collection_id': '149', 'type': 'Replace', 'fieldMapped': 'Object'},
    9: {'collection_id': '155', 'type': 'Replace', 'fieldMapped': 'Object'}
}

# Upload function
def upload_csv(file_path, query_num):
    url = "https://greenestep.giftai.co.in/api/v1/csv/upload?d_type=none&"
    
    headers = {
        'Cookie': f'ticket={cookie}'
    }
    payload = payloads.get(query_num, {})
    with open(file_path, 'rb') as f:
        files = {'csvFile': (file_path, f, 'text/csv')}
        response = requests.post(url, headers=headers, data=payload, files=files)
    print(f"Uploaded Query {query_num}: {response.text}")

# Run each query
for qnum, cursor in queries.items():
    data = list(cursor)
    df = pd.DataFrame(data)
    df.rename(columns={"createdAt": "date"}, inplace=True)
    # Format date columns if present
    for col in df.columns:
        if "date" in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%m-%d-%Y')
    
    csv_file = f"query_{qnum}_mongo.csv"
    
    df.to_csv(csv_file, index=False)
    print(f"Saved CSV: {csv_file}")
    
    upload_csv(csv_file, qnum)

client.close()