from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

import faker
import time

fake = faker.Faker()

# Establish a connection to the database
client = MongoClient('mongodb://yuhan1791:Eu7162001@ac-h4yccje-shard-00-00.u2xe9xd.mongodb.net:27017,ac-h4yccje-shard-00-01.u2xe9xd.mongodb.net:27017,ac-h4yccje-shard-00-02.u2xe9xd.mongodb.net:27017/?ssl=true&replicaSet=atlas-13xx57-shard-0&authSource=admin&retryWrites=true&w=majority', server_api=ServerApi('1'))

# Select the database
db = client['db_speed_test_v2']

# Select the collections
candidates_collection = db['candidates']
job_listings_collection = db['job_listings']
applications_collection = db['applications']

start_time = time.time()

# Generate and insert the data
for _ in range(600):
    # Insert a candidate
    candidate = {
        "name": fake.name(),
        "email": fake.email()
    }
    candidate_id = candidates_collection.insert_one(candidate).inserted_id

    # Insert a job listing
    job_listing = {
        "title": fake.job()
    }
    job_id = job_listings_collection.insert_one(job_listing).inserted_id

    # Insert an application
    application = {
        "candidate_id": candidate_id,
        "job_id": job_id
    }
    applications_collection.insert_one(application)

print(f'Data generation took: {time.time() - start_time} seconds')

# Test read speed
start_time = time.time()

cursor = db.applications.aggregate([
    {"$lookup":
        {
            "from": "candidates",
            "localField": "candidate_id",
            "foreignField": "_id",
            "as": "candidate"
        }
    },
    {"$lookup":
        {
            "from": "job_listings",
            "localField": "job_id",
            "foreignField": "_id",
            "as": "job"
        }
    }
])

for document in cursor:
    pass

print("---- MONGODB Querying Time ----")
print('Data querying took:' + str(time.time() - start_time) + " seconds")
