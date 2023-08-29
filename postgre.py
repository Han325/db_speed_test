import psycopg2
from psycopg2 import sql
import faker
import time

fake = faker.Faker()

# Establish a connection to the database
conn = psycopg2.connect(database="db_speed_test_v2", user="postgres", password="Eu7162001", host="localhost", port="5432")

# Create a cursor object
cur = conn.cursor()

start_time = time.time()

# Generate and insert the data
for _ in range(1000000):
    # Insert a candidate
    candidate_name = fake.name()
    candidate_email = fake.email()
    cur.execute(sql.SQL("INSERT INTO candidates (name, email) VALUES (%s, %s) RETURNING id"), (candidate_name, candidate_email))

    # Get the inserted candidate's id
    candidate_id = cur.fetchone()[0]

    # Insert a job listing
    job_title = fake.job()
    cur.execute(sql.SQL("INSERT INTO job_listings (title) VALUES (%s) RETURNING id"), (job_title,))

    # Get the inserted job listing's id
    job_id = cur.fetchone()[0]

    # Insert an application
    cur.execute(sql.SQL("INSERT INTO applications (candidate_id, job_id) VALUES (%s, %s)"), (candidate_id, job_id))

print(f'Data generation took: {time.time() - start_time} seconds')

# Commit the transaction
conn.commit()

# Test read speed
start_time = time.time()

cur.execute(sql.SQL("SELECT * FROM applications JOIN candidates ON applications.candidate_id = candidates.id JOIN job_listings ON applications.job_id = job_listings.id"))



print("---- POSTGRESQL Querying Time ----")
print(f'Data querying took: {time.time() - start_time} seconds')

# Close the cursor and connection
cur.close()
conn.close()
