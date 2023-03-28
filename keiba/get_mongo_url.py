from pathlib import Path
from prefect import task
import os
import json


@task
def get_mongo_url():
  cred_file = open(os.path.join(str(Path.home()), '.config/.mongo_credential.json')) 
  mongo_cred = json.load(cred_file)
  cred_file.close()
  url = f"mongodb+srv://{mongo_cred['user']}:{mongo_cred['pw']}@horse.wqv7atq.mongodb.net/?retryWrites=true&w=majority"
  return url