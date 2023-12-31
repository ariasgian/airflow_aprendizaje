import os
from dotenv import load_dotenv
import google.auth
#load_dotenv()
#project = os.getenv("PROJECT_ID")

credentials, project_id = google.auth.default()
print(project_id, credentials)