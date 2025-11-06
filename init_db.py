# init_db.py

from database import setup_database
from dotenv import load_dotenv

print("Initializing database...")
load_dotenv()
setup_database()
print("Database initialization complete.")
