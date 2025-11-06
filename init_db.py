# init_db.py
from database import setup_database_standalone
from dotenv import load_dotenv

print("Initializing database...")
load_dotenv()
setup_database_standalone()
print("Database initialization complete.")
