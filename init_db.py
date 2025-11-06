# init_db.py
import sys
from database import setup_database_standalone
from dotenv import load_dotenv

print("==========================================")
print("  DATABASE INITIALIZATION SCRIPT STARTED")
print("==========================================")
try:
    load_dotenv()
    setup_database_standalone()
    print("\n[SUCCESS] Database initialization complete.")
    print("==========================================")
    print("  DATABASE INITIALIZATION SCRIPT FINISHED")
    print("==========================================")
    sys.exit(0)
except Exception as e:
    print(f"\n[FATAL] An error occurred during database initialization: {e}", file=sys.stderr)
    print("==========================================")
    print("  DATABASE INITIALIZATION SCRIPT FAILED")
    print("==========================================")
    sys.exit(1)
