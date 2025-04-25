# bot/database/database_config.py
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL connection parameters
POSTGRES_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'hardinfosearch'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'Paradigma1681')
}

# Connection string format
def get_postgres_uri():
    """Generate PostgreSQL connection URI from config"""
    return f"postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"

# Function to check if PostgreSQL is configured
def is_postgres_configured():
    """Check if PostgreSQL configuration is present in environment variables"""
    return all([
        os.getenv('DB_HOST'),
        os.getenv('DB_PORT'),
        os.getenv('DB_NAME'),
        os.getenv('DB_USER'),
        os.getenv('DB_PASSWORD')
    ])

def get_database_engine():
    """Returns 'postgres' or 'sqlite' based on configuration"""
    return 'postgres' if is_postgres_configured() else 'sqlite'