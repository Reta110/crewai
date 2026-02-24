import os
from dotenv import load_dotenv

load_dotenv()

def get_config() -> dict:
    """Read API and DB configuration from environment."""
    return {
        "db": {
            "host": os.getenv("DB_HOST", "127.0.0.1"),
            "port": int(os.getenv("DB_PORT", "3306")),
            "database": os.getenv("DB_DATABASE", ""),
            "user": os.getenv("DB_USERNAME", "root"),
            "password": os.getenv("DB_PASSWORD", ""),
            "charset": "utf8mb4",
        },
        "api": {
            "base_url": os.getenv("HGT_API_URL", "http://localhost/api"),
            "token": os.getenv("HGT_API_TOKEN", ""),
        }
    }
