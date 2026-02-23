"""Configuración de conexión a la base de datos desde variables de entorno."""
import os
from dotenv import load_dotenv

load_dotenv()


def get_db_config() -> dict:
    """Lee la configuración de MySQL desde el entorno."""
    return {
        "host": os.getenv("DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("DB_PORT", "3306")),
        "database": os.getenv("DB_DATABASE", ""),
        "user": os.getenv("DB_USERNAME", "root"),
        "password": os.getenv("DB_PASSWORD", ""),
        "charset": "utf8mb4",
        "cursorclass": None,  # pymysql usa cursors por defecto
    }
