"""
Configuration management for the Data Warehouse project
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
    
class config:
    """Configuration class for the application"""

    # # Base paths
    # BASE_DIR = Path(__file__).parent.parent
    RAW_DATA_PATH = os.getenv("RAW_DATA_DIR", "D:\Project\data")
    PROCESSED_DATA_DIR = os.getenv("PROCESSED_DATA_DIR", "processed")

    DATABASE_DIR = os.getenv("DATABASE_DIR", "data_warehouse")

    # Database configuration

    DATABASE_PATH = os.getenv("DATABASE_PATH", "data_warehouse/sales_dw.duckdb")
    DATABASE_NAME = os.getenv("DATABASE_NAME", "sales_dw.duckdb")

    # # Dashboard configuration
    # DASHBOARD_TITLE = os.getenv("DASHBOARD_TITLE", "Retail Data Warehouse Dashboard")
    # DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", 8501))
    # DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "localhost")

    # ETL configuration
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    # Date formats
    DATE_FORMAT = os.getenv("DATE_FORMAT", "%Y-%m-%d")
    DATETIME_FORMAT = os.getenv("DATETIME_FORMAT", "%Y-%m-%d %H:%M:%S")

    # # Company information
    # COMPANY_NAME = os.getenv("COMPANY_NAME", "Retail Analytics Co.")
    # TIMEZONE = os.getenv("TIMEZONE", "Asia/Bangkok")

    # # Performance settings
    # MAX_MEMORY_GB = int(os.getenv("MAX_MEMORY_GB", 4))
    # THREAD_COUNT = int(os.getenv("THREAD_COUNT", 4))

    # CSV files mapping
    # CSV_FILES = {
    #     "categories":"categories.csv",
    #     "customers":"customers.csv",
    #     "employee-territories":"employee-territories.csv",
    #     "employees":"employees.csv",
    #     "northwind":"northwind.csv",
    #     "order-details":"order-details.csv",
    #     "orders":"orders.csv",
    #     "products":"products.csv",
    #     "regions":"regions.csv",
    #     "shippers":"shippers.csv",
    #     "suppliers":"suppliers.csv",
    #     "territories":"territories.csv",
    # }
    CSV_FILES = {
        "customers":"customers.csv",
        "discounts":"discounts.csv",
        "employees":"employees.csv",
        "products":"products.csv",
        "stores":"stores.csv",
        "transactions":"transactions.csv",
    }
    # @classmethod
    # def ensure_directories(cls):
    # """Ensure all required directories exist"""
    # directories = [
    # cls.DATA_DIR,
    # cls.RAW_DATA_DIR,
    # cls.PROCESSED_DATA_DIR,
    # cls.DATABASE_DIR
    # ]

    # for directory in directories:
    # directory.mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_csv_path(cls, table_name: str) -> str:
        """Get the full path to a CSV file"""
        if table_name not in cls.CSV_FILES:
            raise ValueError(f"Unknown table: {table_name}")
        return os.path.join(cls.RAW_DATA_PATH,cls.CSV_FILES[table_name])

    @classmethod
    def get_database_path(cls) -> str:
        """Get the full path to the database file"""
        # return cls.DATABASE_DIR / cls.DATABASE_PATH
        return cls.DATABASE_PATH