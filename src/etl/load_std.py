import duckdb as dd
import polars as pl
from typing import Dict, List, Optional
import logging
from pathlib import Path
from src.config import config

# Setup logging
logging.basicConfig(level=getattr(logging, config.LOG_LEVEL))
logger = logging.getLogger(__name__)

class DataLoader:
    """Class for loading data into DuckDB data warehouse"""
    
    def __init__(self):
        self.config = config()
        self.db_path = self.config.DATABASE_PATH
        self.connection = None
    
    def connect(self) -> dd.DuckDBPyConnection:
        """
        Create connection to DuckDB database
        
        Returns:
            DuckDB connection object
        """
        try:
            # Ensure database directory exists
            df_path = Path(self.db_path)
            if not df_path.parent.exists():
                self.db_path.parent.mkdir(parents=True, exist_ok=True)
            
            
            # Create connection
            self.connection =dd.connect(self.db_path)
            logger.info(f"Connected to DuckDB at {self.db_path}")
            return self.connection
            
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def create_schema(self):
        """Create database schema for data warehouse"""
        logger.info("Creating database schema")
        
        if not self.connection:
            self.connect()
        
        try:
            # Create schemas (like create the folders for the database)
            # self.connection.execute("CREATE SCHEMA IF NOT EXISTS warehouse")
            # self.connection.execute("CREATE SCHEMA IF NOT EXISTS fact")
            
            # Create dimension tables
            self.create_dimension_tables()
            
            # # Create fact tables
            self.create_fact_tables()
            
            logger.info("Database schema created successfully")
            
        except Exception as e:
            logger.error(f"Error creating schema: {str(e)}")
            raise
    
    def create_dimension_tables(self):
        """Create dimension tables"""
        
        # Date dimension
        self.connection.execute("""
            CREATE OR REPLACE TABLE dim_date (
                date_key DATE PRIMARY KEY,
                date DATE,
                year INTEGER,
                quarter INTEGER,
                month INTEGER,
                month_name VARCHAR,
                day INTEGER,
                day_of_week INTEGER,
                day_name VARCHAR,
                week_of_year INTEGER,
                is_weekend BOOLEAN
            )
        """)
        
        # Customer dimension
        self.connection.execute("""
            CREATE OR REPLACE TABLE dim_customers (
                customer_id INTEGER PRIMARY KEY,
                company_name VARCHAR,
                first_name VARCHAR,
                last_name VARCHAR,
                full_name VARCHAR,
                email_address VARCHAR,
                job_title VARCHAR,
                business_phone VARCHAR,
                city VARCHAR,
                state_province VARCHAR,
                country_region VARCHAR,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)
        
        # Product dimension
        self.connection.execute("""
            CREATE OR REPLACE TABLE dim_products (
                product_key INTEGER PRIMARY KEY,
                product_code VARCHAR,
                product_name VARCHAR,
                description TEXT,
                category VARCHAR,
                standard_cost DECIMAL(10,2),
                list_price DECIMAL(10,2),
                quantity_per_unit VARCHAR,
                reorder_level INTEGER,
                target_level INTEGER,
                minimum_reorder_quantity DECIMAL(10,2),
                is_discontinued BOOLEAN,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)
        
        # Supplier dimension
        self.connection.execute("""
            CREATE OR REPLACE TABLE dim_suppliers (
                supplier_key INTEGER PRIMARY KEY,
                company_name VARCHAR,
                first_name VARCHAR,
                last_name VARCHAR,
                contact_name VARCHAR,
                email_address VARCHAR,
                job_title VARCHAR,
                business_phone VARCHAR,
                city VARCHAR,
                state_province VARCHAR,
                country_region VARCHAR,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)
        
        # Employee dimension
        self.connection.execute("""
            CREATE OR REPLACE TABLE dim_employees (
                employee_key INTEGER PRIMARY KEY,
                company_name VARCHAR,
                first_name VARCHAR,
                last_name VARCHAR,
                full_name VARCHAR,
                email_address VARCHAR,
                job_title VARCHAR,
                business_phone VARCHAR,
                city VARCHAR,
                state_province VARCHAR,
                country_region VARCHAR,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)
    
    def create_fact_tables(self):
        """Create fact tables"""
        
        # Sales fact table
        self.connection.execute("""
            CREATE OR REPLACE TABLE fact_sales (
                sale_id INTEGER PRIMARY KEY,
                order_id INTEGER,
                customer_key INTEGER,
                employee_key INTEGER,
                product_key INTEGER,
                order_date_key DATE,
                shipped_date_key DATE,
                quantity INTEGER,
                unit_price DECIMAL(10,2),
                discount DECIMAL(5,2),
                gross_amount DECIMAL(12,2),
                net_amount DECIMAL(12,2),
                shipping_fee DECIMAL(10,2),
                taxes DECIMAL(10,2),
                order_status_id INTEGER,
                created_at TIMESTAMP,
                

            )
        """)
        # FOREIGN KEY (customer_key) REFERENCES dim_customers(customer_id),
        # FOREIGN KEY (employee_key) REFERENCES dim_employees(employ{ee_key),
        # FOREIGN KEY (product_key) REFERENCES dim_products(product_key),
        # FOREIGN KEY (order_date_key) REFERENCES dim_date(date_key)
        
        
        # # Purchase fact table
        # self.connection.execute("""
        #     CREATE OR REPLACE TABLE fact.fact_purchases (
        #         purchase_id INTEGER PRIMARY KEY,
        #         purchase_order_id INTEGER,
        #         supplier_key INTEGER,
        #         product_key INTEGER,
        #         order_date_key DATE,
        #         expected_date_key DATE,
        #         quantity INTEGER,
        #         unit_cost DECIMAL(10,2),
        #         total_cost DECIMAL(12,2),
        #         shipping_fee DECIMAL(10,2),
        #         taxes DECIMAL(10,2),
        #         purchase_status_id INTEGER,
        #         created_at TIMESTAMP,
        #         FOREIGN KEY (supplier_key) REFERENCES dimension.dim_suppliers(supplier_key),
        #         FOREIGN KEY (product_key) REFERENCES dimension.dim_products(product_key),
        #         FOREIGN KEY (order_date_key) REFERENCES dimension.dim_date(date_key)
        #     )
        # """)
    
    def load_dataframe(self, df: pl.DataFrame, table_name: str) -> bool:
        """
        Load Polars DataFrame into DuckDB table
        
        Args:
            df: Polars DataFrame to load
            table_name: Name of the target table
            schema: Database schema (dimension or fact)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.connection:
                self.connect()
            
            # Convert Polars DataFrame to Arrow Table for better DuckDB integration
            arrow_table = df.to_arrow()
            
            # Register the Arrow table with DuckDB
            self.connection.register("temp_table", arrow_table)
            
            # Insert data into target table
            full_table_name = f"{table_name}"
            self.connection.execute(f"CREATE OR REPLACE TABLE  {full_table_name} AS SELECT * FROM temp_table")
            
            # Clean up temporary table
            self.connection.unregister("temp_table")
            
            logger.info(f"Successfully loaded {len(df)} rows into {full_table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading data into {table_name}: {str(e)}")
            return False
    
    def load_all_data(self, transformed_data: Dict[str, pl.DataFrame]) -> bool:
        """
        Load all transformed data into the data warehouse
        
        Args:
            transformed_data: Dictionary of transformed DataFrames
            
        Returns:
            True if all data loaded successfully, False otherwise
        """
        logger.info("Starting data loading process")
        
        if not self.connection:
            self.connect()
        
        # Create schema first
        self.create_schema()
        
        # ตรวจสอบว่าตารางถูกสร้างใน schema จริงหรือไม่
        tables_in_schema = self.connection.sql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';")
        tables_in_schema.show()
        
        
        success_count = 0
        total_tables = len(transformed_data)
        
        # Load dimension tables first
        dimension_tables = {k: v for k, v in transformed_data.items() if k.startswith("dim_")}
        
        for table_name, df in dimension_tables.items():
            success = self.load_dataframe(df, table_name)
            if success:
                success_count += 1
        
        # Load fact tables
        fact_tables = {k: v for k, v in transformed_data.items() if k.startswith("fact_")}
        
        for table_name, df in fact_tables.items():
            if self.load_dataframe(df, table_name):
                success_count += 1
        
        logger.info(f"Data loading complete: {success_count}/{total_tables} tables loaded successfully")
        return success_count == total_tables