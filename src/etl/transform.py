"""
Data transformation module for creating dimensional model
"""

import polars as pl
from typing import Dict, List, Optional
import logging
from datetime import datetime
from src.config import config


# Setup logging
logging.basicConfig(level=getattr(logging, 'INFO'),
                    format='%(asctime)s - %(levelname)s - %(message)s'
                    )
logger = logging.getLogger(__name__)


# Setup logging
logging.basicConfig(level=getattr(logging, config.LOG_LEVEL),
                    format='%(asctime)s - %(levelname)s - %(message)s'
                    )
logger = logging.getLogger(__name__)

class DataTransformer:
    def __init__(self):
        self.config = config()
        # self.transformed_data = {}

    def standardize_column_names(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Standardize column names by converting to lowercase and replacing spaces and hyphens with underscores.
        
        Args:
            df: Input DataFrame
        Returns:
            DataFrame with standardized column names    
        """
        new_columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
        return df.rename(dict(zip(df.columns, new_columns)))       

    def transform_Airlines(self,df: pl.DataFrame) -> pl.DataFrame:
        """Transform customers data into dimension table
            1. select columns `id` (rename to `customer_id`), `company` (rename to `company_name`)
                `first_name`, `last_name`, `email_address`, `job_title`, `business_phone`
                , `address`, `city`, `state_province`, `country_region`, 
                    and `zip_postal_code` (renamed to `postal_code`)
            2. create full_name by concatenating first_name and last_name
            3. sort by `id` and filter out rows where `id` is null
            4. create timestamp columns created_at and updated_at
        """
        logging.info("=== Transforming customers dimension ===")
        
        # Standardize column names (lowercase, replace spaces and hyphen with underscores)
        df_clean = self.standardize_column_names(df)
        
        # Transform the DataFrame
        dim_customers = (df_clean.select(
                                    pl.col("id").alias("customer_id"),
                                    pl.col("company").alias("company_name"),
                                    pl.col("first_name"),
                                    pl.col("last_name"),
                                    pl.col("email_address"),
                                    pl.col("job_title"),
                                    pl.col("business_phone"),
                                    pl.col("address"),
                                    pl.col("city"),
                                    pl.col("state_province"),
                                    pl.col("country_region"),
                                    pl.col("zip_postal_code").alias("postal_code"),
                                    pl.concat_str([pl.col("first_name"), pl.col("last_name")], separator=" ").alias("full_name"),
                                    pl.lit(datetime.now()).alias("created_at"),
                                    pl.lit(datetime.now()).alias("updated_at"))
                        .unique(
                            "customer_id"
                            )
                        .sort(
                            "customer_id"
                            )
                        .filter(
                            pl.col("customer_id")
                            .is_not_null()
                            ))
        
        return dim_customers
    
    def transform_Airplanes(self,df: pl.DataFrame) -> pl.DataFrame:
        """
        Transform employees data into dimension table
        1. select columns `id` (rename to `employee_key`), `company`, `first_name`, `last_name`
            , `email_address`, `job_title`, `business_phone`, `city`, `state_province`, 
            and `country_region`
        2. create full_name by concatenating first_name and last_name
        3. create timestamp columns created_at and updated_at
        """
        logging.info("=== Transforming employees dimension ===")
        
        df_clean = self.standardize_column_names(df)
        
        # Create employee dimension
        
        dim_employees = (df_clean.select(
                                    pl.col("id").alias("employee_key"),
                                    pl.col("company"),
                                    pl.col("first_name"),
                                    pl.col("last_name"),
                                    pl.col("email_address"),
                                    pl.col("job_title"),
                                    pl.col("business_phone"),
                                    pl.col("city"),
                                    pl.col("state_province"),
                                    pl.col("country_region"),
                                    pl.concat_str([pl.col("first_name"), pl.col("last_name")], separator=" ").alias("full_name"),
                                    pl.lit(datetime.now()).alias("created_at"),
                                    pl.lit(datetime.now()).alias("updated_at"))
                        .unique(
                            "employee_key"
                            )
                        .sort(
                            "employee_key"
                            )
                        .filter(
                            pl.col("employee_key")
                            .is_not_null()
                            ))
        
        return dim_employees
    
    def transform_Airports(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform products data into dimension table
            1. select columns `id` (rename to `product_key`), `product_code`, `product_name`,
            `description`, `category`, `standard_cost`, `list_price`, `quantity_per_unit`,
            `reorder_level`, `target_level`, `minimum_reorder_quantity`, `discontinued`
            2. create is_discontinued column as boolean based on discontinued value
            3. create timestamp columns created_at and updated_at
        """
        logger.info("Transforming products dimension")
        
        df_clean = self.standardize_column_names(df)
        
        # Create product dimension
        dim_product = (df_clean.select(
                                    pl.col("id").alias("product_key"),
                                    pl.col("product_code"),
                                    pl.col("product_name"),
                                    pl.col("description"),
                                    pl.col("category"),
                                    pl.col("standard_cost"),
                                    pl.col("list_price"),
                                    pl.col("quantity_per_unit"),
                                    pl.col("reorder_level"),
                                    pl.col("target_level"),
                                    pl.col("minimum_reorder_quantity"),
                                    (pl.col("discontinued").cast(str) == "Yes").alias("is_discontinued"),  # Fix: cast to string before compare
                                    pl.lit(datetime.now()).alias("created_at"),
                                    pl.lit(datetime.now()).alias("updated_at"))
                        .unique(
                            "product_key"
                            )
                        .sort(
                            "product_key"
                            )
                        .filter(
                            pl.col("product_key")
                            .is_not_null()
                            ))
        
        return dim_product
    
    def transform_Routes(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform suppliers data into dimension table
            1. select columns `id` (rename to `supplier_key`), `company`, `first_name`, `last_name`
                , `email_address`, `job_title`, `business_phone`, `city`, `state_province`, 
                and `country_region`
            2. create full_name by concatenating first_name and last_name
            3. create timestamp columns created_at and updated_at
        """
        logger.info("Transforming suppliers dimension")
        
        df_clean = self.standardize_column_names(df)
        
        # Create supplier dimension
        dim_supplier = (df_clean.select(
                                    pl.col("id").alias("supplier_key"),
                                    pl.col("company"),
                                    pl.col("first_name"),
                                    pl.col("last_name"),
                                    pl.col("email_address"),
                                    pl.col("job_title"),
                                    pl.col("business_phone"),
                                    pl.col("city"),
                                    pl.col("state_province"),
                                    pl.col("country_region"),
                                    pl.concat_str([pl.col("first_name"), pl.col("last_name")], separator=" ").alias("full_name"),
                                    pl.lit(datetime.now()).alias("created_at"),
                                    pl.lit(datetime.now()).alias("updated_at"))
                        .unique(
                            "supplier_key"
                            )
                        .sort(
                            "supplier_key"
                            )
                        .filter(
                            pl.col("supplier_key")
                            .is_not_null()
                            ))
        
        return dim_supplier
    
    def get_fiscal_quarter(self,start_month: int) -> pl.Expr:
        """
        Returns a Polars expression to calculate the fiscal quarter.

        Args:
            start_month: The month the fiscal year starts (1=Jan, 10=Oct).
        """
        # Formula:
        # 1. Get the month as a number (1-12).
        # 2. Shift the months so the fiscal year starts at 0.
        # 3. Group by 3 to get a 0-3 quarter index.
        # 4. Add 1 to make it 1-based (1-4).
        return (
            (pl.col("date").dt.month() - start_month + 12) % 12 // 3
        ) + 1
    
    def create_date_dimension(self) -> pl.DataFrame:
        """
        Create a date dimension table
            1. generate date range from 2015-01-01 to 2025-12-31
            2. create columns date_key, date, year, quarter, month, month_name
            day, day_of_week, day_name, week_of_year, is_weekend
            3. create fiscal_quarter based on the fiscal year starting in October
        """
        logger.info("Creating date dimension")
        
        # Generate date range
        date_range = pl.date_range(
            start=pl.datetime(1999, 1, 1),
            end=pl.datetime(2025, 12, 31),
            interval="1d",
            eager=True
        )
        
        # Create date dimension with additional attributes
        dim_date = pl.DataFrame({
            "date_key": date_range,
            "date": date_range,
            "year": date_range.dt.year(),
            "quarter": date_range.dt.quarter(),
            "month": date_range.dt.month(),
            "month_name": date_range.dt.strftime("%B"),
            "day": date_range.dt.day(),
            "day_of_week": date_range.dt.weekday(),
            "day_name": date_range.dt.strftime("%A"),
            "week_of_year": date_range.dt.week(),
            "is_weekend": date_range.dt.weekday().is_in([6, 7])
        })
        
        dim_date = dim_date.with_columns(
            self.get_fiscal_quarter(10).alias("fiscal_quarter")
            )
        
        logger.info(f"Created date dimension with {len(dim_date)} records")
        return dim_date
    
    def transform_sales_fact(self, orders_df: pl.DataFrame, order_details_df: pl.DataFrame) -> pl.DataFrame:
        """Transform orders and order details into sales fact table
            1. Clean the data by standardizing column names
            2. Join orders with order details
            3. Select relevant columns and calculate derived metrics
            4. Create timestamp columns created_at and updated_at
        """
        logger.info("Transforming sales fact table")
        
        # Clean the data
        df_orders = self.standardize_column_names(orders_df)
        df_order_details = self.standardize_column_names(order_details_df)
        
        # Join orders with order details
        df_order_join = df_orders.join(
                                    df_order_details,
                                    left_on="id",
                                    right_on="order_id",
                                    how="inner"
                            )
        sales_fact = df_order_join.select([
                                        pl.col("id").alias("sale_id"),
                                        pl.col("customer_id").alias("customer_key"),
                                        pl.col("employee_id").alias("employee_key"),
                                        pl.col("product_id").alias("product_key"),
                                        pl.col("order_date").str.to_datetime(format="%m/%d/%Y %H:%M:%S").alias("order_date_key"),
                                        pl.col("shipped_date").str.to_datetime("%m/%d/%Y %H:%M:%S").alias("shipped_date_key"),
                                        pl.col("quantity"),
                                        pl.col("unit_price"),
                                        pl.col("discount"),
                                        (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"),
                                        (pl.col("quantity") * pl.col("unit_price") * (1 - pl.col("discount") / 100)).alias("net_amount"),
                                        pl.col("shipping_fee"),
                                        pl.col("taxes"),
                                        pl.col("status_id").alias("order_status_id"),
                                        pl.lit(datetime.now()).alias("created_at")
                                ])
        return sales_fact
    
    def transform_all_data(self, raw_data: Dict[str, pl.DataFrame]) -> Dict[str, pl.DataFrame]:
        """
        Transform all raw data into dimensional model
        
        Args:
            raw_data: Dictionary of raw DataFrames
            
        Returns:
            Dictionary of transformed DataFrames
        """
        logger.info("Starting data transformation process")
        
        transformed = {}
        
        # Create dimensions
        if "Airlines" in raw_data:
            transformed["dim_Airlines"] = self.transform_Airlines(raw_data["Airlines"])
   
        if "Airplanes" in raw_data:
            transformed["Airplanes"] = self.transform_Airplanes(raw_data["Airplanes"])

        if "Airports" in raw_data:
            transformed["dim_Airports"] = self.transform_Airports(raw_data["Airports"])
        
        if "Routes" in raw_data:
            transformed["dim_Routes"] = self.transform_Routes(raw_data["Routes"])
        # Create date dimension
        transformed["dim_date"] = self.create_date_dimension()
        
        # Create fact tables
        if "orders" in raw_data and "order_details" in raw_data:
            transformed["fact_sales"] = self.transform_sales_fact(
                raw_data["orders"], 
                raw_data["order_details"]
            )
        
        logger.info(f"Transformation complete. Created {len(transformed)} tables")
        return transformed