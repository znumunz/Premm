"""
Data transformation module for creating dimensional model
"""


from duckdb import df
import polars as pl
from typing import Dict, List, Optional
import logging
from datetime import datetime
from src.config import config
import polars as pl


####
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
          


   def transform_customers(self,df: pl.DataFrame) -> pl.DataFrame:
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
       


#    def transform_customers(self,df_clean: pl.DataFrame) -> pl.DataFrame:
       now = datetime.now()


       dim_customers = df_clean.select(
           pl.col("customer_id"),
           pl.col("name").alias("customer_name"),
           pl.col("email"),
           pl.col("telephone"),
           pl.col("city"),
           pl.col("country"),
           pl.col("gender"),
           pl.col("date_of_birth"),
           pl.col("job_title")
       ).with_columns(
           (
           pl.col("job_title").fill_null("")),
           pl.lit(now).alias("created_at"),
           pl.lit(now).alias("updated_at")
       ).filter(
           pl.col("customer_id").is_not_null()
       ).sort(
           by="customer_id"
       )


       return dim_customers


   
  
   def transform_discounts(self,df: pl.DataFrame) -> pl.DataFrame:
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
      
       dim_discounts = df_clean.select(
           pl.col("start"),
           pl.col("end"),
           pl.col("discont").alias("discount"),
           pl.col("description"),
           pl.col("category").alias("category"),
           pl.col("sub_category").alias("sub_category"),
       ).with_columns(
               pl.col("category").fill_null(""),
               pl.col("sub_category").fill_null(""),
           pl.lit(datetime.now()).alias("created_at"),
           pl.lit(datetime.now()).alias("updated_at")
       ).filter(
           pl.col("category").is_not_null()
       ).sort(
           by="category"
       )
      
       return dim_discounts
  
   def transform_employees(self, df: pl.DataFrame) -> pl.DataFrame:
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
       dim_employees = df_clean.select(
           pl.col("employee_id"),
           pl.col("store_id"),
           pl.col("name"),
           pl.col("position"),
       ).with_columns(
           pl.lit(datetime.now()).alias("created_at"),
           pl.lit(datetime.now()).alias("updated_at")
       ).filter(
           pl.col("employee_id").is_not_null()
       ).sort(
           by="employee_id"
       )
      
       return dim_employees
  
   def transform_products(self, df: pl.DataFrame) -> pl.DataFrame:
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
       dim_products = df_clean.select(
           pl.col("product_id"),
           pl.col("category"),
           pl.col("sub_category"),
           pl.col("description_pt"),
           pl.col("description_de"),
           pl.col("description_fr"),
           pl.col("description_es"),
           pl.col("description_en"),
           pl.col("description_zh"),
           pl.col("color"),
           pl.col("sizes"),
           pl.col("production_cost")
       ).with_columns(
           (
               pl.col("color").fill_null(""),
               pl.col("sizes").fill_null(""),
           pl.lit(datetime.now()).alias("created_at"),
           pl.lit(datetime.now()).alias("updated_at"))
       ).filter(
           pl.col("product_id").is_not_null()
       ).sort(
           by="product_id"
       )
      
       return dim_products
  
   def transform_stores(self, df: pl.DataFrame) -> pl.DataFrame:
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
       dim_stores = df_clean.select(
           pl.col("store_id"),
           pl.col("country"),
           pl.col("city"),
           pl.col("store_name"),
           pl.col("number_of_employees"),
           pl.col("zip_code"),
           pl.col("latitude"),
           pl.col("longitude")
       ).with_columns(
           (
              
           pl.lit(datetime.now()).alias("created_at"),
           pl.lit(datetime.now()).alias("updated_at"))
       ).filter(
           pl.col("store_id").is_not_null()
       ).sort(
           by="store_id"
       )
      
       return dim_stores
  
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
       start=pl.datetime(2023, 1, 1),
       end=pl.datetime(2025, 12, 31),
       interval="1d",
       eager=True
       )


       # Create date dimension with additional attributes
       dim_date = pl.DataFrame({
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
       }).with_columns(pl.col("date").dt.strftime("%d%m%Y").alias("date_key")
       ).sort(by="date_key")


       dim_date = dim_date.with_columns(
       self.get_fiscal_quarter(10).alias("fiscal_quarter")
           )


       logger.info(f"Created date dimension with {len(dim_date)} records")
       return dim_date


   def transform_transactions(self, df: pl.DataFrame) -> pl.DataFrame:
       """Transform transactions data into dimension table
           1. select columns `id` (rename to `transaction_key`), `customer_id` (rename to `customer_key`)
               , `transaction_date` (rename to `transaction_date_key`), `amount`, `payment_method`
           2. create timestamp columns created_at and updated_at
       """
       logger.info("Transforming transactions dimension")
      
       df_clean = self.standardize_column_names(df)
      
       # Create transaction dimension
       dim_transactions = df_clean.select(
           pl.col("Transaction ID"),
           pl.col("Customer ID"),
           pl.col("Transaction Date").str.to_datetime(format="%m/%d/%Y %H:%M:%S").alias("transaction_date_key"),
           pl.col("Amount"),
           pl.col("Payment Method")
       ).with_columns(
           pl.lit(datetime.now()).alias("created_at"),
           pl.lit(datetime.now()).alias("updated_at")
       ).filter(
           pl.col("transaction_key").is_not_null()
       ).sort(
           by="transaction_key"
       )
      
       return dim_transactions




  
   def transform_transactions_fact(self, transactions_df: pl.DataFrame ,exchange_rates: pl.DataFrame) -> pl.DataFrame:
       """Transform orders and order details into sales fact table
       1. Clean the data by standardizing column names
       2. Join orders with order details
       3. Select relevant columns and calculate derived metrics
       4. Create timestamp columns created_at and updated_at
       """
       logger.info("Transforming sales fact table")

       df_clean = self.standardize_column_names(transactions_df)
       transactions_fact = (
        df_clean
        .select([
            pl.col("invoice_id"),
            pl.col("line").alias("line_item"),
            pl.col("customer_id"),
            pl.col("product_id"),
            pl.col("quantity"),
            pl.col("date"),
            pl.col("discount"),
            pl.col("line_total"),
            pl.col("store_id"),
            pl.col("employee_id"),
            pl.col("currency"),
            pl.col("sku").alias("stock_keeping_unit"),
            pl.col("transaction_type"),
            pl.col("payment_method"),
            pl.col("unit_price"),
            pl.lit(datetime.now()).alias("created_at"),
            pl.lit(datetime.now()).alias("updated_at"),
        ]).with_columns(pl.col("date").dt.strftime("%d%m%Y").alias("date_key"))
        .join(exchange_rates, on="currency", how="left")
        # สร้าง unit_price_usd ก่อน
        .with_columns(
            (pl.col("unit_price") * pl.col("rate_to_usd")).alias("unit_price_usd"))
        # คำนวณ gross/net/discount ใช้สูตรเดิมโดยตรง ไม่เรียก unit_price_usd ที่สร้าง
        .with_columns([
            (pl.col("quantity") * (pl.col("unit_price_usd") * pl.col("rate_to_usd"))).alias("total_revenue_usd"),
            (pl.col("quantity") * (pl.col("unit_price_usd") * pl.col("rate_to_usd")) * (1 - pl.col("discount") / 100)).alias("net_amount_usd"),
            (pl.col("quantity") * (pl.col("unit_price_usd") * pl.col("rate_to_usd")) * (pl.col("discount") / 100)).alias("discount_usd"),
            ])
        .sort("date_key")
    )


            

       return transactions_fact

    #    # Clean the data
    #    df_orders = self.standardize_column_names(orders_df)
    #    df_order_details = self.standardize_column_names(order_details_df)


       # Join orders with order details
    #    df_order_join = df_orders.join(
    #    df_order_details,
    #    left_on="id",
    #    right_on="order_id",
    #    how="inner"
    #    )
    #    transactions_fact = df_clean.select([
    #                     pl.col("invoice_id"),
    #                     pl.col("line").alias("line_item"),
    #                     pl.col("customer_id"),
    #                     pl.col("product_id"),
    #                     pl.col("unit_price").alias("unit_price"),
    #                     pl.col("quantity"),
    #                     (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"),
    #                     (pl.col("quantity") * pl.col("unit_price") * (1 - pl.col("discount") / 100)).alias("net_amount"),
    #                     pl.col("date"),
    #                     pl.col("discount"),
    #                     pl.col("line_total"),
    #                     pl.col("store_id"),
    #                     pl.col("employee_id"),
    #                     pl.col("currency"),
    #                     pl.col("currency_symbol"),
    #                     pl.col("sku").alias("stock_keeping_unit"),
    #                     pl.col("transaction_type"),
    #                     pl.col("payment_method"),
    #                     pl.lit(datetime.now()).alias("created_at"),
    #                     pl.lit(datetime.now()).alias("updated_at")]
    #                     ).with_columns(pl.col("date").dt.strftime("%d%m%Y").alias("date_key").join(
    #                                         exchange_rates, on="currency", how="left").with_columns([
    #                         (pl.col("unit_price") * pl.col("rate_to_usd")).alias("unit_price_usd"),
    #                         (pl.col("gross_amount") * pl.col("rate_to_usd")).alias("gross_amount_usd"),
    #                         (pl.col("net_amount") * pl.col("rate_to_usd")).alias("net_amount_usd"),
    #                                                                                                 ])
    #                     ).sort(by='date_key')
                        
    #    return transactions_fact
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
       if "customers" in raw_data:
           transformed["dim_customers"] = self.transform_customers(raw_data["customers"])


       if "discounts" in raw_data:
           transformed["dim_discounts"] = self.transform_discounts(raw_data["discounts"])


       if "employees" in raw_data:
           transformed["dim_employees"] = self.transform_employees(raw_data["employees"])


       if "products" in raw_data:
           transformed["dim_products"] = self.transform_products(raw_data["products"])
       if "stores" in raw_data:
           transformed["dim_stores"] = self.transform_stores(raw_data["stores"])
          


       # Create date dimension
       transformed["dim_date"] = self.create_date_dimension()


       # Create fact tables
       if "transactions" in raw_data and 'exchange_rates' in raw_data:
           transformed["fact_transactions"] = self.transform_transactions_fact(
               raw_data["transactions"],
               raw_data['exchange_rates'])
              



       logger.info(f"Transformation complete. Created {len(transformed)} tables")
       return transformed

