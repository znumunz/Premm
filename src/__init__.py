"""
Data Warehouse Project
============================

A complete data warehouse solution for retail analytics using Python, Polars, DuckDB, and Streamlit.

Modules:
--------
- config: Configuration management
- etl: Extract, Transform, Load pipeline
- models: Data models and schemas


Example:
--------
from src.etl.extract import DataExtractor
from src.etl.transform import DataTransformer
from src.etl.load import DataLoader

# Extract data
extractor = DataExtractor()
raw_data = extractor.extract_all_tables()

# Transform data
transformer = DataTransformer()
transformed_data = transformer.transform_all_data(raw_data)

# Load data
loader = DataLoader()
loader.load_all_data(transformed_data)
"""

__version__ = "1.0.0"
__author__ = "rungseang kongsuk"
__email__ = "rungseang.k@kkumail.com"

# Make key classes available at package level
from .config import *

__all__ = [
"Config",
"__version__",
"__author__",
"__email__"
] # used to define what is imported when using 'from source import *'