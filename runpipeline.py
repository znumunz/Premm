from src import config
from src.etl.extract import SrcChecker,DataExtractor
from src.etl.transform import DataTransformer
from src.etl.load_std import DataLoader
import os
import logging
import polars as pl

# Setup logging
logging.basicConfig(level=getattr(logging, config.LOG_LEVEL),
                    format='%(asctime)s - %(levelname)s - %(message)s'
                    )
logger = logging.getLogger(__name__)

class ETLPipeline():
    """
    This class for managing the ETL pipeline
    """
    def __init__(self):
        self.config =config()
        self.check_src = SrcChecker()
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.loader = DataLoader()

    def run_check_src(self,src: list[str]=['csv']) -> bool:
        """
        Check if the source CSV files exist
        """
        logger.info("Checking source files...")
        for src_type in src:
            if 'csv' in src_type:
                success = self.check_src.check_src_csv()

        return success

        
    def main():
        print('== Starting ETL Pipeline ==')
        # Run ETL pipeline
        pipeline = ETLPipeline() # Create an instance of the ETLPipeline class
        # Check if source files exist
        success = pipeline.run_check_src()
        if not success:
            logger.error("❌ Missing source files. Please check the logs for details.")
            return
    def run_extract_znumunz(self):
        """
        Run the extraction step and return raw data
        """
        logger.info("Running extraction step...")
        raw_data = self.extractor.extract_data()
        if raw_data:
            logger.info("✅ Complete all reading the file.")
        else:
            logger.error("❌ Extraction failed.")
        return raw_data

    def run_transform(self, raw_data: dict) -> dict:
        logger.info("\n" + "="*50)
        logger.info("Running transformation step...")
        logger.info("=" * 50 + "\n")
        
        #transform all data
        transformed_data = self.transformer.transform_all_data(raw_data)
        if not transformed_data:
            logger.error("❌ No data transformed.")
        return transformed_data

    def run_load(self, transformed_data):
        success =  self.loader.load_all_data(transformed_data)
        if success:
            logger.info("✅ Data loaded successfully.")
        else:
            logger.error("❌ Loading data failed.")
        self.loader.disconnect()
        return success 
        
def main():
    logger.info('🚀 ❤️ Starting Data Warehouse ETL Pipeline')
    # Run ETL pipeline
    pipeline = ETLPipeline()  # Create an instance of the ETLPipeline class
    success = pipeline.run_check_src()
    if success:
        raw_data = pipeline.run_extract_znumunz()
        
        if raw_data:
            transformed_data = pipeline.run_transform(raw_data)
            if transformed_data:
                success = pipeline.run_load(transformed_data)
            
                if success:
                    logger.info("✅ ETL pipeline completed successfully.")
                    logger.info("You can now start the dashboard with: streamlit run src/dashboard.py")
                else:
                    logger.error("❌ ETL pipeline failed during loading phase.")
    else:   
        logger.error("❌ Missing source files. Please check the logs for details.")
        return

if __name__ == "__main__":
    main()