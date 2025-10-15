import logging
import os
import pandas as pd
from sqlalchemy.exc import IntegrityError

from database_models import StoreBusinessHours, StoreStatus, StoreTimezones
from database import Session as DBSession

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

batch_size = 10000


def ingest_menu_hours(csv_path):
    """Ingest menu hours data from the CSV into PostgreSQL."""
    
    logger.info(f"Starting ingestion of menu hours data from {csv_path}")
    
    session = DBSession()
    total_ingested = 0
    
    try:
        chunked_data = pd.read_csv(csv_path, chunksize=batch_size)
        total_rows = 0
        
        for chunk in chunked_data:
            total_rows += len(chunk)
            
        logger.info(f"Total rows found: {total_rows:,}")
        
        chunked_data = pd.read_csv(csv_path, chunksize=batch_size)     
        for chunk in chunked_data:
            chunk = chunk.dropna()
            
            batched_data = [ 
                StoreBusinessHours(
                    store_id=row['store_id'],
                    dayOfWeek=str(row['dayOfWeek']),
                    start_time_local=row['start_time_local'],
                    end_time_local=row['end_time_local']
                )
                for _, row in chunk.iterrows()
            ]
            
            try:
                session.bulk_save_objects(batched_data)
                session.commit()
                total_ingested += len(batched_data)
                logger.info(f"Processed batch: {len(batched_data)} records")
            except IntegrityError as e:
                logger.warning(f"Integrity error in batch: {e}")
                session.rollback()
    
    except Exception as e:
        logger.error(f"Error ingesting menu hours data: {e}")
        session.rollback()
        raise
    finally:
        session.close()
    
    logger.info(f"Menu hours ingestion completed. Total ingested: {total_ingested:,}")
    return total_ingested


def ingest_timezones(csv_path):
    """Ingest timezone data from the CSV into PostgreSQL."""
    
    logger.info(f"Starting ingestion of timezone data from {csv_path}")
    
    session = DBSession()
    total_ingested = 0
    
    try:
        chunked_data = pd.read_csv(csv_path, chunksize=batch_size)
        total_rows = 0
        
        for chunk in chunked_data:
            total_rows += len(chunk)
            
        logger.info(f"Total rows found: {total_rows:,}")
        
        chunked_data = pd.read_csv(csv_path, chunksize=batch_size)     
        for chunk in chunked_data:
            chunk = chunk.dropna()
            
            batched_data = [ 
                StoreTimezones(
                    store_id=row['store_id'],
                    timezone_str=row['timezone_str']
                )
                for _, row in chunk.iterrows()
            ]
            
            try:
                session.bulk_save_objects(batched_data)
                session.commit()
                total_ingested += len(batched_data)
                logger.info(f"Processed batch: {len(batched_data)} records")
            except IntegrityError as e:
                logger.warning(f"Integrity error in batch: {e}")
                session.rollback()
    
    except Exception as e:
        logger.error(f"Error ingesting timezone data: {e}")
        session.rollback()
        raise
    finally:
        session.close()
    
    logger.info(f"Timezone ingestion completed. Total ingested: {total_ingested:,}")
    return total_ingested


def ingest_store_status(csv_path):    
    """Ingest store status data from CSV into PostgreSQL."""
    
    logger.info(f"Starting ingestion of store status data from {csv_path}")
    
    session = DBSession()
    total_ingested = 0
    
    try:
        chunked_data = pd.read_csv(csv_path, chunksize=batch_size)
        total_rows = 0
        
        for chunk in chunked_data:
            total_rows += len(chunk)
            
        logger.info(f"Total rows found: {total_rows:,}")
        
        chunked_data = pd.read_csv(csv_path, chunksize=batch_size)     
        for chunk in chunked_data:
            chunk = chunk.dropna()
            
            batched_data = [ 
                StoreStatus(
                    store_id=row['store_id'],
                    status=row['status'],
                    store_status_data=row['timestamp_utc']
                )
                for _, row in chunk.iterrows()
            ]
            
            try:
                session.bulk_save_objects(batched_data)
                session.commit()
                total_ingested += len(batched_data)
                logger.info(f"Processed batch: {len(batched_data)} records")
            except IntegrityError as e:
                logger.warning(f"Integrity error in batch: {e}")
                session.rollback()
    
    except Exception as e:
        logger.error(f"Error ingesting store status data: {e}")
        session.rollback()
        raise
    finally:
        session.close()
    
    logger.info(f"Store status ingestion completed. Total ingested: {total_ingested:,}")
    return total_ingested


def ingest_data(data_dir="data", table="all"):
    """Ingest data from CSV files into PostgreSQL."""
    
    logger.info("Starting CSV data ingestion")
    total_ingested = 0
    
    try:
        if table == "all" or table == "store_status":
            logger.info("Ingesting store status data...")
            count = ingest_store_status(os.path.join(data_dir, "store_status.csv"))
            total_ingested += count
            
        if table == "all" or table == "menu_hours":
            logger.info("Ingesting menu hours data...")
            count = ingest_menu_hours(os.path.join(data_dir, "menu_hours.csv"))
            total_ingested += count
            
        if table == "all" or table == "timezones":
            logger.info("Ingesting timezone data...")
            count = ingest_timezones(os.path.join(data_dir, "timezones.csv"))
            total_ingested += count
            
        logger.info(f" Total records ingested: {total_ingested:,}")
        return True
        
    except Exception as e:
        logger.error(f"Data ingestion failed: {e}")
        return False
    

if __name__ == "__main__":
    logger.info("Starting CSV ingestion")
    success = ingest_data()
    if success:
        logger.info("Ingestion completed successfully!")
    else:
        logger.error("Ingestion failed.")    