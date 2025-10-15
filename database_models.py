from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, Time

Base = declarative_base()

class StoreStatus(Base):
    """Store Status table"""
    __tablename__ = "store_status"
    
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String)
    status = Column(String)
    store_status_data = Column(DateTime)
 

class StoreBusinessHours(Base):
    """Store Business Hours table"""
    __tablename__ = "menu_hours"
    
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String)
    dayOfWeek = Column(Integer)
    start_time_local = Column(Time)
    end_time_local = Column(Time)
    
    

class StoreTimezones(Base):
    """Store Timezones table"""
    
    __tablename__ = "timezones"
    
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String)
    timezone_str = Column(String)
    
    
class ReportDownloads(Base):
    """Report Downloads table"""
    
    __tablename__ = "report_downloads"
    id = Column(Integer, primary_key=True, index=True)
    report_name = Column(String)
    status = Column(String)
    report_file_path = Column(String, nullable=True)
    error_message = Column(String, nullable=True)
        