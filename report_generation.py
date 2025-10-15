import os
import time
import pandas as pd
import pytz
from datetime import datetime, timedelta, time as dt_time
from sqlalchemy.orm import Session
from sqlalchemy import func
from collections import defaultdict
from celery_app import celery_app
from database import Session as DBSession
from database_models import StoreStatus, StoreBusinessHours, StoreTimezones, ReportDownloads

DEFAULT_TIMEZONE = 'America/Chicago'
DEFAULT_BUSINESS_HOURS = (dt_time(0, 0, 0), dt_time(23, 59, 59))

@celery_app.task(bind=True, name="generate_store_report")
def generate_store_report(self, report_id: str):
    db = DBSession()
    start_time = time.time()
    
    try:
        report_record = db.query(ReportDownloads).filter(ReportDownloads.report_name == report_id).first()
        if not report_record:
            report_record = ReportDownloads(report_name=report_id, status="Running")
            db.add(report_record)
        else:
            report_record.status = "Running"
        db.commit()
        
        # Get latest timestamp from store status table
        latest_timestamp = db.query(func.max(StoreStatus.store_status_data)).scalar()
        print(f"Latest Timestamp: {latest_timestamp}")
        
        report_end_time = latest_timestamp.replace(second=0, microsecond=0) + timedelta(minutes=1)
        
        # Get all stores Ids
        query = db.query(StoreStatus.store_id).distinct().all()
        store_ids = [row[0] for row in query]
        total_stores = len(store_ids)

        print(f"Total Number Of Stores Found: {total_stores}")
        
        # Fetch timezones data at once
        rows = db.query(StoreTimezones).filter(StoreTimezones.store_id.in_(store_ids)).all()
        timezone_data = {row.store_id: row.timezone_str for row in rows}

        business_hours_data = defaultdict(dict)
        for row in db.query(StoreBusinessHours).filter(StoreBusinessHours.store_id.in_(store_ids)).all():
            business_hours_data[row.store_id][row.dayOfWeek] = (row.start_time_local, row.end_time_local)
        
        # Fetch status records at once
        status_records = db.query(StoreStatus).filter(StoreStatus.store_id.in_(store_ids)).order_by(StoreStatus.store_id, StoreStatus.store_status_data).all()
        
        status_records_by_store = defaultdict(list)
        for record in status_records:
            status_records_by_store[record.store_id].append(record)
        
        # cache timezone objects
        timezone_cache = {}
        for timezone_str in set(timezone_data.values()):
            try:
                timezone_cache[timezone_str] = pytz.timezone(timezone_str)
            except pytz.UnknownTimeZoneError:
                timezone_cache[timezone_str] = pytz.timezone(DEFAULT_TIMEZONE)
        
        print(f"Data loaded - Stores: {len(store_ids)}, Timezones: {len(timezone_data)}, Business Hours: {len(business_hours_data)}, Status Records: {len(status_records)}, Timezone Objects: {len(timezone_cache)}")

        report_data = []
        successful_stores = 0
        failed_stores = 0
        
        for i, store_id in enumerate(store_ids):
            try:
                metrics = get_stores_status_data(db, store_id, report_end_time, timezone_data, business_hours_data, status_records_by_store, timezone_cache)
                report_data.append(metrics)
                successful_stores += 1
                
                if (i + 1) % 500 == 0 or i == 0:
                    print(f"Progress: {i + 1}/{total_stores} stores processed ({successful_stores} successful, {failed_stores} failed)")
                
                progress = int((i + 1) / total_stores * 100)
                self.update_state(
                    state='PROGRESS', 
                    meta={'current': progress, 'total': 100, 'status': f'Processed {i + 1}/{total_stores} stores ({successful_stores} successful, {failed_stores} failed)'}
                )
                
            except Exception as e:
                print(f"Error processing store {store_id}: {e}")
                failed_stores += 1
                # Add zero metrics for failed stores
                report_data.append({
                    "store_id": store_id,
                    "uptime_last_hour(in minutes)": 0.0,
                    "uptime_last_day(in hours)": 0.0,
                    "uptime_last_week(in hours)": 0.0,
                    "downtime_last_hour(in minutes)": 0.0,
                    "downtime_last_day(in hours)": 0.0,
                    "downtime_last_week(in hours)": 0.0
                })
        
        print(f"Completed processing all stores: {successful_stores} successful, {failed_stores} failed")
        
        # Save report
        print(f"Saving report to CSV...")
        report_df = pd.DataFrame(report_data)
        os.makedirs("reports", exist_ok=True)
        report_filepath = f"reports/{report_id}.csv"
        report_df.to_csv(report_filepath, index=False)
        
        print(f"Report saved to: {report_filepath}")
        print(f"Report contains {len(report_df)} rows")
        
        # Update status
        report_record.status = "Completed"
        report_record.report_file_path = report_filepath
        db.commit()
        
        execution_time = time.time() - start_time
        print(f"Total execution time: {execution_time:.2f} seconds")
        print(f"Report generation completed successfully!")
        
        self.update_state(state='SUCCESS', meta={
            'current': 100, 'total': 100, 'status': 'Report generation completed',
            'execution_time': execution_time, 'total_stores': total_stores, 'report_file': report_filepath
        })
        
        return {'status': 'completed', 'execution_time': execution_time, 'total_stores': total_stores, 'report_file': report_filepath}
        
    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = f"Report generation failed: {str(e)}"
        
        if 'report_record' in locals():
            report_record.status = "Failed"
            report_record.error_message = error_msg
            db.commit()
        
        self.update_state(state='FAILURE', meta={'current': 0, 'total': 100, 'status': error_msg, 'execution_time': execution_time})
        raise e
    finally:
        db.close()


def get_stores_status_data(db: Session, store_id: str, report_end_time: datetime, timezone_data: dict, business_hours_data: dict, status_records_by_store: dict, timezone_cache: dict) -> dict:
    """
   Get store online/offline status data
    """
    timezone_str = timezone_data.get(store_id, DEFAULT_TIMEZONE)
    tz = timezone_cache.get(timezone_str, pytz.timezone(DEFAULT_TIMEZONE))
    
    business_hours = business_hours_data.get(store_id, {})
    if not business_hours:
        for day in range(7):
            business_hours[day] = DEFAULT_BUSINESS_HOURS   # Default to 24/7
    
    status_records = status_records_by_store.get(store_id, [])
    
    if not status_records:
        return {
            "store_id": store_id,
            "uptime_last_hour(in minutes)": 0.0,
            "uptime_last_day(in hours)": 0.0,
            "uptime_last_week(in hours)": 0.0,
            "downtime_last_hour(in minutes)": 0.0,
            "downtime_last_day(in hours)": 0.0,
            "downtime_last_week(in hours)": 0.0
        }
    
    periods = [
        ("last_hour", timedelta(hours=1)),
        ("last_day", timedelta(days=1)),
        ("last_week", timedelta(weeks=1))
    ]
    
    result = {"store_id": store_id}
    
    for period_name, period_delta in periods:
        period_start = report_end_time - period_delta
        
        # Calculate uptime/downtime      
        uptime_mins, downtime_mins = calculate_period_metrics(
            status_records, period_start, report_end_time, tz, business_hours
        )
        
        if period_name == "last_hour":
            result[f"uptime_{period_name}(in minutes)"] = round(uptime_mins, 2)
            result[f"downtime_{period_name}(in minutes)"] = round(downtime_mins, 2)
        else:
            result[f"uptime_{period_name}(in hours)"] = round(uptime_mins / 60.0, 2)
            result[f"downtime_{period_name}(in hours)"] = round(downtime_mins / 60.0, 2)
    
    return result


def calculate_period_metrics(all_status_records, period_start, period_end, tz, business_hours):
    """
    Calculate uptime and downtime for a given time period
    """
    uptime_minutes = 0.0
    downtime_minutes = 0.0

    # Sort all records by timestamp
    sorted_records = sorted(all_status_records, key=lambda x: x.store_status_data)

    # Find the last known record before the period starts to fill the gap
    most_recent_record_before_period = None
    for record in sorted_records:
        if record.store_status_data < period_start:
            if most_recent_record_before_period is None or record.store_status_data > most_recent_record_before_period.store_status_data:
                most_recent_record_before_period = record

    # Find the most recent record at or before period_end
    most_recent_record_at_end = None
    for record in sorted_records:
        if record.store_status_data <= period_end:
            if most_recent_record_at_end is None or record.store_status_data > most_recent_record_at_end.store_status_data:
                most_recent_record_at_end = record

    # Return 0 if no records found
    if most_recent_record_at_end is None:
        return 0.0, 0.0

    # Get all the records present in the time window
    period_records = [
        r for r in sorted_records
        if period_start <= r.store_status_data <= period_end
    ]

    # Case 1: No records found in the time window, assume last known status remains same
    if not period_records:
        business_minutes = calculate_business_hours_duration(period_start, period_end, tz, business_hours)
        if most_recent_record_at_end.status == "active":
            uptime_minutes = business_minutes
        else:
            downtime_minutes = business_minutes
        return uptime_minutes, downtime_minutes

    # Case 2: Records exist in the window, fill the gap from period_start to first record if it exists
    first_record = min(period_records, key=lambda x: x.store_status_data)

    # If the first record is after period_start, fill the gap
    if first_record.store_status_data > period_start:
        gap_minutes = calculate_business_hours_duration(period_start, first_record.store_status_data, tz, business_hours)
        
        # Fill missing time using the most recent known status before the period
        if most_recent_record_before_period and most_recent_record_before_period.status == "active":
            uptime_minutes += gap_minutes
        else:
            downtime_minutes += gap_minutes

    # Calculate uptime/downtime between each record and the next one
    sorted_period_records = sorted(period_records, key=lambda x: x.store_status_data)

    for i, record in enumerate(sorted_period_records):
        segment_end = (
            sorted_period_records[i + 1].store_status_data
            if i < len(sorted_period_records) - 1
            else period_end
        )

        # Calculate overlap duration in business hours
        business_minutes = calculate_business_hours_duration(
            record.store_status_data, segment_end, tz, business_hours
        )

        if record.status == "active":
            uptime_minutes += business_minutes
        else:
            downtime_minutes += business_minutes

    return uptime_minutes, downtime_minutes


def calculate_overlap_minutes(start_dt, end_dt, window_start_dt, window_end_dt):
    """
    Calculate the overlap (in minutes) 
    """
    overlap_start = max(start_dt, window_start_dt)
    overlap_end = min(end_dt, window_end_dt)

    if overlap_start < overlap_end:
        return (overlap_end - overlap_start).total_seconds() / 60.0
    return 0.0

def calculate_business_hours_duration(start_time, end_time, tz, business_hours):
    """
    Calculate how many minutes between start_time and end_time fall within
    the store's business hours (in the store's local timezone)
    """

    total_minutes = 0.0

    # Convert timestamps to the store's local timezone
    start_local = start_time.astimezone(tz)
    end_local = end_time.astimezone(tz)

    current_day = start_local
    while current_day.date() <= end_local.date():
        day_of_week = current_day.weekday()  # Monday = 0, Sunday = 6

        # Skip the day if no business hours is present
        if day_of_week not in business_hours:
            current_day = datetime.combine(current_day.date(), dt_time.min, tzinfo=tz) + timedelta(days=1)
            continue

        open_time_local, close_time_local = business_hours[day_of_week]

        # Case 1: Normal hours (e.g., 10:00 AM to 9:00 PM) for a day
        if open_time_local <= close_time_local:
            business_open = datetime.combine(current_day.date(), open_time_local, tzinfo=tz)
            business_close = datetime.combine(current_day.date(), close_time_local, tzinfo=tz)

            total_minutes += calculate_overlap_minutes(
                start_local, end_local, business_open, business_close
            )

        # Case 2: Overnight hours (e.g., 10:00 PM to 6:00 AM next day) for a day
        else:
            evening_open = datetime.combine(current_day.date(), open_time_local, tzinfo=tz)
            midnight = datetime.combine(current_day.date(), dt_time.max, tzinfo=tz)

            total_minutes += calculate_overlap_minutes(
                start_local, end_local, evening_open, midnight
            )

            morning_open = datetime.combine(
                current_day.date() + timedelta(days=1), dt_time.min, tzinfo=tz
            )
            morning_close = datetime.combine(
                current_day.date() + timedelta(days=1), close_time_local, tzinfo=tz
            )

            total_minutes += calculate_overlap_minutes(
                start_local, end_local, morning_open, morning_close
            )

        current_day = datetime.combine(current_day.date(), dt_time.min, tzinfo=tz) + timedelta(days=1)

    return total_minutes
